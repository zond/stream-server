//! What `/proxy` does with a caller-supplied remote URL.
//!
//! The route is a *relay*: it opens the target with reqwest and streams the
//! bytes back. Nothing it fetches is written to the cache the cleaner walks,
//! and nothing it fetched once is served from disk the second time. That is
//! worth pinning here because embedders lean on it in both directions -- a
//! client sending every remote stream through this server gets the header
//! rewriting and the single egress point, and does *not* get caching.
//!
//! Both URL shapes are exercised, because the server writes one of them
//! itself: stremio-core builds the Core path format
//! (`/proxy/d=<origin>&h=.../<path>`), and `rewrite_playlist` rewrites every
//! line of an HLS playlist into the query format (`/proxy/?d=<url>`).

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

/// The config every test here spreads from: no DHT bootstrap name
/// resolution, so starting a server makes no DNS query (see `embed.rs`).
fn offline_config() -> stream_server::ServerConfig {
    stream_server::ServerConfig {
        resolve_dht_bootstrap_names: false,
        ..stream_server::ServerConfig::default()
    }
}

/// One byte of the body at [`offset`]: a cheap pattern, so a range response
/// can be checked to have come from the offset it claims rather than merely
/// to have the right length.
fn byte_at(offset: usize) -> u8 {
    (offset % 251) as u8
}

/// One request an [`Origin`] was asked for: the request line, and the header
/// names and values in the order they arrived. Tests assert against this
/// rather than against what they *sent*, because the whole point of a proxy
/// bug is the gap between the two.
#[derive(Clone, Debug)]
struct Request {
    line: String,
    headers: Vec<(String, String)>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn range(&self) -> Option<&str> {
        self.header("range")
    }

    /// The request target exactly as it came off the wire -- still
    /// percent-encoded, which is the only form in which a `%2F` can be told
    /// from the separator it stands for.
    fn target(&self) -> &str {
        self.line
            .split_whitespace()
            .nth(1)
            .expect("a request line has a target")
    }
}

/// A one-connection-per-thread origin that answers with whatever its
/// responder writes to the socket, and records every request it was asked
/// for. Threaded rather than sequential because two players streaming at
/// once is exactly what the close test needs.
struct Origin {
    addr: SocketAddr,
    requests: std::sync::mpsc::Receiver<Request>,
}

const ORIGIN_LENGTH: usize = 1024 * 1024;

impl Origin {
    /// The default origin: [`ORIGIN_LENGTH`] bytes of [`byte_at`] with
    /// `Range` support, which is how a test tells "the proxy relayed the
    /// range" from "the proxy fetched the whole file and sliced it".
    fn start() -> anyhow::Result<Self> {
        Self::start_with(|request: &Request, socket: &mut TcpStream| {
            let served = request.range().and_then(|value| {
                let (first, last) = value.trim_start_matches("bytes=").split_once('-')?;
                let first: usize = first.parse().ok()?;
                let last: usize = if last.is_empty() {
                    ORIGIN_LENGTH - 1
                } else {
                    last.parse().ok()?
                };
                Some((first, last))
            });
            let (head, body) = match served {
                Some((first, last)) => {
                    let body: Vec<u8> = (first..=last).map(byte_at).collect();
                    (
                        format!(
                            "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\n\
                             Content-Type: video/mp4\r\n\
                             Content-Range: bytes {first}-{last}/{ORIGIN_LENGTH}\r\n\
                             Content-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        ),
                        body,
                    )
                }
                None => {
                    let body: Vec<u8> = (0..ORIGIN_LENGTH).map(byte_at).collect();
                    (
                        format!(
                            "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\n\
                             Content-Type: video/mp4\r\n\
                             Content-Length: {ORIGIN_LENGTH}\r\nConnection: close\r\n\r\n"
                        ),
                        body,
                    )
                }
            };
            let _ = socket.write_all(head.as_bytes());
            let _ = socket.write_all(&body);
            let _ = socket.flush();
        })
    }

    fn start_with<R>(responder: R) -> anyhow::Result<Self>
    where
        R: Fn(&Request, &mut TcpStream) + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (sender, requests) = std::sync::mpsc::channel();
        let responder = std::sync::Arc::new(responder);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let Ok(peer) = stream.try_clone() else { break };
                let sender = sender.clone();
                let responder = responder.clone();
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(peer);
                    let mut line = String::new();
                    if reader.read_line(&mut line).is_err() {
                        return;
                    }
                    let mut headers = vec![];
                    loop {
                        let mut header = String::new();
                        match reader.read_line(&mut header) {
                            Ok(0) => break,
                            Ok(_) if header.trim().is_empty() => break,
                            Ok(_) => {}
                            Err(_) => break,
                        }
                        if let Some((name, value)) = header.split_once(':') {
                            headers.push((name.trim().to_string(), value.trim().to_string()));
                        }
                    }
                    let request = Request {
                        line: line.trim_end().to_string(),
                        headers,
                    };
                    if sender.send(request.clone()).is_err() {
                        return;
                    }
                    responder(&request, &mut stream);
                });
            }
        });
        Ok(Self { addr, requests })
    }

    /// The next request the origin was asked for.
    fn next_request(&self) -> Request {
        self.requests
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the origin was asked for something")
    }
}

/// Percent-encodes everything but the unreserved set, which is what a `d=`
/// value needs: the target URL's own `:`, `/`, `?` and `&` must survive the
/// query segment they are carried in.
fn encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

struct Fixture {
    handle: stream_server::ServerHandle,
    base: String,
    origin: Origin,
    cache_root: tempfile::TempDir,
    _config_dir: tempfile::TempDir,
}

fn fixture() -> anyhow::Result<Fixture> {
    fixture_with(Origin::start()?)
}

fn fixture_with(origin: Origin) -> anyhow::Result<Fixture> {
    let config_dir = tempfile::tempdir()?;
    let cache_root = tempfile::tempdir()?;
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.path().join("cache")),
        ..offline_config()
    })?;
    let base = format!("http://{}", handle.http_addr());
    Ok(Fixture {
        handle,
        base,
        origin,
        cache_root,
        _config_dir: config_dir,
    })
}

/// The Core path format, which is what stremio-core hands a player: the
/// origin in a `d=` query segment, the target's own path after it.
#[test]
fn the_core_path_format_relays_the_target() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let origin = format!("http://{}", fixture.origin.addr);
    let response = reqwest::blocking::Client::new()
        .get(format!(
            "{}/proxy/d={}/dir/movie.mp4?token=abc",
            fixture.base,
            encode(&origin)
        ))
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("video/mp4"),
        "the origin's own content type is relayed"
    );
    assert_eq!(response.bytes()?.len(), ORIGIN_LENGTH);

    // The path after the `d=` segment, and the proxy URL's own query, both
    // reach the origin: a signed URL loses neither.
    assert_eq!(
        fixture.origin.next_request().line,
        "GET /dir/movie.mp4?token=abc HTTP/1.1"
    );

    drop(fixture.handle);
    Ok(())
}

/// The query format, which is what [`rewrite_playlist`] writes into every
/// HLS playlist the route rewrites -- so a 404 here is every segment of a
/// proxied HLS stream 404ing.
#[test]
fn the_query_format_relays_the_target() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let target = format!("http://{}/dir/movie.mp4?token=abc", fixture.origin.addr);
    let response = reqwest::blocking::Client::new()
        .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .send()?;

    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "/proxy/?d= is a format this server writes itself"
    );
    assert_eq!(response.bytes()?.len(), ORIGIN_LENGTH);

    assert_eq!(
        fixture.origin.next_request().line,
        "GET /dir/movie.mp4?token=abc HTTP/1.1"
    );

    drop(fixture.handle);
    Ok(())
}

/// A player seeking backwards over a proxied stream re-fetches, and the
/// re-fetch has to be a range request the origin answers as one. The proxy
/// forwards `Range` and relays the `206`, its `Content-Range` and
/// `Accept-Ranges` back, so what mpv sees through the proxy is what it would
/// have seen from the origin.
#[test]
fn a_byte_range_is_forwarded_to_the_origin_and_its_206_relayed_back() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let origin = format!("http://{}", fixture.origin.addr);
    let response = reqwest::blocking::Client::new()
        .get(format!(
            "{}/proxy/d={}/movie.mp4",
            fixture.base,
            encode(&origin)
        ))
        .header(reqwest::header::RANGE, "bytes=1000-1099")
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let headers = response.headers().clone();
    assert_eq!(
        headers
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok()),
        Some(format!("bytes 1000-1099/{ORIGIN_LENGTH}").as_str())
    );
    assert_eq!(
        headers
            .get(reqwest::header::ACCEPT_RANGES)
            .and_then(|value| value.to_str().ok()),
        Some("bytes"),
        "mpv reads this to decide the stream can be seeked in"
    );
    let body = response.bytes()?;
    assert_eq!(body.len(), 100);
    assert_eq!(
        body[0],
        byte_at(1000),
        "and the bytes are the ones at that offset"
    );

    // The origin was asked for the range, not for the file: the proxy is not
    // fetching from the start and discarding the front of it.
    assert_eq!(
        fixture.origin.next_request().range(),
        Some("bytes=1000-1099")
    );

    drop(fixture.handle);
    Ok(())
}

/// The proxy relays; it does not cache. Nothing it fetched lands under the
/// cache root, and a second request for the same bytes goes back out to the
/// origin. Anything relying on a proxied stream being cheap to re-read has
/// to provide that cache itself.
#[test]
fn nothing_the_proxy_fetched_is_cached() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let origin = format!("http://{}", fixture.origin.addr);
    let url = format!("{}/proxy/d={}/movie.mp4", fixture.base, encode(&origin));
    let client = reqwest::blocking::Client::new();

    for _ in 0..2 {
        let response = client
            .get(&url)
            .header(reqwest::header::RANGE, "bytes=0-99")
            .send()?;
        assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.bytes()?.len(), 100);
    }

    // Twice asked for, twice fetched.
    for _ in 0..2 {
        assert_eq!(fixture.origin.next_request().range(), Some("bytes=0-99"));
    }

    // And a megabyte of proxied stream leaves nothing behind: the cache root
    // holds only what the torrent engine put there.
    let cached: Vec<_> = walk(&fixture.cache_root.path().join("cache"))
        .into_iter()
        .filter(|entry| entry.is_file())
        .collect();
    assert!(
        cached.is_empty(),
        "the proxy writes nothing to the cache the cleaner walks: {cached:?}"
    );

    drop(fixture.handle);
    Ok(())
}

/// A Core-format target whose own query carries a `d` -- a name the proxy
/// URL format uses too. Reading `d` out of the *request's* query took
/// `d="1"` for the whole target and answered `400 Invalid target URL`; the
/// format is decided by the path shape now, so the parameter goes to the
/// origin like any other.
#[test]
fn a_target_query_may_carry_its_own_d_parameter() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let origin = format!("http://{}", fixture.origin.addr);
    let response = reqwest::blocking::Client::new()
        .get(format!(
            "{}/proxy/d={}/film.mkv?d=1&t=2",
            fixture.base,
            encode(&origin)
        ))
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        fixture.origin.next_request().line,
        "GET /film.mkv?d=1&t=2 HTTP/1.1"
    );

    drop(fixture.handle);
    Ok(())
}

/// The same, with a `d` the old code would have *parsed*: a URL-shaped value
/// was fetched instead of the target the caller named, which is a worse
/// answer than the 400 -- an addon's own `d=` could redirect the whole
/// stream. Here it must reach the origin as a query parameter and nothing
/// else.
#[test]
fn a_url_shaped_d_in_the_target_query_is_not_fetched_instead_of_the_target() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let origin = format!("http://{}", fixture.origin.addr);
    let response = reqwest::blocking::Client::new()
        .get(format!(
            "{}/proxy/d={}/film.mkv?d={}",
            fixture.base,
            encode(&origin),
            encode("http://not-the-target.invalid/other.mkv")
        ))
        .send()?;

    // Answered from the origin the path named, not from the `d=` in its
    // query -- which does not resolve at all, so a fetch of it would have
    // been a 502 rather than these bytes.
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.bytes()?.len(), ORIGIN_LENGTH);
    assert_eq!(
        fixture.origin.next_request().line,
        "GET /film.mkv?d=http%3A%2F%2Fnot-the-target.invalid%2Fother.mkv HTTP/1.1"
    );

    drop(fixture.handle);
    Ok(())
}

/// What the caller encoded is what the origin is asked for. axum decodes
/// the wildcard capture, so reading the path from there turned `%2F` into a
/// separator, `%3F` into the start of a query and dropped everything from
/// `%23` on -- a signed link whose path segment carries a base64 signature
/// 403s, and a file named with a `#` 404s. The `%20` the earlier test used
/// round-tripped by accident: a space survives being decoded and re-encoded,
/// and the other three do not.
#[test]
fn percent_encoding_in_the_path_reaches_the_origin_as_the_caller_wrote_it() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let origin = format!("http://{}", fixture.origin.addr);
    let encoded_path = "a%2Fb/sig%3Dx%2Fy/film%20name%231%3Fnot-a-query.mkv";
    let response = reqwest::blocking::Client::new()
        .get(format!(
            "{}/proxy/d={}/{encoded_path}",
            fixture.base,
            encode(&origin)
        ))
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        fixture.origin.next_request().target(),
        format!("/{encoded_path}"),
        "every escape survives the trip, including the ones that change \
         meaning when they do not"
    );

    drop(fixture.handle);
    Ok(())
}

/// A compressed origin response. This client decodes nothing -- the server's
/// reqwest has no gzip/brotli/deflate feature -- so two things have to hold:
/// the origin is asked for `identity` rather than being handed the player's
/// `accept-encoding: gzip`, and if it compresses anyway the bytes come back
/// with the `content-encoding` that names them. Dropping that header, which
/// is what this route used to do, hands the player gzip labelled as
/// identity. The target is a playlist as well, so it also pins that we do
/// not try to rewrite lines that are not text yet.
#[test]
fn a_compressed_origin_response_keeps_the_header_that_names_its_coding() -> anyhow::Result<()> {
    use std::io::Write as _;

    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(ORIGIN_PLAYLIST.as_bytes())?;
    let gzipped = encoder.finish()?;
    let served = gzipped.clone();

    let origin = Origin::start_with(move |_request: &Request, socket: &mut TcpStream| {
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apple.mpegurl\r\n\
             Content-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            served.len()
        );
        let _ = socket.write_all(head.as_bytes());
        let _ = socket.write_all(&served);
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(origin)?;
    let target = format!("http://{}/live/master.m3u8", fixture.origin.addr);
    let response = reqwest::blocking::Client::new()
        .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .header(reqwest::header::ACCEPT_ENCODING, "gzip, deflate, br")
        .send()?;

    assert_eq!(
        response
            .headers()
            .get(reqwest::header::CONTENT_ENCODING)
            .and_then(|value| value.to_str().ok()),
        Some("gzip"),
        "the coding of the bytes we are handing on is named"
    );
    assert_eq!(
        response.bytes()?.as_ref(),
        gzipped.as_slice(),
        "and the bytes are the origin's own, neither decoded nor rewritten"
    );

    assert_eq!(
        fixture.origin.next_request().header("accept-encoding"),
        Some("identity"),
        "the player's gzip is not forwarded by a proxy that cannot decode it"
    );

    drop(fixture.handle);
    Ok(())
}

/// The playlist an HLS origin serves, and what the proxy must hand the
/// player instead: every line pointing back through the proxy, which makes
/// the body *longer* than the one the origin sent. That length difference is
/// the whole of defect 1 -- a relayed `Content-Length` describing 82 bytes in
/// front of 180 is a response hyper refuses to write.
const ORIGIN_PLAYLIST: &str =
    "#EXTM3U\n#EXT-X-VERSION:3\n#EXTINF:10,\nseg-0.ts\n#EXTINF:10,\nseg-1.ts\n";

fn expected_playlist(origin: SocketAddr) -> String {
    expected_playlist_at(&format!("http://{origin}/live"))
}

/// [`ORIGIN_PLAYLIST`] rewritten with every segment resolved against
/// `directory` -- the directory of the URL the playlist *came from*, which
/// a redirect can move.
fn expected_playlist_at(directory: &str) -> String {
    let mut expected = String::new();
    for line in ORIGIN_PLAYLIST.lines() {
        if line.starts_with('#') {
            expected.push_str(line);
        } else {
            expected.push_str(&format!(
                "/proxy/?d={}",
                encode(&format!("{directory}/{line}"))
            ));
        }
        expected.push('\n');
    }
    expected
}

/// An origin that serves [`ORIGIN_PLAYLIST`] with the framing `framing`
/// spells -- the three ways an origin can delimit a body, all of which the
/// proxy has to survive, because it replaces that body with a longer one.
fn playlist_origin(framing: Framing) -> anyhow::Result<Origin> {
    Origin::start_with(move |_request: &Request, socket: &mut TcpStream| {
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apple.mpegurl\r\n{}\r\n",
            match framing {
                Framing::ContentLength => format!("Content-Length: {}\r\n", ORIGIN_PLAYLIST.len()),
                Framing::Chunked => "Transfer-Encoding: chunked\r\n".to_string(),
                Framing::CloseDelimited => "Connection: close\r\n".to_string(),
            }
        );
        let _ = socket.write_all(head.as_bytes());
        match framing {
            Framing::Chunked => {
                let _ = socket.write_all(
                    format!(
                        "{:x}\r\n{ORIGIN_PLAYLIST}\r\n0\r\n\r\n",
                        ORIGIN_PLAYLIST.len()
                    )
                    .as_bytes(),
                );
            }
            _ => {
                let _ = socket.write_all(ORIGIN_PLAYLIST.as_bytes());
            }
        }
        let _ = socket.flush();
        let _ = socket.shutdown(std::net::Shutdown::Write);
    })
}

#[derive(Clone, Copy)]
enum Framing {
    ContentLength,
    Chunked,
    CloseDelimited,
}

/// Fetches the playlist through the proxy and asserts the player got the
/// whole rewritten thing, framed by its own length rather than the origin's.
fn assert_playlist_is_reframed(framing: Framing) -> anyhow::Result<()> {
    let fixture = fixture_with(playlist_origin(framing)?)?;
    let target = format!("http://{}/live/master.m3u8", fixture.origin.addr);
    let response = reqwest::blocking::Client::new()
        .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let declared = response
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string());
    let body = response.text()?;

    assert_eq!(
        body,
        expected_playlist(fixture.origin.addr),
        "the whole playlist arrives, every line rewritten"
    );
    assert_eq!(
        declared.as_deref(),
        Some(body.len().to_string().as_str()),
        "and the length we declare is the length of what we sent, not what we fetched"
    );
    assert_ne!(
        body.len(),
        ORIGIN_PLAYLIST.len(),
        "the rewrite really did change the length -- otherwise this test proves nothing"
    );

    drop(fixture.handle);
    Ok(())
}

/// The framing the origin sent is the one that used to panic the connection
/// task: `payload claims content-length of 180, custom content-length header
/// claims 82`.
#[test]
fn a_playlist_from_a_content_length_origin_is_framed_by_its_rewritten_length() -> anyhow::Result<()>
{
    assert_playlist_is_reframed(Framing::ContentLength)
}

/// With the origin's `Transfer-Encoding: chunked` relayed, the response
/// closed having written nothing at all.
#[test]
fn a_playlist_from_a_chunked_origin_is_framed_by_its_rewritten_length() -> anyhow::Result<()> {
    assert_playlist_is_reframed(Framing::Chunked)
}

/// The one framing that worked before, by accident -- a close-delimited
/// origin gave the proxy nothing to relay. It must keep working.
#[test]
fn a_playlist_from_a_close_delimited_origin_is_framed_by_its_rewritten_length() -> anyhow::Result<()>
{
    assert_playlist_is_reframed(Framing::CloseDelimited)
}

/// An authenticated HLS stream, end to end: the playlist and every segment
/// need the same `Authorization` the addon put in `h=`, and only the
/// playlist's own URL was carrying it.
///
/// `rewrite_playlist` used to write `d=` and `p=` and nothing else, so the
/// segments came back through the proxy stripped of the header that made
/// them fetchable. Measured before the fix: playlist `200`, every segment
/// `403`, the origin logging `auth=[]`.
#[test]
fn an_authenticated_playlist_carries_its_headers_into_every_segment() -> anyhow::Result<()> {
    const SECRET: &str = "Bearer s3cret";

    let origin = Origin::start_with(|request: &Request, socket: &mut TcpStream| {
        if request.header("authorization") != Some(SECRET) {
            let _ = socket.write_all(
                b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
            return;
        }
        let (content_type, body) = if request.target().ends_with(".m3u8") {
            ("application/vnd.apple.mpegurl", ORIGIN_PLAYLIST.to_string())
        } else {
            ("video/mp2t", "segment bytes".to_string())
        };
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        );
        let _ = socket.write_all(body.as_bytes());
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(origin)?;
    let client = reqwest::blocking::Client::new();
    // The Core path format, which is what stremio-core writes for an addon
    // stream that needs a header.
    let playlist = client
        .get(format!(
            "{}/proxy/d={}&h={}/live/master.m3u8",
            fixture.base,
            encode(&format!("http://{}", fixture.origin.addr)),
            encode(&format!("Authorization:{SECRET}"))
        ))
        .send()?;
    assert_eq!(playlist.status(), reqwest::StatusCode::OK);
    let body = playlist.text()?;
    assert_eq!(
        fixture.origin.next_request().header("authorization"),
        Some(SECRET),
        "the playlist itself was fetched with the header"
    );

    // Every rewritten line carries it too, so the player can fetch what the
    // playlist names.
    let segment = body
        .lines()
        .find(|line| !line.starts_with('#') && !line.is_empty())
        .expect("the rewritten playlist names a segment");
    assert!(
        segment.contains(&format!(
            "&h={}",
            encode(&format!("Authorization:{SECRET}"))
        )),
        "the segment URL carries the header the playlist arrived with: {segment}"
    );

    let fetched = client.get(format!("{}{segment}", fixture.base)).send()?;
    assert_eq!(
        fetched.status(),
        reqwest::StatusCode::OK,
        "and fetching it through the proxy is allowed at the origin"
    );
    assert_eq!(fetched.text()?, "segment bytes");
    assert_eq!(
        fixture.origin.next_request().header("authorization"),
        Some(SECRET)
    );

    drop(fixture.handle);
    Ok(())
}

/// A TLS origin with a certificate nothing will verify: self-signed, and a
/// CA certificate used as an end entity at that, so rustls rejects it
/// whatever the hostname. The PEMs beside this file are throwaways for
/// exactly this listener, which binds loopback and serves one string.
struct TlsOrigin {
    addr: SocketAddr,
    _certificates: tempfile::TempDir,
}

impl TlsOrigin {
    fn start() -> anyhow::Result<Self> {
        let certificates = tempfile::tempdir()?;
        let cert = certificates.path().join("cert.pem");
        let key = certificates.path().join("key.pem");
        std::fs::write(&cert, include_str!("selfsigned-cert.pem"))?;
        std::fs::write(&key, include_str!("selfsigned-key.pem"))?;

        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        // axum-server registers the listener with tokio, which refuses a
        // blocking socket.
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime for the TLS origin");
            runtime.block_on(async move {
                let config = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
                    .await
                    .expect("the fixture certificate is readable");
                let app: axum::Router =
                    axum::Router::new().fallback(axum::routing::get(|| async { "secret bytes" }));
                let _ = axum_server::from_tcp_rustls(listener, config)
                    .expect("the listener is ours")
                    .serve(app.into_make_service())
                    .await;
            });
        });
        Ok(Self {
            addr,
            _certificates: certificates,
        })
    }
}

/// The downgrade, working as advertised: a host whose certificate will not
/// verify is fetched once with verification, once without, and written down
/// by name so the next request pays only one handshake.
///
/// It is addressed as `localhost` rather than `127.0.0.1` so the host this
/// records cannot be confused with the one the redirect test below asserts
/// was *not* recorded -- [`stream_server::unverified_hosts`] is process-wide
/// and these tests share a process.
#[test]
fn a_host_whose_certificate_fails_is_fetched_unverified_and_named() -> anyhow::Result<()> {
    let tls = TlsOrigin::start()?;
    let fixture = fixture()?;
    let target = format!("https://localhost:{}/film.mkv", tls.addr.port());
    let response = reqwest::blocking::Client::new()
        .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .send()?;

    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "the stream plays, which is why the downgrade exists at all"
    );
    assert_eq!(response.text()?, "secret bytes");
    assert!(
        stream_server::unverified_hosts().contains(&"localhost".to_string()),
        "and the host is written down by name: {:?}",
        stream_server::unverified_hosts()
    );

    drop(fixture.handle);
    Ok(())
}

/// The same failure one redirect away, which is where the host recorded
/// used to be the wrong one entirely: the plain-HTTP host that redirected
/// us got marked unverified, and the https host whose handshake actually
/// failed did not.
///
/// reqwest attributes the failure to the URL the request started at, so
/// from here the failing host has no name -- and an unnameable host is not
/// one to write down. The fetch fails instead.
#[test]
fn a_certificate_failure_behind_a_redirect_downgrades_nobody() -> anyhow::Result<()> {
    let tls = TlsOrigin::start()?;
    let tls_addr = tls.addr;
    let redirector = Origin::start_with(move |_request: &Request, socket: &mut TcpStream| {
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 302 Found\r\nLocation: https://127.0.0.1:{}/film.mkv\r\n\
                 Content-Length: 0\r\nConnection: close\r\n\r\n",
                tls_addr.port()
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(redirector)?;
    let target = format!("http://{}/film.mkv", fixture.origin.addr);
    let response = reqwest::blocking::Client::new()
        .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .send()?;

    assert_eq!(
        response.status(),
        reqwest::StatusCode::BAD_GATEWAY,
        "the fetch fails rather than silently downgrading something"
    );
    assert!(
        !stream_server::unverified_hosts().contains(&"127.0.0.1".to_string()),
        "and no plain-HTTP host is marked unverified: {:?}",
        stream_server::unverified_hosts()
    );

    drop(fixture.handle);
    Ok(())
}

/// The measured reproduction of reading the error's prose instead of its
/// type: no TLS anywhere, a refused connection, and a filename with the
/// word "certificate" in it. It marked the host unverified for the life of
/// the process.
#[test]
fn a_filename_cannot_turn_certificate_verification_off() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let response = reqwest::blocking::Client::new()
        .get(format!(
            "{}/proxy/?d={}",
            fixture.base,
            encode("http://127.0.0.1:1/certificate-of-authenticity.mkv")
        ))
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::BAD_GATEWAY);
    assert!(
        !stream_server::unverified_hosts().contains(&"127.0.0.1".to_string()),
        "a connection refused is not a certificate failure: {:?}",
        stream_server::unverified_hosts()
    );

    drop(fixture.handle);
    Ok(())
}

/// `h=` and `r=` are overrides, and each collides with a header that is
/// already there: the player's `User-Agent` on the way out, the origin's
/// `Content-Type` on the way back. Both used to be *added*, so the origin
/// saw two user agents and the player two content types -- and a client
/// reading the first of two read the origin's, which is the value
/// stremio-core sends `r=` to correct.
#[test]
fn a_header_override_replaces_the_header_it_names() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let response = reqwest::blocking::Client::new()
        .get(format!(
            "{}/proxy/d={}&h={}&r={}/movie.mp4",
            fixture.base,
            encode(&format!("http://{}", fixture.origin.addr)),
            encode("User-Agent:addon/1"),
            encode("Content-Type:video/x-corrected")
        ))
        .header(reqwest::header::USER_AGENT, "mpv/0.41")
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let content_types: Vec<&str> = response
        .headers()
        .get_all(reqwest::header::CONTENT_TYPE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .collect();
    assert_eq!(
        content_types,
        vec!["video/x-corrected"],
        "the origin said video/mp4, and r= is what replaces it"
    );

    let request = fixture.origin.next_request();
    let user_agents: Vec<&str> = request
        .headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("user-agent"))
        .map(|(_, value)| value.as_str())
        .collect();
    assert_eq!(
        user_agents,
        vec!["addon/1"],
        "the addon's user agent, not the player's as well"
    );

    drop(fixture.handle);
    Ok(())
}

/// A CDN that redirects to an edge, which is what an ordinary HLS
/// deployment looks like. The playlist's relative lines are relative to
/// where it *came from*, so they have to be resolved against the edge and
/// its directory -- rewriting against the URL we asked for pointed every
/// segment back at the CDN, which does not serve them.
#[test]
fn a_playlist_reached_through_a_redirect_is_rewritten_against_the_edge() -> anyhow::Result<()> {
    let edge = Origin::start_with(|_request: &Request, socket: &mut TcpStream| {
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apple.mpegurl\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{ORIGIN_PLAYLIST}",
                ORIGIN_PLAYLIST.len()
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })?;
    let edge_addr = edge.addr;
    let cdn = Origin::start_with(move |_request: &Request, socket: &mut TcpStream| {
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 302 Found\r\nLocation: http://{edge_addr}/edge/v2/master.m3u8\r\n\
                 Content-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(cdn)?;
    let target = format!("http://{}/cdn/master.m3u8", fixture.origin.addr);
    let response = reqwest::blocking::Client::new()
        .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.text()?,
        expected_playlist_at(&format!("http://{edge_addr}/edge/v2")),
        "every segment resolves against the edge that served the playlist"
    );

    assert_eq!(
        fixture.origin.next_request().line,
        "GET /cdn/master.m3u8 HTTP/1.1"
    );
    assert_eq!(
        edge.next_request().line,
        "GET /edge/v2/master.m3u8 HTTP/1.1",
        "and the redirect was followed, which is what makes the base wrong"
    );

    drop(fixture.handle);
    Ok(())
}

/// A `.m3u8` URL that answers with an error, and a `HEAD` for one that does
/// not. Neither has a playlist in it, and rewriting them said otherwise.
///
/// The 404's error page came back as a playlist of proxy URLs built out of
/// the words in it -- a fabricated segment list an HLS player would
/// dutifully try to fetch. The `HEAD` was worse in a quieter way: the
/// rewrite measured the empty body it had been handed and declared
/// `Content-Length: 0`, so a player sizing the resource before playing it
/// was told there was nothing there.
#[test]
fn only_a_2xx_get_at_a_playlist_url_is_rewritten_as_a_playlist() -> anyhow::Result<()> {
    const MISSING: &str = "no such stream\n";

    let origin = Origin::start_with(|request: &Request, socket: &mut TcpStream| {
        let missing = request.target().contains("missing");
        let (head, content_type, body) = if missing {
            ("HTTP/1.1 404 Not Found", "text/plain", MISSING)
        } else {
            (
                "HTTP/1.1 200 OK",
                "application/vnd.apple.mpegurl",
                ORIGIN_PLAYLIST,
            )
        };
        let _ = socket.write_all(
            format!(
                "{head}\r\nContent-Type: {content_type}\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        );
        // A HEAD gets the headers and no body, as any origin would answer.
        if !request.line.starts_with("HEAD") {
            let _ = socket.write_all(body.as_bytes());
        }
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(origin)?;
    let client = reqwest::blocking::Client::new();
    let proxied = |path: &str| {
        format!(
            "{}/proxy/?d={}",
            fixture.base,
            encode(&format!("http://{}/live/{path}", fixture.origin.addr))
        )
    };

    let missing = client.get(proxied("missing.m3u8")).send()?;
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        missing.text()?,
        MISSING,
        "the origin's own error body, not a playlist made out of it"
    );

    let head = client.head(proxied("master.m3u8")).send()?;
    assert_eq!(head.status(), reqwest::StatusCode::OK);
    assert_eq!(
        head.headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(ORIGIN_PLAYLIST.len().to_string().as_str()),
        "how big the resource is, which is the only thing a HEAD is for"
    );
    assert!(head.bytes()?.is_empty());

    drop(fixture.handle);
    Ok(())
}

/// An origin that never stops sending: a long film, a live stream, the
/// swarm that `network-timeout` is generous for. Closing has to be visible
/// against *this*, not against a body that was about to end anyway.
fn endless_origin() -> anyhow::Result<Origin> {
    Origin::start_with(|_request: &Request, socket: &mut TcpStream| {
        let head = "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\n\
                    Content-Length: 1099511627776\r\n\r\n";
        if socket.write_all(head.as_bytes()).is_err() {
            return;
        }
        // Until the far end goes away. The pause is not sequencing
        // anything -- every assertion below waits on a read -- it just
        // keeps this thread from spinning.
        while socket.write_all(&[0u8; 4096]).is_ok() {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    })
}

/// The whole point of the token: one player's stream ends when the client
/// asks for it, at once, and the other player never notices.
///
/// Both URL shapes carry the token, so one player is given each: the query
/// format's `p=` sits beside `d=`, the Core format's inside the `d=`/`h=`
/// path segment -- where, unlike the request's own query, it cannot leak to
/// the origin.
#[test]
fn closing_one_player_token_ends_that_stream_and_leaves_the_other_playing() -> anyhow::Result<()> {
    use std::io::Read as _;

    let fixture = fixture_with(endless_origin()?)?;
    let origin = format!("http://{}", fixture.origin.addr);
    let client = reqwest::blocking::Client::new();

    let mut watching = client
        .get(format!(
            "{}/proxy/?d={}&p=player-one",
            fixture.base,
            encode(&format!("{origin}/film.mkv"))
        ))
        .send()?;
    let mut other = client
        .get(format!(
            "{}/proxy/d={}&p=player-two/other.mkv",
            fixture.base,
            encode(&origin)
        ))
        .send()?;

    // Both are reading before anything is closed.
    let mut byte = [0u8; 1];
    watching.read_exact(&mut byte)?;
    other.read_exact(&mut byte)?;
    assert_eq!(
        fixture.handle.proxy_streams_live(),
        2,
        "two players attached, which is the accounting this makes possible"
    );
    assert_eq!(
        fixture.origin.next_request().line,
        "GET /film.mkv HTTP/1.1",
        "the token is ours and never travels to the origin"
    );
    assert_eq!(
        fixture.origin.next_request().line,
        "GET /other.mkv HTTP/1.1"
    );

    // Close the first player's stream over the control API, with the bearer
    // token that API requires.
    let control = reqwest::blocking::Client::new()
        .post(format!("{}/proxy-streams/player-one/close", fixture.base))
        .bearer_auth(fixture.handle.auth_token().expect("a generated token"))
        .send()?;
    assert_eq!(control.status(), reqwest::StatusCode::OK);
    assert_eq!(
        control.json::<serde_json::Value>()?,
        serde_json::json!({ "closed": 1 })
    );

    // The closed player's read fails now rather than in a minute's time...
    std::io::copy(&mut watching, &mut std::io::sink())
        .expect_err("the closed stream must break, not end tidily");
    // ...and the other player is still being served.
    other.read_exact(&mut byte)?;
    assert_eq!(fixture.handle.proxy_streams_live(), 1);

    // The library method is the same operation, so an embedder needs no
    // HTTP client for it.
    assert_eq!(fixture.handle.close_proxy_streams("player-two"), 1);
    std::io::copy(&mut other, &mut std::io::sink())
        .expect_err("the second stream is closed the same way");

    // Closing again closes nothing, and says so.
    assert_eq!(fixture.handle.close_proxy_streams("player-two"), 0);
    assert_eq!(fixture.handle.proxy_streams_live(), 0);

    drop(fixture.handle);
    Ok(())
}

/// The half that makes closing stick: the token is retired, so the
/// reconnect ffmpeg makes through the URL it already has is refused rather
/// than served.
///
/// Measured against the real thing before it was built: three closes on one
/// live libmpv reader answered `{"closed":1}` three times, and each answer
/// was followed by a fresh origin fetch at the offset the close had
/// interrupted. A close that only breaks the read is a stutter.
#[test]
fn a_closed_token_is_refused_a_new_stream_and_the_origin_is_never_asked() -> anyhow::Result<()> {
    use std::io::Read as _;

    let fixture = fixture_with(endless_origin()?)?;
    let origin = format!("http://{}", fixture.origin.addr);
    let client = reqwest::blocking::Client::new();
    let film = format!(
        "{}/proxy/?d={}&p=player-one",
        fixture.base,
        encode(&format!("{origin}/film.mkv"))
    );

    let mut watching = client.get(&film).send()?;
    let mut byte = [0u8; 1];
    watching.read_exact(&mut byte)?;
    assert_eq!(fixture.origin.next_request().line, "GET /film.mkv HTTP/1.1");

    assert_eq!(fixture.handle.close_proxy_streams("player-one"), 1);
    std::io::copy(&mut watching, &mut std::io::sink())
        .expect_err("the closed stream breaks, as it always did");

    // The reconnect: the same URL, the same token, and this time there is
    // nothing behind it.
    let refused = client.get(&film).send()?;
    assert_eq!(
        refused.status(),
        reqwest::StatusCode::GONE,
        "gone, not missing -- the stream was here and was ended on purpose"
    );

    // The Core path format carries the token in its own segment, and is
    // refused on the same grounds.
    let refused = client
        .get(format!(
            "{}/proxy/d={}&p=player-one/film.mkv",
            fixture.base,
            encode(&origin)
        ))
        .send()?;
    assert_eq!(refused.status(), reqwest::StatusCode::GONE);

    // Neither refusal reached the origin: the next thing it was asked for
    // is the request made after them, under a token nobody closed -- which
    // still works, because retiring one player's name retires only that
    // player's.
    let mut other = client
        .get(format!(
            "{}/proxy/?d={}&p=player-two",
            fixture.base,
            encode(&format!("{origin}/second.mkv"))
        ))
        .send()?;
    other.read_exact(&mut byte)?;
    assert_eq!(
        fixture.origin.next_request().line,
        "GET /second.mkv HTTP/1.1"
    );
    assert_eq!(fixture.handle.proxy_streams_live(), 1);

    drop(fixture.handle);
    Ok(())
}

/// A live-HLS player refreshing its playlist against an origin that has
/// stopped answering: the read is wedged in the playlist branch, which is
/// precisely the wedge this feature exists for -- and precisely the one it
/// could not reach, because that branch returned before the read was ever
/// registered.
#[test]
fn a_playlist_read_is_registered_and_can_be_closed() -> anyhow::Result<()> {
    // Headers, then nothing, ever. The socket stays open: no FIN, no reset,
    // nothing for a timeout to notice quickly.
    let origin = Origin::start_with(|_request: &Request, socket: &mut TcpStream| {
        let _ = socket.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apple.mpegurl\r\n\
              Content-Length: 4096\r\n\r\n",
        );
        let _ = socket.flush();
        // Until the far end goes away.
        let mut byte = [0u8; 1];
        use std::io::Read as _;
        let _ = socket.read(&mut byte);
    })?;

    let fixture = fixture_with(origin)?;
    let target = format!("http://{}/live/master.m3u8", fixture.origin.addr);
    let url = format!("{}/proxy/?d={}&p=player-hls", fixture.base, encode(&target));
    let reader = std::thread::spawn(move || {
        reqwest::blocking::Client::new()
            .get(url)
            .send()
            .map(|response| response.status())
    });

    // The read is registered while it is stuck, which is what makes it
    // addressable. Bounded so a regression fails instead of hanging.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while fixture.handle.proxy_streams_live() == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the playlist read was never registered"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    assert_eq!(fixture.handle.close_proxy_streams("player-hls"), 1);
    assert_eq!(
        reader.join().expect("the reader thread")?,
        reqwest::StatusCode::BAD_GATEWAY,
        "the wedged playlist read ends now, rather than at a timeout"
    );

    drop(fixture.handle);
    Ok(())
}

/// The control route is a control route: no token, no close.
#[test]
fn closing_a_stream_needs_the_control_token() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let response = reqwest::blocking::Client::new()
        .post(format!("{}/proxy-streams/player-one/close", fixture.base))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);

    drop(fixture.handle);
    Ok(())
}

fn walk(directory: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = vec![];
    let Ok(entries) = std::fs::read_dir(directory) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(walk(&path));
        }
        found.push(path);
    }
    found
}
