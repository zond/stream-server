//! What `/proxy` does with a caller-supplied remote URL.
//!
//! The route is a *relay*: it opens the target with reqwest and streams the
//! bytes back. Nothing it fetches is written to the cache the cleaner walks,
//! and nothing it fetched once is served from disk the second time. That is
//! worth pinning here because embedders lean on it in both directions -- a
//! client sending every remote stream through this server gets the header
//! rewriting and the single egress point, and does *not* get caching.
//!
//! Both URL shapes are exercised. The Core path format
//! (`/proxy/d=<origin>&h=.../<path>`) is the one stremio-core builds and
//! the one this server writes itself: every line of a rewritten HLS
//! playlist comes back in it, because only a path format leaves a nested
//! playlist a directory of its own to resolve *its* lines against. The
//! query format (`/proxy/?d=<url>`) is read and not written -- callers
//! still send it, and most tests below use it because a whole target in
//! one parameter is the shorter thing to write.

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

/// One byte of the body at `offset`: a cheap pattern, so a range response
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

/// The query format. Not what the playlist rewrite writes any more -- that
/// has been the path format since a rewritten line needed a directory of
/// its own -- but callers still send it, so the route still reads it, and
/// the whole target in one parameter has to say everything a target named
/// by path can.
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
/// a redirect can move -- and spelled in the path format the rewrite
/// writes: the proxy's mount, the origin in `d=`, then the segment's own
/// path on it. The path format is what leaves a rewritten line a directory
/// of its own, which is what a nested playlist's relative lines need.
fn expected_playlist_at(directory: &str) -> String {
    let directory = url::Url::parse(&format!("{directory}/")).expect("a directory URL");
    let origin = encode(&directory.origin().ascii_serialization());
    let mut expected = String::new();
    for line in ORIGIN_PLAYLIST.lines() {
        if line.starts_with('#') {
            expected.push_str(line);
        } else {
            let segment = directory.join(line).expect("a segment URL");
            expected.push_str(&format!("/proxy/d={origin}{}", segment.path()));
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
/// whole rewritten thing, framed by hyper from the bytes we actually wrote
/// rather than by anything either end declared.
fn assert_playlist_arrives_whole(framing: Framing) -> anyhow::Result<()> {
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
        declared, None,
        "and nothing declares a length for it: the body is written as it is read, and \
         hyper frames what it writes"
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
fn a_playlist_from_a_content_length_origin_arrives_whole() -> anyhow::Result<()> {
    assert_playlist_arrives_whole(Framing::ContentLength)
}

/// With the origin's `Transfer-Encoding: chunked` relayed, the response
/// closed having written nothing at all.
#[test]
fn a_playlist_from_a_chunked_origin_arrives_whole() -> anyhow::Result<()> {
    assert_playlist_arrives_whole(Framing::Chunked)
}

/// The one framing that worked before, by accident -- a close-delimited
/// origin gave the proxy nothing to relay. It must keep working.
#[test]
fn a_playlist_from_a_close_delimited_origin_arrives_whole() -> anyhow::Result<()> {
    assert_playlist_arrives_whole(Framing::CloseDelimited)
}

/// An origin that serves [`ORIGIN_PLAYLIST`] under `content_type`, whatever
/// path it is asked for -- so a test can name a URL with no extension to
/// fall back on and see whether the content type alone was enough.
fn playlist_origin_typed(content_type: &'static str) -> anyhow::Result<Origin> {
    Origin::start_with(move |_request: &Request, socket: &mut TcpStream| {
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{ORIGIN_PLAYLIST}",
                ORIGIN_PLAYLIST.len()
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })
}

/// The content type that says "playlist" is matched with its case folded
/// away. Apple writes `application/x-mpegURL`; so does this repo's README,
/// and so does the `r=` stremio-core sends for an HLS stream. A
/// case-sensitive `contains("mpegurl")` saw none of them, and the URL here
/// has no `.m3u8` to fall back on -- which is exactly the shape a playlist
/// behind a redirect arrives in.
#[test]
fn a_playlist_is_recognised_however_its_content_type_is_capitalised() -> anyhow::Result<()> {
    for content_type in ["application/x-mpegURL", "application/X-MPEGURL"] {
        let fixture = fixture_with(playlist_origin_typed(content_type)?)?;
        let target = format!("http://{}/live/stream", fixture.origin.addr);
        let response = reqwest::blocking::Client::new()
            .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
            .send()?;

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body = response.text()?;
        assert!(
            body.contains("/proxy/"),
            "{content_type} names a playlist, so its lines come back through the proxy: {body}"
        );
        assert!(
            !body.contains("\nseg-0.ts"),
            "and none of them is left pointing straight at the origin: {body}"
        );

        drop(fixture.handle);
    }
    Ok(())
}

/// The redirect that made the fetched path the wrong thing to ask. A CDN
/// sends a `.m3u8` request on to an edge that serves the same playlist at
/// an extension-less URL under a content type that says nothing -- an
/// ordinary signed-URL deployment -- and the only evidence left that this
/// is a playlist is the URL the caller named. Testing the post-redirect
/// path alone relayed it whole, so a player got a playlist of origin URLs
/// and every segment bypassed the proxy, `h=` and all.
#[test]
fn a_playlist_is_recognised_by_the_url_the_caller_named() -> anyhow::Result<()> {
    let edge = Origin::start_with(|_request: &Request, socket: &mut TcpStream| {
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n\
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
                "HTTP/1.1 302 Found\r\nLocation: http://{edge_addr}/edge/9f3a1c\r\n\
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
    let body = response.text()?;
    assert!(
        body.contains("/proxy/"),
        "the URL the caller named ends .m3u8, and nothing else here says so: {body}"
    );

    drop(fixture.handle);
    Ok(())
}

/// How long the film is. Not a round number and not the playlist's length:
/// the assertion is that the byte count survives, and a length the rewriter
/// could have arrived at by accident would prove nothing.
const FILM_LENGTH: usize = 39_998;

/// An origin serving [`FILM_LENGTH`] bytes of `video/mp4`, ranges and all,
/// under whatever path it is asked for.
fn film_origin() -> anyhow::Result<Origin> {
    Origin::start_with(|request: &Request, socket: &mut TcpStream| {
        let range = request.range().and_then(|value| {
            let (first, last) = value.trim_start_matches("bytes=").split_once('-')?;
            let first: usize = first.parse().ok()?;
            let last = if last.is_empty() {
                FILM_LENGTH - 1
            } else {
                last.parse().ok()?
            };
            Some((first, last))
        });
        let (first, last) = range.unwrap_or((0, FILM_LENGTH - 1));
        let body: Vec<u8> = (first..=last).map(byte_at).collect();
        let head = match range {
            Some(_) => format!(
                "HTTP/1.1 206 Partial Content\r\n\
                 Content-Range: bytes {first}-{last}/{FILM_LENGTH}\r\n"
            ),
            None => "HTTP/1.1 200 OK\r\n".to_string(),
        };
        let _ = socket.write_all(
            format!(
                "{head}Content-Type: video/mp4\r\nAccept-Ranges: bytes\r\n\
                 ETag: \"the-film\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        );
        let _ = socket.write_all(&body);
        let _ = socket.flush();
    })
}

/// A `.m3u8` URL that serves a film, which is the shape the URL test above
/// cannot tell from a real playlist -- and the origin gets the last word.
///
/// Measured before the content type had a veto: the caller named
/// `/s/index.m3u8`, the origin sent it on to `/movie.mp4` and served
/// [`FILM_LENGTH`] bytes of `video/mp4`, and what came back had no
/// `Content-Length`, `Accept-Ranges: none`, no `Content-Range`, no `ETag`
/// -- and the video's bytes through the line rewriter. ffmpeg failed on it.
///
/// The reference is wrong here in exactly the same way (`path.extname` of
/// the caller-named, pre-redirect path, with nothing to overrule it) and
/// this is a deliberate divergence from it.
#[test]
fn a_playlist_url_that_serves_a_film_is_relayed_as_the_film_it_is() -> anyhow::Result<()> {
    let film = film_origin()?;
    let film_addr = film.addr;
    let front = Origin::start_with(move |_request: &Request, socket: &mut TcpStream| {
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 302 Found\r\nLocation: http://{film_addr}/movie.mp4\r\n\
                 Content-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(front)?;
    let client = reqwest::blocking::Client::new();
    let url = format!(
        "{}/proxy/?d={}",
        fixture.base,
        encode(&format!("http://{}/s/index.m3u8", fixture.origin.addr))
    );

    let whole = client.get(&url).send()?;
    assert_eq!(whole.status(), reqwest::StatusCode::OK);
    let headers = whole.headers().clone();
    assert_eq!(
        headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(FILM_LENGTH.to_string().as_str()),
        "the film is framed by its own length, not written as a rewritten body"
    );
    assert_eq!(
        headers
            .get(reqwest::header::ACCEPT_RANGES)
            .and_then(|value| value.to_str().ok()),
        Some("bytes"),
        "and it can still be seeked in"
    );
    assert!(
        headers.get(reqwest::header::ETAG).is_some(),
        "the entity headers that go with a relayed body are relayed"
    );
    let body = whole.bytes()?;
    assert_eq!(body.len(), FILM_LENGTH);
    assert_eq!(body[0], byte_at(0), "and the bytes are the film's own");
    assert!(
        !body.windows(7).any(|window| window == b"/proxy/"),
        "nothing here went through the line rewriter"
    );

    // The other half of the same mistake: a `206` that happens to cover the
    // whole entity was rewritten and answered as a `200`, so a player
    // opening the stream with `Range: bytes=0-` to find out whether the
    // origin is seekable was told it is not.
    let ranged = client
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    assert_eq!(
        ranged.status(),
        reqwest::StatusCode::PARTIAL_CONTENT,
        "a 206 carrying a film stays a 206"
    );
    assert_eq!(
        ranged
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok()),
        Some(format!("bytes 0-{}/{FILM_LENGTH}", FILM_LENGTH - 1).as_str())
    );
    assert_eq!(ranged.bytes()?.len(), FILM_LENGTH);

    drop(fixture.handle);
    Ok(())
}

/// The veto the test above relies on, and the escape hatch that has to
/// survive it: an origin serving a real playlist under `video/mp2t`.
///
/// Unasked, the content type wins and the body is relayed -- which is the
/// veto doing its job, since from here a mislabelled playlist and a film at
/// a `.m3u8` URL are the same response. `r=Content-Type:application/
/// x-mpegURL` is how a caller says otherwise, and it is what stremio-core
/// sends for an HLS stream, so the type the *player* will be given is the
/// one the classification asks.
#[test]
fn a_content_type_override_still_forces_the_playlist_path() -> anyhow::Result<()> {
    let fixture = fixture_with(playlist_origin_typed("video/mp2t")?)?;
    let client = reqwest::blocking::Client::new();
    let target = format!("http://{}/live/master.m3u8", fixture.origin.addr);
    let url = format!("{}/proxy/?d={}", fixture.base, encode(&target));

    let unasked = client.get(&url).send()?;
    assert_eq!(unasked.status(), reqwest::StatusCode::OK);
    assert_eq!(
        unasked.text()?,
        ORIGIN_PLAYLIST,
        "video/mp2t at a .m3u8 URL is relayed: the origin's label is the evidence we have"
    );

    let forced = client
        .get(format!(
            "{url}&r={}",
            encode("Content-Type:application/x-mpegURL")
        ))
        .send()?;
    assert_eq!(forced.status(), reqwest::StatusCode::OK);
    assert_eq!(
        forced
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/x-mpegURL"),
        "the override reaches the player"
    );
    assert_eq!(
        forced.text()?,
        expected_playlist(fixture.origin.addr),
        "and it is the type the classification asked, so every line came back rewritten"
    );

    drop(fixture.handle);
    Ok(())
}

/// The other direction of the same escape hatch, and the one it must not
/// have: `r=` may *force* the playlist path and may never veto it.
///
/// A caller labelling the stream it is describing `video/mp4` is an
/// ordinary thing for an addon to do, and it says nothing about the bytes
/// -- the origin here serves a real `application/x-mpegURL` playlist. While
/// `r=` was merged over the origin's own type before the classification
/// asked, that label suppressed the rewrite: measured, the playlist came
/// back verbatim, so the player resolved every segment against the origin
/// and fetched it direct -- without the `h=` an authenticated stream needs
/// and without the `p=` a close is addressed by. The whole feature falls
/// out of the response.
///
/// An empty `r=Content-Type:` did it by shadowing the origin's type with
/// nothing at all, which is the same bug with no label to blame it on. The
/// URL names no playlist in either case, so the origin's own header is the
/// only evidence there is -- and it is evidence `r=` does not get to erase.
#[test]
fn an_r_content_type_cannot_suppress_the_rewrite_of_a_real_playlist() -> anyhow::Result<()> {
    for forced in ["Content-Type:video/mp4", "Content-Type:"] {
        let fixture = fixture_with(playlist_origin_typed("application/x-mpegURL")?)?;
        let target = format!("http://{}/live/stream", fixture.origin.addr);
        let response = reqwest::blocking::Client::new()
            .get(format!(
                "{}/proxy/?d={}&r={}",
                fixture.base,
                encode(&target),
                encode(forced)
            ))
            .send()?;

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(
            response.text()?,
            expected_playlist(fixture.origin.addr),
            "{forced} does not unsay the origin's application/x-mpegURL"
        );

        drop(fixture.handle);
    }

    // And the override still reaches the player, which is what `r=` is for:
    // forcing the rewrite is not the same as ignoring the header.
    let fixture = fixture_with(playlist_origin_typed("application/x-mpegURL")?)?;
    let target = format!("http://{}/live/stream", fixture.origin.addr);
    let response = reqwest::blocking::Client::new()
        .get(format!(
            "{}/proxy/?d={}&r={}",
            fixture.base,
            encode(&target),
            encode("Content-Type:video/mp4")
        ))
        .send()?;
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("video/mp4"),
        "the caller's label is what the player is told; only the classification ignores it"
    );

    drop(fixture.handle);
    Ok(())
}

/// The four forms a playlist line can take, all four through the proxy,
/// asserted on what the origin was actually asked for.
///
/// An absolute URL on the playlist's own origin, an absolute URL on
/// another, an absolute path and a relative one. The reference reads all
/// four and this is the port of that; before it, an absolute path resolved
/// against the origin the same way a relative one did (right answer, by
/// accident of `Url::join`) and every line came back in the query format,
/// which has no directory for a nested playlist to hang its own lines off.
#[test]
fn every_form_a_playlist_line_can_take_comes_back_through_the_proxy() -> anyhow::Result<()> {
    fn serve(name: &'static str) -> impl Fn(&Request, &mut TcpStream) + Send + Sync + 'static {
        move |request: &Request, socket: &mut TcpStream| {
            let body = format!("{name} {}", request.target());
            let _ = socket.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: video/mp2t\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            );
            let _ = socket.flush();
        }
    }

    let other = Origin::start_with(serve("other"))?;
    let other_addr = other.addr;
    let playlist = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let served = playlist.clone();
    let origin = Origin::start_with(move |request: &Request, socket: &mut TcpStream| {
        if !request.target().ends_with(".m3u8") {
            serve("home")(request, socket);
            return;
        }
        let body = served
            .lock()
            .expect("the playlist is set before it is asked for");
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apple.mpegurl\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(origin)?;
    let home = fixture.origin.addr;
    *playlist.lock().expect("nothing is reading it yet") = format!(
        "#EXTM3U\n\
         http://{home}/live/same-origin.ts\n\
         http://{other_addr}/other/cross-origin.ts\n\
         /root/absolute-path.ts\n\
         relative.ts\n"
    );

    let client = reqwest::blocking::Client::new();
    let target = format!("http://{home}/live/master.m3u8");
    let body = client
        .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .send()?
        .text()?;
    assert_eq!(fixture.origin.next_request().target(), "/live/master.m3u8");

    let lines: Vec<String> = body
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .map(str::to_string)
        .collect();
    assert_eq!(lines.len(), 4, "one rewritten line per URI: {body}");

    // Each rewritten line, fetched the way the player would fetch it, and
    // then the request the origin it names actually received.
    let expected = [
        (&fixture.origin, "home", "/live/same-origin.ts"),
        (&other, "other", "/other/cross-origin.ts"),
        (&fixture.origin, "home", "/root/absolute-path.ts"),
        (&fixture.origin, "home", "/live/relative.ts"),
    ];
    for (line, (origin, name, path)) in lines.iter().zip(expected) {
        assert!(
            line.starts_with("/proxy/d="),
            "the path format, so a line has a directory of its own: {line}"
        );
        let fetched = client.get(format!("{}{line}", fixture.base)).send()?;
        assert_eq!(fetched.status(), reqwest::StatusCode::OK, "fetching {line}");
        assert_eq!(fetched.text()?, format!("{name} {path}"));
        assert_eq!(origin.next_request().target(), path);
    }

    drop(fixture.handle);
    Ok(())
}

/// Master playlist, media playlist, segments -- the ordinary shape of an
/// HLS stream, and the one the query format could not serve. A rewritten
/// line has to keep a directory of its own, because the media playlist's
/// own relative lines are resolved by the player against the URL it fetched
/// the media playlist at: under `/proxy/?d=<whole url>` that made
/// `/proxy/seg-0.ts`, a 404 from our own router before the origin was ever
/// asked.
#[test]
fn a_nested_playlist_resolves_its_own_relative_lines_through_the_proxy() -> anyhow::Result<()> {
    let origin = Origin::start_with(|request: &Request, socket: &mut TcpStream| {
        let target = request.target();
        let (content_type, body) = if target.ends_with("master.m3u8") {
            (
                "application/vnd.apple.mpegurl",
                "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800000\nv/720p/media.m3u8\n".to_string(),
            )
        } else if target.ends_with("media.m3u8") {
            (
                "application/vnd.apple.mpegurl",
                "#EXTM3U\n#EXTINF:10,\nseg-0.ts\n".to_string(),
            )
        } else {
            ("video/mp2t", format!("bytes of {target}"))
        };
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(origin)?;
    let client = reqwest::blocking::Client::new();
    let named = |body: &str| {
        body.lines()
            .find(|line| !line.starts_with('#') && !line.is_empty())
            .expect("the rewritten playlist names something")
            .to_string()
    };

    let target = format!("http://{}/live/master.m3u8", fixture.origin.addr);
    let master = client
        .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .send()?
        .text()?;
    let media_url = named(&master);

    let media = client
        .get(format!("{}{media_url}", fixture.base))
        .send()?
        .text()?;
    let segment_url = named(&media);
    assert!(
        segment_url.starts_with(
            media_url
                .rsplit_once('/')
                .expect("a path format line has a directory")
                .0
        ),
        "the segment sits in the media playlist's own directory: {segment_url}"
    );

    let segment = client
        .get(format!("{}{segment_url}", fixture.base))
        .send()?;
    assert_eq!(segment.status(), reqwest::StatusCode::OK);
    assert_eq!(
        segment.text()?,
        "bytes of /live/v/720p/seg-0.ts",
        "the segment beside the media playlist, not beside the master one"
    );

    assert_eq!(fixture.origin.next_request().target(), "/live/master.m3u8");
    assert_eq!(
        fixture.origin.next_request().target(),
        "/live/v/720p/media.m3u8"
    );
    assert_eq!(
        fixture.origin.next_request().target(),
        "/live/v/720p/seg-0.ts"
    );

    drop(fixture.handle);
    Ok(())
}

/// An authenticated HLS stream, end to end: the playlist and every segment
/// need the same `Authorization` the addon put in `h=`, and only the
/// playlist's own URL was carrying it.
///
/// A rewritten line used to carry `d=` and `p=` and nothing else, so the
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

/// `r=` labels the resource the caller named, and the caller named a
/// playlist. Copying it onto every rewritten line told the player that the
/// MPEG-TS segments and the 16-byte AES key were playlists too -- and
/// `r=Content-Type:application/x-mpegurl` is exactly what stremio-core
/// sends for an HLS stream, so this was every proxied HLS stream, not a
/// corner.
///
/// The reference copies it (its virtual root is the caller's whole opts
/// string) and compounds it: it classifies a response by the content type
/// it has already merged `r=` into, so a segment reached through such a
/// line is called a playlist and run through the line rewriter -- MPEG-TS,
/// rewritten as text. We ask the merged type too, deliberately, since that
/// is how `r=` corrects an origin that mislabels; what keeps the same
/// thing from happening here is this test's subject, that `r=` never
/// reaches the line. Its cross-origin branch drops `r=`, and that is the
/// half ported.
#[test]
fn a_segment_named_by_a_rewritten_line_does_not_carry_the_playlist_s_r() -> anyhow::Result<()> {
    let origin = Origin::start_with(|request: &Request, socket: &mut TcpStream| {
        let (content_type, body) = if request.target().contains(".m3u8") {
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
    let playlist = client
        .get(format!(
            "{}/proxy/d={}&r={}/live/master.m3u8",
            fixture.base,
            encode(&format!("http://{}", fixture.origin.addr)),
            encode("Content-Type:application/x-mpegurl")
        ))
        .send()?;

    assert_eq!(playlist.status(), reqwest::StatusCode::OK);
    assert_eq!(
        playlist
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/x-mpegurl"),
        "the resource the caller named is labelled the way the caller said"
    );
    let body = playlist.text()?;
    let segment = body
        .lines()
        .find(|line| !line.starts_with('#') && !line.is_empty())
        .expect("the rewritten playlist names a segment")
        .to_string();
    assert!(
        !segment.contains("r="),
        "the label belongs to the playlist, not to what it names: {segment}"
    );

    let fetched = client.get(format!("{}{segment}", fixture.base)).send()?;
    assert_eq!(fetched.status(), reqwest::StatusCode::OK);
    assert_eq!(
        fetched
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("video/mp2t"),
        "so the segment arrives as what it is, not as a playlist"
    );
    assert_eq!(fetched.text()?, "segment bytes");

    drop(fixture.handle);
    Ok(())
}

/// `r=` comes from addon metadata, and one of its names is a loaded gun:
/// `r=Content-Length:1` in front of a megabyte is a response hyper will not
/// write. In a debug build the connection task panics on it; in a release
/// build the player is told the film is one byte long and waits for the
/// rest of a body that has already been sent. The framing of this hop is
/// dropped from `r=` exactly as it is dropped from the origin's own
/// headers.
#[test]
fn a_custom_response_header_cannot_reframe_the_response() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let response = reqwest::blocking::Client::new()
        .get(format!(
            "{}/proxy/d={}&r={}&r={}/movie.mp4",
            fixture.base,
            encode(&format!("http://{}", fixture.origin.addr)),
            encode("Content-Length:1"),
            encode("Transfer-Encoding:chunked")
        ))
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(ORIGIN_LENGTH.to_string().as_str()),
        "the length of the body actually being sent, which the addon does not get to name"
    );
    assert_eq!(
        response.bytes()?.len(),
        ORIGIN_LENGTH,
        "and the whole of it arrives"
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

/// The downgrade, working as advertised: an endpoint whose certificate will
/// not verify is fetched once with verification, once without, and written
/// down so the next request pays only one handshake.
///
/// What is written down is the whole origin -- scheme, host *and* port. A
/// certificate is served by a TLS endpoint, not by a name: keyed by host
/// alone, this failure would also have turned verification off for the same
/// host's `:443`, and for `http://localhost`, which has no certificate to
/// verify in the first place.
#[test]
fn an_endpoint_whose_certificate_fails_is_fetched_unverified_and_named() -> anyhow::Result<()> {
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
    let downgraded = stream_server::unverified_origins();
    assert!(
        downgraded.contains(&format!("https://localhost:{}", tls.addr.port())),
        "the endpoint is written down with its scheme and port: {downgraded:?}"
    );
    assert!(
        !downgraded.contains(&"localhost".to_string()),
        "and not as a bare host, which would take every port with it: {downgraded:?}"
    );

    drop(fixture.handle);
    Ok(())
}

/// The same failure one redirect away, which is where the host recorded
/// used to be the wrong one entirely. reqwest attributes a connect failure
/// to the URL the request *started* at, so the redirecting host was marked
/// unverified for a certificate it never presented, and the endpoint whose
/// handshake actually failed was not marked at all -- the same mistake an
/// `https` -> `https` chain makes, where the host downgraded is the *good*
/// one. Its redirect policy is asked before every hop, so the failing one
/// has a name after all.
///
/// The redirector is plain HTTP because a verifiable first hop needs a
/// certificate authority; the hop that fails is the second either way, and
/// it is the second that must be the one written down.
#[test]
fn a_certificate_failure_behind_a_redirect_downgrades_the_endpoint_that_failed()
-> anyhow::Result<()> {
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
    let redirector_addr = fixture.origin.addr;
    let target = format!("http://{redirector_addr}/film.mkv");
    let response = reqwest::blocking::Client::new()
        .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .send()?;

    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "the retry follows the same redirect, and the stream plays"
    );
    assert_eq!(response.text()?, "secret bytes");

    let downgraded = stream_server::unverified_origins();
    assert!(
        downgraded.contains(&format!("https://127.0.0.1:{}", tls_addr.port())),
        "the endpoint whose handshake failed is the one written down: {downgraded:?}"
    );
    assert!(
        !downgraded.contains(&format!("http://{redirector_addr}")),
        "and the host that only redirected us is not: {downgraded:?}"
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
    let downgraded = stream_server::unverified_origins();
    assert!(
        !downgraded.contains(&"http://127.0.0.1:1".to_string()),
        "a connection refused is not a certificate failure: {downgraded:?}"
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

/// An authenticated stream behind a redirect, which is the shape reqwest's
/// default policy silently broke: it strips `Authorization`, `Cookie` and
/// `Proxy-Authorization` on any cross-host *or cross-port* redirect, and a
/// CDN handing off to an edge is exactly that. The playlist fetched `200`
/// from the CDN, the edge saw no credential at all and answered `403`, and
/// nothing in the log said a header had been dropped.
///
/// The redirect chain is walked here now, so `h=` is applied to every hop
/// -- which is the reference's answer too (`redirect: "manual"`, its own
/// loop, and `opts.h.forEach(headers.set(...))` re-applied per hop). Both
/// origins are on loopback, so this is a same-host, cross-*port* redirect:
/// the case that reads as safe and is stripped just the same.
#[test]
fn a_header_from_h_survives_the_redirect_the_origin_chose() -> anyhow::Result<()> {
    const SECRET: &str = "Bearer s3cret";

    let edge = Origin::start_with(|request: &Request, socket: &mut TcpStream| {
        let body = match request.header("authorization") {
            Some(SECRET) => "the edge served it",
            _ => {
                let _ = socket.write_all(
                    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
                return;
            }
        };
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })?;
    let edge_addr = edge.addr;
    let cdn = Origin::start_with(move |_request: &Request, socket: &mut TcpStream| {
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 302 Found\r\nLocation: http://{edge_addr}/edge/film.mkv\r\n\
                 Content-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(cdn)?;
    let response = reqwest::blocking::Client::new()
        .get(format!(
            "{}/proxy/d={}&h={}/cdn/film.mkv",
            fixture.base,
            encode(&format!("http://{}", fixture.origin.addr)),
            encode(&format!("Authorization:{SECRET}"))
        ))
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text()?, "the edge served it");
    assert_eq!(
        fixture.origin.next_request().header("authorization"),
        Some(SECRET),
        "the first hop carried it"
    );
    assert_eq!(
        edge.next_request().header("authorization"),
        Some(SECRET),
        "and so did the hop the origin sent us to"
    );

    drop(fixture.handle);
    Ok(())
}

/// A relative `Location`, resolved against the URL that sent it rather than
/// against the origin. The reference resolves against `dest.href` with the
/// path cut off, which turns `Location: v2/film.mkv` beside
/// `/cdn/2024/film.mkv` into `/v2/film.mkv` at the host root.
#[test]
fn a_relative_location_resolves_beside_the_resource_that_sent_it() -> anyhow::Result<()> {
    let origin = Origin::start_with(|request: &Request, socket: &mut TcpStream| {
        if request.target() == "/cdn/2024/film.mkv" {
            let _ = socket.write_all(
                b"HTTP/1.1 302 Found\r\nLocation: v2/film.mkv\r\n\
                  Content-Length: 0\r\nConnection: close\r\n\r\n",
            );
            let _ = socket.flush();
            return;
        }
        let body = format!("served {}", request.target());
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(origin)?;
    let target = format!("http://{}/cdn/2024/film.mkv", fixture.origin.addr);
    let response = reqwest::blocking::Client::new()
        .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text()?, "served /cdn/2024/v2/film.mkv");
    assert_eq!(fixture.origin.next_request().target(), "/cdn/2024/film.mkv");
    assert_eq!(
        fixture.origin.next_request().target(),
        "/cdn/2024/v2/film.mkv"
    );

    drop(fixture.handle);
    Ok(())
}

/// A redirect that never ends. The hop count is the loop detection: a ring
/// is a chain that does not stop, and a player has to be told so rather
/// than left waiting.
#[test]
fn a_redirect_ring_is_given_up_on_rather_than_followed_forever() -> anyhow::Result<()> {
    let origin = Origin::start_with(|_request: &Request, socket: &mut TcpStream| {
        let _ = socket.write_all(
            b"HTTP/1.1 302 Found\r\nLocation: /round/again\r\n\
              Content-Length: 0\r\nConnection: close\r\n\r\n",
        );
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(origin)?;
    let target = format!("http://{}/round/again", fixture.origin.addr);
    let response = reqwest::blocking::Client::new()
        .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::BAD_GATEWAY);
    assert!(response.text()?.contains("too many redirects"));

    drop(fixture.handle);
    Ok(())
}

/// A chain that goes somewhere new every time, which a ring does not: the
/// hop bound is what ends it, and this is what pins the number. Ten
/// redirects followed (`MAX_REDIRECTS`), so eleven requests, and the
/// eleventh's `Location` is where we stop.
#[test]
fn a_redirect_chain_longer_than_the_hop_bound_is_given_up_on() -> anyhow::Result<()> {
    let origin = Origin::start_with(|request: &Request, socket: &mut TcpStream| {
        let hop: usize = request
            .target()
            .trim_start_matches("/hop/")
            .parse()
            .unwrap_or(0);
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 302 Found\r\nLocation: /hop/{}\r\n\
                 Content-Length: 0\r\nConnection: close\r\n\r\n",
                hop + 1
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(origin)?;
    let target = format!("http://{}/hop/0", fixture.origin.addr);
    let response = reqwest::blocking::Client::new()
        .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::BAD_GATEWAY);
    assert!(response.text()?.contains("too many redirects"));

    for hop in 0..=10 {
        assert_eq!(
            fixture.origin.next_request().target(),
            format!("/hop/{hop}"),
            "every hop up to the bound was walked"
        );
    }
    assert!(
        fixture.origin.requests.try_recv().is_err(),
        "and the eleventh redirect was not followed: the response is already back, so \
         anything further would be here by now"
    );

    drop(fixture.handle);
    Ok(())
}

/// A `302` this proxy will not follow because of where it points: only
/// `http`/`https` is fetched, since the one thing a route that fetches
/// whatever a caller names must not do is let an *origin* send it somewhere
/// no caller could have asked for.
///
/// What the player used to get for that was `302 Found`, the CORS headers
/// and `content-length: 0` -- an unfollowable redirect and a headerless one
/// spelled the same way, and nothing in either saying what happened. The
/// `Location` comes back now, exactly as the origin wrote it.
#[test]
fn an_unfollowed_redirect_still_says_where_it_pointed() -> anyhow::Result<()> {
    let origin = Origin::start_with(|_request: &Request, socket: &mut TcpStream| {
        let _ = socket.write_all(
            b"HTTP/1.1 302 Found\r\nLocation: ftp://files.example.com/film.mkv\r\n\
              Content-Length: 0\r\nConnection: close\r\n\r\n",
        );
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(origin)?;
    let target = format!("http://{}/cdn/film.mkv", fixture.origin.addr);
    // A client that does not follow it either, which is the only way to
    // look at the `302` itself -- reqwest's default policy would chase the
    // `Location` and fail on the scheme, which is a fair account of what
    // this header is worth to whoever reads it.
    let response = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?
        .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::FOUND);
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok()),
        Some("ftp://files.example.com/film.mkv"),
        "the origin's own value, not one resolved or rewritten through the proxy"
    );

    drop(fixture.handle);
    Ok(())
}

/// The same relay, with a *relative* `Location`, which is the form that
/// cannot be handed on as written.
///
/// A relative `Location` is relative to the URL it came from, and the
/// player never saw that URL -- it asked this proxy. Relayed byte for byte,
/// `Location: /elsewhere` resolved against *us*: measured, a player
/// following the `302` we relayed came back to
/// `http://127.0.0.1:<proxy>/elsewhere`, a path this server does not serve,
/// instead of going to the origin that named it. Resolving it against the
/// URL the response came from makes no request of our own -- it just says
/// where the origin pointed, in a form that means the same thing to
/// somebody who was not on the hop.
#[test]
fn a_relative_location_we_will_not_follow_is_relayed_absolute() -> anyhow::Result<()> {
    // `305 Use Proxy`: a status this route relays rather than follows, so
    // the `Location` is ours to hand on and nothing has resolved it already.
    let origin = Origin::start_with(|_request: &Request, socket: &mut TcpStream| {
        let _ = socket.write_all(
            b"HTTP/1.1 305 Use Proxy\r\nLocation: /elsewhere\r\n\
              Content-Length: 0\r\nConnection: close\r\n\r\n",
        );
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(origin)?;
    let target = format!("http://{}/cdn/film.mkv", fixture.origin.addr);
    let response = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?
        .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .send()?;

    assert_eq!(response.status().as_u16(), 305);
    let location = response
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert_eq!(
        location,
        format!("http://{}/elsewhere", fixture.origin.addr),
        "resolved against the URL the response came from, not left to the player"
    );
    assert!(
        !location.starts_with(&fixture.base),
        "and so it does not point the player back at this proxy: {location}"
    );

    drop(fixture.handle);
    Ok(())
}

/// The statuses in `300..400` that are not "the resource is over there".
/// Following any status with a `Location` -- what the reference does -- had
/// the proxy fetch and serve the `Location` of a `300`, a `304`, a `305`
/// and a `306`, answering `200` where the HTTP client this loop replaced
/// relays the status untouched.
///
/// `305 Use Proxy` is the one that matters: it names a proxy to send the
/// request *through*, not a new home for the resource, so an origin that
/// answers it was choosing the host our `h=` credentials get sent to.
#[test]
fn a_3xx_that_does_not_move_the_resource_is_not_followed() -> anyhow::Result<()> {
    const ELSEWHERE: &str = "the resource we must not have fetched";

    let origin = Origin::start_with(|request: &Request, socket: &mut TcpStream| {
        if request.target() == "/elsewhere" {
            let _ = socket.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{ELSEWHERE}",
                    ELSEWHERE.len()
                )
                .as_bytes(),
            );
            let _ = socket.flush();
            return;
        }
        let status = request.target().trim_start_matches("/status/");
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 {status} Something\r\nLocation: /elsewhere\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(origin)?;
    let client = reqwest::blocking::Client::new();
    for status in [300u16, 304, 305, 306] {
        let target = format!("http://{}/status/{status}", fixture.origin.addr);
        let response = client
            .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
            .send()?;

        assert_eq!(
            response.status().as_u16(),
            status,
            "the origin's own status is relayed, not resolved into a fetch"
        );
        assert_eq!(
            response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok()),
            Some(format!("http://{}/elsewhere", fixture.origin.addr).as_str()),
            "with the Location it named -- absolute, so the player can see where it was \
             sent rather than resolving it against us"
        );
        assert_ne!(
            response.text()?,
            ELSEWHERE,
            "and the body it pointed at was never asked for"
        );
        assert_eq!(
            fixture.origin.next_request().target(),
            format!("/status/{status}")
        );
        assert!(
            fixture.origin.requests.try_recv().is_err(),
            "one request, not two"
        );
    }

    drop(fixture.handle);
    Ok(())
}

/// A `.m3u8` URL that answers with an error. There is no playlist in a
/// 404, and rewriting it said otherwise: the error page came back as a
/// playlist of proxy URLs built out of the words in it, a fabricated
/// segment list an HLS player would dutifully try to fetch.
///
/// (The other half of this used to be a `HEAD`, on the grounds that it has
/// no body to rewrite either. It has its own test now, because what a
/// `HEAD` must answer is a longer story than "not that".)
#[test]
fn an_error_page_at_a_playlist_url_is_not_rewritten_as_a_playlist() -> anyhow::Result<()> {
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

    drop(fixture.handle);
    Ok(())
}

/// A `HEAD` and a `GET` at the same playlist URL, field by field.
///
/// The `HEAD` is not rewritten -- there is no body to rewrite -- but it
/// used to fall through to the plain relay along with that, and so
/// advertised the origin's framing for a body this proxy would never
/// serve. Measured: the `HEAD` said `Content-Length: 67` and
/// `Accept-Ranges: bytes` where the `GET` returned 199 chunked bytes and
/// `Accept-Ranges: none`, so a client that sized the resource and then
/// asked for `Range: bytes=0-66` got a `200` carrying 199 of them.
///
/// What a `HEAD` describes is the response a `GET` would get, so every
/// field but the body is now the same for both: no length, because the
/// length is not known until the rewrite has been written, and no claim to
/// ranges.
#[test]
fn a_head_and_a_get_at_a_playlist_url_describe_the_same_response() -> anyhow::Result<()> {
    let origin = Origin::start_with(|request: &Request, socket: &mut TcpStream| {
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apple.mpegurl\r\n\
                 Accept-Ranges: bytes\r\nETag: \"the-playlist\"\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                ORIGIN_PLAYLIST.len()
            )
            .as_bytes(),
        );
        // A HEAD gets the headers and no body, as any origin would answer.
        if !request.line.starts_with("HEAD") {
            let _ = socket.write_all(ORIGIN_PLAYLIST.as_bytes());
        }
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(origin)?;
    let client = reqwest::blocking::Client::new();
    let url = format!(
        "{}/proxy/?d={}",
        fixture.base,
        encode(&format!("http://{}/live/master.m3u8", fixture.origin.addr))
    );
    let head = client.head(&url).send()?;
    let get = client.get(&url).send()?;

    assert_eq!(head.status(), get.status());
    for field in [
        "content-type",
        "accept-ranges",
        "content-length",
        "content-range",
        "etag",
        "last-modified",
    ] {
        assert_eq!(
            head.headers().get(field),
            get.headers().get(field),
            "the HEAD and the GET disagree about {field}"
        );
    }
    assert_eq!(
        head.headers()
            .get(reqwest::header::ACCEPT_RANGES)
            .and_then(|value| value.to_str().ok()),
        Some("none"),
        "neither of them offers a range into a body written as it is read"
    );
    assert!(
        head.headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .is_none(),
        "and neither declares a length for it -- the origin's 67 describes a body \
         nobody is going to be sent"
    );

    assert!(head.bytes()?.is_empty(), "a HEAD is still bodiless");
    assert_eq!(get.text()?, expected_playlist(fixture.origin.addr));

    drop(fixture.handle);
    Ok(())
}

/// A ranged request for a playlist, which is a shape both this proxy and
/// the reference used to get wrong in the same way: a `206` was rewritten,
/// its `Content-Range` describing an entity the rewritten body is not.
///
/// The two `206`s mean different things. `Range: bytes=0-` -- what a player
/// sends to find out whether the origin is seekable -- comes back as the
/// whole entity, and that is a playlist to rewrite and to answer as the
/// `200` it has become. A `206` carrying a *part* is a fragment whose edge
/// lines are cut in half: it is relayed exactly as the origin sent it,
/// range headers and all, with a WARN saying its segments will bypass the
/// proxy.
#[test]
fn a_ranged_playlist_is_rewritten_only_when_the_range_is_all_of_it() -> anyhow::Result<()> {
    let origin = Origin::start_with(|request: &Request, socket: &mut TcpStream| {
        let length = ORIGIN_PLAYLIST.len();
        let (first, last) = match request.range() {
            Some(value) => {
                let (first, last) = value
                    .trim_start_matches("bytes=")
                    .split_once('-')
                    .expect("the test sends a well-formed range");
                (
                    first.parse::<usize>().expect("a first byte"),
                    if last.is_empty() {
                        length - 1
                    } else {
                        last.parse::<usize>().expect("a last byte")
                    },
                )
            }
            None => (0, length - 1),
        };
        let body = &ORIGIN_PLAYLIST[first..=last];
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 206 Partial Content\r\n\
                 Content-Type: application/vnd.apple.mpegurl\r\n\
                 Content-Range: bytes {first}-{last}/{length}\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(origin)?;
    let client = reqwest::blocking::Client::new();
    let url = format!(
        "{}/proxy/?d={}",
        fixture.base,
        encode(&format!("http://{}/live/master.m3u8", fixture.origin.addr))
    );

    let whole = client
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    assert_eq!(
        whole.status(),
        reqwest::StatusCode::OK,
        "the rewritten body is the whole resource, whatever was asked for"
    );
    assert!(
        whole
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .is_none(),
        "and it is not a range of the origin's entity any more"
    );
    assert_eq!(
        whole.text()?,
        expected_playlist(fixture.origin.addr),
        "every line rewritten, exactly as for a 200"
    );

    let part = client
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=10-40")
        .send()?;
    assert_eq!(
        part.status(),
        reqwest::StatusCode::PARTIAL_CONTENT,
        "a fragment is relayed as the fragment it is"
    );
    assert_eq!(
        part.headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok()),
        Some(format!("bytes 10-40/{}", ORIGIN_PLAYLIST.len()).as_str())
    );
    assert_eq!(
        part.text()?,
        ORIGIN_PLAYLIST[10..=40],
        "the origin's own bytes -- there is no rewriting half a line"
    );

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
///
/// What the close ends is the body, not the headers. The rewrite streams,
/// so the player has its `200` as soon as the origin's own headers arrive
/// and is then waiting on bytes that never come -- exactly the shape a
/// wedged media read has, and it fails the same way.
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
        let response = reqwest::blocking::Client::new().get(url).send()?;
        let status = response.status();
        Ok::<_, reqwest::Error>((status, response.text().is_err()))
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
        (reqwest::StatusCode::OK, true),
        "the wedged playlist read ends now, rather than at a timeout"
    );

    drop(fixture.handle);
    Ok(())
}

/// The close arriving during the origin's time-to-first-byte. The token was
/// live when `/proxy` checked it, before the fetch, and retired by the time
/// the origin's headers came back -- so the check that costs the origin
/// nothing is exactly the check that cannot see this.
///
/// Measured before the fix, against a RealDebrid file behind a slow first
/// byte: the close answered `{"closed":0}`, because nothing was registered
/// yet, and 3.9 MB was then relayed under a token that no longer existed.
/// The registration is what refuses now, so the answer is the same `410` a
/// later request would have got.
#[test]
fn closing_during_the_origin_s_time_to_first_byte_ends_that_stream() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};

    const PAYLOAD: usize = 64 * 1024;

    // Nothing at all until the test says so: the request has arrived, the
    // pre-fetch check has passed, and not a header has been written back.
    let answer = std::sync::Arc::new(AtomicBool::new(false));
    let origin_answer = answer.clone();
    let origin = Origin::start_with(move |_request: &Request, socket: &mut TcpStream| {
        while !origin_answer.load(Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\n\
                 Content-Length: {PAYLOAD}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        );
        let _ = socket.write_all(&vec![0u8; PAYLOAD]);
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(origin)?;
    let target = format!("http://{}/slow.mp4", fixture.origin.addr);
    let url = format!(
        "{}/proxy/?d={}&p=player-slow",
        fixture.base,
        encode(&target)
    );
    let reader = std::thread::spawn(move || {
        let response = reqwest::blocking::Client::new().get(url).send()?;
        let status = response.status();
        Ok::<_, reqwest::Error>((status, response.bytes()?.len()))
    });

    // The origin has been asked and has not answered, which is the window.
    fixture.origin.next_request();
    assert_eq!(
        fixture.handle.close_proxy_streams("player-slow"),
        0,
        "there is nothing live to close yet -- that is what makes this a race"
    );
    answer.store(true, Ordering::SeqCst);

    let (status, relayed) = reader.join().expect("the reader thread")?;
    assert_eq!(
        status,
        reqwest::StatusCode::GONE,
        "the stream its client already ended is not served to it"
    );
    assert_ne!(relayed, PAYLOAD, "and the origin's body is not relayed");

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
