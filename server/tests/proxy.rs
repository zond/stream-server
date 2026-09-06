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
        origin: Origin::start()?,
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

/// The playlist an HLS origin serves, and what the proxy must hand the
/// player instead: every line pointing back through the proxy, which makes
/// the body *longer* than the one the origin sent. That length difference is
/// the whole of defect 1 -- a relayed `Content-Length` describing 82 bytes in
/// front of 180 is a response hyper refuses to write.
const ORIGIN_PLAYLIST: &str =
    "#EXTM3U\n#EXT-X-VERSION:3\n#EXTINF:10,\nseg-0.ts\n#EXTINF:10,\nseg-1.ts\n";

fn expected_playlist(origin: SocketAddr) -> String {
    let mut expected = String::new();
    for line in ORIGIN_PLAYLIST.lines() {
        if line.starts_with('#') {
            expected.push_str(line);
        } else {
            expected.push_str(&format!(
                "/proxy/?d={}",
                encode(&format!("http://{origin}/live/{line}"))
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
    let config_dir = tempfile::tempdir()?;
    let cache_root = tempfile::tempdir()?;
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.path().join("cache")),
        ..offline_config()
    })?;
    let origin = playlist_origin(framing)?;
    let target = format!("http://{}/live/master.m3u8", origin.addr);
    let response = reqwest::blocking::Client::new()
        .get(format!(
            "http://{}/proxy/?d={}",
            handle.http_addr(),
            encode(&target)
        ))
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
        expected_playlist(origin.addr),
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

    drop(handle);
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
