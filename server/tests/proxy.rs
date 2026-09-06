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
use std::net::{SocketAddr, TcpListener};

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

/// A minimal origin serving [`ORIGIN_LENGTH`] bytes of [`byte_at`] with
/// `Range` support, on a thread of its own. It records the request line and
/// the `Range` header of everything it is asked for, which is how a test
/// tells "the proxy relayed the range" from "the proxy fetched the whole
/// file and sliced it".
struct Origin {
    addr: SocketAddr,
    requests: std::sync::mpsc::Receiver<(String, Option<String>)>,
}

const ORIGIN_LENGTH: usize = 1024 * 1024;

impl Origin {
    fn start() -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (sender, requests) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let Ok(peer) = stream.try_clone() else { break };
                let mut reader = BufReader::new(peer);
                let mut request_line = String::new();
                if reader.read_line(&mut request_line).is_err() {
                    continue;
                }
                let mut range = None;
                loop {
                    let mut header = String::new();
                    match reader.read_line(&mut header) {
                        Ok(0) => break,
                        Ok(_) if header.trim().is_empty() => break,
                        Ok(_) => {}
                        Err(_) => break,
                    }
                    if let Some((name, value)) = header.split_once(':')
                        && name.eq_ignore_ascii_case("range")
                    {
                        range = Some(value.trim().to_string());
                    }
                }
                let served = range.as_deref().and_then(|value| {
                    let (first, last) = value.trim_start_matches("bytes=").split_once('-')?;
                    let first: usize = first.parse().ok()?;
                    let last: usize = if last.is_empty() {
                        ORIGIN_LENGTH - 1
                    } else {
                        last.parse().ok()?
                    };
                    Some((first, last))
                });
                let _ = sender.send((request_line.trim_end().to_string(), range));
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
                let _ = stream.write_all(head.as_bytes());
                let _ = stream.write_all(&body);
                let _ = stream.flush();
            }
        });
        Ok(Self { addr, requests })
    }

    /// The request line and `Range` header of the next request the origin
    /// was asked for.
    fn next_request(&self) -> (String, Option<String>) {
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
    let (request_line, _) = fixture.origin.next_request();
    assert_eq!(request_line, "GET /dir/movie.mp4?token=abc HTTP/1.1");

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

    let (request_line, _) = fixture.origin.next_request();
    assert_eq!(request_line, "GET /dir/movie.mp4?token=abc HTTP/1.1");

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
    let (_, range) = fixture.origin.next_request();
    assert_eq!(range.as_deref(), Some("bytes=1000-1099"));

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
        let (_, range) = fixture.origin.next_request();
        assert_eq!(range.as_deref(), Some("bytes=0-99"));
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
