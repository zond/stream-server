//! What `/proxy` does with a caller-supplied remote URL.
//!
//! The route opens the target with reqwest, streams the bytes back, and
//! **keeps the whole chunks of them it is allowed to keep**, so the next read
//! of the same bytes is answered from disk (`server/src/proxy_cache.rs`). A
//! client sending every remote stream through this server therefore gets the
//! header rewriting, the single egress point *and* a cache -- but a narrow
//! one, and the tests below pin both halves: what it serves without asking
//! the origin, what it asks the origin for when it holds only part of a
//! range, and every reason it refuses to keep a response at all.
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
use std::path::PathBuf;

/// The config every test here spreads from: no DHT bootstrap name
/// resolution, so starting a server makes no DNS query (see `embed.rs`).
fn offline_config() -> stream_server::ServerConfig {
    stream_server::ServerConfig {
        resolve_dht_bootstrap_names: false,
        // An embedder that keeps a pin record and has nothing in it yet.
        // `None` is not the same thing -- it is "nobody said", which keeps
        // every torrent's data and reports it all as pinned -- and it has a
        // test of its own; spreading it here would turn every retention and
        // idle-pause test in the file into one about a cache that may not be
        // touched.
        pins: Some(Default::default()),
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

/// How the default origin identifies its entity. A response the origin will
/// identify by neither `ETag` nor `Last-Modified` is not one the cache keeps
/// -- nothing could ever tell a second generation of it from the first -- so
/// an origin whose bytes these tests expect to find in the store has to say
/// which entity they are of, the way a real one does.
const ORIGIN_ETAG: &str = "\"the-movie\"";

impl Origin {
    /// The default origin: [`ORIGIN_LENGTH`] bytes of [`byte_at`] with
    /// `Range` support, which is how a test tells "the proxy relayed the
    /// range" from "the proxy fetched the whole file and sliced it".
    fn start() -> anyhow::Result<Self> {
        Self::start_sized(ORIGIN_LENGTH)
    }

    /// The default origin at another length, for a test whose ranges have
    /// to be bigger than [`ORIGIN_LENGTH`] allows.
    fn start_sized(length: usize) -> anyhow::Result<Self> {
        Self::start_with(move |request: &Request, socket: &mut TcpStream| {
            let served = request.range().and_then(|value| {
                let (first, last) = value.trim_start_matches("bytes=").split_once('-')?;
                let first: usize = first.parse().ok()?;
                let last: usize = if last.is_empty() {
                    length - 1
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
                             Content-Type: video/mp4\r\nETag: {ORIGIN_ETAG}\r\n\
                             Content-Range: bytes {first}-{last}/{length}\r\n\
                             Content-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        ),
                        body,
                    )
                }
                None => {
                    let body: Vec<u8> = (0..length).map(byte_at).collect();
                    (
                        format!(
                            "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\n\
                             Content-Type: video/mp4\r\nETag: {ORIGIN_ETAG}\r\n\
                             Content-Length: {length}\r\nConnection: close\r\n\r\n"
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

    /// Whether the origin has been asked for anything it has not been asked
    /// about yet -- how a cache hit is told from a fetch.
    ///
    /// Not a timing assertion: the responder records a request *before* it
    /// writes a byte of the answer, so by the time a proxied response has
    /// been read to its end, an origin fetch that happened has already
    /// arrived here. Nothing has, and nothing will.
    fn was_asked_for_nothing_more(&self) -> bool {
        self.requests.try_recv().is_err()
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
    // Before the first request, which is when the proxy's client is built and
    // the trust set is read. Every proxy request in this binary follows a
    // fixture, so this is the ordering point; the call after the first is a
    // no-op.
    enginefs::http_client::trust_roots_for_tests(vec![TEST_CA.pem.clone()]);
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

/// `/proxy` relays reads and nothing else: a `POST`, `PUT` or `DELETE` is
/// `405` and the origin is asked for nothing. The route is open, under a
/// wildcard CORS, so a relay that took any method would let any page or
/// app on the device write to wherever the device can reach.
#[test]
fn only_reads_are_relayed() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let target = format!("http://{}/dir/movie.mp4", fixture.origin.addr);
    let client = reqwest::blocking::Client::new();
    for method in [
        reqwest::Method::POST,
        reqwest::Method::PUT,
        reqwest::Method::DELETE,
        reqwest::Method::PATCH,
    ] {
        let response = client
            .request(
                method.clone(),
                format!("{}/proxy/?d={}", fixture.base, encode(&target)),
            )
            .body("payload")
            .send()?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::METHOD_NOT_ALLOWED,
            "{method}"
        );
    }
    assert!(
        fixture.origin.was_asked_for_nothing_more(),
        "a refused method reached the origin"
    );

    let head = client
        .head(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .send()?;
    assert_eq!(head.status(), reqwest::StatusCode::OK);

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

/// A range shorter than a chunk leaves nothing behind, and is fetched again
/// every time it is asked for.
///
/// Only *whole* chunks are stored -- there is no hash to tell a complete
/// chunk from a truncated one, so completeness is a rename and a rename only
/// happens when the chunk is full -- and a hundred bytes never fill one. It
/// is the ordinary shape of a probe rather than of playback, and the cache
/// deliberately does nothing for it.
#[test]
fn a_read_too_short_to_fill_a_chunk_is_not_cached() -> anyhow::Result<()> {
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
    assert_eq!(cached_chunks(&fixture).len(), 0);

    drop(fixture.handle);
    Ok(())
}

/// The headline: a range the cache holds whole is answered off disk, and the
/// origin never learns the read happened.
#[test]
fn a_range_the_cache_holds_is_served_without_asking_the_origin() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let origin = format!("http://{}", fixture.origin.addr);
    let url = format!("{}/proxy/d={}/movie.mp4", fixture.base, encode(&origin));
    let client = reqwest::blocking::Client::new();
    let two_chunks = format!("bytes=0-{}", CHUNK * 2 - 1);

    let first = client
        .get(&url)
        .header(reqwest::header::RANGE, &two_chunks)
        .send()?;
    assert_eq!(first.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(first.bytes()?.len() as u64, CHUNK * 2);
    assert_eq!(fixture.origin.next_request().range(), Some(&*two_chunks));
    wait_for_chunks(&fixture, 2);

    let second = client
        .get(&url)
        .header(reqwest::header::RANGE, &two_chunks)
        .send()?;
    assert_eq!(second.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let headers = second.headers().clone();
    assert_eq!(
        header(&headers, "content-range"),
        Some(format!("bytes 0-{}/{ORIGIN_LENGTH}", CHUNK * 2 - 1)).as_deref(),
        "the whole range, off disk"
    );
    assert_eq!(
        header(&headers, "content-length"),
        Some((CHUNK * 2).to_string()).as_deref()
    );
    assert_eq!(
        header(&headers, "accept-ranges"),
        Some("bytes"),
        "the origin proved it answers ranges; this is that claim remembered"
    );
    assert_eq!(
        header(&headers, "content-type"),
        Some("video/mp4"),
        "and the type it labelled the entity with, which a hit has to say too"
    );
    let body = second.bytes()?;
    assert_eq!(body.len() as u64, CHUNK * 2);
    assert_eq!(body[0], byte_at(0));
    assert_eq!(body[CHUNK as usize], byte_at(CHUNK as usize));
    assert_eq!(
        body[(CHUNK * 2 - 1) as usize],
        byte_at((CHUNK * 2 - 1) as usize)
    );

    assert!(
        fixture.origin.was_asked_for_nothing_more(),
        "a range served entirely from the cache makes no origin request"
    );

    drop(fixture.handle);
    Ok(())
}

/// Ranges are the whole job: holding the front of one must narrow what the
/// origin is asked for, not merely save the write.
#[test]
fn a_range_partly_held_fetches_only_the_missing_part() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let origin = format!("http://{}", fixture.origin.addr);
    let url = format!("{}/proxy/d={}/movie.mp4", fixture.base, encode(&origin));
    let client = reqwest::blocking::Client::new();

    let warm = client
        .get(&url)
        .header(reqwest::header::RANGE, format!("bytes=0-{}", CHUNK - 1))
        .send()?;
    assert_eq!(warm.bytes()?.len() as u64, CHUNK);
    fixture.origin.next_request();
    wait_for_chunks(&fixture, 1);

    let response = client
        .get(&url)
        .header(reqwest::header::RANGE, format!("bytes=0-{}", CHUNK * 2 - 1))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        header(response.headers(), "content-range"),
        Some(format!("bytes 0-{}/{ORIGIN_LENGTH}", CHUNK * 2 - 1)).as_deref(),
        "the player is answered about the whole range it asked for"
    );
    let body = response.bytes()?;
    assert_eq!(body.len() as u64, CHUNK * 2);
    assert_eq!(body[0], byte_at(0), "the cached head comes first");
    assert_eq!(
        body[CHUNK as usize],
        byte_at(CHUNK as usize),
        "and the origin's tail is joined to it at the right byte"
    );

    assert_eq!(
        fixture.origin.next_request().range(),
        Some(format!("bytes={}-{}", CHUNK, CHUNK * 2 - 1)).as_deref(),
        "the origin is asked for the gap, from the chunk boundary the cache ended at"
    );

    drop(fixture.handle);
    Ok(())
}
/// An origin whose entity can change under the caller's feet **without
/// changing its length or its content type** -- the one shape a store filed
/// by length and type alone cannot tell apart, and the reason a validator is
/// filed with them.
///
/// Flipping the returned flag serves a second generation of the same
/// resource: same length, same `Content-Type`, every byte different, and a
/// new `ETag`. `honours_if_range` is the difference between an origin that
/// implements the conditional and one that ignores it -- both must leave the
/// player holding bytes from one generation and never a body spliced from
/// two.
fn generational_origin(
    honours_if_range: bool,
) -> anyhow::Result<(std::sync::Arc<std::sync::atomic::AtomicBool>, Origin)> {
    let second = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = second.clone();
    let origin = Origin::start_with(move |request: &Request, socket: &mut TcpStream| {
        let second = flag.load(std::sync::atomic::Ordering::SeqCst);
        let etag = if second { "\"v2\"" } else { "\"v1\"" };
        let byte = |offset: usize| generation_byte(second, offset);
        let asked = request.range().and_then(|value| {
            let (first, last) = value.trim_start_matches("bytes=").split_once('-')?;
            let first: usize = first.parse().ok()?;
            let last: usize = if last.is_empty() {
                ORIGIN_LENGTH - 1
            } else {
                last.parse().ok()?
            };
            Some((first, last))
        });
        // The whole point of `If-Range`: a head the caller still holds is
        // worth a `206` for the tail, and a head that is no longer part of
        // this entity is worth the whole entity instead.
        let stale = request
            .header("if-range")
            .is_some_and(|value| value.trim() != etag);
        let served = if honours_if_range && stale {
            None
        } else {
            asked
        };
        let (head, body) = match served {
            Some((first, last)) => {
                let body: Vec<u8> = (first..=last).map(byte).collect();
                (
                    format!(
                        "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\n\
                         Content-Type: video/mp4\r\nETag: {etag}\r\n\
                         Content-Range: bytes {first}-{last}/{ORIGIN_LENGTH}\r\n\
                         Content-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    ),
                    body,
                )
            }
            None => {
                let body: Vec<u8> = (0..ORIGIN_LENGTH).map(byte).collect();
                (
                    format!(
                        "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\n\
                         Content-Type: video/mp4\r\nETag: {etag}\r\n\
                         Content-Length: {ORIGIN_LENGTH}\r\nConnection: close\r\n\r\n"
                    ),
                    body,
                )
            }
        };
        let _ = socket.write_all(head.as_bytes());
        let _ = socket.write_all(&body);
        let _ = socket.flush();
    })?;
    Ok((second, origin))
}

/// The byte at `offset` in one generation or the other. Complementary, so a
/// body spliced from both is caught wherever the seam falls.
fn generation_byte(second: bool, offset: usize) -> u8 {
    if second {
        !byte_at(offset)
    } else {
        byte_at(offset)
    }
}

/// Every byte of `body` belongs to one generation, and the assertion names
/// which -- a splice fails it at the seam rather than at the length.
fn assert_generation(body: &[u8], second: bool, from: usize, what: &str) {
    for (index, byte) in body.iter().enumerate() {
        assert_eq!(
            *byte,
            generation_byte(second, from + index),
            "{what}: byte {} of the entity is from the other generation",
            from + index
        );
    }
}

/// Warm chunk 0 of the generational origin's entity into the cache, and
/// answer with the URL the rest of the test reads.
fn warm_the_head(fixture: &Fixture) -> anyhow::Result<String> {
    let origin = format!("http://{}", fixture.origin.addr);
    let url = format!("{}/proxy/d={}/movie.mp4", fixture.base, encode(&origin));
    let response = reqwest::blocking::Client::new()
        .get(&url)
        .header(reqwest::header::RANGE, format!("bytes=0-{}", CHUNK - 1))
        .send()?;
    assert_eq!(response.bytes()?.len() as u64, CHUNK);
    fixture.origin.next_request();
    wait_for_chunks(fixture, 1);
    Ok(url)
}

/// A playlist long enough to fill `chunks` whole cache chunks: ordinary
/// `#EXTINF`/segment pairs, cut to the byte so every chunk of it is a whole
/// one and the store keeps them all.
fn playlist_of(chunks: u64) -> Vec<u8> {
    let mut playlist = "#EXTM3U\n#EXT-X-VERSION:3\n".to_string();
    while (playlist.len() as u64) < CHUNK * chunks {
        let line = playlist.len();
        playlist.push_str(&format!("#EXTINF:4.0,\nsegment-{line}.ts\n"));
    }
    playlist.truncate((CHUNK * chunks) as usize);
    playlist.into_bytes()
}

/// A cached head is joined to a fresh `206` only when the origin says the
/// two are parts of one entity, and the joined response is labelled with the
/// validator that said so.
///
/// Length and content type cannot say it: a resource that changes without
/// changing either is exactly the case the store has no other way to see.
/// So the entity is filed under the origin's own validator as well, the
/// narrowed fetch carries it as `If-Range`, and the `206` that comes back
/// has to name it again before a byte of the cached head goes in front.
#[test]
fn a_stitch_is_licensed_by_the_validator_the_head_was_filed_under() -> anyhow::Result<()> {
    let (_generation, origin) = generational_origin(true)?;
    let fixture = fixture_with(origin)?;
    let url = warm_the_head(&fixture)?;

    let response = reqwest::blocking::Client::new()
        .get(&url)
        .header(reqwest::header::RANGE, format!("bytes=0-{}", CHUNK * 2 - 1))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let headers = response.headers().clone();
    assert_eq!(
        header(&headers, "content-range"),
        Some(format!("bytes 0-{}/{ORIGIN_LENGTH}", CHUNK * 2 - 1)).as_deref(),
        "the player is answered about the whole range it asked for"
    );
    assert_eq!(
        header(&headers, "etag"),
        Some("\"v1\""),
        "and the body is labelled with the validator both halves of it are filed under"
    );
    let body = response.bytes()?;
    assert_eq!(body.len() as u64, CHUNK * 2);
    assert_generation(&body, false, 0, "an unchanged entity");

    let asked = fixture.origin.next_request();
    assert_eq!(
        asked.range(),
        Some(format!("bytes={}-{}", CHUNK, CHUNK * 2 - 1)).as_deref(),
        "the origin is asked for the gap, from the chunk boundary the cache ended at"
    );
    assert_eq!(
        asked.header("if-range"),
        Some("\"v1\""),
        "and asked to answer it as a tail only while the head is still part of the entity"
    );

    drop(fixture.handle);
    Ok(())
}

/// The same narrowing against an entity that changed between the two reads.
/// Same length, same type, different bytes -- so nothing but the validator
/// can tell, and the `If-Range` the narrowed fetch carries makes the origin
/// answer with the whole of the new entity instead of a tail that would be
/// spliced onto the old one's head.
#[test]
fn an_entity_that_changed_is_not_spliced_onto_the_cached_head() -> anyhow::Result<()> {
    let (generation, origin) = generational_origin(true)?;
    let fixture = fixture_with(origin)?;
    let url = warm_the_head(&fixture)?;
    generation.store(true, std::sync::atomic::Ordering::SeqCst);

    let response = reqwest::blocking::Client::new()
        .get(&url)
        .header(reqwest::header::RANGE, format!("bytes=0-{}", CHUNK * 2 - 1))
        .send()?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "the head this range was narrowed against is not part of this entity any more, \
         so the origin answers with the whole of the one that is"
    );
    assert_eq!(
        header(response.headers(), "etag"),
        Some("\"v2\""),
        "labelled with the validator of the bytes actually served"
    );
    let body = response.bytes()?;
    assert_eq!(body.len(), ORIGIN_LENGTH);
    assert_generation(&body, true, 0, "a changed entity");

    assert_eq!(
        fixture.origin.next_request().header("if-range"),
        Some("\"v1\""),
        "the condition named the head, which is what made the answer whole"
    );

    drop(fixture.handle);
    Ok(())
}

/// And an origin that ignores `If-Range` altogether -- which is allowed, and
/// is why the condition is not the guard. It answers the narrowed range as a
/// `206` of the *new* entity; the `ETag` on it is not the one the cached head
/// is filed under, so the head is dropped and the origin's own answer is
/// relayed as it stands.
///
/// That answer is a range the player did not ask for, and it says so: the
/// `Content-Range` is the origin's, the body is one generation's, and the
/// player re-reads. A broken read is the price of narrowing against a store
/// that never revalidates; a body spliced out of two generations is not,
/// because nothing downstream could ever find out.
#[test]
fn an_origin_that_ignores_if_range_is_still_not_spliced() -> anyhow::Result<()> {
    let (generation, origin) = generational_origin(false)?;
    let fixture = fixture_with(origin)?;
    let url = warm_the_head(&fixture)?;
    generation.store(true, std::sync::atomic::Ordering::SeqCst);

    let response = reqwest::blocking::Client::new()
        .get(&url)
        .header(reqwest::header::RANGE, format!("bytes=0-{}", CHUNK * 2 - 1))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let headers = response.headers().clone();
    assert_eq!(
        header(&headers, "content-range"),
        Some(format!("bytes {}-{}/{ORIGIN_LENGTH}", CHUNK, CHUNK * 2 - 1)).as_deref(),
        "the origin's own answer, not one that claims the cached head is in front of it"
    );
    assert_eq!(
        header(&headers, "etag"),
        Some("\"v2\""),
        "and the validator of the entity it came from"
    );
    let body = response.bytes()?;
    assert_eq!(body.len() as u64, CHUNK);
    assert_generation(&body, true, CHUNK as usize, "an ignored If-Range");

    drop(fixture.handle);
    Ok(())
}

/// The one way a cached head can still meet a playlist tail, and the guard
/// that keeps them apart.
///
/// The hit's classification asks about the type the *store* filed; the
/// fetch's asks about the URL the body came from, and a redirect to a
/// `.m3u8` is exactly the shape that turns the verdict over between the two
/// -- an extension-less URL at an indifferent origin (`application/octet-
/// stream`), an edge that hands the tail off to a playlist path. Nothing is
/// stale here: same entity, same validator, the `Content-Range` continues
/// the head to the byte. Only the verdict changed.
///
/// So the head is dropped and the origin's own `206` relayed -- a broken
/// read the player re-reads, which is the documented price -- rather than a
/// playlist spliced onto bytes that were never classified as one. It is also
/// the reason the refusal below has to name *which* condition failed: this
/// one fails none of the others.
#[test]
fn a_tail_that_turned_out_to_be_a_playlist_is_not_joined_to_the_head() -> anyhow::Result<()> {
    let body = playlist_of(4);
    let total = body.len();
    let origin = Origin::start_with(move |request: &Request, socket: &mut TcpStream| {
        let asked = request.range().and_then(|value| {
            let (first, last) = value.trim_start_matches("bytes=").split_once('-')?;
            Some((
                first.parse::<usize>().ok()?,
                if last.is_empty() {
                    total - 1
                } else {
                    last.parse::<usize>().ok()?
                },
            ))
        });
        // The tail is somewhere else, and that somewhere names a playlist.
        // The head is served where it was asked for.
        if !request.target().contains("/tail.m3u8") && asked.is_some_and(|(first, _)| first > 0) {
            let _ = socket.write_all(
                b"HTTP/1.1 302 Found\r\nLocation: /tail.m3u8\r\nContent-Length: 0\r\n\
                  Connection: close\r\n\r\n",
            );
            let _ = socket.flush();
            return;
        }
        let Some((first, last)) = asked else { return };
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\n\
                 Content-Type: application/octet-stream\r\nETag: \"the-stream\"\r\n\
                 Content-Range: bytes {first}-{last}/{total}\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                last - first + 1
            )
            .as_bytes(),
        );
        let _ = socket.write_all(&body[first..=last]);
        let _ = socket.flush();
    })?;
    let fixture = fixture_with(origin)?;
    let origin = format!("http://{}", fixture.origin.addr);
    let url = format!("{}/proxy/d={}/stream", fixture.base, encode(&origin));
    let client = reqwest::blocking::Client::new();

    // The head, filed under a type that is not a playlist's and at a URL
    // that names no extension.
    let warm = client
        .get(&url)
        .header(reqwest::header::RANGE, format!("bytes=0-{}", CHUNK * 2 - 1))
        .send()?;
    assert_eq!(warm.bytes()?.len() as u64, CHUNK * 2);
    fixture.origin.next_request();
    wait_for_chunks(&fixture, 2);

    let response = client
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        header(response.headers(), "content-range"),
        Some(format!("bytes {}-{}/{total}", CHUNK * 2, total - 1)).as_deref(),
        "the origin's own answer, not one claiming a head it never described is in front"
    );
    let served = response.bytes()?;
    assert_eq!(served.len() as u64, CHUNK * 2);
    assert_eq!(
        &served[..],
        &playlist_of(4)[(CHUNK * 2) as usize..],
        "the tail as the origin sent it"
    );

    assert_eq!(
        fixture.origin.next_request().range(),
        Some(format!("bytes={}-{}", CHUNK * 2, total - 1)).as_deref(),
        "nothing said this was a playlist until the redirect had been followed"
    );
    assert!(
        fixture
            .origin
            .next_request()
            .target()
            .contains("/tail.m3u8"),
        "and it was the URL the tail came from that said so"
    );
    assert_eq!(
        cached_chunks(&fixture).len(),
        2,
        "a playlist is not filed, so the store still holds only the head"
    );

    drop(fixture.handle);
    Ok(())
}

/// A cache hit and a cache miss classify the same request the same way.
///
/// `r=` is deliberately not in the cache key: it never reaches the origin,
/// so it cannot vary the bytes stored. What it *can* vary is the verdict on
/// what those bytes are -- `r=Content-Type:application/x-mpegURL` is what
/// stremio-core sends for an HLS stream, and it forces the playlist rewrite
/// over an origin that mislabels. A hit that answered before that
/// classification was reached served the very body the rewrite exists to
/// replace, so the same URL played through the proxy on a miss and bypassed
/// it on a hit -- which is what keeping `r=` out of the key promises does
/// not happen.
#[test]
fn a_cache_hit_is_classified_the_way_a_miss_is() -> anyhow::Result<()> {
    // A playlist the origin labels `video/mp4`, four whole chunks of it, so
    // the store keeps it and a request with no `Range` can be answered from
    // the store entire.
    let body = playlist_of(4);
    let origin = Origin::start_with(move |request: &Request, socket: &mut TcpStream| {
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nContent-Type: video/mp4\r\n\
                 ETag: \"the-list\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        );
        if request.line.starts_with("GET") {
            let _ = socket.write_all(&body);
        }
        let _ = socket.flush();
    })?;
    let fixture = fixture_with(origin)?;
    let origin = format!("http://{}", fixture.origin.addr);
    let client = reqwest::blocking::Client::new();
    let forced = encode("Content-Type:application/x-mpegURL");
    let proxied = |path: &str, force: bool| {
        if force {
            format!(
                "{}/proxy/d={}&r={forced}{path}",
                fixture.base,
                encode(&origin)
            )
        } else {
            format!("{}/proxy/d={}{path}", fixture.base, encode(&origin))
        }
    };

    // The miss: never fetched before, so it is classified as it is fetched.
    let missed = client.get(proxied("/cold.mp4", true)).send()?;
    fixture.origin.next_request();
    let missed_status = missed.status();
    let missed_ranges = header(missed.headers(), "accept-ranges").map(str::to_string);
    let missed_body = missed.text()?;
    assert!(
        missed_body.contains("/proxy/d="),
        "a forced mpegurl type is what makes this a playlist, and a playlist is rewritten"
    );

    // The hit: the same origin bytes, filled without `r=` and asked for with
    // it.
    let warm = client.get(proxied("/warm.mp4", false)).send()?;
    assert_eq!(warm.bytes()?.len() as u64, CHUNK * 4);
    fixture.origin.next_request();
    wait_for_chunks(&fixture, 4);

    let hit = client.get(proxied("/warm.mp4", true)).send()?;
    assert_eq!(hit.status(), missed_status);
    assert_eq!(
        header(hit.headers(), "accept-ranges").map(str::to_string),
        missed_ranges,
        "a rewritten body is not one to range into, however its bytes were found"
    );
    let hit_body = hit.text()?;
    assert!(
        hit_body.contains("/proxy/d="),
        "the store holds origin bytes; what is done with them is the same question \
         on a hit as on a fetch"
    );
    assert_eq!(
        hit_body.replace("/warm.mp4", "/cold.mp4"),
        missed_body,
        "and the same answer"
    );

    drop(fixture.handle);
    Ok(())
}

/// The same request, answered from an empty cache, from a half-filled one
/// and from a full one, must come back the same three times.
///
/// This is the previous test's question asked of the *partial* hit, and it
/// is the one the full hit's fix left open: a hit that held only the head of
/// the range skipped the classification entirely, narrowed the player's
/// `Range` down to what it did not hold, and -- the fetched tail being a
/// playlist -- had the cached head dropped by the stitch guard and the
/// origin's `206` relayed raw. Measured against the code before this test:
/// `200` and a rewritten playlist cold and fully cached, `206` and 512 KiB
/// of unrewritten origin bytes half cached, under a `Content-Range` naming a
/// range the player never asked for and with every segment line pointing
/// straight at the origin -- no `h=`, no `p=`. One request, three answers,
/// chosen by how much happened to be on disk.
#[test]
fn a_partly_held_range_is_classified_the_way_a_hit_and_a_miss_are() -> anyhow::Result<()> {
    // A playlist the origin labels `video/mp4` at a URL that names no
    // extension: nothing but `r=` says this is a playlist, which is the
    // whole point -- and four whole chunks of it, so the cache can hold
    // half.
    let body = playlist_of(4);
    let total = body.len();
    let origin = Origin::start_with(move |request: &Request, socket: &mut TcpStream| {
        let served = request.range().and_then(|value| {
            let (first, last) = value.trim_start_matches("bytes=").split_once('-')?;
            let first: usize = first.parse().ok()?;
            let last: usize = if last.is_empty() {
                total - 1
            } else {
                last.parse().ok()?
            };
            Some((first, last))
        });
        let (head, part) = match served {
            Some((first, last)) => (
                format!(
                    "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\n\
                     Content-Type: video/mp4\r\nETag: \"the-list\"\r\n\
                     Content-Range: bytes {first}-{last}/{total}\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n",
                    last - first + 1
                ),
                &body[first..=last],
            ),
            None => (
                format!(
                    "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nContent-Type: video/mp4\r\n\
                     ETag: \"the-list\"\r\nContent-Length: {total}\r\n\
                     Connection: close\r\n\r\n"
                ),
                &body[..],
            ),
        };
        let _ = socket.write_all(head.as_bytes());
        let _ = socket.write_all(part);
        let _ = socket.flush();
    })?;
    let fixture = fixture_with(origin)?;
    let origin = format!("http://{}", fixture.origin.addr);
    let client = reqwest::blocking::Client::new();
    let forced = encode("Content-Type:application/x-mpegURL");
    let proxied = |force: bool| {
        if force {
            format!(
                "{}/proxy/d={}&r={forced}/stream",
                fixture.base,
                encode(&origin)
            )
        } else {
            format!("{}/proxy/d={}/stream", fixture.base, encode(&origin))
        }
    };
    // What a player opens a stream with, and the one shape that reaches the
    // cache holding part of the range: an unranged request is answered only
    // from a complete entry.
    let whole = "bytes=0-";
    let answer = |force: bool| -> anyhow::Result<(String, Option<String>, String)> {
        let response = client
            .get(proxied(force))
            .header(reqwest::header::RANGE, whole)
            .send()?;
        let status = response.status().to_string();
        let ranges = header(response.headers(), "accept-ranges").map(str::to_string);
        Ok((status, ranges, response.text()?))
    };

    // Cold: nothing on disk, so the fetch classifies the body and the
    // rewrite is what the player gets.
    let cold = answer(true)?;
    assert_eq!(
        fixture.origin.next_request().range(),
        Some(whole),
        "and the player's own range went to the origin"
    );
    assert_eq!(cold.0, "200 OK", "a rewritten playlist is the whole body");
    assert_eq!(cold.1.as_deref(), Some("none"));
    assert!(
        cold.2.contains("/proxy/d="),
        "every segment line comes back through this server"
    );
    assert!(
        cached_chunks(&fixture).is_empty(),
        "and a playlist is not a body the cache keeps"
    );

    // Half of it on disk, filled by a request that did not force the type --
    // which is the second player of a stream, or the same one before the
    // addon's `r=` was in play.
    let warm = client
        .get(proxied(false))
        .header(reqwest::header::RANGE, format!("bytes=0-{}", CHUNK * 2 - 1))
        .send()?;
    assert_eq!(warm.bytes()?.len() as u64, CHUNK * 2);
    fixture.origin.next_request();
    wait_for_chunks(&fixture, 2);

    let half_cached = answer(true)?;
    assert_eq!(
        fixture.origin.next_request().range(),
        Some(whole),
        "a request whose body we would replace is not narrowed against a head \
         we are not going to send"
    );
    assert_eq!(
        half_cached, cold,
        "a partial hit answers what an empty cache answered"
    );

    // The rest of it on disk, and the same request once more.
    let rest = client
        .get(proxied(false))
        .header(reqwest::header::RANGE, whole)
        .send()?;
    assert_eq!(rest.bytes()?.len() as u64, CHUNK * 4);
    assert_eq!(
        fixture.origin.next_request().range(),
        Some(format!("bytes={}-{}", CHUNK * 2, CHUNK * 4 - 1)).as_deref(),
        "that fill is an ordinary narrowed fetch; nothing here changes it"
    );
    wait_for_chunks(&fixture, 4);

    let fully_cached = answer(true)?;
    assert_eq!(
        fixture.origin.next_request().range(),
        Some(whole),
        "the full hit steps aside the same way"
    );
    assert_eq!(
        fully_cached, cold,
        "and a full hit answers what an empty cache answered"
    );

    drop(fixture.handle);
    Ok(())
}

/// Two players reading the same stream at different offsets. Neither is in
/// the other's key -- `p=` is the client's name for its own player and is
/// never part of what the cache is filed under -- so what one of them fetched
/// answers the other.
#[test]
fn two_players_reading_one_stream_share_what_either_fetched() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let origin = format!("http://{}", fixture.origin.addr);
    let url = |token: &str| {
        format!(
            "{}/proxy/d={}&p={token}/movie.mp4",
            fixture.base,
            encode(&origin)
        )
    };
    let client = reqwest::blocking::Client::new();
    let head = format!("bytes=0-{}", CHUNK - 1);
    let middle = format!("bytes={}-{}", CHUNK * 2, CHUNK * 3 - 1);

    let one = client
        .get(url("one"))
        .header(reqwest::header::RANGE, &head)
        .send()?;
    assert_eq!(one.bytes()?.len() as u64, CHUNK);
    assert_eq!(fixture.origin.next_request().range(), Some(&*head));

    let two = client
        .get(url("two"))
        .header(reqwest::header::RANGE, &middle)
        .send()?;
    assert_eq!(two.bytes()?.len() as u64, CHUNK);
    assert_eq!(fixture.origin.next_request().range(), Some(&*middle));
    wait_for_chunks(&fixture, 2);

    // A third player, a third token, and neither range costs an origin
    // request.
    for range in [&head, &middle] {
        let response = client
            .get(url("three"))
            .header(reqwest::header::RANGE, range)
            .send()?;
        assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.bytes()?.len() as u64, CHUNK);
    }
    assert!(fixture.origin.was_asked_for_nothing_more());

    drop(fixture.handle);
    Ok(())
}

/// A body that stops part way through a chunk leaves nothing on disk that a
/// later read could take for a complete chunk.
///
/// The origin here promises a megabyte and writes a thousand bytes, which is
/// what a client that vanished mid-fill looks like from the writer's side:
/// the stream ends at a byte that is not a chunk boundary. A chunk is held in
/// memory until it is whole, so there is nothing to half-write and nothing to
/// sweep -- and the next read of those bytes goes back to the origin.
#[test]
fn a_body_that_stops_mid_chunk_leaves_no_chunk_to_serve() -> anyhow::Result<()> {
    let fixture = fixture_with(Origin::start_with(
        |_request: &Request, socket: &mut TcpStream| {
            let _ = socket.write_all(
                format!(
                    "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\n\
                 Content-Type: video/mp4\r\n\
                 Content-Range: bytes 0-{}/{ORIGIN_LENGTH}\r\n\
                 Content-Length: {ORIGIN_LENGTH}\r\nConnection: close\r\n\r\n",
                    ORIGIN_LENGTH - 1
                )
                .as_bytes(),
            );
            let _ = socket.write_all(&(0..1000).map(byte_at).collect::<Vec<u8>>());
            let _ = socket.flush();
        },
    )?)?;
    let origin = format!("http://{}", fixture.origin.addr);
    let url = format!("{}/proxy/d={}/movie.mp4", fixture.base, encode(&origin));
    let client = reqwest::blocking::Client::new();

    // The body breaks where the origin stopped writing; that is the point.
    let response = client
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let _ = response.bytes();
    fixture.origin.next_request();

    assert_eq!(
        cached_chunks(&fixture).len(),
        0,
        "a chunk is written whole or not at all, so a broken fill writes nothing"
    );
    let again = client
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    let _ = again.bytes();
    assert_eq!(
        fixture.origin.next_request().range(),
        Some("bytes=0-"),
        "and the next read of those bytes is a fetch, not a truncated hit"
    );

    drop(fixture.handle);
    Ok(())
}

/// Every reason the cache refuses to keep a response, **each proved by the
/// one thing that makes it fail**.
///
/// The origin serves the same cacheable megabyte at every path, and reads
/// the first path segment as the name of the one defect to introduce into
/// it: a `Cache-Control` directive, a content coding, a missing
/// `Accept-Ranges`, and so on. So every case here is paired with the same
/// response minus its defect, on a URL of its own, and the pair is the whole
/// assertion -- the defective one stores nothing, the control stores its four
/// chunks. A case that failed for some *other* reason would take its control
/// down with it.
///
/// That pairing is the point. The origin this test used to run against
/// emitted `Accept-Ranges: bytes` from one arm only, so six of its seven
/// paths were refused for a rule none of them was written to exercise and the
/// test passed with every one of those rules deleted.
///
/// The accounting is per entity and not a running total over the cache,
/// because the cache does not accumulate across URLs any more: each of these
/// requests opens a stream on a URL of its own, and opening one is what makes
/// the one before it disposable (`server::proxy_retention`). So what a pair
/// claims is that the defective response added no entity of its own and the
/// control's own directory holds its chunks -- which is the claim the running
/// total was standing in for.
///
/// The refusals are deliberately more than the letter of HTTP asks for. A
/// store that never revalidates cannot honour `no-cache` or a `max-age` of
/// zero any other way; an origin that has not said it answers ranges must not
/// have one answered out of the cache later; a body under a content coding is
/// not the body the framing headers a hit writes would describe; and an
/// entity the origin will not identify is one no later read could tell a
/// second generation of from the first.
#[test]
fn nothing_the_rules_refuse_is_cached() -> anyhow::Result<()> {
    let fixture = fixture_with(Origin::start_with(
        |request: &Request, socket: &mut TcpStream| {
            // Every body is a megabyte, so a response that *were* cached
            // would leave four whole chunks behind. A refusal that only
            // looked like one because the body was too short would prove
            // nothing.
            let body: Vec<u8> = (0..ORIGIN_LENGTH).map(byte_at).collect();
            let playlist = std::iter::once("#EXTM3U".to_string())
                .chain((0..40_000).map(|line| format!("# padding line {line}")))
                .collect::<Vec<_>>()
                .join("\n")
                .into_bytes();
            // `/<shape>/<defect>/<name>`: the shape of the response, the one
            // thing wrong with it, and a name to keep one case's cache entry
            // out of another's. Nothing else about the response varies.
            let mut segments = request.target().trim_start_matches('/').split('/');
            let shape = segments.next().unwrap_or_default().to_string();
            let defect = segments.next().unwrap_or_default().to_string();
            let mut content_type = "video/mp4".to_string();
            let mut body = body;
            let mut extra = String::new();
            let mut accept_ranges = true;
            let mut content_length = true;
            let mut etag = true;
            match defect.as_str() {
                "none" => {}
                "no-store" => extra.push_str("Cache-Control: no-store\r\n"),
                "no-cache" => extra.push_str("Cache-Control: no-cache\r\n"),
                "private" => extra.push_str("Cache-Control: private, max-age=600\r\n"),
                "max-age-0" => extra.push_str("Cache-Control: max-age=0\r\n"),
                "coded" => extra.push_str("Content-Encoding: gzip\r\n"),
                "playlist" => {
                    content_type = "application/x-mpegurl".to_string();
                    body = playlist;
                }
                "no-ranges" => accept_ranges = false,
                "no-length" => content_length = false,
                "no-validator" => etag = false,
                other => panic!("the test asked for a defect the origin does not serve: {other}"),
            }
            // A `206` a byte short of the whole entity. It is what the
            // playlist rule needs to be *reachable*: a playlist that is the
            // whole body is replaced by the rewrite, which never reaches the
            // cache at all, so the only response the rule itself decides is
            // the fragment the rewrite declines to touch.
            if shape == "fragment" {
                body.truncate(ORIGIN_LENGTH - 1);
            }
            let head = format!(
                "HTTP/1.1 {}\r\nContent-Type: {content_type}\r\n{extra}{}{}{}\
                 Connection: close\r\n\r\n",
                if shape == "fragment" {
                    format!(
                        "206 Partial Content\r\nContent-Range: bytes 0-{}/{ORIGIN_LENGTH}",
                        ORIGIN_LENGTH - 2
                    )
                } else {
                    "200 OK".to_string()
                },
                if accept_ranges {
                    "Accept-Ranges: bytes\r\n"
                } else {
                    ""
                },
                if etag {
                    format!("ETag: {ORIGIN_ETAG}\r\n")
                } else {
                    String::new()
                },
                if content_length {
                    format!("Content-Length: {}\r\n", body.len())
                } else {
                    // Close-delimited instead, which is a body whose length
                    // the origin never states.
                    String::new()
                },
            );
            let _ = socket.write_all(head.as_bytes());
            if request.line.starts_with("GET") {
                let _ = socket.write_all(&body);
            }
            let _ = socket.flush();
        },
    )?)?;
    let origin = format!("http://{}", fixture.origin.addr);
    let client = reqwest::blocking::Client::new();
    let proxied = |path: &str| format!("{}/proxy/d={}{path}", fixture.base, encode(&origin));

    // Each defect against the same response without it, in the same shape.
    // The entities the cache holds before the pair are what both halves are
    // read against: a directory that was not among them and holds chunks is
    // a response that was cached.
    for (shape, defect) in [
        ("whole", "no-store"),
        ("whole", "no-cache"),
        ("whole", "private"),
        ("whole", "max-age-0"),
        ("whole", "coded"),
        ("fragment", "playlist"),
        ("whole", "no-ranges"),
        ("whole", "no-length"),
        ("whole", "no-validator"),
    ] {
        // Three chunks in a fragment a byte short of the entity, four in a
        // whole one.
        let kept = if shape == "fragment" { 3 } else { 4 };
        settled(&fixture);
        let before = cached_entities(&fixture);
        let response = client
            .get(proxied(&format!("/{shape}/{defect}/film.mp4")))
            .send()?;
        assert!(response.status().is_success(), "{defect}");
        assert!(!response.bytes()?.is_empty(), "{defect}");
        fixture.origin.next_request();
        settled(&fixture);
        assert_nothing_new_is_cached(&fixture, &before, &format!("{defect} must not be cached"));

        // The same response, the same shape, the same length, the same
        // everything but the defect -- and it is kept. So the assertion
        // above is about the rule and not about the origin, the body or the
        // fixture.
        let control = client
            .get(proxied(&format!("/{shape}/none/{defect}.mp4")))
            .send()?;
        assert!(!control.bytes()?.is_empty(), "{defect} control");
        fixture.origin.next_request();
        let entity = wait_for_new_entity(&fixture, &before, kept);
        assert_eq!(
            cached_chunks_by_entity(&fixture).get(&entity).copied(),
            Some(kept),
            "{defect} control caches its chunks and nothing else"
        );
    }

    // The `long-type` rule has no case here and cannot have one. A content
    // type past what one directory name holds is refused
    // (`proxy_cache::can_be_filed`), and if it were not, every chunk write of
    // that response would fail its own `mkdir` -- so nothing is cached either
    // way and no origin can tell the two apart. What the rule buys is that
    // the failure is a decision rather than a line in the log per chunk, and
    // it is pinned where that is visible: beside the function, in
    // `proxy_cache`'s own tests.
    //
    // And the refusals that are about the *request* rather than the
    // response. What each of them refuses is an **entry**, not a response, so
    // "nothing new was cached" does not isolate any of them -- a `HEAD` has
    // no body to store whatever the rule says. What does isolate them is the
    // other half of an entry: the read. Each entity below is filled by an
    // ordinary `GET` first, and the refused request is then made against a
    // store that holds the whole of it. A rule that stopped working would
    // answer that request off disk, and the origin would never hear it.
    //
    // Each case is finished before the next one is filled, because filling
    // the next is what makes this one's chunks disposable.
    //
    // A `HEAD` describes a body it does not carry; an `If-Range` is a
    // conditional, and answering one out of a store that never revalidates
    // would be inventing the condition's answer; an `h=` naming a credential
    // is refused outright rather than keyed, since `/proxy` takes no bearer
    // token of its own and an entry one caller's secret filled is one any
    // other caller could name.
    //
    // Two claims per case, and the second is the one the running total used
    // to carry: the filled entity is intact *and* nothing outside it holds a
    // chunk. A refused request answered from disk would fail the first; one
    // that quietly filed an entry of its own -- under the framing of a body
    // it never carried, which is a directory of its own -- would fail the
    // second and nothing else here would notice it.
    //
    // `fill` says which entity it filled and which entities may hold a chunk
    // after it: what was there before, plus the one it made.
    let fill = |path: &str| -> anyhow::Result<(PathBuf, std::collections::BTreeSet<PathBuf>)> {
        settled(&fixture);
        let before = cached_entities(&fixture);
        let response = client.get(proxied(path)).send()?;
        assert_eq!(response.bytes()?.len(), ORIGIN_LENGTH);
        fixture.origin.next_request();
        let entity = wait_for_new_entity(&fixture, &before, 4);
        let mut known = before;
        known.insert(entity.clone());
        Ok((entity, known))
    };

    let (filled, known) = fill("/whole/none/head.mp4")?;
    let head = client.head(proxied("/whole/none/head.mp4")).send()?;
    assert_eq!(head.status(), reqwest::StatusCode::OK);
    assert!(
        head.bytes()?.is_empty(),
        "a HEAD carries no body, from the cache or from anywhere else"
    );
    assert!(
        fixture.origin.next_request().line.starts_with("HEAD"),
        "a HEAD has no body to keep, so it has no entry to read from either"
    );
    settled(&fixture);
    assert_nothing_new_is_cached(&fixture, &known, "a HEAD must file no entry of its own");
    assert_eq!(
        cached_chunks_by_entity(&fixture).get(&filled).copied(),
        Some(4),
        "and it left the entity the GET filled exactly as it found it"
    );

    // A request with no `Range` is answered from the cache only when the
    // whole entity is there -- which for this one it now is, and it is still
    // the stream being played, so nothing has taken it.
    let response = client.get(proxied("/whole/none/head.mp4")).send()?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        header(response.headers(), "content-length"),
        Some(ORIGIN_LENGTH.to_string()).as_deref()
    );
    assert_eq!(
        header(response.headers(), "etag"),
        Some(ORIGIN_ETAG),
        "and labelled with the validator the entity is filed under"
    );
    assert_eq!(response.bytes()?.len(), ORIGIN_LENGTH);
    assert!(fixture.origin.was_asked_for_nothing_more());

    let (filled, known) = fill("/whole/none/conditional.mp4")?;
    let conditional = client
        .get(proxied("/whole/none/conditional.mp4"))
        .header(reqwest::header::RANGE, "bytes=0-")
        .header(reqwest::header::IF_RANGE, ORIGIN_ETAG)
        .send()?;
    assert_eq!(conditional.bytes()?.len(), ORIGIN_LENGTH);
    assert!(
        fixture.origin.next_request().header("if-range").is_some(),
        "a conditional cannot be answered by a store that never revalidates"
    );
    settled(&fixture);
    assert_nothing_new_is_cached(
        &fixture,
        &known,
        "an If-Range must file no entry of its own",
    );
    assert_eq!(
        cached_chunks_by_entity(&fixture).get(&filled).copied(),
        Some(4),
        "and it left the entity the GET filled exactly as it found it"
    );

    let before = cached_entities(&fixture);
    fill("/whole/none/authenticated.mp4")?;
    let authenticated = format!(
        "{}/proxy/d={}&h={}/whole/none/authenticated.mp4",
        fixture.base,
        encode(&origin),
        encode("Authorization:Bearer s3cret")
    );
    let response = client.get(&authenticated).send()?;
    assert_eq!(response.bytes()?.len(), ORIGIN_LENGTH);
    assert_eq!(
        fixture.origin.next_request().header("authorization"),
        Some("Bearer s3cret"),
        "the credential still travels; it is the entry that is refused, both \
         the writing of one and the reading of one"
    );
    settled(&fixture);
    let mut credentialled = cached_entities(&fixture);
    credentialled.retain(|entity| !before.contains(entity));
    assert_eq!(
        credentialled.len(),
        1,
        "the plain GET's entity, and no second one keyed by the credential"
    );

    drop(fixture.handle);
    Ok(())
}

/// **What `POST /cache/clean` takes is the slack, and only the slack.**
///
/// Cached proxy bytes are ordinary cache and a proxied stream the viewer
/// has left is the first thing that should go -- a clean is one of the four
/// things that takes it, beside the tick, the switch and the running-low
/// bell. What it may not take is the entity being played. There is no age
/// rule and no size rule left to weigh the two against each other: the one
/// stream somebody is inside keeps its window however far over the cap the
/// cache is, and what the report says about that is `over_limit`.
///
/// Which of the two deleters got there first is deliberately not asserted.
/// Opening the second stream is itself what makes the first disposable, so
/// the switch task may have taken those chunks before the clean was asked
/// -- it is the same pass over the same entity under the same turn, and a
/// test that pinned the winner would be pinning a race rather than a rule.
/// What is asserted is the state a client sees when the clean answers.
#[test]
fn cleaning_now_takes_the_proxied_stream_the_viewer_left_and_not_the_one_playing()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let origin = format!("http://{}", fixture.origin.addr);
    let proxied = |path: &str| format!("{}/proxy/d={}{path}", fixture.base, encode(&origin));
    let client = reqwest::blocking::Client::new();

    let response = client
        .get(proxied("/left.mp4"))
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    assert_eq!(response.bytes()?.len(), ORIGIN_LENGTH);
    fixture.origin.next_request();
    let left = wait_for_new_entity(&fixture, &Default::default(), 4);
    // And nobody is reading it any more. The client has every byte, but the
    // read that delivered them lives until hyper drops the response, and
    // while it does those bytes are promised to a body in flight -- which
    // is true, and is the subject of another test.
    nothing_is_reading(&fixture);

    // The viewer opens something else, which is the whole of what makes the
    // first one disposable.
    let response = client
        .get(proxied("/playing.mp4"))
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    assert_eq!(response.bytes()?.len(), ORIGIN_LENGTH);
    fixture.origin.next_request();
    let playing = wait_for_new_entity(
        &fixture,
        &std::collections::BTreeSet::from([left.clone()]),
        4,
    );
    nothing_is_reading(&fixture);

    // A cap under what is cached, so there is something for a clean to be
    // unable to get under. Well above one chunk, so nothing here is about a
    // single file being bigger than the whole cap.
    fixture
        .handle
        .update_settings(serde_json::json!({ "cacheSize": (CHUNK as f64) * 1.5 }))?;
    let report = fixture.handle.clean_cache_now()?;
    settled(&fixture);

    let held = cached_chunks_by_entity(&fixture);
    assert_eq!(
        held.get(&left),
        None,
        "every chunk of the stream the viewer left is gone: {held:?} ({report:?})"
    );
    assert_eq!(
        held.get(&playing).copied(),
        Some(4),
        "and the one being played kept its window: {held:?} ({report:?})"
    );
    assert!(
        report.over_limit > 0,
        "so the clean says it is still over the cap rather than taking what is playing: {report:?}"
    );

    drop(fixture.handle);
    Ok(())
}

/// **`GET /cache.json` counts the proxy's chunks as the fill books them,
/// and names what the stream being played keeps -- with nothing having
/// walked the tree.**
///
/// The figure behind that route used to be an eviction pass's walk of the
/// whole cache root, so it was as old as the last pass and absent before
/// the first: a client's "Storage" screen read 0 for the minutes a device
/// with sixteen thousand files takes to finish one. The two things that put
/// bytes in this cache count them as they land, so the answer is current
/// and costs no `statx`.
#[test]
fn the_cache_figure_follows_the_chunks_the_proxy_wrote() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let empty = fixture.handle.cache_usage()?;
    assert_eq!(
        empty.total_bytes, 0,
        "nothing has been relayed and no torrent holds anything: {empty:?}"
    );
    assert_eq!(empty.protected_bytes, 0);

    let origin = format!("http://{}", fixture.origin.addr);
    let url = format!("{}/proxy/d={}/movie.mp4", fixture.base, encode(&origin));
    let response = reqwest::blocking::Client::new()
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    assert_eq!(response.bytes()?.len(), ORIGIN_LENGTH);
    fixture.origin.next_request();
    wait_for_chunks(&fixture, 4);
    nothing_is_reading(&fixture);

    let cached = cached_chunks(&fixture).len() as u64;
    let usage = fixture.handle.cache_usage()?;
    assert_eq!(
        usage.total_bytes,
        cached * CHUNK,
        "every chunk the fill wrote, and no walk of the root to find them: {usage:?}"
    );
    // Nothing is bounding this entity -- the volume this test runs on is
    // roomier than a megabyte -- and it is the stream being played, so no
    // pass may take any of it and the whole of it is protected.
    assert_eq!(
        (usage.protected_bytes, usage.protected_files),
        (usage.total_bytes, 1),
        "the one entity a player is inside: {usage:?}"
    );

    // **And the figure is the count, not a reading of the tree.** A chunk
    // put into the entity's own directory by hand is a chunk no fill
    // booked: a walk would find it, the count cannot, and which of the two
    // answers this route gives is the whole of this slice. (A count can
    // stand for the tree only because the launch sweep empties the cache
    // before anything is served -- see `proxy_cache::sweep`.)
    let bucket = cached_chunks(&fixture)
        .first()
        .and_then(|chunk| chunk.parent().map(std::path::Path::to_path_buf))
        .expect("a bucket directory the fill made");
    std::fs::write(bucket.join("900"), vec![3u8; CHUNK as usize])?;
    assert_eq!(
        fixture.handle.cache_usage()?.total_bytes,
        usage.total_bytes,
        "nothing here walks the root, so a chunk nothing booked is not in it"
    );
    std::fs::remove_file(bucket.join("900"))?;

    drop(fixture.handle);
    Ok(())
}

/// **A chunk a clean unlinks comes off the count that booked it, and a
/// byte no owner ever booked is neither counted nor taken.**
///
/// A count that never heard the deletions would keep those bytes booked for
/// the life of the process -- and the cap this process publishes is
/// `occupied + available - floor`, so an over-counted occupancy states a
/// *larger* cap, the windows are sized to it, the cache refills past the
/// floor and the next pass has more to take. There is no term in that loop
/// that brings the count back down.
///
/// The whole-file download an earlier version of this server left under the
/// same root is the other half. It belongs to no owner: nothing here booked
/// it, so it is in no count, and a clean that is the owners' own passes has
/// no way to reach it and takes nothing off any count for it. Nothing old
/// matters -- what bounds this cache is what this process wrote.
#[test]
fn the_count_hears_the_clean_and_ignores_what_no_owner_booked() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let legacy = fixture
        .cache_root
        .path()
        .join("cache")
        .join("rqbit-downloads")
        .join("Leftover")
        .join("old.mkv");
    std::fs::create_dir_all(legacy.parent().expect("a parent"))?;
    std::fs::write(&legacy, vec![9u8; 64 * 1024])?;

    let origin = format!("http://{}", fixture.origin.addr);
    let proxied = |path: &str| format!("{}/proxy/d={}{path}", fixture.base, encode(&origin));
    let client = reqwest::blocking::Client::new();

    let response = client
        .get(proxied("/left.mp4"))
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    assert_eq!(response.bytes()?.len(), ORIGIN_LENGTH);
    fixture.origin.next_request();
    // Both readings taken once the cache has stopped moving, and in that
    // order: the usage figure is what the fills booked, and counting the
    // files first compares a disk that is still being written to against a
    // count that has heard every write. On this box every chunk had landed
    // by then anyway; on a Windows runner one had, and the two figures were
    // a megabyte apart.
    nothing_is_reading(&fixture);
    settled(&fixture);
    let cached = cached_chunks(&fixture).len() as u64;
    assert_eq!(
        fixture.handle.cache_usage()?.total_bytes,
        cached * CHUNK,
        "what the fill booked, before anything has taken any of it -- and \
         not the legacy file, which no owner booked"
    );

    // The viewer opens something else, so the first entity is disposable,
    // and a clean takes it.
    let response = client
        .get(proxied("/playing.mp4"))
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    assert_eq!(response.bytes()?.len(), ORIGIN_LENGTH);
    fixture.origin.next_request();
    wait_for_new_entity(&fixture, &Default::default(), 4);
    nothing_is_reading(&fixture);
    fixture.handle.clean_cache_now()?;
    settled(&fixture);

    let left = cached_chunks(&fixture).len() as u64;
    assert!(
        left < cached * 2,
        "the clean really unlinked the entity the viewer left: {left} chunks left"
    );
    assert_eq!(
        fixture.handle.cache_usage()?.total_bytes,
        left * CHUNK,
        "and the count is what the disk holds, not what it held before"
    );
    assert!(
        legacy.exists(),
        "the legacy copy is nobody's: no owner booked it and no owner takes it"
    );

    drop(fixture.handle);
    Ok(())
}

/// **What empties a proxied stream's cache is another one being opened.**
///
/// Not a clock, which is what it used to be: an entity nothing was reading
/// was forgotten ninety seconds after its last delivered byte and its chunks
/// became the cache cleaner's to find, whenever its walk got round to
/// them. A viewer who pauses for an hour has not
/// stopped playing, and a viewer who opens something else has stopped playing
/// whatever the clock says -- so the cache keeps the one stream being played
/// and drops what was left, at the moment it is left.
///
/// This runs through the whole server, which is the point of it being here:
/// the cell the proxy writes when a body opens is the engine's own
/// (`AppState::new_with_shared_settings_and_log_dir`), and what wakes on it
/// is the switch task in `server::serve`. Either of those wired to a cell of
/// its own would leave the first stream's chunks on the disk for ever.
#[test]
fn opening_another_proxied_stream_takes_the_one_it_left_off_the_disk() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let origin = format!("http://{}", fixture.origin.addr);
    let client = reqwest::blocking::Client::new();
    let proxied = |path: &str| format!("{}/proxy/d={}{path}", fixture.base, encode(&origin));

    let first = client.get(proxied("/first.mp4")).send()?;
    assert_eq!(first.bytes()?.len(), ORIGIN_LENGTH);
    fixture.origin.next_request();
    let left = wait_for_new_entity(&fixture, &Default::default(), 4);
    // The body has to be over before the switch, because a body still being
    // delivered keeps the bytes it was framed round however long ago the
    // viewer left: what the switch drops is slack, and an open read is not.
    nothing_is_reading(&fixture);

    // A second later or an hour later -- there is no clock in this -- the
    // viewer opens something else.
    let second = client.get(proxied("/second.mp4")).send()?;
    assert_eq!(second.bytes()?.len(), ORIGIN_LENGTH);
    fixture.origin.next_request();
    let playing = wait_for_new_entity(
        &fixture,
        &std::collections::BTreeSet::from([left.clone()]),
        4,
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if !cached_chunks_by_entity(&fixture).contains_key(&left) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let held = cached_chunks_by_entity(&fixture);
    assert_eq!(
        held.get(&left),
        None,
        "every chunk of the stream the viewer left is gone: {held:?}"
    );
    assert_eq!(
        held.get(&playing).copied(),
        Some(4),
        "and the one being played is untouched"
    );

    drop(fixture.handle);
    Ok(())
}

/// A cache big enough that the tests below are about the window and not
/// about the file: 128 chunks of origin against 32 chunks of budget, so the
/// budget covers a quarter of it and the policy really splits.
const RETENTION_ORIGIN: usize = 32 * 1024 * 1024;
/// 32 chunks. A proxied stream shares nothing, so the whole of it is window.
const RETENTION_BUDGET: u64 = 8 * 1024 * 1024;

/// Configure `bytes` as the cache size, which is the whole of what bounds
/// the streams below.
///
/// **Nothing here runs a pass.** Stating the budget is a publication and
/// not a deletion -- `update_settings` publishes it, because `cacheSize` is
/// half of what the cap is made of and a client that changes it has changed
/// the cap (`server::cache_budget`). Sweeping the volume here first would
/// make these tests pass just as well with the publication back where it
/// was, and a stream started before the first pass -- or between the change
/// and the next pass, which is a minute after the last write at best --
/// would be bounded by nothing at all.
///
/// The reading is `GET /cache.json`'s, which is the same arithmetic
/// (`CacheLimit::effective`) and evicts nothing: it says the volume this
/// test runs on is roomy enough that `cacheSize` is the smaller of the two,
/// so a volume too full to give the configured cap fails the test loudly
/// instead of quietly making it prove nothing.
fn published_budget(fixture: &Fixture, bytes: u64) -> anyhow::Result<()> {
    fixture
        .handle
        .update_settings(serde_json::json!({ "cacheSize": bytes as f64 }))?;
    assert_eq!(
        fixture.handle.cache_usage()?.limit_bytes,
        Some(bytes),
        "a different cap is in force than the one configured; \
         the volume this test runs on cannot give {bytes} bytes"
    );
    Ok(())
}

/// Wait until the cache has stopped moving.
///
/// A chunk is written from a task nobody joins and a retention pass runs on
/// another, so a listing of the cache root taken the moment a body ends is a
/// listing of a directory more chunks are still landing in and a pass is
/// still deleting from. Two counts either side of a pass are then counts of
/// two different caches, which is how the test below came to report *more*
/// chunks after a reclaim than before it -- a number that cannot be the pass
/// having taken anything, and the sign that the count was never measuring
/// the window at all.
///
/// Called after a body has been read to its end, this is a real quiescence
/// and not a guess: no byte of that stream is delivered afterwards, so
/// nothing can start a write or a pass the wait has not already seen
/// (`proxy_cache::DiskWork`). Bounded, and generously, because the bound is
/// not the assertion.
fn settled(fixture: &Fixture) {
    fixture
        .handle
        .proxy_cache_settled(std::time::Duration::from_secs(60))
        .expect("the proxy cache stops writing");
}

/// Wait until no body is open on the proxy cache.
///
/// A read holds its window and its promise until it is dropped, which is
/// after the last byte of it has reached the player -- so a test that has
/// just read a body to its end and asks what is disposable is asking while
/// somebody is still inside those bytes. Bounded so a
/// regression fails instead of hanging.
fn nothing_is_reading(fixture: &Fixture) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        if fixture.handle.proxy_cache_reads() == 0 {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!(
        "{} reads of the proxy cache never ended",
        fixture.handle.proxy_cache_reads()
    );
}

/// The cache holds no more than `chunks` of them, once it has stopped
/// moving -- so the reclaim really has happened by the time this returns,
/// rather than being given ten seconds to.
fn holds_no_more_than(fixture: &Fixture, chunks: usize) {
    settled(fixture);
    let held = cached_chunks(fixture).len();
    assert!(
        held <= chunks,
        "the cache never came down to {chunks} chunks; it holds {held}"
    );
}

/// [`holds_no_more_than`], for a body that has just been read to its end.
///
/// **A pass is armed by a byte reaching a player**, so the chunks that land
/// after the last pass of a play-through concluded are still on the disk
/// when the body ends: there is no byte left to arm the pass that would take
/// them. That tail is a real property and not a fault -- it goes at the next
/// byte, at the next stream, or at the next boot -- but its *size* is whatever
/// the last pass happened to be behind by, which is a property of the
/// machine: measured here, a run that leaves 32 chunks on Linux left 73 on a
/// Windows runner, where the unlinks are slower and the fill outruns them
/// further. A bound asserted on it is a bound on the runner.
///
/// So one byte is read first, from inside the window at the end of the film.
/// It is served from the cache, and it arms the pass that concludes at the
/// playhead playback really ended at; what that pass leaves is what the
/// bound is about.
fn holds_no_more_than_after_the_last_byte(fixture: &Fixture, url: &str, at: u64, chunks: usize) {
    // Settled *first*, and this is the whole of why: the byte below has to be
    // served from the cache, and the last chunk of a body that has just ended
    // may still be on its way to the disk. Asked too early it misses, goes to
    // the origin, and the next `next_request()` in the test gets this probe
    // instead of the fetch it was waiting for -- which is how this helper
    // broke `a_second_player_fetching_does_not_truncate_the_first_ones_read`
    // on a Windows runner while passing here.
    settled(fixture);
    let byte = reqwest::blocking::Client::new()
        .get(url)
        .header(reqwest::header::RANGE, format!("bytes={at}-{at}"))
        .send()
        .expect("the cache answers a byte it holds");
    assert_eq!(byte.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(byte.bytes().expect("the byte").len(), 1);
    // And nothing is asserted here about the origin, though the byte missing
    // the cache is exactly what this helper must not do. `Origin`'s record of
    // what it was asked is a queue, and both ways of looking at it --
    // `next_request` and `was_asked_for_nothing_more` -- take an entry off:
    // a helper that looked would decide what the test sees next. The settle
    // above is what keeps the byte a cache hit, and a caller that counts the
    // origin's requests is what says so when it is not.
    //
    // Settled again, for the pass the byte armed.
    holds_no_more_than(fixture, chunks);
}

/// **The bound, on the other kind of stream.**
///
/// A proxied response streamed end to end, well past a cache budget it does
/// not fit in, never has more on disk than that budget -- and plays: every
/// byte the player is handed is the byte at that offset of the origin's
/// file.
///
/// This is the same proof `enginefs::backend::librqbit`'s
/// `a_stream_past_the_cache_budget_stays_under_it_and_still_plays` makes for
/// a torrent, and it is the same policy making it true. Before the playhead
/// existed there was nothing here for a window to follow: the only thing
/// between a proxied stream and a full disk was the cache cleaner, which
/// walked the volume a minute after the last write at best, and a stream at
/// 20 MB/s writes a gigabyte in that minute.
///
/// Measured, not asserted about: the occupancy is the chunk files really on
/// the disk, counted by walking the cache root while the body is being read.
#[test]
fn a_proxied_stream_past_the_cache_budget_stays_under_it_and_still_plays() -> anyhow::Result<()> {
    use std::io::Read;

    let fixture = fixture_with(Origin::start_sized(RETENTION_ORIGIN)?)?;
    published_budget(&fixture, RETENTION_BUDGET)?;

    let origin = format!("http://{}", fixture.origin.addr);
    let url = format!("{}/proxy/d={}/movie.mp4", fixture.base, encode(&origin));
    let mut response = reqwest::blocking::Client::new()
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);

    // The window's own overshoot, and no more: a pass runs when the playhead
    // has moved a twentieth of a window, and the chunks written between two
    // passes are still on the disk when the first of them measures it. Twice
    // the budget is generous about that and still an order under the 32 MiB
    // going past.
    let bound = (2 * RETENTION_BUDGET / CHUNK) as usize;
    let mut read = 0usize;
    let mut worst = 0usize;
    let mut measured_at = 0usize;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = response.read(&mut buf)?;
        if n == 0 {
            break;
        }
        for (i, byte) in buf[..n].iter().enumerate() {
            assert_eq!(
                *byte,
                byte_at(read + i),
                "the byte at {} is not the origin's",
                read + i
            );
        }
        read += n;
        if read - measured_at >= 1024 * 1024 {
            measured_at = read;
            let held = cached_chunks(&fixture).len();
            worst = worst.max(held);
            assert!(
                held <= bound,
                "{held} chunks on disk after {read} bytes; the budget is {} chunks",
                RETENTION_BUDGET / CHUNK
            );
        }
    }
    assert_eq!(read, RETENTION_ORIGIN, "and it played to the end");

    // And the cache really filled, or the bound above proves nothing: a
    // stream that never reached the budget would satisfy it by having
    // written almost nothing.
    assert!(
        worst as u64 * CHUNK > RETENTION_BUDGET / 2,
        "the cache never filled ({worst} chunks at most)"
    );
    assert!(
        (read as u64) >= RETENTION_BUDGET * 2,
        "and the stream ran well past the budget"
    );

    drop(fixture.handle);
    Ok(())
}

/// **A stream relayed before anything has walked the cache is still
/// bounded.**
///
/// Every other test here states the budget by changing a setting, which is
/// a client acting on a running server. This one has no client and no pass:
/// the `cacheSize` is on the disk before the process starts and nothing
/// calls `POST /settings`. So the only thing that
/// can have stated a budget is the process's own publisher
/// (`server::cache_budget::start`), and if it has not, the budget is
/// `CacheBudget::Unknown`, which installs no retention policy at all -- and
/// all 32 MiB of the origin stays on the disk.
///
/// That window is the reason the publisher exists. It used to be the tail
/// of an eviction pass, so a device with sixteen thousand cache files on
/// eMMC had no budget until the first walk of the root finished, minutes
/// in, and a player is inside the first stream long before that.
#[test]
fn a_stream_relayed_before_anything_has_walked_the_cache_is_still_bounded() -> anyhow::Result<()> {
    use std::io::Read;

    // Before the first request, as `fixture_with` does: this test starts its
    // own server, so it is the ordering point for its own binary.
    enginefs::http_client::trust_roots_for_tests(vec![TEST_CA.pem.clone()]);
    let origin = Origin::start_sized(RETENTION_ORIGIN)?;
    let config_dir = tempfile::tempdir()?;
    let cache_root = tempfile::tempdir()?;
    let config = config_dir.path().join("config");
    std::fs::create_dir_all(&config)?;
    // Configuration the process reads at start, not a change made to a
    // running one. `cacheRoot` is left empty on purpose: the loader fills an
    // empty one in from `ServerConfig::cache_dir`, whereas the default is
    // this machine's real cache directory, which no test may write to.
    let settings = stream_server::ServerSettings {
        cache_root: String::new(),
        cache_size: Some(RETENTION_BUDGET as f64),
        ..Default::default()
    };
    std::fs::write(config.join("settings.json"), serde_json::to_vec(&settings)?)?;
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config),
        cache_dir: Some(cache_root.path().join("cache")),
        ..offline_config()
    })?;
    let fixture = Fixture {
        base: format!("http://{}", handle.http_addr()),
        handle,
        origin,
        cache_root,
        _config_dir: config_dir,
    };
    // The same guard `published_budget` makes: the arithmetic, not an
    // eviction, so a volume too full to give the configured cap fails the
    // test loudly instead of quietly making it prove nothing.
    assert_eq!(
        fixture.handle.cache_usage()?.limit_bytes,
        Some(RETENTION_BUDGET),
        "a different cap is in force than the one in the settings file; \
         the volume this test runs on cannot give {RETENTION_BUDGET} bytes"
    );

    let origin_url = format!("http://{}", fixture.origin.addr);
    let url = format!("{}/proxy/d={}/movie.mp4", fixture.base, encode(&origin_url));
    let mut response = reqwest::blocking::Client::new()
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);

    // Twice the budget, for the overshoot a pass that runs every twentieth
    // of a window leaves behind -- and still an order under the 32 MiB going
    // past.
    let bound = (2 * RETENTION_BUDGET / CHUNK) as usize;
    let mut read = 0usize;
    let mut worst = 0usize;
    let mut measured_at = 0usize;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = response.read(&mut buf)?;
        if n == 0 {
            break;
        }
        for (i, byte) in buf[..n].iter().enumerate() {
            assert_eq!(
                *byte,
                byte_at(read + i),
                "the byte at {} is not the origin's",
                read + i
            );
        }
        read += n;
        if read - measured_at >= 1024 * 1024 {
            measured_at = read;
            let held = cached_chunks(&fixture).len();
            worst = worst.max(held);
            assert!(
                held <= bound,
                "{held} chunks on disk after {read} bytes, with nothing having \
                 walked the cache; the budget is {} chunks",
                RETENTION_BUDGET / CHUNK
            );
        }
    }
    assert_eq!(read, RETENTION_ORIGIN, "and it played to the end");
    // And the cache really filled, or the bound above proves nothing.
    assert!(
        worst as u64 * CHUNK > RETENTION_BUDGET / 2,
        "the cache never filled ({worst} chunks at most)"
    );

    drop(fixture.handle);
    Ok(())
}

/// **What a playback panel is told about a proxied stream, end to end.**
///
/// A client holding the URL it handed its player asks one question and gets
/// the window round the playhead -- and it is the disk it gets, not the
/// policy's intentions: the two halves add up to the chunk files really in
/// the cache, counted here by walking the root. The sharing row is absent,
/// because a proxied response is not seeded and a row of zeroes would say
/// the opposite.
#[test]
fn a_panel_asking_about_a_proxied_stream_is_told_what_is_on_the_disk() -> anyhow::Result<()> {
    use std::io::Read;

    let fixture = fixture_with(Origin::start_sized(RETENTION_ORIGIN)?)?;
    published_budget(&fixture, RETENTION_BUDGET)?;

    let origin = format!("http://{}", fixture.origin.addr);
    let url = format!("{}/proxy/d={}/movie.mp4", fixture.base, encode(&origin));
    let mut response = reqwest::blocking::Client::new()
        .get(&url)
        .header(
            reqwest::header::RANGE,
            format!("bytes=0-{}", 64 * CHUNK - 1),
        )
        .send()?;
    let mut played = Vec::new();
    response.read_to_end(&mut played)?;
    assert_eq!(played.len() as u64, 64 * CHUNK, "the player read the lot");
    holds_no_more_than_after_the_last_byte(
        &fixture,
        &url,
        64 * CHUNK - 1,
        (RETENTION_BUDGET / CHUNK) as usize,
    );

    let numbers = fixture
        .handle
        .stream_numbers(&url)?
        .expect("this server is holding that stream");
    let window = numbers.window.expect("a bounded stream has a window");
    assert_eq!(
        window.behind_bytes + window.ahead_bytes,
        cached_chunks(&fixture).len() as u64 * CHUNK,
        "the two halves are the chunk files really on the disk"
    );
    assert!(
        window.behind_bytes > 0,
        "and a player two thirds through a film can scrub back into what it \
         has already played: {window:?}"
    );
    assert_eq!(
        numbers.sharing, None,
        "a proxied response is not seeded, so there is no sharing row"
    );

    // The same server, asked about streams it is not holding: not an error,
    // just no rows.
    assert_eq!(
        fixture.handle.stream_numbers(&format!(
            "{}/proxy/d={}/other-film.mp4",
            fixture.base,
            encode(&origin)
        ))?,
        None,
        "a proxied URL nothing has ever been read of"
    );
    assert_eq!(
        fixture
            .handle
            .stream_numbers(&format!("{}/{}/0", fixture.base, "f".repeat(40)))?,
        None,
        "a torrent this server has no engine for"
    );
    assert_eq!(
        fixture
            .handle
            .stream_numbers("file:///home/viewer/film.mkv")?,
        None,
        "and a stream that never went through this server at all"
    );

    drop(fixture.handle);
    Ok(())
}

/// **The behaviour the split was costing us.** A short seek back is answered
/// off the disk; a long one is not.
///
/// The window is roughly 90% ahead of the playhead and 10% behind it, and
/// the 10% is what a scan back is for -- a few seconds of rewind, a player
/// re-reading its container index. Under a budget that does not cover the
/// file, that region is the difference between a rewind the origin never
/// hears about and a rewind that costs a fresh fetch over the network.
///
/// Both halves are needed. Without the second one the first would pass on a
/// cache nothing had reclaimed at all, which would be a test of the origin
/// rather than of the policy.
#[test]
fn a_seek_back_inside_the_window_is_served_from_disk_and_one_outside_it_is_not()
-> anyhow::Result<()> {
    use std::io::Read;

    let fixture = fixture_with(Origin::start_sized(RETENTION_ORIGIN)?)?;
    published_budget(&fixture, RETENTION_BUDGET)?;

    let origin = format!("http://{}", fixture.origin.addr);
    let url = format!("{}/proxy/d={}/movie.mp4", fixture.base, encode(&origin));
    let client = reqwest::blocking::Client::new();

    // Play the first half and stop there, deliberately short of the end: a
    // window that has slid back to fit inside the file sits entirely behind
    // a playhead at the last byte, and every backward seek would be served
    // whatever the 10% said.
    const PLAYED_CHUNKS: u64 = 64;
    let mut response = client
        .get(&url)
        .header(
            reqwest::header::RANGE,
            format!("bytes=0-{}", PLAYED_CHUNKS * CHUNK - 1),
        )
        .send()?;
    let mut played = Vec::new();
    response.read_to_end(&mut played)?;
    assert_eq!(played.len() as u64, PLAYED_CHUNKS * CHUNK);
    assert_eq!(
        fixture.origin.next_request().range(),
        Some(format!("bytes=0-{}", PLAYED_CHUNKS * CHUNK - 1).as_str())
    );

    // The reclaim has caught up: what is left is about a window, not the
    // sixteen megabytes that went past.
    holds_no_more_than_after_the_last_byte(
        &fixture,
        &url,
        PLAYED_CHUNKS * CHUNK - 1,
        (2 * RETENTION_BUDGET / CHUNK) as usize,
    );
    assert!(
        fixture.origin.was_asked_for_nothing_more(),
        "reclaiming is not fetching"
    );

    // The scan back: four chunks behind the playhead, which is inside the
    // tenth of the window that sits behind it. A window with nothing behind
    // it puts this chunk outside, and the read below reaches the origin.
    let back = (PLAYED_CHUNKS - 4) * CHUNK;
    let response = client
        .get(&url)
        .header(
            reqwest::header::RANGE,
            format!("bytes={back}-{}", back + CHUNK - 1),
        )
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let body = response.bytes()?;
    assert_eq!(body.len() as u64, CHUNK);
    assert_eq!(
        body[0],
        byte_at(back as usize),
        "and it is the right offset"
    );
    assert!(
        fixture.origin.was_asked_for_nothing_more(),
        "a scan back inside the window is the cache's to answer"
    );

    // And the start of the film, which the window let go of long ago, is
    // not: that is the reclaim having really happened.
    let response = client
        .get(&url)
        .header(reqwest::header::RANGE, format!("bytes=0-{}", CHUNK - 1))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.bytes()?.len() as u64, CHUNK);
    assert_eq!(
        fixture.origin.next_request().range(),
        Some(format!("bytes=0-{}", CHUNK - 1).as_str()),
        "the window reclaimed the head of the file, so the origin is asked for it"
    );

    drop(fixture.handle);
    Ok(())
}

/// **What the window kept, a player may read back whole.**
///
/// A response is framed before its first byte goes out: the `Content-Length`
/// and the `Content-Range` a player is handed are a promise about bytes that
/// are still on the disk when it reads them. The window that keeps a run is
/// 90% ahead of the playhead and 10% behind it, so a body that starts at the
/// front of a run a whole window long has its own tail *outside* the window
/// the moment its first chunk goes out -- and the pass that its own reading
/// starts is what would unlink it. The player then gets a `206` of
/// 8 388 608 bytes that dies after 7 602 176 of them, which is exactly nine
/// tenths of the window, with `IncompleteBody`.
///
/// A torrent's reader is refused this by librqbit -- `drop_pieces` will not
/// forget a piece a reader is waiting on. A proxied read makes the refusal
/// for itself, and this is the whole of what it is for.
#[test]
fn the_run_the_window_kept_is_served_back_whole() -> anyhow::Result<()> {
    use std::io::Read;

    let fixture = fixture_with(Origin::start_sized(RETENTION_ORIGIN)?)?;
    published_budget(&fixture, RETENTION_BUDGET)?;

    let origin = format!("http://{}", fixture.origin.addr);
    let url = format!("{}/proxy/d={}/movie.mp4", fixture.base, encode(&origin));
    let client = reqwest::blocking::Client::new();

    // Play the whole film. What is left when the last byte has gone past is
    // the window round the end of it, which is a window's worth of
    // contiguous chunks -- the shape a player rewinding into the credits
    // asks for.
    let mut response = client
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    let mut played = Vec::new();
    response.read_to_end(&mut played)?;
    assert_eq!(played.len(), RETENTION_ORIGIN);
    fixture.origin.next_request();
    holds_no_more_than_after_the_last_byte(
        &fixture,
        &url,
        RETENTION_ORIGIN as u64 - 1,
        (2 * RETENTION_BUDGET / CHUNK) as usize,
    );

    let run = longest_cached_run(&fixture);
    assert!(
        run.end - run.start >= RETENTION_BUDGET / CHUNK,
        "the window kept a run to ask for: {run:?}"
    );
    let (first, last) = (run.start * CHUNK, run.end * CHUNK - 1);
    let want = last - first + 1;

    let mut response = client
        .get(&url)
        .header(reqwest::header::RANGE, format!("bytes={first}-{last}"))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        header(response.headers(), "content-length"),
        Some(want.to_string()).as_deref(),
        "the response promises the whole run"
    );
    let mut body = Vec::new();
    if let Err(error) = response.read_to_end(&mut body) {
        panic!(
            "the body broke after {} of {want} bytes: {error}",
            body.len()
        );
    }
    assert_eq!(body.len() as u64, want, "and delivers it");
    assert_eq!(body[0], byte_at(first as usize), "at the right offset");
    assert!(
        fixture.origin.was_asked_for_nothing_more(),
        "all of it came off the disk, which is what makes this the cache's promise"
    );

    drop(fixture.handle);
    Ok(())
}

/// **The second player in one entity is not the first one's read-ahead.**
///
/// `p=` is out of the cache key on purpose, so two players reading one
/// stream share its chunks and read it at two offsets. The window follows a
/// playhead, and there are two of them; what neither of them may do is
/// delete the bytes the other has already been promised. Here the second
/// player fetches the head of the film, which drags the window to the front
/// while the first player is still reading the run at the back off the disk.
#[test]
fn a_second_player_fetching_does_not_truncate_the_first_ones_read() -> anyhow::Result<()> {
    use std::io::Read;

    let fixture = fixture_with(Origin::start_sized(RETENTION_ORIGIN)?)?;
    published_budget(&fixture, RETENTION_BUDGET)?;

    let origin = format!("http://{}", fixture.origin.addr);
    let url = |token: &str| {
        format!(
            "{}/proxy/d={}&p={token}/movie.mp4",
            fixture.base,
            encode(&origin)
        )
    };
    let client = reqwest::blocking::Client::new();

    let mut response = client
        .get(url("one"))
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    let mut played = Vec::new();
    response.read_to_end(&mut played)?;
    assert_eq!(played.len(), RETENTION_ORIGIN);
    fixture.origin.next_request();
    holds_no_more_than_after_the_last_byte(
        &fixture,
        &url("one"),
        RETENTION_ORIGIN as u64 - 1,
        (2 * RETENTION_BUDGET / CHUNK) as usize,
    );

    // The first player asks for the run the window kept, and reads the first
    // chunk of it.
    let run = longest_cached_run(&fixture);
    let (first, last) = (run.start * CHUNK, run.end * CHUNK - 1);
    let want = last - first + 1;
    let mut one = client
        .get(url("one"))
        .header(reqwest::header::RANGE, format!("bytes={first}-{last}"))
        .send()?;
    let mut head = vec![0u8; CHUNK as usize];
    one.read_exact(&mut head)?;

    // The second player, at the other end of the film, on bytes the window
    // let go of long ago: its fetch is what moves the window away from the
    // body still being read above.
    let two = client
        .get(url("two"))
        .header(reqwest::header::RANGE, format!("bytes=0-{}", 8 * CHUNK - 1))
        .send()?;
    assert_eq!(two.bytes()?.len() as u64, 8 * CHUNK);
    assert_eq!(
        fixture.origin.next_request().range(),
        Some(format!("bytes=0-{}", 8 * CHUNK - 1).as_str()),
        "the head of the film was reclaimed, so this really went to the origin"
    );

    let mut rest = Vec::new();
    if let Err(error) = one.read_to_end(&mut rest) {
        panic!(
            "the first player's body broke after {} of {want} bytes: {error}",
            head.len() + rest.len()
        );
    }
    assert_eq!(
        (head.len() + rest.len()) as u64,
        want,
        "the first player was handed every byte its response promised"
    );
    assert_eq!(rest[0], byte_at((first + CHUNK) as usize));

    drop(fixture.handle);
    Ok(())
}

/// **A clean is refused the chunks a proxied player is inside.**
///
/// A torrent's reader is protected from a delete by librqbit: it goes
/// through `drop_pieces`, which will not forget a piece a reader is waiting
/// on. A proxied stream has no backend to refuse for it, so what protects
/// its bytes is the owner itself -- the entity being played is not slack,
/// and the window and the promises of a body in flight are what a pass may
/// not take. Before that was so, a pass under a tight cap unlinked the
/// chunk under the player's head and the player found out by failing
/// (`proxy_cache::Cached::body` ends the body in an error rather than
/// serving a hole).
///
/// So the cap is set below what one live window holds and the clean is run
/// on purpose. It leaves the chunks somebody is inside, and it says it
/// could not get under the cap -- which is the honest answer, and the same
/// one it gives for a torrent whose pieces the have-set will not give up.
/// That it takes what nobody is playing is the other half of the claim and
/// it has a test of its own above
/// (`cleaning_now_takes_the_proxied_stream_the_viewer_left_and_not_the_one_playing`).
///
/// **The chunks are named, and that is the whole point of the shape of this
/// test.** It used to count the files before the pass and after it and
/// assert the two numbers were equal, which cannot express this claim: a
/// count cannot tell a chunk a player is inside from one nobody is reading,
/// so an equality of counts passes when a pass takes a protected chunk
/// and happens to leave an unprotected one. It could not even be relied on
/// to fail honestly -- with chunks still landing while the first number was
/// taken, the count *rose* across a reclaim often enough to fail one run of
/// this binary in ten. What is asserted here instead is identity: these
/// files, named before the pass by the response the player was handed, are
/// on the disk after it. Whether the chunks outside the window went away is
/// a different claim, and it is asserted here only as a direction -- the
/// pass did not grow the disk -- never as an equality.
#[test]
fn a_clean_leaves_the_chunks_a_proxied_player_is_inside() -> anyhow::Result<()> {
    use std::io::Read;

    let fixture = fixture_with(Origin::start_sized(RETENTION_ORIGIN)?)?;
    published_budget(&fixture, RETENTION_BUDGET)?;

    let origin = format!("http://{}", fixture.origin.addr);
    let url = format!("{}/proxy/d={}/movie.mp4", fixture.base, encode(&origin));
    let client = reqwest::blocking::Client::new();

    // Play the film through. What is left when the last byte has gone past
    // is the window round the end of it, which is where the player is: a
    // player between two requests has no body open, and the entity stays
    // live, window and all, until a stream opens on something else.
    let mut response = client
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    let mut played = Vec::new();
    response.read_to_end(&mut played)?;
    assert_eq!(played.len(), RETENTION_ORIGIN);
    fixture.origin.next_request();

    // Nothing is on its way to the disk any more, so the chunks named below
    // are named out of a cache that has stopped moving rather than out of
    // one more chunks are still landing in.
    settled(&fixture);
    let kept = longest_cached_run(&fixture);
    assert_eq!(
        kept.end,
        RETENTION_ORIGIN as u64 / CHUNK,
        "the run the window kept ends in the chunk playback stopped in: {kept:?}"
    );
    assert!(
        kept.end - kept.start >= RETENTION_BUDGET / CHUNK,
        "and it is a window's worth of it: {kept:?}"
    );
    // The window, and not the whole run. The two are the same size only
    // when no chunk landed after the last retention pass -- and one that
    // does arms no pass of its own, because a pass is armed by a byte
    // reaching a player and there are no bytes left to deliver. So the run
    // can be wider than the window by whatever the final stride wrote, and
    // that surplus is the pass's to take: it is over the cap and nobody
    // is inside it. Asserting the whole run is protected would be asserting
    // that retention leaves no overshoot, which is a different claim, and
    // one this test would make only on the platforms where the last pass
    // happened to catch everything.
    let window = RETENTION_BUDGET / CHUNK;
    let inside: Vec<u64> = (kept.end.saturating_sub(window)..kept.end).collect();
    let before = cached_chunk_indices(&fixture);

    // A cap of one chunk: everything on the disk is over it, so the only
    // thing that can keep a byte is a window or a promise.
    fixture
        .handle
        .update_settings(serde_json::json!({ "cacheSize": CHUNK as f64 }))?;
    let report = fixture.handle.clean_cache_now()?;
    let left = cached_chunk_indices(&fixture);
    assert_eq!(
        taken_from(&inside, &left),
        Vec::<u64>::new(),
        "the clean took chunks the player is inside: {inside:?} were on the \
         disk and {left:?} are (the run it kept was {kept:?})"
    );
    assert!(
        report.over_limit > 0,
        "and it says so rather than pretending it got under the cap: {report:?}"
    );
    // A direction and not an equality: what a pass may take is a different
    // claim from what it is refused, and the two are not one number.
    assert!(
        left.len() <= before.len(),
        "the pass grew the disk: {before:?} before, {left:?} after"
    );

    // Now the player comes back for those bytes, and this time it is inside
    // them with a body open: the response is framed round the window, which
    // is a promise about chunks that have to still be there when it reads
    // them. The window and not the run, for the reason given above: the
    // run's surplus over the window was the pass's to take, and on the
    // platforms where the last pass left one it did take it -- so a request
    // for the whole run is a request for bytes this test has just agreed
    // may be gone, and the origin answering for them is not a failure of
    // anything.
    let (first, last) = (inside[0] * CHUNK, kept.end * CHUNK - 1);
    let mut player = client
        .get(&url)
        .header(reqwest::header::RANGE, format!("bytes={first}-{last}"))
        .send()?;
    assert_eq!(player.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let promised = content_range(player.headers());
    assert_eq!(
        promised,
        first..last + 1,
        "the whole window survived the pass above, so the body is framed \
         round all of it"
    );
    assert!(
        fixture.origin.was_asked_for_nothing_more(),
        "and every byte of it is the cache's to answer, which is what makes \
         this a window the player is inside rather than one being fetched"
    );
    let mut head = vec![0u8; CHUNK as usize];
    player.read_exact(&mut head)?;
    assert_eq!(head[0], byte_at(first as usize));
    // The read has delivered a chunk, so the pass its playhead armed is one
    // more thing on its way round the disk.
    settled(&fixture);

    // The clean goes round again with that body open. What it may not take
    // now is what the body has yet to deliver -- and *that* is asserted in
    // bytes rather than in file names, because the chunks still owed are not
    // the test's to name: the socket takes bytes ahead of the reader, and a
    // megabyte or three of this run has already been handed over by the time
    // the player has read its first chunk. What the player must be handed is
    // exactly what it was promised, and the read below is the whole of that
    // claim.
    let before = cached_chunk_indices(&fixture);
    let report = fixture.handle.clean_cache_now()?;
    let left = cached_chunk_indices(&fixture);
    assert!(report.over_limit > 0, "and it says so again: {report:?}");
    assert!(
        left.len() <= before.len(),
        "the pass grew the disk: {before:?} before, {left:?} after"
    );

    let mut rest = Vec::new();
    if let Err(error) = player.read_to_end(&mut rest) {
        panic!(
            "the player's body broke after {} of {} bytes: {error}",
            head.len() + rest.len(),
            promised.end - promised.start
        );
    }
    assert_eq!(
        (head.len() + rest.len()) as u64,
        promised.end - promised.start,
        "the player was handed every byte its response promised"
    );
    assert_eq!(rest[0], byte_at((first + CHUNK) as usize));

    drop(fixture.handle);
    Ok(())
}

/// Which of `named` are not in `left`: what a pass took of the chunks a test
/// named before it ran.
fn taken_from(named: &[u64], left: &[u64]) -> Vec<u64> {
    named
        .iter()
        .copied()
        .filter(|index| !left.contains(index))
        .collect()
}

/// The launch-time sweep, through a real restart.
///
/// It empties the proxy cache: the chunk a kill was writing and the chunk
/// beside it that was committed. Nothing here outlives a process, because a
/// proxied entity is kept only while something is playing it -- and a process
/// that has served nothing is playing nothing, so a chunk from the last run is
/// a byte no owner in this one would ever count or reclaim. The piece store's
/// sweep beside it keeps exactly one thing, the pin, for the same reason: a
/// pin is the only claim that was ever meant to survive a restart.
#[test]
fn a_restart_empties_the_proxy_cache() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_root = tempfile::tempdir()?;
    let start = || {
        stream_server::start(stream_server::ServerConfig {
            http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            config_dir: Some(config_dir.path().join("config")),
            cache_dir: Some(cache_root.path().join("cache")),
            ..offline_config()
        })
    };

    let handle = start()?;
    drop(handle);

    // What a process killed mid-write leaves: a chunk that was committed,
    // and one that was still being written to its temporary name.
    let bucket = cache_root
        .path()
        .join("cache")
        .join("rqbit-downloads")
        .join(".proxy")
        .join("0".repeat(64))
        .join(format!("{ORIGIN_LENGTH}_video%2Fmp4"))
        .join("0");
    std::fs::create_dir_all(&bucket)?;
    let committed = bucket.join("0");
    let killed = bucket.join("0.4242-0.part");
    std::fs::write(&committed, vec![0u8; 16])?;
    std::fs::write(&killed, vec![0u8; 16])?;

    let handle = start()?;
    assert!(!killed.exists(), "the temporary is gone");
    assert!(
        !committed.exists(),
        "and so is the committed chunk beside it: nothing here is being played"
    );
    assert!(
        !bucket.exists(),
        "the entity directory goes whole, not chunk by chunk"
    );

    drop(handle);
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

/// The certificate authority this test binary issues fixture certificates
/// from, generated once and registered with `enginefs`'s trust set by
/// [`fixture_with`].
///
/// The fixture used to ship a self-signed CA certificate and serve it as the
/// end entity, which rustls refuses whatever it is trusted as -- so the only
/// HTTPS origin these tests could build was an unverifiable one, and the
/// tests that needed a *working* chain got one only because `/proxy` silently
/// downgraded to accepting anything. That downgrade is gone, so the fixture
/// issues a real chain instead.
struct TestCa {
    certificate: rcgen::Certificate,
    key: rcgen::KeyPair,
    pem: Vec<u8>,
}

static TEST_CA: std::sync::LazyLock<TestCa> = std::sync::LazyLock::new(|| {
    let mut params =
        rcgen::CertificateParams::new(Vec::new()).expect("no subject names on a CA to reject");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.distinguished_name = {
        let mut name = rcgen::DistinguishedName::new();
        name.push(rcgen::DnType::CommonName, "stream-server test CA");
        name
    };
    let key = rcgen::KeyPair::generate().expect("a key for the test CA");
    let certificate = params.self_signed(&key).expect("a self-signed test CA");
    let pem = certificate.pem().into_bytes();
    TestCa {
        certificate,
        key,
        pem,
    }
});

/// A TLS origin whose chain verifies, issued by [`TEST_CA`] for the two names
/// a loopback listener is reached by. The PEMs live in a temporary directory
/// for exactly this listener, which binds loopback and serves one string.
struct TlsOrigin {
    addr: SocketAddr,
    _certificates: tempfile::TempDir,
}

impl TlsOrigin {
    fn start_with(app: axum::Router) -> anyhow::Result<Self> {
        let certificates = tempfile::tempdir()?;
        let cert = certificates.path().join("cert.pem");
        let key = certificates.path().join("key.pem");

        // Both spellings: the tests reach this listener as `localhost` and as
        // `127.0.0.1`, and a certificate valid for one is refused for the
        // other -- which is the whole point of checking the name.
        let leaf_params =
            rcgen::CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;
        let leaf_key = rcgen::KeyPair::generate()?;
        let leaf = leaf_params.signed_by(&leaf_key, &TEST_CA.certificate, &TEST_CA.key)?;
        std::fs::write(&cert, leaf.pem())?;
        std::fs::write(&key, leaf_key.serialize_pem())?;

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

    /// An origin serving a certificate no trust set here will accept: signed
    /// by a second, unregistered CA, so it fails on the issuer rather than on
    /// the name or on a date that will one day pass.
    fn start_untrusted() -> anyhow::Result<Self> {
        let certificates = tempfile::tempdir()?;
        let cert = certificates.path().join("cert.pem");
        let key = certificates.path().join("key.pem");

        let mut ca_params = rcgen::CertificateParams::new(Vec::new())?;
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = rcgen::KeyPair::generate()?;
        let ca = ca_params.self_signed(&ca_key)?;

        let leaf_params =
            rcgen::CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;
        let leaf_key = rcgen::KeyPair::generate()?;
        let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key)?;
        std::fs::write(&cert, leaf.pem())?;
        std::fs::write(&key, leaf_key.serialize_pem())?;

        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
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
                let _ = axum_server::from_tcp_rustls(listener, config)
                    .expect("the listener is ours")
                    .serve(
                        axum::Router::new()
                            .fallback(axum::routing::get(|| async { "secret bytes" }))
                            .into_make_service(),
                    )
                    .await;
            });
        });
        Ok(Self {
            addr,
            _certificates: certificates,
        })
    }
}

/// A certificate that will not verify is a refusal, not a downgrade.
///
/// This route was built with `danger_accept_invalid_certs(true)` from its
/// first commit -- inherited from the closed-source `server.js` proxy it was
/// ported from, commented "Parity with rejectUnauthorized: false", and never
/// a response to any host that was measured. It was later narrowed to a retry
/// that fired only on a certificate error and remembered the origin, which
/// was an improvement and still left every promise the trust policy makes
/// advisory: an on-path attacker can produce a certificate error as easily as
/// a misconfigured CDN can, so the retry handed the attacker exactly what
/// verification was there to deny -- with the stream URL's credential still
/// attached.
///
/// What made it defensible was that the alternative was breaking streams that
/// played. That alternative is gone: `enginefs::http_client_builder` now
/// trusts the platform's own store alongside the compiled-in roots, so a
/// device or organisation that installed a CA verifies again, and what is
/// left failing here is a chain nothing on the device trusts either.
#[test]
fn an_endpoint_whose_certificate_fails_is_refused() -> anyhow::Result<()> {
    let tls = TlsOrigin::start_untrusted()?;
    let fixture = fixture()?;
    let target = format!("https://localhost:{}/film.mkv", tls.addr.port());
    let response = reqwest::blocking::Client::new()
        .get(format!("{}/proxy/?d={}", fixture.base, encode(&target)))
        .send()?;

    assert_eq!(
        response.status(),
        reqwest::StatusCode::BAD_GATEWAY,
        "the fetch fails rather than being retried without verification"
    );
    assert_ne!(
        response.text()?,
        "secret bytes",
        "and the body the origin was holding never reaches the player"
    );

    drop(fixture.handle);
    Ok(())
}

/// The same failure one redirect away, because a chain is where this route
/// used to get the endpoint wrong: reqwest attributes a connect failure to
/// the URL the request *started* at, so the redirecting host was the one
/// marked unverified for a certificate it never presented. Walking the chain
/// here is what gave the failing hop a name; now that nothing is downgraded,
/// what has to hold is that the failure still stops the fetch rather than
/// being lost behind the hop that succeeded.
///
/// The redirector is plain HTTP because a verifiable first hop would need a
/// certificate authority; the hop that fails is the second either way.
#[test]
fn a_certificate_failure_behind_a_redirect_still_refuses() -> anyhow::Result<()> {
    let tls = TlsOrigin::start_untrusted()?;
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
        reqwest::StatusCode::BAD_GATEWAY,
        "the second hop's certificate stops the chain"
    );
    assert_ne!(response.text()?, "secret bytes");

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
/// hops are `https`, which is the whole of the condition: the credentials
/// leave the origin the caller named only over TLS, and over TLS they cross
/// hosts, because that is what buys this `403` back. Both origins are on
/// loopback, so this is a same-host, cross-*port* redirect as well -- the
/// case that reads as safe and is stripped just the same.
#[test]
fn an_https_redirect_to_another_host_still_carries_the_h_credentials() -> anyhow::Result<()> {
    const SECRET: &str = "Bearer s3cret";

    let edge = TlsOrigin::start_with(axum::Router::new().fallback(axum::routing::any(
        |headers: axum::http::HeaderMap| async move {
            match headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
            {
                Some(SECRET) => (axum::http::StatusCode::OK, "the edge served it"),
                _ => (axum::http::StatusCode::FORBIDDEN, ""),
            }
        },
    )))?;
    // What the CDN itself was asked with: without this the test would pass
    // on a proxy that dropped `h=` at the first hop and an edge that never
    // saw a request at all.
    let seen: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>> = Default::default();
    let recorder = seen.clone();
    let location = format!("https://127.0.0.1:{}/edge/film.mkv", edge.addr.port());
    let cdn = TlsOrigin::start_with(axum::Router::new().fallback(axum::routing::any(
        move |headers: axum::http::HeaderMap| {
            let recorder = recorder.clone();
            let location = location.clone();
            async move {
                recorder
                    .lock()
                    .expect("no test panicked holding this")
                    .push(
                        headers
                            .get("authorization")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_string),
                    );
                (
                    axum::http::StatusCode::FOUND,
                    [(axum::http::header::LOCATION, location)],
                )
            }
        },
    )))?;

    let fixture = fixture()?;
    let target = format!("https://127.0.0.1:{}/cdn/film.mkv", cdn.addr.port());
    let response = reqwest::blocking::Client::new()
        .get(format!(
            "{}/proxy/?d={}&h={}",
            fixture.base,
            encode(&target),
            encode(&format!("Authorization:{SECRET}"))
        ))
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.text()?,
        "the edge served it",
        "the hop the origin sent us to was asked with the credential"
    );
    assert_eq!(
        *seen.lock().expect("no test panicked holding this"),
        vec![Some(SECRET.to_string())],
        "and so was the hop the caller named"
    );

    drop(fixture.handle);
    Ok(())
}

/// The one place the trade above must not reach: a redirect that steps down
/// from `https` to plain `http`.
///
/// `h=` is re-applied on every hop, and `redirect_target` takes any
/// `http`/`https` target without comparing it to the scheme it came from --
/// so an `https` origin answering `302 Location: http://...` could have the
/// caller's `Authorization` (or `Cookie`) written onto a cleartext hop that
/// anybody on the path can read. That is not the credential following the
/// resource; it is the origin choosing to publish it, and no `403` is
/// avoided by obliging. reqwest's own policy strips those three names
/// across such a hop (`remove_sensitive_headers`), and walking the chain
/// ourselves is what took that away.
///
/// The rest of `h=` still travels: a `User-Agent` an addon needs is a
/// description, not a secret to spend, and dropping it would break the
/// stream for nothing.
#[test]
fn a_redirect_that_steps_down_to_http_does_not_carry_the_h_credentials() -> anyhow::Result<()> {
    const SECRET: &str = "Bearer s3cret";

    // The cleartext hop, which must be asked for the film with no
    // credential on it -- and with the `h=` that is not one.
    let cleartext = Origin::start_with(|request: &Request, socket: &mut TcpStream| {
        let body = format!(
            "authorization={:?} cookie={:?} user-agent={:?}",
            request.header("authorization"),
            request.header("cookie"),
            request.header("user-agent")
        );
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
    let cleartext_addr = cleartext.addr;

    // The TLS origin that sends us there, recording what it was asked with:
    // the credential has to reach the hop the caller actually named, or
    // this test would pass on a proxy that simply dropped `h=`.
    let seen: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>> = Default::default();
    let recorder = seen.clone();
    let location = format!("http://{cleartext_addr}/film.mkv");
    let tls = TlsOrigin::start_with(axum::Router::new().fallback(axum::routing::any(
        move |headers: axum::http::HeaderMap| {
            let recorder = recorder.clone();
            let location = location.clone();
            async move {
                recorder
                    .lock()
                    .expect("no test panicked holding this")
                    .push(
                        headers
                            .get("authorization")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_string),
                    );
                (
                    axum::http::StatusCode::FOUND,
                    [(axum::http::header::LOCATION, location)],
                )
            }
        },
    )))?;

    let fixture = fixture_with(cleartext)?;
    let target = format!("https://127.0.0.1:{}/film.mkv", tls.addr.port());
    let response = reqwest::blocking::Client::new()
        .get(format!(
            "{}/proxy/?d={}&h={}&h={}&h={}",
            fixture.base,
            encode(&target),
            encode(&format!("Authorization:{SECRET}")),
            encode("Cookie:session=abc"),
            encode("User-Agent:addon/1")
        ))
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.text()?,
        "authorization=None cookie=None user-agent=Some(\"addon/1\")",
        "the credentials stay behind at the downgrade; the rest of h= does not"
    );
    assert_eq!(
        *seen.lock().expect("no test panicked holding this"),
        vec![Some(SECRET.to_string())],
        "and the hop the caller named -- the https one -- was asked with the credential"
    );

    drop(fixture.handle);
    Ok(())
}

/// What a header echo looks like when an origin is asked to report the
/// three things this pair of tests is about: the two credentials, and the
/// `h=` header that is not one.
const ECHOED_HEADERS: [&str; 3] = ["authorization", "cookie", "user-agent"];

fn echo_headers(request: &Request) -> String {
    ECHOED_HEADERS
        .iter()
        .map(|name| format!("{name}={:?}", request.header(name)))
        .collect::<Vec<_>>()
        .join(" ")
}

/// The same, from inside an axum handler.
fn echo_header_map(headers: &axum::http::HeaderMap) -> String {
    ECHOED_HEADERS
        .iter()
        .map(|name| {
            format!(
                "{name}={:?}",
                headers
                    .get(*name)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string)
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// An origin that answers every request with a report of what it was asked
/// with -- so a test can assert on what reached the wire rather than on
/// what it believes was sent.
fn echoing_origin() -> anyhow::Result<Origin> {
    Origin::start_with(|request: &Request, socket: &mut TcpStream| {
        let body = echo_headers(request);
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: video/mp2t\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })
}

/// The lines of a rewritten playlist that name something, in order.
fn rewritten_lines(body: &str) -> Vec<&str> {
    body.lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
        .collect()
}

/// The redirect guard above, defeated in one line by the playlist rewrite:
/// an `https` origin whose playlist names an `http://` segment.
///
/// `h=` travels into every rewritten line, deliberately -- that is what
/// keeps an authenticated stream's segments fetchable. But a rewritten line
/// is the *player's* next request, made automatically, and it is written by
/// us: putting the caller's `Authorization` into a line that names cleartext
/// is walking the credential onto a cleartext hop with the redirect loop's
/// guard bypassed. Measured before the fix, with the line fetched exactly as
/// a player fetches it: the cleartext origin logged
/// `authorization=Some("Bearer s3cret") cookie=Some("session=abc")`.
///
/// The positive halves are here too, because a rule that drops everything
/// passes the negative one: the line naming the `https` origin still
/// carries the credential, and the cleartext line still carries the
/// `User-Agent` that is a description rather than a secret.
#[test]
fn a_rewritten_line_that_steps_down_to_cleartext_carries_no_credential() -> anyhow::Result<()> {
    const SECRET: &str = "Bearer s3cret";

    let cleartext = echoing_origin()?;
    let cleartext_addr = cleartext.addr;

    // The TLS origin: the playlist under `.m3u8`, an echo of what it was
    // asked with anywhere else -- so the `https` line can be fetched and
    // shown to have kept the credential.
    let tls = TlsOrigin::start_with(axum::Router::new().fallback(axum::routing::any(
        move |uri: axum::http::Uri, headers: axum::http::HeaderMap| async move {
            let (content_type, body) = if uri.path().ends_with(".m3u8") {
                (
                    "application/x-mpegURL",
                    format!(
                        "#EXTM3U\n#EXTINF:10,\nhttp://{cleartext_addr}/seg-0.ts\n\
                         #EXTINF:10,\nseg-1.ts\n"
                    ),
                )
            } else {
                ("video/mp2t", echo_header_map(&headers))
            };
            ([(axum::http::header::CONTENT_TYPE, content_type)], body)
        },
    )))?;

    let fixture = fixture_with(cleartext)?;
    let client = reqwest::blocking::Client::new();
    let credentials = format!(
        "&h={}&h={}&h={}",
        encode(&format!("Authorization:{SECRET}")),
        encode("Cookie:session=abc"),
        encode("User-Agent:addon/1")
    );
    let playlist = client
        .get(format!(
            "{}/proxy/?d={}{credentials}",
            fixture.base,
            encode(&format!(
                "https://127.0.0.1:{}/live/master.m3u8",
                tls.addr.port()
            ))
        ))
        .send()?;
    assert_eq!(playlist.status(), reqwest::StatusCode::OK);
    let body = playlist.text()?;
    let lines = rewritten_lines(&body);
    let (cleartext_line, tls_line) = (lines[0], lines[1]);

    assert!(
        !cleartext_line.contains("Authorization") && !cleartext_line.contains("Cookie"),
        "the line naming http carries neither credential: {cleartext_line}"
    );
    assert!(
        cleartext_line.contains(&format!("&h={}", encode("User-Agent:addon/1"))),
        "but it does carry the h= that is a description: {cleartext_line}"
    );
    assert!(
        tls_line.contains(&format!(
            "&h={}",
            encode(&format!("Authorization:{SECRET}"))
        )) && tls_line.contains(&format!("&h={}", encode("Cookie:session=abc"))),
        "the line that stays on https carries them: {tls_line}"
    );

    // And what a player does with those lines, which is the measurement
    // that matters: it fetches them.
    let segment = client
        .get(format!("{}{cleartext_line}", fixture.base))
        .send()?;
    assert_eq!(segment.status(), reqwest::StatusCode::OK);
    assert_eq!(
        segment.text()?,
        "authorization=None cookie=None user-agent=Some(\"addon/1\")",
        "the cleartext origin is asked without the credentials"
    );
    let segment = client.get(format!("{}{tls_line}", fixture.base)).send()?;
    assert_eq!(segment.status(), reqwest::StatusCode::OK);
    assert_eq!(
        segment.text()?,
        format!(
            "authorization=Some({SECRET:?}) cookie=Some(\"session=abc\") user-agent=Some(\"addon/1\")"
        ),
        "and the https one with them -- an authenticated stream still plays"
    );

    drop(fixture.handle);
    Ok(())
}

/// The chain the redirect guard was written for, defeated end to end: an
/// `https` origin redirecting to a plain-`http` playlist.
///
/// The playlist hop is asked with no credentials -- that is the guard
/// working. What the guard cannot reach on its own is the body that comes
/// back over that cleartext hop: rewritten with `h=` re-armed, it handed the
/// player a segment URL carrying the caller's `Authorization` to the very
/// host the credential had just been withheld from, one line later.
///
/// So the drop is sticky through the rewrite as well, `https` lines
/// included: a playlist fetched over cleartext was told what to name in the
/// clear too, and an `https` line in it is not the caller's origin talking.
#[test]
fn a_playlist_fetched_over_a_downgraded_chain_arms_no_line_with_the_credential()
-> anyhow::Result<()> {
    const SECRET: &str = "Bearer s3cret";

    // Filled in once the TLS origin is up, which is before anything is
    // fetched from either of them.
    let tls_addr: std::sync::Arc<std::sync::OnceLock<SocketAddr>> = Default::default();
    let for_playlist = tls_addr.clone();
    let cleartext = Origin::start_with(move |request: &Request, socket: &mut TcpStream| {
        let tls = for_playlist.get().expect("the TLS origin is up");
        let (content_type, body) = if request.target().contains(".m3u8") {
            (
                "application/x-mpegURL",
                format!("#EXTM3U\n#EXTINF:10,\nseg-0.ts\n#EXTINF:10,\nhttps://{tls}/seg-1.ts\n"),
            )
        } else {
            ("video/mp2t", echo_headers(request))
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
    let location = format!("http://{}/live/master.m3u8", cleartext.addr);

    let tls = TlsOrigin::start_with(axum::Router::new().fallback(axum::routing::any(
        move |uri: axum::http::Uri, headers: axum::http::HeaderMap| {
            let location = location.clone();
            async move {
                if uri.path().ends_with("master.m3u8") {
                    return (
                        axum::http::StatusCode::FOUND,
                        [(axum::http::header::LOCATION, location)],
                        String::new(),
                    );
                }
                (
                    axum::http::StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, "video/mp2t".to_string())],
                    echo_header_map(&headers),
                )
            }
        },
    )))?;
    tls_addr
        .set(tls.addr)
        .expect("set once, before any request");

    let fixture = fixture_with(cleartext)?;
    let client = reqwest::blocking::Client::new();
    let playlist = client
        .get(format!(
            "{}/proxy/?d={}&h={}&h={}&h={}",
            fixture.base,
            encode(&format!(
                "https://127.0.0.1:{}/live/master.m3u8",
                tls.addr.port()
            )),
            encode(&format!("Authorization:{SECRET}")),
            encode("Cookie:session=abc"),
            encode("User-Agent:addon/1")
        ))
        .send()?;
    assert_eq!(playlist.status(), reqwest::StatusCode::OK);
    let body = playlist.text()?;

    assert_eq!(
        fixture.origin.next_request().header("authorization"),
        None,
        "the playlist hop itself is asked with no credential -- the guard working"
    );
    for line in rewritten_lines(&body) {
        assert!(
            !line.contains("Authorization") && !line.contains("Cookie"),
            "and nothing it named is armed with one, https lines included: {line}"
        );
        assert!(
            line.contains(&format!("&h={}", encode("User-Agent:addon/1"))),
            "while the rest of h= still travels: {line}"
        );
    }

    // The segment fetch that used to carry it to the cleartext host.
    let segment = client
        .get(format!("{}{}", fixture.base, rewritten_lines(&body)[0]))
        .send()?;
    assert_eq!(segment.status(), reqwest::StatusCode::OK);
    assert_eq!(
        segment.text()?,
        "authorization=None cookie=None user-agent=Some(\"addon/1\")"
    );

    drop(fixture.handle);
    Ok(())
}

/// The cleartext exception at the scope it actually earns: a playlist that
/// arrived over `http` arms the lines naming the origin **the caller
/// named**, and no others. Here that is also the origin the playlist came
/// from; the test above is where a redirect tells the two apart.
///
/// A caller that names an `http://` target has spent the credential on that
/// wire itself, which is why such a playlist's own segments still carry it
/// -- an authenticated plain-`http` stream would lose every segment
/// otherwise. But a wire belongs to one host. Keyed on the playlist's
/// *scheme*, the exception armed every `http` line in it whoever it named:
/// measured, a caller naming `http://A/live/master.m3u8` with
/// `h=Authorization:Bearer s3cret` got back a playlist naming
/// `http://B/seg-0.ts`, and B -- a host nothing in this chain had
/// authenticated to -- logged `authorization=Some("Bearer s3cret")` when
/// the line was fetched the way a player fetches it.
///
/// The positive halves are here because a rule that drops everything passes
/// the negative one: the line back to A still carries the credential, and
/// B's line still carries the `User-Agent` that is a description rather
/// than a secret.
#[test]
fn a_cleartext_line_naming_another_host_carries_no_credential() -> anyhow::Result<()> {
    const SECRET: &str = "Bearer s3cret";

    let elsewhere = echoing_origin()?;
    let elsewhere_addr = elsewhere.addr;
    // The host the caller names: the playlist at a `.m3u8` URL, an echo of
    // what it was asked with anywhere else, so its own segment line can be
    // fetched and reported on too.
    let named = Origin::start_with(move |request: &Request, socket: &mut TcpStream| {
        let (content_type, body) = if request.target().ends_with(".m3u8") {
            (
                "application/x-mpegURL",
                format!(
                    "#EXTM3U\n#EXTINF:10,\nhttp://{elsewhere_addr}/seg-0.ts\n\
                     #EXTINF:10,\nseg-1.ts\n"
                ),
            )
        } else {
            ("video/mp2t", echo_headers(request))
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

    let fixture = fixture_with(named)?;
    let client = reqwest::blocking::Client::new();
    let playlist = client
        .get(format!(
            "{}/proxy/?d={}&h={}&h={}&h={}",
            fixture.base,
            encode(&format!("http://{}/live/master.m3u8", fixture.origin.addr)),
            encode(&format!("Authorization:{SECRET}")),
            encode("Cookie:session=abc"),
            encode("User-Agent:addon/1")
        ))
        .send()?;
    assert_eq!(playlist.status(), reqwest::StatusCode::OK);
    let body = playlist.text()?;
    let lines = rewritten_lines(&body);
    let (other_host, same_host) = (lines[0], lines[1]);

    assert!(
        !other_host.contains("Authorization") && !other_host.contains("Cookie"),
        "the line naming a host the caller never named carries neither \
         credential: {other_host}"
    );
    assert!(
        other_host.contains(&format!("&h={}", encode("User-Agent:addon/1"))),
        "but it does carry the h= that is a description: {other_host}"
    );
    assert!(
        same_host.contains(&format!(
            "&h={}",
            encode(&format!("Authorization:{SECRET}"))
        )) && same_host.contains(&format!("&h={}", encode("Cookie:session=abc"))),
        "while the line back to the host the caller named keeps them -- that \
         is the stream this exception exists for: {same_host}"
    );

    // And what a player does with those lines, which is the measurement
    // that matters: it fetches them.
    let segment = client.get(format!("{}{other_host}", fixture.base)).send()?;
    assert_eq!(segment.status(), reqwest::StatusCode::OK);
    assert_eq!(
        segment.text()?,
        "authorization=None cookie=None user-agent=Some(\"addon/1\")",
        "the third-party host is asked without the credentials"
    );
    let segment = client.get(format!("{}{same_host}", fixture.base)).send()?;
    assert_eq!(segment.status(), reqwest::StatusCode::OK);
    assert_eq!(
        segment.text()?,
        format!(
            "authorization=Some({SECRET:?}) cookie=Some(\"session=abc\") user-agent=Some(\"addon/1\")"
        ),
        "and its own host with them -- an authenticated plain-http stream still plays"
    );

    drop(fixture.handle);
    Ok(())
}

/// Nothing bounds the depth while the exception is keyed on the scheme,
/// because a rewritten line is not a hop of the request that wrote it: it
/// is a fresh `/proxy` request the player makes, and it re-arms `h=` from
/// scratch. Measured: A's playlist names `http://B/b.m3u8`, B's names
/// `http://C/seg-c.ts`, and C -- two origins removed from anything the
/// caller named -- logged `authorization=Some("Bearer s3cret")`.
/// `MAX_REDIRECTS` has nothing to say about this; it counts the hops inside
/// one request.
///
/// Scoped by origin it stops at the first line: B's line is written without
/// the credential, so the request for B's playlist has none to re-arm, and
/// what B names cannot inherit what B was never given. And it stays
/// stopped for a cleartext walk of any depth, because the other way a new
/// cleartext origin could be armed -- a `302` out of an armed line -- no
/// longer carries the credential either (see the redirect test above).
#[test]
fn a_nested_playlist_on_another_host_arms_none_of_its_own_lines() -> anyhow::Result<()> {
    const SECRET: &str = "Bearer s3cret";

    // C, the far end: it reports whatever it was asked with.
    let last = echoing_origin()?;
    let last_addr = last.addr;
    // B, which the caller never named either -- a playlist naming C.
    let middle = Origin::start_with(move |request: &Request, socket: &mut TcpStream| {
        let (content_type, body) = if request.target().ends_with(".m3u8") {
            (
                "application/x-mpegURL",
                format!("#EXTM3U\n#EXTINF:10,\nhttp://{last_addr}/seg-c.ts\n"),
            )
        } else {
            ("video/mp2t", echo_headers(request))
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
    let middle_addr = middle.addr;
    // A, the one the caller does name: a master playlist naming B's.
    let first = Origin::start_with(move |_request: &Request, socket: &mut TcpStream| {
        let body =
            format!("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nhttp://{middle_addr}/live/b.m3u8\n");
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/x-mpegURL\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })?;

    // The fixture's origin is B, so the request it was asked with can be
    // read back directly.
    let fixture = fixture_with(middle)?;
    let client = reqwest::blocking::Client::new();
    let master = client
        .get(format!(
            "{}/proxy/?d={}&h={}&h={}&h={}",
            fixture.base,
            encode(&format!("http://{}/live/master.m3u8", first.addr)),
            encode(&format!("Authorization:{SECRET}")),
            encode("Cookie:session=abc"),
            encode("User-Agent:addon/1")
        ))
        .send()?;
    assert_eq!(master.status(), reqwest::StatusCode::OK);
    let master = master.text()?;
    let to_middle = rewritten_lines(&master)[0].to_string();
    assert!(
        !to_middle.contains("Authorization") && !to_middle.contains("Cookie"),
        "the nested playlist is on another host, so its line is unarmed: {to_middle}"
    );

    let nested = client.get(format!("{}{to_middle}", fixture.base)).send()?;
    assert_eq!(nested.status(), reqwest::StatusCode::OK);
    let nested = nested.text()?;
    assert_eq!(
        fixture.origin.next_request().header("authorization"),
        None,
        "and B was asked for it without the credential"
    );
    let to_last = rewritten_lines(&nested)[0].to_string();
    assert!(
        !to_last.contains("Authorization") && !to_last.contains("Cookie"),
        "so nothing B names is armed either, at any depth: {to_last}"
    );
    assert!(
        to_last.contains(&format!("&h={}", encode("User-Agent:addon/1"))),
        "while the rest of h= still travels the whole way: {to_last}"
    );

    let segment = client.get(format!("{}{to_last}", fixture.base)).send()?;
    assert_eq!(segment.status(), reqwest::StatusCode::OK);
    assert_eq!(
        segment.text()?,
        "authorization=None cookie=None user-agent=Some(\"addon/1\")",
        "which is what C is asked with -- the fetch that used to carry it"
    );

    drop(fixture.handle);
    Ok(())
}

/// Which cleartext origin a playlist arms is the **caller's**, not the one
/// the body came from -- and a cleartext `302` is what tells those two
/// apart.
///
/// Measured against the code this fixes, which keyed the scope on the URL
/// the body came from: a caller naming `http://A/live/master.m3u8` with
/// `h=Authorization:Bearer s3cret` was redirected to
/// `http://B/edge/master.m3u8`, whose playlist names
/// `http://A/back-on-a.ts`. The line home to A came out with no `h=` at
/// all, A logging `authorization=None` when it was fetched -- on a real
/// authenticated stream, every segment `403`ing -- while B's own line was
/// armed, handing the credential to a host the caller never named.
///
/// Both halves are the one rule. A is the origin the caller named and so
/// the origin the credential was spent on, wherever the playlist came from;
/// and the credentials do not cross a cleartext redirect, so B is asked for
/// the playlist without them and writes none into its own lines.
#[test]
fn a_cleartext_redirect_arms_the_origin_the_caller_named_not_the_one_it_landed_on()
-> anyhow::Result<()> {
    const SECRET: &str = "Bearer s3cret";

    // A and B name each other, so one of the two addresses is only known
    // after both listeners are up. Nothing is fetched until the request
    // below, by which time it is set.
    let a_addr: std::sync::Arc<std::sync::OnceLock<SocketAddr>> = Default::default();
    let a_for_edge = a_addr.clone();
    // B, which only the redirect names: a playlist pointing one segment
    // back at A and one at itself, and an echo of what it was asked with
    // for the segment.
    let edge = Origin::start_with(move |request: &Request, socket: &mut TcpStream| {
        let a = a_for_edge
            .get()
            .expect("A is up before anything is fetched");
        let (content_type, body) = if request.target().ends_with(".m3u8") {
            (
                "application/x-mpegURL",
                format!("#EXTM3U\n#EXTINF:10,\nhttp://{a}/back-on-a.ts\n#EXTINF:10,\nseg-b.ts\n"),
            )
        } else {
            ("video/mp2t", echo_headers(request))
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
    let edge_addr = edge.addr;
    // A, which the caller names: it sends the playlist away and echoes what
    // it is asked with for the segment that comes back home.
    let named = Origin::start_with(move |request: &Request, socket: &mut TcpStream| {
        if request.target().ends_with(".m3u8") {
            let _ = socket.write_all(
                format!(
                    "HTTP/1.1 302 Found\r\nLocation: http://{edge_addr}/edge/master.m3u8\r\n\
                     Content-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            );
        } else {
            let body = echo_headers(request);
            let _ = socket.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: video/mp2t\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            );
        }
        let _ = socket.flush();
    })?;
    a_addr.set(named.addr).expect("set once, before any fetch");

    let fixture = fixture_with(named)?;
    let client = reqwest::blocking::Client::new();
    let playlist = client
        .get(format!(
            "{}/proxy/?d={}&h={}&h={}&h={}",
            fixture.base,
            encode(&format!("http://{}/live/master.m3u8", fixture.origin.addr)),
            encode(&format!("Authorization:{SECRET}")),
            encode("Cookie:session=abc"),
            encode("User-Agent:addon/1")
        ))
        .send()?;
    assert_eq!(playlist.status(), reqwest::StatusCode::OK);
    let body = playlist.text()?;
    let lines = rewritten_lines(&body);
    let (home, elsewhere) = (lines[0], lines[1]);

    assert!(
        home.contains(&format!(
            "&h={}",
            encode(&format!("Authorization:{SECRET}"))
        )) && home.contains(&format!("&h={}", encode("Cookie:session=abc"))),
        "the line home to A is armed, wherever the playlist came from: {home}"
    );
    assert!(
        !elsewhere.contains("Authorization") && !elsewhere.contains("Cookie"),
        "B's own line is not: {elsewhere}"
    );
    assert!(
        elsewhere.contains(&format!("&h={}", encode("User-Agent:addon/1"))),
        "though it still carries the h= that is a description: {elsewhere}"
    );
    assert_eq!(
        fixture.origin.next_request().header("authorization"),
        Some(SECRET),
        "the caller named A, so A was asked with the credential"
    );
    assert_eq!(
        edge.next_request().header("authorization"),
        None,
        "and the cleartext redirect did not carry it to B"
    );

    // What a player does with those lines, which is the measurement that
    // matters: it fetches them.
    let segment = client.get(format!("{}{home}", fixture.base)).send()?;
    assert_eq!(segment.status(), reqwest::StatusCode::OK);
    assert_eq!(
        segment.text()?,
        format!(
            "authorization=Some({SECRET:?}) cookie=Some(\"session=abc\") user-agent=Some(\"addon/1\")"
        ),
        "so an authenticated stream's own segments still play"
    );
    let segment = client.get(format!("{}{elsewhere}", fixture.base)).send()?;
    assert_eq!(segment.status(), reqwest::StatusCode::OK);
    assert_eq!(
        segment.text()?,
        "authorization=None cookie=None user-agent=Some(\"addon/1\")",
        "and B, which nothing authenticated to, is asked without the credentials"
    );

    drop(fixture.handle);
    Ok(())
}

/// The other end of the same recursion: an armed cleartext line's own fetch
/// can redirect, and a redirect target is not a line the scoping ever
/// looked at.
///
/// Measured against the code this fixes: A's playlist named A's own
/// `inner.m3u8`, which was armed and rightly so; that line's fetch was
/// answered `302 Location: http://C/far/inner.m3u8`, and the hop rule let a
/// cleartext chain carry the credential across a redirect -- so C, which
/// the caller never named, was asked with `Bearer s3cret`, and C's playlist
/// was then scoped to C, arming its lines too. One new origin per redirect,
/// with `MAX_REDIRECTS` bounding only the hops inside a single request.
///
/// The credentials do not cross a cleartext redirect now, so the chain ends
/// where it starts: A is asked with them at any depth, C is asked without,
/// and what C names inherits nothing.
#[test]
fn a_cleartext_redirect_out_of_an_armed_line_reaches_the_next_host_without_the_credential()
-> anyhow::Result<()> {
    const SECRET: &str = "Bearer s3cret";

    // C, two steps from anything the caller named: a playlist of its own,
    // and an echo for the segment that playlist names.
    let far = Origin::start_with(|request: &Request, socket: &mut TcpStream| {
        let (content_type, body) = if request.target().ends_with(".m3u8") {
            (
                "application/x-mpegURL",
                "#EXTM3U\n#EXTINF:10,\nseg-c.ts\n".to_string(),
            )
        } else {
            ("video/mp2t", echo_headers(request))
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
    let far_addr = far.addr;
    // A: a master naming its own media playlist -- same origin, so armed --
    // and that playlist redirecting to C.
    let named = Origin::start_with(move |request: &Request, socket: &mut TcpStream| {
        if request.target().ends_with("/inner.m3u8") {
            let _ = socket.write_all(
                format!(
                    "HTTP/1.1 302 Found\r\nLocation: http://{far_addr}/far/inner.m3u8\r\n\
                     Content-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            );
        } else {
            let body = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\ninner.m3u8\n";
            let _ = socket.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/x-mpegURL\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            );
        }
        let _ = socket.flush();
    })?;

    let fixture = fixture_with(named)?;
    let client = reqwest::blocking::Client::new();
    let master = client
        .get(format!(
            "{}/proxy/?d={}&h={}&h={}&h={}",
            fixture.base,
            encode(&format!("http://{}/live/master.m3u8", fixture.origin.addr)),
            encode(&format!("Authorization:{SECRET}")),
            encode("Cookie:session=abc"),
            encode("User-Agent:addon/1")
        ))
        .send()?;
    assert_eq!(master.status(), reqwest::StatusCode::OK);
    let master = master.text()?;
    let inner = rewritten_lines(&master)[0].to_string();
    assert!(
        inner.contains(&format!(
            "&h={}",
            encode(&format!("Authorization:{SECRET}"))
        )),
        "A's own media playlist is on A, so its line is armed: {inner}"
    );

    let nested = client.get(format!("{}{inner}", fixture.base)).send()?;
    assert_eq!(nested.status(), reqwest::StatusCode::OK);
    let nested = nested.text()?;
    assert_eq!(
        fixture.origin.next_request().header("authorization"),
        Some(SECRET),
        "the master was asked with the credential"
    );
    assert_eq!(
        fixture.origin.next_request().header("authorization"),
        Some(SECRET),
        "and so was A's own media playlist, one line deeper"
    );
    assert_eq!(
        far.next_request().header("authorization"),
        None,
        "but C, which the redirect named and the caller never did, was not"
    );

    let to_last = rewritten_lines(&nested)[0].to_string();
    assert!(
        !to_last.contains("Authorization") && !to_last.contains("Cookie"),
        "so nothing C names is armed either: {to_last}"
    );
    let segment = client.get(format!("{}{to_last}", fixture.base)).send()?;
    assert_eq!(segment.status(), reqwest::StatusCode::OK);
    assert_eq!(
        segment.text()?,
        "authorization=None cookie=None user-agent=Some(\"addon/1\")",
        "which is what the fetch that used to resume the walk is asked with"
    );

    drop(fixture.handle);
    Ok(())
}

/// The other half of the same symmetry, and the one behaviour this round
/// changed rather than restored: a `302` **home to the origin the caller
/// named** carries the credentials again, even on a cleartext chain that
/// has been away.
///
/// The rewriter has armed that line since the round that keyed the
/// exception on `d=`: a playlist fetched from B may name `http://A/seg.ts`
/// and A is asked with the credential, because the caller published it on A
/// by naming A. A `302` to the same URL is the same request with the same
/// recipient, so the loop refusing it was the two halves disagreeing --
/// harmlessly this time, but by the same lack of a shared rule that leaked
/// in the other direction. One predicate answers both, so it now carries in
/// both, and an authenticated plain-`http` stream that bounces off a CDN
/// and back home plays instead of `403`ing.
///
/// What has not changed is everything the bounce passes through: B, which
/// only the redirect named, is asked with nothing.
#[test]
fn a_cleartext_redirect_home_to_the_origin_the_caller_named_carries_the_credential()
-> anyhow::Result<()> {
    const SECRET: &str = "Bearer s3cret";

    // A and B name each other, so one address is only known after both
    // listeners are up; nothing is fetched until the request below.
    let a_addr: std::sync::Arc<std::sync::OnceLock<SocketAddr>> = Default::default();
    let b_addr: std::sync::Arc<std::sync::OnceLock<SocketAddr>> = Default::default();
    let b_for_a = b_addr.clone();
    let a_for_b = a_addr.clone();
    // A: the film is sent away to B, and whatever comes back home is
    // answered with a report of what it was asked with.
    let named = Origin::start_with(move |request: &Request, socket: &mut TcpStream| {
        if request.target().ends_with("/film.mkv") {
            let b = b_for_a.get().expect("B is up before anything is fetched");
            let _ = socket.write_all(
                format!(
                    "HTTP/1.1 302 Found\r\nLocation: http://{b}/hand-off\r\n\
                     Content-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            );
        } else {
            let body = echo_headers(request);
            let _ = socket.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            );
        }
        let _ = socket.flush();
    })?;
    a_addr.set(named.addr).expect("set once, before any fetch");
    // B, which the caller never named: it sends us straight back to A.
    let away = Origin::start_with(move |_request: &Request, socket: &mut TcpStream| {
        let a = a_for_b.get().expect("A is up before anything is fetched");
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 302 Found\r\nLocation: http://{a}/back-home.mkv\r\n\
                 Content-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        );
        let _ = socket.flush();
    })?;
    b_addr.set(away.addr).expect("set once, before any fetch");

    let fixture = fixture_with(named)?;
    let response = reqwest::blocking::Client::new()
        .get(format!(
            "{}/proxy/?d={}&h={}&h={}&h={}",
            fixture.base,
            encode(&format!("http://{}/film.mkv", fixture.origin.addr)),
            encode(&format!("Authorization:{SECRET}")),
            encode("Cookie:session=abc"),
            encode("User-Agent:addon/1")
        ))
        .send()?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.text()?,
        format!(
            "authorization=Some({SECRET:?}) cookie=Some(\"session=abc\") user-agent=Some(\"addon/1\")"
        ),
        "the hop home to A is asked with the credential A already has"
    );
    assert_eq!(
        fixture.origin.next_request().header("authorization"),
        Some(SECRET),
        "as was the hop the caller named"
    );
    assert_eq!(
        away.next_request().header("authorization"),
        None,
        "while B, in between, was asked with none"
    );

    drop(fixture.handle);
    Ok(())
}

/// The direction the two halves had not been tested against each other in:
/// a **cleartext** chain whose playlist names an `https` host.
///
/// The redirect loop refuses that hop -- a cleartext chain carries the
/// credentials to the origin the caller named and nowhere else, an `https`
/// target included, because a playlist named in the clear is not the
/// caller's origin talking. The rewriter used to allow it: it asked only
/// whether the chain had ever stepped off `https`, and a chain that started
/// in the clear never had.
///
/// Measured against the code this fixes: a caller naming
/// `http://A/live/master.m3u8` with `h=Authorization:Bearer s3cret` got back
/// a playlist naming `https://C/live/master.m3u8` with the credential
/// written into the line, C logged `authorization=Some("Bearer s3cret")`
/// when the line was fetched the way a player fetches it, and C's own
/// playlist -- now an `https` chain of its own -- armed `https://D/seg.ts`,
/// so D logged it too. Two hosts the caller never named, from a chain the
/// loop would not have carried one hop of.
///
/// The positives are here too: A, the origin the caller named and spent the
/// credential on, is still asked with it, and every line still carries the
/// `h=` that is a description rather than a secret.
#[test]
fn a_cleartext_playlist_naming_an_https_host_arms_no_line_with_the_credential() -> anyhow::Result<()>
{
    const SECRET: &str = "Bearer s3cret";

    // D, the far end: it reports what it was asked with.
    let deep = TlsOrigin::start_with(axum::Router::new().fallback(axum::routing::any(
        move |headers: axum::http::HeaderMap| async move {
            (
                [(axum::http::header::CONTENT_TYPE, "video/mp2t")],
                echo_header_map(&headers),
            )
        },
    )))?;
    let deep_addr = deep.addr;
    // C, the `https` host A's playlist names: a playlist of its own naming
    // D, with what C itself was asked with reported in a comment line --
    // which the rewriter leaves alone, so it survives into the body we read.
    let edge = TlsOrigin::start_with(axum::Router::new().fallback(axum::routing::any(
        move |uri: axum::http::Uri, headers: axum::http::HeaderMap| async move {
            if uri.path().ends_with(".m3u8") {
                return (
                    [(axum::http::header::CONTENT_TYPE, "application/x-mpegURL")],
                    format!(
                        "#EXTM3U\n# asked-with {}\n#EXTINF:10,\nhttps://{deep_addr}/deep/seg.ts\n",
                        echo_header_map(&headers)
                    ),
                );
            }
            (
                [(axum::http::header::CONTENT_TYPE, "video/mp2t")],
                echo_header_map(&headers),
            )
        },
    )))?;
    let edge_addr = edge.addr;
    // A, the cleartext host the caller does name: a playlist naming C, and
    // an echo for its own segment line.
    let named = Origin::start_with(move |request: &Request, socket: &mut TcpStream| {
        let (content_type, body) = if request.target().ends_with(".m3u8") {
            (
                "application/x-mpegURL",
                format!(
                    "#EXTM3U\n#EXTINF:10,\nhttps://{edge_addr}/live/master.m3u8\n\
                     #EXTINF:10,\nseg-a.ts\n"
                ),
            )
        } else {
            ("video/mp2t", echo_headers(request))
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

    let fixture = fixture_with(named)?;
    let client = reqwest::blocking::Client::new();
    let playlist = client
        .get(format!(
            "{}/proxy/?d={}&h={}&h={}&h={}",
            fixture.base,
            encode(&format!("http://{}/live/master.m3u8", fixture.origin.addr)),
            encode(&format!("Authorization:{SECRET}")),
            encode("Cookie:session=abc"),
            encode("User-Agent:addon/1")
        ))
        .send()?;
    assert_eq!(playlist.status(), reqwest::StatusCode::OK);
    let body = playlist.text()?;
    let lines = rewritten_lines(&body);
    let (to_edge, home) = (lines[0].to_string(), lines[1].to_string());

    assert_eq!(
        fixture.origin.next_request().header("authorization"),
        Some(SECRET),
        "A, which the caller named, is asked with the credential"
    );
    assert!(
        home.contains(&format!(
            "&h={}",
            encode(&format!("Authorization:{SECRET}"))
        )),
        "and its own segment line keeps it: {home}"
    );
    assert!(
        !to_edge.contains("Authorization") && !to_edge.contains("Cookie"),
        "while the https line names a host the caller never did, on a chain \
         that has never been https: {to_edge}"
    );
    assert!(
        to_edge.contains(&format!("&h={}", encode("User-Agent:addon/1"))),
        "though it still carries the h= that is a description: {to_edge}"
    );

    // What a player does with that line, which is the measurement that
    // matters: it fetches it, and what comes back is another playlist.
    let nested = client.get(format!("{}{to_edge}", fixture.base)).send()?;
    assert_eq!(nested.status(), reqwest::StatusCode::OK);
    let nested = nested.text()?;
    assert!(
        nested.contains("# asked-with authorization=None cookie=None"),
        "C is asked without the credentials: {nested}"
    );
    let to_deep = rewritten_lines(&nested)[0].to_string();
    assert!(
        !to_deep.contains("Authorization") && !to_deep.contains("Cookie"),
        "so nothing C names is armed either, https or not: {to_deep}"
    );

    let segment = client.get(format!("{}{to_deep}", fixture.base)).send()?;
    assert_eq!(segment.status(), reqwest::StatusCode::OK);
    assert_eq!(
        segment.text()?,
        "authorization=None cookie=None user-agent=Some(\"addon/1\")",
        "and D, two hosts from anything the caller named, is asked with none"
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
    // And readable by the client that has to act on it. A browser-hosted
    // client sees only the headers CORS names, and `location` was not in
    // the allow-list -- so the diagnostic this relay exists to be arrived
    // and could not be read in exactly the client shape that reads headers
    // by name.
    assert!(
        response
            .headers()
            .get("access-control-expose-headers")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase()
            .contains("location"),
        "the relayed Location has to be readable from script: {:?}",
        response.headers().get("access-control-expose-headers")
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
    // The player's headers, reported the moment they arrive: what is being
    // closed below is a body a player already has a `200` for, and a close
    // that landed before those headers would leave the player holding a
    // connection that ended rather than a stream that stopped. That is a
    // different thing to test -- the very next test does -- and waiting for
    // the headers here is what tells the two apart instead of leaving it to
    // how loaded the machine is.
    let (headers, arrived) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        let response = reqwest::blocking::Client::new().get(url).send()?;
        let status = response.status();
        let _ = headers.send(status);
        Ok::<_, reqwest::Error>((status, response.text().is_err()))
    });
    assert_eq!(
        arrived.recv().expect("the player's own headers"),
        reqwest::StatusCode::OK,
        "the rewrite streams, so the player has its 200 before a byte of the \
         playlist exists"
    );

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

/// A stitched response -- the cached head off disk, the origin's tail behind
/// it -- is one body under one registration, so a close during the head
/// breaks the read there. It used to register only the tail and chain the
/// head in front of that: `Chain` never polls its second stream until the
/// first has ended, so the close was answered `{"closed":1}`, `live()` fell
/// to zero, and every remaining chunk of the head kept coming off disk to a
/// player whose client had finished with it; the read broke only when the
/// tail was first polled.
#[test]
fn closing_during_the_cached_head_of_a_stitched_response_breaks_the_read() -> anyhow::Result<()> {
    use std::io::Read as _;

    // A head longer than anything between hyper and this socket can buffer,
    // so what the client can still read after the close is bounded by that
    // buffering and not by the head's length: 64 chunks, 16 MiB. (Linux
    // autotunes a loopback socket's send and receive buffers to a few MiB
    // each at most; hyper's own write buffer is smaller than that.)
    const HEAD_CHUNKS: u64 = 64;
    let head_len = HEAD_CHUNKS * CHUNK;
    let fixture = fixture_with(Origin::start_sized((head_len + 4 * CHUNK) as usize)?)?;
    let origin = format!("http://{}", fixture.origin.addr);
    let client = reqwest::blocking::Client::new();

    // The head goes into the cache first, from a read that carries no token.
    let warm = client
        .get(format!(
            "{}/proxy/?d={}",
            fixture.base,
            encode(&format!("{origin}/film.mkv"))
        ))
        .header(reqwest::header::RANGE, format!("bytes=0-{}", head_len - 1))
        .send()?;
    assert_eq!(warm.bytes()?.len() as u64, head_len);
    fixture.origin.next_request();
    wait_for_chunks(&fixture, HEAD_CHUNKS as usize);

    // The stitched read: the whole head off disk, one chunk more from the
    // origin, under the player's token.
    let mut response = client
        .get(format!(
            "{}/proxy/?d={}&p=player-stitched",
            fixture.base,
            encode(&format!("{origin}/film.mkv"))
        ))
        .header(
            reqwest::header::RANGE,
            format!("bytes=0-{}", head_len + CHUNK - 1),
        )
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        fixture.origin.next_request().range(),
        Some(format!("bytes={head_len}-{}", head_len + CHUNK - 1)).as_deref(),
        "only the tail is asked for; the head is the cache's"
    );
    let mut first = vec![0u8; CHUNK as usize];
    response.read_exact(&mut first)?;
    assert_eq!(first[0], byte_at(0), "the player is inside the cached head");
    assert_eq!(fixture.handle.proxy_streams_live(), 1);

    assert_eq!(fixture.handle.close_proxy_streams("player-stitched"), 1);

    // What can still be read is what was already buffered on the way to this
    // socket; then the read breaks -- inside the head, well short of the
    // tail the registration used to cover alone.
    let mut read_after_close = 0u64;
    let mut buffer = vec![0u8; 64 * 1024];
    let outcome = loop {
        match response.read(&mut buffer) {
            Ok(0) => break Ok(()),
            Ok(n) => read_after_close += n as u64,
            Err(error) => break Err(error),
        }
    };
    assert!(
        outcome.is_err(),
        "the closed stream must break, not end tidily"
    );
    // Half the head is far more than any buffering accounts for (measured:
    // one chunk), and far less than the old route served -- all of the head
    // but the chunk hyper had in hand when the tail's first poll broke it.
    assert!(
        read_after_close < head_len / 2,
        "the read broke inside the head: {read_after_close} more bytes after the close, \
         of a {head_len}-byte head"
    );
    assert_eq!(fixture.handle.proxy_streams_live(), 0);

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

/// The proxy cache's chunk size, which is what every range in the tests
/// above is spelled in: a range shorter than one stores nothing, and a
/// boundary is where a narrowed fetch begins.
const CHUNK: u64 = stream_server::PROXY_CACHE_CHUNK_BYTES;

/// One response header, as text.
fn header<'a>(headers: &'a reqwest::header::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

/// The bytes a `206` says it is carrying, as a half-open range, read off its
/// `Content-Range`.
///
/// What a response was *framed* round, which is not always what was asked
/// for: the cache narrows a range to the run it holds. That framing is the
/// promise a body makes, so it is what a test naming the chunks a player is
/// inside has to read them from.
fn content_range(headers: &reqwest::header::HeaderMap) -> std::ops::Range<u64> {
    let value = header(headers, "content-range").expect("a 206 carries a Content-Range");
    let (range, _) = value
        .trim_start_matches("bytes ")
        .split_once('/')
        .expect("a Content-Range names the entity's length");
    let (first, last) = range
        .split_once('-')
        .expect("a Content-Range names its first and last byte");
    let first: u64 = first.parse().expect("a first byte");
    let last: u64 = last.parse().expect("a last byte");
    first..last + 1
}

/// Every chunk file the proxy cache holds: the files under the `.proxy`
/// root, which is inside the one torrent-data root and therefore turns up
/// in a plain walk of the cache root.
///
/// Temporary files are counted as chunks on purpose. A test that asserted
/// "no chunks" while a `.part` sat there would be asserting the wrong thing.
fn cached_chunks(fixture: &Fixture) -> Vec<std::path::PathBuf> {
    walk(&fixture.cache_root.path().join("cache"))
        .into_iter()
        .filter(|path| {
            path.is_file()
                && path
                    .components()
                    .any(|component| component.as_os_str() == ".proxy")
                // A chunk still being written is not one the cache holds --
                // it has no name a read would look at. Counting them made a
                // waiting test return on a chunk that had not landed yet.
                && !path.to_string_lossy().ends_with(".part")
        })
        .collect()
}

/// The chunk files the proxy cache holds, grouped by the entity directory
/// they are in: the bucket directories are per thousand chunks, and their
/// parent is the one directory a response's chunks share and no other
/// response's are in (`server::proxy_cache`'s key, then the framing the
/// entity is filed under).
///
/// What the cache holds is read this way wherever more than one URL is
/// fetched, because a total over the whole root is a moving number now: one
/// stream opening makes the one before it disposable, and its chunks go
/// while the next response is still arriving.
fn cached_chunks_by_entity(fixture: &Fixture) -> std::collections::BTreeMap<PathBuf, usize> {
    let mut by_entity = std::collections::BTreeMap::new();
    for path in cached_chunks(fixture) {
        if let Some(entity) = path.parent().and_then(|bucket| bucket.parent()) {
            *by_entity.entry(entity.to_path_buf()).or_insert(0usize) += 1;
        }
    }
    by_entity
}

/// The entity directories holding at least one chunk right now.
fn cached_entities(fixture: &Fixture) -> std::collections::BTreeSet<PathBuf> {
    cached_chunks_by_entity(fixture).into_keys().collect()
}

/// Nothing outside `before` holds a chunk: the response under test was not
/// cached. Said of the entities rather than of a count, so an entity being
/// deleted meanwhile -- which a stream opening starts -- cannot make a
/// refusal look like one.
fn assert_nothing_new_is_cached(
    fixture: &Fixture,
    before: &std::collections::BTreeSet<PathBuf>,
    what: &str,
) {
    for (entity, chunks) in cached_chunks_by_entity(fixture) {
        assert!(
            before.contains(&entity),
            "{what}: {} holds {chunks} chunks",
            entity.display()
        );
    }
}

/// Wait until an entity directory that was not in `before` holds `chunks`
/// chunks, and say which it is. Bounded so a regression fails instead of
/// hanging.
fn wait_for_new_entity(
    fixture: &Fixture,
    before: &std::collections::BTreeSet<PathBuf>,
    chunks: usize,
) -> PathBuf {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        for (entity, held) in cached_chunks_by_entity(fixture) {
            if !before.contains(&entity) && held >= chunks {
                return entity;
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    panic!(
        "no new entity ever held {chunks} chunks; the cache holds {:?}",
        cached_chunks_by_entity(fixture)
    );
}

/// The chunk indices the proxy cache holds, ascending. A chunk file is named
/// by its index inside a bucket directory, which is the whole of what says
/// which bytes it is.
fn cached_chunk_indices(fixture: &Fixture) -> Vec<u64> {
    let mut indices: Vec<u64> = cached_chunks(fixture)
        .into_iter()
        .filter_map(|path| path.file_name()?.to_str()?.parse().ok())
        .collect();
    indices.sort_unstable();
    indices
}

/// The longest contiguous run of chunks on the disk, as a half-open range of
/// indices. What the window kept, after a pass has reclaimed round it.
fn longest_cached_run(fixture: &Fixture) -> std::ops::Range<u64> {
    let indices = cached_chunk_indices(fixture);
    let mut best = 0..0;
    let mut run = 0..0;
    for index in indices {
        if run.end == index {
            run.end = index + 1;
        } else {
            run = index..index + 1;
        }
        if run.end - run.start > best.end - best.start {
            best = run.clone();
        }
    }
    best
}

/// Wait until the cache holds at least `chunks` of them.
///
/// A chunk is written on the blocking pool once its last byte has gone past,
/// so it lands a moment after the response the test already read. Bounded so
/// a regression fails instead of hanging, and the bound is generous because
/// it is not a timing assertion.
fn wait_for_chunks(fixture: &Fixture, chunks: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if cached_chunks(fixture).len() >= chunks {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    panic!(
        "the cache never held {chunks} chunks; it holds {:?}",
        cached_chunks(fixture)
    );
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
