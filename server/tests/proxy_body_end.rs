//! The one line `/proxy` writes when a proxied body ends.
//!
//! A player that fails part-way through a proxied stream leaves the server
//! nothing to be read back from except this: how many bytes actually left
//! it, of how many the response promised, how long that took and what ended
//! the body. So the test is over the log file rather than over a function --
//! what a field report contains is the whole subject -- and it covers every
//! way a body is served: relayed from an origin, rewritten as a playlist,
//! served whole off the proxy's cache, and cut short by a player that hung
//! up, or by an origin that hung up first. A `HEAD` is here too, for the
//! body it does not have.
//!
//! A binary of its own because `init_logging` installs the process's
//! subscriber once: a second logging test in the same binary would write
//! into whichever tempdir won the race (see `log_redaction.rs`, which is a
//! binary for the same reason).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Reading the log files back, which is what this test asserts over.
#[path = "support/log_lines.rs"]
mod log_lines;

/// The stage this file is about.
const STAGE: &str = "http_proxy_body_end";

/// The proxy cache's chunk size: a range shorter than one stores nothing,
/// so the range the cache-hit case asks for is spelled in these.
const CHUNK: u64 = stream_server::PROXY_CACHE_CHUNK_BYTES;

/// The entity the cache case reads twice, and the one it may keep: an
/// origin that identifies neither by `ETag` nor by `Last-Modified` is not
/// one the cache stores.
const CACHED_ETAG: &str = "\"the-movie\"";

/// What `/short.mp4` is, whole.
const SHORT_LEN: usize = 64 * 1024;

/// What `/slow.mp4` claims to be. Far more than a client reads before it
/// hangs up, which is what makes `bytes_sent` short of `requested_len` the
/// assertion it is.
const SLOW_LEN: usize = 64 * 1024 * 1024;

/// A string that appears nowhere but in the URLs this test sends: the
/// end-of-body line names origins, and a caller's query is the one place a
/// debrid or CDN credential lives.
const SECRET: &str = "s3cr3t-token-do-not-log";

/// One byte of a body at `offset`, so a range can be checked to have come
/// from the offset it claims.
fn byte_at(offset: usize) -> u8 {
    (offset % 251) as u8
}

/// The origin these tests proxy: three resources, one per way a body ends.
///
/// `/short.mp4` is delivered whole and identifies no entity, so the cache
/// keeps nothing of it; `/cached.mp4` answers ranges and names its entity,
/// which is what lets the second read of it be a hit; `/slow.mp4` promises
/// [`SLOW_LEN`] bytes and trickles them until the write fails, which is a
/// player hanging up seen from the other end.
struct Origin {
    addr: SocketAddr,
    requests: Arc<AtomicUsize>,
}

impl Origin {
    fn start() -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let requests = Arc::new(AtomicUsize::new(0));
        let counted = requests.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut socket) = stream else { break };
                let Ok(peer) = socket.try_clone() else { break };
                let counted = counted.clone();
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(peer);
                    let mut line = String::new();
                    if reader.read_line(&mut line).is_err() {
                        return;
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
                    counted.fetch_add(1, Ordering::SeqCst);
                    if line.contains("/slow.mp4") {
                        Self::trickle(&mut socket);
                    } else if line.contains("/cached.mp4") {
                        Self::ranged(&mut socket, range.as_deref());
                    } else if line.contains("/truncated.mp4") {
                        Self::truncated(&mut socket);
                    } else if line.contains("/list.m3u8") {
                        Self::playlist(&mut socket);
                    } else {
                        Self::whole(&mut socket);
                    }
                });
            }
        });
        Ok(Self { addr, requests })
    }

    /// [`SHORT_LEN`] bytes, named by no validator, so nothing of it is kept.
    fn whole(socket: &mut TcpStream) {
        let body: Vec<u8> = (0..SHORT_LEN).map(byte_at).collect();
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\n\
                 Content-Length: {SHORT_LEN}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        );
        let _ = socket.write_all(&body);
        let _ = socket.flush();
    }

    /// The range that was asked for, of an entity the cache may keep.
    fn ranged(socket: &mut TcpStream, range: Option<&str>) {
        let length = (CHUNK * 4) as usize;
        let (first, last) = range
            .and_then(|value| {
                let (first, last) = value.trim_start_matches("bytes=").split_once('-')?;
                let first: usize = first.parse().ok()?;
                let last: usize = if last.is_empty() {
                    length - 1
                } else {
                    last.parse().ok()?
                };
                Some((first, last))
            })
            .unwrap_or((0, length - 1));
        let body: Vec<u8> = (first..=last).map(byte_at).collect();
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\n\
                 Content-Type: video/mp4\r\nETag: {CACHED_ETAG}\r\n\
                 Content-Range: bytes {first}-{last}/{length}\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        );
        let _ = socket.write_all(&body);
        let _ = socket.flush();
    }

    /// An origin that promises [`SHORT_LEN`] bytes and hangs up after a
    /// few: the failure a relayed body reports as its own.
    fn truncated(socket: &mut TcpStream) {
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\n\
                 Content-Length: {SHORT_LEN}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        );
        let _ = socket.write_all(&vec![0u8; 8 * 1024]);
        let _ = socket.flush();
    }

    /// A playlist, which `/proxy` does not relay but rewrites: the body the
    /// player gets is ours, and is framed as it is written rather than
    /// promised a length.
    fn playlist(socket: &mut TcpStream) {
        let body = "#EXTM3U\n#EXTINF:4,\nseg1.ts\n#EXTINF:4,\nseg2.ts\n";
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/x-mpegURL\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        );
        let _ = socket.write_all(body.as_bytes());
        let _ = socket.flush();
    }

    /// [`SLOW_LEN`] bytes promised, delivered a few at a time for as long as
    /// anyone is reading them.
    fn trickle(socket: &mut TcpStream) {
        let _ = socket.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\n\
                 Content-Length: {SLOW_LEN}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        );
        let chunk = vec![0u8; 8 * 1024];
        for _ in 0..(SLOW_LEN / chunk.len()) {
            if socket.write_all(&chunk).is_err() || socket.flush().is_err() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// Percent-encodes everything but the unreserved set, which is what a `d=`
/// value needs.
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

/// The chunk files the proxy cache holds, which is what says a fill has
/// landed and the next read of the same bytes will be a hit.
fn cached_chunks(cache_root: &std::path::Path) -> usize {
    fn walk(dir: &std::path::Path, found: &mut usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, found);
            } else if path
                .components()
                .any(|component| component.as_os_str() == ".proxy")
                // A chunk still being written has no name a read looks at.
                && !path.to_string_lossy().ends_with(".part")
            {
                *found += 1;
            }
        }
    }
    let mut found = 0;
    walk(cache_root, &mut found);
    found
}

/// A body delivered whole, one the origin cut short, one rewritten as a
/// playlist, one served off the cache and one the player hung up on: each
/// is one line, each saying what left the server and what ended it. And a
/// `HEAD`, which is none of them, because it is no body.
#[test]
fn every_proxied_body_says_what_left_the_server() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let config_dir = dir.path().join("config");
    let cache_dir = dir.path().join("cache");
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.clone()),
        cache_dir: Some(cache_dir.clone()),
        init_logging: true,
        resolve_dht_bootstrap_names: false,
        use_public_trackers: false,
        pins: Some(Default::default()),
        ..stream_server::ServerConfig::default()
    })?;
    let base = format!("http://{}", handle.http_addr());
    let origin = Origin::start()?;
    let origin_base = format!("http://{}", origin.addr);
    let client = reqwest::blocking::Client::new();

    // A `HEAD` first, whose response carries no body at all: whatever the
    // rest of this test finds in the log, none of it may be about this.
    let short = format!(
        "{base}/proxy/?d={}",
        encode(&format!("{origin_base}/short.mp4?token={SECRET}"))
    );
    let head = client.head(&short).send()?;
    assert_eq!(head.status(), reqwest::StatusCode::OK);

    // A body delivered whole.
    let response = client.get(&short).send()?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.bytes()?.len(), SHORT_LEN);

    let complete =
        log_lines::wait_for_line(&config_dir, STAGE, "a body delivered whole", |fields| {
            fields["reason"] == "complete" && fields["answered_by"] == origin_base.as_str()
        })?;
    let fields = &complete["fields"];
    assert_eq!(fields["bytes_sent"], SHORT_LEN);
    assert_eq!(
        fields["requested_len"], SHORT_LEN,
        "what the response promised, which a failed body falls short of"
    );
    assert_eq!(fields["status"], 200);
    assert_eq!(fields["error"], "", "nothing ended this one");
    assert_eq!(fields["method"], "GET");
    assert_eq!(fields["range"], "none");
    assert!(
        fields["duration_ms"].is_u64(),
        "how long the body was open: {fields}"
    );
    assert_eq!(
        fields["target_origin"], origin_base,
        "the origin the caller named, and never the URL it named it with"
    );

    // An origin that hung up part-way through the body it promised, which
    // the relayed body fails with as its own.
    let truncated = client
        .get(format!(
            "{base}/proxy/?d={}",
            encode(&format!("{origin_base}/truncated.mp4?token={SECRET}"))
        ))
        .send()?;
    assert_eq!(truncated.status(), reqwest::StatusCode::OK);
    assert!(
        truncated.bytes().is_err(),
        "the body stopped short of the length it promised"
    );

    let broken =
        log_lines::wait_for_line(&config_dir, STAGE, "an origin that hung up", |fields| {
            fields["reason"] == "reader-error"
        })?;
    let fields = &broken["fields"];
    assert_eq!(fields["requested_len"], SHORT_LEN);
    assert!(
        fields["bytes_sent"]
            .as_u64()
            .is_some_and(|sent| sent < SHORT_LEN as u64),
        "short of what it promised: {fields}"
    );
    assert!(
        fields["error"]
            .as_str()
            .is_some_and(|error| !error.is_empty()),
        "and the line says what broke it: {fields}"
    );

    // The `HEAD` answered before that `GET` was sent, so by now a line about
    // it would be in the file. A body that is never sent is not a body.
    assert!(
        log_lines::lines_at_stage(&config_dir, STAGE)
            .iter()
            .all(|line| line["fields"]["method"] != "HEAD"),
        "a HEAD was reported as a body that ended: {:#?}",
        log_lines::lines_at_stage(&config_dir, STAGE)
    );

    // A playlist, which is not relayed but rewritten: our body, framed as it
    // is written, so it promises no length at all.
    let playlist = client
        .get(format!(
            "{base}/proxy/?d={}",
            encode(&format!("{origin_base}/list.m3u8?token={SECRET}"))
        ))
        .send()?;
    assert_eq!(playlist.status(), reqwest::StatusCode::OK);
    let rewritten = playlist.text()?;
    assert!(rewritten.contains("/proxy/"), "the lines were rewritten");

    let written = log_lines::wait_for_line(&config_dir, STAGE, "a rewritten playlist", |fields| {
        fields["requested_len"] == 0 && fields["answered_by"] == origin_base.as_str()
    })?;
    let fields = &written["fields"];
    assert_eq!(fields["reason"], "complete");
    assert_eq!(
        fields["bytes_sent"],
        rewritten.len(),
        "what left the server is the rewritten body, not the origin's"
    );

    // A range served whole off the cache, which used to leave no record at
    // all that a player had been served anything.
    let cached_url = format!(
        "{base}/proxy/?d={}",
        encode(&format!("{origin_base}/cached.mp4?token={SECRET}"))
    );
    let two_chunks = format!("bytes=0-{}", CHUNK * 2 - 1);
    let first = client
        .get(&cached_url)
        .header(reqwest::header::RANGE, &two_chunks)
        .send()?;
    assert_eq!(first.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(first.bytes()?.len() as u64, CHUNK * 2);
    let fetches = origin.requests.load(Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_secs(20);
    while cached_chunks(&cache_dir) < 2 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    let second = client
        .get(&cached_url)
        .header(reqwest::header::RANGE, &two_chunks)
        .send()?;
    assert_eq!(second.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(second.bytes()?.len() as u64, CHUNK * 2);
    assert_eq!(
        origin.requests.load(Ordering::SeqCst),
        fetches,
        "a range the cache holds whole makes no origin request"
    );

    let hit = log_lines::wait_for_line(
        &config_dir,
        STAGE,
        "a range served off the cache",
        |fields| fields["answered_by"] == "cache",
    )?;
    let fields = &hit["fields"];
    assert_eq!(fields["reason"], "complete");
    assert_eq!(fields["bytes_sent"], CHUNK * 2);
    assert_eq!(fields["requested_len"], CHUNK * 2);
    assert_eq!(fields["status"], 206);
    assert_eq!(fields["range"], two_chunks);

    // And a player that hangs up part-way, which is the distinction the
    // whole line exists for: a body that stopped short of what it promised.
    let mut socket = TcpStream::connect(handle.http_addr())?;
    socket.set_read_timeout(Some(Duration::from_secs(10)))?;
    write!(
        socket,
        "GET /proxy/?d={} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        encode(&format!("{origin_base}/slow.mp4?token={SECRET}")),
        handle.http_addr()
    )?;
    socket.flush()?;
    let mut read = 0;
    let mut buffer = [0u8; 4096];
    while read < 8 * 1024 {
        match socket.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(n) => read += n,
        }
    }
    assert!(read > 0, "the proxied body started arriving");
    drop(socket);

    let cut = log_lines::wait_for_line(&config_dir, STAGE, "a player that hung up", |fields| {
        fields["reason"] == "client-disconnect"
    })?;
    let fields = &cut["fields"];
    assert_eq!(fields["requested_len"], SLOW_LEN);
    let bytes_sent = fields["bytes_sent"].as_u64().expect("a byte count");
    assert!(
        bytes_sent < SLOW_LEN as u64,
        "a body the player hung up on is short of what it promised: {fields}"
    );

    // The line is the one place a failed playback is read from, and it may
    // not be the place a caller's credentials are filed.
    let lines = log_lines::lines_at_stage(&config_dir, STAGE);
    assert!(
        !format!("{lines:?}").contains(SECRET),
        "a caller's credential reached the end-of-body line: {lines:#?}"
    );

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}
