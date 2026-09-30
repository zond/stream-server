//! Media ids and the blocking reader over them (`stream_server::media`,
//! `docs/design/media-pipeline.md` §2.3, §2.4): `register`, `resolve`,
//! `open_reader`, `set_buffer` and the reader's `read`/`seek`/`cancel`/drop,
//! over each kind of source the registry resolves -- a torrent file, a link
//! through `/proxy`'s cache, a Google Drive file.
//!
//! Every server here is offline and on ephemeral ports, and every origin is
//! a loopback fake of this file's own ([`Origin`]). A [`MediaReader`] blocks
//! its caller and refuses to be called from inside a runtime, so the tests
//! call it from their own threads, which have none.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use stream_server::{MediaId, MediaReader, MediaSpec, PlayToken, Refusal, ServerConfig};

/// Why a test that seeds its own torrent data runs with the pin set
/// unknown, and which tests may not (see the module).
#[path = "support/fixture_pins.rs"]
mod fixture_pins;

/// The offline config, the control client and a real torrent.
#[path = "support/torrent_fixtures.rs"]
mod torrent_fixtures;
use torrent_fixtures::{bearer_client, offline_config, piece_store, real_torrent};

/// A bound on a mistake, never a wait a correct run spends: a hash check,
/// a read that should answer, a cancel that should wake one.
const BOUND: Duration = Duration::from_secs(120);

/// The fixtures' piece length (`real_torrent`'s).
const PIECE: usize = 16 * 1024;

/// The fixture pattern: a byte that says where it came from.
fn byte_at(offset: usize) -> u8 {
    (offset.wrapping_mul(7) % 251) as u8
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(byte_at).collect()
}

// --- Torrent fixtures -------------------------------------------------------

/// Seed the pieces of `torrent` whose index `keep` accepts, in the piece
/// store where the server reads them, from the files under `content` in
/// the order the metainfo lists them (never the fixture's write order: see
/// AGENTS.md on torrent file order). Whole pieces only: a fixture's files
/// are whole numbers of pieces. **After the server has started**: the
/// launch sweep deletes a piece directory seeded before it.
fn seed(cache_root: &Path, torrent: &[u8], content: &Path, keep: impl Fn(usize) -> bool) {
    let meta = librqbit::torrent_from_bytes(torrent).expect("parse the torrent back");
    let info_hash = meta.info_hash.as_string();
    let info = meta.info.data.validate().expect("validated metainfo");
    let piece_length = info.lengths().default_piece_length() as u64;
    let mut blob = Vec::new();
    for file in info.iter_file_details() {
        let path = content.join(file.filename.to_pathbuf());
        blob.extend(std::fs::read(&path).expect("a fixture file the torrent names"));
    }
    let layout = enginefs::piece_store::PieceLayout::new(
        piece_length,
        blob.len() as u64,
        [enginefs::piece_store::FileSpec {
            len: blob.len() as u64,
            padding: false,
        }],
    )
    .expect("a layout for the fixture");
    let pieces = enginefs::piece_store::PieceStore::new(
        piece_store(cache_root).torrent_dir(&info_hash),
        Arc::new(layout),
    );
    for (index, piece) in blob.chunks(piece_length as usize).enumerate() {
        if !keep(index) {
            continue;
        }
        let path = pieces.piece_path(index as u32);
        std::fs::create_dir_all(path.parent().expect("a bucket")).expect("piece bucket");
        std::fs::write(&path, piece).expect("write a piece");
    }
}

/// A server with one torrent of the named films (each `film_len` bytes of
/// [`payload`]) added and checked, the pieces `keep` accepts seeded: the
/// server, its base URL, the info hash and each film's file index, looked
/// up by name.
struct TorrentFixture {
    handle: stream_server::ServerHandle,
    base: String,
    info_hash: String,
    films: Vec<(String, usize)>,
    _dirs: [tempfile::TempDir; 3],
}

impl TorrentFixture {
    fn start(
        config: ServerConfig,
        names: &[&str],
        film_len: usize,
        keep: impl Fn(usize) -> bool,
    ) -> anyhow::Result<Self> {
        let config_dir = tempfile::tempdir()?;
        let cache_dir = tempfile::tempdir()?;
        let src = tempfile::tempdir()?;
        let content = src.path().join("Films");
        std::fs::create_dir_all(&content)?;
        for name in names {
            std::fs::write(content.join(name), payload(film_len))?;
        }
        let (torrent, info_hash) = real_torrent(&content);
        let cache_root = stream_server::resolved_path(&cache_dir.path().join("cache"));
        stream_server::pretend_volume_space(&cache_root, u64::MAX);
        let handle = stream_server::start(ServerConfig {
            http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            config_dir: Some(config_dir.path().join("config")),
            cache_dir: Some(cache_root.clone()),
            ..config
        })?;
        seed(&cache_root, &torrent, &content, keep);
        let base = format!("http://{}", handle.http_addr());
        bearer_client(&handle)?
            .post(format!("{base}/create"))
            .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
            .send()?
            .error_for_status()?;
        let deadline = Instant::now() + BOUND;
        let stats = loop {
            let stats = serde_json::to_value(handle.engine_stats(&info_hash, &[])?)?;
            match stats["phase"].as_str() {
                Some("checking") | Some("resolvingMetadata") => {
                    anyhow::ensure!(Instant::now() < deadline, "never checked: {stats}");
                    std::thread::sleep(Duration::from_millis(50));
                }
                _ => break stats,
            }
        };
        let films = names
            .iter()
            .map(|name| {
                let index = stats["files"]
                    .as_array()
                    .expect("files")
                    .iter()
                    .position(|file| file["name"] == *name)
                    .unwrap_or_else(|| panic!("no file {name} in {stats}"));
                (name.to_string(), index)
            })
            .collect();
        Ok(Self {
            handle,
            base,
            info_hash,
            films,
            _dirs: [config_dir, cache_dir, src],
        })
    }

    fn index(&self, name: &str) -> usize {
        self.films
            .iter()
            .find(|(film, _)| film == name)
            .map(|(_, index)| *index)
            .expect("a film of the fixture")
    }

    /// The film's streaming URL, as stremio-core builds it.
    fn url(&self, name: &str) -> url::Url {
        url::Url::parse(&format!(
            "{}/{}/{}",
            self.base,
            self.info_hash,
            self.index(name)
        ))
        .expect("a streaming URL")
    }

    fn register(&self, name: &str) -> anyhow::Result<MediaId> {
        self.handle
            .register(MediaSpec::StreamingUrl(self.url(name)))
    }

    /// What a panel is told is committed for sharing of the film.
    fn committed(&self, name: &str) -> anyhow::Result<Option<u64>> {
        Ok(self
            .handle
            .stream_numbers(self.url(name).as_str())?
            .and_then(|numbers| numbers.sharing)
            .and_then(|sharing| sharing.committed_bytes))
    }

    fn stop(self) -> anyhow::Result<()> {
        self.handle.shutdown()?;
        self.handle.join()?;
        Ok(())
    }
}

/// Everything from the reader's position to its end.
fn read_to_end(reader: &mut MediaReader) -> std::io::Result<Vec<u8>> {
    let mut read = Vec::new();
    let mut buf = vec![0u8; 7 * 1024];
    loop {
        match reader.read(&mut buf)? {
            0 => return Ok(read),
            n => read.extend_from_slice(&buf[..n]),
        }
    }
}

/// `f` on a thread of its own, answered within [`BOUND`] or an error that
/// says what never answered. The thread has no runtime, as mpv's has none.
fn within<T: Send + 'static>(
    what: &str,
    f: impl FnOnce() -> T + Send + 'static,
) -> anyhow::Result<T> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(BOUND)
        .map_err(|_| anyhow::anyhow!("{what} did not answer within {BOUND:?}"))
}

/// Poll `ready` until it holds, or fail naming `what`.
fn until(what: &str, mut ready: impl FnMut() -> anyhow::Result<bool>) -> anyhow::Result<()> {
    let deadline = Instant::now() + BOUND;
    while !ready()? {
        anyhow::ensure!(Instant::now() < deadline, "never: {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}

// --- The loopback origin ----------------------------------------------------

/// The refresh token the fake Drive account is reachable by.
const REFRESH_TOKEN: &str = "refresh-tok-media-7c1e";
/// Drive's id for the fake file.
const FILE_ID: &str = "1MediaIdFixtureFile";
/// The length of every file the origin serves.
const ORIGIN_LEN: usize = 300 * 1024;

/// One loopback listener standing in for a CDN, Google Drive and the
/// pairing service at once: `/film.bin` serves ranges, `/whole.bin`
/// answers every request with the whole file (an origin that will not
/// range), `/refresh` mints Drive tokens and `/drive/v3/files/...` serves
/// ranges to whoever holds one.
struct Origin {
    addr: SocketAddr,
    /// Requests for a byte of `/film.bin` or the Drive file.
    ranged: Arc<AtomicUsize>,
}

impl Origin {
    fn start() -> anyhow::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let addr = listener.local_addr()?;
        let ranged = Arc::new(AtomicUsize::new(0));
        let counted = ranged.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let counted = counted.clone();
                std::thread::spawn(move || serve(stream, &counted));
            }
        });
        Ok(Self { addr, ranged })
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }
}

fn serve(mut stream: TcpStream, ranged: &AtomicUsize) {
    let Ok(second) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(second);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
        return;
    }
    let (mut range, mut authorised, mut length) = (None, false, 0usize);
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if let Some(value) = lower.strip_prefix("range: ") {
            range = Some(value.trim().to_string());
        } else if let Some(value) = lower.strip_prefix("content-length: ") {
            length = value.trim().parse().unwrap_or(0);
        } else if lower.starts_with("authorization: bearer access-tok-") {
            authorised = true;
        }
    }
    let path = request_line.split_whitespace().nth(1).unwrap_or("");
    if path == "/refresh" {
        let mut body = vec![0u8; length];
        let _ = std::io::Read::read_exact(&mut reader, &mut body);
        let body = String::from_utf8_lossy(&body);
        if body.contains(REFRESH_TOKEN) {
            respond(
                &mut stream,
                "200 OK",
                "application/json",
                b"{\"accessToken\":\"access-tok-0001\",\"expiresIn\":3599}",
            );
        } else {
            respond(
                &mut stream,
                "401 Unauthorized",
                "application/json",
                b"{\"pairAgain\":true}",
            );
        }
        return;
    }
    let is_drive = path.starts_with(&format!("/drive/v3/files/{FILE_ID}"));
    if is_drive && !authorised {
        respond(&mut stream, "401 Unauthorized", "application/json", b"{}");
        return;
    }
    let body_of = |first: usize, last: usize| -> Vec<u8> { (first..=last).map(byte_at).collect() };
    if path == "/whole.bin" {
        respond(
            &mut stream,
            "200 OK",
            "video/mp4",
            &body_of(0, ORIGIN_LEN - 1),
        );
        return;
    }
    if path != "/film.bin" && !is_drive {
        respond(&mut stream, "404 Not Found", "text/plain", b"");
        return;
    }
    let (first, last) = range
        .as_deref()
        .and_then(|header| header.trim_start_matches("bytes=").split_once('-'))
        .map(|(first, last)| {
            (
                first.parse().unwrap_or(0),
                last.parse::<usize>()
                    .unwrap_or(ORIGIN_LEN - 1)
                    .min(ORIGIN_LEN - 1),
            )
        })
        .unwrap_or((0, ORIGIN_LEN - 1));
    ranged.fetch_add(1, Ordering::SeqCst);
    let body = body_of(first, last);
    let head = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Type: video/x-matroska\r\nETag: \"media-fixture\"\r\n\
         Accept-Ranges: bytes\r\nContent-Range: bytes {first}-{last}/{ORIGIN_LEN}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(&body);
    let _ = stream.flush();
}

fn respond(stream: &mut TcpStream, status: &str, content_type: &str, body: &[u8]) {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
}

/// A server with nothing of its own on the network, pointed at `origin`
/// for Drive.
fn origin_server(
    origin: &Origin,
) -> anyhow::Result<(stream_server::ServerHandle, [tempfile::TempDir; 2])> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let cache_root = stream_server::resolved_path(&cache_dir.path().join("cache"));
    stream_server::pretend_volume_space(&cache_root, u64::MAX);
    let handle = stream_server::start(ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root),
        drive_refresh_endpoint: Some(url::Url::parse(&origin.url("/refresh"))?),
        drive_api_base: Some(url::Url::parse(&origin.url("/"))?),
        ..offline_config()
    })?;
    Ok((handle, [config_dir, cache_dir]))
}

/// The `/proxy` URL stremio-core builds for `target`.
fn proxy_url(handle: &stream_server::ServerHandle, target: &str) -> url::Url {
    url::Url::parse(&format!(
        "http://{}/proxy/?d={}",
        handle.http_addr(),
        urlencoding::encode(target)
    ))
    .expect("a proxy URL")
}

// --- The tests --------------------------------------------------------------

/// **A torrent id reads from the top, reads from where a seek put it, and
/// ends.** Registered by the URL stremio-core builds with `-1` (the
/// largest video, as the stream route picks it), resolved to the one film,
/// read whole, then read again from an offset inside a piece -- a seek is
/// a reopen at the offset, and a reader that only moved its position
/// would answer the top of the file again -- and at the end `Ok(0)`, again
/// and again.
#[test]
fn a_torrent_id_reads_seeks_and_ends() -> anyhow::Result<()> {
    const FILM_LEN: usize = 4 * PIECE;
    const SEEK_TO: u64 = (2 * PIECE + 100) as u64;
    let fixture = TorrentFixture::start(
        fixture_pins::keep_what_the_fixture_seeded(offline_config()),
        &["film.mkv"],
        FILM_LEN,
        |_| true,
    )?;
    let film = payload(FILM_LEN);
    let url = url::Url::parse(&format!("{}/{}/-1", fixture.base, fixture.info_hash))?;
    let id = fixture.handle.register(MediaSpec::StreamingUrl(url))?;

    let resolved = fixture.handle.resolve(&id)?;
    assert_eq!(resolved.name, "film.mkv");
    assert_eq!(resolved.content_type, "video/x-matroska");
    assert_eq!(resolved.len, FILM_LEN as u64);
    assert!(resolved.in_process);
    assert_eq!(resolved.proxy_url, None);

    let mut reader = fixture.handle.open_reader(&id, None)?;
    assert_eq!(reader.len(), FILM_LEN as u64);
    let (reader, whole, tail, end) = within("the torrent reader", move || {
        let whole = read_to_end(&mut reader);
        let at = reader.seek(SEEK_TO);
        let tail = read_to_end(&mut reader);
        let mut buf = [0u8; 16];
        let end = (reader.read(&mut buf), reader.read(&mut buf));
        (reader, whole, at.and(tail), end)
    })?;
    assert_eq!(whole?, film, "the read from the top");
    assert_eq!(
        tail?,
        film[SEEK_TO as usize..],
        "the read after the seek did not start where the seek put it"
    );
    assert_eq!((end.0?, end.1?), (0, 0), "the end of the file is Ok(0)");
    drop(reader);
    fixture.stop()
}

/// **A `/proxy` id reads through the proxy cache, seeks and ends**, and a
/// link whose host will not range is not a refusal: it resolves
/// `in_process: false` with the `/proxy` URL a player can read it forward
/// through -- and only a reader is refused for it, since nothing in this
/// process could seek it.
#[test]
fn a_proxy_id_reads_seeks_and_ends_and_a_non_ranging_one_is_out_of_process() -> anyhow::Result<()> {
    const SEEK_TO: u64 = 200 * 1024 + 3;
    let origin = Origin::start()?;
    let (handle, _dirs) = origin_server(&origin)?;
    let expected = payload(ORIGIN_LEN);

    let id = handle.register(MediaSpec::StreamingUrl(proxy_url(
        &handle,
        &origin.url("/film.bin"),
    )))?;
    let resolved = handle.resolve(&id)?;
    assert_eq!(resolved.name, "film.bin");
    assert_eq!(resolved.content_type, "video/x-matroska");
    assert_eq!(resolved.len, ORIGIN_LEN as u64);
    assert!(resolved.in_process);

    let reader = handle.open_reader(&id, None)?;
    let (reader, whole, tail, end) = within("the proxy reader", move || {
        let mut reader = reader;
        let whole = read_to_end(&mut reader);
        let at = reader.seek(SEEK_TO);
        let tail = read_to_end(&mut reader);
        let end = reader.read(&mut [0u8; 8]);
        (reader, whole, at.and(tail), end)
    })?;
    assert_eq!(whole?, expected);
    assert_eq!(tail?, expected[SEEK_TO as usize..]);
    assert_eq!(end?, 0);
    drop(reader);

    // With a play, the reader is the viewer's player on a proxied stream:
    // its session is off every torrent file, as a `p=` request through
    // `/proxy` puts it.
    let played = handle.open_reader(
        &id,
        Some(PlayToken {
            token: "tv.1".to_string(),
            buffer: Default::default(),
        }),
    )?;
    assert_eq!(
        handle.play_session_of("tv.1"),
        Some(enginefs::retention::sessions::Played::Elsewhere)
    );
    drop(played);

    let target = origin.url("/whole.bin");
    let url = proxy_url(&handle, &target);
    let id = handle.register(MediaSpec::StreamingUrl(url.clone()))?;
    let resolved = handle.resolve(&id)?;
    assert!(!resolved.in_process, "{resolved:?}");
    assert_eq!(resolved.proxy_url.as_deref(), Some(url.as_str()));
    assert_eq!(resolved.name, "whole.bin");
    assert_eq!(
        handle.open_reader(&id, None).err(),
        Some(Refusal::NoRanges),
        "a reader over an origin that will not range"
    );

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **A Drive id reads by range under a grant the supplier hands over at
/// resolve**, against the fake Drive and pairing service; and a supplier
/// with no grant is `noGrant`, not a request with nothing behind it.
#[test]
fn a_drive_id_reads_under_the_grant_its_supplier_gives() -> anyhow::Result<()> {
    const SEEK_TO: u64 = 123_457;
    let origin = Origin::start()?;
    let (handle, _dirs) = origin_server(&origin)?;
    let expected = payload(ORIGIN_LEN);

    let asked = Arc::new(AtomicUsize::new(0));
    let grant: stream_server::GrantSupplier = {
        let asked = asked.clone();
        Arc::new(move || {
            asked.fetch_add(1, Ordering::SeqCst);
            Some(REFRESH_TOKEN.to_string())
        })
    };
    let id = handle.register(MediaSpec::Drive {
        file_id: FILE_ID.to_string(),
        name: Some("A Film.mkv".to_string()),
        grant,
    })?;
    assert_eq!(
        asked.load(Ordering::SeqCst),
        0,
        "registering asked for the grant"
    );
    let resolved = handle.resolve(&id)?;
    assert_eq!(asked.load(Ordering::SeqCst), 1);
    assert_eq!(resolved.name, "A Film.mkv");
    assert_eq!(resolved.len, ORIGIN_LEN as u64);
    assert!(resolved.in_process);

    let reader = handle.open_reader(&id, None)?;
    let (reader, head, tail) = within("the Drive reader", move || {
        let mut reader = reader;
        let mut head = vec![0u8; 1000];
        let mut filled = 0;
        let mut failed = None;
        while filled < head.len() {
            match reader.read(&mut head[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(error) => {
                    failed = Some(error);
                    break;
                }
            }
        }
        head.truncate(filled);
        let tail = reader.seek(SEEK_TO).and_then(|_| read_to_end(&mut reader));
        (reader, failed.map_or(Ok(head), Err), tail)
    })?;
    assert_eq!(head?, expected[..1000]);
    assert_eq!(tail?, expected[SEEK_TO as usize..]);
    assert!(origin.ranged.load(Ordering::SeqCst) > 0);
    drop(reader);

    let unlinked = handle.register(MediaSpec::Drive {
        file_id: FILE_ID.to_string(),
        name: None,
        grant: Arc::new(|| None),
    })?;
    assert_eq!(handle.resolve(&unlinked).err(), Some(Refusal::NoGrant));

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// The fixture for a read that parks: one film whose last piece nobody
/// has -- it is not seeded, and nothing on this offline server could
/// fetch it -- and a reader seeked into that piece. Seeded and kept
/// (`fixture_pins`): the claim is about a piece that is missing, not one
/// taken.
fn parked_reader() -> anyhow::Result<(TorrentFixture, MediaReader)> {
    const FILM_LEN: usize = 4 * PIECE;
    let fixture = TorrentFixture::start(
        fixture_pins::keep_what_the_fixture_seeded(offline_config()),
        &["film.mkv"],
        FILM_LEN,
        |piece| piece != 3,
    )?;
    let id = fixture.register("film.mkv")?;
    let mut reader = fixture.handle.open_reader(&id, None)?;
    let reader = within("a seek into the missing piece", move || {
        reader.seek((3 * PIECE + 10) as u64).map(|_| reader)
    })??;
    Ok((fixture, reader))
}

/// **A read parked on a piece nobody has is woken by `cancel`**, with
/// `Interrupted`, and every later call answers the same at once: the
/// cancel is not a command queued behind the read it interrupts, and it
/// is sticky, as mpv's `cancel_fn` says.
#[test]
fn a_cancel_wakes_a_read_parked_on_a_missing_piece_and_sticks() -> anyhow::Result<()> {
    let (fixture, reader) = parked_reader()?;
    let canceller = reader.canceller();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = reader;
        let parked = reader.read(&mut [0u8; 64]);
        let _ = tx.send((parked, reader));
    });
    // The read has reached the task and is being awaited: what the cancel
    // is about to overtake.
    until("the read is in flight", || Ok(canceller.is_waiting()))?;
    canceller.cancel();
    let (parked, mut reader) = rx
        .recv_timeout(BOUND)
        .map_err(|_| anyhow::anyhow!("the cancel did not wake the parked read"))?;
    assert_eq!(
        parked.err().map(|error| error.kind()),
        Some(std::io::ErrorKind::Interrupted)
    );
    let (reader, later) = within("the calls after a cancel", move || {
        let read = reader.read(&mut [0u8; 8]).err().map(|error| error.kind());
        let seek = reader.seek(0).err().map(|error| error.kind());
        (reader, (read, seek))
    })?;
    assert_eq!(
        later,
        (
            Some(std::io::ErrorKind::Interrupted),
            Some(std::io::ErrorKind::Interrupted)
        ),
        "a cancel is sticky"
    );
    drop(reader);
    fixture.stop()
}

/// **A server stopped under a blocked read answers that read with an
/// error**, and never leaves it waiting on a runtime that is gone.
#[test]
fn a_server_stopped_under_a_blocked_read_answers_it_with_an_error() -> anyhow::Result<()> {
    let (fixture, reader) = parked_reader()?;
    let canceller = reader.canceller();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = reader;
        let parked = reader.read(&mut [0u8; 64]);
        let after = reader.read(&mut [0u8; 64]);
        let _ = tx.send((parked, after));
    });
    until("the read is in flight", || Ok(canceller.is_waiting()))?;
    fixture.handle.shutdown()?;
    let (parked, after) = rx
        .recv_timeout(BOUND)
        .map_err(|_| anyhow::anyhow!("the stop left the blocked read waiting"))?;
    let parked = parked.expect_err("a read answered by a server that stopped");
    assert_ne!(parked.kind(), std::io::ErrorKind::Interrupted, "{parked}");
    assert!(after.is_err(), "a read after the stop answered {after:?}");
    fixture.handle.join()?;
    Ok(())
}

/// **Dropping a reader on a thread with no runtime panics nothing, and
/// ends its stream**: the drop is a channel closing, and the task drops
/// the source -- whose end spawns -- on the runtime. `playing` is the
/// registration, read as the activity light reads it.
#[test]
fn a_reader_dropped_off_the_runtime_ends_its_stream() -> anyhow::Result<()> {
    let fixture = TorrentFixture::start(
        fixture_pins::keep_what_the_fixture_seeded(offline_config()),
        &["film.mkv"],
        2 * PIECE,
        |_| true,
    )?;
    let id = fixture.register("film.mkv")?;
    assert!(!fixture.handle.background_traffic()?.playing);
    let reader = fixture.handle.open_reader(&id, None)?;
    assert!(
        fixture.handle.background_traffic()?.playing,
        "the reader was opened without registering its stream"
    );
    std::thread::spawn(move || drop(reader))
        .join()
        .map_err(|_| anyhow::anyhow!("dropping the reader panicked"))?;
    until("the dropped reader's stream ends", || {
        Ok(!fixture.handle.background_traffic()?.playing)
    })?;
    fixture.stop()
}

/// **A torrent reader opened with a play is the viewer's playback, from the
/// open**: the session moves to its film before a byte is read, and the
/// film draws (under the default budget, which covers it, the draw is the
/// whole film). A reader of the other film opened without a play is an
/// aside: the session stays where it was and that film draws nothing.
#[test]
fn a_reader_with_a_play_moves_the_session_and_draws_and_one_without_does_neither()
-> anyhow::Result<()> {
    const FILM_LEN: usize = 8 * PIECE;
    let fixture = TorrentFixture::start(offline_config(), &["a.bin", "b.bin"], FILM_LEN, |_| true)?;
    let a = fixture.index("a.bin");
    let played = fixture.register("a.bin")?;
    let aside = fixture.register("b.bin")?;
    // An id not resolved yet has no numbers, and asking resolves nothing.
    assert_eq!(fixture.handle.media_stream_numbers(&aside)?, None);
    let session = Some(enginefs::retention::sessions::Played::Torrent {
        info_hash: fixture.info_hash.clone(),
        file_idx: a,
        shares: true,
    });

    let reader = fixture.handle.open_reader(
        &played,
        Some(PlayToken {
            token: "tv.1".to_string(),
            buffer: Default::default(),
        }),
    )?;
    assert_eq!(
        fixture.handle.play_session_of("tv.1"),
        session,
        "opening the played reader did not move the viewer's session"
    );
    let (reader, film) = within("the played reader", move || {
        let mut reader = reader;
        let film = read_to_end(&mut reader);
        (reader, film)
    })?;
    assert_eq!(film?, payload(FILM_LEN));
    assert_eq!(
        fixture.committed("a.bin")?,
        Some(FILM_LEN as u64),
        "the played reader's film shares nothing"
    );
    // And a panel asking by id is told what one asking by URL is.
    let by_id = fixture.handle.media_stream_numbers(&played)?;
    assert_eq!(
        by_id
            .and_then(|numbers| numbers.sharing)
            .and_then(|sharing| sharing.committed_bytes),
        Some(FILM_LEN as u64)
    );
    assert_eq!(
        by_id,
        fixture
            .handle
            .stream_numbers(fixture.url("a.bin").as_str())?
    );

    let other = fixture.handle.open_reader(&aside, None)?;
    let (other, film_b) = within("the aside reader", move || {
        let mut other = other;
        let film = read_to_end(&mut other);
        (other, film)
    })?;
    assert_eq!(film_b?, payload(FILM_LEN));
    assert_eq!(
        fixture.handle.play_session_of("tv.1"),
        session,
        "the aside moved the viewer's session"
    );
    assert_eq!(fixture.committed("b.bin")?, None, "the aside's film draws");
    drop((reader, other));
    fixture.stop()
}

/// **`set_buffer` is taken at the reader's next reopen, and not before**:
/// the lookahead is worked out when a torrent file is opened, so the
/// handle already reading keeps the choice it was opened with, and a seek
/// -- a new handle -- opens with the new one. What the profile changes (how
/// far ahead the engine fetches) is no number this crate is shown, so what
/// is read is the choice the stream's last open was made with.
#[test]
fn set_buffer_is_taken_at_the_next_reopen() -> anyhow::Result<()> {
    use enginefs::backend::priorities::BufferProfile;
    let fixture = TorrentFixture::start(
        fixture_pins::keep_what_the_fixture_seeded(offline_config()),
        &["film.mkv"],
        4 * PIECE,
        |_| true,
    )?;
    let id = fixture.register("film.mkv")?;
    let reader = fixture.handle.open_reader(
        &id,
        Some(PlayToken {
            token: "tv.1".to_string(),
            buffer: BufferProfile::Normal,
        }),
    )?;
    assert_eq!(reader.opened_with_buffer(), Some(BufferProfile::Normal));
    fixture.handle.set_buffer(&id, BufferProfile::Large)?;
    // A seek to where the reader already is -- what mpv makes right after
    // every open -- is no reopen, so it takes nothing either.
    let (reader, read) = within("a read after set_buffer", move || {
        let mut reader = reader;
        let read = reader
            .seek(0)
            .and_then(|at| Ok((at, reader.read(&mut [0u8; 64])?)));
        (reader, read)
    })?;
    assert_eq!(read?.0, 0);
    assert_eq!(
        reader.opened_with_buffer(),
        Some(BufferProfile::Normal),
        "the handle already reading, or a seek to where it was, changed its buffer"
    );
    let (reader, seek) = within("a seek after set_buffer", move || {
        let mut reader = reader;
        let seek = reader.seek(PIECE as u64);
        (reader, seek)
    })?;
    seek?;
    assert_eq!(
        reader.opened_with_buffer(),
        Some(BufferProfile::Large),
        "the reopen did not take the new buffer"
    );
    drop(reader);
    fixture.stop()
}

/// **An id nobody holds is evicted at the cap, and one being read is
/// not.** Registering is no I/O, so the cap is reached with URLs this
/// server serves nothing at; `set_buffer` is the cheapest question that
/// says whether an id is still held.
#[test]
fn an_id_nobody_holds_is_evicted_at_the_cap_and_one_being_read_is_not() -> anyhow::Result<()> {
    let origin = Origin::start()?;
    let (handle, _dirs) = origin_server(&origin)?;
    let held = handle.register(MediaSpec::StreamingUrl(proxy_url(
        &handle,
        &origin.url("/film.bin"),
    )))?;
    let reader = handle.open_reader(&held, None)?;
    let idle = handle.register(MediaSpec::StreamingUrl(url::Url::parse(
        "http://127.0.0.1:1/settings",
    )?))?;
    let normal = enginefs::backend::priorities::BufferProfile::Normal;
    assert_eq!(handle.set_buffer(&idle, normal), Ok(()));

    for _ in 0..stream_server::media::MEDIA_ID_CAP {
        handle.register(MediaSpec::StreamingUrl(url::Url::parse(
            "http://127.0.0.1:1/settings",
        )?))?;
    }
    assert_eq!(handle.set_buffer(&idle, normal), Err(Refusal::UnknownId));
    assert_eq!(handle.resolve(&idle).err(), Some(Refusal::UnknownId));
    assert_eq!(
        handle.set_buffer(&held, normal),
        Ok(()),
        "the id being read was evicted"
    );
    let (reader, head) = within("the held reader", move || {
        let mut reader = reader;
        let head = reader.read(&mut [0u8; 16]);
        (reader, head)
    })?;
    assert!(head? > 0);
    drop(reader);

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}
