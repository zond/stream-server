//! Cast by published token (`stream_server::cast`,
//! `docs/design/media-pipeline.md` §2.7): `ServerHandle::publish` and
//! `unpublish`, and the LAN listener's one route, `/cast/{token}`.
//!
//! Every kind of id casts -- a torrent file, a link through `/proxy`'s
//! cache, a Google Drive file, a finished download of a link, a member of a
//! single-file RAR -- with the range framing every media route shares; an
//! unknown token is a `404` the reachability count still counts; a cast
//! with a play token is the viewer's playback; an unpublish, or the
//! listener's stop, ends a body in flight; and a publication holds its id
//! against eviction. What the LAN listener does *not* serve is
//! `embed.rs`'s `lan_media_listener_serves_published_tokens_and_nothing_else`;
//! that no token reaches a log line is `log_redaction.rs`'s.
//!
//! Every server here is offline and on ephemeral ports, and every origin is
//! a loopback fake of this file's own ([`Origin`]).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use stream_server::{CastToken, MediaId, MediaSpec, PlayToken, Refusal, ServerConfig};

/// Why a test that seeds its own torrent data runs with the pin set
/// unknown, and which tests may not (see the module).
#[path = "support/fixture_pins.rs"]
mod fixture_pins;

/// The hand-built RAR archives, shared with the translator's own tests.
#[cfg(feature = "rar")]
#[path = "support/rar_fixtures.rs"]
mod rar_fixtures;

/// The offline config, the control client and a real torrent.
#[path = "support/torrent_fixtures.rs"]
mod torrent_fixtures;
use torrent_fixtures::{bearer_client, offline_config, piece_store, real_torrent};

/// A bound on a mistake, never a wait a correct run spends.
const BOUND: Duration = Duration::from_secs(120);

/// How long a body that should have been cut is given to end. A cut body
/// ends at once; one that is not cut never ends (its next piece is one
/// nobody has, and [`parked_cast`]'s client has no timeout), so this is the
/// whole of what a broken cut costs a run.
const CUT_BOUND: Duration = Duration::from_secs(20);

/// The fixtures' piece length (`real_torrent`'s).
const PIECE: usize = 16 * 1024;

/// The fixture pattern: a byte that says where it came from.
fn byte_at(offset: usize) -> u8 {
    (offset.wrapping_mul(7) % 251) as u8
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(byte_at).collect()
}

/// Permit and start the LAN listener the way a cast session does: the
/// setting, then the start. Answers the LAN base, `http://addr`.
fn start_lan(handle: &stream_server::ServerHandle) -> anyhow::Result<String> {
    handle.update_settings(serde_json::json!({ "lanMediaEnabled": true }))?;
    let addr = handle
        .set_lan_media(true)?
        .ok_or_else(|| anyhow::anyhow!("set_lan_media(true) answered with no address"))?;
    Ok(format!("http://{addr}"))
}

/// The LAN listener on an ephemeral loopback port.
fn lan_config(config: ServerConfig) -> ServerConfig {
    ServerConfig {
        lan_media_addr: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
        ..config
    }
}

/// `GET` of a cast token, with `range` if one is given.
fn fetch(
    lan: &str,
    token: &CastToken,
    range: Option<&str>,
) -> anyhow::Result<reqwest::blocking::Response> {
    let mut request =
        reqwest::blocking::Client::new().get(format!("{lan}/cast/{}", token.as_str()));
    if let Some(range) = range {
        request = request.header(reqwest::header::RANGE, range);
    }
    Ok(request.send()?)
}

fn header(response: &reqwest::blocking::Response, name: &str) -> String {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// **The cast of `token` answers `expected` (a file of that many bytes)
/// with ranges and `HEAD`**: a `206` with the range's bytes and framing, a
/// `200` with the whole file, and a `HEAD` with the length and no body.
fn assert_casts(lan: &str, token: &CastToken, expected: &[u8]) -> anyhow::Result<()> {
    let len = expected.len();
    let (first, last) = (len / 3 + 7, len / 3 + 7 + 20_000.min(len / 2));
    let response = fetch(lan, token, Some(&format!("bytes={first}-{last}")))?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        header(&response, "content-range"),
        format!("bytes {first}-{last}/{len}")
    );
    assert_eq!(
        header(&response, "content-length"),
        (last - first + 1).to_string()
    );
    assert_eq!(header(&response, "accept-ranges"), "bytes");
    assert_eq!(response.bytes()?.as_ref(), &expected[first..=last]);

    let response = fetch(lan, token, None)?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(header(&response, "content-length"), len.to_string());
    assert_eq!(response.bytes()?.as_ref(), expected);

    let response = reqwest::blocking::Client::new()
        .head(format!("{lan}/cast/{}", token.as_str()))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(header(&response, "content-length"), len.to_string());
    assert!(response.bytes()?.is_empty());
    Ok(())
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

// --- Torrent fixtures -------------------------------------------------------

/// Seed the pieces of `torrent` whose index `keep` accepts, in the piece
/// store where the server reads them, from the files under `content` in
/// the order the metainfo lists them. **After the server has started**:
/// the launch sweep deletes a piece directory seeded before it.
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

/// A server with one torrent of the caller's files added and checked, the
/// pieces `keep` accepts seeded, and the LAN listener started.
struct TorrentFixture {
    handle: stream_server::ServerHandle,
    base: String,
    lan: String,
    info_hash: String,
    files: Vec<(String, usize)>,
    _dirs: [tempfile::TempDir; 3],
}

impl TorrentFixture {
    fn start(
        config: ServerConfig,
        files: &[(&str, Vec<u8>)],
        keep: impl Fn(usize) -> bool,
    ) -> anyhow::Result<Self> {
        let names = files.iter().map(|(name, _)| *name).collect::<Vec<_>>();
        let config_dir = tempfile::tempdir()?;
        let cache_dir = tempfile::tempdir()?;
        let src = tempfile::tempdir()?;
        let content = src.path().join("Films");
        std::fs::create_dir_all(&content)?;
        for (name, bytes) in files {
            std::fs::write(content.join(name), bytes)?;
        }
        let (torrent, info_hash) = real_torrent(&content);
        let cache_root = stream_server::resolved_path(&cache_dir.path().join("cache"));
        stream_server::pretend_volume_space(&cache_root, u64::MAX);
        let handle = stream_server::start(lan_config(ServerConfig {
            http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            config_dir: Some(config_dir.path().join("config")),
            cache_dir: Some(cache_root.clone()),
            ..config
        }))?;
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
        let files = names
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
        let lan = start_lan(&handle)?;
        Ok(Self {
            handle,
            base,
            lan,
            info_hash,
            files,
            _dirs: [config_dir, cache_dir, src],
        })
    }

    fn index(&self, name: &str) -> usize {
        self.files
            .iter()
            .find(|(file, _)| file == name)
            .map(|(_, index)| *index)
            .expect("a file of the fixture")
    }

    /// The file's streaming URL, as stremio-core builds it.
    fn register(&self, name: &str) -> anyhow::Result<MediaId> {
        self.handle
            .register(MediaSpec::StreamingUrl(url::Url::parse(&format!(
                "{}/{}/{}",
                self.base,
                self.info_hash,
                self.index(name)
            ))?))
    }

    fn stop(self) -> anyhow::Result<()> {
        self.handle.shutdown()?;
        self.handle.join()?;
        Ok(())
    }
}

// --- The loopback origin ----------------------------------------------------

/// The refresh token the fake Drive account is reachable by.
const REFRESH_TOKEN: &str = "refresh-tok-cast-41d0";
/// Drive's id for the fake file.
const FILE_ID: &str = "1CastFixtureFile";
/// The length of every file the origin serves.
const ORIGIN_LEN: usize = 300 * 1024;

/// One loopback listener standing in for a CDN, Google Drive and the
/// pairing service at once: `/film.bin` serves ranges, `/whole.bin`
/// answers every request with the whole file, `/refresh` mints
/// Drive tokens and `/drive/v3/files/...` serves ranges to whoever holds
/// one.
struct Origin {
    addr: SocketAddr,
    /// Set by [`Origin::kill`]: every connection after it is dropped
    /// unanswered, and counted in [`Origin::after_death`].
    dead: Arc<AtomicBool>,
    after_death: Arc<AtomicUsize>,
}

impl Origin {
    fn start() -> anyhow::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let addr = listener.local_addr()?;
        let dead = Arc::new(AtomicBool::new(false));
        let after_death = Arc::new(AtomicUsize::new(0));
        let (killed, late) = (dead.clone(), after_death.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                if killed.load(Ordering::SeqCst) {
                    late.fetch_add(1, Ordering::SeqCst);
                    drop(stream);
                    continue;
                }
                std::thread::spawn(move || serve(stream));
            }
        });
        Ok(Self {
            addr,
            dead,
            after_death,
        })
    }

    fn kill(&self) {
        self.dead.store(true, Ordering::SeqCst);
    }

    fn after_death(&self) -> usize {
        self.after_death.load(Ordering::SeqCst)
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }
}

fn serve(mut stream: TcpStream) {
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
        let _ = reader.read_exact(&mut body);
        if String::from_utf8_lossy(&body).contains(REFRESH_TOKEN) {
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
    if path == "/whole.bin" {
        let body: Vec<u8> = (0..ORIGIN_LEN).map(byte_at).collect();
        respond(&mut stream, "200 OK", "video/mp4", &body);
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
    let body: Vec<u8> = (first..=last).map(byte_at).collect();
    let head = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Type: video/x-matroska\r\nETag: \"cast-fixture\"\r\n\
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
/// for Drive, with the LAN listener started: the handle, the LAN base and
/// the directories it lives in.
fn origin_server(
    origin: &Origin,
) -> anyhow::Result<(stream_server::ServerHandle, String, [tempfile::TempDir; 2])> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let cache_root = stream_server::resolved_path(&cache_dir.path().join("cache"));
    stream_server::pretend_volume_space(&cache_root, u64::MAX);
    let handle = stream_server::start(lan_config(ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root),
        drive_refresh_endpoint: Some(url::Url::parse(&origin.url("/refresh"))?),
        drive_api_base: Some(url::Url::parse(&origin.url("/"))?),
        // An embedder that keeps a proxy pin record, empty at boot.
        proxy_pins: Some(Vec::new()),
        ..offline_config()
    }))?;
    let lan = start_lan(&handle)?;
    Ok((handle, lan, [config_dir, cache_dir]))
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

fn stop(handle: stream_server::ServerHandle) -> anyhow::Result<()> {
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

// --- Every kind of id casts --------------------------------------------------

/// **A torrent id casts**: registered by the URL stremio-core builds,
/// published, and read from the LAN listener by range, whole and by
/// `HEAD`.
#[test]
fn a_torrent_id_casts() -> anyhow::Result<()> {
    let film = payload(5 * PIECE + 321);
    let fixture = TorrentFixture::start(
        fixture_pins::keep_what_the_fixture_seeded(offline_config()),
        &[("film.mkv", film.clone())],
        |_| true,
    )?;
    let id = fixture.register("film.mkv")?;
    let token = fixture.handle.publish(&id, None)?;
    assert_casts(&fixture.lan, &token, &film)?;
    let response = fetch(&fixture.lan, &token, None)?;
    assert_eq!(header(&response, "content-type"), "video/x-matroska");
    fixture.stop()
}

/// **A `/proxy` id casts**, through the proxy cache as the app's player
/// reads it -- what the LAN could never fetch before: `/proxy` is an open
/// relay and was never on the LAN listener.
#[test]
fn a_proxy_id_casts() -> anyhow::Result<()> {
    let origin = Origin::start()?;
    let (handle, lan, _dirs) = origin_server(&origin)?;
    let id = handle.register(MediaSpec::StreamingUrl(proxy_url(
        &handle,
        &origin.url("/film.bin"),
    )))?;
    let token = handle.publish(&id, None)?;
    assert_casts(&lan, &token, &payload(ORIGIN_LEN))?;
    stop(handle)
}

/// **A Drive id casts**, under the grant its supplier gives: the receiver
/// sees one file under a random token, never a Drive route.
#[test]
fn a_drive_id_casts() -> anyhow::Result<()> {
    let origin = Origin::start()?;
    let (handle, lan, _dirs) = origin_server(&origin)?;
    let id = handle.register(MediaSpec::Drive {
        file_id: FILE_ID.to_string(),
        name: Some("A Film.mkv".to_string()),
        grant: Arc::new(|| Some(REFRESH_TOKEN.to_string())),
    })?;
    let token = handle.publish(&id, None)?;
    assert_casts(&lan, &token, &payload(ORIGIN_LEN))?;
    stop(handle)
}

/// **A finished download of a link casts with the origin gone**: the id
/// resolves to the held entry, off the disk, and the receiver reads it with
/// no origin asked.
#[test]
fn a_finished_url_download_casts_with_the_origin_gone() -> anyhow::Result<()> {
    let origin = Origin::start()?;
    let (handle, lan, _dirs) = origin_server(&origin)?;
    let target = origin.url("/film.bin");
    let row = handle.pin_proxy_download(stream_server::ProxyDownloadRequest {
        url: Some(target.clone()),
        headers: Default::default(),
        drive_file_id: None,
        refresh_token: None,
        name: Some("The Film.mkv".to_string()),
    })?;
    until("the download completes", || {
        Ok(handle
            .downloads()?
            .iter()
            .any(|download| download.info_hash == row.info_hash && download.complete))
    })?;
    origin.kill();

    let id = handle.register(MediaSpec::StreamingUrl(proxy_url(&handle, &target)))?;
    let token = handle.publish(&id, None)?;
    assert_casts(&lan, &token, &payload(ORIGIN_LEN))?;
    assert_eq!(origin.after_death(), 0, "the dead origin was asked");
    stop(handle)
}

/// **A member of a single-file RAR casts**: the `torrent:` form's id, read
/// from the LAN as the member's own bytes, its own length and its own type.
#[cfg(feature = "rar")]
#[test]
fn a_member_of_a_single_file_rar_casts() -> anyhow::Result<()> {
    let film = rar_fixtures::signposted(6 * PIECE + 1234);
    let extra = payload(3 * PIECE);
    let rar = rar_fixtures::rar5_stored(&[("film.mkv", &film), ("extras.nfo", &extra)]);
    let fixture = TorrentFixture::start(
        fixture_pins::keep_what_the_fixture_seeded(offline_config()),
        &[("film.rar", rar)],
        |_| true,
    )?;
    let id = fixture
        .handle
        .register(MediaSpec::StreamingUrl(url::Url::parse(&format!(
            "{}/rar/stream/torrent:{}%2Ffilm.rar/film.mkv",
            fixture.base, fixture.info_hash
        ))?))?;
    let token = fixture.handle.publish(&id, None)?;
    assert_casts(&fixture.lan, &token, &film)?;
    let response = fetch(&fixture.lan, &token, Some("bytes=0-0"))?;
    assert_eq!(header(&response, "content-type"), "video/x-matroska");
    fixture.stop()
}

/// **A link whose origin will not range is refused, not served**: a
/// player on this device can read it forward through `/proxy`, but nothing
/// here can seek it for a receiver, so `HEAD` and `GET` both answer `501`
/// with the refusal -- never a `200` of no length.
#[test]
fn a_link_that_will_not_range_is_refused_on_the_cast_route() -> anyhow::Result<()> {
    let origin = Origin::start()?;
    let (handle, lan, _dirs) = origin_server(&origin)?;
    let id = handle.register(MediaSpec::StreamingUrl(proxy_url(
        &handle,
        &origin.url("/whole.bin"),
    )))?;
    let token = handle.publish(&id, None)?;
    let client = reqwest::blocking::Client::new();
    for method in [reqwest::Method::HEAD, reqwest::Method::GET] {
        let response = client
            .request(method.clone(), format!("{lan}/cast/{}", token.as_str()))
            .send()?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::NOT_IMPLEMENTED,
            "{method}"
        );
        if method == reqwest::Method::GET {
            let refusal: serde_json::Value = response.json()?;
            assert_eq!(refusal["refused"], "noRanges");
        }
    }
    assert_eq!(handle.lan_media_bodies_served(), 0);
    stop(handle)
}

// --- Tokens, counts, plays ----------------------------------------------------

/// **A token's `Debug` says nothing of it**, so nothing that derives
/// `Debug` around one can put it in a log line; its string is the URL's.
#[test]
fn a_tokens_debug_hides_it() -> anyhow::Result<()> {
    let origin = Origin::start()?;
    let (handle, _lan, _dirs) = origin_server(&origin)?;
    let token = handle.publish(
        &handle.register(MediaSpec::StreamingUrl(proxy_url(
            &handle,
            &origin.url("/film.bin"),
        )))?,
        None,
    )?;
    assert_eq!(token.as_str().len(), 32);
    assert!(token.as_str().bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert!(!format!("{token:?}").contains(token.as_str()));
    stop(handle)
}

/// **An unknown token is a `404`**, and the two counts read it as what it
/// is: the receiver reached this device (the requests count, which a `404`
/// has always counted) and was served nothing (the bodies count). A
/// published token's `GET` is a body; its `HEAD` is not.
#[test]
fn an_unknown_token_is_a_404_counted_as_a_request_and_not_a_body() -> anyhow::Result<()> {
    let origin = Origin::start()?;
    let (handle, lan, _dirs) = origin_server(&origin)?;
    let client = reqwest::blocking::Client::new();
    for path in [
        "/cast/00112233445566778899aabbccddeeff",
        "/cast/x",
        "/cast/",
    ] {
        assert_eq!(
            client.get(format!("{lan}{path}")).send()?.status(),
            reqwest::StatusCode::NOT_FOUND,
            "{path}"
        );
    }
    assert_eq!(handle.lan_media_requests_served(), 3);
    assert_eq!(handle.lan_media_bodies_served(), 0);

    let id = handle.register(MediaSpec::StreamingUrl(proxy_url(
        &handle,
        &origin.url("/film.bin"),
    )))?;
    let token = handle.publish(&id, None)?;
    assert_eq!(
        client
            .head(format!("{lan}/cast/{}", token.as_str()))
            .send()?
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(handle.lan_media_requests_served(), 4);
    assert_eq!(handle.lan_media_bodies_served(), 0, "a HEAD began a body");
    let response = fetch(&lan, &token, Some("bytes=0-99"))?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.bytes()?.len(), 100);
    assert_eq!(handle.lan_media_requests_served(), 5);
    assert_eq!(handle.lan_media_bodies_served(), 1);

    // A token is not the id, and the id is no token.
    assert_ne!(token.as_str(), id.as_str());
    assert_eq!(
        client
            .get(format!("{lan}/cast/{}", id.as_str()))
            .send()?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    assert_eq!(handle.lan_media_bodies_served(), 1);
    stop(handle)
}

/// **A cast published with a play token is the viewer's playback**: the
/// receiver's read moves the viewer's play session to the file, as the
/// app's own player's would; a cast without one is an aside and moves
/// nothing.
#[test]
fn a_cast_with_a_play_token_moves_the_session_and_one_without_does_not() -> anyhow::Result<()> {
    let fixture = TorrentFixture::start(
        offline_config(),
        &[("a.bin", payload(4 * PIECE)), ("b.bin", payload(4 * PIECE))],
        |_| true,
    )?;
    let played = fixture.handle.publish(
        &fixture.register("a.bin")?,
        Some(PlayToken {
            token: "tv.1".to_string(),
            buffer: Default::default(),
        }),
    )?;
    let aside = fixture.handle.publish(&fixture.register("b.bin")?, None)?;
    assert_eq!(fixture.handle.play_session_of("tv.1"), None);

    let response = fetch(&fixture.lan, &played, Some("bytes=0-1023"))?;
    assert_eq!(response.bytes()?.as_ref(), &payload(4 * PIECE)[..1024]);
    let session = Some(enginefs::retention::sessions::Played::Torrent {
        info_hash: fixture.info_hash.clone(),
        file_idx: fixture.index("a.bin"),
        shares: true,
        member: None,
    });
    assert_eq!(
        fixture.handle.play_session_of("tv.1"),
        session,
        "the receiver's read did not move the viewer's session"
    );

    let response = fetch(&fixture.lan, &aside, Some("bytes=0-1023"))?;
    assert_eq!(response.bytes()?.len(), 1024);
    assert_eq!(
        fixture.handle.play_session_of("tv.1"),
        session,
        "the aside cast moved the viewer's session"
    );
    fixture.stop()
}

/// A torrent of one film whose pieces after the second nobody has, cast
/// whole: the body delivers the first two pieces and parks. Answers the
/// fixture, the token and the response with those two pieces read off it.
fn parked_cast() -> anyhow::Result<(TorrentFixture, CastToken, reqwest::blocking::Response)> {
    let film = payload(8 * PIECE);
    let fixture = TorrentFixture::start(
        fixture_pins::keep_what_the_fixture_seeded(offline_config()),
        &[("film.mkv", film.clone())],
        |piece| piece < 2,
    )?;
    let token = fixture
        .handle
        .publish(&fixture.register("film.mkv")?, None)?;
    // No timeout: reqwest's default of thirty seconds would end a body the
    // cut never reached, and the test would pass on it.
    let mut response = reqwest::blocking::Client::builder()
        .timeout(None)
        .build()?
        .get(format!("{}/cast/{}", fixture.lan, token.as_str()))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut head = vec![0u8; 2 * PIECE];
    response.read_exact(&mut head)?;
    assert_eq!(head, film[..2 * PIECE]);
    Ok((fixture, token, response))
}

/// Read what is left of `response` on a thread of its own, and answer
/// whether it ended -- with an error or short of its length -- within
/// [`CUT_BOUND`] of `cut` being called.
fn ends_when(mut response: reqwest::blocking::Response, cut: impl FnOnce()) -> anyhow::Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut rest = Vec::new();
        let _ = tx.send((response.read_to_end(&mut rest), rest.len()));
    });
    // The body is parked on the missing piece, and stays parked: nothing
    // but the cut can end it.
    assert!(
        rx.recv_timeout(Duration::from_millis(500)).is_err(),
        "the body ended before the cut"
    );
    cut();
    let (ended, rest) = rx
        .recv_timeout(CUT_BOUND)
        .map_err(|_| anyhow::anyhow!("the body was not cut: it still waits on a missing piece"))?;
    assert!(
        ended.is_err() || rest < 6 * PIECE,
        "the cut body delivered the whole file"
    );
    Ok(())
}

/// **`unpublish` ends a body mid-stream**: a receiver parked on a piece
/// nobody has sees its body end at once, and the token answers `404` from
/// then on.
#[test]
fn unpublish_ends_a_body_mid_stream() -> anyhow::Result<()> {
    let (fixture, token, response) = parked_cast()?;
    let handle = &fixture.handle;
    ends_when(response, || assert!(handle.unpublish(&token)))?;
    assert!(!fixture.handle.unpublish(&token), "unpublished twice");
    assert_eq!(
        fetch(&fixture.lan, &token, None)?.status(),
        reqwest::StatusCode::NOT_FOUND
    );
    fixture.stop()
}

/// **Stopping the listener unpublishes every token and ends every body**:
/// the receiver mid-file is cut, and a listener started again serves none
/// of the old tokens.
#[test]
fn stopping_the_lan_listener_unpublishes_all_and_ends_bodies() -> anyhow::Result<()> {
    let (fixture, token, response) = parked_cast()?;
    let other = fixture
        .handle
        .publish(&fixture.register("film.mkv")?, None)?;
    let handle = &fixture.handle;
    ends_when(response, || {
        assert_eq!(handle.set_lan_media(false).expect("stopped"), None);
    })?;
    assert!(
        !fixture.handle.unpublish(&token),
        "the stop left it published"
    );
    assert!(
        !fixture.handle.unpublish(&other),
        "the stop left it published"
    );
    let lan = start_lan(&fixture.handle)?;
    for token in [&token, &other] {
        assert_eq!(
            fetch(&lan, token, None)?.status(),
            reqwest::StatusCode::NOT_FOUND
        );
    }
    fixture.stop()
}

/// **A publication keeps its id from eviction until it is unpublished**,
/// as an open reader does; and a publish is refused for an id this server
/// does not hold and while the listener is not running.
#[test]
fn a_publication_holds_its_id_until_unpublished() -> anyhow::Result<()> {
    let origin = Origin::start()?;
    let (handle, _lan, _dirs) = origin_server(&origin)?;
    let nothing =
        || MediaSpec::StreamingUrl(url::Url::parse("http://127.0.0.1:1/settings").expect("a URL"));
    let normal = enginefs::backend::priorities::BufferProfile::Normal;
    let cast = handle.register(nothing())?;
    let token = handle.publish(&cast, None)?;
    let fill = || -> anyhow::Result<()> {
        for _ in 0..stream_server::media::MEDIA_ID_CAP {
            handle.register(nothing())?;
        }
        Ok(())
    };
    fill()?;
    assert_eq!(
        handle.set_buffer(&cast, normal),
        Ok(()),
        "a published id was evicted"
    );
    assert!(handle.unpublish(&token));
    fill()?;
    assert_eq!(handle.set_buffer(&cast, normal), Err(Refusal::UnknownId));
    assert!(
        handle.publish(&cast, None).is_err(),
        "an evicted id published"
    );

    let id = handle.register(nothing())?;
    handle.set_lan_media(false)?;
    let error = handle.publish(&id, None).unwrap_err().to_string();
    assert!(error.contains("not running"), "{error}");
    stop(handle)
}
