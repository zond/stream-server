//! The one line the torrent stream route writes when a playback ends.
//!
//! `stage="http_stream_end"` is the only record that a playback session
//! ended and the only one that says how much of what was asked for actually
//! left the server. It is read off real devices, so what its `reason` says
//! is not a detail: a range delivered whole and a player that hung up
//! part-way have to be told apart in a log nobody can reproduce. They were
//! not -- every completed playback was filed as a `client-disconnect`,
//! because hyper stops polling a body of declared length the moment that
//! length is met and the body therefore never recorded an end (the same
//! defect `/proxy` was born with; see `proxy_body_end.rs`).
//!
//! So this asserts over the log file the field report is read from, with a
//! real torrent fixture behind the route: a client that reads a whole file
//! must produce `complete`, and one that hangs up must still produce
//! `client-disconnect`. A test of only the first would pass a line that
//! said `complete` about everything.
//!
//! A binary of its own because `init_logging` installs the process's
//! subscriber once: a second logging test in the same binary would write
//! into whichever tempdir won the race (`log_redaction.rs` and
//! `proxy_body_end.rs` are binaries for the same reason).

use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Why a test that seeds its own torrent data runs with the pin set
/// unknown, and which tests may not.
#[path = "support/fixture_pins.rs"]
mod fixture_pins;

/// Reading the log files back, which is what this test asserts over.
#[path = "support/log_lines.rs"]
mod log_lines;

/// The stage this file is about.
const STAGE: &str = "http_stream_end";

/// How long a torrent's initial check is given (`embed.rs` waits the same).
const CHECK_WAIT_BOUND: Duration = Duration::from_secs(30);

/// The fixture file's length. Big enough that the sockets between a player
/// and this server cannot swallow the whole of it, which is what makes a
/// client that stops reading a client the server finds out about.
const PAYLOAD: usize = 8 * 1024 * 1024;

/// One byte of the payload at `offset`: a cheap pattern, so a body can be
/// checked to be the fixture's own bytes and not merely the right length.
fn byte_at(offset: usize) -> u8 {
    (offset % 251) as u8
}

/// A real `.torrent` of `dir`, and its info hash. 64 KiB pieces, which for
/// [`PAYLOAD`] is 128 of them to seed rather than 512.
fn real_torrent(dir: &Path) -> (Vec<u8>, String) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let t = librqbit::create_torrent(
            dir,
            librqbit::CreateTorrentOptions {
                name: None,
                trackers: Vec::new(),
                piece_length: Some(64 * 1024),
            },
            &librqbit::spawn_utils::BlockingSpawner::new(1),
        )
        .await
        .expect("create torrent");
        (
            t.as_bytes().expect("serialize").to_vec(),
            t.info_hash().as_string(),
        )
    })
}

/// Pre-seed a one-file torrent's every piece where the server reads them:
/// the piece store. The single-file case of `embed.rs`'s
/// `seed_piece_store_pieces`, with the layout asked of the store.
///
/// **Call this after the server has started**, never before: the
/// launch-time sweep deletes every piece directory the embedder's pin set
/// does not name, and one seeded before the process comes up is exactly
/// that.
fn seed_piece_store(cache_root: &Path, torrent_bytes: &[u8], content: &[u8]) {
    let meta = librqbit::torrent_from_bytes(torrent_bytes).expect("parse the torrent back");
    let info_hash = meta.info_hash.as_string();
    let info = meta.info.data.validate().expect("validated metainfo");
    let piece_length = info.lengths().default_piece_length() as u64;
    assert_eq!(
        content.len() as u64,
        info.lengths().total_length(),
        "the fixture and the torrent disagree about the payload"
    );
    let store =
        enginefs::piece_store::StoreRoot::in_download_dir(&cache_root.join("rqbit-downloads"));
    let layout = enginefs::piece_store::PieceLayout::new(
        piece_length,
        content.len() as u64,
        [enginefs::piece_store::FileSpec {
            len: content.len() as u64,
            padding: false,
        }],
    )
    .expect("a layout for the fixture");
    let pieces =
        enginefs::piece_store::PieceStore::new(store.torrent_dir(&info_hash), Arc::new(layout));
    for (index, piece) in content.chunks(piece_length as usize).enumerate() {
        let path = pieces.piece_path(index as u32);
        std::fs::create_dir_all(path.parent().expect("a bucket")).expect("piece bucket");
        std::fs::write(&path, piece).expect("write a piece");
    }
}

/// The control client: `/create` is a control route and takes the launch's
/// bearer token.
fn bearer_client(
    handle: &stream_server::ServerHandle,
) -> anyhow::Result<reqwest::blocking::Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    let token = handle.auth_token().expect("every launch generates a token");
    headers.insert(
        reqwest::header::AUTHORIZATION,
        format!("Bearer {token}").parse().expect("valid header"),
    );
    Ok(reqwest::blocking::Client::builder()
        .default_headers(headers)
        .build()?)
}

/// The torrent added and checked, and the URL its one file is played from.
fn seeded_stream_url(
    handle: &stream_server::ServerHandle,
    base: &str,
    cache_root: &Path,
    src: &Path,
    payload: &[u8],
) -> anyhow::Result<(String, String)> {
    let content = src.join("Feature");
    std::fs::create_dir_all(&content)?;
    std::fs::write(content.join("movie.bin"), payload)?;
    let (torrent, info_hash) = real_torrent(&content);
    seed_piece_store(cache_root, &torrent, payload);

    let client = bearer_client(handle)?;
    client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?;

    let deadline = Instant::now() + CHECK_WAIT_BOUND;
    let stats = loop {
        let stats: serde_json::Value = serde_json::to_value(handle.engine_stats(&info_hash, &[])?)?;
        match stats["phase"].as_str() {
            Some("checking") | Some("resolvingMetadata") => {
                anyhow::ensure!(
                    Instant::now() < deadline,
                    "still {} after {CHECK_WAIT_BOUND:?}: {stats}",
                    stats["phase"]
                );
                std::thread::sleep(Duration::from_millis(50));
            }
            _ => break stats,
        }
    };
    let idx = stats["files"]
        .as_array()
        .expect("the stats name the torrent's files")
        .iter()
        .position(|file| {
            file["path"]
                .as_str()
                .is_some_and(|p| p.ends_with("movie.bin"))
        })
        .expect("the fixture's own file");
    Ok((info_hash.clone(), format!("{base}/{info_hash}/{idx}")))
}

/// A range delivered whole says so, and a player that hangs up says that
/// instead: the distinction the line exists for, read back out of the log
/// file a field report is made of.
#[test]
fn a_torrent_body_says_whether_it_was_delivered_or_hung_up_on() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let config_root = config_dir.path().join("config");
    let cache_root = stream_server::resolved_path(&cache_dir.path().join("cache"));
    stream_server::pretend_volume_space(&cache_root, u64::MAX);

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_root.clone()),
        cache_dir: Some(cache_root.clone()),
        init_logging: true,
        // Nothing seeds this fixture, so nothing may take its pieces back:
        // see [`fixture_pins`].
        ..fixture_pins::keep_what_the_fixture_seeded(stream_server::ServerConfig {
            resolve_dht_bootstrap_names: false,
            use_public_trackers: false,
            ..stream_server::ServerConfig::default()
        })
    })?;
    let base = format!("http://{}", handle.http_addr());
    let payload: Vec<u8> = (0..PAYLOAD).map(byte_at).collect();
    // After the start, never before (the launch-time sweep).
    let (info_hash, url) = seeded_stream_url(&handle, &base, &cache_root, src.path(), &payload)?;

    // A player that reads the whole file.
    let response = reqwest::blocking::Client::new().get(&url).send()?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body = response.bytes()?;
    assert_eq!(body.len(), PAYLOAD);
    assert_eq!(
        body[PAYLOAD - 1],
        byte_at(PAYLOAD - 1),
        "the fixture's bytes"
    );

    let delivered = log_lines::wait_for_line(&config_root, STAGE, "a file read whole", |fields| {
        fields["reason"] == "complete"
    })?;
    let fields = &delivered["fields"];
    assert_eq!(
        fields["bytes_sent"], PAYLOAD,
        "what left the server, which for a delivered range is all of it"
    );
    assert_eq!(
        fields["requested_len"], PAYLOAD,
        "and what was asked for: the two agree, which is what `complete` means"
    );
    assert_eq!(fields["error"], "", "nothing ended this one");
    assert_eq!(fields["info_hash"], info_hash);
    assert!(
        fields["stream_id"].is_u64() && fields["file_idx"].is_u64(),
        "the line names the stream it is about: {fields}"
    );
    assert!(
        fields["duration_ms"].is_u64(),
        "how long the body was open: {fields}"
    );

    // And a player that hangs up part-way, which must still read as one:
    // a line that said `complete` about every body would be no better than
    // one that said `client-disconnect` about every body.
    let mut socket = std::net::TcpStream::connect(handle.http_addr())?;
    socket.set_read_timeout(Some(Duration::from_secs(10)))?;
    let path = url
        .strip_prefix(&base)
        .expect("the stream URL is on this server");
    write!(
        socket,
        "GET {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        handle.http_addr()
    )?;
    socket.flush()?;
    let mut read = 0;
    let mut buffer = [0u8; 4096];
    while read < 64 * 1024 {
        match socket.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(n) => read += n,
        }
    }
    assert!(read > 0, "the body started arriving");
    drop(socket);

    let cut = log_lines::wait_for_line(&config_root, STAGE, "a player that hung up", |fields| {
        fields["reason"] == "client-disconnect"
    })?;
    let fields = &cut["fields"];
    assert_eq!(fields["requested_len"], PAYLOAD);
    assert!(
        fields["bytes_sent"]
            .as_u64()
            .is_some_and(|sent| sent < PAYLOAD as u64),
        "a body the player hung up on is short of what was asked for: {fields}"
    );

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}
