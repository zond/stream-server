//! What a torrent wants between `POST /create` and the first stream request.
//!
//! Both create routes already work out which file the caller means --
//! `fileMustInclude` names it, `guessFileIdx` asks for the season/episode
//! guess -- and they used to spend that answer on one JSON field. The
//! torrent went on wanting every file, which for a season pack is the whole
//! pack: forty gigabytes fetched, and a `stats.json` poller watching the
//! pack's progress rather than the episode's, for however long the client
//! sits on the details page before it plays anything.
//!
//! So the answer is now the add's want-set
//! (`enginefs::backend::TorrentPlacement::choose`), and what that changes is
//! observable from outside: `stats.json`'s `isFinished` is "nothing left to
//! fetch of what this torrent wants", so with one episode of a two-episode
//! pack on the disk, a create that names that episode is finished and a
//! create that names nothing is not. The control half of the test is what
//! makes that a statement about the want-set and not about the fixture.
//!
//! A binary of its own because the file this would otherwise belong in is
//! being edited elsewhere; the fixture helpers are the usual per-binary
//! copies (`stream_body_end.rs` carries the single-file versions of them).

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Why a test that seeds its own torrent data runs with the pin set
/// unknown, and which tests may not.
#[path = "support/fixture_pins.rs"]
mod fixture_pins;

/// 16 KiB pieces, and both files a whole number of them, so no piece
/// straddles the two: a fixture that seeds one file seeds whole pieces.
const PIECE: u64 = 16 * 1024;
const EPISODE_ONE: &str = "Show.S01E01.mkv";
const EPISODE_TWO: &str = "Show.S01E02.mkv";

/// The wanted episode is the *smaller* file, so the guess that picks it is
/// the season/episode tag doing the work and not the largest-media fallback.
const EPISODE_ONE_LEN: u64 = 2 * PIECE;
const EPISODE_TWO_LEN: u64 = 4 * PIECE;

fn byte_at(offset: usize) -> u8 {
    (offset % 251) as u8
}

fn payload(len: u64) -> Vec<u8> {
    (0..len as usize).map(byte_at).collect()
}

/// A two-episode pack on disk, and the `.torrent` of it with its info hash.
fn season_pack(dir: &Path) -> (Vec<u8>, String) {
    std::fs::write(dir.join(EPISODE_ONE), payload(EPISODE_ONE_LEN)).expect("episode one");
    std::fs::write(dir.join(EPISODE_TWO), payload(EPISODE_TWO_LEN)).expect("episode two");
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let t = librqbit::create_torrent(
            dir,
            librqbit::CreateTorrentOptions {
                name: None,
                trackers: Vec::new(),
                piece_length: Some(PIECE as u32),
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

/// The index of `name` in the torrent's own file order -- which is
/// `create_torrent`'s directory walk, i.e. the filesystem's readdir order
/// and never the order the fixture wrote the files in.
fn file_index(torrent_bytes: &[u8], name: &str) -> usize {
    let meta = librqbit::torrent_from_bytes(torrent_bytes).expect("parse the torrent back");
    let info = meta.info.data.validate().expect("validated metainfo");
    info.iter_file_details()
        .position(|file| {
            file.filename
                .to_pathbuf()
                .file_name()
                .and_then(|n| n.to_str())
                == Some(name)
        })
        .unwrap_or_else(|| panic!("the torrent names {name}"))
}

/// Pre-seed just `only`'s pieces where the server reads them: the piece
/// store. The two-file case of `embed.rs`'s `seed_piece_store_pieces`, read
/// back through the metainfo's own file order.
///
/// **Call this after the server has started**, never before: the launch-time
/// sweep deletes every piece directory the embedder's pin set does not name.
fn seed_one_file(cache_root: &Path, torrent_bytes: &[u8], content: &Path, only: &str) {
    let meta = librqbit::torrent_from_bytes(torrent_bytes).expect("parse the torrent back");
    let info_hash = meta.info_hash.as_string();
    let info = meta.info.data.validate().expect("validated metainfo");
    let piece_length = info.lengths().default_piece_length() as u64;

    let mut blob = Vec::new();
    let mut seeded = 0u64..0u64;
    for file in info.iter_file_details() {
        let relative = file.filename.to_pathbuf();
        let bytes = std::fs::read(content.join(&relative)).expect("the fixture file");
        if relative.file_name().and_then(|n| n.to_str()) == Some(only) {
            seeded = blob.len() as u64..(blob.len() + bytes.len()) as u64;
        }
        blob.extend_from_slice(&bytes);
    }
    assert_eq!(
        blob.len() as u64,
        info.lengths().total_length(),
        "the fixture and the torrent disagree about the payload"
    );
    assert!(!seeded.is_empty(), "the fixture seeds {only}");

    let store =
        enginefs::piece_store::StoreRoot::in_download_dir(&cache_root.join("rqbit-downloads"));
    let layout = enginefs::piece_store::PieceLayout::new(
        piece_length,
        blob.len() as u64,
        [enginefs::piece_store::FileSpec {
            len: blob.len() as u64,
            padding: false,
        }],
    )
    .expect("a layout for the fixture");
    let pieces =
        enginefs::piece_store::PieceStore::new(store.torrent_dir(&info_hash), Arc::new(layout));
    let mut written = 0usize;
    for (index, piece) in blob.chunks(piece_length as usize).enumerate() {
        let start = index as u64 * piece_length;
        let end = start + piece.len() as u64;
        if start < seeded.start || end > seeded.end {
            continue;
        }
        let path = pieces.piece_path(index as u32);
        std::fs::create_dir_all(path.parent().expect("a bucket")).expect("piece bucket");
        std::fs::write(&path, piece).expect("write a piece");
        written += 1;
    }
    assert!(written > 0, "the fixture seeded no piece at all");
}

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

/// Offline like every other embedded-server test (no DNS, no trackers), with
/// the pin set unknown so nothing reclaims what the fixture seeded.
fn config(cache: &Path, config_dir: &Path) -> stream_server::ServerConfig {
    fixture_pins::keep_what_the_fixture_seeded(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.to_path_buf()),
        cache_dir: Some(cache.to_path_buf()),
        resolve_dht_bootstrap_names: false,
        use_public_trackers: false,
        ..Default::default()
    })
}

/// `stats.json`, polled until the torrent is past the states it is only ever
/// passing through (no metadata yet, hash check running). Every fixture here
/// is a blob add with its data already on the disk, so both settle in
/// milliseconds; the budget is for a loaded runner.
fn settled_stats(
    client: &reqwest::blocking::Client,
    base: &str,
    info_hash: &str,
) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stats: serde_json::Value = client
            .get(format!("{base}/{info_hash}/stats.json"))
            .send()
            .expect("stats")
            .json()
            .expect("stats json");
        let phase = stats["phase"].as_str().unwrap_or_default();
        if !matches!(phase, "" | "resolvingMetadata" | "checking") {
            return stats;
        }
        assert!(Instant::now() < deadline, "stuck at {phase}: {stats}");
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Poll until the torrent has everything it wants, or say what it was still
/// short of. The add narrows the want-set from a task parked on the on-disk
/// check rather than blocking the add on it, so this is a wait and not a
/// single read.
fn wait_until_finished(client: &reqwest::blocking::Client, base: &str, info_hash: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stats = settled_stats(client, base, info_hash);
        if stats["isFinished"] == serde_json::Value::Bool(true) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the torrent never got what it wants: {stats}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// A `POST /create` that names its file leaves the torrent wanting that file
/// and nothing else -- before any reader exists, which is the whole window
/// this is about.
///
/// Read through `stats.json`'s `isFinished`, which is "nothing left to
/// fetch of what this torrent wants": with only the named episode on the
/// disk, a torrent that wants only that episode is finished, and one that
/// wants the pack is not and never will be, since there is no swarm
/// anywhere in this suite. The control half is the same fixture, the same
/// seed and the same route with nothing naming a file, so the difference is
/// the want-set and not the data.
///
/// `isFinished` here is the wanted answer and not the hazard the empty
/// want-set is: the file the client asked for really is on the disk, so a
/// torrent that stops asking peers for the rest of the pack is doing the
/// right thing. Nothing in this change can produce an *empty* want-set,
/// which would say the same with nothing on the disk at all (see
/// `add_torrent_placed_wants_the_file_the_choice_names`).
///
/// `phase` is deliberately not the assertion: `Engine::get_statistics`
/// focuses it on its own hint-less guess (the largest media file), so it
/// answers about the pack's biggest episode whatever the torrent wants.
/// That is older than this change and untouched by it.
#[test]
fn a_create_that_names_its_episode_wants_that_episode_and_not_the_pack() -> anyhow::Result<()> {
    let content = tempfile::tempdir()?;
    let (torrent, info_hash) = season_pack(content.path());
    let wanted = file_index(&torrent, EPISODE_ONE);

    // The subject: `guessFileIdx` with the season/episode hints stremio-video
    // sends, which picks the smaller of the two files by its SxxEyy tag.
    let cache = tempfile::tempdir()?;
    let config_dir = tempfile::tempdir()?;
    let handle = stream_server::start(config(cache.path(), config_dir.path()))?;
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    seed_one_file(cache.path(), &torrent, content.path(), EPISODE_ONE);

    let created: serde_json::Value = client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({
            "blob": hex::encode(&torrent),
            "guessFileIdx": {"season": 1, "episode": 1},
        }))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(
        created["guessedFileIdx"].as_u64(),
        Some(wanted as u64),
        "the route still reports the file it picked: {created}"
    );
    wait_until_finished(&client, &base, &info_hash);
    let stats = settled_stats(&client, &base, &info_hash);
    assert_eq!(
        stats["files"][wanted]["complete"],
        serde_json::Value::Bool(true),
        "the seeded episode: {stats}"
    );
    assert_eq!(
        stats["files"][file_index(&torrent, EPISODE_TWO)]["complete"],
        serde_json::Value::Bool(false),
        "the other one is not on the disk and is not wanted: {stats}"
    );

    // The control: the same torrent, the same seeded episode, a create that
    // names no file at all. It wants both episodes, and the second one is
    // not there and cannot arrive.
    let control_cache = tempfile::tempdir()?;
    let control_config = tempfile::tempdir()?;
    let control_handle = stream_server::start(config(control_cache.path(), control_config.path()))?;
    let control_base = format!("http://{}", control_handle.http_addr());
    let control_client = bearer_client(&control_handle)?;
    seed_one_file(control_cache.path(), &torrent, content.path(), EPISODE_ONE);

    let control_created: serde_json::Value = control_client
        .post(format!("{control_base}/create"))
        .json(&serde_json::json!({ "blob": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?
        .json()?;
    assert!(
        control_created["guessedFileIdx"].is_null(),
        "a create that names nothing picks nothing: {control_created}"
    );
    let control_stats = settled_stats(&control_client, &control_base, &info_hash);
    assert_eq!(
        control_stats["isFinished"],
        serde_json::Value::Bool(false),
        "wanting the whole pack, half of which is not on the disk: {control_stats}"
    );
    assert_eq!(
        control_stats["files"][wanted]["complete"],
        serde_json::Value::Bool(true),
        "the same seed as the subject: {control_stats}"
    );

    handle.shutdown()?;
    handle.join()?;
    control_handle.shutdown()?;
    control_handle.join()?;
    Ok(())
}
