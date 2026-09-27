//! The scaffolding every embedded-server test binary builds on: the offline
//! config, the control client, a real torrent of a directory, and the piece
//! store seeded and read back. Shared by `#[path]`, one copy per including
//! binary, so the unused-function lint is off: each includer uses the part
//! its own subject needs.

#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use stream_server::{ServerConfig, ServerHandle};

/// The base config every test spreads from: `ServerConfig::default()`
/// with DHT bootstrap name resolution turned off, so starting a server makes
/// no DNS query and no DNS-over-HTTPS request, and with the public tracker
/// lists off, so adding a torrent announces to nothing. The stock configs
/// leave both on (`embed.rs` asserts it); tests must stay offline, and on a
/// runner with no DNS at all the resolution ladder would otherwise spend its
/// whole budget failing, once per server.
///
/// The tracker half was missing until a Windows CI failure printed a
/// torrent's `sources`: twenty-seven public trackers, one of them answering
/// a scrape 59 seconds old. Every test in `embed.rs` was doing live tracker
/// I/O. [`real_torrent`] passes `trackers: Vec::new()`, which looked like
/// enough and never was -- `EngineFS::merged_trackers` prepends the built-in
/// list and whatever the tracker manager has fetched, below the caller.
pub fn offline_config() -> ServerConfig {
    ServerConfig {
        resolve_dht_bootstrap_names: false,
        use_public_trackers: false,
        // And no multicast either. Two of these switches were not enough:
        // local service discovery stayed on, every test announced its info
        // hashes to the network the runner was on, and two concurrent runs
        // of a fixture built from the same bytes -- the same info hash --
        // found each other and fed each other pieces.
        enable_local_service_discovery: false,
        // An embedder that keeps a pin record and has nothing in it yet.
        // `None` is not the same thing -- it is "nobody said", which keeps
        // every torrent's data and reports it all as pinned -- and it has a
        // test of its own; spreading it here would turn every retention and
        // idle-pause test into one about a cache that may not be touched. A
        // test whose fixture nothing re-seeds opts out through
        // `fixture_pins::keep_what_the_fixture_seeded`.
        pins: Some(Default::default()),
        ..ServerConfig::default()
    }
}

/// Client builder that sends the server's bearer token on every request --
/// every control route requires it, and every server has one.
pub fn bearer_client_builder(handle: &ServerHandle) -> reqwest::blocking::ClientBuilder {
    let mut headers = reqwest::header::HeaderMap::new();
    let token = handle.auth_token().expect("every launch generates a token");
    headers.insert(
        reqwest::header::AUTHORIZATION,
        format!("Bearer {token}").parse().expect("valid header"),
    );
    reqwest::blocking::Client::builder().default_headers(headers)
}

pub fn bearer_client(handle: &ServerHandle) -> anyhow::Result<reqwest::blocking::Client> {
    Ok(bearer_client_builder(handle).build()?)
}

/// A real `.torrent` (correct piece hashes, 16 KiB pieces) built from the
/// files under `dir`, whose name becomes the torrent name. Returns the
/// metainfo bytes and the info hash.
pub fn real_torrent(dir: &Path) -> (Vec<u8>, String) {
    real_torrent_with_pieces(dir, 16 * 1024)
}

/// [`real_torrent`] at another piece length.
pub fn real_torrent_with_pieces(dir: &Path, piece_length: u32) -> (Vec<u8>, String) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let t = librqbit::create_torrent(
            dir,
            librqbit::CreateTorrentOptions {
                name: None,
                trackers: Vec::new(),
                piece_length: Some(piece_length),
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

/// The session's piece store, where all of a torrent's data is.
pub fn piece_store(cache_root: &Path) -> enginefs::piece_store::StoreRoot {
    enginefs::piece_store::StoreRoot::in_download_dir(&cache_root.join("rqbit-downloads"))
}

/// Pre-seed a one-file torrent's every piece where the server reads them:
/// the piece store. The single-file case of `embed.rs`'s
/// `seed_piece_store_pieces`, with the layout asked of the store.
///
/// **Call this after the server has started**, never before: the
/// launch-time sweep deletes every piece directory the embedder's pin set
/// does not name, and one seeded before the process comes up is exactly
/// that.
pub fn seed_single_file(cache_root: &Path, torrent_bytes: &[u8], content: &[u8]) {
    let meta = librqbit::torrent_from_bytes(torrent_bytes).expect("parse the torrent back");
    let info_hash = meta.info_hash.as_string();
    let info = meta.info.data.validate().expect("validated metainfo");
    let piece_length = info.lengths().default_piece_length() as u64;
    assert_eq!(
        content.len() as u64,
        info.lengths().total_length(),
        "the fixture and the torrent disagree about the payload"
    );
    let layout = enginefs::piece_store::PieceLayout::new(
        piece_length,
        content.len() as u64,
        [enginefs::piece_store::FileSpec {
            len: content.len() as u64,
            padding: false,
        }],
    )
    .expect("a layout for the fixture");
    let pieces = enginefs::piece_store::PieceStore::new(
        piece_store(cache_root).torrent_dir(&info_hash),
        Arc::new(layout),
    );
    for (index, piece) in content.chunks(piece_length as usize).enumerate() {
        let path = pieces.piece_path(index as u32);
        std::fs::create_dir_all(path.parent().expect("a bucket")).expect("piece bucket");
        std::fs::write(&path, piece).expect("write a piece");
    }
}

/// How many pieces the store holds for a torrent -- asked of the store, so
/// nothing here has to know how they are laid out.
pub fn pieces_held(cache_root: &Path, info_hash: &str) -> usize {
    piece_store(cache_root).stat(info_hash).pieces.len()
}
