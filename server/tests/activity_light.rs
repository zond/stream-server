//! **The activity light against real peers**: a torrent download from a
//! seeder lights the way down, and seeding it to a leecher lights the way
//! up, with bytes on the wire rather than counters set by hand.
//!
//! The judge's arithmetic is `enginefs::traffic`'s own unit tests; what
//! only a swarm can show is that the counters the server feeds it are the
//! peers' -- summed over the torrents that exist, read without starting
//! anything -- and that nothing playing is what the reading says.

use stream_server::ServerConfig;

#[path = "support/torrent_fixtures.rs"]
mod torrent_fixtures;
use torrent_fixtures::offline_config;

/// The fixture's piece length.
const PIECE: u32 = 256 * 1024;

/// The torrent: at [`PEER_BPS`] it takes four seconds each way, so a
/// transfer is still moving when the first poll takes its baseline and has
/// moved by the time the window after it closes.
const PAYLOAD: usize = 16 * 1024 * 1024;

/// What the seeder may upload and the leecher may download.
const PEER_BPS: u32 = 4 * 1024 * 1024;

/// The longest any wait below may take before it is a failure. A bound on
/// a poll, never a sleep.
const WAIT_BOUND: std::time::Duration = std::time::Duration::from_secs(90);

const POLL: std::time::Duration = std::time::Duration::from_millis(100);

/// Polls `done` until it holds, or fails after [`WAIT_BOUND`] with the last
/// light reading.
fn until(
    handle: &stream_server::ServerHandle,
    what: &str,
    mut done: impl FnMut(&enginefs::traffic::BackgroundTraffic) -> anyhow::Result<bool>,
) -> anyhow::Result<enginefs::traffic::BackgroundTraffic> {
    let deadline = std::time::Instant::now() + WAIT_BOUND;
    loop {
        let reading = handle.background_traffic()?;
        if done(&reading)? {
            return Ok(reading);
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "waited {WAIT_BOUND:?} for {what}: {reading:?}"
        );
        std::thread::sleep(POLL);
    }
}

/// **Downloading lights the way down, seeding lights the way up.** A
/// torrent pinned as a download, fed by a seeder, with nothing playing:
/// the reading's down half comes on and its up half stays off. Once it is
/// whole and the seeder has gone, a leecher that dials the server takes it
/// back out, and now the up half comes on.
#[test]
fn a_download_lights_the_way_down_and_seeding_it_lights_the_way_up() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let leech_dir = tempfile::tempdir()?;
    let cache_root = stream_server::resolved_path(&cache_dir.path().join("cache"));
    stream_server::pretend_volume_space(&cache_root, u64::MAX);

    let payload = src.path().join("payload.bin");
    write_payload(&payload, PAYLOAD);
    let (torrent, info_hash) = torrent_of(&payload)?;

    let handle = stream_server::start(ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    })?;
    let base = format!("http://{}", handle.http_addr());
    torrent_fixtures::bearer_client_builder(&handle)
        .build()?
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?;
    handle.pin_download(&info_hash, 0, &[])?;
    let before = handle.background_traffic()?;
    assert!(!before.active, "nothing has moved yet: {before:?}");

    let listen = handle
        .torrent_listen_addr()
        .ok_or_else(|| anyhow::anyhow!("the session listens for peers"))?;
    let server_peer: std::net::SocketAddr = (std::net::Ipv4Addr::LOCALHOST, listen.port()).into();
    let seeder = Peer::dialling(src.path(), &torrent, server_peer, Rate::Upload)?;

    let down = until(&handle, "the download to light the way down", |reading| {
        Ok(reading.downloading)
    })?;
    assert!(
        !down.uploading && !down.playing && down.active,
        "only the way down, nothing playing: {down:?}"
    );
    assert!(down.bytes_downloaded > 0);

    // Whole, and the seeder gone, so the only peer left is the leecher.
    let deadline = std::time::Instant::now() + WAIT_BOUND;
    while !handle
        .downloads()?
        .iter()
        .any(|row| row.info_hash == info_hash && row.complete)
    {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the download never completed"
        );
        std::thread::sleep(POLL);
    }
    drop(seeder);
    until(&handle, "the way down to go dark", |reading| {
        Ok(!reading.downloading)
    })?;

    let leecher = Peer::dialling(leech_dir.path(), &torrent, server_peer, Rate::Download)?;
    let up = until(&handle, "seeding to light the way up", |reading| {
        Ok(reading.uploading)
    })?;
    assert!(
        !up.downloading && !up.playing && up.active,
        "only the way up, nothing playing: {up:?}"
    );
    assert!(up.bytes_uploaded > 0);
    assert!(
        leecher.fetched() > 0,
        "the leecher's bytes are the ones the server sent"
    );

    drop(leecher);
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// Which way a [`Peer`] is rate-limited.
enum Rate {
    Upload,
    Download,
}

/// A second librqbit session holding (or wanting) the torrent, dialling
/// one address -- the server -- and rate-limited one way to [`PEER_BPS`].
/// It owns its runtime: dropping it stops the peer.
struct Peer {
    _runtime: tokio::runtime::Runtime,
    _session: std::sync::Arc<librqbit::Session>,
    handle: std::sync::Arc<librqbit::ManagedTorrent>,
}

impl Peer {
    fn dialling(
        content_dir: &std::path::Path,
        torrent_bytes: &[u8],
        peer: std::net::SocketAddr,
        rate: Rate,
    ) -> anyhow::Result<Self> {
        let runtime = tokio::runtime::Runtime::new()?;
        let limit = std::num::NonZeroU32::new(PEER_BPS);
        let ratelimits = match rate {
            Rate::Upload => librqbit::limits::LimitsConfig {
                upload_bps: limit,
                download_bps: None,
            },
            Rate::Download => librqbit::limits::LimitsConfig {
                upload_bps: None,
                download_bps: limit,
            },
        };
        let (session, handle) = runtime.block_on(async {
            let session = librqbit::Session::new_with_opts(
                content_dir.to_path_buf(),
                librqbit::SessionOptions {
                    // No DHT, no persistence, no local discovery: the one
                    // address below is the only peer this session has.
                    dht: None,
                    persistence: None,
                    disable_local_service_discovery: true,
                    listen: Some(librqbit::ListenerOptions {
                        listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                        ..Default::default()
                    }),
                    ratelimits,
                    ..Default::default()
                },
            )
            .await?;
            let handle = session
                .add_torrent(
                    librqbit::AddTorrent::from_bytes(bytes::Bytes::copy_from_slice(torrent_bytes)),
                    Some(librqbit::AddTorrentOptions {
                        paused: false,
                        output_folder: Some(content_dir.to_string_lossy().into_owned()),
                        overwrite: true,
                        initial_peers: Some(vec![peer]),
                        ..Default::default()
                    }),
                )
                .await?
                .into_handle()
                .ok_or_else(|| anyhow::anyhow!("the peer's add produced no handle"))?;
            handle.wait_until_initialized().await?;
            anyhow::Ok((session, handle))
        })?;
        Ok(Self {
            _runtime: runtime,
            _session: session,
            handle,
        })
    }

    /// Payload bytes this peer has received, cumulative.
    fn fetched(&self) -> u64 {
        self.handle
            .stats()
            .live
            .map_or(0, |live| live.snapshot.fetched_bytes)
    }
}

/// A single-file torrent over `path`, with [`PIECE`] pieces. Returns the
/// metainfo bytes and the info hash.
fn torrent_of(path: &std::path::Path) -> anyhow::Result<(Vec<u8>, String)> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let torrent = librqbit::create_torrent(
            path,
            librqbit::CreateTorrentOptions {
                name: None,
                trackers: Vec::new(),
                piece_length: Some(PIECE),
            },
            &librqbit::spawn_utils::BlockingSpawner::new(1),
        )
        .await?;
        Ok((
            torrent.as_bytes()?.to_vec(),
            torrent.info_hash().as_string(),
        ))
    })
}

/// `len` bytes with a random head, so this run's info hash is its own and
/// no concurrent run of the same fixture can feed it.
fn write_payload(path: &std::path::Path, len: usize) {
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).expect("a nonce for this run's info hash");
    let mut data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    data[..nonce.len()].copy_from_slice(&nonce);
    std::fs::write(path, data).expect("write payload");
}
