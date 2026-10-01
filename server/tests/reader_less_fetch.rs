//! **What a torrent nobody is reading actually fetches**, measured against a
//! real seeder rather than argued from the code.
//!
//! Everything else in `tests/` that touches retention seeds the piece store
//! by hand and has no swarm at all (see [`fixture_pins`]), so it can say what
//! is *kept* and never what is *fetched*. This file has a second librqbit
//! session holding the whole torrent and dialling the server's own peer
//! listener, so the bytes on the wire are real bytes and the number below is
//! a measurement.
//!
//! The seeder's upload is rate-limited on purpose. The thing being bounded
//! is a *window* -- how long a torrent nobody reads is allowed to run before
//! the reconciler stops it -- and a window has no byte count of its own: over
//! loopback with the limiter off, the same window let 186 MiB of a 256 MiB
//! torrent through on this machine before the tick landed. Pinning the rate
//! turns the window into a number the machine cannot inflate, and the bound
//! below is stated in those units.

use stream_server::ServerConfig;

/// Why a test that seeds its own torrent data runs with the pin set
/// unknown, and which tests may not (see the module). This file is the
/// exception the module names, and measures what the exception costs.
#[path = "support/fixture_pins.rs"]
mod fixture_pins;

/// The offline config, the control client and a real torrent, shared with
/// every binary that starts a server.
#[path = "support/torrent_fixtures.rs"]
mod torrent_fixtures;
use torrent_fixtures::{offline_config, pieces_held};

/// The fixture's piece length. Big enough that 64 MiB is a thousand-odd
/// pieces rather than four thousand files in the store.
const PIECE: u32 = 256 * 1024;

/// The torrent. Sized so that a torrent that never stops is still arriving
/// well past [`FETCH_BOUND`] -- at [`SEEDER_BPS`] the whole thing takes half
/// a minute -- so "still arriving" is a reading and not an artefact of the
/// fixture running out.
const PAYLOAD: usize = 64 * 1024 * 1024;

/// What the seeder is allowed to push. See the module: this is what makes
/// the bound a number instead of a property of the runner's loopback.
const SEEDER_BPS: u32 = 2 * 1024 * 1024;

/// The longest any wait below may take before it is a failure: the stop
/// arriving, the seeder seeing the connection go, the fetch passing
/// [`FETCH_BOUND`]. A bound on a poll, never a sleep -- a run that gets
/// there sooner goes on at once.
const WAIT_BOUND: std::time::Duration =
    std::time::Duration::from_secs(12 * enginefs::reconcile::RECONCILE_INTERVAL.as_secs());

/// How often those polls look.
const POLL: std::time::Duration = std::time::Duration::from_millis(50);

/// The window the claim "nothing more arrives" is measured over, once the
/// torrent has stopped and the seeder has seen the connection go. One
/// whole reconcile interval, so a ladder that started the torrent again on
/// its next tick is inside it. It is the assertion's window and not a wait
/// for anything: nothing is expected to happen in it.
const TAIL: std::time::Duration = enginefs::reconcile::RECONCILE_INTERVAL;

/// The most a torrent nobody reads may fetch before the reconciler stops it.
///
/// **Four reconcile intervals of the seeder's rate.** The torrent runs from
/// the moment it is added until the first timer pass that sees it has no
/// reader and nothing pinned, which is why this is not zero: one interval is
/// the design, and the ladder has no way to be quicker than its own timer.
/// The other three are headroom for a loaded runner whose tick is late --
/// bytes, here, are just a way of writing "the tick did not land".
///
/// Measured on this machine over fourteen runs, four of them concurrent:
/// 5.7 to 6.5 MiB, the stop landing 2.0 s after the add every time. The
/// bound is 16 MiB, a quarter of [`PAYLOAD`], so a torrent that stopped
/// being stopped fails it well before it could finish.
const FETCH_BOUND: u64 = SEEDER_BPS as u64 * 4 * enginefs::reconcile::RECONCILE_INTERVAL.as_secs();

/// A server with a torrent handed to it through `/create`, no stream open
/// on it, and a seeder dialling it.
///
/// The order is deliberate: the payload and the metainfo are built before
/// the server starts, so the add happens as early in the reconciler's first
/// cycle as it can and the window being measured is a whole interval rather
/// than whatever was left of one.
struct Watch {
    handle: stream_server::ServerHandle,
    seeder: Seeder,
    cache_root: std::path::PathBuf,
    info_hash: String,
    _dirs: [tempfile::TempDir; 3],
}

impl Watch {
    fn start(pins: Option<Vec<stream_server::PinKey>>) -> anyhow::Result<Self> {
        let config_dir = tempfile::tempdir()?;
        let cache_dir = tempfile::tempdir()?;
        let src = tempfile::tempdir()?;
        let cache_root = stream_server::resolved_path(&cache_dir.path().join("cache"));
        // Declared before the server starts: the free-space arm of the
        // ladder stops a torrent for want of room, and a run that measured
        // *that* would report the right number for the wrong reason.
        stream_server::pretend_volume_space(&cache_root, u64::MAX);

        let payload = src.path().join("payload.bin");
        write_payload(&payload, PAYLOAD);
        let (torrent, info_hash) = torrent_of(&payload)?;

        let handle = stream_server::start(ServerConfig {
            http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            config_dir: Some(config_dir.path().join("config")),
            cache_dir: Some(cache_root.clone()),
            pins,
            ..offline_config()
        })?;

        let base = format!("http://{}", handle.http_addr());
        let token = handle
            .auth_token()
            .ok_or_else(|| anyhow::anyhow!("every launch generates a token"))?
            .to_owned();
        let client = reqwest::blocking::Client::new();
        client
            .post(format!("{base}/create"))
            .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
            .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
            .send()?
            .error_for_status()?;

        // After the add, never before: the seeder is told one address and
        // dials it, and a dial that arrives before the torrent exists is a
        // handshake the server refuses and does not retry.
        let listen = handle
            .torrent_listen_addr()
            .ok_or_else(|| anyhow::anyhow!("the session listens for peers"))?;
        let seeder = Seeder::dialling(
            src.path(),
            &torrent,
            (std::net::Ipv4Addr::LOCALHOST, listen.port()).into(),
        )?;

        // No stream is opened here, and that is the whole fixture: nothing
        // calls `/{infoHash}/{fileIdx}`, so nothing registers a reader and
        // the ladder's last arm has only the pin set to go on.
        Ok(Self {
            handle,
            seeder,
            cache_root,
            info_hash,
            _dirs: [config_dir, cache_dir, src],
        })
    }

    /// Whether the server's torrent is stopped, by its run state
    /// (`EngineStats::swarm_paused`). A peek: it touches no idle clock.
    fn stopped(&self) -> anyhow::Result<bool> {
        Ok(self.handle.engine_stats(&self.info_hash, &[])?.swarm_paused)
    }

    /// Poll `done` until it holds, or fail after [`WAIT_BOUND`] saying
    /// what was being waited for.
    fn until(
        &self,
        what: &str,
        mut done: impl FnMut(&Self) -> anyhow::Result<bool>,
    ) -> anyhow::Result<()> {
        let deadline = std::time::Instant::now() + WAIT_BOUND;
        while !done(self)? {
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "waited {WAIT_BOUND:?} for {what}: {}",
                self.reading()
            );
            std::thread::sleep(POLL);
        }
        Ok(())
    }

    fn reading(&self) -> Reading {
        Reading {
            fetched: self.seeder.uploaded(),
            held: pieces_held(&self.cache_root, &self.info_hash),
        }
    }

    fn finish(self) -> anyhow::Result<()> {
        self.handle.shutdown()?;
        self.handle.join()?;
        Ok(())
    }
}

/// What the seeder and the store say at one instant.
struct Reading {
    /// Bytes the seeder has pushed -- the number, taken from the side that
    /// cannot stop. The downloader's own counters live in its *running*
    /// state and read as absent once the reconciler has stopped the
    /// torrent, which is the very moment being measured.
    fetched: u64,
    /// Pieces the store holds.
    held: usize,
}

impl std::fmt::Display for Reading {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} of {PAYLOAD} bytes fetched, {} pieces held",
            self.fetched, self.held
        )
    }
}

/// **A torrent nobody reads stops fetching, and what it fetched first is
/// bounded.** The shipping configuration, measured against a real swarm.
///
/// `ServerConfig::pins: Some(Default::default())` is what xtremio publishes
/// for a user who has downloaded nothing -- the ordinary state of the
/// ordinary install. Under it the reconciler's last arm
/// (`reconcile::desired`, `if conditions.playing || conditions.pinned`)
/// stops every torrent with no reader and no pin, and this is that stop
/// costed: **5.7 MiB on this machine, the tick landing 2.0 s after the add,
/// and nothing after it.**
///
/// **Why the bound is not zero.** A torrent is added running -- it has to
/// be, since `/create` is how a client learns a torrent's files and a
/// paused torrent that never reached a peer could not report a swarm at
/// all -- and the ladder is a two-second timer, so there is always one
/// interval in which a reader-less torrent fetches at whatever rate its
/// peers give it. [`FETCH_BOUND`] is four of those intervals at the
/// seeder's pinned rate; see its comment for why the extra three.
///
/// **The bound is the smaller half of this test.** The assertion that
/// carries it is the tail: a whole reconcile interval after the stop in
/// which not one byte arrives. A regression that made the ladder keep
/// running reader-less torrents never stops (the wait fails), or starts it
/// again (the tail fails), whatever the runner's speed; and
/// `an_unknown_pin_set_never_stops_a_torrent_nobody_reads` is the proof
/// that the seeder in this file can in fact feed a torrent that is allowed
/// to run -- without it, a broken dial would pass here by fetching nothing.
#[test]
fn a_torrent_nobody_reads_stops_fetching_within_a_few_reconcile_intervals() -> anyhow::Result<()> {
    let watch = Watch::start(offline_config().pins)?;
    watch.until("the reconciler to stop the torrent", Watch::stopped)?;
    // The stop is a state; what the seeder has pushed is only final once
    // it has seen the connection go.
    watch.until("the seeder to see the connection go", |watch| {
        Ok(watch.seeder.live_peers() == 0)
    })?;
    let at_the_stop = watch.reading();
    std::thread::sleep(TAIL);
    let reading = watch.reading();
    println!("empty pin record: {reading}, {at_the_stop} at the stop");

    anyhow::ensure!(
        reading.fetched == at_the_stop.fetched,
        "a torrent nobody reads was fed again in the {TAIL:?} after it stopped: \
         {at_the_stop} at the stop, {reading} after"
    );
    anyhow::ensure!(
        reading.fetched <= FETCH_BOUND,
        "a torrent nobody reads fetched more than {FETCH_BOUND} bytes before it was stopped: \
         {reading}"
    );
    watch.finish()
}

/// **An unknown pin set never stops it, and that is the documented
/// exception.** The same fixture with [`fixture_pins`]'s one field changed.
///
/// `pins: None` is "nobody told me", which sets `PinsUnknown`, which makes
/// `Engine::is_pinned` answer true for every torrent there is. The ladder's
/// last arm then reads `pinned` and runs the torrent for ever, and
/// `Engine::reclaim_rest` breaks before it takes a piece -- so a torrent
/// nobody has ever read fetches the whole thing and keeps it. Measured
/// here: **still arriving at 2 MiB/s long after the empty record's torrent
/// has stopped**, and with the limiter off a 256 MiB torrent completed in
/// 2.6 s on this machine.
///
/// This is the reading `docs/known-issues.md` keeps open: an engine with no
/// reader fetches the lot when nobody has told the server what is pinned.
/// It is real, and it is what `ServerConfig::default()` does -- `pins: None`
/// is the default -- but it is not what the shipping client configures; its
/// sibling above is.
///
/// [`fixture_pins`]'s module docs warn that a test about the reconciler
/// must not run under `None`, because it would be asserting about a torrent
/// that may not be stopped. That is exactly right, and exactly what this
/// test asserts: not stopping is the subject. Nothing here seeds a store,
/// so none of the rest of that warning applies.
#[test]
fn an_unknown_pin_set_never_stops_a_torrent_nobody_reads() -> anyhow::Result<()> {
    let watch = Watch::start(fixture_pins::keep_what_the_fixture_seeded(offline_config()).pins)?;
    // Past what the empty record's torrent may fetch in all, and then
    // further still: a torrent the ladder had stopped would fail one wait
    // or the other.
    watch.until("the fetch to pass the empty record's bound", |watch| {
        Ok(watch.seeder.uploaded() > FETCH_BOUND)
    })?;
    let past_the_bound = watch.reading();
    watch.until("the fetch to go on past it", |watch| {
        Ok(watch.seeder.uploaded() > past_the_bound.fetched)
    })?;
    anyhow::ensure!(
        !watch.stopped()?,
        "an unknown pin set stopped a torrent nobody reads; if that is now the design, \
         this test and the note in docs/known-issues.md are what has to change: {}",
        watch.reading()
    );
    // The other half of what `PinsUnknown` does: `reclaim_rest` breaks
    // before it takes anything, so every piece that arrived is still on the
    // disk.
    //
    // "Arrived" is the seeder's count, and that count runs ahead of the
    // store by more than one reading can allow for. It grows when the
    // seeder's socket accepts a block, not when the server has read it,
    // and the server keeps librqbit's whole request window outstanding --
    // 128 blocks, eight pieces. Every piece is synced to the disk before
    // the store holds it, so a disk busy with someone else's writes lets
    // that window fill: measured under fsync pressure, the store trailed
    // the count by up to eight pieces at the instant of reading and caught
    // up within a few seconds, never having lost one. So the count is read
    // once, the flow is left running, and the store is given until
    // [`WAIT_BOUND`] to make pieces of everything counted -- all but the
    // four this check has always allowed for.
    let fetched = watch.seeder.uploaded();
    let landed = pieces_complete(&watch.cache_root, &watch.info_hash);
    let deadline = std::time::Instant::now() + WAIT_BOUND;
    let mut held = pieces_held(&watch.cache_root, &watch.info_hash);
    while (held as u64 + 4) < fetched / PIECE as u64 {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "an unknown pin set let a pass take pieces it fetched: {fetched} fetched, \
             {held} held after {WAIT_BOUND:?}"
        );
        std::thread::sleep(POLL);
        held = pieces_held(&watch.cache_root, &watch.info_hash);
    }
    println!("unknown pin set: {fetched} bytes fetched, {held} pieces held");
    // And the count alone could be made up by pieces that arrived while it
    // was being waited for, so the pieces themselves are asked about too:
    // every one that had landed when the count was read is still there.
    let taken: Vec<u32> = landed
        .difference(&pieces_complete(&watch.cache_root, &watch.info_hash))
        .copied()
        .collect();
    anyhow::ensure!(
        taken.is_empty(),
        "an unknown pin set let a pass take pieces it fetched: {taken:?} were held and are gone"
    );
    watch.finish()
}

/// The pieces the store holds a complete copy of, by index -- which pieces,
/// where [`pieces_held`] says how many.
fn pieces_complete(
    cache_root: &std::path::Path,
    info_hash: &str,
) -> std::collections::BTreeSet<u32> {
    torrent_fixtures::piece_store(cache_root)
        .stat(info_hash)
        .pieces
        .into_iter()
        .filter(|piece| piece.complete.is_some())
        .filter_map(|piece| u32::try_from(piece.index).ok())
        .collect()
}

/// A second librqbit session with the whole torrent, listening, dialling
/// one address and rate-limited to [`SEEDER_BPS`].
///
/// It owns its runtime: the session's tasks are spawned on it, so dropping
/// it would stop the seeder mid-measurement.
struct Seeder {
    _runtime: tokio::runtime::Runtime,
    _session: std::sync::Arc<librqbit::Session>,
    handle: std::sync::Arc<librqbit::ManagedTorrent>,
}

impl Seeder {
    fn dialling(
        content_dir: &std::path::Path,
        torrent_bytes: &[u8],
        peer: std::net::SocketAddr,
    ) -> anyhow::Result<Self> {
        let runtime = tokio::runtime::Runtime::new()?;
        let (session, handle) = runtime.block_on(async {
            let session = librqbit::Session::new_with_opts(
                content_dir.to_path_buf(),
                librqbit::SessionOptions {
                    // No DHT, no persistence, no local discovery: the one
                    // address below is the only peer this session can ever
                    // have, so every byte it uploads went to the server
                    // under test.
                    dht: None,
                    persistence: None,
                    disable_local_service_discovery: true,
                    listen: Some(librqbit::ListenerOptions {
                        listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                        ..Default::default()
                    }),
                    ratelimits: librqbit::limits::LimitsConfig {
                        upload_bps: std::num::NonZeroU32::new(SEEDER_BPS),
                        download_bps: None,
                    },
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
                        // The payload is already on disk and is the whole
                        // torrent; `overwrite` is what lets the check find
                        // it instead of starting an empty download.
                        overwrite: true,
                        initial_peers: Some(vec![peer]),
                        ..Default::default()
                    }),
                )
                .await?
                .into_handle()
                .ok_or_else(|| anyhow::anyhow!("the seeder's add produced no handle"))?;
            handle.wait_until_initialized().await?;
            anyhow::Ok((session, handle))
        })?;
        Ok(Self {
            _runtime: runtime,
            _session: session,
            handle,
        })
    }

    /// Payload bytes this seeder has pushed to peers, cumulative.
    fn uploaded(&self) -> u64 {
        self.handle.stats().uploaded_bytes
    }

    /// Peers this seeder is connected to right now: the server, or nobody.
    fn live_peers(&self) -> u32 {
        self.handle
            .stats()
            .live
            .map_or(0, |live| live.snapshot.peer_stats.live)
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

/// A non-trivial payload, **different every run**, so piece hashes mean
/// something and no two runs share an info hash.
///
/// The randomness is not decoration. The embedded server's librqbit session
/// has local service discovery on (`SessionTuning::lsd`; only the DHT's
/// bootstrap names and the public trackers are switched off for tests), so
/// two servers on this machine holding the same info hash find each other
/// and swap pieces. With a fixed payload every run of this file produced the
/// same hash, and four copies run at once fed one another: the store came
/// out holding more pieces than this test's own seeder had uploaded, which
/// is a measurement of the wrong swarm.
fn write_payload(path: &std::path::Path, len: usize) {
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).expect("a nonce for this run's info hash");
    let mut data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    data[..nonce.len()].copy_from_slice(&nonce);
    std::fs::write(path, data).expect("write payload");
}
