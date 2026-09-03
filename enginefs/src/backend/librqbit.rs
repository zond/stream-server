use crate::backend::{
    BackendFileInfo, BackendMemoryDiagnostics, EngineStats, FileStreamTrait, Growler,
    PeerDiscovery, PeerSearch, PieceReadiness, Source, StartupPhase, StatsFile, StatsOptions,
    SwarmCap, TorrentBackend, TorrentFilePriorityPlan, TorrentHandle, TorrentListenPort,
    TorrentPlacement, TorrentSource,
};
use crate::scrape::SwarmScraper;
use anyhow::{Context, Result};
use librqbit::{ManagedTorrent, ManagedTorrentState, Session};
use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Upper bound on how long a stream request blocks waiting for librqbit to
/// leave its `Initializing` state (opening/hash-checking files; for a magnet
/// the metadata has already been resolved by `Session::add_torrent`). A fresh
/// or cached torrent initializes in well under a second; a large partially
/// downloaded torrent on slow storage can take tens of seconds. Requests
/// blocked on this gate mirror Stremio's server.js, which holds the HTTP
/// request until data exists -- but they never hang forever.
pub const TORRENT_INIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Why a torrent could not be waited into a streamable state. Surfaced through
/// `anyhow` so `routes/stream.rs` can downcast it into a proper non-2xx.
#[derive(Debug, thiserror::Error)]
pub enum TorrentInitError {
    #[error("torrent {info_hash} is still initializing after {timeout_secs}s")]
    TimedOut {
        info_hash: String,
        timeout_secs: u64,
    },
    #[error("torrent {info_hash} failed to initialize: {reason}")]
    Failed { info_hash: String, reason: String },
}

/// Initialization gate shared by the real backend and the test fakes: await
/// `wait` (librqbit's `ManagedTorrent::wait_until_initialized`, or a fake
/// standing in for it) bounded by `timeout`. Blocks -- it never returns early
/// with an empty result -- and maps the two failure modes to `TorrentInitError`.
pub(crate) async fn await_initialized<F>(
    info_hash: &str,
    timeout: Duration,
    wait: F,
) -> std::result::Result<(), TorrentInitError>
where
    F: Future<Output = anyhow::Result<()>>,
{
    let start = Instant::now();
    let result = match tokio::time::timeout(timeout, wait).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(TorrentInitError::Failed {
            info_hash: info_hash.to_string(),
            reason: format!("{e:#}"),
        }),
        Err(_elapsed) => Err(TorrentInitError::TimedOut {
            info_hash: info_hash.to_string(),
            timeout_secs: timeout.as_secs(),
        }),
    };
    match &result {
        Ok(()) => debug!(
            info_hash,
            waited_ms = start.elapsed().as_millis() as u64,
            "torrent left the initializing state"
        ),
        Err(e) => warn!(
            info_hash,
            waited_ms = start.elapsed().as_millis() as u64,
            error = %e,
            "torrent did not become ready"
        ),
    }
    result
}

/// A file-selection update that could not be applied because the torrent was
/// still initializing, parked until initialization completes. Latest-wins:
/// re-deferring replaces the queued op, and a direct apply once the torrent is
/// ready supersedes anything still queued (`supersede`). One waiter task per
/// slot drains the queue after the gate opens.
///
/// The "never clobber a newer one" guarantee only covers ops parked *before*
/// the gate opens: `Reconcile`'s planner (`plan_only_files`) ignores the live
/// selection and just applies the active/hot pair captured when it was
/// queued, so a parked op is not re-derived from anything that changed while
/// it waited -- it is latest-wins among what was queued, not a re-plan
/// against fresh state. There is also a sub-microsecond ordering window
/// between the waiter draining a parked op and a direct op landing at the
/// exact moment initialization completes; whichever runs last wins. This is
/// benign: both derive from the same activation (the same file just started
/// streaming) and both selections always include the streamed file, so
/// neither ordering can starve playback.
pub(crate) struct DeferredSelection<Op> {
    pending: Mutex<Option<Op>>,
    waiter_running: AtomicBool,
}

impl<Op: Send + 'static> DeferredSelection<Op> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(None),
            waiter_running: AtomicBool::new(false),
        })
    }

    /// Whether an op is queued waiting for initialization.
    pub(crate) fn has_pending(&self) -> bool {
        self.pending.lock().is_some()
    }

    /// Drop whatever is queued: the caller is about to apply a newer op
    /// directly, which supersedes it.
    pub(crate) fn supersede(&self) -> Option<Op> {
        self.pending.lock().take()
    }

    /// Queue `op` and make sure a waiter is running. `wait` is only awaited by
    /// a newly spawned waiter; `apply` runs for each queued op once the gate
    /// opens. If the gate fails (timeout / init error) the queued op is dropped
    /// with a warning -- the next direct call retries the whole cycle.
    pub(crate) fn defer<W, A, AF>(self: &Arc<Self>, op: Op, wait: W, apply: A)
    where
        W: Future<Output = std::result::Result<(), TorrentInitError>> + Send + 'static,
        A: Fn(Op) -> AF + Send + 'static,
        AF: Future<Output = ()> + Send,
    {
        *self.pending.lock() = Some(op);
        if self.waiter_running.swap(true, Ordering::AcqRel) {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            match wait.await {
                Ok(()) => loop {
                    let next = this.pending.lock().take();
                    match next {
                        Some(op) => apply(op).await,
                        None => {
                            this.waiter_running.store(false, Ordering::Release);
                            // Close the race with a `defer` that queued between our
                            // `take` and the store above: it saw the waiter running
                            // and returned, so we must drain it. Safe to keep looping
                            // here because the gate is Ok(()) for every iteration --
                            // a late arrival re-planned against a successful gate is
                            // never stale.
                            if this.has_pending()
                                && !this.waiter_running.swap(true, Ordering::AcqRel)
                            {
                                continue;
                            }
                            break;
                        }
                    }
                },
                Err(e) => this.handle_gate_error(&e),
            }
        });
    }

    /// Handle a gate that resolved to `Err`: drop at most the one op parked
    /// right now, then release the slot unconditionally -- no
    /// drain-and-recheck loop against this (now stale) verdict. The gate
    /// result is a one-shot value, not something ops that show up afterwards
    /// should be judged against.
    ///
    /// A `defer` that races in between our `take` and the `waiter_running`
    /// release below sees `waiter_running` still true and returns without
    /// spawning a new waiter -- but because we unconditionally release the
    /// slot afterwards, the *next* direct call (which is how first-play and
    /// reconcile keep re-triggering) spawns a fresh waiter with a fresh gate
    /// instead of that op being silently judged against our stale failure,
    /// or worse, kept alive indefinitely by a loop that never lets go.
    ///
    /// Split out from `defer`'s spawned task so this exact edge -- a `defer`
    /// landing between the `take` and the release -- has direct unit
    /// coverage without needing to reproduce a genuine cross-thread race.
    fn handle_gate_error(&self, e: &TorrentInitError) {
        if self.pending.lock().take().is_some() {
            warn!(
                error = %e,
                "dropping deferred file-selection update: torrent never became ready"
            );
        }
        self.waiter_running.store(false, Ordering::Release);
    }
}

/// Per-torrent deferred selection slots, keyed by info hash and shared by
/// every `LibrqbitHandle` clone (handles are re-created by `get_torrent`).
type DeferredSelections = Arc<Mutex<HashMap<String, Arc<DeferredSelection<DeferredOp>>>>>;

/// Per-torrent pinned file sets (`TorrentHandle::pin_file`), keyed by info
/// hash and shared by every handle clone for the same reason as
/// `DeferredSelections`. Consulted by every want-set update so a pinned file
/// survives playback switching. The map itself is not persisted: librqbit
/// persists the resulting `only_files` (so the file keeps downloading
/// across a restart) and `BackendEngineFS::restore_pinned_downloads`
/// rebuilds the map at startup from its `pinned-downloads.json` by calling
/// `pin_file` again for every restored torrent.
type PinnedFiles = Arc<Mutex<HashMap<String, BTreeSet<usize>>>>;

/// The last error text reported to the log for a torrent, keyed by info
/// hash and shared by every handle clone like [`PinnedFiles`]. Statistics
/// are polled while a broken download is on screen, so the full librqbit
/// error chain goes to the log once per distinct error rather than once
/// per poll (see `LibrqbitHandle::client_torrent_error`).
type ReportedErrors = Arc<Mutex<HashMap<String, String>>>;

/// Where each open reader is positioned, keyed by `(info hash, file index)`
/// and shared by every handle clone like [`PinnedFiles`].
///
/// The startup-window progress a client renders has to describe the bytes
/// somebody is actually waiting for. Computed from the file head it
/// described, after a seek, a region nobody was fetching -- so it sat at 0%
/// while the seek region streamed perfectly. `get_file_reader` records the
/// offset it was opened at (a `Range` request, a seek and a re-open all
/// arrive as a fresh reader at the new offset), and `stats` reads it back.
type StreamPositions = Arc<Mutex<HashMap<(String, usize), u64>>>;

/// What a client is told about a torrent librqbit put in an error state.
/// librqbit's `TorrentStats.error` is the `{e:?}` of an anyhow chain
/// naming absolute cache and download paths, so only this fixed message
/// crosses the boundary -- like `MagnetAddError::client_message` and
/// `PinDownloadError::client_message`, the chain itself is for the log.
pub const TORRENT_ERROR_MESSAGE: &str = "the torrent is in an error state (its download folder may be unwritable, full or gone); see server logs";

/// A selection op parked until the torrent initializes.
#[derive(Debug, Clone, Copy)]
struct DeferredOp {
    op: SelectionOp,
    context: &'static str,
}

/// What `apply_selection` does when the torrent is still initializing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitPolicy {
    /// Block (bounded by `TORRENT_INIT_TIMEOUT`) and then apply. Used on the
    /// request path right before a reader is opened, which waits anyway.
    Wait,
    /// Return immediately and apply once initialized. Used for reconcile,
    /// which is also driven from background cleanup loops that must not stall.
    Defer,
    /// Skip: a deselect while nothing has started downloading is moot.
    Skip,
}

/// Public BitTorrent mainline DHT bootstrap nodes used to seed librqbit's
/// routing table when it starts cold (no persisted `dht.json`, or a
/// deserialize failure -- see [`DEFAULT_DHT_BOOTSTRAP_NODES`] doc below).
///
/// We override librqbit's own built-in fallback (`DHT_BOOTSTRAP` in the
/// `zond/rqbit` fork's `crates/dht/src/lib.rs`) because that list is only
/// two hosts (`dht.transmissionbt.com:6881`, `dht.libtorrent.org:25401`),
/// which is a single point of failure: `DhtWorker::bootstrap`
/// (`crates/dht/src/dht.rs`) queries every address in parallel and only
/// reports failure if *all* of them fail to respond, and that failure is
/// fatal to the whole DHT worker task (an unhandled `result?` in
/// `DhtWorker::start`'s `select!` loop kills the DHT, not just the
/// bootstrap step) -- so losing both of two hosts to an outage, a
/// firewall, or transient DNS trouble takes the DHT down with them. Widen
/// the odds instead: this is librqbit's two plus the other conventional
/// public bootstrap nodes mainstream clients (Transmission, libtorrent
/// itself via qBittorrent, uTorrent, Vuze/Azureus) ship, ordered
/// most-reliable first.
///
/// Every host below was confirmed to currently resolve (`getent hosts`,
/// cross-checked with `dig @8.8.8.8`/`dig @1.1.1.1`) before being added.
/// Two other commonly cited candidates were tried and dropped because they
/// did not resolve at the time of writing: `router.bitcomet.com` (NXDOMAIN
/// -- the `bitcomet.com` zone answers but has no such name) and
/// `dht.anacrolix.link` (its CNAME chain currently dead-ends in NXDOMAIN at
/// `pub.instances.scw.cloud`).
///
/// Overridable via the `dhtBootstrapNodes` server setting
/// (`server/src/routes/system.rs`) -- see [`resolve_dht_bootstrap_nodes`].
///
/// **Bootstrapping only matters while the routing table is cold, but
/// "cold" is not automatic.** `PersistentDht::create`
/// (`crates/dht/src/persistence.rs`) loads any existing `dht.json` table
/// before the DHT worker starts, but `DhtState::with_config`'s worker
/// (`crates/dht/src/dht.rs`) unconditionally races `self.bootstrap` against
/// the persisted table on *every* session start -- there is no "skip
/// bootstrap, the table is already warm" branch, and total bootstrap
/// failure kills the DHT worker even with a warm table. So a persisted
/// table (the normal case after the first run) makes bootstrap-host
/// reachability *less consequential* in practice, since the DHT already
/// has real peers to query while the bootstrap requests race in the
/// background, but it does not make bootstrap host reachability
/// irrelevant. Widening the list still hedges the cases where it is fully
/// relevant: first run, a wiped or corrupted `dht.json`, and a cold start
/// on a fresh install or container.
pub const DEFAULT_DHT_BOOTSTRAP_NODES: &[&str] = &[
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "router.utorrent.com:6881",
    "dht.libtorrent.org:25401",
    "dht.aelitis.com:6881",
];

/// Resolve the effective DHT bootstrap address list: `configured` (already
/// validated by `server/src/routes/system.rs`'s `dhtBootstrapNodes` setting
/// -- non-empty `host:port` strings only) if non-empty, REPLACING
/// [`DEFAULT_DHT_BOOTSTRAP_NODES`] entirely; otherwise the default set.
/// Mirrors `SessionOptions.dht.bootstrap_addrs`'s own `None` = built-in
/// convention, except our built-in default is the widened list above
/// rather than librqbit's two-host one.
pub fn resolve_dht_bootstrap_nodes(configured: &[String]) -> Vec<String> {
    if configured.is_empty() {
        DEFAULT_DHT_BOOTSTRAP_NODES
            .iter()
            .map(|s| s.to_string())
            .collect()
    } else {
        configured.to_vec()
    }
}

pub struct LibrqbitBackend {
    pub session: Arc<Session>,
    download_dir: PathBuf,
    deferred_selections: DeferredSelections,
    pinned_files: PinnedFiles,
    reported_errors: ReportedErrors,
    /// Backend-wide reader positions (see [`StreamPositions`]).
    stream_positions: StreamPositions,
    /// Backend-wide tracker-scrape cache, shared by every handle so one
    /// torrent is scraped once however many handles report on it.
    swarm_scraper: Arc<SwarmScraper>,
}

impl LibrqbitBackend {
    /// Open a session storing downloads under `download_dir`, listening for
    /// incoming peers on `listen_port` (see [`TorrentListenPort`]) and
    /// seeding its DHT routing table from `dht_bootstrap_nodes` when cold
    /// (empty uses [`DEFAULT_DHT_BOOTSTRAP_NODES`]; see
    /// [`resolve_dht_bootstrap_nodes`]).
    pub async fn new(
        download_dir: PathBuf,
        listen_port: TorrentListenPort,
        dht_bootstrap_nodes: Vec<String>,
    ) -> Result<(Self, HashMap<String, LibrqbitHandle>)> {
        tokio::fs::create_dir_all(&download_dir).await?;
        debug!(path = ?download_dir, "Storing downloads");

        // librqbit 9.0.1's ListenerOptions binds a single address instead of
        // the old `listen_port_range: 42000..42010`, so a `Fixed` range's
        // port-fallback is done here: try each port in order and keep the
        // first that binds. `Ephemeral` is the single candidate 0, which
        // librqbit itself defaults to and resolves to the bound port.
        let bootstrap_addrs = resolve_dht_bootstrap_nodes(&dht_bootstrap_nodes);
        let session = {
            let mut last_err = None;
            let mut session = None;
            for port in listen_port.candidates() {
                let session_opts = librqbit::SessionOptions {
                    listen: Some(librqbit::ListenerOptions {
                        listen_addr: (std::net::Ipv6Addr::UNSPECIFIED, port).into(),
                        enable_upnp_port_forwarding: true,
                        ..Default::default()
                    }),
                    persistence: Some(librqbit::SessionPersistenceConfig::Json {
                        folder: Some(download_dir.clone()),
                    }),
                    // Persist each torrent's verified-piece bitfield
                    // (`<info hash>.bitv` in the persistence folder) so a
                    // restart validates a sample of pieces instead of
                    // re-hashing every file; a corrupted sample falls back
                    // to the full check. Matters most for pinned offline
                    // downloads, which are large and restart-resident.
                    fastresume: true,
                    // Pin the DHT routing-table dump next to the session
                    // state. librqbit's default resolves through
                    // `directories::ProjectDirs` (HOME/XDG), which has no
                    // answer on Android and would fail `Session::new`.
                    // `bootstrap_addrs` widens librqbit's two-host default
                    // (or applies the operator's `dhtBootstrapNodes`
                    // override) -- see `DEFAULT_DHT_BOOTSTRAP_NODES`.
                    dht: Some(librqbit::DhtSessionConfig {
                        bootstrap_addrs: Some(bootstrap_addrs.clone()),
                        persistence: Some(librqbit::dht::DhtPersistenceConfig {
                            config_filename: Some(download_dir.join("dht.json")),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    connect: Some(librqbit::ConnectionOptions {
                        peer_opts: Some(librqbit::PeerConnectionOptions {
                            connect_timeout: Some(Duration::from_secs(10)),
                            read_write_timeout: Some(Duration::from_secs(30)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                match Session::new_with_opts(download_dir.clone(), session_opts).await {
                    Ok(s) => {
                        session = Some(s);
                        break;
                    }
                    Err(e) => {
                        debug!(port, error = %e, "librqbit listen port unavailable; trying next");
                        last_err = Some(e);
                    }
                }
            }
            match session {
                Some(s) => s,
                None => {
                    return Err(last_err.unwrap_or_else(|| {
                        anyhow::anyhow!("no librqbit listen port available in {listen_port:?}")
                    }));
                }
            }
        };
        let deferred_selections: DeferredSelections = Default::default();
        let pinned_files: PinnedFiles = Default::default();
        let reported_errors: ReportedErrors = Default::default();
        let stream_positions: StreamPositions = Default::default();
        let swarm_scraper = SwarmScraper::network();
        // Restore from session
        let mut restored_handles = session.with_torrents(|iter| {
            let mut map = HashMap::new();
            for (_id, handle) in iter {
                let info_hash = handle.info_hash().as_string();
                map.insert(
                    info_hash.clone(),
                    LibrqbitHandle {
                        handle: handle.clone(),
                        info_hash,
                        session: session.clone(),
                        deferred_selections: deferred_selections.clone(),
                        pinned_files: pinned_files.clone(),
                        reported_errors: reported_errors.clone(),
                        stream_positions: stream_positions.clone(),
                        swarm_scraper: swarm_scraper.clone(),
                    },
                );
            }
            map
        });

        // Restore from .cache directory
        let cache_dir = download_dir.join(".cache");
        if let Ok(mut entries) = tokio::fs::read_dir(&cache_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "torrent")
                    && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                {
                    let info_hash = stem.to_string();
                    if !restored_handles.contains_key(&info_hash)
                        && let Ok(bytes) = tokio::fs::read(&path).await
                    {
                        let bytes = bytes::Bytes::from(bytes);
                        let add_torrent = librqbit::AddTorrent::from_bytes(bytes);
                        match session.add_torrent(add_torrent, None).await {
                            Ok(response) => {
                                if let librqbit::AddTorrentResponse::Added(_, handle)
                                | librqbit::AddTorrentResponse::AlreadyManaged(_, handle) =
                                    response
                                {
                                    restored_handles.insert(
                                        info_hash.clone(),
                                        LibrqbitHandle {
                                            handle,
                                            info_hash,
                                            session: session.clone(),
                                            deferred_selections: deferred_selections.clone(),
                                            pinned_files: pinned_files.clone(),
                                            reported_errors: reported_errors.clone(),
                                            stream_positions: stream_positions.clone(),
                                            swarm_scraper: swarm_scraper.clone(),
                                        },
                                    );
                                }
                            }
                            Err(e) => warn!(error = %e, "Failed to add torrent from cache"),
                        }
                    }
                }
            }
        }

        Ok((
            Self {
                session,
                download_dir,
                deferred_selections,
                pinned_files,
                reported_errors,
                stream_positions,
                swarm_scraper,
            },
            restored_handles,
        ))
    }

    /// Hermetic constructor for tests: no listen port, no DHT, no persistence,
    /// no UPnP — never binds the production 42000-42010 range or touches the
    /// network. Not compiled into release builds.
    #[cfg(test)]
    pub async fn new_for_tests(download_dir: PathBuf) -> Result<Self> {
        tokio::fs::create_dir_all(&download_dir).await?;
        let session_opts = librqbit::SessionOptions {
            // dht: None disables DHT and its persistence together; listen: None
            // never binds a port; persistence: None keeps the session hermetic.
            dht: None,
            listen: None,
            persistence: None,
            ..Default::default()
        };
        let session = Session::new_with_opts(download_dir.clone(), session_opts).await?;
        Ok(Self {
            session,
            download_dir,
            deferred_selections: Default::default(),
            pinned_files: Default::default(),
            stream_positions: Default::default(),
            reported_errors: Default::default(),
            swarm_scraper: SwarmScraper::disabled(),
        })
    }
}

/// Pure helper mapping a file's length and chunk-tracker have-bytes to the
/// (downloaded, progress) pair reported in /stats.json. Clamps have > len
/// (last-piece rounding in the chunk tracker) and treats zero-length files as
/// fully downloaded with progress 1.0.
fn file_progress_fields(len: u64, have: u64) -> (u64, f64) {
    let downloaded = have.min(len);
    let progress = if len == 0 {
        1.0
    } else {
        downloaded as f64 / len as f64
    };
    (downloaded, progress)
}

/// A file-selection operation on a multi-file torrent, mapped by
/// `plan_only_files` onto librqbit's `only_files` want-set.
#[derive(Debug, Clone, Copy)]
enum SelectionOp {
    /// Exclusive selection of one file for streaming (plus the pinned set).
    Prepare(usize),
    /// Deselect one file, keeping the rest of the selection (a pinned file
    /// is never deselected).
    Clear(usize),
    /// Want exactly the union of the active and hot files (plus the pinned
    /// set).
    Reconcile {
        active: Option<usize>,
        hot: Option<usize>,
    },
    /// Add one file to whatever is currently selected (plus the pinned set)
    /// without disturbing the playback selection.
    Pin(usize),
}

/// Pure planner mapping the current `only_files` selection, the pinned set
/// and an operation to the new selection to apply. `None` means "apply
/// nothing".
///
/// Invariants enforced here (unit-tested):
/// - Single-file torrents are always fully wanted: never touch selection.
/// - The result is never an empty set (that would make nothing wanted and
///   starve playback).
/// - `Clear` of a file that is not currently selected (a newer `Prepare`
///   already switched away) is a no-op, so late delayed-cleanup and HLS-lease
///   expiry cannot clobber the active selection.
/// - Every in-range pinned index is in every result, and `Clear` of a pinned
///   index is a no-op: playback switching never deselects an offline
///   download.
/// - Out-of-range indices are dropped; a plan left empty by that is a no-op.
fn plan_only_files(
    current: Option<&[usize]>,
    file_count: usize,
    pinned: &BTreeSet<usize>,
    op: SelectionOp,
) -> Option<HashSet<usize>> {
    if file_count <= 1 {
        return None;
    }
    let in_range = |i: &usize| *i < file_count;
    let with_pinned = |set: HashSet<usize>| -> Option<HashSet<usize>> {
        let set: HashSet<usize> = set
            .into_iter()
            .chain(pinned.iter().copied())
            .filter(in_range)
            .collect();
        if set.is_empty() { None } else { Some(set) }
    };
    match op {
        SelectionOp::Prepare(idx) => {
            if idx >= file_count {
                return None;
            }
            with_pinned(std::iter::once(idx).collect())
        }
        SelectionOp::Clear(idx) => {
            let current = current?;
            if !current.contains(&idx) || pinned.contains(&idx) {
                return None;
            }
            let remainder: HashSet<usize> = current.iter().copied().filter(|i| *i != idx).collect();
            if remainder.is_empty() {
                None
            } else {
                with_pinned(remainder)
            }
        }
        SelectionOp::Reconcile { active, hot } => {
            with_pinned(active.into_iter().chain(hot).collect())
        }
        SelectionOp::Pin(idx) => {
            if idx >= file_count {
                return None;
            }
            // `current == None` is librqbit for "everything wanted": pinning
            // narrows that to the pinned set, which is the point of an
            // offline download (fetch this file, not the whole torrent).
            let mut set: HashSet<usize> = current.unwrap_or_default().iter().copied().collect();
            set.insert(idx);
            with_pinned(set)
        }
    }
}

pub struct LibrqbitHandle {
    pub handle: Arc<ManagedTorrent>,
    pub info_hash: String,
    /// Kept so the handle can apply per-file selection via
    /// `Session::update_only_files` (librqbit persists file selection on the
    /// session, not the torrent handle).
    session: Arc<Session>,
    /// Backend-wide deferred-selection slots (see `DeferredSelection`).
    deferred_selections: DeferredSelections,
    /// Backend-wide pinned file sets (see `PinnedFiles`).
    pinned_files: PinnedFiles,
    /// Backend-wide last-reported error texts (see [`ReportedErrors`]).
    reported_errors: ReportedErrors,
    /// Backend-wide reader positions (see [`StreamPositions`]).
    stream_positions: StreamPositions,
    /// Backend-wide swarm-scrape cache (see [`SwarmScraper`]).
    swarm_scraper: Arc<SwarmScraper>,
}

/// Put `trackers` into a magnet link as `tr=` params.
///
/// librqbit's `Session::add_torrent` takes a magnet's trackers from the magnet
/// URL's own `tr=` params only; `AddTorrentOptions::trackers` is merged in the
/// torrent-file branch alone (session.rs: the `AddTorrent::Url` magnet arm
/// builds `InternalAddResult.trackers` from `Magnet::trackers`, the `other`
/// arm extends the metainfo's announce list with `opts.trackers`). So the
/// merged tracker list has to travel inside the URL, or a magnet add reaches
/// librqbit tracker-less (DHT-only) and `stats().sources` comes back empty.
///
/// Appends one percent-encoded `tr=` per tracker not already in the URL
/// (`Magnet::parse` collects every `tr` via `Url::query_pairs`, which
/// decodes them again). A bare 40-hex info hash, which librqbit also accepts,
/// becomes a full magnet link first. Anything else is returned unchanged.
pub fn magnet_with_trackers(url: &str, trackers: &[String]) -> String {
    let magnet = if url.len() == 40 && url.bytes().all(|b| b.is_ascii_hexdigit()) {
        format!("magnet:?xt=urn:btih:{url}")
    } else {
        url.to_string()
    };
    if !magnet.starts_with("magnet:") {
        return magnet;
    }
    let Ok(mut parsed) = url::Url::parse(&magnet) else {
        return magnet;
    };
    let existing: Vec<String> = parsed
        .query_pairs()
        .filter(|(key, _)| key == "tr")
        .map(|(_, value)| value.into_owned())
        .collect();
    let mut pairs = parsed.query_pairs_mut();
    for tracker in trackers {
        if !existing.contains(tracker) {
            pairs.append_pair("tr", tracker);
        }
    }
    drop(pairs);
    parsed.to_string()
}

/// Move a file, falling back to copy + remove when `rename` cannot cross
/// the device boundary (`EXDEV`; `ERROR_NOT_SAME_DEVICE` on Windows -- both
/// `ErrorKind::CrossesDevices`). A failed copy removes its partial target.
pub(crate) async fn move_file(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    match tokio::fs::rename(src, dst).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::CrossesDevices => {
            copy_then_remove(src, dst).await
        }
        Err(e) => Err(e),
    }
}

/// Whether a file of a torrent being relocated is worth moving: it has
/// verified bytes per the chunk tracker (`have`, known once the torrent
/// has initialized), or -- `have` unknown, the torrent still Initializing
/// -- it has blocks allocated on disk, which a pre-sized sparse placeholder
/// has not (on platforms without block counts every existing file moves).
fn has_data_to_move(have: Option<u64>, metadata: &std::fs::Metadata) -> bool {
    match have {
        Some(have) => have > 0,
        None => has_allocated_blocks(metadata),
    }
}

#[cfg(unix)]
fn has_allocated_blocks(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    metadata.blocks() > 0
}

#[cfg(not(unix))]
fn has_allocated_blocks(_metadata: &std::fs::Metadata) -> bool {
    true
}

async fn copy_then_remove(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    if let Err(e) = tokio::fs::copy(src, dst).await {
        let _ = tokio::fs::remove_file(dst).await;
        return Err(e);
    }
    tokio::fs::remove_file(src).await
}

impl LibrqbitBackend {
    fn wrap(&self, handle: Arc<ManagedTorrent>) -> LibrqbitHandle {
        let info_hash = handle.info_hash().as_string();
        LibrqbitHandle {
            handle,
            info_hash,
            session: self.session.clone(),
            deferred_selections: self.deferred_selections.clone(),
            pinned_files: self.pinned_files.clone(),
            reported_errors: self.reported_errors.clone(),
            stream_positions: self.stream_positions.clone(),
            swarm_scraper: self.swarm_scraper.clone(),
        }
    }

    /// Shared body of `remove_torrent` (`delete_files: false`, keeping the
    /// downloaded data on disk like the libtorrent backend's
    /// `remove_torrent(handle, false)` did) and `remove_torrent_and_files`.
    async fn delete_torrent(&self, info_hash: &str, delete_files: bool) -> Result<()> {
        let id = librqbit::api::TorrentIdOrHash::parse(info_hash)
            .with_context(|| format!("invalid info hash {info_hash}"))?;
        let output_folder = self
            .session
            .get(id)
            .map(|handle| handle.output_folder().to_path_buf());
        self.session
            .delete(id, delete_files)
            .await
            .with_context(|| format!("failed to remove torrent {info_hash}"))?;
        self.deferred_selections.lock().remove(info_hash);
        self.pinned_files.lock().remove(info_hash);
        self.reported_errors.lock().remove(info_hash);
        // `Session::delete(_, false)` only removes empty directories on the
        // delete_files=true branch, so a torrent that never wrote anything
        // (or whose files were cleaned out) would leave its output folder
        // behind on every idle sweep. `remove_dir` fails on a non-empty
        // directory, so this can only ever drop an empty folder -- and never
        // the session root, which single-file torrents write straight into.
        if let Some(folder) = output_folder
            && folder != self.download_dir
            && let Err(e) = tokio::fs::remove_dir(&folder).await
            && !matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
            )
        {
            debug!(error = %e, path = ?folder, "Left the torrent's output folder in place");
        }
        // Best-effort: drop the cached .torrent file so the restore path in
        // `new()` does not resurrect the torrent on the next startup.
        let cached = self
            .download_dir
            .join(".cache")
            .join(format!("{info_hash}.torrent"));
        if let Err(e) = tokio::fs::remove_file(&cached).await
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!(error = %e, path = ?cached, "Failed to remove cached torrent file");
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl TorrentBackend for LibrqbitBackend {
    type Handle = LibrqbitHandle;

    async fn add_torrent(
        &self,
        source: TorrentSource,
        trackers: Vec<String>,
    ) -> Result<Self::Handle> {
        self.add_torrent_placed(source, trackers, TorrentPlacement::default())
            .await
    }

    /// `placement.output_folder` becomes librqbit's per-torrent
    /// `output_folder` (persisted with the torrent, so a restart restores
    /// the place) and `placement.only_files` its initial want-set; librqbit
    /// rejects an out-of-range index at add time. `overwrite: true` always:
    /// resuming on top of existing files is the normal case here (a restart,
    /// a relocated torrent).
    async fn add_torrent_placed(
        &self,
        source: TorrentSource,
        trackers: Vec<String>,
        placement: TorrentPlacement,
    ) -> Result<Self::Handle> {
        let add_torrent = match source {
            // See `magnet_with_trackers`: for a magnet only the URL's own
            // `tr=` params count; `opts.trackers` below covers .torrent adds.
            TorrentSource::Url(url) => {
                librqbit::AddTorrent::Url(magnet_with_trackers(&url, &trackers).into())
            }
            TorrentSource::Bytes(bytes) => {
                librqbit::AddTorrent::from_bytes(bytes::Bytes::from(bytes))
            }
        };
        let response = self
            .session
            .add_torrent(
                add_torrent,
                Some(librqbit::AddTorrentOptions {
                    overwrite: true,
                    trackers: Some(trackers),
                    output_folder: placement
                        .output_folder
                        .map(|folder| folder.to_string_lossy().into_owned()),
                    only_files: placement.only_files,
                    ..Default::default()
                }),
            )
            .await
            .context("Failed to add torrent to librqbit")?;

        let (_id, handle) = match response {
            librqbit::AddTorrentResponse::Added(id, handle)
            | librqbit::AddTorrentResponse::AlreadyManaged(id, handle) => (id, handle),
            _ => return Err(anyhow::anyhow!("Unexpected response from librqbit")),
        };

        let info_hash = handle.info_hash().as_string();
        Ok(LibrqbitHandle {
            handle,
            info_hash,
            session: self.session.clone(),
            deferred_selections: self.deferred_selections.clone(),
            pinned_files: self.pinned_files.clone(),
            reported_errors: self.reported_errors.clone(),
            stream_positions: self.stream_positions.clone(),
            swarm_scraper: self.swarm_scraper.clone(),
        })
    }

    /// librqbit has no relocate call, so: drop the torrent from the session
    /// keeping its files (`Session::delete(_, false)` also drops its
    /// persisted record and `.bitv` bitfield), move every file that holds
    /// verified data from the old output folder into the new one (rename,
    /// copy + remove across devices), then re-add it from its own metainfo
    /// bytes with the placement and `overwrite: true` -- librqbit
    /// hash-checks the moved data (`checking` phase), so nothing verified is
    /// lost. Files without any verified bytes are not moved but deleted
    /// with the old folder: librqbit pre-sizes every wanted file when a
    /// torrent goes live (all of them for a plain magnet add), so a streamed
    /// season pack has a full-length sparse placeholder per episode, and a
    /// cross-device copy would write every one of them out as zeros. The
    /// re-added torrent pre-sizes what it wants in the new folder itself.
    /// If the move or the re-add fails the torrent is re-added where it was
    /// (best effort) and the error returned. The deferred-selection slot
    /// belonged to the old torrent and is dropped; the pin set stays.
    async fn relocate_torrent(
        &self,
        info_hash: &str,
        placement: TorrentPlacement,
        trackers: Vec<String>,
    ) -> Result<Self::Handle> {
        let id = librqbit::api::TorrentIdOrHash::parse(info_hash)
            .with_context(|| format!("invalid info hash {info_hash}"))?;
        let handle = self
            .session
            .get(id)
            .with_context(|| format!("torrent {info_hash} is not managed"))?;
        let metadata = handle
            .metadata
            .load_full()
            .with_context(|| format!("torrent {info_hash} has no metadata yet"))?;
        let target = placement
            .output_folder
            .clone()
            .context("relocation needs an output folder")?;
        let old_folder = handle.output_folder().to_path_buf();
        let old_only_files = handle.only_files();
        if old_folder == target {
            return Ok(self.wrap(handle));
        }
        // Per-file verified bytes, snapshotted while the torrent still has a
        // chunk tracker (empty while it is Initializing -- then the file's
        // own allocation decides, see `has_data_to_move`).
        let file_progress = handle.stats().file_progress;
        self.session
            .delete(id, false)
            .await
            .with_context(|| format!("failed to drop torrent {info_hash} before relocating"))?;
        self.deferred_selections.lock().remove(info_hash);

        let relocated = async {
            tokio::fs::create_dir_all(&target)
                .await
                .with_context(|| format!("creating {}", target.display()))?;
            for (idx, file) in metadata.file_infos.iter().enumerate() {
                let src = old_folder.join(&file.relative_filename);
                let Ok(src_metadata) = tokio::fs::metadata(&src).await else {
                    continue;
                };
                if !has_data_to_move(file_progress.get(idx).copied(), &src_metadata) {
                    debug!(
                        src = %src.display(),
                        "no verified data; dropping the placeholder instead of moving it"
                    );
                    tokio::fs::remove_file(&src)
                        .await
                        .with_context(|| format!("removing {}", src.display()))?;
                    continue;
                }
                let dst = target.join(&file.relative_filename);
                if tokio::fs::try_exists(&dst).await.unwrap_or(false) {
                    // Data already in the destination (downloaded there
                    // before) wins over the source: librqbit pre-sizes
                    // every wanted file in the old folder, so the source
                    // is often a sparse placeholder that would wipe
                    // verified bytes. The re-check sorts out what the
                    // destination actually has.
                    debug!(
                        src = %src.display(),
                        dst = %dst.display(),
                        "destination file exists; keeping it and dropping the source"
                    );
                    tokio::fs::remove_file(&src)
                        .await
                        .with_context(|| format!("removing {}", src.display()))?;
                    continue;
                }
                if let Some(parent) = dst.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                move_file(&src, &dst)
                    .await
                    .with_context(|| format!("moving {} to {}", src.display(), dst.display()))?;
            }
            // The old folder is a per-torrent one only for multi-file
            // torrents (single-file ones write into the session root, which
            // stays); drop it once empty, like `remove_torrent` does.
            if old_folder != self.download_dir
                && let Err(e) = tokio::fs::remove_dir(&old_folder).await
                && !matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
                )
            {
                debug!(error = %e, path = ?old_folder, "Left the old output folder in place");
            }
            self.add_torrent_placed(
                TorrentSource::Bytes(metadata.torrent_bytes.to_vec()),
                trackers.clone(),
                placement,
            )
            .await
        }
        .await;
        match relocated {
            Ok(handle) => Ok(handle),
            Err(error) => {
                warn!(
                    info_hash,
                    error = %format!("{error:#}"),
                    "relocation failed; re-adding the torrent where it was"
                );
                if let Err(e) = self
                    .add_torrent_placed(
                        TorrentSource::Bytes(metadata.torrent_bytes.to_vec()),
                        trackers,
                        TorrentPlacement {
                            output_folder: Some(old_folder),
                            only_files: old_only_files,
                        },
                    )
                    .await
                {
                    warn!(info_hash, error = %format!("{e:#}"), "re-adding the torrent failed too");
                }
                Err(error)
            }
        }
    }

    async fn get_torrent(&self, info_hash: &str) -> Option<Self::Handle> {
        let id = librqbit::api::TorrentIdOrHash::parse(info_hash).ok()?;
        let handle = self.session.get(id)?;
        let info_hash = handle.info_hash().as_string();
        Some(LibrqbitHandle {
            handle,
            info_hash,
            session: self.session.clone(),
            deferred_selections: self.deferred_selections.clone(),
            pinned_files: self.pinned_files.clone(),
            reported_errors: self.reported_errors.clone(),
            stream_positions: self.stream_positions.clone(),
            swarm_scraper: self.swarm_scraper.clone(),
        })
    }

    async fn remove_torrent(&self, info_hash: &str) -> Result<()> {
        self.delete_torrent(info_hash, false).await
    }

    /// `Session::delete(_, true)`: librqbit removes the files and, for a
    /// torrent with its own output folder, that folder once empty.
    async fn remove_torrent_and_files(&self, info_hash: &str) -> Result<()> {
        self.delete_torrent(info_hash, true).await
    }

    async fn list_torrents(&self) -> Vec<String> {
        self.session.with_torrents(|iter| {
            iter.map(|(_id, handle)| handle.info_hash().as_string())
                .collect()
        })
    }

    async fn memory_diagnostics(&self) -> BackendMemoryDiagnostics {
        BackendMemoryDiagnostics::default()
    }
}

#[async_trait::async_trait]
impl TorrentHandle for LibrqbitHandle {
    fn info_hash(&self) -> String {
        self.handle.info_hash().as_string()
    }

    fn name(&self) -> Option<String> {
        self.handle
            .metadata
            .load_full()
            .and_then(|m| m.info.name().map(|n| n.to_string()))
    }

    async fn stats(&self) -> EngineStats {
        let stats = self.handle.stats();
        // CAUTION: librqbit's Speed.mbps is MiB/s, NOT megabits/s
        // (librqbit-core speed_estimator.rs: mbps() = bps()/1024/1024), so the
        // conversion to bytes/s is a plain * 1 MiB with no /8.
        let (download_speed, upload_speed) = if let Some(ref live) = stats.live {
            (
                live.download_speed.mbps * 1_048_576.0,
                live.upload_speed.mbps * 1_048_576.0,
            )
        } else {
            (0.0, 0.0)
        };

        // progress_bytes is persisted have-bytes (survives restarts), matching
        // libtorrent's total_done semantics; fetched_bytes resets each session.
        let downloaded = stats.progress_bytes;
        let uploaded = stats.uploaded_bytes;

        let peer_discovery = stats
            .live
            .as_ref()
            .map(|l| PeerDiscovery {
                seen: l.snapshot.peer_stats.seen as u64,
                queued: l.snapshot.peer_stats.queued as u64,
                connecting: l.snapshot.peer_stats.connecting as u64,
                live: l.snapshot.peer_stats.live as u64,
            })
            .unwrap_or_default();
        let (peers, queued, unique) = (
            peer_discovery.live,
            peer_discovery.queued,
            peer_discovery.seen,
        );
        // Connected peers whose bitfield covers the whole torrent. librqbit
        // maintains this as a transition counter next to the live/live_tcp/
        // live_utp/live_socks aggregates, so reading it is O(1) and needs no
        // walk over per-peer bitfields. It is bounded by `peers`: only live
        // peers are counted, never ones we merely know an address for, and
        // never a tracker's seeder count (we do not scrape).
        let connected_seeders = stats
            .live
            .as_ref()
            .map(|l| l.snapshot.peer_stats.live_seeders as u64)
            .unwrap_or(0);

        let has_metadata = self.handle.metadata.load().is_some();
        let phase = startup_phase(has_metadata, &stats.state, stats.finished);
        // Hash-check progress straight from the Initializing state (the same
        // counter librqbit mirrors into `progress_bytes` while initializing).
        let checked_bytes = match phase {
            StartupPhase::Checking => self.handle.with_state(|state| match state {
                ManagedTorrentState::Initializing(init) => Some(init.get_checked_bytes()),
                _ => None,
            }),
            _ => None,
        };
        let check_total_bytes = checked_bytes.map(|_| stats.total_bytes);
        // The verified-piece bitfield only exists once the chunk tracker does
        // (Paused/Live); `api_dump_haves` is the one public accessor for it.
        let haves = match phase {
            StartupPhase::Buffering | StartupPhase::Ready => {
                librqbit::Api::new(self.session.clone(), None)
                    .api_dump_haves(librqbit::api::TorrentIdOrHash::Hash(
                        self.handle.info_hash(),
                    ))
                    .ok()
                    .map(|(bitfield, _pieces)| bitfield)
            }
            _ => None,
        };
        // The startup window is the same for every buffer profile (see
        // `BufferProfile`), so the reported readiness is too.
        let startup_window = crate::backend::priorities::librqbit_stream_lookahead_bytes(
            crate::backend::priorities::PlaybackIntent::DirectInitial,
            crate::backend::priorities::BufferProfile::Normal,
        );

        let pinned = self.pinned_set();
        let positions = self.stream_positions.lock().clone();
        let mut torrent_piece_length = None;
        let mut files = Vec::new();
        let mut total_size = 0u64;
        let mut offset = 0u64;
        if let Some(m) = self.handle.metadata.load_full() {
            let piece_length = m.lengths().default_piece_length() as u64;
            torrent_piece_length = Some(piece_length);
            for (i, f) in m.info.iter_file_details().enumerate() {
                let filename = f.filename.to_string();
                // file_progress is empty while the torrent is Initializing.
                let have = stats.file_progress.get(i).copied().unwrap_or(0);
                let (file_downloaded, file_progress) = file_progress_fields(f.len, have);
                // Complete = every byte verified; `have` is clamped to the
                // length above, so equality is the whole test. A torrent
                // without a piece map yet (empty file_progress) reports 0
                // and so never claims completion of a non-empty file.
                let complete = file_downloaded == f.len;
                let read_from = positions.get(&(self.info_hash.clone(), i)).copied();
                let window = haves.as_ref().map(|bf| {
                    crate::backend::priorities::initial_window_progress(
                        offset,
                        f.len,
                        piece_length,
                        startup_window,
                        read_from.unwrap_or(0),
                        |piece| bf.get(piece as usize).is_some_and(|bit| *bit),
                    )
                });
                files.push(StatsFile {
                    name: filename.clone(),
                    path: filename,
                    length: f.len,
                    offset,
                    downloaded: file_downloaded,
                    progress: file_progress,
                    initial_window_ready_bytes: window.map(|(ready, _)| ready),
                    initial_window_bytes: window.map(|(_, total)| total),
                    pinned: pinned.contains(&i),
                    complete,
                });
                total_size += f.len;
                offset += f.len;
            }
        }

        // server.js lists the torrent's peer sources here; we report the
        // tracker set the torrent was added with (fixed for its lifetime, see
        // `add_trackers`) so clients can verify which trackers reached the
        // engine. librqbit exposes no per-tracker announce bookkeeping, so the
        // counters stay 0 and `lastStarted` empty.
        let mut sources: Vec<Source> = self
            .handle
            .shared()
            .trackers
            .iter()
            .map(|url| Source {
                url: url.to_string(),
                ..Source::default()
            })
            .collect();
        sources.sort_by(|a, b| a.url.cmp(&b.url));

        // Swarm-wide counts, which is a different question from
        // `connected_seeders`: what the trackers say about everybody, not
        // what our own connections show. Cached and rate-limited by the
        // scraper -- this call never waits on the network, because players
        // poll stats.json about once a second.
        //
        // A torrent whose metadata has not arrived cannot be shown to be
        // public, so it counts as private and is left alone: an unsolicited
        // scrape is exactly what private trackers ban accounts over.
        let private = self
            .handle
            .with_metadata(|m| m.info.info().private)
            .unwrap_or(true);
        let tracker_urls: Vec<String> = sources.iter().map(|s| s.url.clone()).collect();
        let swarm = self.swarm_scraper.snapshot(
            &self.info_hash,
            self.handle.info_hash().0,
            &tracker_urls,
            private,
        );
        for source in &mut sources {
            if let Some(counts) = swarm.per_tracker.get(&source.url) {
                source.seeders = Some(counts.seeders);
                source.leechers = Some(counts.leechers);
                source.completed = Some(counts.completed);
            }
        }

        EngineStats {
            name: self.name().unwrap_or_else(|| "Unknown".to_string()),
            info_hash: self.info_hash(),
            piece_length: torrent_piece_length,
            files,
            sources,
            opts: StatsOptions {
                dht: true,
                tracker: true,
                path: "".to_string(),
                growler: Growler {
                    flood: 0,
                    pulse: None,
                },
                peer_search: PeerSearch {
                    max: 100,
                    min: 10,
                    sources: vec![],
                },
                swarm_cap: SwarmCap {
                    max_speed: None,
                    min_peers: None,
                },
                connections: None,
                handshake_timeout: None,
                timeout: None,
                r#virtual: false,
            },
            download_speed,
            upload_speed,
            downloaded,
            uploaded,
            peers,
            unchoked: peers,
            queued,
            unique,
            connection_tries: 0,
            peer_search_running: true,
            stream_len: total_size,
            stream_name: "".to_string(),
            stream_progress: if stats.total_bytes > 0 {
                stats.progress_bytes as f64 / stats.total_bytes as f64
            } else {
                0.0
            },
            swarm_connections: peers,
            swarm_paused: false,
            swarm_size: peers,
            connected_seeders,
            swarm_seeders: swarm.seeders,
            swarm_leechers: swarm.leechers,
            swarm_scrape_age_secs: swarm.age_secs,
            is_finished: stats.finished,
            has_metadata,
            phase,
            checked_bytes,
            check_total_bytes,
            initial_window_ready_bytes: None,
            initial_window_bytes: None,
            peer_discovery,
            error: self.client_torrent_error(stats.error.as_deref()),
            pinned_files: pinned.into_iter().collect(),
        }
    }

    /// Cheap: librqbit's TorrentStats.finished is precomputed from the chunk
    /// tracker's have/needed counters (no per-piece walk here).
    async fn is_finished(&self) -> bool {
        self.handle.stats().finished
    }

    /// Per-file completion from chunk-tracker have-bytes. `file_progress` is
    /// empty while the torrent is still Initializing, in which case the file
    /// is reported incomplete.
    async fn is_file_complete(&self, file_idx: usize) -> bool {
        let stats = self.handle.stats();
        let Some(have) = stats.file_progress.get(file_idx).copied() else {
            return false;
        };
        let Some(len) = self
            .handle
            .metadata
            .load_full()
            .and_then(|m| m.file_infos.get(file_idx).map(|fi| fi.len))
        else {
            return false;
        };
        have >= len
    }

    /// Deliberate no-op: librqbit (zond/rqbit `feat/configurable-stream-lookahead`)
    /// has no API to add trackers to a torrent that is already managed. The
    /// tracker set lives in `ManagedTorrentShared::trackers`, a plain
    /// `HashSet<Url>` with no interior mutability, and `Session::make_peer_rx`
    /// hands `TrackerComms::start` a one-shot snapshot of it when the torrent
    /// goes live; `TrackerComms::add_tracker` is private startup plumbing. The
    /// only way to change a torrent's trackers is to remove and re-add it,
    /// which would drop its peers and piece state mid-stream. So trackers must
    /// be supplied to `add_torrent` by whichever request creates the engine
    /// (see `routes::compat::get_or_create_engine` in the server crate), and
    /// `stats().sources` reports the set that was actually used.
    async fn add_trackers(&self, _trackers: Vec<String>) -> Result<()> {
        Ok(())
    }

    async fn get_file_reader(
        &self,
        file_idx: usize,
        start_offset: u64,
        _priority: u8,
        _bitrate: Option<u64>,
        intent: crate::backend::priorities::PlaybackIntent,
        buffer: crate::backend::priorities::BufferProfile,
    ) -> Result<Box<dyn FileStreamTrait>> {
        // Where the startup window is measured from now on. A `Range`
        // request, a seek and a re-open all reach the backend as a fresh
        // reader at the new offset, so this is the whole of "follow the
        // reader" (see [`StreamPositions`]).
        self.stream_positions
            .lock()
            .insert((self.info_hash.clone(), file_idx), start_offset);
        // librqbit's FileStream requires the Paused or Live state; opening it
        // while the torrent is still Initializing fails immediately, which the
        // HTTP route would turn into a failed first play. Block here instead.
        self.await_initialized().await?;
        // Size the per-stream lookahead window by playback intent instead of
        // librqbit's fixed 32 MiB default: a narrow startup window verifies the
        // head pieces faster, while seeks/sequential get generous read-ahead.
        let opts = librqbit::FileStreamOptions {
            lookahead_bytes: crate::backend::priorities::librqbit_stream_lookahead_bytes(
                intent, buffer,
            ),
        };
        let stream = self
            .handle
            .clone()
            .stream_with_options(file_idx, opts)
            .await
            .context("Failed to stream from librqbit")?;
        Ok(Box::new(stream))
    }

    async fn get_files(&self) -> Vec<BackendFileInfo> {
        let mut files = Vec::new();
        if let Some(m) = self.handle.metadata.load_full() {
            for f in m.info.iter_file_details() {
                files.push(BackendFileInfo {
                    name: f.filename.to_string(),
                    length: f.len,
                });
            }
        }
        files
    }

    /// The torrent's resolved output folder (`ManagedTorrent::output_folder`,
    /// public since librqbit 9) joined with the file's relative name from
    /// the metadata's `file_infos` -- exactly where librqbit's storage
    /// writes it. `None` while a magnet is still resolving or for a bad
    /// index. This is for handing a *complete* file to a local player;
    /// reads of an in-progress file keep going through the FileStream,
    /// which blocks on missing pieces where a sparse file would not.
    async fn file_path(&self, file_idx: usize) -> Option<PathBuf> {
        let metadata = self.handle.metadata.load_full()?;
        let file = metadata.file_infos.get(file_idx)?;
        Some(self.handle.output_folder().join(&file.relative_filename))
    }

    /// librqbit's resolved `output_folder` for the torrent: the placement's
    /// folder when one was given, else the session root (single-file
    /// torrents) or `<root>/<torrent name>` (multi-file).
    fn output_folder(&self) -> Option<PathBuf> {
        Some(self.handle.output_folder().to_path_buf())
    }

    /// Select `file_idx` as the only wanted file (exclusive downloading, per
    /// the trait contract) on multi-file torrents. Blocks (bounded) while the
    /// torrent is still Initializing -- librqbit refuses selection updates in
    /// that state, and this runs right before the reader is opened, which has
    /// to wait anyway. Other selection failures are best-effort: logged and
    /// swallowed, since playback still works with the selection unchanged.
    /// Err is returned for a provably-bad file index or when the torrent never
    /// becomes ready (`TorrentInitError`).
    ///
    /// librqbit persists only_files across restarts; the next prepare or
    /// reconcile simply rewrites it.
    async fn prepare_file_for_streaming(&self, file_idx: usize) -> Result<()> {
        let Some(file_count) = self.file_count_from_metadata() else {
            warn!(
                info_hash = %self.info_hash,
                file_idx,
                "prepare_file_for_streaming: metadata not resolved; skipping file gating"
            );
            return Ok(());
        };
        if file_idx >= file_count {
            anyhow::bail!("File index {file_idx} out of range ({file_count} files)");
        }
        self.apply_selection(
            SelectionOp::Prepare(file_idx),
            file_count,
            "prepare_file_for_streaming",
            InitPolicy::Wait,
        )
        .await
    }

    // keep_file_downloading stays the default-equivalent no-op: its only call
    // site (lib.rs activate_file) is guarded by !is_multifile, where the whole
    // torrent is a single file and therefore always wanted.
    async fn keep_file_downloading(&self, _file_idx: usize) -> Result<()> {
        Ok(())
    }

    /// Deselect `file_idx`, keeping the rest of the current selection. Called
    /// from delayed cleanup and HLS-lease expiry, possibly AFTER a newer file
    /// was prepared -- the planner refuses to clear a file that is no longer
    /// selected and refuses to produce an empty want-set, so stale cleanups
    /// can never clobber the active selection.
    async fn clear_file_streaming(&self, file_idx: usize) -> Result<()> {
        let Some(file_count) = self.file_count_from_metadata() else {
            return Ok(());
        };
        self.apply_selection(
            SelectionOp::Clear(file_idx),
            file_count,
            "clear_file_streaming",
            InitPolicy::Skip,
        )
        .await
    }

    /// Pin `file_idx` (see the trait doc): record it in the backend-wide pin
    /// set, which every later planner run unions in, and add it to the
    /// current selection right away. Deferred like `reconcile_file_priorities`
    /// while the torrent is Initializing -- the pin is already recorded, so a
    /// prepare/reconcile that lands first carries it anyway. Err only for a
    /// provably-bad index; other selection failures are best-effort.
    async fn pin_file(&self, file_idx: usize) -> Result<()> {
        let file_count = self.file_count_from_metadata();
        if let Some(file_count) = file_count
            && file_idx >= file_count
        {
            anyhow::bail!("File index {file_idx} out of range ({file_count} files)");
        }
        self.pinned_files
            .lock()
            .entry(self.info_hash.clone())
            .or_default()
            .insert(file_idx);
        let Some(file_count) = file_count else {
            // Metadata still resolving: the pin is recorded and the first
            // selection update after resolution applies it.
            return Ok(());
        };
        self.apply_selection(
            SelectionOp::Pin(file_idx),
            file_count,
            "pin_file",
            InitPolicy::Defer,
        )
        .await
    }

    /// Forget the pin (see the trait doc). The selection is left alone: the
    /// file may be the one currently streaming, and only the engine layer
    /// knows what should stay wanted -- it reconciles right after.
    async fn unpin_file(&self, file_idx: usize) -> Result<()> {
        let mut pinned = self.pinned_files.lock();
        if let Some(set) = pinned.get_mut(&self.info_hash) {
            set.remove(&file_idx);
            if set.is_empty() {
                pinned.remove(&self.info_hash);
            }
        }
        Ok(())
    }

    /// The engine's primary multi-file switching hook
    /// (reconcile_multifile_engine in lib.rs): want exactly the union of the
    /// active and hot files. While the torrent is Initializing the update is
    /// deferred (latest wins) and applied as soon as librqbit accepts it, so
    /// first-play gating takes effect instead of being silently dropped; the
    /// call itself returns immediately because it is also driven from
    /// background cleanup loops.
    async fn reconcile_file_priorities(&self, plan: TorrentFilePriorityPlan) -> Result<()> {
        let Some(file_count) = self.file_count_from_metadata() else {
            return Ok(());
        };
        self.apply_selection(
            SelectionOp::Reconcile {
                active: plan.active_file,
                hot: plan.hot_file.map(|h| h.file_idx),
            },
            file_count,
            "reconcile_file_priorities",
            InitPolicy::Defer,
        )
        .await
    }

    /// Block until the piece covering `offset` of `file_idx` is readable, or
    /// the timeout elapses. Mirrors the libtorrent behavioral contract:
    /// timeouts and soft conditions return Ok(ready: false, reason), Err is
    /// reserved for structural failures (bad file index).
    ///
    /// Mechanism: open a short-lived librqbit FileStream and seek to `offset`.
    /// Registering the stream moves librqbit's per-stream lookahead window
    /// (sized by `intent` and `buffer` via `librqbit_stream_lookahead_bytes`,
    /// matching the
    /// window the real read will request) to that offset and reconnects
    /// not-needed peers --
    /// the deadline-equivalent priority yank -- and the subsequent 1-byte read
    /// parks on the piece waker until the piece covering `offset` verifies.
    /// The temporary stream drops at function exit, deregistering its window.
    async fn wait_for_piece_ready(
        &self,
        file_idx: usize,
        offset: u64,
        timeout: Duration,
        intent: crate::backend::priorities::PlaybackIntent,
        buffer: crate::backend::priorities::BufferProfile,
    ) -> Result<PieceReadiness> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let start = std::time::Instant::now();
        debug!(
            info_hash = %self.info_hash,
            file_idx,
            offset,
            timeout_ms = timeout.as_millis() as u64,
            ?intent,
            "wait_for_piece_ready: begin"
        );

        // Phase 1: wait for the torrent to leave Initializing (FileStream
        // requires Paused or Live). Bounded by the caller's timeout and the
        // global gate; a soft failure keeps the libtorrent-style contract.
        if let Err(e) = await_initialized(
            &self.info_hash,
            timeout.min(TORRENT_INIT_TIMEOUT),
            self.handle.wait_until_initialized(),
        )
        .await
        {
            let reason = match e {
                TorrentInitError::TimedOut { .. } => "initializing-timeout".to_string(),
                TorrentInitError::Failed { reason, .. } => format!("init-failed: {reason}"),
            };
            return Ok(self.readiness(start, false, -1, 0, 1, reason));
        }
        // Metadata is resolved before librqbit creates the ManagedTorrent, so
        // this is purely defensive.
        let Some(metadata) = self.handle.metadata.load_full() else {
            return Ok(self.readiness(start, false, -1, 0, 1, "no-metadata".to_string()));
        };

        let fi = metadata
            .file_infos
            .get(file_idx)
            .with_context(|| format!("File index {file_idx} out of range"))?;
        let piece_length = metadata.lengths().default_piece_length() as u64;
        let piece = ((fi.offset_in_torrent + offset) / piece_length) as i32;
        if fi.len > 0 && offset >= fi.len {
            return Ok(self.readiness(
                start,
                false,
                piece,
                0,
                1,
                "piece-out-of-file-range".to_string(),
            ));
        }

        // Phase 2: open the stream. Use the same intent-sized window as the
        // real read so the priority yank moves the lookahead exactly where
        // playback will request it.
        let lookahead = librqbit::FileStreamOptions {
            lookahead_bytes: crate::backend::priorities::librqbit_stream_lookahead_bytes(
                intent, buffer,
            ),
        };
        let mut stream = match self
            .handle
            .clone()
            .stream_with_options(file_idx, lookahead)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                return Ok(self.readiness(
                    start,
                    false,
                    piece,
                    0,
                    1,
                    format!("stream-unavailable: {e:#}"),
                ));
            }
        };

        // Seek completes synchronously for FileStream (poll_complete is
        // always Ready) and also moves the shared per-stream window.
        if let Err(e) = stream.seek(std::io::SeekFrom::Start(offset)).await {
            return Ok(self.readiness(start, false, piece, 0, 1, format!("seek-error: {e}")));
        }

        // Phase 3: 1-byte read bounded by the remaining timeout. A successful
        // read of 0 bytes is EOF at the exact file end, which still means the
        // requested position is servable.
        let remaining = timeout.saturating_sub(start.elapsed());
        let mut buf = [0u8; 1];
        let result = match tokio::time::timeout(remaining, stream.read(&mut buf)).await {
            Ok(Ok(_n)) => self.readiness(start, true, piece, 1, 1, "stream-read".to_string()),
            Ok(Err(e)) => self.readiness(start, false, piece, 0, 1, format!("read-error: {e}")),
            Err(_) => self.readiness(start, false, piece, 0, 1, "timeout".to_string()),
        };
        debug!(
            info_hash = %self.info_hash,
            file_idx,
            offset,
            ready = result.ready,
            reason = %result.reason,
            elapsed_ms = result.elapsed_ms,
            "wait_for_piece_ready: end"
        );
        Ok(result)
    }
}

/// Map librqbit's torrent state onto the client-facing startup phase.
/// `Initializing` is the on-disk hash check; `Live`/`Paused` both have a piece
/// map and are `Ready` only when the whole torrent is finished -- otherwise
/// `Buffering` until [`EngineStats::focus_stream_file`] judges the stream
/// file's initial window. Missing metadata wins over everything (a resolving
/// magnet has no pieces to check or buffer).
fn startup_phase(
    has_metadata: bool,
    state: &librqbit::TorrentStatsState,
    finished: bool,
) -> StartupPhase {
    use librqbit::TorrentStatsState as S;
    if !has_metadata {
        return StartupPhase::ResolvingMetadata;
    }
    match state {
        S::Initializing { .. } => StartupPhase::Checking,
        S::Live | S::Paused if finished => StartupPhase::Ready,
        S::Live | S::Paused => StartupPhase::Buffering,
        S::Error => StartupPhase::Error,
    }
}

impl LibrqbitHandle {
    /// File count from resolved metadata; None while a magnet is resolving.
    fn file_count_from_metadata(&self) -> Option<usize> {
        self.handle.metadata.load_full().map(|m| m.file_infos.len())
    }

    /// What a client may be told about librqbit's `TorrentStats.error`:
    /// the fixed [`TORRENT_ERROR_MESSAGE`], never the error itself, which
    /// is the `{e:?}` of an anyhow chain naming absolute cache and
    /// download paths (`PinDownloadError::client_message` draws the same
    /// line). The chain goes to the log instead -- once per distinct
    /// error, since statistics are polled for as long as a broken download
    /// is on screen. `None` in, `None` out, and the record is dropped so a
    /// torrent that recovers reports its next error again.
    fn client_torrent_error(&self, error: Option<&str>) -> Option<String> {
        let mut reported = self.reported_errors.lock();
        let Some(error) = error else {
            reported.remove(&self.info_hash);
            return None;
        };
        if reported.get(&self.info_hash).map(String::as_str) != Some(error) {
            warn!(info_hash = %self.info_hash, error, "torrent_error_state");
            reported.insert(self.info_hash.clone(), error.to_string());
        }
        Some(TORRENT_ERROR_MESSAGE.to_string())
    }

    /// Snapshot of this torrent's pinned file indices.
    fn pinned_set(&self) -> BTreeSet<usize> {
        self.pinned_files
            .lock()
            .get(&self.info_hash)
            .cloned()
            .unwrap_or_default()
    }

    /// True for any torrent state that must go through the init gate rather
    /// than take the fast path: literally `Initializing` (opening/hash-
    /// checking files, where `update_only_files` bails and `FileStream`
    /// cannot be opened), `Error` (already failed -- routing it through
    /// `wait_until_initialized` turns librqbit's raw error into
    /// `TorrentInitError::Failed` instead of letting a `FileStream` open
    /// attempt fail downstream with a bare 500), and the transient `None`
    /// state (only visible mid state-swap; librqbit itself treats seeing it
    /// as a bug, so gating it here is purely defensive, not load-bearing).
    fn is_initializing(&self) -> bool {
        self.handle.with_state(|s| {
            matches!(
                s,
                ManagedTorrentState::Initializing(_)
                    | ManagedTorrentState::Error(_)
                    | ManagedTorrentState::None
            )
        })
    }

    /// Block until the torrent leaves the Initializing state, bounded by
    /// `TORRENT_INIT_TIMEOUT`. Cheap fast path when already initialized.
    async fn await_initialized(&self) -> std::result::Result<(), TorrentInitError> {
        if !self.is_initializing() {
            return Ok(());
        }
        debug!(
            info_hash = %self.info_hash,
            "torrent is initializing; holding the request until it is ready"
        );
        await_initialized(
            &self.info_hash,
            TORRENT_INIT_TIMEOUT,
            self.handle.wait_until_initialized(),
        )
        .await
    }

    /// Owned `'static` gate future for a spawned deferred-selection waiter.
    fn init_gate_future(
        &self,
    ) -> impl Future<Output = std::result::Result<(), TorrentInitError>> + Send + 'static {
        let handle = self.handle.clone();
        let info_hash = self.info_hash.clone();
        async move {
            await_initialized(
                &info_hash,
                TORRENT_INIT_TIMEOUT,
                handle.wait_until_initialized(),
            )
            .await
        }
    }

    fn deferred_selection(&self) -> Arc<DeferredSelection<DeferredOp>> {
        self.deferred_selections
            .lock()
            .entry(self.info_hash.clone())
            .or_insert_with(DeferredSelection::new)
            .clone()
    }

    /// Look up this torrent's deferred-selection slot without creating one --
    /// a torrent that never deferred anything should not get a map entry just
    /// because a direct apply checked whether there was something to
    /// supersede.
    fn deferred_selection_if_present(&self) -> Option<Arc<DeferredSelection<DeferredOp>>> {
        self.deferred_selections
            .lock()
            .get(&self.info_hash)
            .cloned()
    }

    /// Park `op` until the torrent initializes (see `DeferredSelection`).
    fn defer_selection(&self, op: SelectionOp, context: &'static str) {
        debug!(
            info_hash = %self.info_hash,
            ?op,
            context,
            "torrent is initializing; deferring file selection until it is ready"
        );
        let applier = self.clone();
        self.deferred_selection().defer(
            DeferredOp { op, context },
            self.init_gate_future(),
            move |deferred: DeferredOp| {
                let handle = applier.clone();
                async move {
                    let Some(file_count) = handle.file_count_from_metadata() else {
                        return;
                    };
                    handle
                        .apply_selection_now(deferred.op, file_count, deferred.context)
                        .await;
                }
            },
        );
    }

    /// Apply a selection op, handling the Initializing state per `policy`
    /// (see `InitPolicy`). Err only for a failed/timed-out `Wait`; everything
    /// else is best-effort and logged.
    async fn apply_selection(
        &self,
        op: SelectionOp,
        file_count: usize,
        context: &'static str,
        policy: InitPolicy,
    ) -> Result<()> {
        if self.is_initializing() {
            match policy {
                InitPolicy::Wait => self.await_initialized().await?,
                InitPolicy::Defer => {
                    self.defer_selection(op, context);
                    return Ok(());
                }
                InitPolicy::Skip => {
                    debug!(
                        info_hash = %self.info_hash,
                        ?op,
                        context,
                        "torrent is initializing; skipping file selection update"
                    );
                    return Ok(());
                }
            }
        }
        // A direct Prepare/Reconcile sets the whole selection and so supersedes
        // anything still parked from before the torrent became ready. A Clear
        // only removes one file and must not discard a parked reconcile. Use
        // the non-inserting lookup: a torrent that never deferred anything
        // has no slot, and checking should not create one.
        if !matches!(op, SelectionOp::Clear(_))
            && let Some(slot) = self.deferred_selection_if_present()
        {
            slot.supersede();
        }
        if !self.apply_selection_now(op, file_count, context).await
            && policy == InitPolicy::Defer
            && self.is_initializing()
        {
            // Lost the race with a (re-)initialization: park it after all.
            self.defer_selection(op, context);
        }
        Ok(())
    }

    /// Run the pure planner against the live `only_files` selection and apply
    /// the result via `Session::update_only_files`. Returns whether librqbit
    /// accepted the update (a no-op plan counts as accepted). Failures are
    /// logged, not propagated: with the selection unchanged librqbit still
    /// downloads and playback works through the blocking reader.
    async fn apply_selection_now(
        &self,
        op: SelectionOp,
        file_count: usize,
        context: &'static str,
    ) -> bool {
        let current = self.handle.only_files();
        let pinned = self.pinned_set();
        let Some(set) = plan_only_files(current.as_deref(), file_count, &pinned, op) else {
            return true;
        };
        match self.session.update_only_files(&self.handle, &set).await {
            Ok(()) => {
                debug!(
                    info_hash = %self.info_hash,
                    ?op,
                    context,
                    selection = ?set,
                    "Updated librqbit file selection"
                );
                true
            }
            Err(e) => {
                warn!(
                    info_hash = %self.info_hash,
                    ?op,
                    context,
                    error = %e,
                    "Failed to update librqbit file selection (best-effort; selection unchanged)"
                );
                false
            }
        }
    }

    /// Build a PieceReadiness with live peer count and download rate filled
    /// from one stats snapshot.
    fn readiness(
        &self,
        start: std::time::Instant,
        ready: bool,
        piece: i32,
        ready_pieces: u32,
        target_pieces: u32,
        reason: String,
    ) -> PieceReadiness {
        let stats = self.handle.stats();
        let (peers, download_rate) = stats
            .live
            .as_ref()
            .map(|l| {
                (
                    l.snapshot.peer_stats.live as u64,
                    // Speed.mbps is MiB/s; convert to bytes/s.
                    (l.download_speed.mbps * 1_048_576.0) as u64,
                )
            })
            .unwrap_or((0, 0));
        PieceReadiness {
            ready,
            piece,
            ready_pieces,
            target_pieces,
            elapsed_ms: start.elapsed().as_millis() as u64,
            peers,
            download_rate,
            reason,
        }
    }
}

impl Clone for LibrqbitHandle {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            info_hash: self.info_hash.clone(),
            session: self.session.clone(),
            deferred_selections: self.deferred_selections.clone(),
            pinned_files: self.pinned_files.clone(),
            reported_errors: self.reported_errors.clone(),
            stream_positions: self.stream_positions.clone(),
            swarm_scraper: self.swarm_scraper.clone(),
        }
    }
}

/// The file names of a serialized torrent, in the torrent's own order.
///
/// `librqbit::create_torrent` walks a directory with `walkdir` and never
/// sorts, so a torrent built from a fixture folder lists its files in the
/// filesystem's readdir order. On ext4 that order is a hash of the name,
/// seeded per filesystem: the same fixture yields one order here and the
/// reverse on a CI runner. A test must therefore look its file up by name
/// and never assume the order it wrote the files in.
#[cfg(test)]
pub(crate) fn torrent_file_names(torrent_bytes: &[u8]) -> Vec<String> {
    let meta = librqbit::torrent_from_bytes(torrent_bytes).expect("parse torrent");
    let info = &meta.info.data;
    fn decode(bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).into_owned()
    }
    match info.files.as_ref() {
        // Multi-file: each entry is a path split into components.
        Some(files) => files
            .iter()
            .map(|f| {
                f.path
                    .iter()
                    .map(|c| decode(c.as_ref()))
                    .collect::<Vec<_>>()
                    .join("/")
            })
            .collect(),
        // Single-file: the torrent name is the one file.
        None => info
            .name
            .as_ref()
            .map(|n| decode(n.as_ref()))
            .into_iter()
            .collect(),
    }
}

/// The index `name` has in the torrent's file list. See
/// [`torrent_file_names`]: a file index is never the fixture's creation
/// order, so tests look it up instead of hardcoding it.
#[cfg(test)]
pub(crate) fn torrent_file_index(torrent_bytes: &[u8], name: &str) -> usize {
    let names = torrent_file_names(torrent_bytes);
    names
        .iter()
        .position(|n| n == name)
        .unwrap_or_else(|| panic!("{name} is not among the torrent's files: {names:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::TorrentBackend;

    /// Purely a string check -- no DNS resolution, no network. Every entry
    /// must be a nonempty host and a nonzero port, same shape a
    /// `dhtBootstrapNodes` entry is validated against
    /// (`server/src/routes/system.rs`'s `is_valid_dht_bootstrap_node`).
    fn assert_valid_host_port(entry: &str) {
        let (host, port) = entry
            .rsplit_once(':')
            .unwrap_or_else(|| panic!("{entry:?} has no host:port split"));
        assert!(!host.is_empty(), "{entry:?} has an empty host");
        let port: u16 = port
            .parse()
            .unwrap_or_else(|e| panic!("{entry:?} has an unparseable port: {e}"));
        assert_ne!(port, 0, "{entry:?} has the zero port");
    }

    #[test]
    fn default_dht_bootstrap_nodes_is_nonempty_and_well_formed() {
        assert!(
            !DEFAULT_DHT_BOOTSTRAP_NODES.is_empty(),
            "a single point of failure is exactly what widening this list is for"
        );
        for entry in DEFAULT_DHT_BOOTSTRAP_NODES {
            assert_valid_host_port(entry);
        }
    }

    #[test]
    fn resolve_dht_bootstrap_nodes_falls_back_to_the_default_when_unconfigured() {
        assert_eq!(
            resolve_dht_bootstrap_nodes(&[]),
            DEFAULT_DHT_BOOTSTRAP_NODES
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn resolve_dht_bootstrap_nodes_lets_a_configured_list_replace_the_default() {
        let configured = vec!["example.test:6881".to_string()];
        let resolved = resolve_dht_bootstrap_nodes(&configured);
        // Replaces, not appends: none of the built-in defaults survive
        // alongside the operator's override.
        assert_eq!(resolved, configured);
        for default_entry in DEFAULT_DHT_BOOTSTRAP_NODES {
            assert!(
                !resolved.contains(&default_entry.to_string()),
                "the configured list should have replaced the default entirely"
            );
        }
    }

    /// How long a bounded state-wait in these tests may take before it
    /// gives up. These waits poll for something a background task or the
    /// blocking hash-check pool has to do, so the bound is not a timing
    /// assertion: it is only there so a regression fails instead of
    /// hanging. Generous on purpose -- a CI runner under load (or
    /// `--test-threads=16` on two cores) can be an order of magnitude
    /// slower than an idle laptop, and a tight bound turns that into a
    /// spurious failure.
    const TEST_WAIT_BOUND: Duration = Duration::from_secs(60);

    /// `Ephemeral` sessions never collide: the OS hands each its own port.
    /// (Whether a second bind of an already-taken *fixed* port fails is
    /// platform-dependent -- Windows lets it through -- so that is not
    /// asserted here.)
    #[tokio::test(flavor = "multi_thread")]
    async fn ephemeral_sessions_coexist() {
        let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();

        let (a, _) = LibrqbitBackend::new(
            dirs[0].path().to_path_buf(),
            TorrentListenPort::Ephemeral,
            Vec::new(),
        )
        .await
        .expect("first ephemeral session");
        let (b, _) = LibrqbitBackend::new(
            dirs[1].path().to_path_buf(),
            TorrentListenPort::Ephemeral,
            Vec::new(),
        )
        .await
        .expect("second ephemeral session alongside the first");

        let pa = a.session.listen_addr().expect("listening").port();
        let pb = b.session.listen_addr().expect("listening").port();
        assert_ne!(pa, 0);
        assert_ne!(pb, 0);
        assert_ne!(pa, pb, "each ephemeral session gets its own port");
    }

    /// Write `len` patterned bytes to `path` (deterministic, non-trivial data
    /// so piece hashes are meaningful).
    pub(super) async fn write_payload(path: &std::path::Path, len: usize) {
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        tokio::fs::write(path, &data).await.expect("write payload");
    }

    /// Create a .torrent for `path` with small pieces so tests stay fast.
    /// Returns (serialized torrent bytes, info hash as hex). We extract what we
    /// need immediately rather than holding the borrowing result.
    pub(super) async fn make_torrent(path: &std::path::Path) -> (Vec<u8>, String) {
        let t = librqbit::create_torrent(
            path,
            librqbit::CreateTorrentOptions {
                name: None,
                trackers: Vec::new(),
                piece_length: Some(16384),
            },
            &librqbit::spawn_utils::BlockingSpawner::new(1),
        )
        .await
        .expect("create torrent");
        (
            t.as_bytes().expect("serialize torrent").to_vec(),
            t.info_hash().as_string(),
        )
    }

    /// Backend + torrent added from bytes; payload seeded iff the caller wrote
    /// the payload file into the download dir beforehand.
    pub(super) async fn backend_with_torrent(
        download_dir: &std::path::Path,
        torrent_bytes: &[u8],
    ) -> (LibrqbitBackend, LibrqbitHandle) {
        let backend = LibrqbitBackend::new_for_tests(download_dir.to_path_buf())
            .await
            .expect("hermetic session");
        let handle = backend
            .add_torrent(TorrentSource::Bytes(torrent_bytes.to_vec()), vec![])
            .await
            .expect("add torrent");
        (backend, handle)
    }

    /// `stats().sources` must list the trackers the torrent was added with:
    /// it is the only place a client can confirm its `tr=` trackers reached
    /// the engine, since librqbit cannot add trackers after the fact.
    #[tokio::test]
    async fn stats_sources_list_the_trackers_the_torrent_was_added_with() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 16 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let backend = LibrqbitBackend::new_for_tests(dir.clone())
            .await
            .expect("hermetic session");
        let handle = backend
            .add_torrent(
                TorrentSource::Bytes(torrent_bytes),
                vec![
                    "udp://two.invalid:6969/announce".to_string(),
                    "https://one.invalid/announce".to_string(),
                ],
            )
            .await
            .expect("add torrent");

        let stats = TorrentHandle::stats(&handle).await;
        let urls: Vec<&str> = stats.sources.iter().map(|s| s.url.as_str()).collect();
        assert_eq!(
            urls,
            [
                "https://one.invalid/announce",
                "udp://two.invalid:6969/announce"
            ],
            "sorted tracker URLs, got {urls:?}"
        );
        assert!(stats.sources.iter().all(|s| s.num_requests == 0));
    }

    /// End to end through the stats path: a tracker scrape's counters land on
    /// the matching `sources` entry, and the swarm totals are the max across
    /// the trackers that answered -- a tracker that did not answer reports
    /// nothing at all rather than zero.
    ///
    /// The hermetic backend disables scraping outright, so the handle is
    /// rebuilt around a scraper wired to a stub transport: no socket is
    /// opened here either.
    #[tokio::test]
    async fn stats_report_swarm_counts_from_the_tracker_scrape() {
        use crate::scrape::{ScrapeOutcome, ScrapeTransport, SwarmCounts};

        struct StubTrackers;

        #[async_trait::async_trait]
        impl ScrapeTransport for StubTrackers {
            async fn scrape(&self, tracker: &str, _info_hash: [u8; 20]) -> ScrapeOutcome {
                if tracker.starts_with("https://one") {
                    ScrapeOutcome::Counts(SwarmCounts {
                        seeders: 12,
                        leechers: 4,
                        completed: 900,
                    })
                } else {
                    ScrapeOutcome::Failed
                }
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 16 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let backend = LibrqbitBackend::new_for_tests(dir.clone())
            .await
            .expect("hermetic session");
        let handle = backend
            .add_torrent(
                TorrentSource::Bytes(torrent_bytes),
                vec![
                    "udp://two.invalid:6969/announce".to_string(),
                    "https://one.invalid/announce".to_string(),
                ],
            )
            .await
            .expect("add torrent");
        let handle = LibrqbitHandle {
            swarm_scraper: SwarmScraper::with_transport(Arc::new(StubTrackers)),
            ..handle.clone()
        };

        // The first poll can only schedule the round -- stats never wait on
        // the network -- so poll until it has landed. The bound is only there
        // so a regression fails instead of hanging.
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        let stats = loop {
            let stats = TorrentHandle::stats(&handle).await;
            if stats.swarm_seeders.is_some() || std::time::Instant::now() >= deadline {
                break stats;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };

        assert_eq!(
            stats.swarm_seeders,
            Some(12),
            "max over the answering trackers"
        );
        assert_eq!(stats.swarm_leechers, Some(4));
        assert!(
            stats.swarm_scrape_age_secs.is_some(),
            "figures carry an age: {:?}",
            stats.swarm_scrape_age_secs
        );

        let scraped = stats
            .sources
            .iter()
            .find(|s| s.url.starts_with("https://one"))
            .expect("the scraped tracker is listed");
        assert_eq!(scraped.seeders, Some(12));
        assert_eq!(scraped.leechers, Some(4));
        assert_eq!(scraped.completed, Some(900));

        let unanswered = stats
            .sources
            .iter()
            .find(|s| s.url.starts_with("udp://two"))
            .expect("the failing tracker is still listed");
        assert_eq!(unanswered.seeders, None, "a failed scrape is not a zero");
        assert_eq!(unanswered.leechers, None);
        assert_eq!(unanswered.completed, None);
    }

    /// `connected_seeders` is librqbit's `live_seeders` aggregate -- connected
    /// peers whose bitfield covers the whole torrent -- and not one of the
    /// discovery counters that sit beside it in the same struct.
    ///
    /// No peer can ever go live in a hermetic session (`new_for_tests` binds
    /// no port and runs no DHT), so the count itself stays 0 here; the live
    /// case is `wait_for_piece_ready_live_swarm`, which needs a real swarm and
    /// is `#[ignore]`d. What is pinned down is the wiring: `initial_peers`
    /// hands the torrent one address that will never answer, so `seen` and
    /// `unique` climb off zero while `live_seeders` does not -- a mapping to
    /// the wrong counter reports that address as a seeder.
    #[tokio::test(flavor = "multi_thread")]
    async fn stats_connected_seeders_mirrors_librqbits_live_seeder_count() {
        let src = tempfile::tempdir().unwrap();
        let payload = src.path().join("payload.bin");
        write_payload(&payload, 16 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        // A download dir of its own, so the torrent is a leecher with nothing
        // on disk rather than an instantly-finished seed.
        let dl = tempfile::tempdir().unwrap();
        let backend = LibrqbitBackend::new_for_tests(dl.path().to_path_buf())
            .await
            .expect("hermetic session");
        // Straight to the session: `add_torrent` has no `initial_peers`
        // parameter, and with no DHT and no trackers that is the only way a
        // peer address can reach the torrent at all. Loopback port 9 is the
        // discard port -- nothing listens there, so the peer is seen and then
        // dies without ever going live.
        let response = backend
            .session
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes::Bytes::from(torrent_bytes)),
                Some(librqbit::AddTorrentOptions {
                    overwrite: true,
                    initial_peers: Some(vec![(std::net::Ipv4Addr::LOCALHOST, 9).into()]),
                    ..Default::default()
                }),
            )
            .await
            .expect("add torrent");
        let (librqbit::AddTorrentResponse::Added(_, inner)
        | librqbit::AddTorrentResponse::AlreadyManaged(_, inner)) = response
        else {
            panic!("expected the torrent to be added");
        };
        let handle = LibrqbitHandle {
            info_hash: inner.info_hash().as_string(),
            handle: inner,
            session: backend.session.clone(),
            deferred_selections: Default::default(),
            pinned_files: Default::default(),
            stream_positions: Default::default(),
            reported_errors: Default::default(),
            swarm_scraper: SwarmScraper::disabled(),
        };

        // Bounded poll: the initial peer reaches the peer list once the
        // torrent has finished initializing and gone live. The bound only
        // keeps a regression from hanging.
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        let live_seeders = loop {
            let peer_stats = handle.handle.stats().live.map(|l| l.snapshot.peer_stats);
            match peer_stats {
                Some(p) if p.seen > 0 => break p.live_seeders,
                _ => assert!(
                    std::time::Instant::now() < deadline,
                    "the torrent never saw its initial peer"
                ),
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(live_seeders, 0, "an address nobody answers is not a seeder");

        let stats = TorrentHandle::stats(&handle).await;
        assert_eq!(
            stats.connected_seeders, live_seeders as u64,
            "connected_seeders must be librqbit's live_seeders"
        );
        assert!(
            stats.unique > 0,
            "the initial peer must have been seen, or the counters are all \
             trivially equal and this proves nothing"
        );
        assert_ne!(
            stats.connected_seeders, stats.unique,
            "connected_seeders is the seeder count, not the seen-peer count"
        );
    }

    /// The magnet branch of librqbit's `Session::add_torrent` ignores
    /// `AddTorrentOptions::trackers`, so the trackers have to be `tr=` params
    /// of the URL it is given -- percent-encoded, one per tracker, exactly as
    /// its `Magnet::parse` reads them back.
    #[test]
    fn magnet_with_trackers_encodes_each_tracker_as_a_tr_param() {
        let trackers = vec![
            "udp://one.invalid:6969/announce".to_string(),
            "https://two.invalid/announce?x=100%25".to_string(),
        ];
        let magnet = magnet_with_trackers(
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
            &trackers,
        );
        assert_eq!(
            magnet,
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567\
             &tr=udp%3A%2F%2Fone.invalid%3A6969%2Fannounce\
             &tr=https%3A%2F%2Ftwo.invalid%2Fannounce%3Fx%3D100%2525"
        );
        // What librqbit will actually see: `Magnet::parse` collects `tr`.
        let parsed = librqbit::Magnet::parse(&magnet).expect("valid magnet");
        assert_eq!(parsed.trackers, trackers);
        assert_eq!(
            parsed.as_id20().unwrap().as_string(),
            "0123456789abcdef0123456789abcdef01234567"
        );
    }

    #[test]
    fn magnet_with_trackers_keeps_existing_trs_and_skips_duplicates() {
        let magnet = magnet_with_trackers(
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=x&tr=udp%3A%2F%2Fone.invalid%2Fannounce",
            &[
                "udp://one.invalid/announce".to_string(),
                "udp://two.invalid/announce".to_string(),
            ],
        );
        let parsed = librqbit::Magnet::parse(&magnet).expect("valid magnet");
        assert_eq!(
            parsed.trackers,
            ["udp://one.invalid/announce", "udp://two.invalid/announce"]
        );
        assert_eq!(parsed.name.as_deref(), Some("x"));
    }

    #[test]
    fn magnet_with_trackers_upgrades_bare_hashes_and_leaves_other_urls_alone() {
        let trackers = vec!["udp://one.invalid/announce".to_string()];
        let magnet = magnet_with_trackers("0123456789abcdef0123456789abcdef01234567", &trackers);
        assert_eq!(
            magnet,
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&tr=udp%3A%2F%2Fone.invalid%2Fannounce"
        );
        assert_eq!(
            magnet_with_trackers("https://example.invalid/a.torrent", &trackers),
            "https://example.invalid/a.torrent"
        );
        assert_eq!(
            magnet_with_trackers(
                "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
                &[]
            ),
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
        );
    }

    /// End-to-end check of the same thing against a real session: a magnet
    /// added with a custom tracker must list it in `stats().sources` once
    /// metadata has resolved. Needs peers, so it runs manually:
    ///
    /// ```sh
    /// STREAM_SERVER_TEST_MAGNET='magnet:?xt=urn:btih:...' \
    ///     cargo test -p enginefs --release magnet_add_keeps_custom_trackers_live_swarm -- --ignored --nocapture
    /// ```
    #[ignore = "requires network and STREAM_SERVER_TEST_MAGNET; see doc comment"]
    #[tokio::test(flavor = "multi_thread")]
    async fn magnet_add_keeps_custom_trackers_live_swarm() {
        let magnet = std::env::var("STREAM_SERVER_TEST_MAGNET")
            .expect("set STREAM_SERVER_TEST_MAGNET to a magnet link");
        let tmp = tempfile::tempdir().unwrap();
        let (backend, _restored) = LibrqbitBackend::new(
            tmp.path().to_path_buf(),
            TorrentListenPort::Ephemeral,
            Vec::new(),
        )
        .await
        .expect("network session");
        let custom = "udp://custom-tracker.invalid:6969/announce".to_string();
        let handle = backend
            .add_torrent(TorrentSource::Url(magnet), vec![custom.clone()])
            .await
            .expect("add magnet");
        let stats = TorrentHandle::stats(&handle).await;
        let urls: Vec<&str> = stats.sources.iter().map(|s| s.url.as_str()).collect();
        assert!(urls.contains(&custom.as_str()), "sources: {urls:?}");
    }

    #[tokio::test]
    async fn add_get_list_remove_torrent() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 96 * 1024).await;
        let (torrent_bytes, expected_hash) = make_torrent(&payload).await;

        let (backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;

        // add_torrent must report the real info hash, not a placeholder.
        assert_eq!(handle.info_hash, expected_hash);
        handle.handle.wait_until_initialized().await.unwrap();

        let listed = backend.list_torrents().await;
        assert_eq!(listed, vec![expected_hash.clone()]);

        let got = backend.get_torrent(&expected_hash).await;
        assert!(got.is_some());
        assert_eq!(got.unwrap().info_hash, expected_hash);

        // Unknown but well-formed hash -> None; garbage -> None.
        let missing_hash = "0".repeat(40);
        assert!(backend.get_torrent(&missing_hash).await.is_none());
        assert!(backend.get_torrent("not-a-hash").await.is_none());

        backend.remove_torrent(&expected_hash).await.unwrap();
        assert!(backend.list_torrents().await.is_empty());
        assert!(backend.get_torrent(&expected_hash).await.is_none());
        // Removing again is an error (matches libtorrent's not-found Err).
        assert!(backend.remove_torrent(&expected_hash).await.is_err());
    }

    #[test]
    fn file_progress_fields_maps_have_bytes() {
        // Normal partial progress.
        assert_eq!(file_progress_fields(100, 25), (25, 0.25));
        // Complete.
        assert_eq!(file_progress_fields(100, 100), (100, 1.0));
        // have > len is clamped (last-piece rounding in the chunk tracker).
        assert_eq!(file_progress_fields(100, 120), (100, 1.0));
        // Zero-length files are trivially complete.
        assert_eq!(file_progress_fields(0, 0), (0, 1.0));
        // Initializing torrents report an empty file_progress vec -> have = 0.
        assert_eq!(file_progress_fields(100, 0), (0, 0.0));
    }

    #[test]
    fn startup_phase_maps_librqbit_states() {
        use librqbit::TorrentStatsState as S;
        // Missing metadata wins regardless of state.
        assert_eq!(
            startup_phase(false, &S::Live, false),
            StartupPhase::ResolvingMetadata
        );
        assert_eq!(
            startup_phase(false, &S::Initializing { paused: false }, false),
            StartupPhase::ResolvingMetadata
        );
        // Hash check, paused or not.
        assert_eq!(
            startup_phase(true, &S::Initializing { paused: false }, false),
            StartupPhase::Checking
        );
        assert_eq!(
            startup_phase(true, &S::Initializing { paused: true }, false),
            StartupPhase::Checking
        );
        // Piece map exists: ready only when the whole torrent is finished
        // (per-file refinement happens in EngineStats::focus_stream_file).
        assert_eq!(
            startup_phase(true, &S::Live, false),
            StartupPhase::Buffering
        );
        assert_eq!(startup_phase(true, &S::Live, true), StartupPhase::Ready);
        assert_eq!(
            startup_phase(true, &S::Paused, false),
            StartupPhase::Buffering
        );
        assert_eq!(startup_phase(true, &S::Paused, true), StartupPhase::Ready);
        assert_eq!(startup_phase(true, &S::Error, false), StartupPhase::Error);
    }

    /// A seeded torrent (payload already in the download dir) is `ready`
    /// with its whole initial window on disk, straight from librqbit's have
    /// bitfield via `api_dump_haves`.
    #[tokio::test]
    async fn stats_phase_ready_with_full_initial_window_for_seeded_torrent() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        let payload_len = 96 * 1024u64;
        write_payload(&payload, payload_len as usize).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let mut stats = TorrentHandle::stats(&handle).await;
        assert_eq!(stats.phase, StartupPhase::Ready);
        assert_eq!(stats.checked_bytes, None);
        // Window is clamped to the (small) file length.
        assert_eq!(stats.files[0].initial_window_bytes, Some(payload_len));
        assert_eq!(stats.files[0].initial_window_ready_bytes, Some(payload_len));
        stats.focus_stream_file(0);
        assert_eq!(stats.phase, StartupPhase::Ready);
        assert_eq!(stats.initial_window_ready_bytes, Some(payload_len));
        assert_eq!(stats.initial_window_bytes, Some(payload_len));
    }

    /// The startup window follows the reader, and the piece length is
    /// reported alongside it. Anchored at the file head, the window
    /// described bytes nobody was fetching after a seek -- it sat at 0%
    /// while the seek region streamed perfectly -- and without the piece
    /// length a client cannot tell a slow download from a window that is
    /// simply smaller than one piece and can only read 0% or 100%.
    #[tokio::test]
    async fn stats_window_follows_the_reader_and_reports_the_piece_length() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        let payload_len = 96 * 1024u64;
        write_payload(&payload, payload_len as usize).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let stats = TorrentHandle::stats(&handle).await;
        let piece_length = stats.piece_length.expect("metadata is resolved");
        assert!(piece_length > 0);
        assert_eq!(
            stats.files[0].initial_window_bytes,
            Some(payload_len),
            "a fresh torrent is measured from the head"
        );

        // Open a reader part-way in, as a `Range` request or a seek does.
        // Start on a piece boundary so the expected window is exactly the
        // tail, whatever the fixture's piece length turns out to be.
        let seek_to = payload_len - piece_length;
        let _reader = handle
            .get_file_reader(
                0,
                seek_to,
                0,
                None,
                crate::backend::priorities::PlaybackIntent::DirectSeek,
                crate::backend::priorities::BufferProfile::Normal,
            )
            .await
            .unwrap();

        let stats = TorrentHandle::stats(&handle).await;
        assert_eq!(
            stats.files[0].initial_window_bytes,
            Some(piece_length),
            "the window is what the reader is waiting for, not the head"
        );
        assert_eq!(stats.piece_length, Some(piece_length));
    }

    /// An unseeded torrent with no peers sits in `buffering` with an empty
    /// initial window, and its peer-discovery counters are all zero.
    #[tokio::test]
    async fn stats_phase_buffering_with_empty_initial_window_for_unseeded_torrent() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let src_dir = tmp.path().join("src");
        tokio::fs::create_dir_all(&src_dir).await.unwrap();
        let payload = src_dir.join("payload.bin");
        let payload_len = 64 * 1024u64;
        write_payload(&payload, payload_len as usize).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let download_dir = tmp.path().join("dl");
        let (_backend, handle) = backend_with_torrent(&download_dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let mut stats = TorrentHandle::stats(&handle).await;
        assert_eq!(stats.phase, StartupPhase::Buffering);
        assert_eq!(stats.files[0].initial_window_bytes, Some(payload_len));
        assert_eq!(stats.files[0].initial_window_ready_bytes, Some(0));
        assert_eq!(stats.peer_discovery, PeerDiscovery::default());
        stats.focus_stream_file(0);
        assert_eq!(stats.phase, StartupPhase::Buffering);
        assert_eq!(stats.initial_window_ready_bytes, Some(0));
        assert_eq!(stats.initial_window_bytes, Some(payload_len));
    }

    #[tokio::test]
    async fn stats_report_full_progress_for_seeded_torrent() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        let payload_len = 96 * 1024u64;
        write_payload(&payload, payload_len as usize).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let stats = TorrentHandle::stats(&handle).await;
        assert!(stats.has_metadata);
        assert!(stats.is_finished);
        assert_eq!(stats.downloaded, payload_len);
        assert!((stats.stream_progress - 1.0).abs() < f64::EPSILON);
        assert_eq!(stats.files.len(), 1);
        assert_eq!(stats.files[0].length, payload_len);
        assert_eq!(stats.files[0].downloaded, payload_len);
        assert!((stats.files[0].progress - 1.0).abs() < f64::EPSILON);

        assert!(TorrentHandle::is_finished(&handle).await);
        assert!(handle.is_file_complete(0).await);
        assert!(!handle.is_file_complete(1).await, "out-of-range file");
    }

    #[tokio::test]
    async fn stats_report_zero_progress_for_unseeded_torrent() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        // Create the torrent from a payload OUTSIDE the download dir so the
        // session has none of the data.
        let src_dir = tmp.path().join("src");
        tokio::fs::create_dir_all(&src_dir).await.unwrap();
        let payload = src_dir.join("payload.bin");
        let payload_len = 64 * 1024u64;
        write_payload(&payload, payload_len as usize).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;

        let download_dir = tmp.path().join("dl");
        let (_backend, handle) = backend_with_torrent(&download_dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let stats = TorrentHandle::stats(&handle).await;
        assert!(stats.has_metadata);
        assert!(!stats.is_finished);
        assert_eq!(stats.downloaded, 0);
        assert_eq!(stats.files.len(), 1);
        assert_eq!(stats.files[0].downloaded, 0);
        assert_eq!(stats.files[0].progress, 0.0);

        assert!(!TorrentHandle::is_finished(&handle).await);
        assert!(!handle.is_file_complete(0).await);
    }

    // Multi-thread flavor: FileStream reads go through block_in_place.
    #[tokio::test(flavor = "multi_thread")]
    async fn wait_for_piece_ready_is_ready_on_seeded_torrent() {
        use crate::backend::TorrentHandle;
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 96 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;
        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let r = handle
            .wait_for_piece_ready(
                0,
                0,
                TEST_WAIT_BOUND,
                PlaybackIntent::DirectInitial,
                BufferProfile::Normal,
            )
            .await
            .unwrap();
        assert!(r.ready, "seeded torrent must be ready: {}", r.reason);
        assert_eq!(r.reason, "stream-read");
        assert_eq!(r.piece, 0);
        assert_eq!((r.ready_pieces, r.target_pieces), (1, 1));

        // Mid-file offset: piece index = offset / piece_length (single-file
        // torrent, so the file starts at torrent offset 0).
        let offset = 40_000u64;
        let r = handle
            .wait_for_piece_ready(
                0,
                offset,
                TEST_WAIT_BOUND,
                PlaybackIntent::DirectSeek,
                BufferProfile::Normal,
            )
            .await
            .unwrap();
        assert!(r.ready, "mid-file offset must be ready: {}", r.reason);
        assert_eq!(r.piece, (offset / 16384) as i32);
    }

    // Exercises the get_file_reader -> stream_with_options wiring: every intent
    // must produce a positive lookahead window (stream_with_options asserts
    // lookahead_bytes > 0), so a successful open+read confirms the intent-sized
    // window is applied rather than rejected.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_file_reader_applies_intent_sized_lookahead() {
        use crate::backend::TorrentHandle;
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        use tokio::io::AsyncReadExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 96 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;
        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        // A narrow-window intent (4 MiB) and a wide-window intent (128 MiB)
        // both yield a readable stream.
        for intent in [PlaybackIntent::DirectInitial, PlaybackIntent::DirectSeek] {
            // Sanity: the helper the reader uses is positive for this intent.
            assert!(
                crate::backend::priorities::librqbit_stream_lookahead_bytes(
                    intent,
                    BufferProfile::Normal
                ) > 0,
                "lookahead must be positive for {intent:?}"
            );
            let mut reader = handle
                .get_file_reader(0, 0, 100, None, intent, BufferProfile::Normal)
                .await
                .unwrap_or_else(|e| panic!("get_file_reader failed for {intent:?}: {e:#}"));
            let mut buf = [0u8; 1];
            let n = reader.read(&mut buf).await.expect("read first byte");
            assert_eq!(n, 1, "seeded file must yield a byte for {intent:?}");
            assert_eq!(buf[0], 0, "first payload byte is (0 % 251) == 0");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wait_for_piece_ready_times_out_without_peers() {
        use crate::backend::TorrentHandle;
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        let tmp = tempfile::tempdir().unwrap();
        let src_dir = tmp.path().join("src");
        tokio::fs::create_dir_all(&src_dir).await.unwrap();
        let payload = src_dir.join("payload.bin");
        write_payload(&payload, 64 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;
        let (_backend, handle) = backend_with_torrent(&tmp.path().join("dl"), &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let timeout = Duration::from_millis(300);
        let r = handle
            .wait_for_piece_ready(
                0,
                0,
                timeout,
                PlaybackIntent::DirectInitial,
                BufferProfile::Normal,
            )
            .await
            .unwrap();
        assert!(!r.ready);
        assert_eq!(r.reason, "timeout");
        assert!(r.elapsed_ms >= 300, "elapsed_ms = {}", r.elapsed_ms);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wait_for_piece_ready_rejects_bad_targets() {
        use crate::backend::TorrentHandle;
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 32 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;
        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        // Offset past the end of the file: soft failure, not Err.
        let r = handle
            .wait_for_piece_ready(
                0,
                1_000_000,
                Duration::from_secs(1),
                PlaybackIntent::DirectSeek,
                BufferProfile::Normal,
            )
            .await
            .unwrap();
        assert!(!r.ready);
        assert_eq!(r.reason, "piece-out-of-file-range");

        // Bad file index: structural failure -> Err.
        assert!(
            handle
                .wait_for_piece_ready(
                    7,
                    0,
                    Duration::from_secs(1),
                    PlaybackIntent::DirectInitial,
                    BufferProfile::Normal,
                )
                .await
                .is_err()
        );
    }

    /// Real-swarm integration test for the piece-yank path (needs actual
    /// piece download from live peers, which the hermetic harness cannot
    /// provide). Run manually with a well-seeded magnet link:
    ///
    /// ```sh
    /// STREAM_SERVER_TEST_MAGNET='magnet:?xt=urn:btih:...' \
    ///     cargo test -p enginefs --release wait_for_piece_ready_live_swarm -- --ignored --nocapture
    /// ```
    ///
    /// Uses a network-enabled session (DHT on, real listen port), so it must
    /// stay #[ignore]d in CI.
    #[ignore = "requires network and STREAM_SERVER_TEST_MAGNET; see doc comment"]
    #[tokio::test(flavor = "multi_thread")]
    async fn wait_for_piece_ready_live_swarm() {
        use crate::backend::TorrentHandle;
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        let magnet = std::env::var("STREAM_SERVER_TEST_MAGNET")
            .expect("set STREAM_SERVER_TEST_MAGNET to a magnet link");
        let tmp = tempfile::tempdir().unwrap();
        let (backend, _restored) = LibrqbitBackend::new(
            tmp.path().to_path_buf(),
            TorrentListenPort::Ephemeral,
            Vec::new(),
        )
        .await
        .expect("network session");
        let handle = backend
            .add_torrent(TorrentSource::Url(magnet), vec![])
            .await
            .expect("add magnet");
        let r = handle
            .wait_for_piece_ready(
                0,
                0,
                Duration::from_secs(120),
                PlaybackIntent::DirectInitial,
                BufferProfile::Normal,
            )
            .await
            .expect("structural failure");
        eprintln!("readiness: {r:?}");
        assert!(r.ready, "first piece did not arrive: {}", r.reason);
        assert_eq!(r.reason, "stream-read");
    }

    #[tokio::test(start_paused = true)]
    async fn await_initialized_blocks_until_ready_then_succeeds() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let wait = {
            let notify = notify.clone();
            async move {
                notify.notified().await;
                Ok(())
            }
        };
        let gate = tokio::spawn(await_initialized("abc", Duration::from_secs(60), wait));
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!gate.is_finished(), "gate must block while initializing");
        notify.notify_one();
        gate.await.unwrap().expect("gate opens once initialized");
    }

    #[tokio::test(start_paused = true)]
    async fn await_initialized_times_out_and_reports_failures() {
        let never = std::future::pending::<anyhow::Result<()>>();
        let started = tokio::time::Instant::now();
        let err = await_initialized("abc", Duration::from_secs(5), never)
            .await
            .expect_err("must time out");
        assert_eq!(started.elapsed(), Duration::from_secs(5));
        match err {
            TorrentInitError::TimedOut {
                info_hash,
                timeout_secs,
            } => {
                assert_eq!(info_hash, "abc");
                assert_eq!(timeout_secs, 5);
            }
            other => panic!("expected TimedOut, got {other:?}"),
        }

        let failed = async { Err(anyhow::anyhow!("disk exploded")) };
        let err = await_initialized("abc", Duration::from_secs(5), failed)
            .await
            .expect_err("init failure must propagate");
        match err {
            TorrentInitError::Failed { info_hash, reason } => {
                assert_eq!(info_hash, "abc");
                assert_eq!(reason, "disk exploded");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn deferred_selection_coalesces_and_applies_after_gate() {
        use std::sync::atomic::AtomicUsize;
        let applied = Arc::new(parking_lot::Mutex::new(Vec::<u32>::new()));
        let applies = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(tokio::sync::Notify::new());
        let slot: Arc<DeferredSelection<u32>> = DeferredSelection::new();

        let gate = |notify: &Arc<tokio::sync::Notify>| {
            let notify = notify.clone();
            async move {
                notify.notified().await;
                Ok(())
            }
        };
        let apply = |applied: &Arc<parking_lot::Mutex<Vec<u32>>>, applies: &Arc<AtomicUsize>| {
            let applied = applied.clone();
            let applies = applies.clone();
            move |op: u32| {
                let applied = applied.clone();
                let applies = applies.clone();
                async move {
                    applies.fetch_add(1, Ordering::SeqCst);
                    applied.lock().push(op);
                }
            }
        };

        slot.defer(1, gate(&notify), apply(&applied, &applies));
        slot.defer(2, gate(&notify), apply(&applied, &applies));
        slot.defer(3, gate(&notify), apply(&applied, &applies));
        assert!(slot.has_pending());
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            applies.load(Ordering::SeqCst),
            0,
            "nothing applies before the gate"
        );

        notify.notify_waiters();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            *applied.lock(),
            vec![3],
            "latest op wins, older ones coalesce away"
        );
        assert!(!slot.has_pending());

        // A direct apply supersedes a parked op.
        slot.defer(4, gate(&notify), apply(&applied, &applies));
        assert_eq!(slot.supersede(), Some(4));
        assert!(!slot.has_pending());
        // Let the spawned waiter register on the Notify before waking it
        // (notify_waiters only reaches already-registered waiters).
        tokio::time::sleep(Duration::from_secs(1)).await;
        notify.notify_waiters();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(*applied.lock(), vec![3]);

        // A failed gate drops the parked op with a warning instead of applying.
        let failing = async {
            Err(TorrentInitError::TimedOut {
                info_hash: "x".into(),
                timeout_secs: 1,
            })
        };
        slot.defer(5, failing, apply(&applied, &applies));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(*applied.lock(), vec![3]);
        assert!(!slot.has_pending());
    }

    /// Regression for the lost-update edge in the waiter's Err path: a
    /// `defer` landing between the waiter's `take` and its
    /// `waiter_running.store(false, ..)` must not be swallowed by a loop
    /// that keeps re-checking it against the (now stale) gate result -- the
    /// waiter must hand the slot back so the *next* `defer` spawns a fresh
    /// waiter with a fresh gate instead.
    ///
    /// The real race is between two OS threads landing on two adjacent,
    /// non-yielding instructions (a `Mutex::take` and an `AtomicBool::store`),
    /// which cannot be reproduced deterministically through async task
    /// scheduling -- nothing yields in between for another task to run. So
    /// this test reconstructs the exact interleaving by hand, one step at a
    /// time (this test module is a descendant of the defining module, so
    /// `DeferredSelection`'s fields are visible): it performs the waiter's
    /// `take` (finding nothing, as if it had just drained everything), then
    /// -- standing in for the racing thread -- calls the real `defer` to
    /// queue a fresh op while `waiter_running` is still true, exactly as
    /// `defer` itself does when it observes an already-running waiter, and
    /// only then performs the waiter's release. This is the one ordering
    /// `handle_gate_error`'s single, non-looping `take` can never itself
    /// reproduce (it only ever takes once, before it releases), which is
    /// exactly why the fix is correct: unlike the old drain-and-recheck loop,
    /// there is no code path left that could re-take and drop this op under
    /// the stale verdict.
    #[tokio::test(start_paused = true)]
    async fn deferred_selection_defer_racing_gate_error_is_not_lost() {
        let slot: Arc<DeferredSelection<u32>> = DeferredSelection::new();
        let applied = Arc::new(parking_lot::Mutex::new(Vec::<u32>::new()));
        let apply = {
            let applied = applied.clone();
            move |op: u32| {
                let applied = applied.clone();
                async move {
                    applied.lock().push(op);
                }
            }
        };

        // A waiter is running and has just taken the last op it had (or
        // started with none) -- `pending` is empty, `waiter_running` is true.
        slot.waiter_running.store(true, Ordering::Release);
        assert!(slot.pending.lock().take().is_none());

        // Right here, in the gap before the waiter releases the slot, a real
        // `defer` call races in with a fresh op. It observes `waiter_running`
        // still true, so it queues the op and returns without spawning a
        // second waiter -- trusting the existing one to drain it.
        let never_used_gate = std::future::pending::<std::result::Result<(), TorrentInitError>>();
        slot.defer(7, never_used_gate, apply.clone());
        assert!(
            slot.has_pending(),
            "the racing defer must have queued its op"
        );

        // The waiter concludes its Err verdict and releases the slot -- the
        // exact tail `handle_gate_error` performs after taking whatever was
        // pending *at the time it ran* (nothing, in this interleaving).
        slot.waiter_running.store(false, Ordering::Release);

        // Op 7 must have survived: nothing re-took it under the stale Err,
        // and the slot must be free so the next trigger spawns a fresh
        // waiter with a fresh gate.
        assert!(
            slot.has_pending(),
            "op queued during the error handoff must not be dropped"
        );
        assert!(
            !slot.waiter_running.load(Ordering::Acquire),
            "the slot must be free for the next defer to spawn a fresh waiter"
        );

        // The next trigger (as a subsequent reconcile/prepare call would
        // issue) must pick it up and apply it -- proving the op is not
        // permanently stranded, only deferred to that next trigger.
        let notify = Arc::new(tokio::sync::Notify::new());
        let gate = {
            let notify = notify.clone();
            async move {
                notify.notified().await;
                Ok(())
            }
        };
        slot.defer(7, gate, apply);
        tokio::time::sleep(Duration::from_secs(1)).await;
        notify.notify_waiters();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            *applied.lock(),
            vec![7],
            "op queued during the error handoff must still be applied by the next waiter"
        );
        assert!(!slot.has_pending());
    }

    /// Regression for the first-play readiness race: prepare/reconcile and the
    /// reader are called the instant the torrent is added, without the test
    /// waiting for `wait_until_initialized` first. The gate must make the
    /// selection stick and the reader open regardless of whether librqbit is
    /// still hash-checking (8 MiB of payload widens that window).
    #[tokio::test(flavor = "multi_thread")]
    async fn selection_and_reader_wait_for_initializing_torrent() {
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        use crate::backend::{TorrentFilePriorityPlan, TorrentHandle};
        use tokio::io::AsyncReadExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let content_dir = dir.join("multi");
        tokio::fs::create_dir_all(&content_dir).await.unwrap();
        write_payload(&content_dir.join("a.bin"), 4 * 1024 * 1024).await;
        write_payload(&content_dir.join("b.bin"), 4 * 1024 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&content_dir).await;

        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        let was_initializing = handle.is_initializing();

        // Reconcile (request-path activation) defers while initializing ...
        handle
            .reconcile_file_priorities(TorrentFilePriorityPlan {
                active_file: Some(1),
                hot_file: None,
                generation: 1,
                reason: "test",
            })
            .await
            .unwrap();
        // ... and prepare blocks until the torrent is ready, then applies.
        handle.prepare_file_for_streaming(1).await.unwrap();
        assert!(!handle.is_initializing());
        assert_eq!(handle.handle.only_files(), Some(vec![1]));

        let mut reader = handle
            .get_file_reader(
                1,
                0,
                1,
                None,
                PlaybackIntent::DirectInitial,
                BufferProfile::Normal,
            )
            .await
            .expect("reader opens after initialization");
        let mut buf = [0u8; 1];
        assert_eq!(reader.read(&mut buf).await.unwrap(), 1);
        assert_eq!(buf[0], 0);

        // A deferred reconcile that raced initialization must settle to the
        // latest selection state, never leave a stale parked op behind.
        let slot = handle.deferred_selection();
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        while slot.has_pending() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!slot.has_pending(), "deferred op must drain after init");
        assert_eq!(handle.handle.only_files(), Some(vec![1]));
        assert!(
            was_initializing,
            "test must actually exercise the initializing-torrent gate, not degrade into a \
             happy-path test where the torrent was already ready by the first call"
        );
    }

    /// A reconcile deferred during initialization is applied afterwards even
    /// when no request-path prepare follows it.
    #[tokio::test(flavor = "multi_thread")]
    async fn deferred_reconcile_applies_on_real_torrent() {
        use crate::backend::{TorrentFilePriorityPlan, TorrentHandle};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let content_dir = dir.join("multi");
        tokio::fs::create_dir_all(&content_dir).await.unwrap();
        write_payload(&content_dir.join("a.bin"), 4 * 1024 * 1024).await;
        write_payload(&content_dir.join("b.bin"), 4 * 1024 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&content_dir).await;
        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;

        handle
            .reconcile_file_priorities(TorrentFilePriorityPlan {
                active_file: Some(0),
                hot_file: None,
                generation: 1,
                reason: "test",
            })
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        while handle.handle.only_files() != Some(vec![0]) && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(handle.handle.only_files(), Some(vec![0]));
        assert!(!handle.deferred_selection().has_pending());
    }

    #[test]
    fn plan_only_files_rules() {
        use SelectionOp::*;
        // Nothing pinned: the pre-pin rules hold unchanged.
        let none = BTreeSet::new();
        let plan = |current: Option<&[usize]>, file_count: usize, op: SelectionOp| {
            plan_only_files(current, file_count, &none, op)
        };
        let set = |v: &[usize]| Some(v.iter().copied().collect::<HashSet<usize>>());

        // Single-file torrents: never touch selection, for any op.
        assert_eq!(plan(None, 1, Prepare(0)), None);
        assert_eq!(plan(Some(&[0]), 1, Clear(0)), None);
        assert_eq!(
            plan(
                None,
                0,
                Reconcile {
                    active: Some(0),
                    hot: None
                }
            ),
            None
        );

        // Prepare: exclusive selection.
        assert_eq!(plan(None, 3, Prepare(1)), set(&[1]));
        assert_eq!(plan(Some(&[0, 2]), 3, Prepare(1)), set(&[1]));
        // Prepare out of range: apply nothing.
        assert_eq!(plan(None, 3, Prepare(3)), None);

        // Clear: refuse to empty the set.
        assert_eq!(plan(Some(&[1]), 3, Clear(1)), None);
        // Clear of a stale file after a switch: no-op.
        assert_eq!(plan(Some(&[0]), 3, Clear(1)), None);
        // Clear with no selection at all: no-op.
        assert_eq!(plan(None, 3, Clear(1)), None);
        // Clear leaving a non-empty remainder applies it.
        assert_eq!(plan(Some(&[0, 1]), 3, Clear(1)), set(&[0]));

        // Reconcile: union of active and hot, never empty.
        assert_eq!(
            plan(
                Some(&[1]),
                3,
                Reconcile {
                    active: Some(0),
                    hot: Some(2)
                }
            ),
            set(&[0, 2])
        );
        assert_eq!(
            plan(
                Some(&[1]),
                3,
                Reconcile {
                    active: None,
                    hot: None
                }
            ),
            None
        );
        // Out-of-range indices are dropped; empty result applies nothing.
        assert_eq!(
            plan(
                None,
                3,
                Reconcile {
                    active: Some(9),
                    hot: Some(1)
                }
            ),
            set(&[1])
        );
        assert_eq!(
            plan(
                None,
                3,
                Reconcile {
                    active: Some(9),
                    hot: None
                }
            ),
            None
        );
        // Pin adds to the current selection; with nothing selected ("all
        // wanted") it narrows to just the pin. Out of range: nothing.
        assert_eq!(plan(Some(&[1]), 3, Pin(2)), set(&[1, 2]));
        assert_eq!(plan(None, 3, Pin(2)), set(&[2]));
        assert_eq!(plan(None, 3, Pin(3)), None);
    }

    /// The pinned set is unioned into every plan, so playback switching
    /// (exclusive `Prepare`, `Reconcile` to another file, `Clear` after a
    /// stream ends) never deselects an offline download.
    #[test]
    fn plan_only_files_unions_pinned_into_every_branch() {
        use SelectionOp::*;
        let pinned: BTreeSet<usize> = [0].into_iter().collect();
        let plan = |current: Option<&[usize]>, file_count: usize, op: SelectionOp| {
            plan_only_files(current, file_count, &pinned, op)
        };
        let set = |v: &[usize]| Some(v.iter().copied().collect::<HashSet<usize>>());

        // Single-file torrents: still never touched, pinned or not.
        assert_eq!(plan(None, 1, Prepare(0)), None);
        assert_eq!(plan(None, 1, Pin(0)), None);

        // Prepare is exclusive *plus* the pin.
        assert_eq!(plan(None, 3, Prepare(1)), set(&[0, 1]));
        assert_eq!(plan(Some(&[2]), 3, Prepare(0)), set(&[0]));

        // Clear never drops the pinned file, even when it is the only one
        // selected or a stale cleanup targets it.
        assert_eq!(plan(Some(&[0, 1]), 3, Clear(0)), None);
        assert_eq!(plan(Some(&[0]), 3, Clear(0)), None);
        // Clearing another file keeps the pin in the remainder.
        assert_eq!(plan(Some(&[0, 1]), 3, Clear(1)), set(&[0]));
        assert_eq!(plan(Some(&[1, 2]), 3, Clear(1)), set(&[0, 2]));

        // Reconcile chains the pin; with no active/hot file the pin alone
        // is the want-set instead of "apply nothing".
        assert_eq!(
            plan(
                Some(&[0]),
                3,
                Reconcile {
                    active: Some(2),
                    hot: Some(1)
                }
            ),
            set(&[0, 1, 2])
        );
        assert_eq!(
            plan(
                Some(&[1]),
                3,
                Reconcile {
                    active: None,
                    hot: None
                }
            ),
            set(&[0])
        );

        // Pin keeps the current selection and adds the new pin.
        let both: BTreeSet<usize> = [0, 2].into_iter().collect();
        assert_eq!(
            plan_only_files(Some(&[1]), 3, &both, Pin(2)),
            set(&[0, 1, 2])
        );

        // An out-of-range pinned index (metadata mismatch) is dropped, and
        // a plan that is empty apart from it is a no-op.
        let stale: BTreeSet<usize> = [7].into_iter().collect();
        assert_eq!(
            plan_only_files(Some(&[1]), 3, &stale, Prepare(2)),
            set(&[2])
        );
        assert_eq!(
            plan_only_files(
                None,
                3,
                &stale,
                Reconcile {
                    active: None,
                    hot: None
                }
            ),
            None
        );
    }

    /// End-to-end on a real multi-file torrent: a pinned file stays in
    /// librqbit's `only_files` through the whole playback lifecycle of
    /// another file, and leaves it only after an unpin plus a reconcile.
    /// `stats()` reports the pin per file and torrent-wide.
    #[tokio::test(flavor = "multi_thread")]
    async fn pinned_file_survives_playback_switching_on_real_torrent() {
        use crate::backend::{TorrentFilePriorityPlan, TorrentHandle};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let content_dir = dir.join("multi");
        tokio::fs::create_dir_all(&content_dir).await.unwrap();
        write_payload(&content_dir.join("a.bin"), 48 * 1024).await;
        write_payload(&content_dir.join("b.bin"), 64 * 1024).await;
        write_payload(&content_dir.join("c.bin"), 32 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&content_dir).await;

        let (backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let selection = |h: &LibrqbitHandle| {
            let mut v = h.handle.only_files().unwrap_or_default();
            v.sort_unstable();
            v
        };
        let reconcile = |h: LibrqbitHandle, active: Option<usize>| async move {
            h.reconcile_file_priorities(TorrentFilePriorityPlan {
                active_file: active,
                hot_file: None,
                generation: 1,
                reason: "test",
            })
            .await
            .unwrap();
        };

        // Pin narrows "everything wanted" to the pin.
        handle.pin_file(0).await.unwrap();
        assert_eq!(selection(&handle), vec![0]);
        // Idempotent.
        handle.pin_file(0).await.unwrap();
        assert_eq!(selection(&handle), vec![0]);

        // Playback of another file: exclusive prepare keeps the pin ...
        handle.prepare_file_for_streaming(1).await.unwrap();
        assert_eq!(selection(&handle), vec![0, 1]);
        // ... reconcile to yet another file keeps it ...
        reconcile(handle.clone(), Some(2)).await;
        assert_eq!(selection(&handle), vec![0, 2]);
        // ... and clearing the pinned file is refused.
        handle.clear_file_streaming(0).await.unwrap();
        assert_eq!(selection(&handle), vec![0, 2]);

        // A handle re-created by get_torrent shares the pin set.
        let again = backend.get_torrent(&handle.info_hash).await.unwrap();
        let stats = TorrentHandle::stats(&again).await;
        assert_eq!(stats.pinned_files, vec![0]);
        assert!(stats.files[0].pinned);
        assert!(!stats.files[1].pinned && !stats.files[2].pinned);
        // Seeded torrent: every file is complete.
        assert!(stats.files.iter().all(|f| f.complete), "{:?}", stats.files);

        // Unpin alone leaves the selection; the next reconcile drops it.
        again.unpin_file(0).await.unwrap();
        assert_eq!(selection(&handle), vec![0, 2]);
        assert!(TorrentHandle::stats(&handle).await.pinned_files.is_empty());
        reconcile(handle.clone(), Some(2)).await;
        assert_eq!(selection(&handle), vec![2]);

        // Out-of-range pin is a structural error and records nothing.
        assert!(handle.pin_file(3).await.is_err());
        assert!(TorrentHandle::stats(&handle).await.pinned_files.is_empty());
    }

    /// `complete` follows the per-file progress: nothing on disk means no
    /// file is complete (and none is pinned by default).
    #[tokio::test]
    async fn stats_report_incomplete_unpinned_files_for_unseeded_torrent() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let src_dir = tmp.path().join("src");
        tokio::fs::create_dir_all(&src_dir).await.unwrap();
        write_payload(&src_dir.join("payload.bin"), 64 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&src_dir.join("payload.bin")).await;
        let (_backend, handle) = backend_with_torrent(&tmp.path().join("dl"), &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let stats = TorrentHandle::stats(&handle).await;
        assert!(!stats.files[0].complete);
        assert!(!stats.files[0].pinned);
        assert!(stats.pinned_files.is_empty());
        let json = serde_json::to_value(&stats).unwrap();
        assert_eq!(json["pinnedFiles"], serde_json::json!([]));
        assert_eq!(json["files"][0]["pinned"], false);
        assert_eq!(json["files"][0]["complete"], false);
    }

    /// A pin issued while the torrent is still hash-checking is parked on
    /// the deferred-selection path and applied once librqbit accepts
    /// selection updates, so a "download" pressed during startup sticks.
    #[tokio::test(flavor = "multi_thread")]
    async fn pin_file_defers_while_initializing() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let content_dir = dir.join("multi");
        tokio::fs::create_dir_all(&content_dir).await.unwrap();
        write_payload(&content_dir.join("a.bin"), 4 * 1024 * 1024).await;
        write_payload(&content_dir.join("b.bin"), 4 * 1024 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&content_dir).await;

        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        let was_initializing = handle.is_initializing();
        handle.pin_file(1).await.unwrap();
        // Recorded immediately, applied once initialized.
        assert_eq!(TorrentHandle::stats(&handle).await.pinned_files, vec![1]);
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        while handle.handle.only_files() != Some(vec![1]) && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(handle.handle.only_files(), Some(vec![1]));
        assert!(!handle.deferred_selection().has_pending());
        assert!(
            was_initializing,
            "test must exercise the initializing gate, not a torrent that was already ready"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multifile_selection_lifecycle() {
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        use crate::backend::{TorrentFilePriorityPlan, TorrentHandle};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        // Multi-file torrents land in <download_dir>/<torrent name>/, so seed
        // the payloads exactly there by creating the torrent from that dir.
        let content_dir = dir.join("multi");
        tokio::fs::create_dir_all(&content_dir).await.unwrap();
        write_payload(&content_dir.join("a.bin"), 48 * 1024).await;
        write_payload(&content_dir.join("b.bin"), 64 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&content_dir).await;

        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();
        assert_eq!(handle.file_count().await, 2);

        let selection = |h: &LibrqbitHandle| {
            let mut v = h.handle.only_files().unwrap_or_default();
            v.sort_unstable();
            v
        };

        // Prepare selects exclusively.
        handle.prepare_file_for_streaming(1).await.unwrap();
        assert_eq!(selection(&handle), vec![1]);

        // Clearing the only selected file would empty the set -> no-op.
        handle.clear_file_streaming(1).await.unwrap();
        assert_eq!(selection(&handle), vec![1]);

        // Reconcile switches to the active file.
        handle
            .reconcile_file_priorities(TorrentFilePriorityPlan {
                active_file: Some(0),
                hot_file: None,
                generation: 1,
                reason: "test",
            })
            .await
            .unwrap();
        assert_eq!(selection(&handle), vec![0]);

        // Stale clear of a file that is no longer selected -> no-op.
        handle.clear_file_streaming(1).await.unwrap();
        assert_eq!(selection(&handle), vec![0]);

        // Gating must not starve the selected, streamed file.
        let r = handle
            .wait_for_piece_ready(
                0,
                0,
                TEST_WAIT_BOUND,
                PlaybackIntent::DirectInitial,
                BufferProfile::Normal,
            )
            .await
            .unwrap();
        assert!(r.ready, "selected file must stay readable: {}", r.reason);

        // Out-of-range prepare is a structural error.
        assert!(handle.prepare_file_for_streaming(2).await.is_err());
    }

    #[tokio::test]
    async fn single_file_torrent_selection_is_untouched() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 32 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;
        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        handle.prepare_file_for_streaming(0).await.unwrap();
        handle.clear_file_streaming(0).await.unwrap();
        assert_eq!(handle.handle.only_files(), None);
    }

    /// `Session::delete(_, false)` leaves the output folder behind even
    /// when it is empty; `remove_torrent` cleans that up -- and only that:
    /// a folder with data in it and the session root itself stay.
    #[tokio::test]
    async fn remove_torrent_removes_empty_output_folder_only() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        tokio::fs::create_dir_all(&src).await.unwrap();
        write_payload(&src.join("a.bin"), 32 * 1024).await;
        write_payload(&src.join("b.bin"), 32 * 1024).await;
        let (multi_bytes, multi_hash) = make_torrent(&src).await;
        let single_payload = tmp.path().join("single.bin");
        write_payload(&single_payload, 16 * 1024).await;
        let (single_bytes, single_hash) = make_torrent(&single_payload).await;

        let dl = tmp.path().join("dl");
        let backend = LibrqbitBackend::new_for_tests(dl.clone())
            .await
            .expect("hermetic session");
        let multi = backend
            .add_torrent(TorrentSource::Bytes(multi_bytes.clone()), vec![])
            .await
            .unwrap();
        multi.handle.wait_until_initialized().await.unwrap();
        let single = backend
            .add_torrent(TorrentSource::Bytes(single_bytes), vec![])
            .await
            .unwrap();
        single.handle.wait_until_initialized().await.unwrap();
        assert_eq!(
            single.handle.output_folder(),
            dl,
            "single-file torrents write into the root"
        );
        let multi_dir = multi.handle.output_folder().to_path_buf();
        assert_eq!(multi_dir, dl.join("src"));
        assert!(multi_dir.is_dir());

        // Nothing downloaded: the multi-file torrent's folder is empty
        // (drop whatever librqbit pre-created) and goes with the torrent.
        tokio::fs::remove_dir_all(&multi_dir).await.unwrap();
        tokio::fs::create_dir_all(&multi_dir).await.unwrap();
        backend.remove_torrent(&multi_hash).await.unwrap();
        assert!(!multi_dir.exists(), "empty output folder must be removed");

        // The root is never removed, however empty, and a single-file
        // torrent's data survives in it.
        for entry in std::fs::read_dir(&dl).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                std::fs::remove_dir_all(&path).unwrap();
            } else {
                std::fs::remove_file(&path).unwrap();
            }
        }
        backend.remove_torrent(&single_hash).await.unwrap();
        assert!(dl.is_dir(), "session root must survive");

        // A folder that still holds data is left alone.
        tokio::fs::create_dir_all(&dl.join("src")).await.unwrap();
        write_payload(&dl.join("src").join("a.bin"), 32 * 1024).await;
        let multi = backend
            .add_torrent(TorrentSource::Bytes(multi_bytes), vec![])
            .await
            .unwrap();
        multi.handle.wait_until_initialized().await.unwrap();
        backend.remove_torrent(&multi_hash).await.unwrap();
        assert!(dl.join("src").join("a.bin").is_file());
    }

    /// `file_path` is the torrent's output folder joined with the file's
    /// relative name -- straight in the session root for a single-file
    /// torrent, under the torrent's own folder for a multi-file one -- and
    /// points at the real bytes. Out of range: None. `get_file_path` is the
    /// same path as a string.
    #[tokio::test]
    async fn file_path_points_at_the_file_on_disk() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();

        let payload = dir.join("payload.bin");
        write_payload(&payload, 32 * 1024).await;
        let (single_bytes, _) = make_torrent(&payload).await;
        let content_dir = dir.join("multi");
        tokio::fs::create_dir_all(&content_dir).await.unwrap();
        write_payload(&content_dir.join("a.bin"), 16 * 1024).await;
        write_payload(&content_dir.join("b.bin"), 24 * 1024).await;
        let (multi_bytes, _) = make_torrent(&content_dir).await;
        // The torrent's file order is the filesystem's readdir order, not
        // the order the fixture wrote the files in: look every index up.
        let a = torrent_file_index(&multi_bytes, "a.bin");
        let b = torrent_file_index(&multi_bytes, "b.bin");

        let backend = LibrqbitBackend::new_for_tests(dir.clone())
            .await
            .expect("hermetic session");
        let single = backend
            .add_torrent(TorrentSource::Bytes(single_bytes), vec![])
            .await
            .unwrap();
        let multi = backend
            .add_torrent(TorrentSource::Bytes(multi_bytes), vec![])
            .await
            .unwrap();

        let path = single.file_path(0).await.expect("single-file path");
        assert_eq!(path, dir.join("payload.bin"));
        assert_eq!(
            single.get_file_path(0).await.as_deref(),
            Some(path.to_string_lossy().as_ref())
        );
        let bytes = tokio::fs::read(&path).await.expect("path exists on disk");
        assert_eq!(bytes.len(), 32 * 1024);
        assert!(bytes.iter().enumerate().all(|(i, b)| *b == (i % 251) as u8));
        assert_eq!(single.file_path(1).await, None, "out of range");

        assert_eq!(
            multi.file_path(b).await.as_deref(),
            Some(content_dir.join("b.bin").as_path())
        );
        assert_eq!(
            tokio::fs::metadata(multi.file_path(a).await.unwrap())
                .await
                .unwrap()
                .len(),
            16 * 1024
        );
        assert_eq!(multi.file_path(2).await, None);
    }

    /// `add_torrent_placed` hands librqbit the placement: the torrent's
    /// files live in exactly `output_folder` (no name sub-folder), only the
    /// listed files are wanted, `output_folder()` reports the folder, and
    /// data already present there is picked up by the hash check
    /// (`overwrite: true`). An out-of-range `only_files` index is refused
    /// at add time.
    /// Storage that opens without complaint and then fails the initial
    /// check -- how librqbit actually reaches its Error state, since a
    /// storage that cannot be opened at all fails the add itself instead.
    struct BrokenStorage(String);

    impl librqbit::storage::TorrentStorage for BrokenStorage {
        fn init(
            &mut self,
            _shared: &librqbit::ManagedTorrentShared,
            _metadata: &librqbit::TorrentMetadata,
        ) -> Result<()> {
            Ok(())
        }
        fn pread_exact(&self, _file_id: usize, _offset: u64, _buf: &mut [u8]) -> Result<()> {
            anyhow::bail!("{}", self.0)
        }
        fn pwrite_all(&self, _file_id: usize, _offset: u64, _buf: &[u8]) -> Result<()> {
            Ok(())
        }
        fn remove_file(&self, _file_id: usize, _filename: &std::path::Path) -> Result<()> {
            Ok(())
        }
        fn remove_directory_if_empty(&self, _path: &std::path::Path) -> Result<()> {
            Ok(())
        }
        fn ensure_file_length(&self, _file_id: usize, _length: u64) -> Result<()> {
            Ok(())
        }
        fn take(&self) -> Result<Box<dyn librqbit::storage::TorrentStorage>> {
            anyhow::bail!("{}", self.0)
        }
    }

    #[derive(Clone)]
    struct BrokenStorageFactory(String);

    impl librqbit::storage::StorageFactory for BrokenStorageFactory {
        type Storage = BrokenStorage;
        fn create(
            &self,
            _shared: &librqbit::ManagedTorrentShared,
            _metadata: &librqbit::TorrentMetadata,
        ) -> Result<Self::Storage> {
            Ok(BrokenStorage(self.0.clone()))
        }
        fn clone_box(&self) -> librqbit::storage::BoxStorageFactory {
            use librqbit::storage::StorageFactoryExt;
            self.clone().boxed()
        }
    }

    /// librqbit says WHY a torrent it put in the Error state is stuck
    /// (`TorrentStats.error`), and that reason is the `{e:?}` of an anyhow
    /// chain naming absolute cache and download paths. The client gets a
    /// fixed message instead -- non-empty, so the download screen can say
    /// more than "error", and path-free; the chain goes to the log alone,
    /// and only once per distinct error, since statistics are polled for
    /// as long as the broken download is on screen.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_torrent_error_reaches_the_client_without_the_server_paths() {
        use crate::backend::TorrentHandle;
        use librqbit::storage::StorageFactoryExt;
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        tokio::fs::create_dir_all(&src).await.unwrap();
        write_payload(&src.join("a.bin"), 32 * 1024).await;
        let (bytes, hash) = make_torrent(&src).await;

        let backend = LibrqbitBackend::new_for_tests(tmp.path().join("dl"))
            .await
            .expect("hermetic session");
        // The shape of a real librqbit storage failure: an anyhow chain
        // naming the absolute path it could not use.
        let librqbit_error = format!(
            "error opening {:?} in read/write mode",
            tmp.path().join("dl").join("src").join("a.bin")
        );
        // The server path as the error renders it. `Debug` on a path escapes
        // the separator, so on Windows the chain holds `C:\\dir\\dl` where
        // `Path::to_str` would give `C:\dir\dl`: match the formatting the
        // error itself used rather than the raw path, or the check passes
        // vacuously on one platform and fails on the other.
        let server_path = format!("{:?}", tmp.path());
        let server_path = server_path.trim_matches('"');
        assert!(
            librqbit_error.contains(server_path),
            "the fixture error names the server path: {librqbit_error}"
        );
        let response = backend
            .session
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes),
                Some(librqbit::AddTorrentOptions {
                    storage_factory: Some(BrokenStorageFactory(librqbit_error.clone()).boxed()),
                    ..Default::default()
                }),
            )
            .await
            .expect("the add succeeds; the check is what fails");
        let (librqbit::AddTorrentResponse::Added(_, managed)
        | librqbit::AddTorrentResponse::AlreadyManaged(_, managed)) = response
        else {
            panic!("torrent not added");
        };
        let handle = LibrqbitHandle {
            handle: managed,
            info_hash: hash.clone(),
            session: backend.session.clone(),
            deferred_selections: backend.deferred_selections.clone(),
            pinned_files: backend.pinned_files.clone(),
            reported_errors: backend.reported_errors.clone(),
            stream_positions: backend.stream_positions.clone(),
            swarm_scraper: backend.swarm_scraper.clone(),
        };

        let mut stats = handle.stats().await;
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        while stats.phase != StartupPhase::Error && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
            stats = handle.stats().await;
        }
        assert_eq!(stats.phase, StartupPhase::Error, "{:?}", stats.phase);
        let reported = stats.error.expect("the reason reaches the client");
        assert!(!reported.is_empty());
        assert!(
            !reported.contains(server_path)
                && !reported.contains(tmp.path().to_str().unwrap())
                && !reported.contains("a.bin")
                && !reported.contains(&hash),
            "no server path leaks into the response: {reported}"
        );
        assert!(
            handle
                .reported_errors
                .lock()
                .get(&hash)
                .is_some_and(|logged| logged.contains(server_path)),
            "the full chain is kept for the log, and logged once"
        );

        // Recovered: the record goes, so the next error is logged again.
        assert_eq!(handle.client_torrent_error(None), None);
        assert!(handle.reported_errors.lock().is_empty());
    }

    #[tokio::test]
    async fn add_torrent_placed_uses_the_folder_and_want_set() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        tokio::fs::create_dir_all(&src).await.unwrap();
        write_payload(&src.join("a.bin"), 32 * 1024).await;
        write_payload(&src.join("b.bin"), 48 * 1024).await;
        let (bytes, hash) = make_torrent(&src).await;
        // The torrent's file order is the filesystem's readdir order, not
        // the order the fixture wrote the files in: look every index up.
        // (Both files are whole 16 KiB pieces, so neither order makes them
        // share a boundary piece.)
        let a = torrent_file_index(&bytes, "a.bin");
        let b = torrent_file_index(&bytes, "b.bin");

        let dl = tmp.path().join("dl");
        let backend = LibrqbitBackend::new_for_tests(dl.clone())
            .await
            .expect("hermetic session");
        let folder = tmp.path().join("offline").join(&hash);
        tokio::fs::create_dir_all(&folder).await.unwrap();
        // Pre-seed the wanted file where the placement points.
        tokio::fs::copy(src.join("b.bin"), folder.join("b.bin"))
            .await
            .unwrap();

        let handle = backend
            .add_torrent_placed(
                TorrentSource::Bytes(bytes.clone()),
                vec![],
                TorrentPlacement {
                    output_folder: Some(folder.clone()),
                    only_files: Some(vec![b]),
                },
            )
            .await
            .expect("add with placement");
        assert_eq!(handle.output_folder(), Some(folder.clone()));
        assert_eq!(handle.handle.only_files(), Some(vec![b]));
        assert_eq!(
            handle.file_path(b).await.as_deref(),
            Some(folder.join("b.bin").as_path())
        );
        handle.handle.wait_until_initialized().await.unwrap();
        let stats = handle.stats().await;
        assert!(
            stats.files[b].complete,
            "pre-seeded file verified: {stats:?}"
        );
        assert!(!stats.files[a].complete);
        assert!(
            !dl.join("src").exists(),
            "nothing lands in the session root"
        );

        backend.remove_torrent(&hash).await.unwrap();
        let err = match backend
            .add_torrent_placed(
                TorrentSource::Bytes(bytes),
                vec![],
                TorrentPlacement {
                    output_folder: Some(folder),
                    only_files: Some(vec![2]),
                },
            )
            .await
        {
            Ok(_) => panic!("out-of-range only_files must be refused"),
            Err(err) => err,
        };
        assert!(format!("{err:#}").contains("out of range"), "{err:#}");
    }

    /// `relocate_torrent` moves a torrent's files -- multi-file: out of its
    /// `<root>/<name>` folder (which goes once empty); single-file: out of
    /// the root itself -- into the placement's folder, re-adds it there
    /// wanting the placement's files, and librqbit's re-check finds the
    /// moved data complete. Already in place: same handle, nothing moved.
    #[tokio::test]
    async fn relocate_torrent_moves_the_data_and_rechecks_it_in_place() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let dl = tmp.path().join("dl");
        // Seed both torrents in the session root as if streamed there.
        let multi_src = dl.join("show");
        tokio::fs::create_dir_all(&multi_src).await.unwrap();
        write_payload(&multi_src.join("e1.bin"), 40 * 1024).await;
        write_payload(&multi_src.join("e2.bin"), 24 * 1024).await;
        let (multi_bytes, multi_hash) = make_torrent(&multi_src).await;
        // The torrent's file order is the filesystem's readdir order, not
        // the order the fixture wrote the files in: look every index up.
        let e1 = torrent_file_index(&multi_bytes, "e1.bin");
        let e2 = torrent_file_index(&multi_bytes, "e2.bin");
        let single_src = dl.join("movie.bin");
        write_payload(&single_src, 20 * 1024).await;
        let (single_bytes, single_hash) = make_torrent(&single_src).await;

        let backend = LibrqbitBackend::new_for_tests(dl.clone())
            .await
            .expect("hermetic session");
        let multi = backend
            .add_torrent(TorrentSource::Bytes(multi_bytes), vec![])
            .await
            .unwrap();
        multi.handle.wait_until_initialized().await.unwrap();
        assert_eq!(multi.output_folder(), Some(multi_src.clone()));
        let single = backend
            .add_torrent(TorrentSource::Bytes(single_bytes), vec![])
            .await
            .unwrap();
        single.handle.wait_until_initialized().await.unwrap();
        assert!(multi.stats().await.files.iter().all(|f| f.complete));

        let offline = tmp.path().join("offline");
        let multi_target = offline.join(&multi_hash);
        let moved = backend
            .relocate_torrent(
                &multi_hash,
                TorrentPlacement {
                    output_folder: Some(multi_target.clone()),
                    only_files: Some(vec![e2]),
                },
                vec![],
            )
            .await
            .expect("relocate multi-file torrent");
        assert_eq!(moved.output_folder(), Some(multi_target.clone()));
        assert_eq!(moved.handle.only_files(), Some(vec![e2]));
        assert!(multi_target.join("e1.bin").is_file());
        assert!(multi_target.join("e2.bin").is_file());
        assert!(!multi_src.exists(), "emptied source folder is removed");
        moved.handle.wait_until_initialized().await.unwrap();
        let stats = moved.stats().await;
        assert!(
            stats.files.iter().all(|f| f.complete),
            "moved data verified by the re-check: {stats:?}"
        );
        assert_eq!(
            moved.file_path(e2).await.as_deref(),
            Some(multi_target.join("e2.bin").as_path())
        );
        assert_eq!(backend.list_torrents().await.len(), 2);

        // Already there: same torrent, no re-add.
        let again = backend
            .relocate_torrent(
                &multi_hash,
                TorrentPlacement {
                    output_folder: Some(multi_target.clone()),
                    only_files: Some(vec![e1]),
                },
                vec![],
            )
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&again.handle, &moved.handle));

        let single_target = offline.join(&single_hash);
        let moved = backend
            .relocate_torrent(
                &single_hash,
                TorrentPlacement {
                    output_folder: Some(single_target.clone()),
                    only_files: None,
                },
                vec![],
            )
            .await
            .expect("relocate single-file torrent");
        assert!(single_target.join("movie.bin").is_file());
        assert!(!single_src.exists());
        assert!(dl.is_dir(), "the session root stays");
        moved.handle.wait_until_initialized().await.unwrap();
        assert!(moved.stats().await.files[0].complete);
        assert_eq!(
            moved.file_path(0).await.as_deref(),
            Some(single_target.join("movie.bin").as_path())
        );
    }

    /// librqbit pre-sizes every wanted file when a torrent goes live, so a
    /// torrent added in the root without data still has full-length
    /// placeholders there. Relocating it onto a folder that already holds
    /// the real bytes must keep those: the placeholder is dropped, the
    /// destination file stays and verifies complete.
    #[tokio::test]
    async fn relocate_torrent_keeps_a_file_already_at_the_destination() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("show");
        tokio::fs::create_dir_all(&src).await.unwrap();
        // Whole pieces per file, whichever order the torrent lists them
        // in: no boundary piece is shared with e1, whose data is absent.
        write_payload(&src.join("e1.bin"), 32 * 1024).await;
        write_payload(&src.join("e2.bin"), 16 * 1024).await;
        let (bytes, hash) = make_torrent(&src).await;
        // Readdir order decides the file indices -- look them up.
        let e1 = torrent_file_index(&bytes, "e1.bin");
        let e2 = torrent_file_index(&bytes, "e2.bin");

        let dl = tmp.path().join("dl");
        let backend = LibrqbitBackend::new_for_tests(dl.clone())
            .await
            .expect("hermetic session");
        // Added without data: the root folder gets empty placeholders.
        let handle = backend
            .add_torrent(TorrentSource::Bytes(bytes), vec![])
            .await
            .unwrap();
        handle.handle.wait_until_initialized().await.unwrap();
        let root_folder = dl.join("show");
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        while !root_folder.join("e2.bin").exists() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            root_folder.join("e2.bin").is_file(),
            "librqbit pre-sizes the files"
        );
        assert!(!handle.stats().await.files[e2].complete);

        // The destination already holds the real e2.bin.
        let target = tmp.path().join("offline").join(&hash);
        tokio::fs::create_dir_all(&target).await.unwrap();
        tokio::fs::copy(src.join("e2.bin"), target.join("e2.bin"))
            .await
            .unwrap();

        let moved = backend
            .relocate_torrent(
                &hash,
                TorrentPlacement {
                    output_folder: Some(target.clone()),
                    only_files: Some(vec![e2]),
                },
                vec![],
            )
            .await
            .expect("relocate");
        moved.handle.wait_until_initialized().await.unwrap();
        let stats = moved.stats().await;
        assert!(stats.files[e2].complete, "destination data kept: {stats:?}");
        assert!(!stats.files[e1].complete);
        assert!(!root_folder.exists(), "placeholders dropped, folder gone");
        let bytes = tokio::fs::read(target.join("e2.bin")).await.unwrap();
        assert!(bytes.iter().enumerate().all(|(i, b)| *b == (i % 251) as u8));
    }

    /// A file of the relocated torrent without a single verified byte is a
    /// pre-sized sparse placeholder (librqbit sizes every wanted file at
    /// init, and a plain add wants everything): it is dropped with the old
    /// folder, never moved -- a cross-device copy would write its whole
    /// nominal length as zeros into the destination. The file with data
    /// moves and verifies.
    #[tokio::test]
    async fn relocate_torrent_drops_empty_placeholders_instead_of_moving_them() {
        use crate::backend::TorrentHandle;
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("show");
        tokio::fs::create_dir_all(&src).await.unwrap();
        // Whole pieces per file, whichever order the torrent lists them
        // in: e1's verified pieces never spill into e2.
        write_payload(&src.join("e1.bin"), 32 * 1024).await;
        write_payload(&src.join("e2.bin"), 16 * 1024).await;
        let (bytes, hash) = make_torrent(&src).await;
        // Readdir order decides the file indices -- look them up.
        let e1 = torrent_file_index(&bytes, "e1.bin");
        let e2 = torrent_file_index(&bytes, "e2.bin");
        // Only e1's data is in the session root.
        tokio::fs::remove_file(src.join("e2.bin")).await.unwrap();

        let dl = tmp.path().join("dl");
        let root_folder = dl.join("show");
        tokio::fs::create_dir_all(&root_folder).await.unwrap();
        tokio::fs::rename(src.join("e1.bin"), root_folder.join("e1.bin"))
            .await
            .unwrap();
        let backend = LibrqbitBackend::new_for_tests(dl.clone())
            .await
            .expect("hermetic session");
        let handle = backend
            .add_torrent(TorrentSource::Bytes(bytes), vec![])
            .await
            .unwrap();
        handle.handle.wait_until_initialized().await.unwrap();
        let deadline = std::time::Instant::now() + TEST_WAIT_BOUND;
        while !root_folder.join("e2.bin").exists() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            root_folder.join("e2.bin").is_file(),
            "librqbit pre-sizes the unwanted-so-far file"
        );
        let stats = handle.stats().await;
        assert!(
            stats.files[e1].complete && !stats.files[e2].complete,
            "{stats:?}"
        );

        let target = tmp.path().join("offline").join(&hash);
        let moved = backend
            .relocate_torrent(
                &hash,
                TorrentPlacement {
                    output_folder: Some(target.clone()),
                    only_files: Some(vec![e1]),
                },
                vec![],
            )
            .await
            .expect("relocate");
        assert!(target.join("e1.bin").is_file(), "the data moved");
        // librqbit's storage opens (creates, empty) every file of the
        // re-added torrent, so the test is the length, not the existence.
        let e2_len = tokio::fs::metadata(target.join("e2.bin"))
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        assert_eq!(
            e2_len, 0,
            "the placeholder was not carried into the destination"
        );
        assert!(
            !root_folder.exists(),
            "placeholder dropped, old folder gone"
        );
        moved.handle.wait_until_initialized().await.unwrap();
        let stats = moved.stats().await;
        assert!(stats.files[e1].complete, "moved data verified: {stats:?}");
        assert!(!stats.files[e2].complete);
    }

    /// While a torrent is still Initializing there is no chunk tracker to
    /// ask, so a file's own allocation decides: a sparse placeholder has no
    /// blocks, a file with bytes written has.
    #[cfg(unix)]
    #[tokio::test]
    async fn has_data_to_move_falls_back_to_allocated_blocks() {
        let tmp = tempfile::tempdir().unwrap();
        let sparse = tmp.path().join("sparse.bin");
        std::fs::File::create(&sparse)
            .unwrap()
            .set_len(8 * 1024 * 1024)
            .unwrap();
        let written = tmp.path().join("written.bin");
        write_payload(&written, 64 * 1024).await;
        let sparse_meta = std::fs::metadata(&sparse).unwrap();
        let written_meta = std::fs::metadata(&written).unwrap();

        assert!(!has_data_to_move(None, &sparse_meta), "no blocks, no data");
        assert!(has_data_to_move(None, &written_meta));
        // Known have-bytes win over the allocation either way.
        assert!(has_data_to_move(Some(1), &sparse_meta));
        assert!(!has_data_to_move(Some(0), &written_meta));
    }

    /// `remove_torrent_and_files` on a torrent placed in its own folder
    /// takes the pre-sized files and the folder with it; `remove_torrent`
    /// keeps them.
    #[tokio::test]
    async fn remove_torrent_and_files_takes_the_placed_folder_with_it() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        tokio::fs::create_dir_all(&src).await.unwrap();
        write_payload(&src.join("movie.bin"), 20 * 1024).await;
        let (bytes, hash) = make_torrent(&src.join("movie.bin")).await;
        let dl = tmp.path().join("dl");
        let backend = LibrqbitBackend::new_for_tests(dl.clone())
            .await
            .expect("hermetic session");
        let place = |folder: &std::path::Path| TorrentPlacement {
            output_folder: Some(folder.to_path_buf()),
            only_files: Some(vec![0]),
        };

        let kept = tmp.path().join("offline").join("kept");
        let handle = backend
            .add_torrent_placed(TorrentSource::Bytes(bytes.clone()), vec![], place(&kept))
            .await
            .unwrap();
        handle.handle.wait_until_initialized().await.unwrap();
        assert!(kept.join("movie.bin").is_file(), "pre-sized placeholder");
        backend.remove_torrent(&hash).await.unwrap();
        assert!(
            kept.join("movie.bin").is_file(),
            "remove_torrent keeps files"
        );

        let gone = tmp.path().join("offline").join("gone");
        let handle = backend
            .add_torrent_placed(TorrentSource::Bytes(bytes), vec![], place(&gone))
            .await
            .unwrap();
        handle.handle.wait_until_initialized().await.unwrap();
        assert!(gone.join("movie.bin").is_file());
        backend.remove_torrent_and_files(&hash).await.unwrap();
        assert!(!gone.exists(), "files and folder removed: {gone:?}");
        assert!(backend.list_torrents().await.is_empty());
        assert!(dl.is_dir(), "session root untouched");
    }

    /// The cross-device fallback of `move_file` copies then removes the
    /// source, and leaves no partial target behind when the copy fails.
    #[tokio::test]
    async fn copy_then_remove_moves_the_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src.bin");
        let dst = tmp.path().join("nested").join("dst.bin");
        write_payload(&src, 8 * 1024).await;
        tokio::fs::create_dir_all(dst.parent().unwrap())
            .await
            .unwrap();
        super::copy_then_remove(&src, &dst).await.unwrap();
        assert!(!src.exists());
        let bytes = tokio::fs::read(&dst).await.unwrap();
        assert_eq!(bytes.len(), 8 * 1024);
        assert!(bytes.iter().enumerate().all(|(i, b)| *b == (i % 251) as u8));

        let missing = tmp.path().join("missing.bin");
        let target = tmp.path().join("partial.bin");
        assert!(super::copy_then_remove(&missing, &target).await.is_err());
        assert!(!target.exists());
        assert!(
            super::move_file(&dst, &tmp.path().join("back.bin"))
                .await
                .is_ok()
        );
        assert!(!dst.exists());
    }

    #[tokio::test]
    async fn remove_torrent_drops_cached_torrent_file() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 32 * 1024).await;
        let (torrent_bytes, hash) = make_torrent(&payload).await;

        let (backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let cache_dir = dir.join(".cache");
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        let cached = cache_dir.join(format!("{hash}.torrent"));
        tokio::fs::write(&cached, &torrent_bytes).await.unwrap();

        backend.remove_torrent(&hash).await.unwrap();
        assert!(!cached.exists(), "cached .torrent should be removed");
        // Data files are kept (delete_files=false).
        assert!(payload.exists(), "payload must survive remove_torrent");
    }
}
