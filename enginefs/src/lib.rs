use crate::engine::Engine;
use anyhow::{Context, Result};
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::AbortHandle;
use tracing::debug;

pub mod backend;
pub mod cache;
pub mod chunk_store;
pub mod disk_cache;
pub mod engine;
pub mod files;
pub mod http_client;
pub mod metadata_cache;
pub mod metadata_pins;
pub mod piece_cache;
pub mod piece_store;
pub mod piece_waiter;
pub mod reconcile;
pub mod retention;
pub mod scrape;
pub mod tracker_prober;
pub mod trackers;
pub mod traffic;

// Re-export TrackerStorage for use by server crate
pub use http_client::http_client_builder;
pub use trackers::TrackerStorage;

use crate::backend::librqbit::LibrqbitBackend;
use crate::backend::priorities::EngineCacheConfig;

use crate::backend::{
    BackendMemoryDiagnostics, Footprint, HotFilePriorityPlan, RunState, TorrentBackend,
    TorrentFilePriorityPlan, TorrentHandle, TorrentListenPort, TorrentPlacement, TorrentSource,
};

const INACTIVE_TORRENT_REMOVE_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes
/// Free space the cache is kept out of on the volume the torrents write to.
///
/// One number, three readers, and it is one number so they cannot drift:
/// the server's stream route refuses to start a stream that would write to
/// disk with less than this free; its cache cleaner evicts the cache back
/// to it; and the engine's reconciler stops a torrent that is writing when
/// the volume falls under it ([`reconcile::desired`]'s free-space arm). The
/// third is what makes the other two hold. librqbit's storage writes the
/// whole file it wants and stops only at ENOSPC, which it treats as a fatal
/// torrent error -- so without the reconciler a torrent larger than the free
/// space ran the volume to zero between two cleaner passes (40 s at full
/// speed on the television that prompted this), and with the volume at
/// zero every other stream and the OS around them failed too. It looks
/// every [`FREE_SPACE_WATCH_INTERVAL`], so a torrent can overshoot the
/// floor by that long of writing; the floor is sized to absorb it.
///
/// Offline downloads are the fourth writer and keep their own margin,
/// [`PIN_FREE_SPACE_MARGIN`], checked once when a pin is accepted; a pin
/// can therefore settle the volume under this line by design, and the
/// reconciler stops it there like anything else.
pub const CACHE_FREE_SPACE_FLOOR: u64 = 512 * 1024 * 1024;
/// How often the reconciler reads the volume. One `statvfs` per tick, since
/// there is one volume -- the piece store's -- microseconds, so it can
/// afford to be short, and it has to be: a torrent at 20 MB/s writes 40 MB
/// per tick past the floor before anything sees it.
/// [`reconcile::RECONCILE_INTERVAL`] is an alias of this, so the pass and
/// the reading cannot drift apart.
pub const FREE_SPACE_WATCH_INTERVAL: Duration = Duration::from_secs(2);
/// A stopped torrent is started again only once the volume has this much
/// *over* the floor. Without the hysteresis a torrent started at the floor
/// writes a few MB, is stopped again, and flaps: each stop drops its peers
/// and each start re-announces. The band's memory is the torrent's own run
/// state and nothing else -- see [`reconcile::line`].
///
/// [`reconcile::line`]: reconcile
pub const FREE_SPACE_RESUME_MARGIN: u64 = 64 * 1024 * 1024;
/// How long the volume a stopped torrent writes to may stay short with that
/// torrent's readers parked before they are failed
/// (`Engine::refuse_reads_for_space`). The cache cleaner normally settles it
/// well inside this -- the stop notifies it, and its pass either makes room
/// (and the next reconcile starts the torrent) or evicts it -- so a reader
/// sees a buffering blip, not a failure. This is the bound for a server
/// whose cleaner is off or stuck: a parked read that nothing will complete
/// is a player spinning for ever.
///
/// Counted per volume rather than per torrent, because what decides whether
/// a parked read has anything coming is the disk, not when this particular
/// torrent happened to be stopped on it.
pub const STOPPED_READ_STALL_BOUND: Duration = Duration::from_secs(20);
/// How long after the cache cleaner has evicted a torrent stopped for space
/// ([`BackendEngineFS::evict_stopped_torrent`]) a request for the same hash
/// is refused rather than re-added. A player whose body was just failed
/// reconnects within a second and stremio-core's stats poll asks about the
/// hash every second; either would re-add the torrent and refill the disk
/// the cleaner has just emptied, in a loop nobody asked for. A user who
/// sees the error and presses play again arrives later than this.
pub const EVICTED_FOR_SPACE_RETRY_AFTER: Duration = Duration::from_secs(30);
/// Free space that must remain on the download volume after a pinned file's
/// missing bytes are written; `pin_download` refuses below it
/// ([`PinDownloadError::InsufficientSpace`]). Re-pinning a complete file
/// needs nothing and is never refused.
pub const PIN_FREE_SPACE_MARGIN: u64 = 500 * 1024 * 1024;
/// Where the pin set is persisted, relative to the download dir (see
/// `BackendEngineFS::pinned_downloads_path`).
const PINNED_DOWNLOADS_FILE: &str = "pinned-downloads.json";

/// Serialize `pins` to `path` through a uniquely named temp file in the
/// same directory and a rename, so a crash leaves the old file intact and
/// concurrent writers never see each other's temp file.
async fn write_pinned_downloads(
    path: &std::path::Path,
    pins: &BTreeMap<String, Vec<usize>>,
) -> Result<()> {
    static NEXT_TMP: AtomicU64 = AtomicU64::new(0);
    let json = serde_json::to_vec_pretty(pins)?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension(format!(
        "json.tmp-{}-{}",
        std::process::id(),
        NEXT_TMP.fetch_add(1, Ordering::Relaxed)
    ));
    tokio::fs::write(&tmp, json).await?;
    if let Err(error) = tokio::fs::rename(&tmp, path).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(error.into());
    }
    Ok(())
}
/// How long a magnet add may spend resolving metadata inside the backend
/// before it is given up on. librqbit's `Session::add_torrent` has no timeout
/// of its own, so without this an unresolvable magnet (no peers, dead
/// trackers) would keep its add task and registry entry forever and every
/// waiter would hang. 90 s is well past what a peer-less swarm needs to prove
/// itself and short enough that a player still gets an answer. Must stay below
/// `INACTIVE_TORRENT_REMOVE_TIMEOUT`: a `get_or_add_magnet` waiter polls the
/// registry once and then waits at most this long, so its entry can never be
/// swept as idle while it is still waiting.
pub const METADATA_RESOLVE_TIMEOUT: Duration = Duration::from_secs(90);
const _: () = assert!(
    METADATA_RESOLVE_TIMEOUT.as_secs() < INACTIVE_TORRENT_REMOVE_TIMEOUT.as_secs(),
    "a waiting magnet add must time out before it can be swept as idle"
);
/// How long a torrent must be quiet before the idle arm of
/// [`crate::reconcile::desired`] stops it (with seeding off).
///
/// Public because it is the wait an end-to-end test of that arm has to sit
/// out: `server`'s embed suite drives the whole loop through a running
/// server and cannot ask the ladder anything, so it measures its own waits
/// from here rather than from a copy of the number.
pub const INACTIVE_TORRENT_PAUSE_GRACE: Duration = Duration::from_secs(15);

/// How long after the reconciler last moved a torrent its *timer* will
/// leave it stopped -- the anti-flap dwell, applied in
/// [`BackendEngineFS::start_if_stopped`] and nowhere else.
///
/// The ladder's own lines already carry most of the hysteresis: the
/// free-space arm measures a stopped torrent against a higher line than a
/// running one, and the idle arm wants [`INACTIVE_TORRENT_PAUSE_GRACE`] of
/// quiet. What is left over is an input that moves for reasons of its own
/// around one of those lines -- a lease that expires and is renewed, a
/// metadata slot that is briefly unreadable -- and each crossing of it
/// costs a swarm: a stop drops every peer and a start re-announces to
/// trackers that enforce a minimum announce interval.
///
/// A whole [`INACTIVE_TORRENT_PAUSE_GRACE`], reusing that number rather
/// than inventing a second one, because it is the same judgement: how long
/// this server waits before believing that a torrent's activity has really
/// changed. Neither a stop nor a playback start goes through it -- see
/// [`BackendEngineFS::start_if_stopped`] for why each is exempt.
pub(crate) const RECONCILE_MIN_DWELL: Duration = INACTIVE_TORRENT_PAUSE_GRACE;

/// Instance-relative clock for the idle bookkeeping (engine `last_accessed`,
/// magnet-add polls). Seconds since the owning
/// [`BackendEngineFS`] was created, measured with a `tokio::time::Instant` so
/// it follows paused/advanced time under `#[tokio::test(start_paused = true)]`
/// (it is the std clock otherwise). One epoch per instance rather than a
/// process-global one: every test runtime has its own paused clock, and a
/// global `Instant` captured under one of them would be meaningless under the
/// next.
#[derive(Clone, Copy, Debug)]
pub struct Clock {
    epoch: tokio::time::Instant,
}

impl Clock {
    fn start() -> Self {
        Self {
            epoch: tokio::time::Instant::now(),
        }
    }

    pub fn now_secs(&self) -> u64 {
        self.epoch.elapsed().as_secs()
    }
}

type EngineRegistry<H> = Arc<RwLock<HashMap<String, Arc<Engine<H>>>>>;

/// Why a shared magnet add ended without an engine. `Clone` (the backend
/// error is `Arc`-wrapped) so it can be handed to every waiter of the shared
/// add and kept as the add's failure record.
#[derive(Debug, Clone, thiserror::Error)]
pub enum MagnetAddError {
    /// The backend did not return within [`METADATA_RESOLVE_TIMEOUT`]: no
    /// peer supplied the info dictionary in time. Routes map this to 504.
    #[error("metadata for {info_hash} did not resolve within {}s", .timeout.as_secs())]
    MetadataTimeout {
        info_hash: String,
        timeout: Duration,
    },
    /// The add task was aborted (its registry entry was swept as idle).
    #[error("magnet add for {info_hash} was cancelled")]
    Cancelled { info_hash: String },
    /// The add task ended abnormally before the backend answered: it panicked
    /// (debug builds only -- the release profile's `panic = "abort"` takes the
    /// whole process down instead of unwinding).
    #[error("magnet add task for {info_hash} failed: {reason}")]
    TaskFailed { info_hash: String, reason: String },
    /// The backend's `add_torrent` itself failed.
    #[error("{error:#}")]
    Backend {
        info_hash: String,
        error: Arc<anyhow::Error>,
    },
    /// The torrent was stopped for want of disk space and the cache cleaner
    /// evicted it whole -- torrent and partial download -- because nothing
    /// else on the volume could go ([`BackendEngineFS::evict_stopped_torrent`]).
    /// Until `retry_after_secs` on the engine's clock, a request for the
    /// hash gets this instead of a fresh add; see
    /// [`EVICTED_FOR_SPACE_RETRY_AFTER`]. Routes map it to 507.
    #[error(
        "torrent {info_hash} was stopped for want of disk space and its partial download evicted"
    )]
    EvictedForSpace {
        info_hash: String,
        retry_after_secs: u64,
    },
}

impl MagnetAddError {
    /// What an HTTP client may be told about this failure. The timeout
    /// message is its own `Display` (it names only the info hash and the
    /// bound); every other variant collapses to a fixed string, because the
    /// backend error chain can carry absolute download-dir paths and a task
    /// failure the panic payload. Those belong in the server log -- log the
    /// error itself (`%error`) at the call site -- never in a response body.
    pub fn client_message(&self) -> String {
        match self {
            Self::MetadataTimeout { .. } => self.to_string(),
            Self::Backend { .. } | Self::TaskFailed { .. } | Self::Cancelled { .. } => {
                "backend refused the torrent; see server logs".to_string()
            }
            Self::EvictedForSpace { .. } => {
                "the torrent was stopped for want of disk space and its partial download evicted; \
                 free some space and retry"
                    .to_string()
            }
        }
    }

    /// Whether a blocking lookup at `now_secs` may retry the add this error
    /// ended: always, except inside the cooling-off period of an eviction
    /// for space.
    fn may_retry_at(&self, now_secs: u64) -> bool {
        match self {
            Self::EvictedForSpace {
                retry_after_secs, ..
            } => now_secs >= *retry_after_secs,
            _ => true,
        }
    }
}

/// Outcome of a magnet add shared between every waiter.
pub type MagnetAddResult<H> = Result<Arc<Engine<H>>, MagnetAddError>;

/// Why [`BackendEngineFS::pin_download`] could not pin a file.
#[derive(Debug, thiserror::Error)]
pub enum PinDownloadError {
    /// The engine could not be created (metadata timeout, backend refusal).
    #[error(transparent)]
    MagnetAdd(#[from] MagnetAddError),
    /// The torrent has no such file.
    #[error("file index {file_idx} out of range ({file_count} files)")]
    FileNotFound { file_idx: usize, file_count: usize },
    /// The volume the pieces land on has less than the file's missing bytes
    /// plus [`PIN_FREE_SPACE_MARGIN`] available.
    #[error(
        "not enough free space for the download: {required} bytes needed (including a {margin} byte margin), {available} available"
    )]
    InsufficientSpace {
        required: u64,
        available: u64,
        margin: u64,
    },
    /// The backend refused the pin.
    #[error(transparent)]
    Backend(#[from] anyhow::Error),
}

impl PinDownloadError {
    /// What an HTTP handler may put in a response body. The space and
    /// file-index errors go verbatim (they carry nothing but numbers); the
    /// backend ones -- whose chains name absolute cache paths (librqbit's
    /// `error opening {path}`) -- become a generic sentence, and a failed
    /// magnet add
    /// defers to [`MagnetAddError::client_message`]. The full error is for
    /// the server log.
    pub fn client_message(&self) -> String {
        match self {
            Self::MagnetAdd(error) => error.client_message(),
            Self::FileNotFound { .. } | Self::InsufficientSpace { .. } => self.to_string(),
            Self::Backend(_) => "backend refused the download; see server logs".to_string(),
        }
    }
}

/// Whether `remaining` more bytes may be written to a volume with
/// `available` free bytes while keeping `margin` free. Nothing left to
/// write is always allowed (a complete file re-pinned on a full disk).
pub fn free_space_allows(available: u64, remaining: u64, margin: u64) -> bool {
    remaining == 0 || available >= remaining.saturating_add(margin)
}

/// `probe` applied to `path` or, failing that, its nearest existing ancestor
/// (the torrent's folder may not exist yet) -- how the free-space and
/// volume-id probes are asked about a folder. `Err` only when no ancestor
/// can be probed.
/// Free space a test has declared for a root and everything under it,
/// standing in for the volume probe -- see [`pretend_volume_space`].
type DeclaredVolumeSpace = parking_lot::Mutex<Vec<(std::path::PathBuf, u64)>>;
static DECLARED_VOLUME_SPACE: std::sync::OnceLock<DeclaredVolumeSpace> = std::sync::OnceLock::new();

/// Declare how much free space the volume under `root` has, for every probe
/// this crate's **default** free-space probe takes of a path below it from
/// now on.
///
/// A test seam and nothing else, and the same one `stream_server`'s
/// `pretend_available_space` is for the stream route's own probe: a volume
/// cannot be filled on demand, so without this the free-space arm of a
/// reconciler running inside a real server -- rather than one a unit test
/// drives by hand with `set_free_space_probe` -- could not be exercised at
/// all. Keyed by root so parallel tests, each with its own temp cache root,
/// never see each other's declaration.
#[doc(hidden)]
pub fn pretend_volume_space(root: impl Into<std::path::PathBuf>, bytes: u64) {
    let root = root.into();
    let mut declared = DECLARED_VOLUME_SPACE
        .get_or_init(|| parking_lot::Mutex::new(Vec::new()))
        .lock();
    declared.retain(|(declared, _)| *declared != root);
    declared.push((root, bytes));
}

/// What [`pretend_volume_space`] declared for `path`, if anything.
fn declared_volume_space(path: &std::path::Path) -> Option<u64> {
    let declared = DECLARED_VOLUME_SPACE.get()?.lock();
    declared
        .iter()
        .find(|(root, _)| path.starts_with(root))
        .map(|(_, bytes)| *bytes)
}

fn probe_at_existing_ancestor(
    probe: &(dyn Fn(&std::path::Path) -> std::io::Result<u64> + Send + Sync),
    path: &std::path::Path,
) -> std::io::Result<u64> {
    let mut candidate = Some(path);
    let mut last_error = None;
    while let Some(dir) = candidate {
        match probe(dir) {
            Ok(available) => return Ok(available),
            Err(e) => {
                last_error = Some(e);
                candidate = dir.parent();
            }
        }
    }
    Err(last_error.unwrap_or_else(|| std::io::Error::other("empty path")))
}

/// `Fn(path) -> u64` probe of the volume holding a path: available bytes
/// (`fs4::available_space`), or an identity (`volume_id`) telling two paths
/// on the same volume apart from two on different ones.
type VolumeProbe = Arc<dyn Fn(&std::path::Path) -> std::io::Result<u64> + Send + Sync>;

/// An identity of the volume holding `path`, equal for two paths on the
/// same volume: the device id on Unix; the path prefix (drive letter or
/// UNC share) on Windows, where std exposes no stable volume serial.
///
/// Public because the server's cache cleaner asks the same question of its
/// walk roots -- a free-space cap is a statement about a volume, so roots on
/// two volumes cannot share one budget -- and two answers to "are these the
/// same volume" that disagree would be worse than either.
#[cfg(unix)]
pub fn volume_id(path: &std::path::Path) -> std::io::Result<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).map(|metadata| metadata.dev())
}

#[cfg(windows)]
pub fn volume_id(path: &std::path::Path) -> std::io::Result<u64> {
    use std::hash::{Hash, Hasher};
    match path.components().next() {
        Some(std::path::Component::Prefix(prefix)) => {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            prefix.as_os_str().to_ascii_lowercase().hash(&mut hasher);
            Ok(hasher.finish())
        }
        _ => Err(std::io::Error::other("relative path has no volume")),
    }
}

#[cfg(not(any(unix, windows)))]
pub fn volume_id(_path: &std::path::Path) -> std::io::Result<u64> {
    Err(std::io::Error::other("volume identity unavailable"))
}

/// One pinned offline download, see [`BackendEngineFS::pinned_downloads`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PinnedDownload {
    pub info_hash: String,
    pub file_idx: usize,
}

/// What [`BackendEngineFS::unpin_download`] did, so a caller can report it
/// rather than echo what it asked for. `deleted_files` is what actually
/// left the disk, which is not the same as the request's `delete_files`: a
/// dormant pin whose torrent lived in the cache root has nothing this layer
/// can name, and a failed delete is logged, not raised.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnpinOutcome {
    /// Whether a pin was actually cleared -- false for an unknown torrent
    /// or a file nothing had pinned.
    pub unpinned: bool,
    /// Whether the data this call was asked to delete is gone.
    pub deleted_files: bool,
}

/// A magnet add whose backend `add_torrent` has not returned yet.
///
/// librqbit resolves a magnet's metadata *inside* `Session::add_torrent`, with
/// no timeout, and no `ManagedTorrent` exists until it succeeds. During that
/// window the torrent is in neither the backend nor `engines`, so this entry is
/// the only way another request can learn the torrent is being set up (and
/// join the wait instead of starting a duplicate resolution).
#[derive(Clone)]
pub struct PendingMagnetAdd<H: TorrentHandle> {
    /// Completes when the add finishes -- with the engine, or with the
    /// [`MagnetAddError`] that ended it (timeout included). The add runs
    /// detached, so a waiter that gives up does not cancel it.
    pub done: Shared<BoxFuture<'static, MagnetAddResult<H>>>,
    /// The merged tracker list the torrent is being added with (defaults +
    /// cached + request-supplied), for reporting while metadata resolves.
    pub trackers: Arc<[String]>,
    /// Identifies this add in the registry, so a late finish of a superseded
    /// add cannot touch its successor's entry.
    id: u64,
    /// Aborts the add task; used when the registry sweeps the entry as idle.
    abort: AbortHandle,
    /// How many lookups found this add already in flight and joined it
    /// instead of starting their own. The caller that *started* the add
    /// reads it once the add is done, to learn whether the torrent it got
    /// back is still its own to tear down -- see `pin_download`, whose
    /// refused pin removes the torrent it added, and must not when a
    /// stream request is holding the same engine.
    joiners: Arc<AtomicUsize>,
}

/// Registry-wide id source for [`PendingMagnetAdd::id`].
static NEXT_ADD_ID: AtomicU64 = AtomicU64::new(1);

impl<H: TorrentHandle + 'static> PendingMagnetAdd<H> {
    /// A lookup that found this add in flight and is about to wait on it.
    fn joined(&self) {
        self.joiners.fetch_add(1, Ordering::SeqCst);
    }

    /// How many lookups joined this add so far.
    pub fn joiners(&self) -> usize {
        self.joiners.load(Ordering::SeqCst)
    }
}

/// The failure record a magnet add leaves behind: the error that ended it and
/// the trackers it ran with, so a non-blocking poller can report `phase:
/// error` with a reason instead of an eternal `resolvingMetadata`.
#[derive(Debug, Clone)]
pub struct FailedMagnetAdd {
    pub error: MagnetAddError,
    pub trackers: Arc<[String]>,
}

/// What `lookup_or_begin_add_magnet` found, and whether it was this call
/// that started the add it is reporting -- a joiner and the starter both
/// see `Adding`, and only the starter may treat the torrent as its own.
struct Lookup<H: TorrentHandle> {
    lookup: EngineLookup<H>,
    started: bool,
}

impl<H: TorrentHandle> Lookup<H> {
    /// Something that was already there: an engine, someone else's add, or
    /// the record of a failed one.
    fn found(lookup: EngineLookup<H>) -> Self {
        Self {
            lookup,
            started: false,
        }
    }
}

/// The engine a blocking magnet add ended with, and this call's relation to
/// it (see `BackendEngineFS::add_magnet_placed`).
struct AddedMagnet<H: TorrentHandle> {
    engine: Arc<Engine<H>>,
    /// This call started the add. False for an engine that existed already
    /// and for an add someone else started and this call joined.
    started_here: bool,
    /// Lookups that joined the add while it ran -- each of them holds, or
    /// is about to hold, the same engine. Zero unless `started_here`.
    joiners: usize,
}

/// Non-blocking lookup result of [`BackendEngineFS::get_or_begin_add_magnet`].
pub enum EngineLookup<H: TorrentHandle> {
    /// The engine exists (metadata known).
    Ready(Arc<Engine<H>>),
    /// A magnet add is in flight; await `done` for the engine.
    Adding(PendingMagnetAdd<H>),
    /// The last add for this hash failed (timed out, backend error, task
    /// panic in debug builds) and nothing has retried it since. Only the blocking
    /// [`BackendEngineFS::get_or_add_magnet`] retries -- a fresh play request
    /// gets a fresh attempt, while pollers keep seeing the failure -- and the
    /// record is dropped once nothing has asked about the hash for
    /// `INACTIVE_TORRENT_REMOVE_TIMEOUT`.
    Failed(FailedMagnetAdd),
}

/// What the registry knows about a magnet add that has no engine yet.
enum MagnetAddState<H: TorrentHandle> {
    Adding(PendingMagnetAdd<H>),
    Failed(FailedMagnetAdd),
}

struct MagnetAddEntry<H: TorrentHandle> {
    state: MagnetAddState<H>,
    /// `Clock::now_secs()` of the last lookup that returned this entry; the eviction
    /// loop drops (and aborts) entries nobody has asked about for
    /// `INACTIVE_TORRENT_REMOVE_TIMEOUT`.
    last_polled_secs: AtomicU64,
}

impl<H: TorrentHandle> MagnetAddEntry<H> {
    fn touch(&self, now: u64) {
        self.last_polled_secs.store(now, Ordering::SeqCst);
    }

    fn idle_for(&self, now: u64) -> Duration {
        Duration::from_secs(now.saturating_sub(self.last_polled_secs.load(Ordering::SeqCst)))
    }
}

type MagnetAddRegistry<H> = Arc<RwLock<HashMap<String, MagnetAddEntry<H>>>>;

const DEFAULT_TRACKERS: &[&str] = &[
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://9.rarbg.com:2810/announce",
    "udp://tracker.openbittorrent.com:80/announce",
    "http://tracker.openbittorrent.com:80/announce",
    "udp://opentracker.i2p.rocks:6969/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://tracker.moeking.me:6969/announce",
    "udp://ipv4.tracker.harry.lu:80/announce",
];

pub struct BackendEngineFS<B: TorrentBackend> {
    pub backend: Arc<B>,
    engines: EngineRegistry<B::Handle>,
    tracker_manager: Arc<crate::trackers::TrackerManager>,
    pub cache_dir: std::path::PathBuf,
    pub download_dir: std::path::PathBuf,
    /// Track active streams per info_hash for legacy compatibility
    active_streams: Arc<RwLock<HashMap<String, usize>>>,
    /// Track active requests per specific streamed file so cleanup does not race probe retries.
    active_file_streams: Arc<RwLock<HashMap<(String, usize), usize>>>,
    /// Tracks the most recently active streamed file for legacy diagnostics.
    /// Active scheduling is driven by active_file_streams so several torrents can stream at once.
    active_file: Arc<RwLock<Option<(String, usize)>>>,
    /// For multi-file torrents, only the latest requested file is allowed to be
    /// wanted at a time. Single-file torrents bypass this selector.
    active_multifile_files: Arc<RwLock<HashMap<String, MultiFileActiveSelection>>>,
    priority_generation: Arc<AtomicU64>,
    /// Optional disk cache for persisting completed files. No constructor
    /// populates it in the librqbit-only build, and nothing reads it either,
    /// so it is dead code today; kept for a future backend that wants it.
    #[allow(dead_code)]
    disk_cache: Option<Arc<disk_cache::DiskCacheManager>>,
    /// When false, torrents are paused once their download completes.
    seeding_enabled: Arc<AtomicBool>,
    /// Magnet adds still inside the backend's `add_torrent`, plus the failure
    /// records of ones that ended without an engine, keyed by info hash. See
    /// [`PendingMagnetAdd`] and [`FailedMagnetAdd`].
    magnet_adds: MagnetAddRegistry<B::Handle>,
    /// One lock per info hash serialising `pin_download` and
    /// `unpin_download` for the same torrent -- a pin that has to add the
    /// torrent must not be raced by an unpin that deletes its data; entries
    /// live only while a call holds or waits for them.
    pin_locks: parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Persisted pins of torrents the backend did not have at startup
    /// (see [`Self::restore_pinned_downloads`]): kept in the persisted file
    /// and applied by the next `pin_download` of the torrent, or dropped by
    /// `unpin_download`. Never held across an `.await`.
    dormant_pins: parking_lot::Mutex<BTreeMap<String, std::collections::BTreeSet<usize>>>,
    /// Available-bytes probe for the free-space check in `pin_download` and
    /// for the reconciler's arm (`fs4::available_space`; tests substitute
    /// one). Both ask it about one folder: the piece store's root.
    free_space_probe: VolumeProbe,
    /// Epoch of every `*_secs` timestamp this instance and its engines keep.
    clock: Clock,
    /// The housekeeping sweep started by the constructor, kept so its owner
    /// can cancel it. See [`Self::take_sweep_task`].
    sweep_task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Rung once per reconciler pass that stopped a torrent for want of
    /// space, for the cache cleaner to run a pass at once rather than on its
    /// next poll -- see [`Self::out_of_space_signal`].
    out_of_space_notify: Arc<tokio::sync::Notify>,
    /// One lock per info hash, serialising the reconciler's decisions about
    /// one torrent without serialising them across torrents. See
    /// [`crate::reconcile::HashLocks`].
    reconcile_locks: crate::reconcile::HashLocks,
    /// The last free-space reading of every output volume, written by the
    /// reconciler's pass and shared with every [`Engine`] this instance
    /// makes, so that "is this torrent stopped for want of space?" is a
    /// map lookup rather than a `statvfs` per asker. See
    /// [`crate::reconcile::Volumes`].
    volumes: Arc<crate::reconcile::Volumes>,
    /// What the cache cleaner says the torrent-data volume may hold,
    /// written by the cleaner through [`Self::set_cache_budget`] and shared
    /// with every [`Engine`] this instance makes. Unknown until a pass has
    /// run: see [`crate::retention`].
    budget: Arc<crate::retention::RetentionBudget>,
}

/// What an [`Engine`] needs besides its backend handle: the epoch its
/// timestamps are on, the per-volume free-space readings it answers
/// [`Engine::is_stopped_for_space`] from, and the cache budget its
/// retention policy is sized against.
///
/// They travel together because every place that makes an engine needs all
/// of them, including the spawned magnet add, which outlives the request
/// that started it and so cannot borrow them from `&self`.
#[derive(Clone)]
struct EngineParts {
    clock: Clock,
    volumes: Arc<crate::reconcile::Volumes>,
    budget: Arc<crate::retention::RetentionBudget>,
}

#[derive(Debug, Clone)]
struct MultiFileActiveSelection {
    file_idx: usize,
    generation: u64,
    source: &'static str,
    last_seen_secs: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ActiveFileStreamSnapshot {
    pub info_hash: String,
    pub file_idx: usize,
    pub count: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ActiveFileSnapshot {
    pub info_hash: String,
    pub file_idx: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MultiFileActiveSelectionSnapshot {
    pub info_hash: String,
    pub file_idx: usize,
    pub generation: u64,
    pub source: String,
    pub last_seen_secs: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StreamActivitySnapshot {
    pub uptime_secs: u64,
    pub engine_count: usize,
    pub engine_active_streams: usize,
    pub active_file_priority_generation: u64,
    pub active_streams: HashMap<String, usize>,
    pub active_file_streams: Vec<ActiveFileStreamSnapshot>,
    pub active_file: Option<ActiveFileSnapshot>,
    pub active_multifile_selections: Vec<MultiFileActiveSelectionSnapshot>,
    /// The torrents the backend's state machine reports stopped, right now
    /// -- an observation, taken when the snapshot is built.
    ///
    /// It used to be the engines carrying an `idle_paused` flag this
    /// process had written, which reported nothing at all about a pause
    /// that survived a restart, and reported a pause for a torrent whose
    /// resume had failed. Neither is possible of a reading taken from the
    /// state machine. It no longer says *why* each is stopped, because
    /// nothing in this server remembers that any more: the reason is
    /// recomputed from live conditions on every reconciler pass
    /// ([`crate::reconcile::desired`]).
    pub paused_torrents: Vec<String>,
}

impl StreamActivitySnapshot {
    /// Whether a player is reading from this server right now.
    ///
    /// Two of the fields here are live and the rest are sticky, and telling
    /// them apart is the whole of the answer:
    ///
    /// * `engine_active_streams` counts open file readers -- up when one is
    ///   handed out, down when the [`crate::files::FileHandle`] is dropped.
    /// * `active_streams` counts the stream responses on top of them
    ///   (`on_stream_start`/`on_stream_end`).
    ///
    /// `active_file` and `active_multifile_selections` are the ones to leave
    /// out. They name the file most recently chosen, and they deliberately
    /// outlive the stream, because the want-set is planned from them -- a
    /// light driven by either would come on with the first playback of the
    /// session and never go out again.
    pub fn playback_is_live(&self) -> bool {
        self.engine_active_streams > 0 || self.active_streams.values().any(|count| *count > 0)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EngineDiagnosticsSnapshot {
    pub uptime_secs: u64,
    pub streams: StreamActivitySnapshot,
    pub memory: BackendMemoryDiagnostics,
}

/// One walk of the engines, for the cache cleaner --
/// [`BackendEngineFS::reclaim_verdicts`].
///
/// Two answers and not three classes: what the policy will part with, and
/// what can only be taken whole. Everything else the cleaner used to be
/// told -- which torrents were protected, which were dead -- is inside the
/// gate now, because both were the same question about announcements.
#[derive(Debug, Default, Clone)]
pub struct ReclaimVerdicts {
    /// Whether a given piece of a given torrent may be reclaimed. A torrent
    /// it has never heard of is cache nobody speaks for.
    pub gate: crate::retention::ReclaimGate,
    /// Unpinned torrents the free-space arm or librqbit's own ENOSPC
    /// stopped. Not a protection: the gate refuses their pieces because
    /// they are announced, and this list is what lets the cleaner take one
    /// **whole**, through the engine, when nothing else can go.
    pub stopped_for_space: Vec<String>,
}

pub type EngineFS = BackendEngineFS<LibrqbitBackend>;

/// Undoes what [`BackendEngineFS::on_stream_start`] registered, if that call
/// is dropped before it returns.
///
/// The registers a stream start writes -- the two stream counters, and for
/// a multi-file torrent the active selection -- have **no expiry**. Nothing
/// ages them out and nothing recomputes them; they are ended by the caller
/// calling [`BackendEngineFS::on_stream_end`], and by nothing else. So a
/// registration that outlives the call which wrote it is not a stale
/// reading that corrects itself, it is a permanent one:
/// `torrent_activity_registers` reads `playing` true for that torrent for
/// the life of the process, the idle arm can therefore never fire, the
/// housekeeping sweep never removes the engine, and with seeding off the
/// torrent downloads a film nobody is watching until the server restarts.
///
/// The window is not small. `on_stream_start` increments both counters and
/// then **awaits** the reconcile, which is inside the backend for as long
/// as starting a torrent takes; before that it awaits `activate_file`,
/// which for a multi-file torrent awaits the backend again. Dropping that
/// future is not an edge case either -- it is how every one of these
/// handlers ends when a player closes the connection.
///
/// It undoes only what it saw land, so it is correct under concurrency: it
/// decrements the counters it incremented rather than removing the entries
/// (another stream on the same file keeps its own count), and it drops the
/// multi-file selection only when the file it names has no stream left at
/// all.
///
/// **Exactly one of this and the caller's guard ever fires**: returning
/// from `on_stream_start` is what disarms this, and the caller has no
/// registration to end until `on_stream_start` has returned. Two owners
/// decrementing for one increment would end somebody else's stream, which
/// is the failure this is not allowed to trade for.
struct StreamStartRollback {
    active_streams: Arc<RwLock<HashMap<String, usize>>>,
    active_file_streams: Arc<RwLock<HashMap<(String, usize), usize>>>,
    active_multifile_files: Arc<RwLock<HashMap<String, MultiFileActiveSelection>>>,
    active_file: Arc<RwLock<Option<(String, usize)>>>,
    key: (String, usize),
    counted_stream: bool,
    counted_file_stream: bool,
    armed: bool,
}

impl StreamStartRollback {
    fn armed<B: TorrentBackend + 'static>(
        efs: &BackendEngineFS<B>,
        info_hash: String,
        file_idx: usize,
    ) -> Self {
        Self {
            active_streams: efs.active_streams.clone(),
            active_file_streams: efs.active_file_streams.clone(),
            active_multifile_files: efs.active_multifile_files.clone(),
            active_file: efs.active_file.clone(),
            key: (info_hash, file_idx),
            counted_stream: false,
            counted_file_stream: false,
            armed: true,
        }
    }

    /// The torrent-wide counter has been incremented.
    fn counted_stream(&mut self) {
        self.counted_stream = true;
    }

    /// The per-file counter has been incremented.
    fn counted_file_stream(&mut self) {
        self.counted_file_stream = true;
    }

    /// `on_stream_start` returned; the caller owns the registration now.
    fn handed_over(&mut self) {
        self.armed = false;
    }
}

impl Drop for StreamStartRollback {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let active_streams = self.active_streams.clone();
        let active_file_streams = self.active_file_streams.clone();
        let active_multifile_files = self.active_multifile_files.clone();
        let active_file = self.active_file.clone();
        let key = std::mem::take(&mut self.key);
        let counted_stream = self.counted_stream;
        let counted_file_stream = self.counted_file_stream;
        // Spawned because the maps are async locks and a `Drop` cannot
        // await one. The task is the whole of the undo, so nothing is left
        // half-undone if the runtime stops it: every step of it is
        // idempotent and conditional on what is still there.
        tokio::spawn(async move {
            if counted_stream {
                let mut streams = active_streams.write().await;
                if let Some(count) = streams.get_mut(&key.0) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        streams.remove(&key.0);
                    }
                }
            }
            let file_streams_remain = {
                let mut streams = active_file_streams.write().await;
                if counted_file_stream && let Some(count) = streams.get_mut(&key) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        streams.remove(&key);
                    }
                }
                streams.contains_key(&key)
            };
            // A stream that is still reading this file owns the selection;
            // only a file nothing is left reading gives it up.
            if !file_streams_remain {
                {
                    let mut selections = active_multifile_files.write().await;
                    if selections
                        .get(&key.0)
                        .is_some_and(|selection| selection.file_idx == key.1)
                    {
                        selections.remove(&key.0);
                    }
                }
                let mut active = active_file.write().await;
                if active.as_ref() == Some(&key) {
                    *active = None;
                }
            }
            tracing::debug!(
                info_hash = %key.0,
                file_idx = key.1,
                "stream start rolled back: its caller walked away before it returned"
            );
        });
    }
}

impl<B: TorrentBackend + 'static> BackendEngineFS<B> {
    pub fn new_with_backend(
        backend: B,
        restored_handles: HashMap<String, B::Handle>,
        cache_dir: std::path::PathBuf,
        download_dir: std::path::PathBuf,
    ) -> Self {
        Self::new_with_backend_and_storage(backend, restored_handles, cache_dir, download_dir, None)
    }

    pub fn new_with_backend_and_storage(
        backend: B,
        restored_handles: HashMap<String, B::Handle>,
        cache_dir: std::path::PathBuf,
        download_dir: std::path::PathBuf,
        tracker_storage: Option<Arc<dyn crate::trackers::TrackerStorage>>,
    ) -> Self {
        let clock = Clock::start();
        let volumes = Arc::new(crate::reconcile::Volumes::new(
            crate::piece_store::StoreRoot::in_download_dir(&download_dir)
                .path()
                .to_path_buf(),
        ));
        // A backend that sets piece reclaim restores every torrent paused
        // and wanting every hole in its storage, because the piece-level
        // want-set is not in the record. Until this process has put the
        // want-set back -- `restore_pinned_downloads`, below -- the
        // reconciler must not start one, and the honest way to say so is
        // that it has not been settled yet
        // (`reconcile::Conditions::settled`). Every other engine is made by
        // an add, which carries its want-set with it.
        let restored_unsettled = backend.sets_piece_reclaim();
        let budget = Arc::new(crate::retention::RetentionBudget::default());
        let mut engines_map = HashMap::new();
        for (hash, handle) in restored_handles {
            let engine =
                Engine::new_with_handle(handle, &hash, clock, volumes.clone(), budget.clone());
            // Nothing in this process has used it, and the last one's
            // reading did not survive -- so there is no reading, which is
            // not the same as a reading of now. See
            // `Engine::forget_last_active`.
            engine.forget_last_active();
            if restored_unsettled {
                engine.mark_unsettled();
            }
            engines_map.insert(hash.clone(), Arc::new(engine));
        }

        let engines = Arc::new(RwLock::new(engines_map));

        // Create tracker manager with or without storage
        let tracker_manager = match tracker_storage {
            Some(storage) => Arc::new(crate::trackers::TrackerManager::new_with_storage(storage)),
            None => Arc::new(crate::trackers::TrackerManager::new()),
        };

        let efs = Self {
            backend: Arc::new(backend),
            engines: engines.clone(),
            tracker_manager,
            cache_dir,
            download_dir: download_dir.clone(),
            active_streams: Arc::new(RwLock::new(HashMap::new())),
            active_file_streams: Arc::new(RwLock::new(HashMap::new())),
            active_file: Arc::new(RwLock::new(None)),
            active_multifile_files: Arc::new(RwLock::new(HashMap::new())),
            priority_generation: Arc::new(AtomicU64::new(0)),
            disk_cache: None,
            seeding_enabled: Arc::new(AtomicBool::new(true)),
            magnet_adds: Arc::new(RwLock::new(HashMap::new())),
            pin_locks: parking_lot::Mutex::new(HashMap::new()),
            dormant_pins: parking_lot::Mutex::new(BTreeMap::new()),
            free_space_probe: Arc::new(|path| match declared_volume_space(path) {
                Some(bytes) => Ok(bytes),
                None => fs4::available_space(path),
            }),
            clock,
            sweep_task: parking_lot::Mutex::new(None),
            out_of_space_notify: Arc::new(tokio::sync::Notify::new()),
            reconcile_locks: Default::default(),
            volumes,
            budget,
        };

        let engines_clone = engines.clone();
        let backend_clone = efs.backend.clone();
        let active_streams_clone = efs.active_streams.clone();
        let active_file_streams_clone = efs.active_file_streams.clone();
        let active_file_clone = efs.active_file.clone();
        let active_multifile_files_clone = efs.active_multifile_files.clone();
        let magnet_adds_clone = efs.magnet_adds.clone();
        let clock = efs.clock;
        let sweep = tokio::spawn(async move {
            loop {
                // Magnet-registry pruning; torrent removal is gated by
                // the much longer inactivity timeout below. Pausing is not
                // here any more -- that is the reconciler's, on its own
                // two-second tick.
                tokio::time::sleep(Duration::from_secs(15)).await;
                let mut to_remove = Vec::new();
                let now = clock.now_secs();

                // Magnet adds nobody has asked about for the inactivity window:
                // a failure record that was never retried, or (should the add
                // somehow outlive its own timeout) an add still in flight,
                // whose task is aborted. Bounds the registry the way the
                // engine sweep below bounds `engines`.
                {
                    let mut adds = magnet_adds_clone.write().await;
                    adds.retain(|info_hash, entry| {
                        let idle = entry.idle_for(now);
                        if idle <= INACTIVE_TORRENT_REMOVE_TIMEOUT {
                            return true;
                        }
                        match &entry.state {
                            MagnetAddState::Adding(pending) => {
                                pending.abort.abort();
                                tracing::info!(
                                    info_hash = %info_hash,
                                    idle_secs = idle.as_secs(),
                                    "Aborted idle magnet add"
                                );
                            }
                            MagnetAddState::Failed(failed) => {
                                debug!(
                                    info_hash = %info_hash,
                                    idle_secs = idle.as_secs(),
                                    error = %failed.error,
                                    "Dropped idle magnet add failure record"
                                );
                            }
                        }
                        false
                    });
                }

                {
                    let read = engines_clone.read().await;
                    for (hash, engine) in read.iter() {
                        let engine_active_streams = engine
                            .active_streams
                            .load(std::sync::atomic::Ordering::SeqCst);
                        let last = engine
                            .last_accessed
                            .load(std::sync::atomic::Ordering::SeqCst);
                        let age_secs = now.saturating_sub(last);
                        if age_secs <= INACTIVE_TORRENT_REMOVE_TIMEOUT.as_secs() {
                            continue;
                        }

                        let active_stream_count = {
                            let streams = active_streams_clone.read().await;
                            streams.get(hash).copied().unwrap_or(0)
                        };
                        let active_file_stream_count = {
                            let streams = active_file_streams_clone.read().await;
                            streams
                                .iter()
                                .filter(|((stream_hash, _), _)| stream_hash == hash)
                                .map(|(_, count)| *count)
                                .sum::<usize>()
                        };
                        let active_file_matches = {
                            let active = active_file_clone.read().await;
                            active
                                .as_ref()
                                .map(|(stream_hash, _)| stream_hash == hash)
                                .unwrap_or(false)
                        };
                        let active_multifile_matches = {
                            let selections = active_multifile_files_clone.read().await;
                            selections.contains_key(hash)
                        };
                        // An offline download is idle by nature (nothing
                        // reads it until it is complete); removing the
                        // torrent from the session would stop it.
                        let pinned = engine.is_pinned();

                        let skip_reason = if pinned {
                            Some("pinned_files")
                        } else if engine_active_streams > 0 {
                            Some("engine_active_streams")
                        } else if active_stream_count > 0 {
                            Some("active_streams")
                        } else if active_file_stream_count > 0 {
                            Some("active_file_streams")
                        } else if active_file_matches {
                            Some("active_file")
                        } else if active_multifile_matches {
                            Some("active_multifile_file")
                        } else {
                            None
                        };

                        if let Some(skip_reason) = skip_reason {
                            tracing::debug!(
                                info_hash = %hash,
                                age_secs,
                                engine_active_streams,
                                active_stream_count,
                                active_file_stream_count,
                                active_multifile_matches,
                                removed = false,
                                skip_reason,
                                "Skipping inactive-engine cleanup"
                            );
                        } else {
                            tracing::debug!(
                                info_hash = %hash,
                                age_secs,
                                engine_active_streams,
                                active_stream_count,
                                active_file_stream_count,
                                active_multifile_matches,
                                removed = true,
                                "Scheduling inactive-engine cleanup"
                            );
                            to_remove.push(hash.clone());
                        }
                    }
                }

                if !to_remove.is_empty() {
                    let mut write = engines_clone.write().await;
                    for hash in &to_remove {
                        debug!(info_hash = %hash, "Auto-removing inactive engine");
                        write.remove(hash);
                    }
                    drop(write);

                    // Actually stop the torrents in the backend session
                    for hash in to_remove {
                        if let Err(e) = backend_clone.remove_torrent(&hash).await {
                            tracing::warn!(
                                info_hash = %hash,
                                error = %e,
                                removed = false,
                                "Failed to remove inactive torrent from backend"
                            );
                        } else {
                            tracing::info!(
                                info_hash = %hash,
                                removed = true,
                                "Removed inactive torrent from backend"
                            );
                        }
                    }
                }
            }
        });
        *efs.sweep_task.lock() = Some(sweep);

        efs
    }

    /// Take the housekeeping sweep this constructor started -- the loop above
    /// that prunes the magnet registry and removes idle torrents -- for the
    /// caller to abort when it shuts down.
    /// `server::run` puts it with the other long-lived tasks it cancels. It
    /// comes out once; a second call (the stream and download engines are
    /// often the same `Arc`) yields `None`.
    ///
    /// It is the other task with the shape described on
    /// [`crate::trackers::TrackerManager::take_refresh_task`], and the one
    /// that is always in it: between sweeps it is parked on a 15-second
    /// `tokio::time::sleep`, so every single shutdown finds it holding a
    /// timer for the driver to fire on its way down. Aborting it costs
    /// nothing -- every step of the sweep is housekeeping the process is
    /// about to stop needing, and each `.await` in it is a lock or a backend
    /// call that cancellation simply drops.
    pub fn take_sweep_task(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.sweep_task.lock().take()
    }

    /// Take the tracker manager's periodic refresh task, for the caller to
    /// abort when it shuts down -- `server::run` puts it with the other
    /// long-lived tasks it cancels. It comes out once; a second call (the
    /// stream and download engines are often the same `Arc`) yields `None`.
    /// See [`crate::trackers::TrackerManager::take_refresh_task`].
    pub fn take_tracker_refresh_task(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.tracker_manager.take_refresh_task()
    }

    /// Start the reconciler: [`Self::reconcile_tick`] every
    /// [`crate::reconcile::RECONCILE_INTERVAL`] for as long as this engine
    /// exists. The caller
    /// owns the task -- `server::run` puts it with the other forever loops
    /// it aborts on shutdown -- and the task holds the engine weakly, so an
    /// embedder that drops the engine without aborting it ends it too. Not
    /// started by the constructor, unlike the housekeeping sweep: the tests
    /// drive the tick by hand against a probe of their own, and a
    /// reconciler running behind them against the real volume would stop
    /// their fake torrents whenever the machine happened to be short of
    /// disk.
    ///
    /// This replaced the free-space watch, whose whole policy is now the
    /// free-space arm of [`crate::reconcile::desired`].
    pub fn start_reconciler(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(crate::reconcile::RECONCILE_INTERVAL);
            loop {
                interval.tick().await;
                let Some(engine_fs) = weak.upgrade() else {
                    return;
                };
                engine_fs.reconcile_tick().await;
            }
        })
    }

    /// One pass of the reconciler: what every torrent should be doing, from
    /// what is true of it now, and the calls that make it so. Returns the
    /// decisions in registry order, for tests and for a caller that wants
    /// them.
    ///
    /// One free-space probe per distinct output folder, and no `stats()`
    /// anywhere: every question asked of a handle here
    /// ([`TorrentHandle::run_state`], [`TorrentHandle::has_metadata`],
    /// [`TorrentHandle::is_finished`]) is one the trait promises to answer
    /// from state the backend already holds, because this runs every two
    /// seconds over every torrent there is.
    ///
    /// A pass that stopped anything rings [`Self::out_of_space_signal`]
    /// once, whatever it stopped and however many: the cache cleaner it
    /// wakes walks every root anyway, so a second ring would only make it
    /// walk them twice.
    pub async fn reconcile_tick(&self) -> Vec<(String, crate::reconcile::Decision)> {
        self.reconcile_tick_at(self.clock.now_secs()).await
    }

    /// [`Self::reconcile_tick`] with the clock reading handed in.
    ///
    /// The clock is an input to the pass like the volume probe is, and it
    /// is separated for the same reason: a test that drives a **real**
    /// librqbit session cannot run under `tokio`'s paused clock (its
    /// sockets and its check threads need time to actually pass), so
    /// without this the only way to reach the idle arm from one would be to
    /// sit out [`INACTIVE_TORRENT_PAUSE_GRACE`] of wall time per test.
    async fn reconcile_tick_at(&self, now: u64) -> Vec<(String, crate::reconcile::Decision)> {
        // Cloned out and the guard dropped before the first `.await`: this
        // is a write-preferring `RwLock`, so a read guard held across an
        // await parks every later reader behind any writer that queues
        // meanwhile.
        let engines: Vec<_> = self.engines.read().await.values().cloned().collect();
        let mut probed = false;
        let mut decisions = Vec::with_capacity(engines.len());
        let mut stopped_any = false;
        let store = self.piece_store();
        for engine in engines {
            if let Some(decision) = self
                .reconcile_engine(
                    &engine,
                    crate::reconcile::Trigger::Timer,
                    now,
                    &mut probed,
                    &mut stopped_any,
                )
                .await
            {
                decisions.push((engine.info_hash.clone(), decision));
            }
            // The retention pass rides this tick rather than a timer of its
            // own: it is the same interval, over the same engines, and it
            // costs one `read_dir` per bucket for a torrent something is
            // actually reading and a `None` for every other.
            self.retain_engine(&engine, &store).await;
        }
        if stopped_any {
            self.out_of_space_notify.notify_one();
        }
        decisions
    }

    /// Reconcile one torrent, for a caller with a reason of its own -- a
    /// playback starting on it, which is measured against a different
    /// free-space line (see [`crate::reconcile::desired`]). `None` when
    /// there is no such engine, or when its backend owns its own lifecycle.
    ///
    /// Looks the engine up with [`Self::peek_engine`], never `get_engine`:
    /// a reconcile is an observation, and counting it as a poll would both
    /// keep an idle torrent from ever being removed and reset the very
    /// idleness the decision is made of.
    ///
    /// [`crate::reconcile::Trigger::PlaybackStart`] says why the decision is
    /// being taken, **not** that anything is playing: `playing` is read from
    /// the activity registers like every other condition. A caller must
    /// therefore register its stream before it asks, or it will be told what
    /// to do with a torrent nobody is watching.
    pub async fn reconcile_hash(
        &self,
        info_hash: &str,
        trigger: crate::reconcile::Trigger,
    ) -> Option<crate::reconcile::Decision> {
        let engine = self.peek_engine(info_hash).await?;
        let now = self.clock.now_secs();
        let mut probed = false;
        let mut stopped_any = false;
        let decision = self
            .reconcile_engine(&engine, trigger, now, &mut probed, &mut stopped_any)
            .await;
        if stopped_any {
            self.out_of_space_notify.notify_one();
        }
        decision
    }

    /// Decide for one engine and act on the decision, under that hash's
    /// reconcile lock.
    ///
    /// `None` for a backend that manages its own playback lifecycle: it
    /// pauses and resumes its torrents itself, and a second opinion from
    /// here would be a second owner of the same state -- which is the whole
    /// class of bug this reconciler exists to end.
    ///
    /// **Every pause and every unpause in the process is made here.** Both
    /// arms of the ladder are this reconciler's -- the free-space one and
    /// the idle one -- and there is nowhere else left that calls the
    /// backend's pause or unpause at all. That is the point of the whole
    /// design: eight call sites each hand-rolling "set the flag, call
    /// resume" in three different orders is what produced four consecutive
    /// defects, and what is left instead is one ladder, one actuator and
    /// no record of who stopped what.
    ///
    /// Only [`Verdict::for_space`] separates the two stops afterwards, and
    /// only for things that are statements about the *device*: the cache
    /// cleaner's wake-up and the read refusal. The stop call itself is the
    /// same call.
    async fn reconcile_engine(
        &self,
        engine: &Arc<Engine<B::Handle>>,
        trigger: crate::reconcile::Trigger,
        now: u64,
        probed: &mut bool,
        stopped_any: &mut bool,
    ) -> Option<crate::reconcile::Decision> {
        if engine.handle.manages_playback_lifecycle() {
            return None;
        }
        let _guard = self.reconcile_locks.lock(&engine.info_hash).await;
        // The volume the pieces land on, which is one folder for every
        // torrent in the session (`Volumes::data_folder`) -- so a pass
        // probes it once, not once per torrent.
        let folder = self.volumes.data_folder().to_path_buf();
        if !*probed {
            *probed = true;
            self.probe_volume(&folder, now);
        }
        let conditions = crate::reconcile::Conditions {
            run_state: engine.handle.run_state(),
            settled: engine.is_settled(),
            playing: self.torrent_is_active(&engine.info_hash, engine, now).await,
            pinned: engine.is_pinned(),
            seeding_enabled: self.seeding_enabled.load(Ordering::Relaxed),
            has_metadata: engine.handle.has_metadata().await,
            finished: engine.handle.is_finished().await,
            available: self.volumes.available(),
            idle_for: engine.quiet_for(now),
        };
        let verdict = crate::reconcile::verdict(&conditions, trigger);
        // Every input, so a field log answers *why* on its own: a decision
        // without its inputs cannot be argued with.
        tracing::debug!(
            info_hash = %engine.info_hash,
            decision = ?verdict.decision,
            for_space = verdict.for_space,
            ?trigger,
            run_state = ?conditions.run_state,
            playing = conditions.playing,
            pinned = conditions.pinned,
            seeding_enabled = conditions.seeding_enabled,
            has_metadata = conditions.has_metadata,
            finished = conditions.finished,
            available = ?conditions.available,
            idle_secs = conditions.idle_for.map(|d| d.as_secs()),
            settled = conditions.settled,
            "torrent_reconciled"
        );

        match verdict.decision {
            crate::reconcile::Decision::Stop => {
                // One call for both arms. It is guarded on `Live` and not
                // on the arm, because what makes a stop safe is the state
                // it is made from: pausing an *initializing* torrent wedges
                // its check for good (`file_ops.rs:113` bails it,
                // `mod.rs:590-593` returns `Ok` without changing the state,
                // and `wait_until_initialized` then polls a torrent with no
                // check running for ever), and the ladder's first arm
                // answers `Stop` for exactly that reading.
                let stopped_here = self.stop_if_running(engine, &conditions, now).await;
                if verdict.for_space {
                    self.after_stopping_for_space(
                        engine,
                        &conditions,
                        now,
                        stopped_here,
                        stopped_any,
                    );
                } else {
                    // Not a statement about the device, so it lifts one.
                    self.let_reads_park_again(engine);
                }
            }
            crate::reconcile::Decision::Run => {
                self.start_if_stopped(engine, &conditions, trigger, now)
                    .await;
                self.let_reads_park_again(engine);
            }
            // `Error`, and a probe that failed on a timer pass. Both are
            // "no opinion", and lifting a refusal is an opinion: a torrent
            // the backend killed has its refusal lifted by
            // `restart_from_error` when the cleaner puts it back to work,
            // and a `statvfs` that stopped answering is evidence neither
            // that the volume filled nor that it cleared -- the same reason
            // `reconcile::Volumes::record` leaves the stall clock alone for
            // one.
            crate::reconcile::Decision::Leave => {}
        }
        Some(verdict.decision)
    }

    /// The ladder's `Stop`, for both of its arms: stop the torrent if it is
    /// running.
    ///
    /// Once, not once per pass -- the next pass sees it `Paused` and makes
    /// no call. Guarded on [`RunState::Live`] rather than on "not paused",
    /// because the third state a stop must never be made from is
    /// `Initializing`: see the caller.
    ///
    /// Whether the stop succeeded is not recorded anywhere; what the next
    /// pass reads is the state machine, which is the only thing that knows.
    /// A refusal is logged at debug and nothing else, because the ordinary
    /// cause of one is a race with a check that settled between the reading
    /// and the call.
    async fn stop_if_running(
        &self,
        engine: &Arc<Engine<B::Handle>>,
        conditions: &crate::reconcile::Conditions,
        now: u64,
    ) -> bool {
        if conditions.run_state != RunState::Live {
            return false;
        }
        match engine.handle.stop_torrent().await {
            Ok(()) => {
                engine.record_transition(now);
                tracing::info!(
                    info_hash = %engine.info_hash,
                    available = ?conditions.available,
                    playing = conditions.playing,
                    pinned = conditions.pinned,
                    seeding_enabled = conditions.seeding_enabled,
                    idle_secs = conditions.idle_for.map(|d| d.as_secs()),
                    "torrent_stopped_by_reconciler"
                );
                true
            }
            Err(error) => {
                debug!(
                    info_hash = %engine.info_hash,
                    error = %format!("{error:#}"),
                    "the backend would not stop the torrent"
                );
                false
            }
        }
    }

    /// What the free-space arm's `Stop` does on top of the stop call, and
    /// what only it does: it is the one arm that is a statement about the
    /// *device*.
    ///
    /// The cache cleaner is woken (through `stopped_any`, once per pass
    /// however many torrents it stopped -- the cleaner walks every root
    /// anyway). And a torrent that is stopped on a volume that has been
    /// short for [`STOPPED_READ_STALL_BOUND`] has its reads failed
    /// ([`Engine::refuse_reads_for_space`]): a read parked on a piece that
    /// is not being fetched is a player buffering with no end, and the
    /// bound is how long the cache cleaner gets to settle it first.
    ///
    /// The bound is read on the same pass as the stop, not only on a later
    /// one: what decides whether a parked read has anything coming is how
    /// long the *volume* has had no room, not how long this torrent has
    /// been stopped on it. A torrent stopped now, onto a volume that filled
    /// ten minutes ago, has readers as doomed as one stopped then.
    fn after_stopping_for_space(
        &self,
        engine: &Arc<Engine<B::Handle>>,
        conditions: &crate::reconcile::Conditions,
        now: u64,
        stopped_here: bool,
        stopped_any: &mut bool,
    ) {
        if stopped_here {
            *stopped_any = true;
            tracing::warn!(
                info_hash = %engine.info_hash,
                available = ?conditions.available,
                floor = CACHE_FREE_SPACE_FLOOR,
                pinned = conditions.pinned,
                "torrent_stopped_for_space"
            );
        }
        let short_for = self.volumes.short_for(now).unwrap_or_default();
        if !engine.reads_refused() && short_for >= STOPPED_READ_STALL_BOUND {
            tracing::warn!(
                info_hash = %engine.info_hash,
                short_secs = short_for.as_secs(),
                available = ?conditions.available,
                "the volume a stopped torrent writes to has been short for a while; \
                 failing its readers rather than leaving them parked"
            );
            engine.refuse_reads_for_space();
        }
    }

    /// Act on a `Run` for one engine: start it if it is stopped, whoever
    /// stopped it and whenever -- the previous process included, which is
    /// the pause nothing on master could lift.
    ///
    /// A torrent that is already `Live` is left alone. Lifting the read
    /// refusal is *not* done here, precisely because of that case: see
    /// [`Self::let_reads_park_again`].
    ///
    /// **The dwell.** A [`crate::reconcile::Trigger::Timer`] will not start
    /// a torrent within [`RECONCILE_MIN_DWELL`] of the reconciler's last
    /// start or stop of it. Every stop drops the swarm and every start
    /// re-announces to trackers that enforce a minimum announce interval,
    /// so a condition that oscillates around one of the ladder's lines
    /// costs peers rather than merely CPU. It is deliberately asymmetric,
    /// and both halves of the asymmetry matter:
    ///
    /// * A **stop** is never delayed by it. The free-space arm's stop is
    ///   what keeps a volume from filling, and a disk fills in seconds.
    /// * A **[`crate::reconcile::Trigger::PlaybackStart`]** is never
    ///   delayed by it. Somebody is waiting for the stream, and making them
    ///   wait out a dwell to protect an announce budget is the wrong trade
    ///   in the one case where a human can tell.
    ///
    /// What is left is the timer's own start, which nobody is waiting for
    /// and which will come round again in
    /// [`crate::reconcile::RECONCILE_INTERVAL`].
    ///
    /// A torrent this reconciler has never moved is exempt as well, and
    /// that question is asked of [`Engine::last_transition_at`] as a
    /// `None` rather than as a reading of zero: the clock answers zero for
    /// the whole first second of the process, and a stop made in it is a
    /// stop like any other -- see [`crate::engine::NEVER_MOVED`].
    async fn start_if_stopped(
        &self,
        engine: &Arc<Engine<B::Handle>>,
        conditions: &crate::reconcile::Conditions,
        trigger: crate::reconcile::Trigger,
        now: u64,
    ) {
        if conditions.run_state != RunState::Paused {
            return;
        }
        if let Some(moved_at) = engine.last_transition_at()
            && trigger == crate::reconcile::Trigger::Timer
        {
            let since_transition = Duration::from_secs(now.saturating_sub(moved_at));
            if since_transition < RECONCILE_MIN_DWELL {
                debug!(
                    info_hash = %engine.info_hash,
                    since_secs = since_transition.as_secs(),
                    "not starting a torrent this soon after the last time it was moved"
                );
                return;
            }
        }
        match engine.handle.start_torrent().await {
            Ok(()) => {
                engine.record_transition(now);
                tracing::info!(
                    info_hash = %engine.info_hash,
                    available = ?conditions.available,
                    ?trigger,
                    "torrent_started_by_reconciler"
                );
            }
            Err(error) => tracing::warn!(
                info_hash = %engine.info_hash,
                error = %format!("{error:#}"),
                "the backend would not start a torrent the reconciler wants running"
            ),
        }
    }

    /// Let reads through this engine park again rather than fail with
    /// `StorageFull` ([`Engine::allow_reads`]).
    ///
    /// The refusal is recomputed like everything else here: it stands while
    /// the free-space arm is stopping this torrent and the volume has been
    /// short for [`STOPPED_READ_STALL_BOUND`], and it is lifted on the
    /// first pass that arm is not the one deciding. It is a claim about the
    /// *device*, and no other arm of the ladder makes one.
    ///
    /// Tying it to the start call instead is how a refusal that nothing
    /// could ever clear shipped. The two ways out of a refusal that are not
    /// a start are both ordinary: the torrent is idle-paused when the
    /// cleaner frees the volume, so the reconcile that follows answers the
    /// idle arm's `Stop` and starts nothing; and the user then presses play,
    /// which resumes it through `activate_file` before
    /// [`Self::reconcile_hash`] is asked, so by the time the answer is `Run`
    /// the torrent is already `Live` and there is no start to hang the lift
    /// on. Every read on that engine then failed with `StorageFull`, for
    /// good, on a volume with room to spare.
    fn let_reads_park_again(&self, engine: &Arc<Engine<B::Handle>>) {
        if !engine.reads_refused() {
            return;
        }
        tracing::info!(
            info_hash = %engine.info_hash,
            "the volume this torrent writes to has room again; its reads wait rather than fail"
        );
        engine.allow_reads();
        // Nothing should be parked (a refusal wakes every reader and they
        // return `StorageFull`), but a read that arrived between the lift
        // and this line is one nobody would wake otherwise.
        engine.wake_readers();
    }

    /// Probe `folder`'s volume and record the reading for this pass and for
    /// every later asker ([`crate::reconcile::Volumes`]). A probe that
    /// failed is recorded as `None`, which the ladder reads as unknown and
    /// never as full.
    fn probe_volume(&self, folder: &std::path::Path, now: u64) {
        let available = match probe_at_existing_ancestor(&*self.free_space_probe, folder) {
            Ok(available) => Some(available),
            Err(error) => {
                debug!(
                    folder = %folder.display(),
                    %error,
                    "could not read the free space of the volume the pieces land on; \
                     the reconciler leaves its torrents alone"
                );
                None
            }
        };
        self.volumes.record(available, now);
    }

    /// Whether anything is using this torrent right now: a response body
    /// open on it, a file stream, a multi-file selection, or a reader
    /// parked inside the engine. These were questions asked in three
    /// places -- the housekeeping sweep's idle pause, the per-stream
    /// grace-period task and this -- and the first two are gone: the
    /// ladder is the only thing that asks.
    ///
    /// A `true` answer is stamped on the engine ([`Engine::mark_active`]),
    /// and that stamp is the whole of the idle arm's grace clock. It is
    /// written here, where the registers are actually read, rather than
    /// taken from `Engine::last_accessed`: that one is the registry's
    /// idle-eviction clock and counts every lookup, so every `stats.json`
    /// poll -- which reaches its engine through [`Self::get_engine`] --
    /// reset it, and a client with a details page open kept a torrent
    /// nobody was watching downloading for ever with seeding off.
    async fn torrent_is_active(
        &self,
        info_hash: &str,
        engine: &Engine<B::Handle>,
        now: u64,
    ) -> bool {
        let active = self.torrent_activity_registers(info_hash, engine).await;
        if active {
            engine.mark_active(now);
        }
        active
    }

    /// [`Self::torrent_is_active`] without the stamp: the four registers
    /// and nothing else.
    async fn torrent_activity_registers(
        &self,
        info_hash: &str,
        engine: &Engine<B::Handle>,
    ) -> bool {
        if engine.active_streams.load(Ordering::SeqCst) > 0 {
            return true;
        }
        if self
            .active_streams
            .read()
            .await
            .get(info_hash)
            .copied()
            .unwrap_or(0)
            > 0
        {
            return true;
        }
        if self
            .active_file_streams
            .read()
            .await
            .iter()
            .any(|((hash, _), count)| hash == info_hash && *count > 0)
        {
            return true;
        }
        self.active_multifile_files
            .read()
            .await
            .contains_key(info_hash)
    }

    /// Completes once the reconciler has stopped a torrent for want of space
    /// since the last time this completed (or since the engine was made, if
    /// a stop came first). One permit, not a counter: the cleaner that awaits
    /// this runs one pass per wake-up, and a pass covers every stopped
    /// torrent there is.
    pub async fn out_of_space_signal(&self) {
        self.out_of_space_notify.notified().await
    }

    /// The tracker list a torrent is added with: the built-in defaults, the
    /// tracker manager's cached list (ranked by RTT), and any request-supplied
    /// extras, sorted and de-duplicated.
    async fn merged_trackers(&self, extra_trackers: Option<Vec<String>>) -> Vec<String> {
        let mut trackers: Vec<String> = DEFAULT_TRACKERS.iter().map(|s| s.to_string()).collect();
        trackers.extend(self.tracker_manager.get_trackers().await);
        if let Some(extra) = extra_trackers {
            trackers.extend(extra);
        }
        trackers.sort();
        trackers.dedup();
        trackers
    }

    /// What [`Self::register_engine`] needs of this instance.
    fn engine_parts(&self) -> EngineParts {
        EngineParts {
            clock: self.clock,
            volumes: self.volumes.clone(),
            budget: self.budget.clone(),
        }
    }

    /// Wrap a backend handle in an `Engine` and publish it, or return the
    /// engine already registered for the same info hash.
    async fn register_engine(
        engines: &EngineRegistry<B::Handle>,
        handle: B::Handle,
        parts: EngineParts,
    ) -> Arc<Engine<B::Handle>> {
        let info_hash = handle.info_hash();
        let mut engines = engines.write().await;
        if let Some(engine) = engines.get(&info_hash) {
            engine.touch();
            return engine.clone();
        }
        let engine = Arc::new(Engine::new_with_handle(
            handle,
            &info_hash,
            parts.clock,
            parts.volumes,
            parts.budget,
        ));
        engines.insert(info_hash, engine.clone());
        engine
    }

    /// Add a torrent from a `.torrent` blob or URL and publish its engine.
    ///
    /// Honours the [`MagnetAddError::EvictedForSpace`] cooling-off period,
    /// which the magnet path enforces in `lookup_or_begin_add_magnet`. It
    /// has to be enforced here too and for the same reason: the cleaner
    /// evicts a stopped torrent only when a pass could free nothing else,
    /// and a re-add inside the window refills the volume it just emptied.
    /// The magnet path is the one a player's reconnect and stremio-core's
    /// stats poll take, so this one needs a user action to reach -- but
    /// "the client re-creates the torrent it has the file for" is exactly
    /// what a Stremio client does with a `.torrent` addon result, and the
    /// refusal is a `507` that says why rather than a disk that fills
    /// again.
    ///
    /// The check is before the add because a backend add is already writing
    /// files by the time it could be asked what it added; the hash comes
    /// from [`TorrentBackend::source_info_hash`], and a source whose hash
    /// cannot be known without fetching it is added as before. The error is
    /// the typed [`MagnetAddError`] inside the `anyhow`, so a route can
    /// `downcast_ref` it to the same status the magnet path gives.
    pub async fn add_torrent(
        &self,
        source: TorrentSource,
        extra_trackers: Option<Vec<String>>,
    ) -> Result<Arc<Engine<B::Handle>>> {
        if let Some(info_hash) = self.backend.source_info_hash(&source)
            && let Some(refusal) = self.evicted_for_space_refusal(&info_hash).await
        {
            tracing::warn!(
                info_hash,
                "refusing a torrent-file add of a hash evicted for want of disk space"
            );
            return Err(anyhow::Error::new(refusal));
        }
        let trackers = self.merged_trackers(extra_trackers).await;
        debug!(count = trackers.len(), "Adding torrent with trackers");
        let handle = self.backend.add_torrent(source, trackers).await?;
        Ok(Self::register_engine(&self.engines, handle, self.engine_parts()).await)
    }

    /// The standing [`MagnetAddError::EvictedForSpace`] for `info_hash`, if
    /// the cooling-off period from [`Self::evict_stopped_torrent`] has not
    /// run out. `None` for every other recorded failure: only this one
    /// refuses a retry, and only for its window.
    async fn evicted_for_space_refusal(&self, info_hash: &str) -> Option<MagnetAddError> {
        let now = self.clock.now_secs();
        let adds = self.magnet_adds.read().await;
        let MagnetAddState::Failed(failed) = &adds.get(info_hash)?.state else {
            return None;
        };
        match &failed.error {
            error @ MagnetAddError::EvictedForSpace { .. } if !error.may_retry_at(now) => {
                Some(error.clone())
            }
            _ => None,
        }
    }

    /// Existing engine for `info_hash`, or the in-flight magnet add for it --
    /// started here from a bare `magnet:?xt=urn:btih:` link with
    /// `extra_trackers` merged in if neither exists -- or the failure record
    /// of the last add if it ended without an engine. Never waits for metadata
    /// and never retries a failed add (see [`EngineLookup::Failed`]).
    ///
    /// Concurrent callers for one info hash share a single backend add: the
    /// first request's tracker list is the one used (librqbit cannot add
    /// trackers to a torrent later, see `LibrqbitHandle::add_trackers`), and
    /// the add runs detached so a poller that disconnects does not cancel the
    /// resolution a player is waiting on. Each add is bounded by
    /// [`METADATA_RESOLVE_TIMEOUT`].
    pub async fn get_or_begin_add_magnet(
        &self,
        info_hash: &str,
        extra_trackers: Option<Vec<String>>,
    ) -> EngineLookup<B::Handle> {
        self.lookup_or_begin_add_magnet(
            info_hash,
            extra_trackers,
            false,
            TorrentPlacement::default(),
        )
        .await
        .lookup
    }

    /// [`Self::get_or_begin_add_magnet`], waiting for an in-flight add and
    /// retrying a failed one.
    pub async fn get_or_add_magnet(
        &self,
        info_hash: &str,
        extra_trackers: Option<Vec<String>>,
    ) -> Result<Arc<Engine<B::Handle>>, MagnetAddError> {
        self.get_or_add_magnet_placed(info_hash, extra_trackers, TorrentPlacement::default())
            .await
    }

    /// [`Self::get_or_add_magnet`] with a [`TorrentPlacement`] for the add
    /// this call starts. Like the trackers, the want-set only counts when
    /// this call is the one that adds the torrent: an existing engine or an
    /// in-flight add is joined as it stands, and what it wants is then the
    /// reconciler's to settle (see `pin_download`, which unions the pin in).
    pub async fn get_or_add_magnet_placed(
        &self,
        info_hash: &str,
        extra_trackers: Option<Vec<String>>,
        placement: TorrentPlacement,
    ) -> Result<Arc<Engine<B::Handle>>, MagnetAddError> {
        self.add_magnet_placed(info_hash, extra_trackers, placement)
            .await
            .map(|added| added.engine)
    }

    /// [`Self::get_or_add_magnet_placed`], also saying whether this call
    /// is the one that added the torrent and who else has it -- what a
    /// caller that may tear the torrent down again needs to know.
    async fn add_magnet_placed(
        &self,
        info_hash: &str,
        extra_trackers: Option<Vec<String>>,
        placement: TorrentPlacement,
    ) -> Result<AddedMagnet<B::Handle>, MagnetAddError> {
        let Lookup { lookup, started } = self
            .lookup_or_begin_add_magnet(info_hash, extra_trackers, true, placement)
            .await;
        match lookup {
            EngineLookup::Ready(engine) => Ok(AddedMagnet {
                engine,
                started_here: false,
                joiners: 0,
            }),
            EngineLookup::Adding(pending) => {
                let engine = pending.done.clone().await?;
                Ok(AddedMagnet {
                    engine,
                    started_here: started,
                    // Read after the add is done and the engine published:
                    // a lookup that arrives later finds the engine, not the
                    // add, so this is the whole count.
                    joiners: pending.joiners(),
                })
            }
            EngineLookup::Failed(failed) => Err(failed.error),
        }
    }

    async fn lookup_or_begin_add_magnet(
        &self,
        info_hash: &str,
        extra_trackers: Option<Vec<String>>,
        retry_failed: bool,
        placement: TorrentPlacement,
    ) -> Lookup<B::Handle> {
        let info_hash = info_hash.to_lowercase();
        if let Some(engine) = self.get_engine(&info_hash).await {
            return Lookup::found(EngineLookup::Ready(engine));
        }
        // Merge before taking the registry lock: the tracker manager may
        // refresh its list over the network.
        let trackers = self.merged_trackers(extra_trackers).await;

        let mut adds = self.magnet_adds.write().await;
        // Re-check under the registry lock: an add publishes its engine before
        // removing itself from the registry, so one of the two is always
        // visible here, and a finished add must not be restarted.
        if let Some(engine) = self.engines.read().await.get(&info_hash).cloned() {
            engine.touch();
            return Lookup::found(EngineLookup::Ready(engine));
        }
        let now = self.clock.now_secs();
        if let Some(entry) = adds.get(&info_hash) {
            entry.touch(now);
            match &entry.state {
                MagnetAddState::Adding(pending) => {
                    // Counted under the registry lock, so the add cannot
                    // finish and publish between the count and the join.
                    pending.joined();
                    return Lookup::found(EngineLookup::Adding(pending.clone()));
                }
                MagnetAddState::Failed(failed)
                    if !retry_failed || !failed.error.may_retry_at(now) =>
                {
                    return Lookup::found(EngineLookup::Failed(failed.clone()));
                }
                MagnetAddState::Failed(failed) => {
                    debug!(info_hash, error = %failed.error, "Retrying failed magnet add");
                }
            }
        }

        debug!(
            info_hash,
            count = trackers.len(),
            "Adding magnet with trackers"
        );
        let pending = Self::spawn_magnet_add(
            self.backend.clone(),
            self.engines.clone(),
            self.magnet_adds.clone(),
            self.engine_parts(),
            info_hash.clone(),
            trackers,
            placement,
        );
        adds.insert(
            info_hash,
            MagnetAddEntry {
                state: MagnetAddState::Adding(pending.clone()),
                last_polled_secs: AtomicU64::new(now),
            },
        );
        Lookup {
            lookup: EngineLookup::Adding(pending),
            started: true,
        }
    }

    /// Start the detached, time-bounded backend add for `info_hash` and the
    /// supervisor that settles its registry entry.
    ///
    /// The supervisor awaits the add task's `JoinHandle`, so the entry is
    /// settled however the add ends -- engine published (entry removed),
    /// backend error or timeout (entry becomes its failure record), abort or a
    /// panic (likewise; a panic only gets that far in debug builds -- the
    /// release profile's `panic = "abort"` kills the process) -- without
    /// depending on any waiter polling `done`.
    /// A stats poller that never awaits therefore still sees the failure.
    fn spawn_magnet_add(
        backend: Arc<B>,
        engines: EngineRegistry<B::Handle>,
        adds: MagnetAddRegistry<B::Handle>,
        parts: EngineParts,
        info_hash: String,
        trackers: Vec<String>,
        placement: TorrentPlacement,
    ) -> PendingMagnetAdd<B::Handle> {
        let id = NEXT_ADD_ID.fetch_add(1, Ordering::Relaxed);
        let trackers: Arc<[String]> = trackers.into();

        let add = {
            let hash = info_hash.clone();
            let trackers = trackers.clone();
            tokio::spawn(async move {
                let source = TorrentSource::Url(format!("magnet:?xt=urn:btih:{hash}"));
                let add = backend.add_torrent_placed(source, trackers.to_vec(), placement);
                match tokio::time::timeout(METADATA_RESOLVE_TIMEOUT, add).await {
                    Ok(Ok(handle)) => Ok(Self::register_engine(&engines, handle, parts).await),
                    Ok(Err(error)) => Err(MagnetAddError::Backend {
                        info_hash: hash,
                        error: Arc::new(error),
                    }),
                    Err(_elapsed) => {
                        // librqbit's `add_torrent` is not cancel-safe: dropping
                        // it mid-way can leave the torrent inserted in the
                        // session but never `start()`ed, so a retry would get
                        // `AlreadyManaged` for a torrent that will never
                        // resolve and the hash would be stuck. Best-effort
                        // removal; the torrent usually does not exist yet, so
                        // an error here is the normal case and is not
                        // reported.
                        if let Err(error) = backend.remove_torrent(&hash).await {
                            debug!(
                                info_hash = %hash,
                                %error,
                                "nothing to remove from the backend after metadata timeout"
                            );
                        }
                        Err(MagnetAddError::MetadataTimeout {
                            info_hash: hash,
                            timeout: METADATA_RESOLVE_TIMEOUT,
                        })
                    }
                }
            })
        };
        let abort = add.abort_handle();

        let supervisor = {
            let hash = info_hash.clone();
            let trackers = trackers.clone();
            tokio::spawn(async move {
                let result = match add.await {
                    Ok(result) => result,
                    Err(join_error) if join_error.is_cancelled() => {
                        Err(MagnetAddError::Cancelled {
                            info_hash: hash.clone(),
                        })
                    }
                    Err(join_error) => Err(MagnetAddError::TaskFailed {
                        info_hash: hash.clone(),
                        reason: join_error.to_string(),
                    }),
                };
                let mut adds = adds.write().await;
                let ours = matches!(
                    adds.get(&hash).map(|entry| &entry.state),
                    Some(MagnetAddState::Adding(pending)) if pending.id == id
                );
                if ours {
                    match &result {
                        Ok(_) => {
                            adds.remove(&hash);
                        }
                        Err(error) => {
                            tracing::warn!(info_hash = %hash, %error, "Magnet add failed");
                            if let Some(entry) = adds.get_mut(&hash) {
                                entry.state = MagnetAddState::Failed(FailedMagnetAdd {
                                    error: error.clone(),
                                    trackers,
                                });
                            }
                        }
                    }
                }
                result
            })
        };
        let done = supervisor
            .map(move |joined| match joined {
                Ok(result) => result,
                Err(join_error) => Err(MagnetAddError::TaskFailed {
                    info_hash,
                    reason: format!("supervisor: {join_error}"),
                }),
            })
            .boxed()
            .shared();

        PendingMagnetAdd {
            done,
            trackers,
            id,
            abort,
            joiners: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The in-flight magnet add for `info_hash`, if its engine does not exist yet.
    pub async fn pending_magnet_add(&self, info_hash: &str) -> Option<PendingMagnetAdd<B::Handle>> {
        match self.magnet_add_state(info_hash).await? {
            MagnetAddState::Adding(pending) => Some(pending),
            MagnetAddState::Failed(_) => None,
        }
    }

    /// The failure record of the last magnet add for `info_hash`, if it ended
    /// without an engine and has not been retried or swept since.
    pub async fn failed_magnet_add(&self, info_hash: &str) -> Option<FailedMagnetAdd> {
        match self.magnet_add_state(info_hash).await? {
            MagnetAddState::Adding(_) => None,
            MagnetAddState::Failed(failed) => Some(failed),
        }
    }

    /// Counts as a poll of the entry for idle eviction.
    async fn magnet_add_state(&self, info_hash: &str) -> Option<MagnetAddState<B::Handle>> {
        let adds = self.magnet_adds.read().await;
        let entry = adds.get(&info_hash.to_lowercase())?;
        entry.touch(self.clock.now_secs());
        Some(match &entry.state {
            MagnetAddState::Adding(pending) => MagnetAddState::Adding(pending.clone()),
            MagnetAddState::Failed(failed) => MagnetAddState::Failed(failed.clone()),
        })
    }

    /// The registry's engine for `info_hash` **without** counting as a
    /// poll, unlike [`Self::get_engine`]. For observers -- a progress
    /// logger, a diagnostics sweep -- that must not keep a torrent alive
    /// just by looking at it.
    pub async fn peek_engine(&self, info_hash: &str) -> Option<Arc<Engine<B::Handle>>> {
        let engines = self.engines.read().await;
        engines.get(&info_hash.to_lowercase()).cloned()
    }

    pub async fn get_engine(&self, info_hash: &str) -> Option<Arc<Engine<B::Handle>>> {
        Self::lookup_engine(&self.engines, info_hash).await
    }

    /// [`Self::get_engine`] over the registry rather than `&self`, for a
    /// caller that outlives the request it came in on.
    async fn lookup_engine(
        engines: &EngineRegistry<B::Handle>,
        info_hash: &str,
    ) -> Option<Arc<Engine<B::Handle>>> {
        let engines = engines.read().await;
        let engine = engines.get(&info_hash.to_lowercase()).cloned();
        if let Some(engine) = &engine {
            engine.touch();
        }
        engine
    }

    pub async fn get_or_add_engine(&self, info_hash: &str) -> Result<Arc<Engine<B::Handle>>> {
        Ok(self.get_or_add_magnet(info_hash, None).await?)
    }

    pub async fn remove_engine(&self, info_hash: &str) {
        let mut engines = self.engines.write().await;
        engines.remove(&info_hash.to_lowercase());
    }

    /// Drop the registry entry for `engine`'s hash only while it still is
    /// `engine` (an entry someone else published meanwhile stays). Returns
    /// whether anything was removed.
    async fn remove_engine_if_current(&self, engine: &Arc<Engine<B::Handle>>) -> bool {
        let mut engines = self.engines.write().await;
        match engines.get(&engine.info_hash) {
            Some(current) if Arc::ptr_eq(current, engine) => {
                engines.remove(&engine.info_hash);
                true
            }
            _ => false,
        }
    }

    /// One retention pass over one engine, logged when it did anything.
    ///
    /// The whole of what bounds the streaming cache between cleaner runs:
    /// the cleaner walks the volume once a minute at best, and a torrent
    /// playing at 20 MB/s writes a gigabyte in that time. This runs on the
    /// reconciler's two-second tick, asks the policy where the playhead has
    /// left us, and gives back what the window no longer covers.
    async fn retain_engine(
        &self,
        engine: &Arc<Engine<B::Handle>>,
        store: &crate::piece_store::StoreRoot,
    ) {
        let Some(pass) = engine.retain(store).await else {
            return;
        };
        if pass != crate::retention::RetentionPass::default() {
            debug!(
                info_hash = %engine.info_hash,
                committed = pass.committed,
                reclaimed = pass.reclaimed,
                withdrawn = pass.withdrawn,
                "retention pass"
            );
        }
    }

    /// Take the bytes behind `pieces` of one torrent off the disk, have-set
    /// first, and say how many complete piece files really left it.
    ///
    /// **The cleaner's way in, and the only one it has.** It used to call
    /// `StoreRoot::delete_piece` itself, which was safe only for as long as
    /// everything it was allowed to touch belonged to no torrent in the
    /// session. The policy now hands it pieces of torrents that *are* in
    /// the session -- everything outside a playback window is fair game --
    /// and unlinking one of those behind librqbit's back leaves it
    /// advertising a piece it does not have and answering a peer's request
    /// with a read past the end of nothing. So every path goes through the
    /// claim in [`crate::retention::take_claimed`], and this is the call
    /// that gets there from an info hash.
    ///
    /// A hash the session holds nothing for, or holds a torrent its own
    /// error stopped, has no live have-set for a deletion to disagree with:
    /// the next start rebuilds it by asking the storage, which under this
    /// design *is* the piece files. Those go straight to the store.
    pub async fn release_pieces(&self, info_hash: &str, pieces: &[u32]) -> usize {
        let store = self.piece_store();
        let handle = self.backend.get_torrent(info_hash).await;
        let live = handle.filter(|handle| {
            !matches!(
                handle.run_state(),
                crate::backend::RunState::Error | crate::backend::RunState::Gone
            )
        });
        let Some(_handle) = live else {
            return store.delete_pieces(info_hash, pieces.iter().copied());
        };
        // Through the engine, so the question the cleaner asked before its
        // walk is asked again against the live policy -- see
        // `Engine::release_reclaimable`. An engine-less torrent the session
        // still runs announces everything it holds by the gate's own rule,
        // so there is nothing here to take.
        let Some(engine) = self.peek_engine(info_hash).await else {
            tracing::debug!(
                info_hash = %info_hash,
                "the session runs this torrent but nothing here holds it; leaving its pieces alone"
            );
            return 0;
        };
        engine.release_reclaimable(&store, pieces).await
    }

    /// What the cache cleaner says the torrent-data volume may hold, as of
    /// its last pass.
    ///
    /// **Pushed in, never recomputed.** The cleaner's cap is
    /// `min(cacheSize, occupied + available - floor)`; a second reading of
    /// the same volume taken here would disagree with it, and the two
    /// layers would evict against different numbers. `None` is the shape
    /// `CacheLimit::effective` answers in for "no cap at all", and it is
    /// not the same thing as never having been told
    /// ([`crate::retention::CacheBudget::Unknown`], which is what this
    /// starts as).
    pub fn set_cache_budget(&self, limit: Option<u64>) {
        self.budget.set(limit);
    }

    /// The budget itself, for the *other* adapter over the chunk store.
    ///
    /// `/proxy`'s cache is bounded by the same policy against the same
    /// number, and it has to be the same number: two readings of "how much
    /// room is there" over one volume is how two layers come to evict
    /// against different limits. Handing out the shared cell rather than a
    /// copy is what makes that structural -- there is one place the
    /// cleaner's cap is written, and everything that reads it reads that.
    pub fn cache_budget(&self) -> Arc<crate::retention::RetentionBudget> {
        self.budget.clone()
    }

    /// What one walk of the engines tells the cache cleaner: what the
    /// retention policy will part with, piece by piece, and which torrents
    /// can only be taken whole.
    ///
    /// **This is what `eviction_classes` became.** The cleaner used to be
    /// handed three lists of info hashes -- protected, dead,
    /// stopped-for-space -- and to decide from them which of the store's
    /// pieces it might unlink itself. Two of those were one question in two
    /// spellings, and the policy is what answers it: *have we told a peer
    /// about this piece?* What we announce may not be taken, whoever holds
    /// it and whatever stopped it; what we announce to nobody is cache like
    /// any other.
    ///
    /// A **pinned** torrent has no policy, so it announces everything and
    /// releases nothing -- a pin is a retention property, and the user asked
    /// for those bytes. A torrent the backend stopped with an error that is
    /// not a want of space announces nothing at all: there is no live
    /// have-set for a deletion to disagree with, and the next start rebuilds
    /// it by asking the storage, so every piece may go and goes first.
    ///
    /// A torrent stopped for want of space is not a *protection* any more
    /// and never needed to be one: it keeps its piece map and announces it
    /// again the moment it resumes, so the gate refuses its pieces like any
    /// other announced ones. What it still needs is the second list, which
    /// is not about permission but about a different operation -- it is
    /// evicted whole, through [`Self::evict_stopped_torrent`], and only when
    /// nothing else can go.
    pub async fn reclaim_verdicts(&self) -> ReclaimVerdicts {
        let engines: Vec<_> = self.engines.read().await.values().cloned().collect();
        let mut verdicts = ReclaimVerdicts::default();
        for engine in engines {
            let info_hash = engine.info_hash.to_lowercase();
            if engine.is_pinned() {
                verdicts.gate.insert_announced(info_hash);
                continue;
            }
            let out_of_space =
                engine.is_stopped_for_space().await || engine.handle.is_out_of_space().await;
            if out_of_space {
                verdicts.stopped_for_space.push(info_hash.clone());
            } else if engine.handle.is_in_error_state().await {
                verdicts.gate.insert_dead(info_hash);
                continue;
            }
            engine.gate_entry(&mut verdicts.gate);
        }
        // A dormant pin has no engine to speak for it -- that is what
        // dormant means -- and its bytes are its pieces exactly like a live
        // engine's.
        for pin in self.dormant_pinned_downloads() {
            verdicts.gate.insert_announced(pin.info_hash.to_lowercase());
        }
        verdicts
    }

    /// The piece store this engine's data is in:
    /// `<cacheRoot>/rqbit-downloads/.pieces`, the same root the session's
    /// default storage factory is built on.
    ///
    /// **The one location question this layer asks, and the store is what it
    /// answers it.** Everything past here names a torrent by info hash and a
    /// piece by index; where those land is
    /// [`crate::piece_store::StoreRoot`]'s and nobody else's.
    ///
    /// It replaces `download_folder`, which answered `<downloadsDir>/<info
    /// hash>` -- the folder a pin used to place a torrent in, and used to
    /// have to relocate one into. A pin decides no location any more, so
    /// there is exactly one place a torrent's data can be, and it is the
    /// same one for a streamed torrent and an offline download.
    pub fn piece_store(&self) -> crate::piece_store::StoreRoot {
        crate::piece_store::StoreRoot::in_download_dir(&self.download_dir)
    }

    /// Info hashes of torrents the backend stopped because the volume they
    /// write to ran out of space, and of torrents the reconciler's
    /// free-space arm stopped before it could
    /// ([`Engine::is_stopped_for_space`]).
    ///
    /// A full disk is the one torrent error worth acting on rather than
    /// reporting: the swarm is fine, the torrent is fine, the device is out
    /// of room. The caller that can do something about it is the server's
    /// cache cleaner -- this is how it finds out there is anything to evict
    /// *for*. Cheap on purpose (no I/O: the free-space half is a lookup of
    /// the reconciler's last reading), because it is asked on a timer.
    pub async fn out_of_space_torrents(&self) -> Vec<String> {
        let engines: Vec<_> = self.engines.read().await.values().cloned().collect();
        let mut hashes = Vec::new();
        for engine in engines {
            if engine.held_stopped_for_space().await || engine.handle.is_out_of_space().await {
                hashes.push(engine.handle.info_hash());
            }
        }
        hashes
    }

    /// Turn seeding on or off session-wide, and put back to work the
    /// torrents the idle arm had stopped for want of it.
    ///
    /// `seeding_enabled` is one of the ladder's conditions
    /// ([`crate::reconcile::Conditions`]), so flipping it changes what
    /// every torrent should be doing -- and the reconciler is asked at once
    /// rather than at its next tick, so that the switch takes effect when
    /// it is moved.
    ///
    /// [`crate::reconcile::Trigger::Timer`], not `PlaybackStart`, even
    /// though a person did move the switch: nobody is *waiting* on the
    /// answer, no reader is about to open, and the two things that trigger
    /// changes are both concessions made to someone who is. A torrent
    /// stopped for want of space keeps its resume margin here, which is
    /// what stops this call restarting torrents into a nearly-full volume
    /// -- and `server::run` applies the persisted setting at startup, over
    /// every torrent the last process left stopped, where "a user is
    /// waiting" would be simply false.
    ///
    /// **Awaited, and the registry dropped first.** The engines are
    /// snapshotted out of `self.engines` and the read guard dropped before
    /// the first `.await`: it is a write-preferring `RwLock`, so a read
    /// guard held across an await parks every later reader behind any
    /// writer that queues meanwhile -- and the await here reaches
    /// `Session::unpause`, which flushes librqbit's persistence file. One
    /// torrent's disk write would otherwise stall every route that wants to
    /// look an engine up.
    ///
    /// It used to spawn instead, over `if idle_paused.swap(false) &&
    /// resume()`, which is `false && ...` in a fresh process: dead code
    /// after every restart, and a restart is exactly when torrents come up
    /// stopped with nothing in this process able to say why.
    pub async fn set_seeding_enabled(&self, enabled: bool) {
        self.seeding_enabled.store(enabled, Ordering::Relaxed);
        self.backend.set_seeding_enabled(enabled);
        tracing::info!(seeding_enabled = enabled, "Seeding policy updated");

        let hashes: Vec<String> = {
            let engines = self.engines.read().await;
            engines.keys().cloned().collect()
        };
        for hash in hashes {
            self.reconcile_hash(&hash, crate::reconcile::Trigger::Timer)
                .await;
        }
    }

    pub fn seeding_enabled(&self) -> bool {
        self.seeding_enabled.load(Ordering::Relaxed)
    }

    /// Put the torrent `info_hash` back to work after the backend stopped it
    /// with an **error** -- and only then. `false` when no engine holds that
    /// hash any more (it was swept while space was being reclaimed) and when
    /// the torrent is not in the error state; neither is a failure.
    ///
    /// The cache cleaner calls this over the torrents it has just made room
    /// for, which are of two kinds. The ones librqbit stopped with its own
    /// ENOSPC error are this method's: restarting one re-checks its storage
    /// and goes live again, and it is the one transition the reconciler
    /// will not make (its ladder answers `Leave` for the error state,
    /// because a restart before the room exists would only fail again). The
    /// ones the reconciler stopped are *not*: it starts them itself on its
    /// next pass, from the volume reading it takes then, and having a
    /// second caller unpause them from a reading nobody rechecked is the
    /// shape of bug this whole design is closing. So this refuses them, and
    /// the guard is the state machine's answer rather than any note about
    /// who stopped what.
    pub async fn restart_from_error(&self, info_hash: &str) -> Result<bool> {
        let engine = self.engines.read().await.get(info_hash).cloned();
        let Some(engine) = engine else {
            return Ok(false);
        };
        if !engine.handle.is_in_error_state().await {
            return Ok(false);
        }
        engine.handle.restart_from_error().await?;
        // The error is gone, so the reads that were failed while it stood
        // have something to wait for again.
        engine.allow_reads();
        engine.wake_readers();
        Ok(true)
    }

    /// Evict a torrent stopped for want of disk space, whole: the torrent
    /// leaves the registry and the session, its files go with it
    /// ([`TorrentBackend::remove_torrent_and_files`]), its readers are
    /// failed, and a request for the hash inside
    /// [`EVICTED_FOR_SPACE_RETRY_AFTER`] is answered
    /// [`MagnetAddError::EvictedForSpace`] rather than re-adding it.
    /// `false` -- and nothing done -- for a hash no engine holds, for one
    /// that is not stopped for space, and for a pinned one.
    ///
    /// The cache cleaner calls this when a pass could get under the cap by
    /// no other means (`cache_cleaner::evict`); it decides *when*, this
    /// layer does the removing, because the two records of the data have to
    /// go together. A torrent librqbit paused still holds its files open
    /// (an unlink frees no block) and its piece map still says it has them
    /// (a resume reads pieces from a file that is empty), so the files may
    /// not simply be deleted under it; `Session::delete` takes the state,
    /// closes the files, deletes them, and leaves any open `FileStream`
    /// erroring on its next poll rather than reading a file that is gone.
    ///
    /// What the user sees is the point of it. They started a film larger
    /// than the free space; it stopped; they press play again. Before this
    /// the previous attempt's corpse held the space, protected as a live
    /// engine's files, and the retry failed for want of the room the corpse
    /// took. Now the retry finds the space (and, within the cooling-off
    /// period, a `507` that says why rather than a fresh add that would
    /// refill the disk to fail the same way). The earlier attempt's
    /// progress is lost -- but it was progress into a file that could not
    /// have been finished on this volume anyway.
    pub async fn evict_stopped_torrent(&self, info_hash: &str) -> Result<bool> {
        let info_hash = info_hash.to_lowercase();
        let engine = self.engines.read().await.get(&info_hash).cloned();
        let Some(engine) = engine else {
            return Ok(false);
        };
        if engine.is_pinned()
            || !(engine.is_stopped_for_space().await || engine.handle.is_out_of_space().await)
        {
            return Ok(false);
        }
        // Readers first, so a read that races the delete fails with the
        // device's error rather than the backend's.
        engine.refuse_reads_for_space();
        if !self.remove_engine_if_current(&engine).await {
            return Ok(false);
        }
        let now = self.clock.now_secs();
        self.magnet_adds.write().await.insert(
            info_hash.clone(),
            MagnetAddEntry {
                state: MagnetAddState::Failed(FailedMagnetAdd {
                    error: MagnetAddError::EvictedForSpace {
                        info_hash: info_hash.clone(),
                        retry_after_secs: now
                            .saturating_add(EVICTED_FOR_SPACE_RETRY_AFTER.as_secs()),
                    },
                    trackers: Arc::from(Vec::new()),
                }),
                last_polled_secs: AtomicU64::new(now),
            },
        );
        self.backend
            .remove_torrent_and_files(&info_hash)
            .await
            .with_context(|| format!("evicting {info_hash}, stopped for want of disk space"))?;
        tracing::info!(
            info_hash = %info_hash,
            retry_after_secs = EVICTED_FOR_SPACE_RETRY_AFTER.as_secs(),
            "evicted a torrent stopped for want of disk space, with its partial download"
        );
        Ok(true)
    }

    /// The backend's view of the DHT -- see [`crate::backend::DhtStatus`].
    /// Cheap enough to call per request; the sticky `ever_bootstrapped` bit
    /// is latched by the backend on every observation.
    pub fn dht_status(&self) -> crate::backend::DhtStatus {
        self.backend.dht_status()
    }

    /// Every existing engine's [`crate::backend::TransferTotals`], keyed by
    /// info hash -- the activity light's reading of the connection.
    ///
    /// A peek, not a poll, like [`Self::peek_engine`]: `last_accessed` is
    /// left alone, where [`Engine::get_statistics`] touches it. That is not
    /// a nicety. The light asks every second or two for the life of the
    /// process, and a reading that counted as a poll would hold every
    /// torrent out of the idle sweep for ever -- seeding on, and the light
    /// then lit by the traffic it caused. It also creates nothing: the
    /// engines that exist are iterated and no hash is looked up, so it never
    /// goes near `get_or_begin_add_magnet`. A hash removed since the last
    /// reading is simply absent, and its bytes leave the sum with it.
    pub async fn transfer_totals(&self) -> HashMap<String, crate::backend::TransferTotals> {
        let engines = self.engines.read().await;
        engines
            .iter()
            // A torrent whose backend cannot state its counters -- paused,
            // checking, errored -- contributes nothing to the sum, which is
            // what it contributed before this could be said in two ways:
            // the light reads a *difference* of two sums, and a sum that
            // dropped reads as "not grown". See [`crate::traffic`]. The
            // absence itself matters only where the totals are reported as
            // totals, which is [`Self::torrent_stream_numbers`].
            .map(|(hash, engine)| {
                (
                    hash.clone(),
                    engine.handle.transfer_totals().unwrap_or_default(),
                )
            })
            .collect()
    }

    /// What this server holds of one file a player is inside, and what that
    /// torrent has moved over this session -- the numbers a client's
    /// playback panel shows for a torrent stream.
    ///
    /// `None` is a hash no engine exists for: a stream this server is not
    /// holding, which is not an error and has no rows. Inside a `Some`,
    /// `window` and `committed_bytes` are absent together and mean exactly
    /// what [`Engine::policy_reading`] says they mean -- no policy governs
    /// this file, no reader has been inside it, or the reader has moved to
    /// another file. `transfer` is absent for a torrent whose backend keeps
    /// no counters to read: see
    /// [`crate::backend::TorrentHandle::transfer_totals`], which is where
    /// that absence is decided and why it is not a zero.
    ///
    /// **A peek, like [`Self::transfer_totals`], and for the same reason.**
    /// It creates nothing -- no engine, no magnet add, so it never goes near
    /// `get_or_begin_add_magnet` -- and it does not count as a poll, so a
    /// panel that asks every second cannot hold a torrent out of the idle
    /// sweep just by looking at it. It is *not* free, though: the window is
    /// counted from a listing of the torrent's piece directories, which is
    /// a `getdents` per thousand pieces on the blocking pool. Ask it while
    /// a panel is open, not for the life of the process.
    pub async fn torrent_stream_numbers(
        &self,
        info_hash: &str,
        file_idx: usize,
    ) -> Option<crate::retention::TorrentStreamNumbers> {
        let engine = self.peek_engine(info_hash).await?;
        let transfer = engine.handle.transfer_totals();
        let Some(reading) = engine.policy_reading(file_idx) else {
            return Some(crate::retention::TorrentStreamNumbers {
                window: None,
                committed_bytes: None,
                transfer,
            });
        };
        // Off the reactor: `held` lists one directory per thousand pieces.
        // The policy reading is already a value, so nothing is held across
        // it -- see `retention::PolicyReading`.
        let store = self.piece_store();
        let hash = info_hash.to_string();
        let held = tokio::task::spawn_blocking(move || store.held(&hash))
            .await
            .ok()?;
        Some(crate::retention::TorrentStreamNumbers {
            window: Some(reading.window(&held)),
            committed_bytes: Some(reading.committed_bytes()),
            transfer,
        })
    }

    pub async fn get_all_statistics(&self) -> HashMap<String, crate::backend::EngineStats> {
        let engines = self.engines.read().await;
        let mut stats = HashMap::new();
        for (hash, engine) in engines.iter() {
            stats.insert(hash.clone(), engine.get_statistics().await);
        }
        stats
    }

    pub async fn list_engines(&self) -> Vec<String> {
        let engines = self.engines.read().await;
        engines.keys().cloned().collect()
    }

    pub async fn stream_activity_snapshot(&self) -> StreamActivitySnapshot {
        let engines = self.engines.read().await;
        let engine_count = engines.len();
        let engine_active_streams = engines
            .values()
            .map(|engine| {
                engine
                    .active_streams
                    .load(std::sync::atomic::Ordering::SeqCst)
            })
            .sum();
        let paused_torrents = engines
            .iter()
            .filter(|(_, engine)| engine.handle.run_state() == RunState::Paused)
            .map(|(hash, _)| hash.clone())
            .collect();
        drop(engines);

        let active_streams = self.active_streams.read().await.clone();
        let active_file_streams = self
            .active_file_streams
            .read()
            .await
            .iter()
            .map(|((info_hash, file_idx), count)| ActiveFileStreamSnapshot {
                info_hash: info_hash.clone(),
                file_idx: *file_idx,
                count: *count,
            })
            .collect();
        let active_file = self
            .active_file
            .read()
            .await
            .as_ref()
            .map(|(info_hash, file_idx)| ActiveFileSnapshot {
                info_hash: info_hash.clone(),
                file_idx: *file_idx,
            });
        let now = self.clock.now_secs();
        let active_multifile_selections = self
            .active_multifile_files
            .read()
            .await
            .iter()
            .map(|(info_hash, selection)| MultiFileActiveSelectionSnapshot {
                info_hash: info_hash.clone(),
                file_idx: selection.file_idx,
                generation: selection.generation,
                source: selection.source.to_string(),
                last_seen_secs: selection.last_seen_secs,
            })
            .collect();

        StreamActivitySnapshot {
            uptime_secs: now,
            engine_count,
            engine_active_streams,
            active_file_priority_generation: self.priority_generation.load(Ordering::Relaxed),
            active_streams,
            active_file_streams,
            active_file,
            active_multifile_selections,
            paused_torrents,
        }
    }

    /// [`StreamActivitySnapshot::playback_is_live`] without the snapshot.
    ///
    /// The snapshot is the definition -- which of the fields mean "somebody
    /// is watching" is decided there and tested there -- but building one
    /// to read two of its fields costs six lock acquisitions and four
    /// cloned collections. This is the same two questions asked directly:
    /// the per-engine reader counts and the stream-response counts, two
    /// read locks (the `engines` one included, so this too queues behind an
    /// add or remove -- the saving is the other locks and the clones, not
    /// that wait) and nothing cloned.
    /// Short-circuits, so a server with a reader open answers from the
    /// first. A client polls this every second or two through the activity
    /// light, which is what makes the difference worth two definitions;
    /// `narrow_playback_query_agrees_with_the_snapshot` keeps them one.
    pub async fn playback_is_live(&self) -> bool {
        let readers_open = self
            .engines
            .read()
            .await
            .values()
            .any(|engine| engine.active_streams.load(Ordering::SeqCst) > 0);
        if readers_open {
            return true;
        }
        self.active_streams
            .read()
            .await
            .values()
            .any(|count| *count > 0)
    }

    pub async fn diagnostics_snapshot(&self) -> EngineDiagnosticsSnapshot {
        let streams = self.stream_activity_snapshot().await;
        let memory = self.backend.memory_diagnostics().await;

        EngineDiagnosticsSnapshot {
            uptime_secs: self.clock.now_secs(),
            streams,
            memory,
        }
    }

    /// Called when a stream starts for a torrent file.
    /// Several torrent files may be active at once; cleanup is per file stream.
    ///
    /// **Cancel-safe**: dropped before it returns, it leaves nothing
    /// registered. See `StreamStartRollback` for what that is worth --
    /// the registers this writes have no expiry, so one that outlives the
    /// request that wrote it is read as `playing` for the life of the
    /// process. Once this *has* returned the caller owns the registration
    /// and must end it with [`Self::on_stream_end`]; the two never both
    /// fire, because returning is what disarms the rollback.
    pub async fn on_stream_start(&self, info_hash: &str, file_idx: usize) {
        let info_hash = info_hash.to_lowercase();
        let mut rollback = StreamStartRollback::armed(self, info_hash.clone(), file_idx);
        let native_lifecycle = self
            .get_engine(&info_hash)
            .await
            .is_some_and(|engine| engine.handle.manages_playback_lifecycle());
        if native_lifecycle {
            if let Some(engine) = self.get_engine(&info_hash).await {
                engine.touch();
            }
            *self.active_file.write().await = Some((info_hash.clone(), file_idx));
        } else {
            self.activate_file(&info_hash, file_idx, "stream").await;
        }

        // Also update legacy active_streams counter
        {
            let mut streams = self.active_streams.write().await;
            let count = streams.entry(info_hash.clone()).or_insert(0);
            *count += 1;
        }
        rollback.counted_stream();
        {
            let mut streams = self.active_file_streams.write().await;
            let count = streams.entry((info_hash.clone(), file_idx)).or_insert(0);
            *count += 1;
        }
        rollback.counted_file_stream();

        tracing::debug!(
            "Stream started for {} file_idx={} (shared mode)",
            info_hash,
            file_idx
        );

        // Last, because the stream has to be registered before the question
        // is asked: `Trigger::PlaybackStart` says *why* the decision is
        // being taken, not that anything is playing, and `playing` is read
        // from the registers above like every other condition.
        //
        // It is here because of the hysteresis band. A torrent stopped for
        // want of space is not started again by a timer until the volume
        // clears the floor *plus* `FREE_SPACE_RESUME_MARGIN`, and in
        // between a request would otherwise open a reader on a torrent
        // nothing is fetching for -- the player's spinner, with no end and
        // no error. A user pressing play is owed the floor itself, which
        // is what this trigger is measured against, and nothing else in
        // the request can do it: starting a stopped torrent is the
        // reconciler's and only the reconciler's.
        self.reconcile_hash(&info_hash, crate::reconcile::Trigger::PlaybackStart)
            .await;

        // Handed over: from here the caller's guard owns the registration.
        rollback.handed_over();
    }

    /// Mark the torrent as active: librqbit has no session-wide streaming
    /// mode, so what this can do is stamp the activity, touch the engine
    /// and ask the reconciler whether the torrent should now be running.
    ///
    /// It used to read `if engine.idle_paused.swap(false) && resume()`,
    /// which on a fresh process is `false && ...` -- dead code after every
    /// restart, and a restart is exactly when a torrent comes up stopped
    /// with nothing in this process able to say why.
    ///
    /// **This call writes no activity register, and stamps no clock.** It
    /// says a reader is about to be opened, which is
    /// [`crate::reconcile::Trigger::PlaybackStart`] -- and the trigger says
    /// only *why* the question is being asked, never that anything is
    /// playing. So on the ladder's own conditions this is a torrent nobody
    /// is using, and until the idle arm was made the timer's alone the
    /// ladder could answer `Stop` for the very torrent it had been asked to
    /// focus (seeding off, registers empty, `idle_for` `None`, which the
    /// idle arm reads as quiet). That was latent only because the one
    /// production caller happens to run `on_stream_start` two lines earlier
    /// in `routes::stream`.
    ///
    /// The fix is not a stamp here. Stamping `Engine::last_active_at` from
    /// a call that read no register invents the observation the idle arm
    /// then measures its grace from -- the freshness mistake this whole
    /// design keeps deleting -- and buys a whole
    /// `INACTIVE_TORRENT_PAUSE_GRACE` of it on any torrent any caller
    /// names. The arm is gated on [`crate::reconcile::Trigger::Timer`]
    /// instead ([`crate::reconcile::verdict`]), so the ordering at the call
    /// site does not matter and nothing is claimed that was not read.
    pub async fn focus_torrent(&self, target_info_hash: &str) {
        let info_hash = target_info_hash.to_lowercase();
        let Some(engine) = self.get_engine(&info_hash).await else {
            return;
        };
        if engine.handle.manages_playback_lifecycle() {
            return;
        }
        engine.touch();
        self.reconcile_hash(&info_hash, crate::reconcile::Trigger::PlaybackStart)
            .await;
    }

    async fn activate_file(&self, info_hash: &str, file_idx: usize, source: &'static str) {
        let mut is_multifile = false;
        if let Some(engine) = self.get_engine(info_hash).await {
            engine.touch();
            if engine.handle.manages_playback_lifecycle() {
                *self.active_file.write().await = Some((info_hash.to_string(), file_idx));
                return;
            }
            is_multifile = engine.handle.file_count().await > 1;

            // No resume here any more. Starting a torrent that is stopped
            // is the reconciler's, and it has to be asked *after* the
            // activity this call is part of is registered, or it reads a
            // torrent nobody is watching: the caller therefore asks it
            // itself once it has finished registering (`on_stream_start`).
        }

        {
            let mut active = self.active_file.write().await;
            *active = Some((info_hash.to_string(), file_idx));
        }

        if is_multifile {
            self.activate_multifile_file(info_hash, file_idx, None, source)
                .await;
        }
    }

    pub async fn activate_multifile_file_for_playback(
        &self,
        info_hash: &str,
        file_idx: usize,
        hot_file: Option<HotFilePriorityPlan>,
        source: &'static str,
    ) {
        let info_hash = info_hash.to_lowercase();
        if self
            .get_engine(&info_hash)
            .await
            .is_some_and(|engine| engine.handle.manages_playback_lifecycle())
        {
            return;
        }
        self.activate_multifile_file(&info_hash, file_idx, hot_file, source)
            .await;
    }

    async fn activate_multifile_file(
        &self,
        info_hash: &str,
        file_idx: usize,
        hot_file: Option<HotFilePriorityPlan>,
        source: &'static str,
    ) {
        let Some(engine) = self.get_engine(info_hash).await else {
            return;
        };

        if engine.handle.file_count().await <= 1 {
            return;
        }

        let generation = self.priority_generation.fetch_add(1, Ordering::SeqCst) + 1;
        let now = self.clock.now_secs();
        let previous_file_idx = {
            let mut selections = self.active_multifile_files.write().await;
            selections
                .insert(
                    info_hash.to_string(),
                    MultiFileActiveSelection {
                        file_idx,
                        generation,
                        source,
                        last_seen_secs: now,
                    },
                )
                .map(|selection| selection.file_idx)
        };

        {
            let mut streams = self.active_file_streams.write().await;
            streams.retain(|(hash, idx), _| hash.as_str() != info_hash || *idx == file_idx);
        }
        {
            let mut active = self.active_file.write().await;
            *active = Some((info_hash.to_string(), file_idx));
        }

        engine.touch();
        // No reconcile here. Its caller reaches it through `activate_file`
        // and asks the reconciler for itself once it has finished
        // registering its activity (`on_stream_start`), so a call here
        // would be a second decision about the same torrent in the same
        // request -- and one taken from a half-registered reading.

        Self::reconcile_multifile_engine(engine, Some(file_idx), hot_file, generation, source)
            .await;

        tracing::debug!(
            info_hash = %info_hash,
            file_idx,
            previous_file_idx,
            generation,
            source,
            "multifile_active_file_selected"
        );
    }

    async fn reconcile_multifile_engine(
        engine: Arc<Engine<B::Handle>>,
        active_file: Option<usize>,
        hot_file: Option<HotFilePriorityPlan>,
        generation: u64,
        reason: &'static str,
    ) -> bool {
        if engine.handle.manages_playback_lifecycle() {
            return true;
        }
        if engine.handle.file_count().await <= 1 {
            return false;
        }

        if let Err(err) = engine
            .handle
            .reconcile_file_priorities(TorrentFilePriorityPlan {
                active_file,
                hot_file,
                generation,
                reason,
            })
            .await
        {
            tracing::warn!(
                info_hash = %engine.info_hash,
                generation,
                reason,
                error = %err,
                "Failed to reconcile multi-file priorities"
            );
        }

        true
    }

    /// Pin `file_idx` of `info_hash` as an offline download: the file stays
    /// wanted no matter which file is being played, and the engine is exempt
    /// from idle removal and the seeding-disabled pause for as long as it has
    /// a pinned file. Creates the engine if needed (through the magnet
    /// registry, waiting for metadata -- the file index has to be validated
    /// against the file list) with `extra_trackers` merged in, resumes a
    /// torrent the idle policy had paused, and reconciles the want-set so the
    /// pin takes effect now. Idempotent.
    ///
    /// **The pin moves nothing.** It is a retention property, not a
    /// location: a torrent's bytes are piece files under the store's one
    /// root ([`crate::piece_store`]) whether they were fetched for a stream
    /// or for a download, so a pin of a torrent that is already managed
    /// changes what is *kept*, not where anything is. This call used to
    /// relocate such a torrent into `<downloadsDir>/<info hash>` -- drop it
    /// from the backend, move its files, re-add it there, park the hash as
    /// an in-flight add for the length of a copy that could take minutes,
    /// and rebuild the registry's engine on the far side. All of that is
    /// gone, with the placement that asked for it.
    ///
    /// Persisted: the pin set is written to `pinned-downloads.json` in the
    /// download dir on every change and re-applied by
    /// [`Self::restore_pinned_downloads`] at startup to the torrents the
    /// backend restored (librqbit keeps the file in its persisted
    /// `only_files`, so the download itself resumes; the pin makes it exempt
    /// from eviction again). Pins the restore found no torrent for stay
    /// dormant in that file and come back with the torrent: a pin of it
    /// applies them alongside the new one.
    ///
    /// Calls for the same info hash run one at a time (`pin_locks`), which
    /// is what keeps an [`Self::unpin_download`] out of the window where a
    /// pin is still adding the torrent: unlocked, the unpin would find no
    /// engine, delete the dormant pin's pieces, and leave the pin landing
    /// behind it -- a download persisted, protected and without its bytes.
    pub async fn pin_download(
        &self,
        info_hash: &str,
        file_idx: usize,
        extra_trackers: Option<Vec<String>>,
    ) -> Result<Arc<Engine<B::Handle>>, PinDownloadError> {
        let info_hash = info_hash.to_lowercase();
        let lock = self.pin_lock(&info_hash);
        let guard = lock.lock().await;
        let result = self
            .pin_download_locked(&info_hash, file_idx, extra_trackers)
            .await;
        drop(guard);
        self.release_pin_lock(&info_hash, lock);
        result
    }

    /// The lock serialising [`Self::pin_download`] and
    /// [`Self::unpin_download`] for one info hash, created on demand. Hand
    /// it to [`Self::release_pin_lock`] once the guard is dropped.
    fn pin_lock(&self, info_hash: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.pin_locks
            .lock()
            .entry(info_hash.to_string())
            .or_default()
            .clone()
    }

    /// Drop the map's entry for a released [`Self::pin_lock`] when nobody
    /// is waiting for it (`lock` is ours plus the map's -- a waiter holds
    /// its own clone, which keeps the entry alive).
    fn release_pin_lock(&self, info_hash: &str, lock: Arc<tokio::sync::Mutex<()>>) {
        let mut locks = self.pin_locks.lock();
        if Arc::strong_count(&lock) == 2 {
            locks.remove(info_hash);
        }
    }

    /// [`Self::pin_download`] with the per-hash lock held.
    async fn pin_download_locked(
        &self,
        info_hash: &str,
        file_idx: usize,
        extra_trackers: Option<Vec<String>>,
    ) -> Result<Arc<Engine<B::Handle>>, PinDownloadError> {
        let placement = TorrentPlacement {
            only_files: Some(vec![file_idx]),
        };
        let was_managed = self.get_engine(info_hash).await.is_some();
        let AddedMagnet {
            engine,
            started_here,
            joiners,
        } = self
            .add_magnet_placed(info_hash, extra_trackers, placement)
            .await?;
        let checked = self
            .check_pin_preconditions(&engine, file_idx, was_managed)
            .await;
        if let Err(error) = checked {
            // Torn down only when demonstrably this pin's and nobody
            // else's, which takes two questions and not one.
            //
            // *Was the add mine?* This call started it and nothing joined
            // it while metadata resolved (`joiners` counts the lookups that
            // found the add in flight and waited on it). A stream request
            // that looked the hash up meanwhile joined this very add and is
            // holding the same engine, and dropping the torrent from the
            // backend fails every read it is about to make (and any it has
            // already opened), so a joined torrent stays for the idle
            // sweeper.
            //
            // *Is anyone on it now?* `joiners` cannot answer that: it
            // counts who joined the **pending add**, and the engine has
            // been published by the time this runs, so every lookup after
            // that -- a stream opening on the freshly resolved torrent
            // being exactly the one that matters -- finds the engine and
            // increments nothing. The window is not one free-space probe
            // wide either: it spans the whole precondition check, which
            // awaits `file_count()` and `stats()` before it ever probes.
            // So the live registers are asked as well
            // ([`Self::torrent_activity_registers`]: the engine's own
            // stream count and the three activity maps), the same evidence
            // the idle arm uses for "nobody is watching this". Without
            // them a reader that opened inside that window had its torrent
            // dropped from the session under it.
            //
            // It used to take the torrent's placement as the evidence
            // instead ("it sits in the folder only pins place under"),
            // which meant a refused pin left the torrent behind whenever no
            // separate downloads directory was configured -- which was
            // every default install, and is now every install. The add being this call's own is the evidence, and
            // it is one this layer still has.
            // **The torrent goes; the bytes stay.** A refusal happens
            // before anything is downloaded, so an add's own writes are
            // nothing under this storage -- there is no pre-sized
            // placeholder to sweep up any more -- while whatever the store
            // does hold for the hash was fetched by an earlier stream or an
            // earlier session, and is cache for the cleaner rather than
            // this pin's to delete. That is why nothing here asks where a
            // torrent's data is: it used to take the files whenever the
            // pin's own placement folder had not existed before the add.
            let added_by_this_pin = started_here
                && joiners == 0
                && !engine.is_pinned()
                && !self.torrent_activity_registers(info_hash, &engine).await;
            if added_by_this_pin {
                self.remove_engine_if_current(&engine).await;
                if let Err(e) = self.backend.remove_torrent(info_hash).await {
                    debug!(info_hash, error = %e, "could not drop the torrent added for a refused pin");
                }
            }
            return Err(error);
        }
        // `is_pinned()` is what keeps the idle sweeper off the engine, so
        // it is recorded before the backend is asked for anything. Undone
        // below if the pin does not go through (unless the file was pinned
        // already -- a re-pin changes nothing).
        let newly_pinned = engine.pinned_files.write().insert(file_idx);
        if let Err(error) = engine.handle.pin_file(file_idx).await {
            if newly_pinned {
                engine.pinned_files.write().remove(&file_idx);
            }
            return Err(PinDownloadError::Backend(error));
        }
        // The torrent is back (or here for the first time since a boot
        // without it): its dormant pins apply with this one.
        let dormant = self.dormant_pins.lock().remove(info_hash);
        for idx in dormant.into_iter().flatten().filter(|idx| *idx != file_idx) {
            match engine.handle.pin_file(idx).await {
                Ok(()) => {
                    engine.pinned_files.write().insert(idx);
                }
                Err(error) => {
                    tracing::warn!(info_hash, file_idx = idx, %error, "could not re-apply a dormant pin");
                }
            }
        }
        engine.touch();
        // The pin is registered above, so the ladder reads `pinned` and
        // wants this torrent running whatever the idle arm would have said
        // -- and it is the reconciler that starts it, because a pause that
        // survived a restart is one no record in this process can explain.
        // Awaited: the pin is an instruction to download now.
        self.reconcile_hash(&engine.info_hash, crate::reconcile::Trigger::PlaybackStart)
            .await;
        self.reconcile_with_active_selection(engine.clone(), "pin_download")
            .await;
        self.persist_pinned_downloads().await;
        tracing::info!(
            info_hash = %engine.info_hash,
            file_idx,
            pinned = ?engine.pinned_file_indices(),
            "download_pinned"
        );
        Ok(engine)
    }

    /// Forget the pin on `file_idx` of `info_hash`. Returns whether it was
    /// pinned (false for an unknown torrent or an unpinned file; true for a
    /// dormant pin of a torrent the backend does not have, which is then
    /// dropped from the persisted set).
    ///
    /// Without `delete_files` only the pin goes: the data stays, the engine
    /// becomes an ordinary one again (idle removal applies), and the
    /// want-set is reconciled against the current playback selection --
    /// with nothing playing that is a no-op, so the file keeps downloading
    /// until the engine is swept or another file is prepared.
    ///
    /// With `delete_files` the data goes too, whether or not the file was
    /// pinned (the caller wants the download gone; a pin lost to a crash
    /// must not leave the bytes behind), see
    /// `Self::delete_download_data`: the whole torrent when this was its
    /// last pin, only this file while other pins hold. A *dormant* pin has
    /// no engine to delete anything through, so its bytes -- the torrent's
    /// directory in the piece store -- are taken by
    /// `Self::delete_dormant_download_data`, which first makes sure the
    /// session neither holds nor is adding the torrent; while the pin
    /// stands [`Self::protected_torrents`] keeps the cleaner off that
    /// directory, so this is what takes it now rather than in thirty days.
    /// With no engine **and** no pin there is nothing this call may delete:
    /// the bytes belong to no download it knows of and stay for the
    /// cleaner. What was really deleted is reported, not what was asked
    /// for ([`UnpinOutcome`]). A `file_idx` the torrent does not
    /// have is then refused with [`PinDownloadError::FileNotFound`], as
    /// [`Self::pin_download`] refuses it: a stale index must not be read as
    /// "delete the whole torrent".
    ///
    /// Takes the same per-hash lock as [`Self::pin_download`]: an unpin
    /// issued while a pin of that hash is still resolving metadata queues
    /// behind it and applies to the finished pin. Unlocked it would find no
    /// engine (the hash is parked in the magnet registry for the length of
    /// the add), report that nothing was pinned, delete the pieces as a
    /// dormant pin's, and leave the pin to land and be persisted behind
    /// it.
    pub async fn unpin_download(
        &self,
        info_hash: &str,
        file_idx: usize,
        delete_files: bool,
    ) -> Result<UnpinOutcome, PinDownloadError> {
        let info_hash = info_hash.to_lowercase();
        let lock = self.pin_lock(&info_hash);
        let guard = lock.lock().await;
        let result = self
            .unpin_download_locked(&info_hash, file_idx, delete_files)
            .await;
        drop(guard);
        self.release_pin_lock(&info_hash, lock);
        result
    }

    /// [`Self::unpin_download`] with the per-hash lock held.
    async fn unpin_download_locked(
        &self,
        info_hash: &str,
        file_idx: usize,
        delete_files: bool,
    ) -> Result<UnpinOutcome, PinDownloadError> {
        let info_hash = info_hash.to_string();
        let Some(engine) = self.get_engine(&info_hash).await else {
            // No torrent, but maybe a dormant pin waiting for it.
            let (was_dormant, hash_still_pinned) = {
                let mut dormant = self.dormant_pins.lock();
                let removed = dormant
                    .get_mut(&info_hash)
                    .is_some_and(|indices| indices.remove(&file_idx));
                if dormant
                    .get(&info_hash)
                    .is_some_and(|indices| indices.is_empty())
                {
                    dormant.remove(&info_hash);
                }
                (removed, dormant.contains_key(&info_hash))
            };
            if was_dormant {
                self.persist_pinned_downloads().await;
                tracing::info!(info_hash, file_idx, "dormant_download_unpinned");
            }
            // `was_dormant` is the warrant, and there is no other one
            // here. Without an engine and without a pin this layer holds
            // no record tying the hash's bytes to a download at all: they
            // are whatever an earlier stream left in the store, which is
            // the cleaner's to reclaim by age, not this call's to unlink.
            // Ungated, an unpin of a hash nobody ever pinned -- a stale
            // client index, a retry after the registry dropped the engine
            // -- was a delete of any torrent's cache.
            let deleted_files = delete_files
                && was_dormant
                && self
                    .delete_dormant_download_data(&info_hash, file_idx, hash_still_pinned)
                    .await;
            return Ok(UnpinOutcome {
                unpinned: was_dormant,
                deleted_files,
            });
        };
        if delete_files {
            // A delete is destructive far beyond the one file: with no pin
            // left on the torrent it takes the whole torrent, its files and
            // its folder. An index the torrent does not have must therefore
            // be refused here exactly as `pin_download` refuses it (404),
            // never silently widened into "delete everything". Only once
            // the metadata is in: a torrent still resolving it reports no
            // files and nothing can be validated against.
            let file_count = engine.handle.file_count().await;
            if file_count > 0 && file_idx >= file_count {
                return Err(PinDownloadError::FileNotFound {
                    file_idx,
                    file_count,
                });
            }
        }
        let was_pinned = engine.pinned_files.write().remove(&file_idx);
        engine.handle.unpin_file(file_idx).await?;
        if delete_files {
            // Before the want-set is re-planned, because it is planned from
            // exactly this bookkeeping: `reconcile_with_active_selection`
            // unions the registered active file into `only_files`, so
            // deleting the file that is playing would leave it selected,
            // librqbit writing on through the handle it keeps open on the
            // torrent's files, and the unlinked inode's blocks growing with
            // nothing able to reclaim them until the process exits.
            self.forget_playback_of(&info_hash, file_idx).await;
        }
        // Recomputed before anything is deleted: the backend must not be
        // writing a file this call is about to remove, and that holds for a
        // file that was never pinned too (the delete is what the caller
        // asked for either way). Skipped when the whole torrent goes with
        // it -- there is no handle left to reconcile against, and nothing
        // left to want.
        let drops_torrent = delete_files && !engine.is_pinned();
        if (was_pinned || delete_files) && !drops_torrent {
            self.reconcile_with_active_selection(engine.clone(), "unpin_download")
                .await;
        }
        let deleted_files = if delete_files {
            self.delete_download_data(&engine, file_idx).await
        } else {
            false
        };
        if was_pinned {
            self.persist_pinned_downloads().await;
            tracing::info!(
                info_hash = %engine.info_hash,
                file_idx,
                pinned = ?engine.pinned_file_indices(),
                deleted = deleted_files,
                "download_unpinned"
            );
        }
        Ok(UnpinOutcome {
            unpinned: was_pinned,
            deleted_files,
        })
    }

    /// Delete what a *dormant* pin of `info_hash` (no torrent in the
    /// backend) left on disk, for an unpin that asked to take the data with
    /// it. That is the torrent's directory in the piece store, which holds
    /// every byte of it and nothing of any other torrent. Returns whether
    /// the data actually went.
    ///
    /// It used to be `<downloadsDir>/<info hash>` -- the folder a pin placed
    /// a torrent in, under a settings key since removed -- and without such
    /// a directory configured there was nothing this layer could name at
    /// all, so an explicit `deleteFiles` unpin of a dormant pin deleted
    /// nothing on a default install. Asking the store
    /// answers for every pin, because a pin is a retention flag and the
    /// store is the one place a torrent's bytes are.
    ///
    /// **That precondition is checked here rather than assumed.** The
    /// caller reached this path because the *registry* had no engine, and
    /// the registry is not the session: [`Self::remove_engine`] drops an
    /// entry and leaves the torrent running (the TUI's delete key does
    /// exactly that, and so does the idle sweep for the moment between
    /// dropping the entry and telling the backend), and a magnet add
    /// parks the hash outside both for as long as metadata takes. Unlinking
    /// the directory in either case is the corruption this layer exists to
    /// avoid: the torrent goes on believing it holds those pieces,
    /// advertises them, and answers a peer's request with a read past the
    /// end of nothing. So the session is asked -- [`TorrentBackend::get_torrent`]
    /// and [`Self::pending_magnet_add`] -- and only a hash it has never
    /// heard of is deleted by hand. A torrent it *does* hold is deleted
    /// through the backend instead ([`TorrentBackend::remove_torrent_and_files`]),
    /// which takes the torrent out of the session before its storage
    /// releases the pieces: the same interlock, kept in the same one place.
    /// A hash still being added is left alone entirely -- there is no
    /// handle to remove it through yet, and the add is about to give the
    /// torrent a have-set built from the pieces on disk.
    ///
    /// There is no have-set to keep in step for the remaining case: the
    /// backend does not have this torrent, which is what dormant means, so
    /// nothing in the session believes it holds these pieces. (A live
    /// torrent's per-file delete is the opposite case, and
    /// `Self::delete_download_data` holds `drop_file_pieces`' claim across
    /// the unlink for it.)
    ///
    /// Nothing goes while another file of the same hash is still pinned --
    /// the directory holds that file's pieces too. Either way the bytes are
    /// reachable by the cleaner, which walks the store; this call is what
    /// makes an explicit `deleteFiles` unpin take effect at once instead of
    /// waiting on the age rule, and what takes the directory out of
    /// [`Self::protected_paths`] with the pin.
    async fn delete_dormant_download_data(
        &self,
        info_hash: &str,
        file_idx: usize,
        hash_still_pinned: bool,
    ) -> bool {
        if hash_still_pinned {
            tracing::info!(
                info_hash,
                file_idx,
                "other files of the torrent are still pinned; its pieces stay"
            );
            return false;
        }
        if self.pending_magnet_add(info_hash).await.is_some() {
            tracing::warn!(
                info_hash,
                file_idx,
                "the torrent is being added right now; its pieces stay for the add to find"
            );
            return false;
        }
        if self.backend.get_torrent(info_hash).await.is_some() {
            // Not dormant at all: the registry lost the engine but the
            // session still holds the torrent. It goes through the backend,
            // whose delete removes the torrent first and releases the
            // pieces through its storage -- never by hand, behind a
            // have-set that would go on advertising them.
            return match self.backend.remove_torrent_and_files(info_hash).await {
                Ok(()) => {
                    tracing::info!(info_hash, file_idx, "download_deleted_through_the_session");
                    true
                }
                Err(error) => {
                    tracing::warn!(
                        info_hash,
                        file_idx,
                        %error,
                        "could not delete the torrent the session still holds"
                    );
                    false
                }
            };
        }
        let folder = self.piece_store().torrent_dir(info_hash);
        match tokio::fs::remove_dir_all(&folder).await {
            Ok(()) => {
                tracing::info!(
                    info_hash,
                    file_idx,
                    folder = %folder.display(),
                    "dormant_download_deleted"
                );
                true
            }
            // "Nothing there" is not "freed", and under this storage it is
            // the ordinary answer for a hash nothing ever downloaded: the
            // flag says what left the disk, never what was asked for.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                tracing::warn!(
                    info_hash,
                    file_idx,
                    folder = %folder.display(),
                    %error,
                    "could not delete the dormant download's pieces"
                );
                false
            }
        }
    }

    /// Delete what `file_idx` of `engine` occupies on disk, for an unpin
    /// that was asked to take the data with it. With no pin left on the
    /// torrent that is the whole torrent: dropped from the registry and
    /// from the backend with its files and its (then empty) per-torrent
    /// folder ([`TorrentBackend::remove_torrent_and_files`]). While other
    /// files of it stay pinned the torrent must keep running, so only this
    /// file goes. That is two deletions, because a torrent's bytes are piece
    /// files ([`crate::piece_store`]) and the whole-file copy an earlier
    /// version of this server wrote may also still be sitting at the path
    /// the backend reports: **the dropped pieces**, and that path -- which is
    /// truncated before it is unlinked, since librqbit keeps an open `File`
    /// on every file of a running torrent and an unlink alone would not free
    /// a byte. The caller reconciles the want-set without the file first, so
    /// the backend does not write it again. Best effort: a failure is logged,
    /// the unpin stands, and the returned flag says whether anything
    /// actually left the disk -- never that it was already absent.
    ///
    /// The bytes are one record of the file and the backend's have-set is
    /// the other, and deleting the first does nothing to the second: left
    /// alone, librqbit went on reporting the deleted file complete,
    /// advertising its pieces and answering a peer's request with a read
    /// past the end of nothing, and a re-pin of the file found nothing to
    /// download and declared it finished -- an "offline" episode that is an
    /// immediate read error. So the have-set is edited first
    /// ([`TorrentHandle::drop_file_pieces`]) and the claim it returns is
    /// held until the unlink is done -- which is
    /// [`crate::retention::take_claimed`]'s job, and it is that function's
    /// job because it is the *only* place either half happens: while the
    /// claim stands nothing can download
    /// a piece back into the file being deleted, and dropping it is what
    /// re-queues the boundary piece the still-pinned neighbour shares. A
    /// backend that cannot drop (a torrent restored at startup -- see the
    /// librqbit handle's doc for why, and for what the next restart does
    /// about it) is a warning and the delete goes ahead: the caller asked
    /// for the disk back, and the stale have-set is the lesser of the two
    /// lies. It goes ahead over the *file*, though, and not over the
    /// pieces: the claim is where their indices come from, and taking
    /// pieces a backend still believes it has is the corruption this whole
    /// dance exists to avoid.
    async fn delete_download_data(&self, engine: &Arc<Engine<B::Handle>>, file_idx: usize) -> bool {
        if engine.is_pinned() {
            let Some(path) = engine.handle.file_path(file_idx).await else {
                tracing::warn!(
                    info_hash = %engine.info_hash,
                    file_idx,
                    "backend knows no path for the file; its data stays on disk"
                );
                return false;
            };
            let dropped = match engine.handle.drop_file_pieces(file_idx).await {
                Ok(dropped) => dropped,
                Err(error) => {
                    tracing::warn!(
                        info_hash = %engine.info_hash,
                        file_idx,
                        error = %format!("{error:#}"),
                        "the backend keeps the deleted file's pieces in its have-set: until the \
                         next restart it will report the file complete, advertise its pieces, \
                         and a re-pin will download nothing"
                    );
                    None
                }
            };
            // Truncated before it is unlinked: librqbit opens every file of
            // a torrent at storage init and keeps the `File` for the
            // torrent's lifetime, so an unlink alone drops the directory
            // entry while the inode's blocks stay allocated until the
            // torrent is dropped -- and the caller asked for the disk back.
            match tokio::fs::OpenOptions::new().write(true).open(&path).await {
                Ok(file) => {
                    if let Err(error) = file.set_len(0).await {
                        tracing::warn!(
                            info_hash = %engine.info_hash,
                            file_idx,
                            path = %path.display(),
                            %error,
                            "could not truncate the download's file before deleting it"
                        );
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => tracing::warn!(
                    info_hash = %engine.info_hash,
                    file_idx,
                    path = %path.display(),
                    %error,
                    "could not open the download's file to release its blocks"
                ),
            }
            let file_removed = match tokio::fs::remove_file(&path).await {
                Ok(()) => true,
                // Nothing at that path, which is the ordinary case now: the
                // torrent's bytes are the piece files taken below, and this
                // path is a whole-file copy only an earlier version of this
                // server ever wrote. "Already absent" is not "freed".
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => {
                    tracing::warn!(
                        info_hash = %engine.info_hash,
                        file_idx,
                        path = %path.display(),
                        %error,
                        "could not delete the download's file"
                    );
                    false
                }
            };
            // And the bytes themselves. `drop_file_pieces` is librqbit's own
            // have-set bookkeeping and frees nothing -- it hands back the
            // piece indices precisely so that whoever asked can delete them
            // -- so without this the caller was told the disk had come back
            // while every piece of the file was still in the store, and
            // nothing would ever have reclaimed them: the directory is
            // protected for as long as the torrent has any pin left.
            let pieces_freed = match dropped {
                // The claim goes with the pieces, into the one place that
                // orders the unlink against the have-set
                // ([`crate::retention::take_claimed`]) -- and is released
                // there, once the bytes are gone.
                Some(claim) => {
                    crate::retention::take_claimed(&self.piece_store(), &engine.info_hash, claim)
                }
                // No claim, so no list of pieces to take and no right to
                // take them: the backend still believes it has them.
                None => 0,
            };
            let deleted = file_removed || pieces_freed > 0;
            if deleted {
                tracing::info!(
                    info_hash = %engine.info_hash,
                    file_idx,
                    path = %path.display(),
                    file_removed,
                    pieces_freed,
                    "download_file_deleted"
                );
            }
            return deleted;
        }
        self.remove_engine_if_current(engine).await;
        match self
            .backend
            .remove_torrent_and_files(&engine.info_hash)
            .await
        {
            Ok(()) => {
                tracing::info!(
                    info_hash = %engine.info_hash,
                    file_idx,
                    "download_deleted"
                );
                true
            }
            Err(error) => {
                tracing::warn!(
                    info_hash = %engine.info_hash,
                    file_idx,
                    %error,
                    "could not delete the download's torrent"
                );
                false
            }
        }
    }

    /// `pinned-downloads.json` in the download dir: `{ "<info hash>": [file
    /// indices] }`, the pin set as of the last change.
    pub fn pinned_downloads_path(&self) -> std::path::PathBuf {
        self.download_dir.join(PINNED_DOWNLOADS_FILE)
    }

    /// Write the current pin set -- the engines' pins plus the dormant ones
    /// -- to [`Self::pinned_downloads_path`] (atomically: temp file +
    /// rename). Best effort: a failure is logged, the in-memory pins stand.
    async fn persist_pinned_downloads(&self) {
        let live = self.pinned_downloads().await;
        let mut pins = self.dormant_pins.lock().clone();
        for pin in live {
            pins.entry(pin.info_hash).or_default().insert(pin.file_idx);
        }
        let pins: BTreeMap<String, Vec<usize>> = pins
            .into_iter()
            .map(|(hash, indices)| (hash, indices.into_iter().collect()))
            .collect();
        let path = self.pinned_downloads_path();
        if let Err(error) = write_pinned_downloads(&path, &pins).await {
            tracing::warn!(path = %path.display(), %error, "could not persist pinned downloads");
        }
    }

    /// Re-apply the pins persisted by the last run to the engines the
    /// backend restored (see [`Self::pin_download`]). Pins of torrents the
    /// backend does not have right now are kept dormant, in memory and in
    /// the file: librqbit skips (and keeps the record of) a torrent it
    /// cannot re-add at startup -- its output folder on a volume that is
    /// not mounted -- and brings it back on a later boot, when the pin
    /// must still be there; a `pin_download` of the torrent meanwhile
    /// applies them, an `unpin_download` drops them. Only a pin of a file
    /// the torrent does not have is dropped. Returns the number of pins
    /// restored. Called once at startup, after the engines are registered.
    pub async fn restore_pinned_downloads(&self) -> usize {
        let path = self.pinned_downloads_path();
        // Every path through the file yields a pin map, empty where it used
        // to return early. The tail of this function is what tells the
        // reconciler that the want-set is back on every restored torrent
        // (`reconcile::Conditions::settled`), and a boot with no pin file at
        // all -- which is most boots -- must reach it: skipping it would
        // leave every restored torrent stopped for good.
        let pins = match tokio::fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice::<BTreeMap<String, Vec<usize>>>(&bytes)
                .unwrap_or_else(|error| {
                    tracing::warn!(path = %path.display(), %error, "ignoring unreadable pinned downloads file");
                    BTreeMap::new()
                }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "could not read pinned downloads file");
                BTreeMap::new()
            }
        };
        let mut restored = 0;
        let mut dormant = BTreeMap::new();
        for (info_hash, indices) in &pins {
            let info_hash = info_hash.to_lowercase();
            let Some(engine) = self.get_engine(&info_hash).await else {
                dormant.insert(info_hash, indices.iter().copied().collect());
                continue;
            };
            for &file_idx in indices {
                if let Err(error) = engine.handle.pin_file(file_idx).await {
                    tracing::warn!(info_hash, file_idx, %error, "could not restore pin");
                    continue;
                }
                engine.pinned_files.write().insert(file_idx);
                restored += 1;
            }
            engine.touch();
        }
        let dormant_count = dormant.len();
        if dormant_count > 0 {
            tracing::info!(
                torrents = ?dormant.keys().collect::<Vec<_>>(),
                "keeping persisted pins of torrents the backend did not restore"
            );
            self.dormant_pins.lock().extend(dormant);
        }
        if restored > 0 || dormant_count > 0 {
            self.persist_pinned_downloads().await;
        }
        // The want-set is back, so the reconciler may start what should be
        // running (`reconcile::Conditions::settled`). Over *every* engine
        // and not only the pinned ones: what this call re-applies for a
        // torrent with no pins is the empty pin set, and that is as
        // re-applied as it will ever get. A torrent librqbit restored
        // paused for want of a want-set is otherwise never started by
        // anything, which is a restart that comes up downloading and
        // seeding nothing.
        for engine in self.engines.read().await.values() {
            engine.mark_settled();
        }
        tracing::info!(restored, "pinned_downloads_restored");
        restored
    }

    /// Reconcile the piece store against what this session actually holds:
    /// every torrent's pieces under the store root ([`Self::piece_store`])
    /// that nothing
    /// claims are deleted.
    ///
    /// Cleanup that only runs on the way out is cleanup that does not run.
    /// Android kills a backgrounded app without ceremony, so the process dies
    /// between a torrent being added and anything recording that it exists,
    /// and between a torrent being removed and its pieces going with it. What
    /// is left behind is not visible as a download, is not counted by anything
    /// that asks the engine what it holds, and nothing would ever reclaim it:
    /// exactly the invisible disk usage one file per piece exists to stop
    /// producing.
    ///
    /// A claim is *anything the session still has a record of*, never merely
    /// what came up on this boot. Three sources, and the third is the one that
    /// makes the difference between a sweep and a data loss:
    ///
    /// 1. The restored engines.
    /// 2. The dormant pins -- a pin whose torrent the backend did not restore
    ///    has no engine at all, and its data is the offline download the user
    ///    is waiting to come back.
    /// 3. Whatever librqbit's own session persistence still records
    ///    ([`piece_store::session_recorded_hashes`]): `session.json` and the
    ///    per-torrent `<hash>.bitv` / `.torrent` files in the persistence
    ///    folder, which is this engine's `download_dir`. A torrent librqbit
    ///    persisted and failed to restore on this boot -- an output volume
    ///    that is not mounted, an add that errored -- is in neither 1 nor 2,
    ///    and deleting its pieces would destroy a download the session's own
    ///    records still point at, on a boot where the *only* thing wrong was
    ///    the restore.
    ///
    /// Called once at startup, after the backend has restored its torrents and
    /// [`Self::restore_pinned_downloads`] has read the pin file, and before any
    /// route can add anything. That ordering is what makes it safe -- a
    /// torrent being added concurrently would have a directory and not yet a
    /// claim.
    pub async fn sweep_unadopted_pieces(&self) -> crate::piece_store::SweepReport {
        let mut adopted: std::collections::HashSet<String> = self
            .engines
            .read()
            .await
            .keys()
            .map(|info_hash| info_hash.to_lowercase())
            .collect();
        adopted.extend(
            self.dormant_pins
                .lock()
                .keys()
                .map(|info_hash| info_hash.to_lowercase()),
        );
        let root = self.piece_store();
        let persistence_folder = self.download_dir.clone();
        match tokio::task::spawn_blocking(move || {
            let mut adopted = adopted;
            adopted.extend(crate::piece_store::session_recorded_hashes(
                &persistence_folder,
            ));
            crate::piece_store::sweep_unadopted(&root, &adopted)
        })
        .await
        {
            Ok(report) => report,
            Err(error) => {
                tracing::warn!(%error, "the piece store sweep did not finish");
                crate::piece_store::SweepReport::default()
            }
        }
    }

    /// Every pinned download, ordered by info hash then file index.
    pub async fn pinned_downloads(&self) -> Vec<PinnedDownload> {
        let engines = self.engines.read().await;
        let mut pinned: Vec<PinnedDownload> = engines
            .iter()
            .flat_map(|(info_hash, engine)| {
                engine
                    .pinned_file_indices()
                    .into_iter()
                    .map(|file_idx| PinnedDownload {
                        info_hash: info_hash.clone(),
                        file_idx,
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        pinned.sort_by(|a, b| (&a.info_hash, a.file_idx).cmp(&(&b.info_hash, b.file_idx)));
        pinned
    }

    /// The pins [`Self::restore_pinned_downloads`] found no torrent for,
    /// ordered like [`Self::pinned_downloads`] -- persisted pins of a
    /// torrent the backend did not restore (a `.torrent` that will not
    /// parse, an add that errored). They are not downloading anything: the
    /// torrent comes back on a later boot, or with the next
    /// [`Self::pin_download`] of it, which applies them alongside its own.
    /// A caller listing downloads reports these as the stalled entries they
    /// are instead of losing them.
    pub fn dormant_pinned_downloads(&self) -> Vec<PinnedDownload> {
        self.dormant_pins
            .lock()
            .iter()
            .flat_map(|(info_hash, indices)| {
                indices.iter().map(|file_idx| PinnedDownload {
                    info_hash: info_hash.clone(),
                    file_idx: *file_idx,
                })
            })
            .collect()
    }

    /// The file exists, and the volume the pin's bytes land on has room for
    /// what it will write plus [`PIN_FREE_SPACE_MARGIN`]: the pinned file's
    /// missing bytes.
    ///
    /// **That volume is the piece store's root, for every torrent**
    /// ([`Self::piece_store`]), and
    /// there is no other candidate left to get it wrong: the store is the
    /// session's default storage and takes one root of its own (see
    /// `backend::librqbit::session_storage_factory`), and a pin chooses no
    /// location at all. It used to probe the placement,
    /// `<downloadsDir>/<infoHash>`, where no payload byte was ever written
    /// -- which passed a pin onto a full store and refused one that had all
    /// the room it needed.
    ///
    /// **The missing bytes and nothing else.** This used to size a pin that
    /// relocated the torrent onto another volume as a *copy* of every file
    /// with data in it, because the move rewrote each of them at the
    /// destination. Nothing moves any more.
    ///
    /// A volume that cannot be probed is not held against the pin (logged).
    ///
    /// A torrent that is still `checking` data that may already be there
    /// (`may_have_data_in_place`: it was managed before this call -- a
    /// restart, a stream) is not measured at all: `downloaded` reads 0
    /// until the check ends, so a complete file would be refused as if it
    /// had everything left to write -- and refusing changes nothing about a
    /// download librqbit already wants. A torrent this pin *added* is
    /// measured even while it checks: nothing of it is on disk, and the 0
    /// its files report is the truth about it.
    async fn check_pin_preconditions(
        &self,
        engine: &Arc<Engine<B::Handle>>,
        file_idx: usize,
        may_have_data_in_place: bool,
    ) -> Result<(), PinDownloadError> {
        let file_count = engine.handle.file_count().await;
        if file_idx >= file_count {
            return Err(PinDownloadError::FileNotFound {
                file_idx,
                file_count,
            });
        }
        let stats = engine.handle.stats().await;
        if may_have_data_in_place && stats.phase == crate::backend::StartupPhase::Checking {
            return Ok(());
        }
        let required = stats
            .files
            .get(file_idx)
            .map(|file| file.length.saturating_sub(file.downloaded))
            .unwrap_or(0);
        if required == 0 {
            return Ok(());
        }
        let volume = self.piece_store().path().to_path_buf();
        match probe_at_existing_ancestor(&*self.free_space_probe, &volume) {
            Ok(available) if free_space_allows(available, required, PIN_FREE_SPACE_MARGIN) => {
                Ok(())
            }
            Ok(available) => Err(PinDownloadError::InsufficientSpace {
                required: required.saturating_add(PIN_FREE_SPACE_MARGIN),
                available,
                margin: PIN_FREE_SPACE_MARGIN,
            }),
            Err(error) => {
                tracing::warn!(
                    info_hash = %engine.info_hash,
                    path = %volume.display(),
                    %error,
                    "could not probe free space; pinning anyway"
                );
                Ok(())
            }
        }
    }

    #[cfg(test)]
    fn set_free_space_probe(
        &mut self,
        probe: impl Fn(&std::path::Path) -> std::io::Result<u64> + Send + Sync + 'static,
    ) {
        self.free_space_probe = Arc::new(probe);
    }

    /// Forget that `file_idx` of `info_hash` is being played: its active
    /// selection, its lease and its stream count go, as
    /// `activate_multifile_file` drops them for the files a new selection
    /// supersedes. What a delete needs before it reconciles -- the want-set
    /// is planned from this bookkeeping, and a file still registered as the
    /// active one is unioned back into `only_files` however it was deleted.
    /// A selection naming another file of the torrent is left alone: that
    /// file keeps playing.
    async fn forget_playback_of(&self, info_hash: &str, file_idx: usize) {
        {
            let mut selections = self.active_multifile_files.write().await;
            if selections
                .get(info_hash)
                .is_some_and(|selection| selection.file_idx == file_idx)
            {
                selections.remove(info_hash);
            }
        }
        let key = (info_hash.to_string(), file_idx);
        self.active_file_streams.write().await.remove(&key);
        {
            let mut active = self.active_file.write().await;
            if active.as_ref() == Some(&key) {
                *active = None;
            }
        }
    }

    /// Re-plan the engine's want-set from whatever multi-file selection is
    /// currently active (or none) so a pin change is applied without
    /// disturbing playback. The one hot-file caller (`routes/stream.rs`)
    /// always passes the active file as the hot file, so dropping the hot
    /// plan here loses nothing.
    async fn reconcile_with_active_selection(
        &self,
        engine: Arc<Engine<B::Handle>>,
        reason: &'static str,
    ) {
        let selection = self
            .active_multifile_files
            .read()
            .await
            .get(&engine.info_hash)
            .cloned();
        Self::reconcile_multifile_engine(
            engine,
            selection.as_ref().map(|s| s.file_idx),
            None,
            selection.as_ref().map(|s| s.generation).unwrap_or(0),
            reason,
        )
        .await;
    }

    /// Called when a stream ends for a torrent file
    pub async fn on_stream_end(&self, info_hash: &str, file_idx: usize) {
        let info_hash = info_hash.to_lowercase();

        // Decremented and dropped at zero, not read: what the last stream
        // ending changes is a condition the reconciler reads for itself.
        {
            let mut streams = self.active_streams.write().await;
            if let Some(count) = streams.get_mut(&info_hash) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    streams.remove(&info_hash);
                }
            }
        }

        let file_streams_remaining = {
            let mut streams = self.active_file_streams.write().await;
            let key = (info_hash.clone(), file_idx);
            if let Some(count) = streams.get_mut(&key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    streams.remove(&key);
                    0
                } else {
                    *count
                }
            } else {
                0
            }
        };

        let native_lifecycle = if let Some(engine) = self.get_engine(&info_hash).await {
            // Reset idle age so removal happens after the stream becomes
            // inactive, not after the stream originally started.
            engine.touch();
            engine.handle.manages_playback_lifecycle()
        } else {
            false
        };

        // Nothing schedules a pause here any more, and nothing stamps
        // anything either. The last stream ending is not a decision, it is
        // a change of condition: the reconciler reads `idle_for` from
        // `Engine::last_active_at`, which was stamped while this stream was
        // *running* -- by `on_stream_start`'s own reconcile and by every
        // tick that read the registers true since -- and stops the torrent
        // on the first tick after `INACTIVE_TORRENT_PAUSE_GRACE` of quiet.
        // One ladder, with no task per stream deciding a second time.

        if !native_lifecycle && file_streams_remaining == 0 {
            self.schedule_file_cleanup(info_hash.clone(), file_idx)
                .await;
        }

        let remaining = self.active_streams.read().await.values().sum::<usize>();
        tracing::debug!(
            "Stream ended for {} file_idx={}, total active streams: {}, file streams remaining: {}",
            info_hash,
            file_idx,
            remaining,
            file_streams_remaining
        );
    }

    async fn schedule_file_cleanup(&self, info_hash: String, file_idx: usize) {
        drop(
            self.schedule_file_cleanup_after(info_hash, file_idx, Duration::from_secs(5))
                .await,
        );
    }

    async fn schedule_file_cleanup_after(
        &self,
        info_hash: String,
        file_idx: usize,
        delay: Duration,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if self
            .get_engine(&info_hash)
            .await
            .is_some_and(|engine| engine.handle.manages_playback_lifecycle())
        {
            return None;
        }
        let engines = self.engines.clone();
        let active_file = self.active_file.clone();
        let active_file_streams = self.active_file_streams.clone();
        let active_multifile_files = self.active_multifile_files.clone();
        let scheduled_generation = {
            let selections = self.active_multifile_files.read().await;
            selections
                .get(&info_hash)
                .filter(|selection| selection.file_idx == file_idx)
                .map(|selection| selection.generation)
        };

        Some(tokio::spawn(async move {
            tokio::time::sleep(delay).await;

            let key = (info_hash.clone(), file_idx);
            let still_active = {
                let streams = active_file_streams.read().await;
                streams.get(&key).copied().unwrap_or(0) > 0
            };
            if still_active {
                tracing::debug!(
                    "Skipping delayed cleanup for {} idx={} because a new stream started",
                    info_hash,
                    file_idx
                );
                return;
            }

            let active_selection = {
                let mut selections = active_multifile_files.write().await;
                match selections.get(&info_hash).cloned() {
                    Some(selection) if selection.file_idx == file_idx => {
                        if scheduled_generation == Some(selection.generation) {
                            selections.remove(&info_hash);
                            tracing::info!(
                                info_hash = %info_hash,
                                file_idx,
                                generation = selection.generation,
                                reason = "delayed-cleanup",
                                "multifile_active_file_cleared"
                            );
                            None
                        } else {
                            tracing::debug!(
                                info_hash = %info_hash,
                                file_idx,
                                scheduled_generation,
                                active_generation = selection.generation,
                                "Skipping delayed cleanup because multi-file selection is newer"
                            );
                            return;
                        }
                    }
                    Some(selection) => Some(selection),
                    None => None,
                }
            };

            {
                let mut active = active_file.write().await;
                if let Some((ref h, idx)) = *active
                    && h == &info_hash
                    && idx == file_idx
                {
                    tracing::info!(
                        "Delayed cleanup: clearing active file for {} file_idx={}",
                        info_hash,
                        file_idx
                    );
                    *active = active_selection
                        .as_ref()
                        .map(|selection| (info_hash.clone(), selection.file_idx));
                }
            }

            let engine = {
                let engines = engines.read().await;
                engines.get(&info_hash).cloned()
            };
            if let Some(engine) = engine {
                let reconciled = Self::reconcile_multifile_engine(
                    engine.clone(),
                    active_selection
                        .as_ref()
                        .map(|selection| selection.file_idx),
                    None,
                    active_selection
                        .as_ref()
                        .map(|selection| selection.generation)
                        .or(scheduled_generation)
                        .unwrap_or(0),
                    "delayed-cleanup",
                )
                .await;
                if reconciled {
                    tracing::info!(
                        info_hash = %info_hash,
                        file_idx,
                        active_file = active_selection.as_ref().map(|selection| selection.file_idx),
                        "Delayed cleanup: reconciled multi-file priorities"
                    );
                    return;
                }

                if let Err(e) = engine.handle.clear_file_streaming(file_idx).await {
                    tracing::warn!(
                        "Failed to clear file priorities for {} idx={}: {}",
                        info_hash,
                        file_idx,
                        e
                    );
                } else {
                    tracing::info!(
                        "Delayed cleanup: cleared file priorities for {} idx={}",
                        info_hash,
                        file_idx
                    );
                }
            }
        }))
    }

    /// Get a reference to the backend for direct access
    pub fn get_backend(&self) -> &Arc<B> {
        &self.backend
    }
}

impl BackendEngineFS<LibrqbitBackend> {
    pub async fn new(
        root_dir: std::path::PathBuf,
        _cache_config: EngineCacheConfig,
    ) -> Result<Self> {
        let download_dir = root_dir.join("rqbit-downloads");
        let (backend, restored) = LibrqbitBackend::new(
            download_dir.clone(),
            TorrentListenPort::default(),
            Vec::new(),
            crate::backend::dht_bootstrap::DhtBootstrapDns::default().resolvers_in(&download_dir),
        )
        .await?;
        let efs = Self::new_with_backend(backend, restored, root_dir.join("cache"), download_dir);
        efs.restore_pinned_downloads().await;
        efs.sweep_unadopted_pieces().await;
        Ok(efs)
    }

    /// What of `config` reaches librqbit: `listen_port`,
    /// `dht_bootstrap_nodes` and `dht_bootstrap_dns` as the session's
    /// listener and DHT, and of `speed_profile` and `privacy` exactly what
    /// [`crate::backend::librqbit::SessionTuning::from_settings`] can
    /// express -- the rest of the `bt*` settings has no librqbit knob, and
    /// [`crate::backend::librqbit::bt_settings_support`] says so setting by
    /// setting. All of it is read once, here; `update_torrent_settings` is
    /// what a later change gets.
    pub async fn new_with_storage(
        root_dir: std::path::PathBuf,
        config: crate::backend::BackendConfig,
        tracker_storage: Option<Arc<dyn crate::trackers::TrackerStorage>>,
    ) -> Result<Self> {
        let download_dir = root_dir.join("rqbit-downloads");
        let (backend, restored) = LibrqbitBackend::new_with_settings(
            download_dir.clone(),
            config.listen_port,
            config.dht_bootstrap_nodes,
            config.dht_bootstrap_dns.resolvers_in(&download_dir),
            crate::backend::librqbit::SessionTuning::from_settings(
                &config.speed_profile,
                &config.privacy,
            ),
        )
        .await?;
        let efs = Self::new_with_backend_and_storage(
            backend,
            restored,
            root_dir.join("cache"),
            download_dir,
            tracker_storage,
        );
        efs.restore_pinned_downloads().await;
        efs.sweep_unadopted_pieces().await;
        Ok(efs)
    }

    /// librqbit sessions always persist downloads to disk, so the disk-backed
    /// constructor is the same as the regular one.
    pub async fn new_disk_backed(
        root_dir: std::path::PathBuf,
        config: crate::backend::BackendConfig,
        tracker_storage: Option<Arc<dyn crate::trackers::TrackerStorage>>,
    ) -> Result<Self> {
        Self::new_with_storage(root_dir, config, tracker_storage).await
    }

    /// Push a `bt*` settings change at the running session and report what
    /// became of it ([`crate::backend::BtSettingsReport`]): the download
    /// limit is applied now, a session-start setting that changed is
    /// listed as waiting for the next start, and the settings librqbit has
    /// no knob for are listed as not honoured -- every time, so the answer
    /// a client gets is never a bare echo of what it sent. Until this
    /// returned something, every one of these settings was accepted,
    /// persisted and documented, and none reached librqbit.
    pub async fn update_torrent_settings(
        &self,
        profile: &crate::backend::TorrentSpeedProfile,
        privacy: &crate::backend::TorrentPrivacyConfig,
    ) -> crate::backend::BtSettingsReport {
        let report = self.backend.apply_settings(profile, privacy);
        tracing::info!(
            applied_live = ?report.applied_live,
            pending_restart = ?report.pending_restart,
            "torrent session settings updated"
        );
        report
    }

    /// Shrink to, or grow back from, a [`Footprint`] -- see the enum for
    /// what `Lean` sheds and what it deliberately keeps (seeding). Forwarded
    /// to the backend, which applies it to every torrent now and to every
    /// one added later; nothing at the engine level changes, since no
    /// engine is paused or dropped for it. Idempotent, synchronous, cheap.
    pub fn set_footprint(&self, footprint: Footprint) {
        self.backend.set_footprint(footprint);
    }

    /// The [`Footprint`] in force.
    pub fn footprint(&self) -> Footprint {
        self.backend.footprint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::librqbit::{DeferredSelection, await_initialized};
    use crate::backend::{
        BackendFileInfo, EngineStats, FileStreamTrait, Growler, PeerDiscovery, PeerSearch,
        PieceReadiness, RunState, StartupPhase, StatsFile, StatsOptions, SwarmCap,
        TorrentFilePriorityPlan,
    };
    use crate::reconcile::{Decision, Trigger};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    const TEST_HASH: &str = "0123456789abcdef0123456789abcdef01234567";

    #[derive(Default)]
    struct FakeCounters {
        clear_file_streaming: AtomicUsize,
        reconcile_file_priorities: AtomicUsize,
        prepare_file_for_streaming: AtomicUsize,
        get_file_reader: AtomicUsize,
        /// Selection updates / reader opens that went through while the fake
        /// torrent was still initializing. The gate must keep this at zero.
        applied_while_initializing: AtomicUsize,
        last_active_file: Mutex<Option<usize>>,
        last_generation: AtomicU64,
        /// Test knob: the piece indices `drop_file_pieces` hands back as
        /// the ones the backend has agreed to forget. Empty by default,
        /// which is a backend with nothing had.
        drops_pieces: Mutex<Vec<u32>>,
        /// Test knob: report every file as fully on disk (seeded torrent)
        /// instead of the default half-downloaded state.
        seeded: AtomicBool,
        pin_file: AtomicUsize,
        unpin_file: AtomicUsize,
        /// The ranges `drop_pieces` was asked to forget and what each was
        /// to do afterwards, in order -- every reclaim must ask before it
        /// removes a byte, and the per-file delete must ask for the
        /// re-select its still-pinned neighbour needs.
        dropped_ranges: Mutex<Vec<(std::ops::Range<u32>, crate::backend::AfterRelease)>>,
        /// Test knob: the backend keeps a have-set and will not give the
        /// pieces up -- a torrent added without piece reclaim, or one whose
        /// state has no chunk tracker to edit.
        refuses_drop: AtomicBool,
        /// Test knob: the backend forgets exactly the pieces it was asked to
        /// forget, which is librqbit's answer when no peer is mid-flight and
        /// no reader is inside the range. `drops_pieces` is the other shape
        /// -- a fixed set it will part with whatever it is asked -- and a
        /// test that needs to see *which* pieces a caller asked about, on the
        /// disk rather than in a recorded call, needs this one.
        drops_what_it_is_asked: AtomicBool,
        /// Test knob: the files this fake torrent still wants bytes of.
        /// `None` -- the default -- is every one of them, which is
        /// librqbit's own spelling of a want-set nothing has narrowed.
        wanted_files: Mutex<Option<std::collections::BTreeSet<usize>>>,
        /// Every `set_pieces_advertised` call, in order: which range, and
        /// whether it was put into what we announce or held back out of it.
        advertised: Mutex<Vec<(std::ops::Range<u32>, bool)>>,
        /// Test knob: park the next `set_pieces_advertised` call. The fake
        /// sends on the first channel as it enters the call and waits on
        /// the second before returning, so a test can ask its questions
        /// with a retention pass really in flight -- the policy out of its
        /// slot and a backend call awaited -- instead of racing a sleep
        /// against one.
        advertise_gate: Mutex<
            Option<(
                tokio::sync::oneshot::Sender<()>,
                tokio::sync::oneshot::Receiver<()>,
            )>,
        >,
        /// The fake handle's own pin set (what the real backend keeps in its
        /// `PinnedFiles` map), reported through `stats()`.
        pinned: Mutex<std::collections::BTreeSet<usize>>,
        /// The folder the fake says its files are in, which only
        /// `file_path` reads; a test sets it where librqbit's own
        /// placement would put the files.
        output_folder: Mutex<Option<std::path::PathBuf>>,
        /// Test knob: the backend stopped this torrent because the volume is
        /// full, as librqbit does on an ENOSPC write.
        out_of_space: AtomicBool,
        /// Test knob: the backend stopped this torrent with an error (of
        /// any kind -- `out_of_space` says which). A dead torrent is this
        /// set and `out_of_space` clear.
        in_error_state: AtomicBool,
        /// What the fake torrent has moved over the connection, reported
        /// through `transfer_totals()`; a test adds to these where peers
        /// would.
        fetched: AtomicU64,
        uploaded: AtomicU64,
        /// Test knob: how many pieces each file of the fake torrent spans.
        /// Zero -- the default -- is one piece per file, which is the
        /// geometry every test that does not set this was written against.
        /// A test that needs a playhead to have pieces on both sides of it
        /// raises it.
        pieces_per_file: AtomicU64,
        /// How many times the torrent was put back to work after that.
        restart_from_error: AtomicUsize,
        /// How many times the reconciler stopped and started the torrent.
        stop_torrent: AtomicUsize,
        start_torrent: AtomicUsize,
        /// Whether the fake torrent is stopped, set by the calls that stop
        /// one and cleared by the calls that start it again. Without it the
        /// fake torrent is always running, and anything that reconciles a
        /// run state would be tested against a torrent that never obeys.
        paused: AtomicBool,
        /// How many times `has_metadata` was asked -- the reconciler's tick
        /// asks every torrent once per pass, so this is how a test sees the
        /// loop actually running.
        has_metadata: AtomicUsize,
        /// Test knob: this handle pauses and resumes its own torrent, like
        /// the native Android lifecycle backend. Everything at this layer
        /// that decides when a torrent should run has to leave it alone.
        native_lifecycle: AtomicBool,
        /// Test knob: `start_torrent` answers `Ok` and leaves the torrent
        /// stopped, which is what librqbit does when an initial check is in
        /// flight -- `Session::unpause` clears the persisted flag and
        /// returns, and the check's continuation parks the torrent back in
        /// `Paused` from the `start_paused` it captured before the unpause
        /// arrived.
        swallow_start: AtomicBool,
        /// Test knobs: hold `stop_torrent` / `start_torrent` open at the
        /// matching gate until the test releases it, so a test can see what
        /// the caller is still holding while it awaits the backend.
        ///
        /// A slow stop is the ordinary case, not a contrived one:
        /// `Session::pause` and `Session::unpause` both flush librqbit's
        /// persistence file before they return. A caller that holds the
        /// engine registry across one is invisible against a fake that
        /// answers in the same poll.
        hold_stop: AtomicBool,
        stop_gate: tokio::sync::Notify,
        hold_start: AtomicBool,
        start_gate: tokio::sync::Notify,
    }

    /// Simulates librqbit's `Initializing` state for the fake torrent: the
    /// fake handle's reader/selection paths go through the same
    /// `await_initialized` gate and `DeferredSelection` machinery as the real
    /// backend, with `wait_future` standing in for
    /// `ManagedTorrent::wait_until_initialized`.
    struct FakeInit {
        ready: AtomicBool,
        notify: tokio::sync::Notify,
        timeout: Duration,
        deferred: Arc<DeferredSelection<TorrentFilePriorityPlan>>,
    }

    impl FakeInit {
        fn new(ready: bool, timeout: Duration) -> Arc<Self> {
            Arc::new(Self {
                ready: AtomicBool::new(ready),
                notify: tokio::sync::Notify::new(),
                timeout,
                deferred: DeferredSelection::new(),
            })
        }

        fn is_ready(&self) -> bool {
            self.ready.load(Ordering::SeqCst)
        }

        fn mark_ready(&self) {
            self.ready.store(true, Ordering::SeqCst);
            self.notify.notify_waiters();
        }

        /// Owned future that resolves once `mark_ready` has been called
        /// (polls like librqbit's implementation so a missed notify is
        /// harmless).
        fn wait_future(
            self: &Arc<Self>,
        ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send + 'static {
            let me = self.clone();
            async move {
                loop {
                    if me.is_ready() {
                        return Ok(());
                    }
                    let _ = tokio::time::timeout(Duration::from_millis(100), me.notify.notified())
                        .await;
                }
            }
        }
    }

    #[derive(Clone)]
    struct FakeHandle {
        info_hash: String,
        counters: Arc<FakeCounters>,
        files: Vec<BackendFileInfo>,
        init: Arc<FakeInit>,
    }

    impl FakeHandle {
        async fn gate(&self) -> Result<()> {
            await_initialized(&self.info_hash, self.init.timeout, self.init.wait_future()).await?;
            if !self.init.is_ready() {
                self.counters
                    .applied_while_initializing
                    .fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }

        fn record_reconcile(&self, plan: &TorrentFilePriorityPlan) {
            if !self.init.is_ready() {
                self.counters
                    .applied_while_initializing
                    .fetch_add(1, Ordering::SeqCst);
            }
            self.counters
                .reconcile_file_priorities
                .fetch_add(1, Ordering::SeqCst);
            *self.counters.last_active_file.lock().unwrap() = plan.active_file;
            self.counters
                .last_generation
                .store(plan.generation, Ordering::SeqCst);
        }
    }

    struct FakeBackend {
        /// Every torrent the fake session holds, keyed by info hash. The
        /// first one stands in for whatever `add_torrent` is asked for.
        handles: Vec<FakeHandle>,
        /// Info hashes `remove_torrent` was asked to drop, in order.
        removed: Arc<Mutex<Vec<String>>>,
        /// Info hashes `remove_torrent_and_files` was asked to drop.
        removed_with_files: Arc<Mutex<Vec<String>>>,
        /// The placement of every `add_torrent_placed`, in order.
        placements: Arc<Mutex<Vec<TorrentPlacement>>>,
        /// Test knob: `get_torrent` finds nothing (the torrent is gone from
        /// the session).
        hide_torrents: Arc<AtomicBool>,
        /// Test knob: while set, `add_torrent_placed` blocks (after
        /// recording the placement) until the test adds a permit to
        /// `add_hold`, standing in for metadata still resolving.
        hold_add: Arc<AtomicBool>,
        add_hold: Arc<tokio::sync::Semaphore>,
    }

    impl FakeBackend {
        fn new(handles: Vec<FakeHandle>) -> Self {
            Self {
                handles,
                removed: Arc::new(Mutex::new(Vec::new())),
                removed_with_files: Arc::new(Mutex::new(Vec::new())),
                placements: Arc::new(Mutex::new(Vec::new())),
                hide_torrents: Arc::new(AtomicBool::new(false)),
                hold_add: Arc::new(AtomicBool::new(false)),
                add_hold: Arc::new(tokio::sync::Semaphore::new(0)),
            }
        }
    }

    #[async_trait::async_trait]
    impl TorrentBackend for FakeBackend {
        type Handle = FakeHandle;

        /// A freshly added torrent is running, as a real backend's is --
        /// including one added again after an eviction took the last copy
        /// away. The handle is shared with whatever engine held it before,
        /// so without this an add would inherit that torrent's pause.
        async fn add_torrent(
            &self,
            _source: TorrentSource,
            _trackers: Vec<String>,
        ) -> Result<Self::Handle> {
            let handle = self.handles[0].clone();
            handle.counters.paused.store(false, Ordering::SeqCst);
            Ok(handle)
        }

        /// This backend's one torrent is `TEST_HASH`, whatever the source
        /// says -- as `add_torrent` above already assumes. Implemented so
        /// the checks `EngineFS::add_torrent` makes *before* handing a
        /// source to a backend can be tested at all.
        fn source_info_hash(&self, _source: &TorrentSource) -> Option<String> {
            Some(TEST_HASH.to_string())
        }

        async fn add_torrent_placed(
            &self,
            _source: TorrentSource,
            _trackers: Vec<String>,
            placement: TorrentPlacement,
        ) -> Result<Self::Handle> {
            let handle = self.handles[0].clone();
            handle.counters.paused.store(false, Ordering::SeqCst);
            self.placements.lock().unwrap().push(placement);
            if self.hold_add.load(Ordering::SeqCst) {
                self.add_hold.acquire().await.unwrap().forget();
            }
            Ok(handle)
        }

        async fn get_torrent(&self, info_hash: &str) -> Option<Self::Handle> {
            if self.hide_torrents.load(Ordering::SeqCst) {
                return None;
            }
            self.handles
                .iter()
                .find(|h| h.info_hash == info_hash)
                .cloned()
        }

        async fn remove_torrent(&self, info_hash: &str) -> Result<()> {
            self.removed.lock().unwrap().push(info_hash.to_string());
            Ok(())
        }

        async fn remove_torrent_and_files(&self, info_hash: &str) -> Result<()> {
            self.removed_with_files
                .lock()
                .unwrap()
                .push(info_hash.to_string());
            Ok(())
        }

        async fn list_torrents(&self) -> Vec<String> {
            self.handles.iter().map(|h| h.info_hash.clone()).collect()
        }

        async fn memory_diagnostics(&self) -> BackendMemoryDiagnostics {
            BackendMemoryDiagnostics::default()
        }
    }

    impl FakeHandle {
        /// The `pieces_per_file` knob, read as a count rather than as a raw
        /// zero: see [`FakeCounters::pieces_per_file`].
        fn pieces_per_file(&self) -> u64 {
            self.counters.pieces_per_file.load(Ordering::SeqCst).max(1)
        }

        /// The fake torrent's layout: its files in order, at a piece length
        /// of the first file's length over `pieces_per_file`. Nothing for a
        /// torrent with no files or one whose pieces would be zero bytes
        /// long, neither of which is a torrent.
        fn layout(&self) -> Option<std::sync::Arc<crate::piece_store::PieceLayout>> {
            let piece_length = self.piece_length()?;
            let total: u64 = self.files.iter().map(|file| file.length).sum();
            crate::piece_store::PieceLayout::new(
                piece_length,
                total,
                self.files
                    .iter()
                    .map(|file| crate::piece_store::FileSpec::payload(file.length)),
            )
            .ok()
            .map(std::sync::Arc::new)
        }
    }

    #[async_trait::async_trait]
    impl TorrentHandle for FakeHandle {
        fn info_hash(&self) -> String {
            self.info_hash.clone()
        }

        /// The counters, and **only while this fake torrent is running** --
        /// librqbit keeps them in its live state and has none to read for a
        /// torrent that is paused, checking, out of space or in error, so a
        /// fake that answered zero for those would hide exactly the bug
        /// that costs a viewer their session's numbers.
        fn transfer_totals(&self) -> Option<crate::backend::TransferTotals> {
            (self.run_state() == crate::backend::RunState::Live).then(|| {
                crate::backend::TransferTotals {
                    fetched: self.counters.fetched.load(Ordering::SeqCst),
                    uploaded: self.counters.uploaded.load(Ordering::SeqCst),
                }
            })
        }

        fn name(&self) -> Option<String> {
            Some("fake".to_string())
        }

        async fn stats(&self) -> EngineStats {
            // Mirror the real backend's phase derivation: no files stands in
            // for missing metadata, a not-yet-ready FakeInit for librqbit's
            // Initializing hash check, and the seeded knob for a finished
            // torrent. The piece map (initial windows) only exists once
            // initialized.
            let initialized = self.init.is_ready();
            let seeded = self.counters.seeded.load(Ordering::SeqCst);
            let phase = if self.files.is_empty() {
                StartupPhase::ResolvingMetadata
            } else if !initialized {
                StartupPhase::Checking
            } else if seeded {
                StartupPhase::Ready
            } else {
                StartupPhase::Buffering
            };
            let total_len: u64 = self.files.iter().map(|f| f.length).sum();
            let pinned = self.counters.pinned.lock().unwrap().clone();
            let mut offset = 0u64;
            let files = self
                .files
                .iter()
                .enumerate()
                .map(|(idx, file)| {
                    let downloaded = if seeded { file.length } else { file.length / 2 };
                    let window = initialized.then(|| {
                        let total = file
                            .length
                            .min(crate::backend::priorities::MAX_STARTUP_WINDOW_BYTES);
                        (downloaded.min(total), total)
                    });
                    let stats_file = StatsFile {
                        name: file.name.clone(),
                        path: file.name.clone(),
                        length: file.length,
                        offset,
                        downloaded,
                        progress: if seeded { 1.0 } else { 0.5 },
                        initial_window_ready_bytes: window.map(|(ready, _)| ready),
                        initial_window_bytes: window.map(|(_, total)| total),
                        in_flight_piece: None,
                        pinned: pinned.contains(&idx),
                        complete: seeded,
                    };
                    offset += file.length;
                    stats_file
                })
                .collect();
            EngineStats {
                name: "fake".to_string(),
                info_hash: self.info_hash.clone(),
                piece_length: None,
                files,
                sources: vec![],
                opts: StatsOptions {
                    connections: None,
                    dht: true,
                    growler: Growler::default(),
                    handshake_timeout: None,
                    path: String::new(),
                    peer_search: PeerSearch::default(),
                    swarm_cap: SwarmCap::default(),
                    timeout: None,
                    tracker: true,
                    r#virtual: false,
                },
                download_speed: 0.0,
                upload_speed: 0.0,
                downloaded: 50,
                uploaded: 0,
                unchoked: 0,
                peers: 0,
                queued: 0,
                unique: 0,
                connection_tries: 0,
                peer_search_running: false,
                stream_len: 100,
                stream_name: "video.mkv".to_string(),
                stream_progress: 0.5,
                swarm_connections: 0,
                swarm_paused: false,
                swarm_size: 0,
                connected_seeders: 0,
                swarm_seeders: None,
                swarm_leechers: None,
                swarm_scrape_age_secs: None,
                is_finished: seeded,
                has_metadata: !self.files.is_empty(),
                phase,
                checked_bytes: (phase == StartupPhase::Checking).then_some(0),
                check_total_bytes: (phase == StartupPhase::Checking).then_some(total_len),
                initial_window_ready_bytes: None,
                initial_window_bytes: None,
                in_flight_piece: None,
                peer_discovery: PeerDiscovery::default(),
                error: None,
                pinned_files: pinned.into_iter().collect(),
            }
        }

        async fn add_trackers(&self, _trackers: Vec<String>) -> Result<()> {
            Ok(())
        }

        async fn pin_file(&self, file_idx: usize) -> Result<()> {
            if file_idx >= self.files.len() {
                anyhow::bail!("file index {file_idx} out of range");
            }
            self.counters.pin_file.fetch_add(1, Ordering::SeqCst);
            self.counters.pinned.lock().unwrap().insert(file_idx);
            Ok(())
        }

        async fn unpin_file(&self, file_idx: usize) -> Result<()> {
            self.counters.unpin_file.fetch_add(1, Ordering::SeqCst);
            self.counters.pinned.lock().unwrap().remove(&file_idx);
            Ok(())
        }

        /// Where the file lies in the fake torrent's pieces, read off the
        /// same layout the piece store's own arithmetic is built on.
        ///
        /// Equal-length files that divide evenly by `pieces_per_file` -- the
        /// geometry every fixture here has -- get a piece range of their own,
        /// which is what this used to compute by hand. Files of *different*
        /// lengths share their boundary pieces exactly as a real torrent
        /// without BEP-47 padding does, and that is a thing a test needs to
        /// be able to say.
        async fn file_pieces(&self, file_idx: usize) -> Option<crate::backend::FilePieceSpan> {
            let layout = self.layout()?;
            let pieces = layout.pieces_overlapping_file(file_idx).ok()?;
            if pieces.is_empty() {
                return None;
            }
            let first = layout.piece_offset(pieces.start);
            let last = layout
                .piece_offset(pieces.end - 1)
                .saturating_add(layout.piece_length_of(pieces.end - 1));
            Some(crate::backend::FilePieceSpan {
                pieces,
                offset: self.files[..file_idx].iter().map(|file| file.length).sum(),
                bytes: last - first,
            })
        }

        async fn file_wants(&self) -> Option<crate::backend::FileWants> {
            Some(crate::backend::FileWants {
                layout: self.layout()?,
                wanted: self.counters.wanted_files.lock().unwrap().clone(),
            })
        }

        async fn set_pieces_advertised(
            &self,
            pieces: std::ops::Range<u32>,
            advertised: bool,
        ) -> Result<usize> {
            // Parked inside the call, if a test asked for it: see
            // `FakeCounters::advertise_gate`.
            let gate = self.counters.advertise_gate.lock().unwrap().take();
            if let Some((entered, release)) = gate {
                let _ = entered.send(());
                let _ = release.await;
            }
            let count = (pieces.end - pieces.start) as usize;
            self.counters
                .advertised
                .lock()
                .unwrap()
                .push((pieces, advertised));
            Ok(count)
        }

        /// One piece per file, matching `file_pieces` above, so a policy
        /// can be sized without a layout.
        fn piece_length(&self) -> Option<u64> {
            Some(self.files.first()?.length / self.pieces_per_file())
        }

        async fn drop_pieces(
            &self,
            pieces: std::ops::Range<u32>,
            after: crate::backend::AfterRelease,
        ) -> Result<Option<crate::backend::DroppedFilePieces>> {
            let asked = pieces.clone();
            self.counters
                .dropped_ranges
                .lock()
                .unwrap()
                .push((pieces, after));
            if self.counters.refuses_drop.load(Ordering::SeqCst) {
                anyhow::bail!("this fake will not forget a piece it has");
            }
            let dropped = if self.counters.drops_what_it_is_asked.load(Ordering::SeqCst) {
                asked.collect()
            } else {
                self.counters.drops_pieces.lock().unwrap().clone()
            };
            Ok(Some(crate::backend::DroppedFilePieces::new(dropped, ())))
        }

        /// Like the real backend: the folder the backend says it writes to
        /// joined with the file's name, unknown without a folder. The
        /// folder is the fake's own bookkeeping (`FakeCounters`), as
        /// librqbit's is librqbit's: no layer above the backend chooses it
        /// or reads it back.
        async fn file_path(&self, file_idx: usize) -> Option<std::path::PathBuf> {
            let folder = self.counters.output_folder.lock().unwrap().clone()?;
            Some(folder.join(&self.files.get(file_idx)?.name))
        }

        /// The fake's state machine, from the knobs its tests set: the
        /// error ones first (as librqbit's error state outranks its pause),
        /// then `FakeInit` for a check still running, then the `paused` flag
        /// the four calls that stop and start a torrent write. A test that
        /// reconciles run states needs the last of those: without it the
        /// fake torrent is always running, and the reconciler would only
        /// ever be tested against a torrent that never obeys it.
        fn run_state(&self) -> RunState {
            if self.counters.in_error_state.load(Ordering::SeqCst)
                || self.counters.out_of_space.load(Ordering::SeqCst)
            {
                RunState::Error
            } else if !self.init.is_ready() {
                RunState::Initializing {
                    pause_requested: self.counters.paused.load(Ordering::SeqCst),
                }
            } else if self.counters.paused.load(Ordering::SeqCst) {
                RunState::Paused
            } else {
                RunState::Live
            }
        }

        fn manages_playback_lifecycle(&self) -> bool {
            self.counters.native_lifecycle.load(Ordering::SeqCst)
        }

        /// Counted, so a test can see the reconciler's tick reach this
        /// torrent; the answer itself is the fake's files, as the real
        /// backend's is its metadata slot.
        async fn has_metadata(&self) -> bool {
            self.counters.has_metadata.fetch_add(1, Ordering::SeqCst);
            !self.files.is_empty()
        }

        async fn is_out_of_space(&self) -> bool {
            self.counters.out_of_space.load(Ordering::SeqCst)
        }

        async fn is_finished(&self) -> bool {
            self.counters.seeded.load(Ordering::SeqCst)
        }

        /// Refuses a torrent that is already stopped, as librqbit's
        /// `Session::pause` does. Without that a reconciler pausing the
        /// same torrent every tick would look exactly like one that pauses
        /// it once.
        ///
        /// The counter counts the *asking*, not the succeeding, for the
        /// same reason: a caller that asks every two seconds and is refused
        /// every two seconds has a bug -- a log line per stopped torrent
        /// per tick -- and a counter that only saw the first call could not
        /// tell it from a caller that asks once.
        async fn stop_torrent(&self) -> Result<()> {
            self.counters.stop_torrent.fetch_add(1, Ordering::SeqCst);
            if self.counters.hold_stop.load(Ordering::SeqCst) {
                self.counters.stop_gate.notified().await;
            }
            if self.counters.paused.swap(true, Ordering::SeqCst) {
                anyhow::bail!("already paused");
            }
            Ok(())
            // Recorded nowhere: nothing in this design remembers why.
        }

        /// Refuses a torrent that is not stopped, as `Session::unpause`
        /// does -- and, with `swallow_start` set, answers `Ok` while
        /// leaving it stopped, which is what librqbit does to an unpause
        /// that lands during an initial check.
        async fn start_torrent(&self) -> Result<()> {
            if !self.counters.paused.load(Ordering::SeqCst) {
                anyhow::bail!("not paused");
            }
            self.counters.start_torrent.fetch_add(1, Ordering::SeqCst);
            if self.counters.hold_start.load(Ordering::SeqCst) {
                self.counters.start_gate.notified().await;
            }
            if !self.counters.swallow_start.load(Ordering::SeqCst) {
                self.counters.paused.store(false, Ordering::SeqCst);
            }
            Ok(())
        }

        async fn is_in_error_state(&self) -> bool {
            self.counters.in_error_state.load(Ordering::SeqCst)
                || self.counters.out_of_space.load(Ordering::SeqCst)
        }

        async fn restart_from_error(&self) -> Result<()> {
            self.counters
                .restart_from_error
                .fetch_add(1, Ordering::SeqCst);
            self.counters.out_of_space.store(false, Ordering::SeqCst);
            self.counters.paused.store(false, Ordering::SeqCst);
            Ok(())
        }

        /// Mirrors the real backend: apply directly when ready, otherwise
        /// park the plan (latest wins) until the fake initializes.
        async fn reconcile_file_priorities(&self, plan: TorrentFilePriorityPlan) -> Result<()> {
            if self.init.is_ready() {
                self.init.deferred.supersede();
                self.record_reconcile(&plan);
                return Ok(());
            }
            let applier = self.clone();
            self.init.deferred.defer(
                plan,
                {
                    let info_hash = self.info_hash.clone();
                    let timeout = self.init.timeout;
                    let wait = self.init.wait_future();
                    async move { await_initialized(&info_hash, timeout, wait).await }
                },
                move |plan: TorrentFilePriorityPlan| {
                    let handle = applier.clone();
                    async move { handle.record_reconcile(&plan) }
                },
            );
            Ok(())
        }

        async fn get_file_reader(
            &self,
            file_idx: usize,
            _start_offset: u64,
            _priority: u8,
            _bitrate: Option<u64>,
            _intent: crate::backend::priorities::PlaybackIntent,
            _buffer: crate::backend::priorities::BufferProfile,
        ) -> Result<Box<dyn FileStreamTrait>> {
            self.gate().await?;
            self.counters.get_file_reader.fetch_add(1, Ordering::SeqCst);
            let len = self
                .files
                .get(file_idx)
                .map(|f| f.length as usize)
                .unwrap_or(0);
            Ok(Box::new(std::io::Cursor::new(vec![0xAB; len])))
        }

        async fn get_files(&self) -> Vec<BackendFileInfo> {
            self.files.clone()
        }

        async fn prepare_file_for_streaming(&self, _file_idx: usize) -> Result<()> {
            self.gate().await?;
            self.counters
                .prepare_file_for_streaming
                .fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn clear_file_streaming(&self, _file_idx: usize) -> Result<()> {
            self.counters
                .clear_file_streaming
                .fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn wait_for_piece_ready(
            &self,
            _file_idx: usize,
            _offset: u64,
            _timeout: Duration,
            _intent: crate::backend::priorities::PlaybackIntent,
            _buffer: crate::backend::priorities::BufferProfile,
        ) -> Result<PieceReadiness> {
            Ok(PieceReadiness {
                ready: true,
                piece: 0,
                ready_pieces: 1,
                target_pieces: 1,
                elapsed_ms: 0,
                peers: 0,
                download_rate: 0,
                reason: "fake".to_string(),
            })
        }
    }

    fn test_enginefs() -> (BackendEngineFS<FakeBackend>, Arc<FakeCounters>) {
        test_enginefs_with_file_count(1)
    }

    /// The two seeds, and why they are two.
    ///
    /// A torrent added a moment ago has been used by nothing -- but that is
    /// a fact about an interval that did not exist, not about a past this
    /// process failed to see, so it has a reading and the grace applies to
    /// it. A restored one has no reading at all, and reading `now` there is
    /// the claim this design keeps having to delete.
    ///
    /// Folding them into one seed breaks one of the two. Seeding everything
    /// with the absence stops a freshly added torrent on its first tick with
    /// seeding off, dropping the swarm it has just dialled and paying a
    /// re-announce at the start of playback. Seeding everything with `now`
    /// is the "used at boot" claim that hands every restored torrent a fresh
    /// grace on every restart -- see
    /// `backend::librqbit::tests::a_restart_with_seeding_off_stops_what_the_last_process_left_without_waiting`
    /// for that half, over a real persisted session.
    #[tokio::test]
    async fn an_added_engine_has_a_reading_where_a_restored_one_has_none() {
        let (enginefs, _counters) = test_enginefs();

        // The fixture registers its torrent through the restored map, which
        // is the half with no reading to take.
        let restored = enginefs.get_engine(TEST_HASH).await.unwrap();
        assert_eq!(
            restored.quiet_for(0),
            None,
            "nothing in this process has used a torrent it inherited"
        );
        assert_eq!(
            enginefs.reconcile_tick_at(0).await,
            vec![(TEST_HASH.to_string(), crate::reconcile::Decision::Run)],
            "and with seeding on it runs regardless"
        );

        // An engine this process built has one, and it is zero: no interval
        // has passed in which anything could have used it.
        let fresh = Engine::new_with_handle(
            restored.handle.clone(),
            TEST_HASH,
            enginefs.clock,
            enginefs.volumes.clone(),
            enginefs.budget.clone(),
        );
        assert_eq!(
            fresh.quiet_for(0),
            Some(Duration::ZERO),
            "a torrent added this instant has been idle for no time at all"
        );
        assert!(
            Duration::ZERO < INACTIVE_TORRENT_PAUSE_GRACE,
            "so the idle arm cannot reach it yet"
        );
    }

    /// An observer -- the per-stream progress logger, a diagnostics sweep --
    /// must be able to read an engine without that being what keeps its
    /// torrent out of the idle sweep. `get_engine` counts as a poll on
    /// purpose; `peek_engine` must not.
    #[tokio::test]
    async fn peek_engine_does_not_count_as_a_poll() {
        let (enginefs, _counters) = test_enginefs();
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        let stale = u64::MAX;
        engine.last_accessed.store(stale, Ordering::SeqCst);

        assert!(enginefs.peek_engine(TEST_HASH).await.is_some());
        assert_eq!(
            engine.last_accessed.load(Ordering::SeqCst),
            stale,
            "peeking left the idle clock alone"
        );

        enginefs.get_engine(TEST_HASH).await.unwrap();
        assert_ne!(
            engine.last_accessed.load(Ordering::SeqCst),
            stale,
            "a real lookup still counts as a poll"
        );
        assert!(enginefs.peek_engine("no-such-hash").await.is_none());
    }

    /// The persisted pin set as JSON (`{}` when the file is not there yet).
    fn read_pinned_downloads(path: &std::path::Path) -> serde_json::Value {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap(),
            Err(_) => serde_json::json!({}),
        }
    }

    fn test_enginefs_with_file_count(
        file_count: usize,
    ) -> (BackendEngineFS<FakeBackend>, Arc<FakeCounters>) {
        test_enginefs_with_files(
            (0..file_count)
                .map(|idx| (format!("video-{idx}.mkv"), 100))
                .collect(),
        )
    }

    fn test_enginefs_with_files(
        files: Vec<(String, u64)>,
    ) -> (BackendEngineFS<FakeBackend>, Arc<FakeCounters>) {
        let (enginefs, counters, _init) =
            test_enginefs_with_init(files, FakeInit::new(true, Duration::from_secs(60)));
        (enginefs, counters)
    }

    /// A scratch root of its own for one fake-engine fixture.
    ///
    /// Never a fixed path under `std::env::temp_dir()`: the pin set is
    /// persisted to `<download dir>/pinned-downloads.json` on every change
    /// and the tests run in parallel, so a shared root has every pinning
    /// test writing (and racing the rename of) one file another test reads.
    /// The parent is wiped once per test process so the roots do not pile
    /// up across runs.
    fn fake_engine_root() -> std::path::PathBuf {
        static ROOTS: AtomicUsize = AtomicUsize::new(0);
        static WIPE: std::sync::Once = std::sync::Once::new();
        let parent = std::env::temp_dir().join("enginefs-fake-engine-tests");
        WIPE.call_once(|| {
            let _ = std::fs::remove_dir_all(&parent);
        });
        let root = parent.join(ROOTS.fetch_add(1, Ordering::SeqCst).to_string());
        std::fs::create_dir_all(root.join("downloads")).unwrap();
        root
    }

    /// Engine over a fake torrent that is still initializing (`ready: false`)
    /// with a caller-chosen initialization timeout.
    fn test_enginefs_initializing(
        file_count: usize,
        timeout: Duration,
    ) -> (
        BackendEngineFS<FakeBackend>,
        Arc<FakeCounters>,
        Arc<FakeInit>,
    ) {
        test_enginefs_with_init(
            (0..file_count)
                .map(|idx| (format!("video-{idx}.mkv"), 100))
                .collect(),
            FakeInit::new(false, timeout),
        )
    }

    fn test_enginefs_with_init(
        files: Vec<(String, u64)>,
        init: Arc<FakeInit>,
    ) -> (
        BackendEngineFS<FakeBackend>,
        Arc<FakeCounters>,
        Arc<FakeInit>,
    ) {
        let counters = Arc::new(FakeCounters::default());
        let handle = FakeHandle {
            info_hash: TEST_HASH.to_string(),
            counters: counters.clone(),
            files: files
                .into_iter()
                .map(|(name, length)| BackendFileInfo { name, length })
                .collect(),
            init: init.clone(),
        };
        let mut restored = HashMap::new();
        restored.insert(TEST_HASH.to_string(), handle.clone());
        let root = fake_engine_root();
        let enginefs = BackendEngineFS::new_with_backend(
            FakeBackend::new(vec![handle]),
            restored,
            root.join("cache"),
            root.join("downloads"),
        );
        (enginefs, counters, init)
    }

    const OTHER_HASH: &str = "fedcba9876543210fedcba9876543210fedcba98";

    /// Engine over two initialized two-file fake torrents (`TEST_HASH`,
    /// `OTHER_HASH`) with separate counters, plus the backend's removal log,
    /// for policies that must treat engines differently.
    struct TwoEngines {
        enginefs: BackendEngineFS<FakeBackend>,
        counters: [Arc<FakeCounters>; 2],
        removed: Arc<Mutex<Vec<String>>>,
    }

    fn test_enginefs_with_two_engines() -> TwoEngines {
        let make = |hash: &str| {
            let counters = Arc::new(FakeCounters::default());
            let handle = FakeHandle {
                info_hash: hash.to_string(),
                counters: counters.clone(),
                files: (0..2)
                    .map(|idx| BackendFileInfo {
                        name: format!("video-{idx}.mkv"),
                        length: 100,
                    })
                    .collect(),
                init: FakeInit::new(true, Duration::from_secs(60)),
            };
            (handle, counters)
        };
        let (a, counters_a) = make(TEST_HASH);
        let (b, counters_b) = make(OTHER_HASH);
        let restored = HashMap::from([
            (TEST_HASH.to_string(), a.clone()),
            (OTHER_HASH.to_string(), b.clone()),
        ]);
        let backend = FakeBackend::new(vec![a, b]);
        let removed = backend.removed.clone();
        let root = fake_engine_root();
        let enginefs = BackendEngineFS::new_with_backend(
            backend,
            restored,
            root.join("cache"),
            root.join("downloads"),
        );
        TwoEngines {
            enginefs,
            counters: [counters_a, counters_b],
            removed,
        }
    }

    /// How long a bounded state-wait in these tests may take before it
    /// gives up. Under `start_paused` this is virtual time; in the handful
    /// of real-time tests it is wall clock, and generous on purpose -- the
    /// bound is not a timing assertion, only there so a regression fails
    /// instead of hanging, and a tight one turns CPU contention on a CI
    /// runner into a spurious failure.
    const TEST_WAIT_BOUND: Duration = Duration::from_secs(60);

    /// Poll `cond` until it holds or `bound` of (virtual) time elapses.
    async fn wait_until(bound: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let deadline = tokio::time::Instant::now() + bound;
        while !cond() {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        true
    }

    /// The engine hands its tracker refresher over exactly once, so
    /// `server::run` can abort it with the rest of its background tasks
    /// instead of leaving a detached loop to arm a timer on a runtime that is
    /// going down. Aborting it here is also what keeps this test offline:
    /// the storage-less refresher fetches the tracker list on its first poll.
    #[tokio::test]
    async fn the_engine_hands_its_tracker_refresh_task_over_once() {
        let root = fake_engine_root();
        let enginefs = BackendEngineFS::new_with_backend(
            FakeBackend::new(Vec::new()),
            HashMap::new(),
            root.join("cache"),
            root.join("downloads"),
        );

        let task = enginefs
            .take_tracker_refresh_task()
            .expect("the engine must own its tracker refresher");
        task.abort();

        assert!(
            enginefs.take_tracker_refresh_task().is_none(),
            "the task has one owner; the second engine sharing this Arc must not get it too"
        );
    }

    /// The same contract for the housekeeping sweep, which the constructor
    /// starts for itself and which nothing could cancel while it was
    /// detached. Neither task is polled here -- a current-thread test that
    /// never awaits leaves both parked on their first instruction -- which is
    /// also what keeps this offline.
    #[tokio::test]
    async fn the_engine_hands_its_housekeeping_sweep_over_once() {
        let root = fake_engine_root();
        let enginefs = BackendEngineFS::new_with_backend(
            FakeBackend::new(Vec::new()),
            HashMap::new(),
            root.join("cache"),
            root.join("downloads"),
        );

        let task = enginefs
            .take_sweep_task()
            .expect("the engine must own the sweep it started");
        task.abort();
        if let Some(refresher) = enginefs.take_tracker_refresh_task() {
            refresher.abort();
        }

        assert!(
            enginefs.take_sweep_task().is_none(),
            "the task has one owner; the second engine sharing this Arc must not get it too"
        );
    }

    // (a) A torrent that is Initializing when the request arrives: the request
    // blocks (no error, no empty body) and succeeds once the torrent is ready.
    #[tokio::test(start_paused = true)]
    async fn get_file_waits_for_initializing_torrent_then_succeeds() {
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        use tokio::io::AsyncReadExt;
        let (enginefs, counters, init) =
            test_enginefs_initializing(2, crate::backend::librqbit::TORRENT_INIT_TIMEOUT);
        let engine = enginefs.get_engine(TEST_HASH).await.expect("engine");

        let init_delay = Duration::from_secs(2);
        let flipper = {
            let init = init.clone();
            tokio::spawn(async move {
                tokio::time::sleep(init_delay).await;
                init.mark_ready();
            })
        };

        let started = tokio::time::Instant::now();
        let mut file = engine
            .try_get_file_with_intent(
                1,
                0,
                1,
                PlaybackIntent::DirectInitial,
                BufferProfile::Normal,
            )
            .await
            .expect("get_file must succeed once the torrent initializes");
        flipper.await.unwrap();

        assert!(
            started.elapsed() >= init_delay,
            "request must block through the initializing window, returned after {:?}",
            started.elapsed()
        );
        assert!(init.is_ready());
        assert_eq!(
            counters.applied_while_initializing.load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            counters.prepare_file_for_streaming.load(Ordering::SeqCst),
            1
        );
        assert_eq!(counters.get_file_reader.load(Ordering::SeqCst), 1);
        assert_eq!(engine.active_streams.load(Ordering::SeqCst), 1);

        // The reader handed back is a real, readable stream.
        let mut buf = [0u8; 4];
        file.read_exact(&mut buf).await.expect("read from reader");
        assert_eq!(buf, [0xAB; 4]);
    }

    // (b) A torrent that never initializes: a clean error within the timeout
    // bound, never a hang, and no stream accounted as started.
    #[tokio::test(start_paused = true)]
    async fn get_file_fails_cleanly_when_torrent_never_initializes() {
        use crate::backend::librqbit::TorrentInitError;
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        let init_timeout = Duration::from_secs(3);
        let (enginefs, counters, init) = test_enginefs_initializing(2, init_timeout);
        let engine = enginefs.get_engine(TEST_HASH).await.expect("engine");

        let started = tokio::time::Instant::now();
        // Outer bound is the "no hang" assertion (virtual time auto-advances).
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            engine.try_get_file_with_intent(
                1,
                0,
                1,
                PlaybackIntent::DirectInitial,
                BufferProfile::Normal,
            ),
        )
        .await
        .expect("get_file must not hang past the initialization timeout");
        let err = result.err().expect("never-initializing torrent must fail");
        assert!(
            started.elapsed() >= init_timeout && started.elapsed() < init_timeout * 2,
            "must fail after exactly one init timeout, took {:?}",
            started.elapsed()
        );
        match err.torrent_init_error() {
            Some(TorrentInitError::TimedOut {
                info_hash,
                timeout_secs,
            }) => {
                assert_eq!(info_hash, TEST_HASH);
                assert_eq!(*timeout_secs, init_timeout.as_secs());
            }
            other => panic!("expected TimedOut, got {other:?} ({err})"),
        }
        assert!(!init.is_ready());
        assert_eq!(
            counters.applied_while_initializing.load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            counters.prepare_file_for_streaming.load(Ordering::SeqCst),
            0
        );
        assert_eq!(counters.get_file_reader.load(Ordering::SeqCst), 0);
        assert_eq!(engine.active_streams.load(Ordering::SeqCst), 0);
    }

    // (c) A file-selection reconcile issued while the torrent is Initializing
    // is not dropped: it is parked and applied once the torrent is ready, with
    // the latest plan winning when several were issued in the window.
    #[tokio::test(start_paused = true)]
    async fn deferred_reconcile_is_applied_once_initialized() {
        use crate::backend::priorities::PlaybackIntent;
        let (enginefs, counters, init) =
            test_enginefs_initializing(3, crate::backend::librqbit::TORRENT_INIT_TIMEOUT);

        let hot = |file_idx: usize| {
            Some(HotFilePriorityPlan {
                file_idx,
                start_offset: 0,
                priority: 1,
                intent: PlaybackIntent::DirectInitial,
                bitrate_bytes_per_sec: None,
            })
        };
        // Both activations return immediately: reconcile defers, it must not
        // block the caller (background cleanup loops also drive it).
        let started = tokio::time::Instant::now();
        enginefs
            .activate_multifile_file_for_playback(TEST_HASH, 1, hot(1), "test-first")
            .await;
        enginefs
            .activate_multifile_file_for_playback(TEST_HASH, 2, hot(2), "test-second")
            .await;
        assert_eq!(started.elapsed(), Duration::ZERO);
        assert!(init.deferred.has_pending(), "plan must be parked");
        assert_eq!(counters.reconcile_file_priorities.load(Ordering::SeqCst), 0);
        assert_eq!(*counters.last_active_file.lock().unwrap(), None);

        // Nothing is applied while still initializing, however long it takes.
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(counters.reconcile_file_priorities.load(Ordering::SeqCst), 0);

        init.mark_ready();
        assert!(
            wait_until(Duration::from_secs(5), || counters
                .reconcile_file_priorities
                .load(Ordering::SeqCst)
                == 1)
            .await,
            "deferred reconcile must be applied after initialization"
        );
        // Coalesced: only the latest plan (file 2, generation 2) was applied.
        assert_eq!(*counters.last_active_file.lock().unwrap(), Some(2));
        assert_eq!(counters.last_generation.load(Ordering::SeqCst), 2);
        assert_eq!(
            counters.applied_while_initializing.load(Ordering::SeqCst),
            0
        );
        assert!(!init.deferred.has_pending());
        // Give the waiter a chance to misbehave (double apply) -- it must not.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(counters.reconcile_file_priorities.load(Ordering::SeqCst), 1);

        // Once ready, reconciles apply directly (and supersede nothing).
        enginefs
            .activate_multifile_file_for_playback(TEST_HASH, 0, hot(0), "test-live")
            .await;
        assert_eq!(counters.reconcile_file_priorities.load(Ordering::SeqCst), 2);
        assert_eq!(*counters.last_active_file.lock().unwrap(), Some(0));
    }

    #[tokio::test]
    async fn multi_file_selects_only_requested_file() {
        let (enginefs, counters) = test_enginefs_with_file_count(3);

        enginefs.on_stream_start(TEST_HASH, 1).await;

        let snapshot = enginefs.stream_activity_snapshot().await;
        assert_eq!(snapshot.active_multifile_selections.len(), 1);
        assert_eq!(snapshot.active_multifile_selections[0].file_idx, 1);
        assert_eq!(*counters.last_active_file.lock().unwrap(), Some(1));
        assert_eq!(counters.reconcile_file_priorities.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn multi_file_latest_request_wins() {
        let (enginefs, counters) = test_enginefs_with_file_count(3);

        enginefs.on_stream_start(TEST_HASH, 1).await;
        enginefs.on_stream_start(TEST_HASH, 2).await;

        let snapshot = enginefs.stream_activity_snapshot().await;
        assert_eq!(snapshot.active_multifile_selections.len(), 1);
        assert_eq!(snapshot.active_multifile_selections[0].file_idx, 2);
        assert_eq!(*counters.last_active_file.lock().unwrap(), Some(2));
        assert!(
            snapshot
                .active_file_streams
                .iter()
                .all(|stream| stream.file_idx == 2)
        );
        assert_eq!(counters.reconcile_file_priorities.load(Ordering::SeqCst), 2);
    }

    /// Startup phase: no files stands in for unresolved metadata, so the
    /// engine-level stats must say so and carry no check/window numbers.
    #[tokio::test]
    async fn stats_phase_is_resolving_metadata_without_metadata() {
        let (enginefs, _counters) = test_enginefs_with_files(vec![]);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        let stats = engine.get_statistics().await;
        assert_eq!(stats.phase, StartupPhase::ResolvingMetadata);
        assert!(!stats.has_metadata);
        assert_eq!(stats.checked_bytes, None);
        assert_eq!(stats.check_total_bytes, None);
        assert_eq!(stats.initial_window_ready_bytes, None);
        assert_eq!(stats.initial_window_bytes, None);
    }

    /// Startup phase: while the fake torrent is initializing (librqbit's
    /// hash check) the phase is `checking` with check progress exposed and no
    /// initial-window numbers (there is no piece map yet). Once initialized
    /// it moves on to `buffering` with the window numbers filled in.
    #[tokio::test]
    async fn stats_phase_is_checking_until_initialized() {
        let (enginefs, _counters, init) = test_enginefs_initializing(1, Duration::from_secs(5));
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        let stats = engine.get_statistics().await;
        assert_eq!(stats.phase, StartupPhase::Checking);
        assert_eq!(stats.checked_bytes, Some(0));
        assert_eq!(stats.check_total_bytes, Some(100));
        assert_eq!(stats.initial_window_ready_bytes, None);
        assert_eq!(stats.initial_window_bytes, None);
        assert_eq!(stats.files[0].initial_window_bytes, None);

        init.mark_ready();
        let stats = engine.get_statistics().await;
        assert_eq!(stats.phase, StartupPhase::Buffering);
        assert_eq!(stats.checked_bytes, None);
        assert_eq!(stats.check_total_bytes, None);
        assert_eq!(stats.initial_window_ready_bytes, Some(50));
        assert_eq!(stats.initial_window_bytes, Some(100));
    }

    /// Startup phase: `buffering` until the stream file's initial window is
    /// fully on disk, then `ready`. The window is the head of the *guessed*
    /// stream file, mirrored to the top level from `files[]`.
    #[tokio::test]
    async fn stats_phase_flips_to_ready_when_initial_window_is_on_disk() {
        let (enginefs, counters) = test_enginefs_with_files(vec![
            ("video-0.mkv".to_string(), 100),
            ("video-1.mkv".to_string(), 60),
        ]);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        let stats = engine.get_statistics().await;
        assert_eq!(stats.phase, StartupPhase::Buffering);
        assert_eq!(stats.stream_name, "video-0.mkv", "largest file is guessed");
        assert_eq!(stats.initial_window_ready_bytes, Some(50));
        assert_eq!(stats.initial_window_bytes, Some(100));
        assert_eq!(stats.files[1].initial_window_ready_bytes, Some(30));
        assert_eq!(stats.files[1].initial_window_bytes, Some(60));

        counters.seeded.store(true, Ordering::SeqCst);
        let stats = engine.get_statistics().await;
        assert_eq!(stats.phase, StartupPhase::Ready);
        assert_eq!(stats.initial_window_ready_bytes, Some(100));
        assert_eq!(stats.initial_window_bytes, Some(100));
        assert!(stats.is_finished);
    }

    /// `focus_stream_file` re-judges the phase for the exact file a client
    /// asked about (`/{infoHash}/{fileIdx}/stats.json`), only in the
    /// buffering/ready phases, and ignores out-of-range indices. It also
    /// lifts that file's in-flight piece to the top level, and only that
    /// file's: the piece another file's reader waits on says nothing about
    /// the stream this request is about.
    #[test]
    fn focus_stream_file_refines_phase_per_file() {
        let file = |ready: u64, total: u64| StatsFile {
            name: "f".into(),
            path: "f".into(),
            length: total,
            offset: 0,
            downloaded: ready,
            progress: 0.0,
            initial_window_ready_bytes: Some(ready),
            initial_window_bytes: Some(total),
            in_flight_piece: (ready < total).then_some(crate::backend::InFlightPiece {
                index: 3,
                downloaded_bytes: ready,
                total_bytes: total,
                verified: false,
            }),
            pinned: false,
            complete: ready == total,
        };
        let base = EngineStats {
            name: "t".into(),
            info_hash: TEST_HASH.into(),
            piece_length: None,
            files: vec![file(10, 100), file(60, 60)],
            sources: vec![],
            opts: StatsOptions {
                connections: None,
                dht: true,
                growler: Growler::default(),
                handshake_timeout: None,
                path: String::new(),
                peer_search: PeerSearch::default(),
                swarm_cap: SwarmCap::default(),
                timeout: None,
                tracker: true,
                r#virtual: false,
            },
            download_speed: 0.0,
            upload_speed: 0.0,
            downloaded: 0,
            uploaded: 0,
            unchoked: 0,
            peers: 0,
            queued: 0,
            unique: 0,
            connection_tries: 0,
            peer_search_running: false,
            stream_len: 0,
            stream_name: String::new(),
            stream_progress: 0.0,
            swarm_connections: 0,
            swarm_paused: false,
            swarm_size: 0,
            connected_seeders: 0,
            swarm_seeders: None,
            swarm_leechers: None,
            swarm_scrape_age_secs: None,
            is_finished: false,
            has_metadata: true,
            phase: StartupPhase::Buffering,
            checked_bytes: None,
            check_total_bytes: None,
            initial_window_ready_bytes: None,
            initial_window_bytes: None,
            in_flight_piece: None,
            peer_discovery: PeerDiscovery::default(),
            error: None,
            pinned_files: Vec::new(),
        };

        let mut stats = base.clone();
        stats.focus_stream_file(0);
        assert_eq!(stats.phase, StartupPhase::Buffering);
        assert_eq!(stats.initial_window_ready_bytes, Some(10));
        assert_eq!(stats.initial_window_bytes, Some(100));
        assert_eq!(stats.in_flight_piece, base.files[0].in_flight_piece);

        let mut stats = base.clone();
        stats.focus_stream_file(1);
        assert_eq!(stats.phase, StartupPhase::Ready);
        assert_eq!(stats.initial_window_ready_bytes, Some(60));
        // File 1 has no reader waiting on a piece; file 0's must not leak in.
        assert_eq!(stats.in_flight_piece, None);

        // Out of range: untouched.
        let mut stats = base.clone();
        stats.focus_stream_file(7);
        assert_eq!(stats.phase, StartupPhase::Buffering);
        assert_eq!(stats.initial_window_bytes, None);
        assert_eq!(stats.in_flight_piece, None);

        // Not a piece-map phase: untouched even though the file is complete.
        let mut stats = base.clone();
        stats.phase = StartupPhase::Checking;
        stats.focus_stream_file(1);
        assert_eq!(stats.phase, StartupPhase::Checking);
        assert_eq!(stats.initial_window_bytes, None);
        assert_eq!(stats.in_flight_piece, None);
    }

    /// Wire contract: the new fields serialize camelCase and additively; the
    /// server.js-compatible keys stremio-core parses are all still present.
    #[tokio::test]
    async fn stats_json_keeps_legacy_fields_and_adds_phase_fields() {
        let (enginefs, _counters) = test_enginefs();
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        let value = serde_json::to_value(engine.get_statistics().await).unwrap();
        let obj = value.as_object().unwrap();

        for key in [
            "name",
            "infoHash",
            "files",
            "sources",
            "opts",
            "downloadSpeed",
            "uploadSpeed",
            "downloaded",
            "uploaded",
            "unchoked",
            "peers",
            "queued",
            "unique",
            "connectionTries",
            "peerSearchRunning",
            "streamLen",
            "streamName",
            "streamProgress",
            "swarmConnections",
            "swarmPaused",
            "swarmSize",
            "isFinished",
            "hasMetadata",
        ] {
            assert!(obj.contains_key(key), "legacy key {key} missing: {value}");
        }
        assert_eq!(value["phase"], "buffering");
        assert_eq!(value["checkedBytes"], serde_json::Value::Null);
        assert_eq!(value["checkTotalBytes"], serde_json::Value::Null);
        assert_eq!(value["initialWindowReadyBytes"], 50);
        assert_eq!(value["initialWindowBytes"], 100);
        assert_eq!(
            value["peerDiscovery"],
            serde_json::json!({ "seen": 0, "queued": 0, "connecting": 0, "live": 0, "known": 0 })
        );
        assert_eq!(value["connectedSeeders"], 0);
        // Swarm scrape figures. The fake backend scrapes nothing, so all
        // three are present and null -- "we do not know", which a client
        // must be able to tell from a swarm that really has no seeders.
        for key in ["swarmSeeders", "swarmLeechers", "swarmScrapeAgeSecs"] {
            assert!(obj.contains_key(key), "{key} missing: {value}");
            assert_eq!(value[key], serde_json::Value::Null, "{key}: {value}");
        }
        // Sub-piece progress is `null` at the top level, never a zeroed
        // object: the fake backend has no reader on any file, so nothing is
        // in flight and a client must be able to tell that from "0 of 16 MB
        // downloaded". Per file it is omitted entirely for the same reason.
        assert!(obj.contains_key("inFlightPiece"), "{value}");
        assert_eq!(value["inFlightPiece"], serde_json::Value::Null, "{value}");
        assert!(
            !value["files"][0]
                .as_object()
                .unwrap()
                .contains_key("inFlightPiece"),
            "{value}"
        );
        assert_eq!(value["files"][0]["initialWindowReadyBytes"], 50);
        assert_eq!(value["files"][0]["initialWindowBytes"], 100);
        let file_keys = value["files"][0].as_object().unwrap();
        for key in ["name", "path", "length", "offset", "downloaded", "progress"] {
            assert!(file_keys.contains_key(key), "legacy file key {key} missing");
        }
        // Offline-download additions, always present (camelCase).
        assert_eq!(value["pinnedFiles"], serde_json::json!([]));
        assert_eq!(value["files"][0]["pinned"], false);
        assert_eq!(value["files"][0]["complete"], false);
    }

    /// Per-tracker scrape counters are additive and camelCase, and are
    /// omitted rather than zeroed while a tracker has told us nothing --
    /// `seeders: 0` has to mean the tracker said zero.
    #[test]
    fn source_scrape_counters_serialize_additively() {
        use crate::backend::Source;

        let unscraped = serde_json::to_value(Source {
            url: "udp://t.example:1337/announce".into(),
            ..Source::default()
        })
        .unwrap();
        let obj = unscraped.as_object().unwrap();
        for key in [
            "url",
            "lastStarted",
            "numFound",
            "numFoundUniq",
            "numRequests",
        ] {
            assert!(
                obj.contains_key(key),
                "legacy key {key} missing: {unscraped}"
            );
        }
        for key in ["seeders", "leechers", "completed"] {
            assert!(!obj.contains_key(key), "{key} must be absent: {unscraped}");
        }

        let scraped = serde_json::to_value(Source {
            url: "udp://t.example:1337/announce".into(),
            seeders: Some(0),
            leechers: Some(3),
            completed: Some(91),
            ..Source::default()
        })
        .unwrap();
        assert_eq!(scraped["seeders"], 0, "a real zero is reported: {scraped}");
        assert_eq!(scraped["leechers"], 3);
        assert_eq!(scraped["completed"], 91);
    }

    /// `complete` flips with the per-file progress and the JSON keeps the
    /// bool shape a client can key on; a resolving magnet lists no pins.
    #[tokio::test]
    async fn stats_complete_flag_follows_file_progress() {
        let (enginefs, counters) = test_enginefs();
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        assert!(!engine.get_statistics().await.files[0].complete);
        counters.seeded.store(true, Ordering::SeqCst);
        let value = serde_json::to_value(engine.get_statistics().await).unwrap();
        assert_eq!(value["files"][0]["complete"], true);
        assert_eq!(value["files"][0]["downloaded"], value["files"][0]["length"]);

        let resolving =
            serde_json::to_value(EngineStats::resolving_metadata(TEST_HASH, &[])).unwrap();
        assert_eq!(resolving["pinnedFiles"], serde_json::json!([]));
    }

    /// Paused time: the wait is for the 5 s delayed-cleanup task to fire,
    /// which the virtual clock does the instant the runtime is idle. Slept
    /// for real it would be six seconds of wall clock *and* a race -- a
    /// loaded runner that had not run the task by then would pass
    /// vacuously, one that ran it late would fail.
    #[tokio::test(start_paused = true)]
    async fn old_cleanup_cannot_clear_newer_multifile_active_file() {
        let (enginefs, counters) = test_enginefs_with_file_count(3);

        enginefs.on_stream_start(TEST_HASH, 1).await;
        enginefs
            .schedule_file_cleanup(TEST_HASH.to_string(), 1)
            .await;
        enginefs.on_stream_start(TEST_HASH, 2).await;
        tokio::time::sleep(Duration::from_secs(6)).await;

        let snapshot = enginefs.stream_activity_snapshot().await;
        assert_eq!(snapshot.active_multifile_selections.len(), 1);
        assert_eq!(snapshot.active_multifile_selections[0].file_idx, 2);
        assert_eq!(*counters.last_active_file.lock().unwrap(), Some(2));
        assert_eq!(counters.clear_file_streaming.load(Ordering::SeqCst), 0);
    }

    /// Which of the snapshot's fields mean "somebody is watching". The
    /// sticky ones are the trap: they are what the want-set is planned from,
    /// so they outlive the stream on purpose, and a light driven by them
    /// would come on with the first playback of the session and stay on.
    #[tokio::test]
    async fn only_the_live_fields_count_as_playback() {
        let (enginefs, _counters) = test_enginefs_with_file_count(2);

        enginefs.on_stream_start(TEST_HASH, 1).await;
        assert!(
            enginefs.stream_activity_snapshot().await.playback_is_live(),
            "an open stream is somebody watching"
        );

        enginefs.on_stream_end(TEST_HASH, 1).await;
        let after = enginefs.stream_activity_snapshot().await;
        assert!(
            after.active_file.is_some() || !after.active_multifile_selections.is_empty(),
            "the sticky bookkeeping is still there -- otherwise this asserts nothing"
        );
        assert!(
            !after.playback_is_live(),
            "the stream is over: what is left names the last file chosen, not a reader"
        );
    }

    /// `BackendEngineFS::playback_is_live` is the snapshot's answer without
    /// the snapshot, so it is checked against the snapshot at every state
    /// the live fields can put the server in -- each one on its own,
    /// since the query short-circuits and a field it never reached would
    /// pass by accident behind one it did.
    #[tokio::test]
    async fn narrow_playback_query_agrees_with_the_snapshot() {
        let (enginefs, _counters) = test_enginefs_with_file_count(2);
        async fn both_agree(enginefs: &BackendEngineFS<FakeBackend>, expected: bool, why: &str) {
            let snapshot = enginefs.stream_activity_snapshot().await.playback_is_live();
            let narrow = enginefs.playback_is_live().await;
            assert_eq!(snapshot, expected, "snapshot: {why}");
            assert_eq!(narrow, expected, "narrow query: {why}");
        }

        both_agree(&enginefs, false, "nothing has happened yet").await;

        // A reader open on the engine, with no stream response around it.
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        engine.active_streams.fetch_add(1, Ordering::SeqCst);
        both_agree(&enginefs, true, "an open file reader").await;
        engine.active_streams.fetch_sub(1, Ordering::SeqCst);
        both_agree(&enginefs, false, "the reader closed").await;

        // A stream response, which is a separate count.
        enginefs.on_stream_start(TEST_HASH, 1).await;
        both_agree(&enginefs, true, "a stream response in flight").await;
        enginefs.on_stream_end(TEST_HASH, 1).await;
        both_agree(
            &enginefs,
            false,
            "the response ended, sticky fields notwithstanding",
        )
        .await;
    }

    /// The light's reading of the connection is a peek over the engines that
    /// exist: it touches no idle clock, looks nothing up, adds nothing, an
    /// engine that has gone is gone from the sum, and a torrent whose
    /// counters cannot be read contributes nothing to it rather than
    /// dropping out of it.
    #[tokio::test]
    async fn transfer_totals_is_a_peek_over_the_engines_that_exist() {
        use crate::backend::TransferTotals;
        let (enginefs, counters) = test_enginefs();
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        let stale = u64::MAX;
        engine.last_accessed.store(stale, Ordering::SeqCst);

        let totals = enginefs.transfer_totals().await;
        assert_eq!(totals.len(), 1);
        assert_eq!(totals[TEST_HASH], TransferTotals::default());

        counters.fetched.store(4_096, Ordering::SeqCst);
        counters.uploaded.store(512, Ordering::SeqCst);
        assert_eq!(
            enginefs.transfer_totals().await[TEST_HASH],
            TransferTotals {
                fetched: 4_096,
                uploaded: 512,
            }
        );
        assert_eq!(
            engine.last_accessed.load(Ordering::SeqCst),
            stale,
            "reading the counters counted as a poll: a light asking every second would \
             keep this torrent out of the idle sweep for ever"
        );

        // A torrent nothing can read the counters of is still one of the
        // engines that exist, and contributes nothing: the light reads a
        // difference of two sums, so a sum that dropped when a torrent
        // paused would read as "not grown" -- which is what an unreadable
        // torrent means here -- only by accident, and a sum that grew again
        // on the unpause would read as traffic that never moved.
        counters.paused.store(true, Ordering::SeqCst);
        let paused = enginefs.transfer_totals().await;
        assert_eq!(
            paused.get(TEST_HASH),
            Some(&TransferTotals::default()),
            "an unreadable torrent left the sum instead of contributing nothing to it"
        );
        counters.paused.store(false, Ordering::SeqCst);

        enginefs.remove_engine(TEST_HASH).await;
        assert!(
            enginefs.transfer_totals().await.is_empty(),
            "a removed engine is absent, not re-added"
        );
        assert!(enginefs.list_engines().await.is_empty());
        assert!(
            enginefs.magnet_adds.read().await.is_empty(),
            "asking for the totals began no add"
        );
    }

    /// A stream request on a torrent that is stopped -- for any reason,
    /// including one from a previous process -- starts it, and selects the
    /// file that was asked for.
    ///
    /// Asserted on the run state, not on a resume counter: the counter said
    /// a call was made, which is a different claim from "the torrent is
    /// running", and it is the claim that let four consecutive defects
    /// through. The torrent here is stopped the way librqbit stops one, so
    /// nothing in the process holds any note about it.
    #[tokio::test(start_paused = true)]
    async fn a_stopped_torrent_is_started_by_a_request_for_one_of_its_files() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(3);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        stop_torrent(&enginefs, TEST_HASH).await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);

        enginefs.on_stream_start(TEST_HASH, 2).await;

        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Live,
            "the request started the torrent it is going to read from"
        );
        let snapshot = enginefs.stream_activity_snapshot().await;
        assert_eq!(snapshot.active_multifile_selections.len(), 1);
        assert_eq!(snapshot.active_multifile_selections[0].file_idx, 2);
        assert_eq!(*counters.last_active_file.lock().unwrap(), Some(2));
        assert!(
            snapshot.paused_torrents.is_empty(),
            "and the snapshot reports what the torrent is doing"
        );
    }

    /// The idle arm does not stop a torrent whose multi-file selection is
    /// still live, however long ago the HTTP stream that made it ended.
    ///
    /// This used to be a test of the grace-period task, which read the same
    /// five activity registers the sweep read and the reconciler now reads.
    /// Three readers of one question is what the reconciler replaced; the
    /// question itself is unchanged.
    #[tokio::test(start_paused = true)]
    async fn the_idle_arm_leaves_a_live_multifile_selection_alone() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(3);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);

        enginefs.on_stream_start(TEST_HASH, 1).await;
        // The HTTP stream goes; the selection it made stays.
        enginefs.active_streams.write().await.clear();
        enginefs.active_file_streams.write().await.clear();
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE * 2).await;

        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
    }

    /// Seeding off, nothing active, quiet for the grace: stopped. The
    /// positive case the two above are the exceptions to.
    #[tokio::test(start_paused = true)]
    async fn the_idle_arm_stops_a_torrent_with_nothing_left_on_it() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(3);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE).await;

        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);
    }

    /// An engine made late has a reading, and the grace applies to it.
    ///
    /// This asserted the opposite until the seed was split, on the argument
    /// that "a server that adds a torrent every few minutes has an idle arm
    /// that never fires". That argument is arithmetically wrong: the grace
    /// is `INACTIVE_TORRENT_PAUSE_GRACE`, so an engine that starts its clock
    /// at its own creation escapes the arm for exactly that long and no
    /// longer -- the arm fires on the next tick after it.
    ///
    /// What it cost was the mirror of the bug it was written against. An add
    /// with seeding off was stopped on its first tick, dropping the swarm it
    /// had just dialled, and started again at the first byte of playback --
    /// a pause, an unpause and a re-announce, which is the churn the dwell
    /// exists to prevent.
    ///
    /// The distinction the old seed could not draw: a torrent this process
    /// added has been used by nothing over an interval that did not exist,
    /// which is a reading of zero; a torrent it *restored* has no reading at
    /// all. Only the second may be read as quiet. The restart half is
    /// `backend::librqbit::tests::a_restart_with_seeding_off_stops_what_the_last_process_left_without_waiting`.
    ///
    /// The engine is made through the ordinary add, and what is asserted is
    /// what the torrent then does.
    #[tokio::test(start_paused = true)]
    async fn an_engine_made_late_keeps_its_grace_and_loses_it_on_time() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);

        // Nothing is registered, and the process runs on for graces.
        enginefs.remove_engine(TEST_HASH).await;
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE * 4).await;

        let engine = enginefs
            .add_torrent(TorrentSource::Bytes(b"a .torrent blob".to_vec()), None)
            .await
            .expect("the add publishes an engine");
        assert_eq!(
            engine.handle.run_state(),
            RunState::Live,
            "a freshly added torrent is running, as a real backend's is"
        );

        // Its own clock starts here, however long the process has run.
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Run)],
            "an add is not idle the instant it is made"
        );
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
        assert_eq!(counters.stop_torrent.load(Ordering::SeqCst), 0);

        // And a grace later, with nothing having played it, the arm fires --
        // so the grace is deferred, not spent.
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE).await;
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Stop)],
            "nothing has played it for a whole grace"
        );
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);
        assert_eq!(counters.stop_torrent.load(Ordering::SeqCst), 1);
    }

    /// A magnet that has not resolved its info dictionary keeps running
    /// whatever the idle arm would say: what it is fetching is the
    /// dictionary, it writes no file data while it does, and stopping it is
    /// how a magnet comes never to resolve.
    #[tokio::test(start_paused = true)]
    async fn the_idle_arm_leaves_a_torrent_still_fetching_its_metadata_alone() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(0);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE).await;

        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
    }

    #[tokio::test]
    async fn single_file_bypasses_multifile_selector() {
        let (enginefs, counters) = test_enginefs();

        enginefs.on_stream_start(TEST_HASH, 0).await;

        let snapshot = enginefs.stream_activity_snapshot().await;
        assert!(snapshot.active_multifile_selections.is_empty());
        assert_eq!(counters.reconcile_file_priorities.load(Ordering::SeqCst), 0);
    }

    // --- torrent placement through the magnet registry ---

    /// The placement given to `get_or_add_magnet_placed` reaches the
    /// backend add the registry starts; an existing engine is returned as
    /// is, and the plain `get_or_add_magnet` adds with the default
    /// placement.
    #[tokio::test]
    async fn magnet_registry_passes_the_placement_to_the_backend_add() {
        let counters = Arc::new(FakeCounters::default());
        let handle = FakeHandle {
            info_hash: TEST_HASH.to_string(),
            counters,
            files: vec![BackendFileInfo {
                name: "video.mkv".into(),
                length: 100,
            }],
            init: FakeInit::new(true, Duration::from_secs(60)),
        };
        let backend = FakeBackend::new(vec![handle]);
        let placements = backend.placements.clone();
        let root = fake_engine_root();
        let enginefs = BackendEngineFS::new_with_backend(
            backend,
            HashMap::new(),
            root.join("cache"),
            root.join("downloads"),
        );

        let placement = TorrentPlacement {
            only_files: Some(vec![0]),
        };
        let engine = enginefs
            .get_or_add_magnet_placed(TEST_HASH, None, placement.clone())
            .await
            .expect("added");
        assert_eq!(engine.info_hash, TEST_HASH);
        assert_eq!(placements.lock().unwrap().as_slice(), &[placement]);

        // Already managed: no second add, whatever the placement.
        enginefs
            .get_or_add_magnet_placed(TEST_HASH, None, TorrentPlacement { only_files: None })
            .await
            .expect("joined");
        assert_eq!(placements.lock().unwrap().len(), 1);

        enginefs.remove_engine(TEST_HASH).await;
        enginefs
            .get_or_add_magnet(TEST_HASH, None)
            .await
            .expect("re-added");
        assert_eq!(
            placements.lock().unwrap().last(),
            Some(&TorrentPlacement::default()),
            "the plain add uses the backend's default placement"
        );
    }

    // --- pinning is retention, not placement ---

    /// Engine over the fake backend with nothing managed yet: a pin has to
    /// add the torrent, so what the add asks for is observable.
    fn test_enginefs_unmanaged() -> (BackendEngineFS<FakeBackend>, Arc<FakeCounters>) {
        let counters = Arc::new(FakeCounters::default());
        let handle = FakeHandle {
            info_hash: TEST_HASH.to_string(),
            counters: counters.clone(),
            files: (0..2)
                .map(|idx| BackendFileInfo {
                    name: format!("video-{idx}.mkv"),
                    length: 100,
                })
                .collect(),
            init: FakeInit::new(true, Duration::from_secs(60)),
        };
        let root = fake_engine_root();
        let enginefs = BackendEngineFS::new_with_backend(
            FakeBackend::new(vec![handle]),
            HashMap::new(),
            root.join("cache"),
            root.join("downloads"),
        );
        (enginefs, counters)
    }

    /// A pin of an unmanaged torrent adds it wanting the pinned file and
    /// names no folder at all: the placement carries a want-set and nothing
    /// else, so a pinned torrent is added exactly as a streamed one is.
    #[tokio::test]
    async fn pin_download_adds_an_unmanaged_torrent_wanting_only_the_pinned_file() {
        let (enginefs, _counters) = test_enginefs_unmanaged();
        let engine = enginefs.pin_download(TEST_HASH, 1, None).await.unwrap();
        assert_eq!(
            enginefs.backend.placements.lock().unwrap().as_slice(),
            &[TorrentPlacement {
                only_files: Some(vec![1]),
            }]
        );
        assert_eq!(engine.pinned_file_indices(), vec![1]);
    }

    /// **A pinned download does not move.** A torrent already managed --
    /// streamed first, its pieces already in the store -- is pinned where
    /// it is: no add, no removal, and the registry keeps the very engine it
    /// had, so every reader open on it goes on reading.
    ///
    /// It used to be dropped from the backend and re-added under
    /// `<downloadsDir>/<info hash>`, with the hash parked as an in-flight
    /// add for the length of the move and the engine rebuilt on the far
    /// side. The pin is a retention property: what it changes is the
    /// want-set and what the cleaner may take, never a location.
    #[tokio::test]
    async fn pin_download_leaves_a_managed_torrent_exactly_where_it_is() {
        let (enginefs, counters) = test_enginefs_with_file_count(3);
        // Wherever the backend says its files are -- a folder no layer
        // above it chose -- the pin leaves it there.
        *counters.output_folder.lock().unwrap() = Some("/cache/rqbit-downloads/show".into());
        let before = enginefs.get_engine(TEST_HASH).await.unwrap();

        let engine = enginefs.pin_download(TEST_HASH, 2, None).await.unwrap();
        assert!(
            Arc::ptr_eq(&before, &engine),
            "the pin answers with the engine the stream was reading from"
        );
        assert!(
            Arc::ptr_eq(&enginefs.get_engine(TEST_HASH).await.unwrap(), &before),
            "and the registry still holds it: nothing was published in its place"
        );
        assert!(
            enginefs.backend.placements.lock().unwrap().is_empty(),
            "a managed torrent is not added again"
        );
        assert!(enginefs.backend.removed.lock().unwrap().is_empty());
        assert!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty()
        );
        assert_eq!(engine.pinned_file_indices(), vec![2]);

        // And a second pin of the same torrent joins the first.
        let engine = enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert!(Arc::ptr_eq(&before, &engine));
        assert_eq!(engine.pinned_file_indices(), vec![0, 2]);
        assert_eq!(
            engine.get_statistics().await.pinned_files,
            vec![0, 2],
            "and the backend has both"
        );
        assert!(enginefs.backend.placements.lock().unwrap().is_empty());
    }

    /// Two pins of one torrent issued together (two episodes of a season
    /// pack, the client's re-pin loop) both land: they take the per-hash
    /// lock in turn, and the second finds the first's pin already on the
    /// engine.
    #[tokio::test]
    async fn concurrent_pins_of_one_torrent_both_land() {
        let (enginefs, _counters) = test_enginefs_with_file_count(3);
        let (first, second) = tokio::join!(
            enginefs.pin_download(TEST_HASH, 0, None),
            enginefs.pin_download(TEST_HASH, 2, None)
        );
        let first = first.expect("first pin");
        let second = second.expect("second pin");
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.pinned_file_indices(), vec![0, 2]);
        assert_eq!(
            enginefs.pinned_downloads().await,
            vec![
                PinnedDownload {
                    info_hash: TEST_HASH.to_string(),
                    file_idx: 0
                },
                PinnedDownload {
                    info_hash: TEST_HASH.to_string(),
                    file_idx: 2
                }
            ]
        );
        assert!(enginefs.pin_locks.lock().is_empty(), "no lock left behind");
    }

    /// A torrent the backend stopped for want of disk space is listed for the
    /// cleaner, and restarting it is what takes it off the list -- so the
    /// cleaner can find it, reclaim space, and put it back to work instead of
    /// leaving playback dead against a healthy swarm.
    #[tokio::test]
    async fn out_of_space_torrents_are_listed_and_can_be_restarted() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);

        // A healthy torrent is nobody's business.
        assert!(enginefs.out_of_space_torrents().await.is_empty());
        assert_eq!(counters.restart_from_error.load(Ordering::SeqCst), 0);

        counters.out_of_space.store(true, Ordering::SeqCst);
        assert_eq!(
            enginefs.out_of_space_torrents().await,
            vec![TEST_HASH.to_string()]
        );

        assert!(enginefs.restart_from_error(TEST_HASH).await.unwrap());
        assert_eq!(counters.restart_from_error.load(Ordering::SeqCst), 1);
        assert!(
            enginefs.out_of_space_torrents().await.is_empty(),
            "a restarted torrent is no longer stopped"
        );

        // A hash no engine holds any more -- swept while space was being
        // reclaimed -- is not an error, and restarts nothing.
        assert!(
            !enginefs
                .restart_from_error("ffffffffffffffffffffffffffffffffffffffff")
                .await
                .unwrap()
        );
        assert_eq!(counters.restart_from_error.load(Ordering::SeqCst), 1);
    }

    /// A torrent the backend stopped with an error nothing will retry is
    /// dead, and its data is the cleaner's to take first -- it used to be
    /// protected like a live engine's, which on a full television kept
    /// 700 MB of two dead torrents' bytes from every later stream. One that
    /// died of a full disk is not dead (the cleaner's recovery restarts it
    /// once there is room), and a pinned one stays protected however it
    /// died: an unpin is how the user gives those bytes up.
    #[tokio::test]
    async fn a_dead_torrents_files_are_the_cleaners_to_take_first() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        let hash = TEST_HASH.to_lowercase();

        let live = enginefs.reclaim_verdicts().await;
        assert!(
            !live.gate.releases(&hash, 0),
            "a live torrent announces what it has, so nothing of it goes"
        );
        assert!(!live.gate.goes_first(&hash));

        counters.in_error_state.store(true, Ordering::SeqCst);
        let dead = enginefs.reclaim_verdicts().await;
        assert!(
            dead.gate.releases(&hash, 0),
            "a dead one announces nothing, so every piece of it goes"
        );
        assert!(dead.gate.goes_first(&hash), "and its data goes first");

        // Out of space is not dead: that one is listed whole, for the
        // recovery to restart or the cleaner to take as a last resort, and
        // its pieces are still announced.
        counters.out_of_space.store(true, Ordering::SeqCst);
        let stopped = enginefs.reclaim_verdicts().await;
        assert!(!stopped.gate.releases(&hash, 0));
        assert!(!stopped.gate.goes_first(&hash));
        assert_eq!(stopped.stopped_for_space, vec![hash.clone()]);
        counters.out_of_space.store(false, Ordering::SeqCst);

        // Pinned and dead: the pin outranks the death.
        enginefs.pin_download(TEST_HASH, 1, None).await.unwrap();
        let pinned = enginefs.reclaim_verdicts().await;
        assert!(!pinned.gate.releases(&hash, 0));
        assert!(!pinned.gate.goes_first(&hash));
    }

    /// A dormant pin has no engine, so nothing in the engine walk names it,
    /// and the cleaner walks every root to the bottom. Without its piece
    /// directory in the protected set, an offline download whose torrent the
    /// backend did not restore would be aged out from under the user.
    ///
    /// That directory is the whole of what it protects. A dormant pin used
    /// to have a second entry, `<downloadsDir>/<info hash>` -- the folder a
    /// pin placed a torrent in -- which is not where any byte of it is, and
    /// which no longer exists as an idea.
    #[tokio::test]
    async fn the_gate_covers_a_dormant_pin_and_lets_go_the_moment_it_is_unpinned() {
        let (enginefs, _counters) = test_enginefs_with_file_count(1);
        std::fs::create_dir_all(&enginefs.download_dir).unwrap();
        std::fs::write(
            enginefs.pinned_downloads_path(),
            serde_json::to_vec(&serde_json::json!({ OTHER_HASH: [0] })).unwrap(),
        )
        .unwrap();
        enginefs.restore_pinned_downloads().await;

        let gate = enginefs.reclaim_verdicts().await.gate;
        assert!(
            !gate.releases(&TEST_HASH.to_lowercase(), 0),
            "the live engine"
        );
        assert!(
            !gate.releases(&OTHER_HASH.to_lowercase(), 0),
            "and the dormant pin, which has no engine to speak for it"
        );
        assert!(
            gate.releases("ffffffffffffffffffffffffffffffffffffffff", 0),
            "and nothing else"
        );

        // And it stops being protected the moment the pin does, so the bytes
        // an unpin leaves behind become ordinary cache.
        assert!(
            enginefs
                .unpin_download(OTHER_HASH, 0, false)
                .await
                .unwrap()
                .unpinned
        );
        let after = enginefs.reclaim_verdicts().await.gate;
        assert!(after.releases(&OTHER_HASH.to_lowercase(), 0));
        assert!(
            !after.releases(&TEST_HASH.to_lowercase(), 0),
            "the live engine is protected for as long as it runs"
        );
    }

    /// An engine speaks for its pieces and for nothing else, wherever the
    /// backend says its files are.
    ///
    /// This used to be the other way round -- the protected set was the
    /// backend's `file_path` per file, plus the piece directory -- and it had
    /// to be, while the session wrote whole files. It cannot stay that way
    /// now that the piece store is the default: `<output folder>/<name>` is
    /// still a name the backend reports and is no longer a byte the torrent
    /// owns, and there is deliberately no migration, so the whole-file copy
    /// an earlier version wrote is sitting at exactly that path with nothing
    /// but the cache cleaner ever going to reclaim it. An engine that named
    /// it would keep its own superseded data alive for as long as the
    /// torrent is in the session, orphaned and immortal both.
    ///
    /// The gate has no way left to name a path at all -- it answers about
    /// an info hash and a piece index -- so the question this asks is that
    /// the answer does not move when the backend's output folder does.
    #[tokio::test]
    async fn an_engine_speaks_for_its_pieces_and_not_the_files_it_used_to_write() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        let root = enginefs.download_dir.clone();
        let hash = TEST_HASH.to_lowercase();

        // The whole-file copy an earlier version of this server would have
        // written, at the path the backend reports for file 0.
        let legacy = root.join("show").join("video-0.mkv");
        std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        std::fs::write(&legacy, b"a film nothing reads any more").unwrap();

        for folder in [
            None,
            Some(root.join("show")),
            Some(std::path::PathBuf::from("/offline").join(TEST_HASH)),
        ] {
            *counters.output_folder.lock().unwrap() = folder.clone();
            let gate = enginefs.reclaim_verdicts().await.gate;
            assert!(
                !gate.releases(&hash, 0),
                "its pieces, whatever the output folder is: {folder:?}"
            );
            assert!(
                gate.releases("ffffffffffffffffffffffffffffffffffffffff", 0),
                "and nothing else: {folder:?}"
            );
        }

        assert!(
            legacy.is_file(),
            "the bytes are still there -- there is no migration"
        );
    }

    // --- the reconciler's free-space arm ---

    /// A reader whose every read parks, like a `FileStream` on a piece the
    /// torrent is not downloading.
    struct ParkedStream;

    impl tokio::io::AsyncRead for ParkedStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
    }

    impl tokio::io::AsyncSeek for ParkedStream {
        fn start_seek(
            self: std::pin::Pin<&mut Self>,
            _position: std::io::SeekFrom,
        ) -> std::io::Result<()> {
            Ok(())
        }
        fn poll_complete(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<u64>> {
            std::task::Poll::Ready(Ok(0))
        }
    }

    /// A fixture whose housekeeping sweep is not running. The sweep no
    /// longer pauses anything, but it still removes engines it finds idle,
    /// and these tests advance the clock past every timeout there is: left
    /// running it would take the very torrent they watch the reconciler
    /// decide about out of the registry underneath them.
    fn test_enginefs_for_reconciler(
        file_count: usize,
    ) -> (BackendEngineFS<FakeBackend>, Arc<FakeCounters>) {
        let (enginefs, counters) = test_enginefs_with_file_count(file_count);
        if let Some(sweep) = enginefs.take_sweep_task() {
            sweep.abort();
        }
        (enginefs, counters)
    }

    /// Every call the reconciler could make and does not, in one place.
    fn assert_nothing_moved(counters: &FakeCounters) {
        assert_eq!(counters.stop_torrent.load(Ordering::SeqCst), 0);
        assert_eq!(counters.start_torrent.load(Ordering::SeqCst), 0);
        assert_eq!(counters.restart_from_error.load(Ordering::SeqCst), 0);
    }

    /// What the torrent is actually doing, which is the only thing any of
    /// these tests may conclude from: the backend's state machine, never a
    /// flag this codebase wrote.
    async fn run_state_of(enginefs: &BackendEngineFS<FakeBackend>, hash: &str) -> RunState {
        enginefs
            .peek_engine(hash)
            .await
            .expect("the engine is still registered")
            .handle
            .run_state()
    }

    /// The idle arm, acted on: seeding off, nothing playing, quiet for the
    /// whole grace, and a volume with room to spare -- so the torrent is
    /// stopped, by the reconciler, on the pass that decided it.
    ///
    /// Stopped **once**, not once per pass. The backend refuses a second
    /// pause, so a reconciler that asked every tick would log a failure
    /// every two seconds for every stopped torrent there is; the counter
    /// counts the asking rather than the succeeding, which is what makes
    /// that visible.
    #[tokio::test(start_paused = true)]
    async fn the_idle_arm_stops_the_torrent_and_asks_only_once() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE).await;

        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Stop)]
        );
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Paused,
            "the idle arm decided Stop and the reconciler made it so"
        );
        assert_eq!(counters.stop_torrent.load(Ordering::SeqCst), 1);

        enginefs.reconcile_tick().await;
        enginefs.reconcile_tick().await;
        assert_eq!(
            counters.stop_torrent.load(Ordering::SeqCst),
            1,
            "a torrent that is already stopped is not asked to stop again"
        );
    }

    /// The anti-flap dwell, and both halves of its asymmetry.
    ///
    /// A condition that oscillates around one of the ladder's lines costs
    /// peers rather than CPU -- every stop drops the swarm and every start
    /// re-announces -- so after the reconciler moves a torrent, its
    /// **timer** leaves it stopped for `RECONCILE_MIN_DWELL`. A user is
    /// never made to wait that out: a playback start goes through it, which
    /// is the half that keeps the dwell from becoming a stall somebody can
    /// see.
    #[tokio::test(start_paused = true)]
    async fn the_timer_waits_out_a_dwell_after_moving_a_torrent_and_a_playback_does_not() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE).await;

        // The idle arm stops it, which is the transition the dwell runs from.
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);

        // The condition that stopped it goes away at once. Written to the
        // flag rather than through `set_seeding_enabled`, which would
        // reconcile for itself with the trigger that is exempt.
        enginefs.seeding_enabled.store(true, Ordering::Relaxed);
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Run)],
            "the ladder wants it running again"
        );
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Paused,
            "and the timer leaves it alone this soon after moving it"
        );
        assert_eq!(counters.start_torrent.load(Ordering::SeqCst), 0);

        // A user pressing play is not made to wait for it.
        enginefs.on_stream_start(TEST_HASH, 0).await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
        assert_eq!(counters.start_torrent.load(Ordering::SeqCst), 1);
    }

    /// The dwell is a delay and not a refusal: once it is out, the timer
    /// starts the torrent with nobody having asked.
    #[tokio::test(start_paused = true)]
    async fn the_timer_starts_the_torrent_once_the_dwell_is_out() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE).await;
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);

        enginefs.seeding_enabled.store(true, Ordering::Relaxed);
        tokio::time::advance(RECONCILE_MIN_DWELL + Duration::from_secs(1)).await;

        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
        assert_eq!(counters.start_torrent.load(Ordering::SeqCst), 1);
    }

    /// A move made inside the process's first second is a move, and the
    /// dwell that follows it is the same dwell.
    ///
    /// [`Clock::now_secs`] is `epoch.elapsed().as_secs()`, so it answers 0
    /// for a whole second, and the field the dwell reads used to start at 0
    /// as its "never moved" -- which the timer treats as exempt. A real
    /// transition recorded in that first second was therefore read as "this
    /// reconciler has never moved it" and the dwell was skipped for it. The
    /// startup path reaches it on any quick boot: `server::run` applies the
    /// persisted seeding setting before the reconciler's timer starts, and
    /// that call reconciles every engine there is -- on a volume under the
    /// floor, stopping every torrent that wants to write.
    ///
    /// The stop here is the free-space arm's, because the idle arm cannot
    /// fire this early now that its grace runs from the process's own
    /// start -- which is the point: the collision is about the *clock*, not
    /// about which arm moved the torrent.
    #[tokio::test(start_paused = true)]
    async fn a_move_in_the_processs_first_second_still_holds_the_dwell() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(CACHE_FREE_SPACE_FLOOR - 1));
        assert_eq!(
            enginefs.clock.now_secs(),
            0,
            "the whole point: the process is still inside its first second"
        );

        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Stop)]
        );
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);

        // The volume clears at once, well over the resume margin.
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Run)],
            "the ladder wants it running again"
        );
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Paused,
            "and the timer leaves it alone this soon after moving it"
        );
        assert_eq!(counters.start_torrent.load(Ordering::SeqCst), 0);

        // A delay and not a refusal, the same as for a move made later.
        tokio::time::advance(RECONCILE_MIN_DWELL).await;
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
        assert_eq!(counters.start_torrent.load(Ordering::SeqCst), 1);
    }

    /// The activity inputs, both ways round: a torrent with a stream open
    /// on it runs however long it has been since anything asked it for a
    /// byte, and the same torrent with the stream gone is stopped.
    #[tokio::test(start_paused = true)]
    async fn a_stream_open_on_a_torrent_keeps_it_running() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);

        enginefs.on_stream_start(TEST_HASH, 0).await;
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE).await;
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Run)]
        );

        // The stream goes, without going through `on_stream_end` -- which
        // would stamp the engine itself, and this test is about the
        // reconciler's own reading.
        enginefs.active_streams.write().await.clear();
        enginefs.active_file_streams.write().await.clear();

        // The grace runs from the last time the torrent was *used*, which
        // the tick above observed, so it starts again here rather than
        // being already spent: a torrent watched until a second ago has not
        // been idle for the grace, whatever the clock said before the
        // stream opened.
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Run)]
        );
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE).await;
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Stop)]
        );
    }

    /// A stream that begins and ends between two ticks still spends the
    /// grace.
    ///
    /// The idle arm's clock is written where activity is *observed*, and
    /// for a stream longer than a [`RECONCILE_INTERVAL`] that is the
    /// reconciler's own pass. A short one -- an HLS segment, a player's
    /// probe read -- can be opened and closed without any timer pass seeing
    /// it; what stamps the engine for it is the reconcile
    /// [`Self::on_stream_start`] awaits for itself, which is taken after
    /// the registers are set and so reads them true. Without that stamp the
    /// very next tick stops the torrent the player is about to ask for the
    /// next segment of: peers dropped and re-announced between two
    /// segments, which is the flapping [`INACTIVE_TORRENT_PAUSE_GRACE`]
    /// exists to prevent.
    #[tokio::test(start_paused = true)]
    async fn a_stream_shorter_than_a_tick_still_spends_the_grace() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);

        // Opened and closed without a single pass in between.
        enginefs.on_stream_start(TEST_HASH, 0).await;
        enginefs.on_stream_end(TEST_HASH, 0).await;

        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Run)],
            "the player is between two segments, not gone"
        );
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE).await;
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Stop)],
            "and once the grace is out it really is gone"
        );
    }

    /// A client polling the statistics is *looking at* a torrent, not
    /// watching it, and the idle arm must not confuse the two.
    ///
    /// `GET /{infoHash}/stats.json` reaches its engine through
    /// [`BackendEngineFS::get_engine`], which counts as a poll for the
    /// registry's idle eviction -- rightly: nothing may drop an engine a
    /// client is still asking about. The idle arm's grace used to be
    /// measured from that same `last_accessed`, and it was the arm's only
    /// quiet test, so a details page left open in the client -- which polls
    /// every few seconds -- reset the grace before it could ever run out.
    /// Seeding off, nobody watching, and the torrent downloading all night.
    ///
    /// Master's sweep read the five activity registers and never
    /// `last_accessed`, so it had no such hole; the reconciler has to keep
    /// that guarantee, which is the whole point of the seeding-off policy.
    ///
    /// The torrent is used first, so what is under test is the grace
    /// running out rather than the "nothing has ever used this" answer.
    #[tokio::test(start_paused = true)]
    async fn polling_the_statistics_does_not_keep_an_idle_torrent_running() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);

        // Watched once, then not: the registers are cleared by hand so that
        // the only thing touching this engine afterwards is the poll.
        enginefs.on_stream_start(TEST_HASH, 0).await;
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
        enginefs.active_streams.write().await.clear();
        enginefs.active_file_streams.write().await.clear();

        // Five minutes of a details page polling every ten seconds. Both
        // halves of what the route does to the engine, because both write
        // `last_accessed`: `routes::system::stats_target` finds it with
        // `get_engine`, and then `engine.get_statistics()` builds the body.
        // Either one alone, read as activity, is enough to hold the grace
        // open for ever.
        for _ in 0..30 {
            let engine = enginefs
                .get_engine(TEST_HASH)
                .await
                .expect("the statistics route reaches its engine this way");
            engine.get_statistics().await;
            tokio::time::advance(Duration::from_secs(10)).await;
            enginefs.reconcile_tick().await;
        }

        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Paused,
            "nobody is watching it, and we have promised to upload nothing"
        );
        assert_eq!(counters.stop_torrent.load(Ordering::SeqCst), 1);
    }

    /// The free-space arm through the whole tick, and the arm is above
    /// `playing` on purpose: seeding is on and a stream is open, so the
    /// only thing that can stop this torrent is the volume under it -- and
    /// the torrent it stops is exactly the one somebody is watching, which
    /// is the one filling the disk.
    ///
    /// Stopped once, not once per tick (the backend refuses a second pause,
    /// so a reconciler that asked every tick would log a failure every two
    /// seconds), and started again the moment the same torrent stops
    /// wanting to write.
    #[tokio::test(start_paused = true)]
    async fn a_volume_under_the_floor_stops_even_a_playing_torrent() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(CACHE_FREE_SPACE_FLOOR - 1));
        enginefs.on_stream_start(TEST_HASH, 0).await;

        assert!(enginefs.seeding_enabled.load(Ordering::Relaxed));
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Stop)]
        );
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);

        enginefs.reconcile_tick().await;
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);
        assert_eq!(
            counters.stop_torrent.load(Ordering::SeqCst),
            1,
            "a torrent that is already stopped is not asked to stop again"
        );

        // The same torrent with everything it wants writes nothing, so the
        // volume under it is not about it: keeping it stopped would cost
        // its seeding for no bytes saved. The dwell is sat out first, since
        // the reconciler stopped this torrent itself and a start of it is
        // the timer's own.
        tokio::time::advance(RECONCILE_MIN_DWELL).await;
        counters.seeded.store(true, Ordering::SeqCst);
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Run)]
        );
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
    }

    /// A volume that cannot be probed is not a full one: the timer says
    /// nothing about it, and a playback starting on the same torrent at the
    /// same moment gets its stream.
    #[tokio::test(start_paused = true)]
    async fn an_unreadable_volume_stops_nothing_and_starts_a_playback() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Err(std::io::Error::other("no statvfs here")));

        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Leave)]
        );
        assert_eq!(
            enginefs
                .reconcile_hash(TEST_HASH, Trigger::PlaybackStart)
                .await,
            Some(Decision::Run)
        );
    }

    /// A magnet still fetching its info dictionary keeps its swarm, whatever
    /// else is true: seeding off, idle for the whole grace, and a volume
    /// with nothing left on it.
    #[tokio::test(start_paused = true)]
    async fn a_torrent_without_metadata_is_decided_to_run() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(0);
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE).await;

        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Run)]
        );
    }

    /// The hysteresis, read off the torrent rather than off a stored bit:
    /// a stopped torrent inside the band stays stopped, and the same
    /// reading with the margin cleared starts it. Nothing about this
    /// torrent changes between the two ticks except the volume.
    #[tokio::test(start_paused = true)]
    async fn a_stopped_torrent_is_left_stopped_inside_the_band() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        let available = Arc::new(AtomicU64::new(
            CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN - 1,
        ));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));

        // The torrent is stopped, and the reconciler reads that off the
        // torrent's state and not off any note of who stopped it.
        enginefs
            .peek_engine(TEST_HASH)
            .await
            .unwrap()
            .handle
            .stop_torrent()
            .await
            .unwrap();
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Stop)]
        );
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Paused,
            "inside the band a stopped torrent stays stopped"
        );

        available.store(
            CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN,
            Ordering::SeqCst,
        );
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Run)]
        );
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
        assert_eq!(
            counters.restart_from_error.load(Ordering::SeqCst),
            0,
            "the space lift is not the error restart"
        );
    }

    /// Looking at a torrent is not using it. The reconciler polls every
    /// engine every two seconds, so an eye that counted as a poll would
    /// keep every torrent this server has ever seen from ever being idle --
    /// including for the idle arm of its own decision.
    #[tokio::test(start_paused = true)]
    async fn reconciling_a_torrent_does_not_count_as_using_it() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE).await;

        assert_eq!(
            enginefs.reconcile_hash(TEST_HASH, Trigger::Timer).await,
            Some(Decision::Stop)
        );
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Stop)],
            "the torrent is still as idle as it was before it was looked at"
        );
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Stop)],
            "and stays so however often it is looked at"
        );
    }

    /// A pin is a promise to have the file offline, so the idle policy does
    /// not apply to the torrent holding it -- with seeding off and nothing
    /// playing, the pinned torrent still runs.
    #[tokio::test(start_paused = true)]
    async fn a_pinned_torrent_is_decided_to_run() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE).await;

        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Run)]
        );
    }

    /// A backend that pauses and resumes its own torrents gets no second
    /// opinion from here. Two owners of one pause is the whole class of bug
    /// the reconciler exists to end, and it must not start by becoming one.
    #[tokio::test(start_paused = true)]
    async fn a_torrent_whose_backend_owns_its_lifecycle_is_left_alone() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        counters.native_lifecycle.store(true, Ordering::SeqCst);
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE).await;

        assert!(enginefs.reconcile_tick().await.is_empty());
        assert_eq!(
            enginefs.reconcile_hash(TEST_HASH, Trigger::Timer).await,
            None
        );
    }

    /// Every torrent gets a decision, and the volume they share is read
    /// once for the pass: this runs every two seconds over every torrent
    /// there is, and one `statvfs` per torrent is a syscall per torrent per
    /// tick for one number.
    #[tokio::test(start_paused = true)]
    async fn a_pass_decides_for_every_torrent_and_probes_each_volume_once() {
        let TwoEngines {
            mut enginefs,
            counters: _counters,
            removed: _removed,
        } = test_enginefs_with_two_engines();
        if let Some(sweep) = enginefs.take_sweep_task() {
            sweep.abort();
        }
        let probes = Arc::new(AtomicUsize::new(0));
        let counted = probes.clone();
        enginefs.set_free_space_probe(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(u64::MAX)
        });

        let mut decided: Vec<String> = enginefs
            .reconcile_tick()
            .await
            .into_iter()
            .map(|(hash, decision)| {
                assert_eq!(decision, Decision::Run);
                hash
            })
            .collect();
        decided.sort();
        assert_eq!(decided, vec![TEST_HASH.to_string(), OTHER_HASH.to_string()]);
        assert_eq!(
            probes.load(Ordering::SeqCst),
            1,
            "both torrents write to the same folder"
        );
    }

    /// The loop runs on its own, on its own interval, and moves nothing
    /// while it does.
    #[tokio::test(start_paused = true)]
    async fn the_reconciler_ticks_by_itself() {
        let (enginefs, counters) = test_enginefs_for_reconciler(1);
        let enginefs = Arc::new(enginefs);
        let task = enginefs.start_reconciler();

        assert!(
            wait_until(TEST_WAIT_BOUND, || counters
                .has_metadata
                .load(Ordering::SeqCst)
                >= 3)
            .await,
            "the reconciler asked the torrent about itself, tick after tick"
        );
        task.abort();
        assert_nothing_moved(&counters);
    }

    /// The whole point: a torrent that is writing is stopped when the
    /// volume falls under the floor, before the filesystem stops it with
    /// ENOSPC and librqbit declares it dead -- and it is listed for the
    /// cleaner, which is what makes room for it. Stopped once, not once per
    /// tick; started again only once the volume is a margin over the floor,
    /// so it does not flap at the line.
    ///
    /// Every assertion is on what the torrent is doing -- the backend's own
    /// state machine -- and none on any record of who stopped it. There is
    /// no such record any more, and while there was one, every test in this
    /// area asserted on it and so none of them could see a stop that did
    /// not happen.
    #[tokio::test(start_paused = true)]
    async fn a_writing_torrent_is_stopped_under_the_floor_and_started_over_the_margin() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        let available = Arc::new(AtomicU64::new(CACHE_FREE_SPACE_FLOOR));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        // At the floor: fine.
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
        assert!(!engine.is_stopped_for_space().await);

        // A byte under it: stopped, once, and the cleaner's business now.
        available.store(CACHE_FREE_SPACE_FLOOR - 1, Ordering::SeqCst);
        enginefs.reconcile_tick().await;
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);
        assert_eq!(counters.stop_torrent.load(Ordering::SeqCst), 1);
        assert!(engine.is_stopped_for_space().await);
        assert_eq!(
            enginefs.out_of_space_torrents().await,
            vec![TEST_HASH.to_string()]
        );
        assert!(!engine.reads_refused(), "its readers wait for the cleaner");

        // Back over the floor but inside the margin: still stopped -- the
        // margin is what it has to see cleared before anything starts it
        // again -- and said so, because the ladder holding a torrent stopped
        // is the condition a client and the cleaner both need to know about.
        // Eviction still measures the floor, so the torrent's own files stay
        // protected while the volume is over it.
        available.store(
            CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN - 1,
            Ordering::SeqCst,
        );
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);
        assert!(!engine.is_stopped_for_space().await, "the floor is clear");
        assert!(
            !enginefs.out_of_space_torrents().await.is_empty(),
            "but the ladder is still holding it, and says so"
        );

        // The margin over: started again, and off the cleaner's list. Past
        // the dwell as well, which every timer start of a torrent this
        // reconciler stopped has to be.
        available.store(
            CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN,
            Ordering::SeqCst,
        );
        tokio::time::advance(RECONCILE_MIN_DWELL).await;
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
        assert_eq!(counters.start_torrent.load(Ordering::SeqCst), 1);
        assert_eq!(
            counters.restart_from_error.load(Ordering::SeqCst),
            0,
            "the space lift is its own transition, not the error restart"
        );
        assert!(!engine.is_stopped_for_space().await);
        assert!(enginefs.out_of_space_torrents().await.is_empty());
    }

    /// The reconciler stops writers. A torrent that has everything it
    /// wants, and one the backend has already stopped with an error: neither
    /// is writing, and stopping them would only cost peers (and, for the
    /// finished one, its seeding) for nothing. Nor is a volume that cannot
    /// be probed a full one.
    #[tokio::test(start_paused = true)]
    async fn the_reconciler_stops_only_what_writes() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(0));
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        counters.seeded.store(true, Ordering::SeqCst);
        enginefs.reconcile_tick().await;
        counters.seeded.store(false, Ordering::SeqCst);

        counters.in_error_state.store(true, Ordering::SeqCst);
        enginefs.reconcile_tick().await;
        counters.in_error_state.store(false, Ordering::SeqCst);

        enginefs.set_free_space_probe(|_| Err(std::io::Error::other("no statvfs here")));
        enginefs.reconcile_tick().await;

        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
        assert_eq!(counters.stop_torrent.load(Ordering::SeqCst), 0);
        assert!(!engine.is_stopped_for_space().await);

        // Whereas the same torrent, writing, is stopped -- pinned or not
        // (the pin is accepted while there is room, as a pin is, and the
        // volume fills under it).
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);
        assert!(engine.is_stopped_for_space().await);
    }

    /// The ladder measures the volume the *pieces* land on, which since the
    /// piece store became the session's default storage is the only volume
    /// any payload byte is written to. A torrent's output folder still
    /// names a place -- and a pinned one names a place under `downloadsDir`,
    /// a setting whose entire purpose is a second card -- but nothing writes
    /// there any more, so probing it answers about the wrong device in both
    /// directions: it would leave a torrent running onto a full store, and
    /// stop one whose store has room because some other card is full.
    #[tokio::test(start_paused = true)]
    async fn the_free_space_arm_measures_the_volume_the_pieces_land_on() {
        // Placed as a pin is: an output folder on a card of its own.
        let placed = std::path::PathBuf::from("/offline").join(TEST_HASH);

        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        *counters.output_folder.lock().unwrap() = Some(placed.clone());
        let pieces = enginefs.piece_store().path().to_path_buf();
        enginefs.set_free_space_probe(move |path| {
            Ok(if path.starts_with(&pieces) {
                0
            } else {
                u64::MAX
            })
        });
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.reconcile_tick().await;
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Paused,
            "the volume the pieces are written to is full, so the torrent must be stopped"
        );
        assert!(
            engine.is_stopped_for_space().await,
            "and a client asking what is wrong is told the device is"
        );
        assert_eq!(
            enginefs.out_of_space_torrents().await,
            vec![TEST_HASH.to_string()],
            "and the cleaner is asked for the room that would end it"
        );

        // The other direction: the placed folder's card is full and the
        // store's has room, so there is nothing to stop.
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        *counters.output_folder.lock().unwrap() = Some(placed);
        let pieces = enginefs.piece_store().path().to_path_buf();
        enginefs.set_free_space_probe(move |path| {
            Ok(if path.starts_with(&pieces) {
                u64::MAX
            } else {
                0
            })
        });
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.reconcile_tick().await;
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Live,
            "a full card nothing writes to is not this torrent's problem"
        );
        assert!(!engine.is_stopped_for_space().await);
        assert!(enginefs.out_of_space_torrents().await.is_empty());
    }

    /// The master bug this closes. The free-space watch skipped any engine
    /// whose `idle_paused` flag was set *before* it looked at the volume, so
    /// an idle-paused torrent on a full volume was never marked stopped for
    /// space -- while the stream route's `507` was gated on that mark alone,
    /// so the next playback was let through and put the torrent straight
    /// back onto the full disk.
    ///
    /// Nothing here is keyed on who stopped it, because there is nothing
    /// left that could be: the question is asked of the torrent's state and
    /// the volume's, so the answer is the same whichever policy took the
    /// pause, and the reconciler will not start it while the volume is
    /// short whatever else is true of it.
    #[tokio::test(start_paused = true)]
    async fn a_torrent_already_stopped_on_a_full_volume_is_stopped_for_space_too() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(0));
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        // Stopped before the volume was ever looked at -- an idle stop this
        // process took, or one it inherited from the last one.
        stop_torrent(&enginefs, TEST_HASH).await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);

        enginefs.reconcile_tick().await;
        assert!(
            engine.is_stopped_for_space().await,
            "it is stopped and its volume is full; who stopped it is not the question"
        );
        assert_eq!(
            enginefs.out_of_space_torrents().await,
            vec![TEST_HASH.to_string()],
            "so the cleaner is told there is something to make room for"
        );

        // A playback starting on it asks the reconciler, and the reconcile
        // refuses to start it: measured against the floor, which is what
        // the volume is under, and consulting no record of who stopped it
        // first. On master this is where a resume that could not tell one
        // pause from another put the torrent back onto the full disk.
        enginefs.on_stream_start(TEST_HASH, 0).await;
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Paused,
            "nothing left it running on the full volume"
        );
        assert_eq!(
            counters.start_torrent.load(Ordering::SeqCst),
            0,
            "and the reconciler did not start it either"
        );
    }

    /// The hysteresis band is a window a starting playback must not fall
    /// into, and `on_stream_start` is what closes it.
    ///
    /// A torrent stopped for want of space is not started again by a timer
    /// until the volume has cleared the floor *plus* the resume margin --
    /// the margin exists so a timer does not restart something into a
    /// nearly-full volume for nobody. A user pressing play is not nobody,
    /// and inside that band nothing else would start the torrent for them,
    /// because starting a stopped torrent is the reconciler's alone. The
    /// reader would open on a torrent
    /// nothing is fetching for and park -- a spinner with no end and no
    /// error, which is the failure the stall bound exists to convert into
    /// an error twenty seconds later.
    #[tokio::test(start_paused = true)]
    async fn a_playback_starting_inside_the_hysteresis_band_starts_the_torrent() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        let available = Arc::new(AtomicU64::new(CACHE_FREE_SPACE_FLOOR - 1));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));

        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);

        // Inside the band: over the floor, under the floor plus the margin.
        available.store(
            CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN - 1,
            Ordering::SeqCst,
        );
        enginefs.reconcile_tick().await;
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Paused,
            "a timer leaves it alone in the band, which is what the margin is for"
        );

        enginefs.on_stream_start(TEST_HASH, 0).await;
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Live,
            "the reader about to open on it has something fetching for it"
        );
        assert_eq!(counters.start_torrent.load(Ordering::SeqCst), 1);
    }

    /// The pause a fresh process cannot explain is the one it must be able
    /// to lift, and this is that case: seeding is on, the volume is roomy,
    /// the ladder wants the torrent running, and it is stopped by something
    /// that left no note -- which is every pause that survived a restart.
    ///
    /// On master the three call sites that could have started it all read
    /// `if idle_paused.swap(false) && resume()`, which is `false && ...` in
    /// a fresh process. Nothing started it, ever.
    #[tokio::test(start_paused = true)]
    async fn a_stop_nobody_can_explain_is_lifted_by_the_reconciler() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        assert!(enginefs.seeding_enabled.load(Ordering::Relaxed));
        stop_torrent(&enginefs, TEST_HASH).await;

        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Run)],
            "the ladder wants it running"
        );
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
        assert_eq!(counters.start_torrent.load(Ordering::SeqCst), 1);
    }

    /// The defect that four rounds of this work kept re-introducing, and it
    /// is a property of librqbit rather than of any policy: `Session::unpause`
    /// writes `paused = false` and returns success, and if an initial check
    /// is in flight the continuation applies the `start_paused` it captured
    /// when the check began and parks the torrent in `Paused`. The unpause
    /// is swallowed. Every earlier fix believed the return value and the
    /// flag, and left the torrent stopped for good.
    ///
    /// A reconciler cannot be fooled by that, because it does not believe
    /// its own past calls: the next pass reads the state machine, finds the
    /// torrent still stopped, and starts it again. Asserted on what the
    /// torrent is doing and never on the call having been made.
    #[tokio::test(start_paused = true)]
    async fn an_unpause_the_backend_swallows_is_made_again_next_tick() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(CACHE_FREE_SPACE_FLOOR - 1));
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);

        // Room again -- but the torrent is inside a check that will park it
        // back in `Paused` whatever the unpause said. Each timer start
        // waits out a dwell from the last call the reconciler made on this
        // torrent, the swallowed one included: what it does not do is
        // believe that call happened.
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        counters.swallow_start.store(true, Ordering::SeqCst);
        tokio::time::advance(RECONCILE_MIN_DWELL).await;
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Run)]
        );
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Paused,
            "the backend swallowed the unpause, whatever it answered"
        );

        // The check has ended. One tick, and the torrent is running.
        counters.swallow_start.store(false, Ordering::SeqCst);
        tokio::time::advance(RECONCILE_MIN_DWELL).await;
        enginefs.reconcile_tick().await;
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Live,
            "the next pass reads the state machine and starts it again"
        );
    }

    /// A stop rings the cleaner, and what the cleaner then does is make
    /// room; the reconciler's next pass is what starts the torrent, from a
    /// volume reading it takes itself.
    ///
    /// `restart_from_error` is deliberately not that path any more. It is
    /// the transition out of the backend's error state and nothing else, so
    /// it refuses a torrent that is merely stopped -- one method that meant
    /// "the error was dealt with" to one caller and "the space came back" to
    /// another is how an earlier defect got in.
    #[tokio::test(start_paused = true)]
    async fn a_stop_rings_the_cleaner_and_the_next_tick_with_room_starts_the_torrent() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        let available = Arc::new(AtomicU64::new(0));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        let rung = tokio::time::timeout(Duration::from_millis(10), enginefs.out_of_space_signal());
        assert!(rung.await.is_err(), "nothing has been stopped yet");

        enginefs.reconcile_tick().await;
        tokio::time::timeout(TEST_WAIT_BOUND, enginefs.out_of_space_signal())
            .await
            .expect("the stop rang the cleaner");
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);
        assert!(engine.is_stopped_for_space().await);

        // The cleaner's error restart is not the space lift: this torrent
        // is stopped, not errored, and nothing happens to it here.
        assert!(!enginefs.restart_from_error(TEST_HASH).await.unwrap());
        assert_eq!(counters.restart_from_error.load(Ordering::SeqCst), 0);
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);

        // The cleaner made room. The next pass past the dwell is what
        // starts it.
        available.store(u64::MAX, Ordering::SeqCst);
        tokio::time::advance(RECONCILE_MIN_DWELL).await;
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
        assert!(!engine.is_stopped_for_space().await);
        assert!(enginefs.out_of_space_torrents().await.is_empty());
    }

    /// The other half of that: a torrent the backend really did stop with
    /// an error is the cleaner's to restart, and the reconciler will not
    /// touch it however much room there is.
    ///
    /// The restart also lets its reads park again. A torrent whose volume
    /// was short long enough for the stall bound to fail its readers, and
    /// which then died of the ENOSPC the bound was waiting out, comes back
    /// through here -- and a reader opened on it afterwards must wait for
    /// pieces that are being fetched again rather than be handed
    /// `StorageFull` for a disk the cleaner has since emptied.
    #[tokio::test(start_paused = true)]
    async fn the_error_restart_is_the_cleaners_and_the_reconciler_leaves_it() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        let available = Arc::new(AtomicU64::new(0));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        // Stopped, then short for long enough that its readers are failed.
        enginefs.reconcile_tick().await;
        tokio::time::advance(STOPPED_READ_STALL_BOUND).await;
        enginefs.reconcile_tick().await;
        assert!(engine.reads_refused());

        // And then the backend kills it outright.
        available.store(u64::MAX, Ordering::SeqCst);
        counters.out_of_space.store(true, Ordering::SeqCst);
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Leave)]
        );
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Error);
        assert_eq!(counters.start_torrent.load(Ordering::SeqCst), 0);
        assert!(engine.reads_refused(), "and nothing has fetched for them");

        assert!(enginefs.restart_from_error(TEST_HASH).await.unwrap());
        assert_eq!(counters.restart_from_error.load(Ordering::SeqCst), 1);
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
        assert!(
            !engine.reads_refused(),
            "it is fetching again, so a read on it waits rather than fails"
        );
    }

    /// The `Error` arm is only the *settled* one's, and the difference is a
    /// read refusal that would otherwise never lapse.
    ///
    /// `Leave` is deliberately narrower than "the backend killed it": a
    /// torrent this process has not put its want-set back on is not a
    /// statement about a disk, so the ladder answers `Stop` for it -- which
    /// calls nothing (there is nothing running to stop) but does let its
    /// reads park again. Hand the whole `Error` state to the cleaner and
    /// that lift never happens: `restart_from_error` is the only other
    /// thing that lifts one, and the cleaner will not restart a torrent
    /// whose want-set is not back either, so a reader opened on it is
    /// handed `StorageFull` on a volume with room to spare, for good.
    ///
    /// Asserted through [`poll_a_read`] rather than on `reads_refused`,
    /// which is the flag the code under test writes; what a player gets is
    /// this.
    #[tokio::test(start_paused = true)]
    async fn an_errored_torrent_is_the_cleaners_only_once_its_want_set_is_back() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        let available = Arc::new(AtomicU64::new(0));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        // Stopped for space, short past the stall bound, so its readers are
        // failed; then the backend kills the torrent outright.
        enginefs.reconcile_tick().await;
        tokio::time::advance(STOPPED_READ_STALL_BOUND).await;
        enginefs.reconcile_tick().await;
        available.store(u64::MAX, Ordering::SeqCst);
        counters.out_of_space.store(true, Ordering::SeqCst);
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Error);

        // Settled: the cleaner owns it, and its refusal is held for the
        // cleaner to lift by restarting it.
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Leave)]
        );
        assert_eq!(
            poll_a_read(&engine)
                .await
                .expect("the refusal stands")
                .kind(),
            std::io::ErrorKind::StorageFull
        );

        // Unsettled -- a torrent restored without its want-set, which the
        // cleaner will not restart either. The refusal lapses.
        engine.mark_unsettled();
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Stop)]
        );
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Error);
        assert!(
            poll_a_read(&engine).await.is_none(),
            "no arm of the ladder is claiming the device is full, so a read waits"
        );
        assert_eq!(
            counters.stop_torrent.load(Ordering::SeqCst),
            1,
            "and the `Stop` made no call on a torrent that is not running"
        );
    }

    /// To the backend a torrent the reconciler stopped is merely paused,
    /// which the client would show as buffering for ever; the statistics say
    /// what is actually wrong, in the field a torrent error has always used,
    /// and stop saying it when the torrent is back.
    #[tokio::test(start_paused = true)]
    async fn a_torrent_stopped_for_space_reports_it_in_its_statistics() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        let available = Arc::new(AtomicU64::new(0));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        assert_eq!(engine.get_statistics().await.error, None);

        enginefs.reconcile_tick().await;
        let stats = engine.get_statistics().await;
        assert_eq!(stats.phase, StartupPhase::Error);
        assert_eq!(
            stats.error.as_deref(),
            Some(crate::engine::STOPPED_FOR_SPACE_MESSAGE)
        );

        available.store(u64::MAX, Ordering::SeqCst);
        enginefs.reconcile_tick().await;
        let stats = engine.get_statistics().await;
        assert_ne!(stats.phase, StartupPhase::Error);
        assert_eq!(stats.error, None);
    }

    /// A torrent stopped for space is not the cleaner's to unlink piece by
    /// piece -- it keeps its piece map and announces it again the moment it
    /// resumes -- and it is listed whole, so the cleaner can take it through
    /// the engine when nothing else can go. Stopped by the watch or by
    /// librqbit's ENOSPC alike; a pinned one is never listed however it
    /// stopped.
    #[tokio::test]
    async fn a_torrent_stopped_for_space_is_listed_whole_for_the_cleaner() {
        let (mut enginefs, counters) = test_enginefs_with_file_count(2);
        let hash = TEST_HASH.to_lowercase();
        let whole = vec![hash.clone()];

        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.reconcile_tick().await;
        let verdicts = enginefs.reclaim_verdicts().await;
        assert_eq!(verdicts.stopped_for_space, whole);
        assert!(
            !verdicts.gate.releases(&hash, 0),
            "still announced, so not a piece at a time"
        );
        assert!(!verdicts.gate.goes_first(&hash));

        // librqbit's own ENOSPC stop reads the same.
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.reconcile_tick().await;
        assert!(
            enginefs
                .reclaim_verdicts()
                .await
                .stopped_for_space
                .is_empty()
        );
        counters.out_of_space.store(true, Ordering::SeqCst);
        assert_eq!(enginefs.reclaim_verdicts().await.stopped_for_space, whole);

        // The pin outranks the stop.
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        let verdicts = enginefs.reclaim_verdicts().await;
        assert!(!verdicts.gate.releases(&hash, 0));
        assert!(verdicts.stopped_for_space.is_empty());
    }

    /// Evicting a stopped torrent takes the torrent and its files together
    /// (the two records of the data), fails its readers, and refuses the
    /// hash for a cooling-off period -- a player's reconnect and a stats
    /// poll would otherwise re-add it within the second and refill the disk
    /// the cleaner had just emptied. A user's retry, later, is a fresh add.
    /// Nothing is evicted that is not stopped for space, or that is pinned.
    #[tokio::test(start_paused = true)]
    async fn evicting_a_stopped_torrent_takes_it_whole_and_refuses_the_hash_a_while() {
        let (mut enginefs, _counters) = test_enginefs_with_file_count(1);
        let removed_with_files = enginefs.backend.removed_with_files.clone();
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        // Live: not the cleaner's to evict.
        assert!(!enginefs.evict_stopped_torrent(TEST_HASH).await.unwrap());
        assert!(removed_with_files.lock().unwrap().is_empty());

        // Pinned while there was room, then stopped as the volume fills:
        // the pin keeps it.
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.reconcile_tick().await;
        assert!(engine.is_stopped_for_space().await);
        assert!(!enginefs.evict_stopped_torrent(TEST_HASH).await.unwrap());
        enginefs.unpin_download(TEST_HASH, 0, false).await.unwrap();
        assert!(
            engine.is_stopped_for_space().await,
            "the unpin does not restart it"
        );

        // Stopped and unpinned: gone whole.
        assert!(enginefs.evict_stopped_torrent(TEST_HASH).await.unwrap());
        assert_eq!(
            *removed_with_files.lock().unwrap(),
            vec![TEST_HASH.to_string()]
        );
        assert!(enginefs.peek_engine(TEST_HASH).await.is_none());
        assert!(engine.reads_refused(), "its readers are failed");
        assert!(enginefs.out_of_space_torrents().await.is_empty());
        assert!(
            !enginefs.restart_from_error(TEST_HASH).await.unwrap(),
            "and there is nothing left to restart"
        );
        assert!(
            !enginefs.evict_stopped_torrent(TEST_HASH).await.unwrap(),
            "evicting it again does nothing"
        );

        // Inside the cooling-off period every lookup meets the eviction:
        // the poller as a failure record, the blocking add as an error,
        // and neither re-adds the torrent.
        match enginefs.get_or_begin_add_magnet(TEST_HASH, None).await {
            EngineLookup::Failed(failed) => assert!(
                matches!(failed.error, MagnetAddError::EvictedForSpace { .. }),
                "{:?}",
                failed.error
            ),
            _ => panic!("the eviction is what a poller sees"),
        }
        match enginefs.get_or_add_magnet(TEST_HASH, None).await {
            Err(MagnetAddError::EvictedForSpace { info_hash, .. }) => {
                assert_eq!(info_hash, TEST_HASH);
            }
            Ok(_) => panic!("a stream request is refused, not served a fresh add"),
            Err(other) => panic!("refused for the wrong reason: {other:?}"),
        }
        assert!(enginefs.peek_engine(TEST_HASH).await.is_none());

        // After it, a request is a fresh add.
        tokio::time::advance(EVICTED_FOR_SPACE_RETRY_AFTER).await;
        let readded = enginefs
            .get_or_add_magnet(TEST_HASH, None)
            .await
            .expect("the cooling-off period is over");
        assert!(
            !Arc::ptr_eq(&readded, &engine),
            "a new engine, not the corpse"
        );
        assert!(!readded.is_stopped_for_space().await && !readded.reads_refused());
    }

    /// The cooling-off period covers the `.torrent`-file path too.
    ///
    /// It was enforced only in `lookup_or_begin_add_magnet`, so a client
    /// that re-created the torrent from the file rather than from the hash
    /// walked straight past it and started refilling the volume the cleaner
    /// had just emptied -- and the eviction is the pass's last resort,
    /// taken only when nothing else could go. The check is before the add,
    /// because a backend add has already created files by the time it could
    /// be asked what it added.
    #[tokio::test(start_paused = true)]
    async fn a_torrent_file_re_add_is_refused_inside_the_eviction_cooldown() {
        let (mut enginefs, _counters) = test_enginefs_with_file_count(1);
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.reconcile_tick().await;
        assert!(enginefs.evict_stopped_torrent(TEST_HASH).await.unwrap());

        let Err(refused) = enginefs
            .add_torrent(TorrentSource::Bytes(b"a .torrent blob".to_vec()), None)
            .await
        else {
            panic!("the hash is inside its cooling-off period and must be refused");
        };
        match refused.downcast_ref::<MagnetAddError>() {
            Some(MagnetAddError::EvictedForSpace { info_hash, .. }) => {
                assert_eq!(info_hash, TEST_HASH, "and it names the hash it refused");
            }
            _ => panic!("the route needs the typed error to answer 507: {refused:#}"),
        }
        assert!(
            enginefs.peek_engine(TEST_HASH).await.is_none(),
            "the refusal must not have added the torrent on the way to erroring"
        );

        // After the window it is an ordinary add again.
        tokio::time::advance(EVICTED_FOR_SPACE_RETRY_AFTER).await;
        assert!(
            enginefs
                .add_torrent(TorrentSource::Bytes(b"a .torrent blob".to_vec()), None)
                .await
                .is_ok(),
            "the cooling-off period is over"
        );
        assert!(enginefs.peek_engine(TEST_HASH).await.is_some());
    }

    /// A read parked on a piece a stopped torrent will not download is a
    /// player spinning for ever. Refusing reads wakes the parked one to
    /// fail with `StorageFull`, fails a new one at its first poll, and the
    /// reconciler does the refusing itself once the volume has been short
    /// for [`STOPPED_READ_STALL_BOUND`] -- the bound for a cleaner that is
    /// not there to settle it sooner.
    #[tokio::test(start_paused = true)]
    async fn readers_of_a_torrent_stopped_for_space_are_failed_rather_than_parked() {
        use tokio::io::{AsyncRead, AsyncReadExt};
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        let available = Arc::new(AtomicU64::new(0));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        let mut reader = crate::files::FileHandle::new(
            100,
            "video-0.mkv".to_string(),
            Box::new(ParkedStream),
            engine.clone(),
            0,
            0,
        );
        engine.active_streams.fetch_add(1, Ordering::SeqCst);

        // Parked, and registered with the engine.
        let mut buf = [0u8; 16];
        let parked = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(
                std::pin::Pin::new(&mut reader)
                    .poll_read(cx, &mut tokio::io::ReadBuf::new(&mut buf))
                    .is_pending(),
            )
        })
        .await;
        assert!(parked, "a read on a missing piece parks");

        // Stopped, inside the stall bound: the read stays parked (the
        // cleaner is expected to settle it), so a task on it does not end.
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);
        assert!(engine.is_stopped_for_space().await);
        let waiting = tokio::spawn(async move {
            let mut buf = [0u8; 16];
            let result = reader.read(&mut buf).await;
            (reader, result)
        });
        tokio::time::advance(STOPPED_READ_STALL_BOUND - Duration::from_secs(1)).await;
        enginefs.reconcile_tick().await;
        assert!(!engine.reads_refused());
        assert!(!waiting.is_finished(), "still parked inside the bound");

        // The bound passed: the reconciler fails the readers, and the
        // parked read is woken to see it.
        tokio::time::advance(Duration::from_secs(1)).await;
        enginefs.reconcile_tick().await;
        assert!(engine.reads_refused());
        let (mut reader, result) = tokio::time::timeout(TEST_WAIT_BOUND, waiting)
            .await
            .expect("the parked read was woken")
            .unwrap();
        assert_eq!(
            result.expect_err("and failed").kind(),
            std::io::ErrorKind::StorageFull
        );

        // A fresh read fails at once, without touching the stream.
        let mut buf = [0u8; 16];
        assert_eq!(
            reader.read(&mut buf).await.expect_err("refused").kind(),
            std::io::ErrorKind::StorageFull
        );

        // Back to work: reads park again as before.
        available.store(u64::MAX, Ordering::SeqCst);
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
        assert!(!engine.is_stopped_for_space().await && !engine.reads_refused());
        let parked = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(
                std::pin::Pin::new(&mut reader)
                    .poll_read(cx, &mut tokio::io::ReadBuf::new(&mut buf))
                    .is_pending(),
            )
        })
        .await;
        assert!(parked);
        drop(reader);
    }

    /// The stall bound is the volume's, not the torrent's.
    ///
    /// A torrent stopped a minute after the disk filled has readers as
    /// doomed as one stopped the moment it did -- what they are waiting for
    /// is room on the same disk. Counting from each torrent's own stop gave
    /// every latecomer a fresh twenty seconds of a player's spinner for a
    /// volume that had been full the whole time.
    ///
    /// Both torrents here write to the one folder. The second is finished
    /// while the bound runs down, so nothing stops it; the moment it wants
    /// to write again it is stopped, and its readers are failed on that same
    /// pass, with no bound of its own.
    #[tokio::test(start_paused = true)]
    async fn the_stall_bound_belongs_to_the_volume_and_not_to_each_torrent() {
        let TwoEngines {
            mut enginefs,
            counters,
            removed: _removed,
        } = test_enginefs_with_two_engines();
        if let Some(sweep) = enginefs.take_sweep_task() {
            sweep.abort();
        }
        enginefs.set_free_space_probe(|_| Ok(0));
        let [first, latecomer] = counters;
        latecomer.seeded.store(true, Ordering::SeqCst);

        let engines = [
            enginefs.get_engine(TEST_HASH).await.unwrap(),
            enginefs.get_engine(OTHER_HASH).await.unwrap(),
        ];
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);
        assert_eq!(
            run_state_of(&enginefs, OTHER_HASH).await,
            RunState::Live,
            "a finished torrent writes nothing, so the floor is not about it"
        );

        tokio::time::advance(STOPPED_READ_STALL_BOUND).await;
        enginefs.reconcile_tick().await;
        assert!(engines[0].reads_refused(), "the volume has been short");
        assert!(!engines[1].reads_refused(), "and this one is still running");

        // It wants to write again: stopped, and its readers failed with it
        // on the very same pass.
        assert_eq!(first.stop_torrent.load(Ordering::SeqCst), 1);
        latecomer.seeded.store(false, Ordering::SeqCst);
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, OTHER_HASH).await, RunState::Paused);
        assert_eq!(latecomer.stop_torrent.load(Ordering::SeqCst), 1);
        assert!(
            engines[1].reads_refused(),
            "the clock it is judged by is the volume's, which has been short all along"
        );
    }

    /// One poll of a fresh reader on this engine: `None` when the read
    /// parks -- what a read on a piece that is still coming does -- and the
    /// error when it is refused outright.
    ///
    /// Asserted on instead of `Engine::reads_refused`, which is the flag
    /// the code under test writes. What a player actually gets is this.
    async fn poll_a_read(engine: &Arc<Engine<FakeHandle>>) -> Option<std::io::Error> {
        use tokio::io::AsyncRead;
        let mut reader = crate::files::FileHandle::new(
            100,
            "video-0.mkv".to_string(),
            Box::new(ParkedStream),
            engine.clone(),
            0,
            0,
        );
        // Balanced by `FileHandle::drop`, which subtracts one.
        engine.active_streams.fetch_add(1, Ordering::SeqCst);
        let mut buf = [0u8; 16];
        std::future::poll_fn(|cx| {
            let polled = std::pin::Pin::new(&mut reader)
                .poll_read(cx, &mut tokio::io::ReadBuf::new(&mut buf));
            std::task::Poll::Ready(match polled {
                std::task::Poll::Pending => None,
                std::task::Poll::Ready(result) => {
                    Some(result.expect_err("ParkedStream never completes a read"))
                }
            })
        })
        .await
    }

    /// Stop the torrent the way anything stops one -- the reconciler, a
    /// previous process, librqbit's own restore -- and leave no note behind
    /// saying who did: there is nowhere left to put one. A test that wants
    /// a stopped torrent asks for a stopped torrent.
    async fn stop_torrent(enginefs: &BackendEngineFS<FakeBackend>, hash: &str) {
        enginefs
            .peek_engine(hash)
            .await
            .expect("the engine is registered")
            .handle
            .stop_torrent()
            .await
            .expect("a live torrent stops");
    }

    /// The read refusal is the free-space arm's, and it goes when that arm
    /// is not the one deciding -- here on the pass that finds the torrent
    /// already running.
    ///
    /// The refusal used to be lifted only by the reconciler's own
    /// `start_torrent`, and this is the sequence that leaves no such start
    /// to hang it on. It is all ordinary: the torrent is stopped (seeding
    /// off and nothing playing is one way, a previous process is another);
    /// the volume then falls under the floor and stays there past the stall
    /// bound, so the free-space arm -- which sits above the idle arm and
    /// does not care why the torrent is stopped -- fails its readers; the
    /// cleaner empties the volume; and the user presses play. The playback
    /// start's own reconcile is what starts it, and on the *next* pass the
    /// torrent is already `Live`, so there is no start left to hang the
    /// lift on. Every read on that engine then failed with `StorageFull`,
    /// for good, on a volume with room to spare.
    #[tokio::test(start_paused = true)]
    async fn a_playback_that_finds_its_torrent_running_still_lifts_the_read_refusal() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        let available = Arc::new(AtomicU64::new(0));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        stop_torrent(&enginefs, TEST_HASH).await;

        enginefs.reconcile_tick().await;
        tokio::time::advance(STOPPED_READ_STALL_BOUND).await;
        enginefs.reconcile_tick().await;
        assert_eq!(
            poll_a_read(&engine)
                .await
                .expect("nothing is fetching for this read")
                .kind(),
            std::io::ErrorKind::StorageFull
        );

        // The cleaner empties the volume; the user presses play.
        available.store(u64::MAX, Ordering::SeqCst);
        enginefs.on_stream_start(TEST_HASH, 0).await;
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Live,
            "the playback start's reconcile started it"
        );
        assert!(
            poll_a_read(&engine).await.is_none(),
            "and its reads work again on a volume with room to spare"
        );
    }

    /// The same refusal, lifted by the arm that starts nothing at all.
    ///
    /// The cleaner makes room while the torrent is still the idle policy's,
    /// so the pass that follows answers the idle arm's `Stop`: no start, no
    /// call of any kind. The refusal still has to go -- it says the device
    /// has no room, and the device has room.
    #[tokio::test(start_paused = true)]
    async fn a_volume_with_room_lifts_the_refusal_of_a_torrent_the_idle_policy_holds() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        let available = Arc::new(AtomicU64::new(0));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        stop_torrent(&enginefs, TEST_HASH).await;

        enginefs.reconcile_tick().await;
        tokio::time::advance(STOPPED_READ_STALL_BOUND).await;
        enginefs.reconcile_tick().await;
        assert_eq!(
            poll_a_read(&engine)
                .await
                .expect("nothing is fetching for this read")
                .kind(),
            std::io::ErrorKind::StorageFull
        );

        available.store(u64::MAX, Ordering::SeqCst);
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Stop)],
            "nothing is playing, so this pass is the idle arm's"
        );
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Paused,
            "which starts nothing"
        );
        assert_eq!(counters.start_torrent.load(Ordering::SeqCst), 0);
        assert!(
            poll_a_read(&engine).await.is_none(),
            "and its reads still stop failing for a disk that is no longer full"
        );
    }

    /// A torrent that is paused for a reason of its own, on a volume the
    /// rest of the server is happy with, is not out of disk -- and its
    /// files keep the cleaner's protection.
    ///
    /// The band between the floor and the resume margin is the *ladder's*
    /// hysteresis: it decides when a stopped torrent may be started again.
    /// Judging "is this torrent stopped for want of space?" there instead
    /// of at the floor answers for every paused torrent on a volume with
    /// 513 MiB free -- which `ensure_download_disk_ready` serves from
    /// without complaint and the cleaner's own cap treats as fine -- and
    /// two things follow that are both wrong: the client is shown a torrent
    /// error, and the files leave `EvictionClasses::protected` for
    /// `stopped_for_space`, which the cache cleaner does not protect. Those
    /// go to the ordinary oldest-first eviction and are unlinked piecemeal
    /// under a torrent that still holds them open with a piece map that
    /// says it has them.
    #[tokio::test(start_paused = true)]
    async fn a_paused_torrent_inside_the_margin_is_reported_and_offered_to_the_cleaner() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(CACHE_FREE_SPACE_FLOOR + 1));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        stop_torrent(&enginefs, TEST_HASH).await;
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE).await;

        // The pass takes the volume's reading; the volume never went under
        // the floor at all.
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);

        // Inside the margin the ladder is holding this torrent stopped, and
        // `after_stopping_for_space` will fail its reads once the volume has
        // been short for `STOPPED_READ_STALL_BOUND`. So the readers that
        // report a condition are asked at the ladder's own line: a client
        // that is about to be told `StorageFull` is not told everything is
        // fine, and the cleaner is asked for the room that would end it.
        //
        // This asserted the opposite, on the rule that the hysteresis was
        // the ladder's line and nobody else's. That rule made the band an
        // absorbing state: reads refused, `stats.json` reporting buffering
        // with no error, `out_of_space_torrents` empty so no recovery pass
        // ever ran -- and the band is where a cleaner pass leaves the volume
        // by construction, since `CacheLimit::effective` stops the instant
        // `available` reaches the floor. A pinned download stalled at
        // whatever percent it had reached, in silence, for good.
        let stats = engine.get_statistics().await;
        assert_eq!(
            stats.phase,
            StartupPhase::Error,
            "the ladder is holding it stopped, so the client is told so"
        );
        assert!(stats.error.is_some());
        assert!(!enginefs.out_of_space_torrents().await.is_empty());

        // Eviction keeps the floor, deliberately: taking a torrent's files
        // is about whether there is room *now*, not about what the ladder is
        // waiting for, and the volume is over the floor.
        let verdicts = enginefs.reclaim_verdicts().await;
        assert!(
            verdicts.stopped_for_space.is_empty(),
            "and its files are not the cleaner's to take whole"
        );
        assert!(
            !verdicts.gate.releases(&TEST_HASH.to_lowercase(), 0),
            "nor a piece at a time"
        );
        assert!(!engine.is_stopped_for_space().await);
    }

    /// Re-enabling seeding starts the torrents the idle arm stopped, and it
    /// does not hold the engine registry while it waits for the backend to
    /// do it.
    ///
    /// `engines` is a write-preferring `RwLock`: a read guard held across
    /// an await parks every later reader behind any writer that queues
    /// meanwhile, and the await here is `Session::unpause`, which flushes
    /// librqbit's persistence file. One torrent's disk write would stall
    /// every route that wants to look an engine up.
    #[tokio::test]
    async fn re_enabling_seeding_starts_without_holding_the_engine_registry() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        let enginefs = Arc::new(enginefs);
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        stop_torrent(&enginefs, TEST_HASH).await;

        counters.hold_start.store(true, Ordering::SeqCst);
        let switch = tokio::spawn({
            let enginefs = enginefs.clone();
            async move { enginefs.set_seeding_enabled(true).await }
        });
        assert!(
            wait_until(TEST_WAIT_BOUND, || counters
                .start_torrent
                .load(Ordering::SeqCst)
                == 1)
            .await,
            "the start reached the backend and is waiting there"
        );

        let engines = enginefs.engines.clone();
        let writer = tokio::spawn(async move { drop(engines.write().await) });
        assert!(
            wait_until(TEST_WAIT_BOUND, || writer.is_finished()).await,
            "a writer on the engine registry is not parked behind that call"
        );

        counters.start_gate.notify_one();
        switch.await.expect("the switch finished");
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
    }

    /// The same of the reconciler's own tick, whose await is
    /// `Session::pause` -- the other half of the same persistence flush --
    /// and which reaches every torrent there is rather than one.
    #[tokio::test]
    async fn the_reconcilers_tick_stops_without_holding_the_engine_registry() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(CACHE_FREE_SPACE_FLOOR - 1));
        let enginefs = Arc::new(enginefs);

        counters.hold_stop.store(true, Ordering::SeqCst);
        let tick = tokio::spawn({
            let enginefs = enginefs.clone();
            async move { enginefs.reconcile_tick().await }
        });
        assert!(
            wait_until(TEST_WAIT_BOUND, || counters
                .stop_torrent
                .load(Ordering::SeqCst)
                == 1)
            .await,
            "the stop reached the backend and is waiting there"
        );

        let engines = enginefs.engines.clone();
        let writer = tokio::spawn(async move { drop(engines.write().await) });
        assert!(
            wait_until(TEST_WAIT_BOUND, || writer.is_finished()).await,
            "a writer on the engine registry is not parked behind that call"
        );

        counters.stop_gate.notify_one();
        tick.await.expect("the tick finished");
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);
    }

    /// A request that walks away while its reconcile is inside the backend
    /// leaves nothing behind: no half-applied state the next caller has to
    /// undo, no lock nobody will release, and no detached task still
    /// running the decision.
    ///
    /// Dropping a future is how every HTTP handler ends when a player
    /// closes the connection, and the reconcile is now awaited *on* that
    /// future rather than spawned beside it -- which is what makes this
    /// worth pinning. Both outcomes are acceptable and either may happen:
    /// the backend's start had already taken effect, or it had not. What
    /// may not happen is a torrent stuck between the two, or a hash whose
    /// reconcile lock is never released, because that would freeze every
    /// later decision about that torrent for the life of the process.
    #[tokio::test]
    async fn a_reconcile_whose_caller_walked_away_leaves_nothing_behind() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        let enginefs = Arc::new(enginefs);
        stop_torrent(&enginefs, TEST_HASH).await;

        counters.hold_start.store(true, Ordering::SeqCst);
        let request = tokio::spawn({
            let enginefs = enginefs.clone();
            async move { enginefs.on_stream_start(TEST_HASH, 0).await }
        });
        assert!(
            wait_until(TEST_WAIT_BOUND, || counters
                .start_torrent
                .load(Ordering::SeqCst)
                == 1)
            .await,
            "the reconcile reached the backend and is waiting there"
        );

        // The player closed the connection.
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        counters.start_gate.notify_one();

        // Nothing is left holding the hash: a later decision about this
        // torrent gets in, rather than waiting on a guard that was dropped
        // with the future that held it.
        assert!(
            wait_until(TEST_WAIT_BOUND, || enginefs.reconcile_locks.len() == 0).await,
            "the reconcile lock was released with the future that held it"
        );

        // And whichever way it landed, the state is one of the two the
        // ladder recognises -- never a torrent that is neither.
        let after = run_state_of(&enginefs, TEST_HASH).await;
        assert!(
            matches!(after, RunState::Paused | RunState::Live),
            "{after:?}"
        );

        // Nothing detached is still working on it: the call count does not
        // move on its own once the gate is open.
        let calls = counters.start_torrent.load(Ordering::SeqCst);
        tokio::task::yield_now().await;
        assert_eq!(
            counters.start_torrent.load(Ordering::SeqCst),
            calls,
            "a task nobody owns is still reconciling this torrent"
        );

        // And the next reconcile finishes the job either way, which is the
        // property that makes an abandoned reconcile harmless.
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
    }

    /// The same request, walking away at the same moment, leaves no stream
    /// registered either.
    ///
    /// `on_stream_start` increments both stream counters and *then* awaits
    /// the reconcile, which is inside the backend for as long as starting a
    /// torrent takes. The registers it writes have no expiry: nothing ages
    /// them out, and the only thing that ends one is the `on_stream_end`
    /// the caller's guard makes -- which does not exist yet, because
    /// `on_stream_start` has not returned. So a count left behind here is
    /// left behind for good, and the server answers "something is playing"
    /// about this torrent for the rest of the process: the idle arm can
    /// never fire and the sweep never removes the engine.
    ///
    /// Asserted through `playback_is_live`, which is what a client polls
    /// for its activity light, rather than through the counters themselves.
    #[tokio::test]
    async fn a_stream_start_whose_caller_walked_away_registers_no_stream() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        let enginefs = Arc::new(enginefs);
        stop_torrent(&enginefs, TEST_HASH).await;
        assert!(
            !enginefs.playback_is_live().await,
            "nothing is playing before the request arrives"
        );

        counters.hold_start.store(true, Ordering::SeqCst);
        let request = tokio::spawn({
            let enginefs = enginefs.clone();
            async move { enginefs.on_stream_start(TEST_HASH, 0).await }
        });
        assert!(
            wait_until(TEST_WAIT_BOUND, || counters
                .start_torrent
                .load(Ordering::SeqCst)
                == 1)
            .await,
            "the stream is registered and the reconcile it triggered is inside the backend"
        );

        // The player closed the connection before there was a body to read.
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        counters.hold_start.store(false, Ordering::SeqCst);
        counters.start_gate.notify_one();

        let deadline = tokio::time::Instant::now() + TEST_WAIT_BOUND;
        while enginefs.playback_is_live().await {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the stream the abandoned request registered is still registered"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// And it gives up the *file* it selected, which the test above cannot
    /// see.
    ///
    /// Two independent reasons, both of which left the other half of the
    /// rollback -- the per-file counter, the multi-file selection and the
    /// active-file slot -- covered by nothing. `playback_is_live` is a
    /// deliberately narrow oracle: it reads `engine_active_streams` and
    /// `active_streams` and nothing else, because `active_file` and the
    /// selections outlive the stream on purpose (the want-set is planned
    /// from them). But `playing` -- what the ladder actually reads -- comes
    /// from `torrent_activity_registers`, which reads four registers,
    /// `active_file_streams` and a bare `active_multifile_files.contains_key`
    /// among them. And the test above runs over a single-file torrent, so
    /// `activate_file` never reaches `activate_multifile_file` and the
    /// selection branch is not entered at all.
    ///
    /// So this one is multi-file, and asserts through the reconciler: with
    /// seeding off and the grace elapsed, a torrent nothing is using is
    /// stopped. A count or a selection left behind by the abandoned request
    /// is left behind for the life of the process -- nothing ages either
    /// out, and the `on_stream_end` that would clear them belongs to a
    /// guard that was never built -- so `playing` reads true for ever, the
    /// idle arm can never fire, and with seeding off the torrent downloads
    /// a film nobody is watching until the server restarts.
    #[tokio::test]
    async fn a_stream_start_whose_caller_walked_away_gives_up_the_file_it_selected() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(2);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        let enginefs = Arc::new(enginefs);
        stop_torrent(&enginefs, TEST_HASH).await;

        counters.hold_start.store(true, Ordering::SeqCst);
        let request = tokio::spawn({
            let enginefs = enginefs.clone();
            async move { enginefs.on_stream_start(TEST_HASH, 1).await }
        });
        assert!(
            wait_until(TEST_WAIT_BOUND, || counters
                .start_torrent
                .load(Ordering::SeqCst)
                == 1)
            .await,
            "the stream and its file selection are registered and the reconcile              they triggered is inside the backend"
        );

        // The player closed the connection before there was a body to read.
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        counters.hold_start.store(false, Ordering::SeqCst);
        counters.start_gate.notify_one();

        // The undo is a spawned task, so the ladder is asked until it
        // answers rather than once: each pass advances past the grace and
        // reads the registers for itself. Bounded, so a register left
        // behind fails instead of hanging -- and it fails on every pass,
        // since nothing ever clears one.
        tokio::time::pause();
        let idle_stop = vec![(TEST_HASH.to_string(), Decision::Stop)];
        let mut decided = Vec::new();
        for _ in 0..100 {
            tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE + Duration::from_secs(1)).await;
            decided = enginefs.reconcile_tick().await;
            if decided == idle_stop {
                break;
            }
        }
        assert_eq!(
            decided, idle_stop,
            "the file the abandoned request selected still reads as playback"
        );
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Paused,
            "and the torrent it was starting is running for nobody"
        );
    }

    /// A torrent asked to be focused runs, with nothing else registered
    /// anywhere.
    ///
    /// Every condition here says stop: seeding is off, no stream, file
    /// stream or multi-file selection names this torrent, and it has been
    /// quiet for the whole grace. `Trigger::PlaybackStart` does not change
    /// any of that -- it says why the question is being asked, not that
    /// anything is playing -- so the idle arm being the `Timer`'s alone is
    /// the only thing between this reading and `Decision::Stop` on the very
    /// torrent the caller named.
    ///
    /// It was latent rather than absent because the one production caller
    /// runs `on_stream_start` two lines earlier (`routes::stream`), which
    /// does register a stream; a reordering there would have made it live.
    /// This test is the reason that ordering no longer matters.
    ///
    /// Note what is *not* asserted: nothing here reads `last_active_at`.
    /// `focus_torrent` stamps nothing, so after this call the torrent is
    /// still one nothing has been seen using -- and the next `Timer` pass
    /// stops it again, two seconds later, which is the whole cost of the
    /// arm being trigger-gated.
    #[tokio::test(start_paused = true)]
    async fn focusing_a_torrent_starts_it_with_nothing_else_registered() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        stop_torrent(&enginefs, TEST_HASH).await;
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE + Duration::from_secs(1)).await;

        enginefs.focus_torrent(TEST_HASH).await;

        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Live,
            "the torrent the caller asked to focus is the one the ladder stopped"
        );
        assert_eq!(counters.start_torrent.load(Ordering::SeqCst), 1);

        // And the concession is one tick wide, not a grace period: nothing
        // registered a stream and nothing stamped a clock, so the timer --
        // which reads the same conditions and is the owner of the idle
        // policy -- stops it again on its very next pass.
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Stop)]
        );
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);
    }

    /// The statistics snapshot's list of stopped torrents is an
    /// observation, taken from the backend's state machine when the
    /// snapshot is built.
    ///
    /// It used to be the engines carrying an `idle_paused` flag this
    /// process had written, which is empty in a fresh process while the
    /// pauses it described are not -- so after every restart it reported
    /// nothing at all, and the diagnostics line built on it read zero on
    /// exactly the boot where somebody would be looking.
    #[tokio::test(start_paused = true)]
    async fn the_snapshot_lists_the_torrents_that_are_actually_stopped() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));

        assert!(
            enginefs
                .stream_activity_snapshot()
                .await
                .paused_torrents
                .is_empty(),
            "a running torrent is not listed"
        );

        // Stopped with no note anywhere -- which is what a pause inherited
        // from the previous process looks like.
        stop_torrent(&enginefs, TEST_HASH).await;
        assert_eq!(
            enginefs.stream_activity_snapshot().await.paused_torrents,
            vec![TEST_HASH.to_string()]
        );

        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
        assert!(
            enginefs
                .stream_activity_snapshot()
                .await
                .paused_torrents
                .is_empty(),
            "and it stops being listed the moment it is running again"
        );
    }

    // --- free-space check before pinning ---

    #[test]
    fn free_space_allows_requires_the_margin_unless_nothing_is_left_to_write() {
        assert!(free_space_allows(1_000, 500, 400));
        assert!(free_space_allows(900, 500, 400));
        assert!(!free_space_allows(899, 500, 400));
        assert!(!free_space_allows(0, 1, 0));
        assert!(
            free_space_allows(0, 0, 400),
            "complete file: nothing to write"
        );
        assert!(!free_space_allows(u64::MAX - 1, u64::MAX, 1), "no overflow");
    }

    /// `free_space_at` probes the nearest existing ancestor -- the torrent's
    /// folder does not exist before its first write -- and the default
    /// probe answers for a real directory.
    #[test]
    fn probe_at_existing_ancestor_walks_up_to_an_existing_ancestor() {
        let probed = Mutex::new(Vec::new());
        let probe = |path: &std::path::Path| {
            probed.lock().unwrap().push(path.to_path_buf());
            if path == std::path::Path::new("/root") {
                Ok(42)
            } else {
                Err(std::io::Error::from(std::io::ErrorKind::NotFound))
            }
        };
        assert_eq!(
            probe_at_existing_ancestor(&probe, std::path::Path::new("/root/downloads/hash"))
                .unwrap(),
            42
        );
        assert_eq!(
            probed.lock().unwrap().as_slice(),
            &[
                std::path::PathBuf::from("/root/downloads/hash"),
                "/root/downloads".into(),
                "/root".into()
            ]
        );
        assert!(
            probe_at_existing_ancestor(&probe, std::path::Path::new("/nowhere/at/all")).is_err()
        );

        let tmp = tempfile::tempdir().unwrap();
        let real = |path: &std::path::Path| fs4::available_space(path);
        assert!(probe_at_existing_ancestor(&real, tmp.path()).unwrap() > 0);
        assert!(
            probe_at_existing_ancestor(&real, &tmp.path().join("not").join("yet")).unwrap() > 0
        );
    }

    /// A pin is refused when the volume lacks the file's missing bytes plus
    /// the margin, the torrent the refused pin added is dropped again, a
    /// complete file needs no space, and a volume that cannot be probed does
    /// not block the pin.
    #[tokio::test]
    async fn pin_download_refuses_without_the_free_space_margin() {
        let (mut enginefs, _counters) = test_enginefs_unmanaged();
        // Fake files are 100 bytes, half downloaded: 50 remain.
        let available = Arc::new(AtomicU64::new(PIN_FREE_SPACE_MARGIN + 49));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));

        let err = match enginefs.pin_download(TEST_HASH, 0, None).await {
            Ok(_) => panic!("must refuse"),
            Err(err) => err,
        };
        match err {
            PinDownloadError::InsufficientSpace {
                required,
                available,
                margin,
            } => {
                assert_eq!(required, PIN_FREE_SPACE_MARGIN + 50);
                assert_eq!(available, PIN_FREE_SPACE_MARGIN + 49);
                assert_eq!(margin, PIN_FREE_SPACE_MARGIN);
            }
            other => panic!("unexpected error: {other}"),
        }
        assert!(enginefs.get_engine(TEST_HASH).await.is_none());
        assert_eq!(
            enginefs.backend.removed.lock().unwrap().as_slice(),
            &[TEST_HASH.to_string()],
            "the torrent this pin added goes with the refusal, its bytes stay"
        );
        assert!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty()
        );
        assert!(enginefs.pinned_downloads().await.is_empty());

        available.store(PIN_FREE_SPACE_MARGIN + 50, Ordering::SeqCst);
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert_eq!(enginefs.pinned_downloads().await.len(), 1);

        // Already managed and pinned: a refused second pin keeps the engine.
        available.store(0, Ordering::SeqCst);
        assert!(matches!(
            enginefs.pin_download(TEST_HASH, 1, None).await,
            Err(PinDownloadError::InsufficientSpace { .. })
        ));
        assert!(enginefs.get_engine(TEST_HASH).await.is_some());
        assert_eq!(enginefs.backend.removed.lock().unwrap().len(), 1);
        assert!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty()
        );

        // Complete file: nothing to write, no space needed.
        let (mut enginefs, counters) = test_enginefs_with_file_count(2);
        counters.seeded.store(true, Ordering::SeqCst);
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();

        // Unprobeable volume: pinned anyway.
        let (mut enginefs, _counters) = test_enginefs_with_file_count(2);
        enginefs.set_free_space_probe(|_| Err(std::io::Error::other("no statvfs here")));
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
    }

    /// A pin is measured against the volume its bytes land on, which is the
    /// piece store's root -- the only volume a pin can write to, now that
    /// nothing places a torrent anywhere. It used to probe
    /// `<downloadsDir>/<info hash>`, which passed a pin onto a full store
    /// and refused one that had all the room it needed.
    #[tokio::test]
    async fn a_pin_is_measured_against_the_volume_the_pieces_land_on() {
        // Fake files are 100 bytes, half downloaded: 50 remain to write.
        let (mut enginefs, _counters) = test_enginefs_with_file_count(2);
        let pieces = enginefs.piece_store().path().to_path_buf();
        enginefs.set_free_space_probe(move |path| {
            Ok(if path.starts_with(&pieces) {
                PIN_FREE_SPACE_MARGIN + 49
            } else {
                u64::MAX
            })
        });
        match enginefs.pin_download(TEST_HASH, 0, None).await {
            Err(PinDownloadError::InsufficientSpace {
                required,
                available,
                ..
            }) => {
                assert_eq!(required, PIN_FREE_SPACE_MARGIN + 50);
                assert_eq!(
                    available,
                    PIN_FREE_SPACE_MARGIN + 49,
                    "the store's volume is what was short, and what is reported"
                );
            }
            Ok(_) => panic!("must refuse: the volume the pieces land on is short"),
            Err(other) => panic!("unexpected error: {other}"),
        }

        // The other direction: every other volume is full, and the pin
        // writes to none of them.
        let (mut enginefs, _counters) = test_enginefs_with_file_count(2);
        let pieces = enginefs.piece_store().path().to_path_buf();
        enginefs.set_free_space_probe(move |path| {
            Ok(if path.starts_with(&pieces) {
                u64::MAX
            } else {
                0
            })
        });
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert_eq!(enginefs.pinned_downloads().await.len(), 1);
    }

    /// Re-pinning a file of a torrent that is still checking (a restart, a
    /// stream the pin joined) is not refused for space: its `downloaded`
    /// reads 0 until the check ends, and the download is already
    /// librqbit's to continue. A torrent added by the pin itself is
    /// measured even while initializing: nothing of it is on disk, and the
    /// 0 its files report is the truth about it.
    #[tokio::test]
    async fn re_pin_of_a_checking_torrent_skips_the_space_check() {
        let (mut enginefs, _counters, _init) =
            test_enginefs_initializing(2, Duration::from_secs(60));
        enginefs.set_free_space_probe(|_| Ok(0));
        assert_eq!(
            enginefs
                .get_engine(TEST_HASH)
                .await
                .unwrap()
                .get_statistics()
                .await
                .phase,
            StartupPhase::Checking
        );
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();

        // Freshly added for the pin: measured.
        let counters = Arc::new(FakeCounters::default());
        let handle = FakeHandle {
            info_hash: TEST_HASH.to_string(),
            counters,
            files: vec![BackendFileInfo {
                name: "video.mkv".into(),
                length: 100,
            }],
            init: FakeInit::new(false, Duration::from_secs(60)),
        };
        let root = fake_engine_root();
        let mut enginefs = BackendEngineFS::new_with_backend(
            FakeBackend::new(vec![handle]),
            HashMap::new(),
            root.join("cache"),
            root.join("downloads"),
        );
        enginefs.set_free_space_probe(|_| Ok(0));
        assert!(matches!(
            enginefs.pin_download(TEST_HASH, 0, None).await,
            Err(PinDownloadError::InsufficientSpace { .. })
        ));
    }

    /// Engine over an unmanaged fake torrent that is still checking (as a
    /// real torrent is right after `add_torrent` returns), for pins that
    /// add it.
    fn test_enginefs_unmanaged_checking() -> (BackendEngineFS<FakeBackend>, Arc<FakeCounters>) {
        let counters = Arc::new(FakeCounters::default());
        let handle = FakeHandle {
            info_hash: TEST_HASH.to_string(),
            counters: counters.clone(),
            files: (0..2)
                .map(|idx| BackendFileInfo {
                    name: format!("video-{idx}.mkv"),
                    length: 100,
                })
                .collect(),
            init: FakeInit::new(false, Duration::from_secs(60)),
        };
        let root = fake_engine_root();
        let enginefs = BackendEngineFS::new_with_backend(
            FakeBackend::new(vec![handle]),
            HashMap::new(),
            root.join("cache"),
            root.join("downloads"),
        );
        (enginefs, counters)
    }

    /// A refused pin drops the torrent it added and leaves every byte on
    /// disk: the refusal comes before anything is downloaded, so the add
    /// wrote nothing of its own, and what the store already holds for the
    /// hash was fetched by an earlier stream or an earlier session whose
    /// backend records are gone. Those bytes are cache for the cleaner, not
    /// a failed pin's to delete.
    ///
    /// The pin used to take the files whenever `<downloadsDir>/<info hash>`
    /// had not existed before its add -- a question about a whole-file copy
    /// nothing reads, asked in place of the only one that could matter.
    ///
    /// Data on disk is no reason to *skip* the free-space check either: the
    /// file's missing bytes are what the pin will write, so it is measured
    /// and refused on a disk with no room.
    #[tokio::test]
    async fn a_refused_pin_drops_its_torrent_and_keeps_the_bytes() {
        let (mut enginefs, _counters) = test_enginefs_unmanaged_checking();
        let pieces = enginefs.piece_store().torrent_dir(TEST_HASH);
        std::fs::create_dir_all(pieces.join("0")).unwrap();
        let piece = pieces.join("0").join("1");
        std::fs::write(&piece, [7u8; 100]).unwrap();
        enginefs.set_free_space_probe(|_| Ok(0));
        assert!(matches!(
            enginefs.pin_download(TEST_HASH, 0, None).await,
            Err(PinDownloadError::InsufficientSpace { .. })
        ));
        assert!(enginefs.get_engine(TEST_HASH).await.is_none());
        assert_eq!(
            enginefs.backend.removed.lock().unwrap().as_slice(),
            &[TEST_HASH.to_string()],
            "dropped keeping its files"
        );
        assert!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty()
        );
        assert!(piece.is_file(), "pieces this pin did not fetch survive it");

        // Refused for another reason (no such file): same answer, and the
        // same one with nothing on disk for the hash at all.
        let (mut enginefs, _counters) = test_enginefs_unmanaged_checking();
        enginefs.set_free_space_probe(|_| Ok(0));
        assert!(matches!(
            enginefs.pin_download(TEST_HASH, 5, None).await,
            Err(PinDownloadError::FileNotFound { .. })
        ));
        assert_eq!(
            enginefs.backend.removed.lock().unwrap().as_slice(),
            &[TEST_HASH.to_string()],
            "dropped keeping its files"
        );
        assert!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty()
        );
    }

    /// A pin that joins a magnet add another request started (a stream's
    /// stats poll resolving metadata) and is then refused leaves that
    /// engine alone: the torrent is theirs, and dropping it would fail the
    /// stream about to open on it.
    #[tokio::test]
    async fn refused_pin_leaves_a_joined_stream_add_alone() {
        let (mut enginefs, _counters) = test_enginefs_unmanaged();
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.backend.hold_add.store(true, Ordering::SeqCst);
        assert!(matches!(
            enginefs.get_or_begin_add_magnet(TEST_HASH, None).await,
            EngineLookup::Adding(_)
        ));

        let release = async {
            // Wait for the state, not for a stopwatch: the stream's add has
            // reached the (held) backend and the pin has taken the per-hash
            // lock, so whether it started an add of its own is decided.
            assert!(
                wait_until(TEST_WAIT_BOUND, || {
                    !enginefs.backend.placements.lock().unwrap().is_empty()
                        && enginefs.pin_locks.lock().contains_key(TEST_HASH)
                })
                .await,
                "the stream's add and the pin both got going"
            );
            assert_eq!(
                enginefs.backend.placements.lock().unwrap().as_slice(),
                &[TorrentPlacement::default()],
                "the pin joined the stream's add instead of starting its own"
            );
            enginefs.backend.add_hold.add_permits(1);
        };
        let (result, ()) = tokio::join!(enginefs.pin_download(TEST_HASH, 0, None), release);
        assert!(matches!(
            result,
            Err(PinDownloadError::InsufficientSpace { .. })
        ));
        let engine = enginefs
            .get_engine(TEST_HASH)
            .await
            .expect("the stream's engine stays");
        assert!(!engine.is_pinned());
        assert!(enginefs.backend.removed.lock().unwrap().is_empty());
        assert!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty()
        );
        assert_eq!(enginefs.backend.placements.lock().unwrap().len(), 1);
    }

    /// `joiners` counts who joined the *pending add*, and by the time the
    /// preconditions are checked the engine has been published: a stream
    /// that opens on the freshly resolved torrent finds the engine, joins
    /// nothing, and is invisible to that count. The window is the whole
    /// precondition check -- `file_count()`, `stats()` and the free-space
    /// probe -- so the refusal asks the live activity registers too, and
    /// leaves the torrent to the reader that is on it.
    #[tokio::test]
    async fn a_refused_pin_leaves_the_torrent_a_reader_took_while_it_checked() {
        let (mut enginefs, _counters) = test_enginefs_unmanaged();
        let engines = enginefs.engines.clone();
        enginefs.set_free_space_probe(move |_| {
            // A stream-shaped read of the published engine, inside the
            // window: it registers on the engine and holds it.
            let engine = engines
                .try_read()
                .expect("the registry is idle here")
                .get(TEST_HASH)
                .cloned()
                .expect("the add published its engine before this check");
            engine.active_streams.fetch_add(1, Ordering::SeqCst);
            Ok(0)
        });

        assert!(matches!(
            enginefs.pin_download(TEST_HASH, 0, None).await,
            Err(PinDownloadError::InsufficientSpace { .. })
        ));
        let engine = enginefs
            .get_engine(TEST_HASH)
            .await
            .expect("the reader's torrent stays registered");
        assert!(!engine.is_pinned());
        assert!(
            enginefs.backend.removed.lock().unwrap().is_empty()
                && enginefs
                    .backend
                    .removed_with_files
                    .lock()
                    .unwrap()
                    .is_empty(),
            "and stays in the session, instead of being dropped under the read"
        );
    }

    /// The other order: the pin's add is the one in flight and a stream
    /// request joins *it*. A teardown that took the torrent whenever this
    /// call had started the add would remove it -- with its files -- from
    /// under the stream the moment the pin was refused. Whoever joined the
    /// add holds the engine, so the refusal leaves the torrent for the idle
    /// sweeper.
    #[tokio::test]
    async fn refused_pin_leaves_the_torrent_a_stream_joined_its_add_for() {
        let (mut enginefs, _counters) = test_enginefs_unmanaged();
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.backend.hold_add.store(true, Ordering::SeqCst);

        let stream = async {
            assert!(
                wait_until(TEST_WAIT_BOUND, || {
                    !enginefs.backend.placements.lock().unwrap().is_empty()
                })
                .await,
                "the pin's add reached the backend"
            );
            // A stream-shaped lookup: it finds the pin's add and joins it.
            let joined = match enginefs.get_or_begin_add_magnet(TEST_HASH, None).await {
                EngineLookup::Adding(pending) => pending,
                _ => panic!("the stream should have joined the in-flight add"),
            };
            assert_eq!(
                enginefs.backend.placements.lock().unwrap().len(),
                1,
                "one add, the pin's"
            );
            enginefs.backend.add_hold.add_permits(1);
            joined.done.await.expect("the add itself succeeds")
        };
        let (result, streamed) = tokio::join!(enginefs.pin_download(TEST_HASH, 0, None), stream);
        assert!(matches!(
            result,
            Err(PinDownloadError::InsufficientSpace { .. })
        ));
        let engine = enginefs
            .get_engine(TEST_HASH)
            .await
            .expect("and it is still registered for the stream to read from");
        assert!(Arc::ptr_eq(&engine, &streamed));
        assert!(!engine.is_pinned());
        assert!(
            enginefs.backend.removed.lock().unwrap().is_empty()
                && enginefs
                    .backend
                    .removed_with_files
                    .lock()
                    .unwrap()
                    .is_empty(),
            "nothing was torn down from under the stream"
        );

        // Nobody joined: the same refusal drops the torrent (keeping its
        // bytes, which are not this pin's).
        enginefs.remove_engine(TEST_HASH).await;
        enginefs.backend.hold_add.store(false, Ordering::SeqCst);
        assert!(matches!(
            enginefs.pin_download(TEST_HASH, 0, None).await,
            Err(PinDownloadError::InsufficientSpace { .. })
        ));
        assert!(enginefs.get_engine(TEST_HASH).await.is_none());
        assert_eq!(
            enginefs.backend.removed.lock().unwrap().as_slice(),
            &[TEST_HASH.to_string()]
        );
        assert!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty()
        );
    }

    /// `LibrqbitBackend` behind a shim that answers a magnet add for a known
    /// hash with the torrent's own bytes: the hermetic session has no peers
    /// to resolve metadata from, so this is how `pin_download`'s fresh-add
    /// path is driven against the real backend.
    struct BytesForMagnet {
        inner: LibrqbitBackend,
        torrents: HashMap<String, Vec<u8>>,
    }

    impl BytesForMagnet {
        fn resolve(&self, source: TorrentSource) -> TorrentSource {
            if let TorrentSource::Url(url) = &source
                && let Some(hash) = url.strip_prefix("magnet:?xt=urn:btih:")
                && let Some(bytes) = self.torrents.get(hash)
            {
                return TorrentSource::Bytes(bytes.clone());
            }
            source
        }
    }

    #[async_trait::async_trait]
    impl TorrentBackend for BytesForMagnet {
        type Handle = <LibrqbitBackend as TorrentBackend>::Handle;

        async fn add_torrent(
            &self,
            source: TorrentSource,
            trackers: Vec<String>,
        ) -> Result<Self::Handle> {
            self.inner.add_torrent(self.resolve(source), trackers).await
        }

        async fn add_torrent_placed(
            &self,
            source: TorrentSource,
            trackers: Vec<String>,
            placement: TorrentPlacement,
        ) -> Result<Self::Handle> {
            self.inner
                .add_torrent_placed(self.resolve(source), trackers, placement)
                .await
        }

        async fn get_torrent(&self, info_hash: &str) -> Option<Self::Handle> {
            self.inner.get_torrent(info_hash).await
        }

        async fn remove_torrent(&self, info_hash: &str) -> Result<()> {
            self.inner.remove_torrent(info_hash).await
        }

        async fn remove_torrent_and_files(&self, info_hash: &str) -> Result<()> {
            self.inner.remove_torrent_and_files(info_hash).await
        }

        async fn list_torrents(&self) -> Vec<String> {
            self.inner.list_torrents().await
        }

        async fn memory_diagnostics(&self) -> BackendMemoryDiagnostics {
            self.inner.memory_diagnostics().await
        }
    }

    /// A .torrent for `path` (small pieces): its bytes and hex info hash.
    async fn real_torrent(path: &std::path::Path) -> (Vec<u8>, String) {
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

    /// The cache-purge case against the real backend: the session's records
    /// are gone but the data of `e1.bin` is still where the backend writes
    /// it. A fresh pin of that file is accepted although the volume reports
    /// no free space (librqbit verifies the data in place; while it does,
    /// the file is not counted as missing), and a pin of the torrent's other
    /// file -- refused once the check shows it missing, or accepted
    /// unmeasured while the check runs -- never takes that data with it: a
    /// refused pin drops its torrent and nothing else.
    #[tokio::test]
    async fn fresh_pin_over_pre_seeded_data_survives_with_the_real_backend() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("show");
        std::fs::create_dir_all(&src).unwrap();
        let payload: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
        std::fs::write(src.join("e1.bin"), &payload).unwrap();
        std::fs::write(src.join("e2.bin"), vec![3u8; 16 * 1024]).unwrap();
        let (bytes, hash) = real_torrent(&src).await;
        // A torrent's file order is the filesystem's readdir order, not the
        // order the fixture wrote the files in: look the indices up. (Both
        // files are whole pieces, so neither order shares a boundary piece.)
        let e1 = crate::backend::librqbit::torrent_file_index(&bytes, "e1.bin");
        let e2 = crate::backend::librqbit::torrent_file_index(&bytes, "e2.bin");
        // Where librqbit itself puts a multi-file torrent: this layer
        // names no folder, so the fixture seeds the one the backend uses.
        let folder = tmp.path().join("dl").join("show");
        std::fs::create_dir_all(&folder).unwrap();
        std::fs::write(folder.join("e1.bin"), &payload).unwrap();

        let make = || async {
            let inner = LibrqbitBackend::new_for_tests(tmp.path().join("dl"))
                .await
                .expect("hermetic session");
            let mut enginefs = BackendEngineFS::new_with_backend(
                BytesForMagnet {
                    inner,
                    torrents: HashMap::from([(hash.clone(), bytes.clone())]),
                },
                HashMap::new(),
                tmp.path().join("cache"),
                tmp.path().join("dl"),
            );
            enginefs.set_free_space_probe(|_| Ok(0));
            enginefs
        };

        let enginefs = make().await;
        let engine = enginefs
            .pin_download(&hash, e1, None)
            .await
            .expect("a complete file in place needs no space");
        engine.handle.handle.wait_until_initialized().await.unwrap();
        let stats = engine.get_statistics().await;
        assert!(stats.files[e1].complete, "verified in place: {stats:?}");
        assert_eq!(engine.pinned_file_indices(), vec![e1]);
        enginefs.backend.remove_torrent(&hash).await.unwrap();
        drop(enginefs);

        let enginefs = make().await;
        match enginefs.pin_download(&hash, e2, None).await {
            Ok(engine) => {
                // Still checking when measured: accepted unmeasured, and
                // the data in place is what the check finds.
                engine.handle.handle.wait_until_initialized().await.unwrap();
                assert!(engine.get_statistics().await.files[e1].complete);
            }
            Err(PinDownloadError::InsufficientSpace { .. }) => {
                assert!(enginefs.get_engine(&hash).await.is_none());
                assert!(enginefs.backend.list_torrents().await.is_empty());
            }
            Err(other) => panic!("unexpected error: {other}"),
        }
        assert_eq!(
            std::fs::read(folder.join("e1.bin")).unwrap(),
            payload,
            "pre-existing data survives a refused pin"
        );
    }

    /// Deleting one pinned file of two, against the real backend. The bytes
    /// going is half of it; librqbit forgetting it had them is the other
    /// half, and the one that was missing: the deleted file kept reporting
    /// complete, and a re-pin found nothing to download.
    #[tokio::test(flavor = "multi_thread")]
    async fn per_file_delete_forgets_the_pieces_with_the_real_backend() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("show");
        std::fs::create_dir_all(&src).unwrap();
        // Whole pieces each, so neither file's index depends on which
        // shares a boundary piece with which.
        let payload = |seed: u8| -> Vec<u8> { (0..64 * 1024).map(|i| (i as u8) ^ seed).collect() };
        std::fs::write(src.join("e1.bin"), payload(1)).unwrap();
        std::fs::write(src.join("e2.bin"), payload(2)).unwrap();
        let (bytes, hash) = real_torrent(&src).await;
        let e1 = crate::backend::librqbit::torrent_file_index(&bytes, "e1.bin");
        let e2 = crate::backend::librqbit::torrent_file_index(&bytes, "e2.bin");
        // librqbit's own folder for the torrent: nothing here chooses one.
        let folder = tmp.path().join("dl").join("show");
        std::fs::create_dir_all(&folder).unwrap();
        std::fs::write(folder.join("e1.bin"), payload(1)).unwrap();
        std::fs::write(folder.join("e2.bin"), payload(2)).unwrap();

        // The per-file delete forgets the deleted file's pieces from
        // librqbit's have-set, which needs `piece_reclaim` -- so the session
        // must run on a storage that can release a piece. (The shipped
        // filesystem session cannot, and there the delete degrades to
        // bytes-gone-but-have-set-stale-until-restart -- covered by
        // `dropping_pieces_without_reclaim_is_refused_by_name`.)
        let inner = LibrqbitBackend::new_for_tests_reclaiming(tmp.path().join("dl"))
            .await
            .expect("hermetic session");
        let mut enginefs = BackendEngineFS::new_with_backend(
            BytesForMagnet {
                inner,
                torrents: HashMap::from([(hash.clone(), bytes.clone())]),
            },
            HashMap::new(),
            tmp.path().join("cache"),
            tmp.path().join("dl"),
        );
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));

        let engine = enginefs.pin_download(&hash, e1, None).await.unwrap();
        enginefs.pin_download(&hash, e2, None).await.unwrap();
        engine.handle.handle.wait_until_initialized().await.unwrap();
        let stats = engine.get_statistics().await;
        assert!(
            stats.files[e1].complete && stats.files[e2].complete,
            "seeded in place: {stats:?}"
        );

        let outcome = enginefs.unpin_download(&hash, e1, true).await.unwrap();
        assert!(outcome.unpinned && outcome.deleted_files);
        assert!(!folder.join("e1.bin").exists(), "the bytes are gone");
        assert!(folder.join("e2.bin").is_file(), "the other pin's are not");
        let engine = enginefs
            .get_engine(&hash)
            .await
            .expect("the torrent keeps running");
        let stats = engine.get_statistics().await;
        assert!(
            !stats.files[e1].complete,
            "librqbit no longer claims the deleted file: {:?}",
            stats.files
        );
        assert_eq!(stats.files[e1].downloaded, 0, "{:?}", stats.files);
        assert!(stats.files[e2].complete, "{:?}", stats.files);
        assert_eq!(
            engine.handle.handle.stats().file_progress[e1],
            0,
            "and it says so in its own terms"
        );

        // A re-pin has something to download again, instead of reporting a
        // finished file that reads as an error.
        enginefs.pin_download(&hash, e1, None).await.unwrap();
        let stats = engine.handle.handle.stats();
        assert!(
            !stats.finished,
            "the re-pinned file is wanted and missing: {stats}"
        );
        assert_eq!(stats.progress_bytes, 64 * 1024, "{stats}");
    }

    // --- pin persistence across restarts ---

    /// What the startup sweep must and must not take. The claims come from
    /// two places, and both have to count: the torrents the backend restored,
    /// and the pins it did not -- a dormant pin has no engine, so a sweep that
    /// asked only the engine registry would delete the offline download the
    /// user is waiting to come back.
    #[tokio::test]
    async fn the_startup_sweep_keeps_what_the_session_and_the_pins_claim() {
        const ORPHAN_HASH: &str = "1111111111111111111111111111111111111111";
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        let handle = FakeHandle {
            info_hash: TEST_HASH.to_string(),
            counters: Arc::new(FakeCounters::default()),
            files: vec![BackendFileInfo {
                name: "video-0.mkv".to_string(),
                length: 100,
            }],
            init: FakeInit::new(true, Duration::from_secs(60)),
        };
        let enginefs = BackendEngineFS::new_with_backend(
            FakeBackend::new(vec![handle.clone()]),
            HashMap::from([(TEST_HASH.to_string(), handle)]),
            root.join("cache"),
            root.join("downloads"),
        );
        // A pin of a torrent the backend did not restore: dormant, and its
        // data has to survive anyway.
        std::fs::create_dir_all(root.join("downloads")).unwrap();
        std::fs::write(
            enginefs.pinned_downloads_path(),
            serde_json::to_vec(&serde_json::json!({ OTHER_HASH: [0] })).unwrap(),
        )
        .unwrap();
        enginefs.restore_pinned_downloads().await;
        assert_eq!(enginefs.dormant_pinned_downloads().len(), 1);

        let pieces = enginefs.piece_store().path().to_path_buf();
        for hash in [TEST_HASH, OTHER_HASH, ORPHAN_HASH] {
            let dir = pieces.join(hash).join("0");
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("0"), [1u8; 1024]).unwrap();
        }

        let report = enginefs.sweep_unadopted_pieces().await;
        assert_eq!(report.removed, 1, "{report:?}");
        assert!(pieces.join(TEST_HASH).is_dir(), "the restored torrent's");
        assert!(pieces.join(OTHER_HASH).is_dir(), "the dormant pin's");
        assert!(!pieces.join(ORPHAN_HASH).exists(), "nothing claims this");

        assert_eq!(
            enginefs.sweep_unadopted_pieces().await,
            crate::piece_store::SweepReport::default(),
            "and running it again on the next launch does nothing"
        );
        assert!(pieces.join(TEST_HASH).is_dir());
    }

    /// A torrent librqbit persisted but did not restore on this boot is still
    /// the session's, and its pieces are not the sweep's to take.
    ///
    /// A restore is allowed to fail -- the volume it writes to is not mounted
    /// yet, its `.torrent` will not parse, the add errored -- and none of that
    /// says anything about the data. `session.json` and the `<hash>.bitv`
    /// fastresume bitfield still name the torrent, so a claim set built from
    /// the engines that happened to come up would delete a whole download out
    /// from under the record that still refers to it. The claim has to be
    /// "what the session still has a record of", not "what came up this time".
    #[tokio::test]
    async fn the_startup_sweep_keeps_the_pieces_of_a_torrent_the_session_still_records() {
        const ORPHAN_HASH: &str = "1111111111111111111111111111111111111111";
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        let download_dir = root.join("rqbit-downloads");
        // Nothing came up: no restored handle, no pin. Only librqbit's own
        // persistence records, which is the whole point.
        let enginefs = BackendEngineFS::new_with_backend(
            FakeBackend::new(Vec::new()),
            HashMap::new(),
            root.join("cache"),
            download_dir.clone(),
        );
        std::fs::create_dir_all(&download_dir).unwrap();
        std::fs::write(
            download_dir.join("session.json"),
            serde_json::to_vec(&serde_json::json!({
                "torrents": {
                    "0": {
                        "info_hash": TEST_HASH,
                        "trackers": [],
                        "output_folder": download_dir.join(TEST_HASH),
                        "only_files": null,
                        "is_paused": false,
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        // A torrent whose entry `session.json` has lost but whose fastresume
        // bitfield is still there: the have-record of a real download, and
        // reason enough not to delete what it describes.
        std::fs::write(download_dir.join(format!("{OTHER_HASH}.bitv")), [0u8; 8]).unwrap();

        let pieces = enginefs.piece_store().path().to_path_buf();
        for hash in [TEST_HASH, OTHER_HASH, ORPHAN_HASH] {
            let dir = pieces.join(hash).join("0");
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("0"), [1u8; 1024]).unwrap();
        }

        let report = enginefs.sweep_unadopted_pieces().await;
        assert_eq!(report.removed, 1, "{report:?}");
        assert!(
            pieces.join(TEST_HASH).is_dir(),
            "`session.json` still records this torrent"
        );
        assert!(
            pieces.join(OTHER_HASH).is_dir(),
            "and the fastresume bitfield records this one"
        );
        assert!(
            !pieces.join(ORPHAN_HASH).exists(),
            "no record of any kind claims this"
        );
    }

    /// Pins are written to `pinned-downloads.json` on every change and
    /// re-applied at startup to the torrents the backend restored; pins of
    /// files that do not exist are dropped and the file rewritten; an
    /// unreadable file is ignored.
    #[tokio::test]
    async fn pinned_downloads_are_persisted_and_restored() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        let make = |files: usize| {
            let counters = Arc::new(FakeCounters::default());
            let handle = FakeHandle {
                info_hash: TEST_HASH.to_string(),
                counters: counters.clone(),
                files: (0..files)
                    .map(|idx| BackendFileInfo {
                        name: format!("video-{idx}.mkv"),
                        length: 100,
                    })
                    .collect(),
                init: FakeInit::new(true, Duration::from_secs(60)),
            };
            let restored = HashMap::from([(TEST_HASH.to_string(), handle.clone())]);
            let enginefs = BackendEngineFS::new_with_backend(
                FakeBackend::new(vec![handle]),
                restored,
                root.join("cache"),
                root.join("downloads"),
            );
            (enginefs, counters)
        };
        let read_pins = |path: &std::path::Path| -> serde_json::Value {
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
        };

        let (first, _counters) = make(3);
        let path = first.pinned_downloads_path();
        assert_eq!(path, root.join("downloads").join("pinned-downloads.json"));
        assert_eq!(first.restore_pinned_downloads().await, 0, "nothing yet");
        first.pin_download(TEST_HASH, 2, None).await.unwrap();
        first.pin_download(TEST_HASH, 1, None).await.unwrap();
        assert_eq!(read_pins(&path), serde_json::json!({ TEST_HASH: [1, 2] }));
        assert!(
            first
                .unpin_download(TEST_HASH, 2, false)
                .await
                .unwrap()
                .unpinned
        );
        assert_eq!(read_pins(&path), serde_json::json!({ TEST_HASH: [1] }));
        drop(first);

        // "Restart": a new engine over the backend's restored torrent.
        let (second, counters) = make(3);
        assert!(second.pinned_downloads().await.is_empty());
        assert_eq!(second.restore_pinned_downloads().await, 1);
        let engine = second.get_engine(TEST_HASH).await.unwrap();
        assert_eq!(engine.pinned_file_indices(), vec![1]);
        assert!(engine.is_pinned(), "exempt from eviction again");
        assert_eq!(counters.pin_file.load(Ordering::SeqCst), 1);
        assert_eq!(engine.get_statistics().await.pinned_files, vec![1]);
        assert_eq!(read_pins(&path), serde_json::json!({ TEST_HASH: [1] }));
        drop(second);

        // An index the torrent does not have is dropped; a torrent the
        // backend does not have keeps its pin (dormant, see below); the
        // rest is restored.
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                OTHER_HASH: [0],
                TEST_HASH.to_uppercase(): [0, 7],
            }))
            .unwrap(),
        )
        .unwrap();
        let (third, _counters) = make(3);
        assert_eq!(third.restore_pinned_downloads().await, 1);
        assert_eq!(
            third.pinned_downloads().await,
            vec![PinnedDownload {
                info_hash: TEST_HASH.to_string(),
                file_idx: 0
            }]
        );
        assert_eq!(
            read_pins(&path),
            serde_json::json!({ OTHER_HASH: [0], TEST_HASH: [0] })
        );
        drop(third);

        std::fs::write(&path, b"not json").unwrap();
        let (fourth, _counters) = make(3);
        assert_eq!(fourth.restore_pinned_downloads().await, 0);
        assert!(fourth.pinned_downloads().await.is_empty());
    }

    /// A pin whose torrent the backend did not bring back at startup (its
    /// output folder on a volume that was not mounted -- librqbit skips the
    /// torrent but keeps its record) is not lost: it stays in the file
    /// through that run and any pins made meanwhile, is applied on a later
    /// boot that has the torrent, comes along with a pin of the torrent
    /// made before then, and is dropped by an unpin.
    #[tokio::test]
    async fn pins_of_torrents_the_backend_did_not_restore_are_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        let make = |restored: bool, backend_hash: &str| {
            let counters = Arc::new(FakeCounters::default());
            let handle = FakeHandle {
                info_hash: backend_hash.to_string(),
                counters: counters.clone(),
                files: (0..3)
                    .map(|idx| BackendFileInfo {
                        name: format!("video-{idx}.mkv"),
                        length: 100,
                    })
                    .collect(),
                init: FakeInit::new(true, Duration::from_secs(60)),
            };
            let restored = if restored {
                HashMap::from([(backend_hash.to_string(), handle.clone())])
            } else {
                HashMap::new()
            };
            let enginefs = BackendEngineFS::new_with_backend(
                FakeBackend::new(vec![handle]),
                restored,
                root.join("cache"),
                root.join("downloads"),
            );
            (enginefs, counters)
        };
        let read_pins = |path: &std::path::Path| -> serde_json::Value {
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
        };

        let (first, _counters) = make(true, TEST_HASH);
        let path = first.pinned_downloads_path();
        first.pin_download(TEST_HASH, 1, None).await.unwrap();
        drop(first);

        // Boot without the torrent: nothing applied, nothing lost -- not
        // even by another pin, which rewrites the file.
        let (second, _counters) = make(false, OTHER_HASH);
        assert_eq!(second.restore_pinned_downloads().await, 0);
        assert!(second.pinned_downloads().await.is_empty());
        // Not live, but listable: a caller enumerating downloads reports
        // them as stalled instead of dropping them from its list.
        assert_eq!(
            second.dormant_pinned_downloads(),
            vec![PinnedDownload {
                info_hash: TEST_HASH.to_string(),
                file_idx: 1
            }]
        );
        assert_eq!(read_pins(&path), serde_json::json!({ TEST_HASH: [1] }));
        second.pin_download(OTHER_HASH, 0, None).await.unwrap();
        assert_eq!(
            read_pins(&path),
            serde_json::json!({ OTHER_HASH: [0], TEST_HASH: [1] })
        );
        // An unpin reaches a dormant pin too.
        assert!(
            second
                .unpin_download(TEST_HASH, 1, false)
                .await
                .unwrap()
                .unpinned
        );
        assert!(
            !second
                .unpin_download(TEST_HASH, 1, false)
                .await
                .unwrap()
                .unpinned
        );
        assert!(second.dormant_pinned_downloads().is_empty());
        assert_eq!(read_pins(&path), serde_json::json!({ OTHER_HASH: [0] }));
        drop(second);

        // Next boot with the torrent back: applied.
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({ TEST_HASH: [1] })).unwrap(),
        )
        .unwrap();
        let (third, _counters) = make(true, TEST_HASH);
        assert_eq!(third.restore_pinned_downloads().await, 1);
        assert_eq!(
            third
                .get_engine(TEST_HASH)
                .await
                .unwrap()
                .pinned_file_indices(),
            vec![1]
        );
        assert_eq!(read_pins(&path), serde_json::json!({ TEST_HASH: [1] }));
        drop(third);

        // Absent at boot, pinned again before the next one (the client's
        // re-pin): the dormant pins come with the new one.
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({ TEST_HASH: [1, 2] })).unwrap(),
        )
        .unwrap();
        let (fourth, counters) = make(false, TEST_HASH);
        assert_eq!(fourth.restore_pinned_downloads().await, 0);
        let engine = fourth.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert_eq!(engine.pinned_file_indices(), vec![0, 1, 2]);
        assert_eq!(engine.get_statistics().await.pinned_files, vec![0, 1, 2]);
        assert_eq!(counters.pin_file.load(Ordering::SeqCst), 3);
        assert_eq!(
            read_pins(&path),
            serde_json::json!({ TEST_HASH: [0, 1, 2] })
        );
        assert!(fourth.dormant_pins.lock().is_empty());
    }

    /// `client_message` is what a route may echo: the numeric errors as
    /// they are, never a backend chain, which names server paths.
    #[test]
    fn pin_download_error_client_message_leaks_no_paths() {
        let backend = PinDownloadError::Backend(
            anyhow::anyhow!("error opening \"/home/user/offline/abc/Movie.mkv\"")
                .context("relocating abc into /home/user/offline/abc"),
        );
        assert!(
            format!("{backend:#}").contains("/home/user"),
            "the log has it"
        );
        let message = backend.client_message();
        assert!(!message.is_empty());
        assert!(!message.contains("/home/user"), "{message}");

        let magnet = PinDownloadError::MagnetAdd(MagnetAddError::Backend {
            info_hash: "abc".into(),
            error: Arc::new(anyhow::anyhow!("cannot open /home/user/downloads/x")),
        });
        assert!(!magnet.client_message().contains("/home/user"));
        let timeout = MagnetAddError::MetadataTimeout {
            info_hash: "abc".into(),
            timeout: METADATA_RESOLVE_TIMEOUT,
        };
        assert_eq!(
            PinDownloadError::MagnetAdd(timeout.clone()).client_message(),
            timeout.client_message()
        );

        let space = PinDownloadError::InsufficientSpace {
            required: 10,
            available: 3,
            margin: 2,
        };
        assert_eq!(space.client_message(), space.to_string());
        let missing = PinDownloadError::FileNotFound {
            file_idx: 9,
            file_count: 2,
        };
        assert_eq!(missing.client_message(), missing.to_string());
    }

    // --- pinned offline downloads ---

    /// `pin_download` records the pin on the engine and the handle, applies
    /// it through a reconcile that keeps the current playback selection,
    /// and surfaces it in stats and `pinned_downloads`; `unpin_download`
    /// undoes exactly that.
    #[tokio::test]
    async fn pin_download_pins_file_and_reconciles_around_playback() {
        let (enginefs, counters) = test_enginefs_with_file_count(3);

        let engine = enginefs.pin_download(TEST_HASH, 1, None).await.unwrap();
        assert_eq!(engine.pinned_file_indices(), vec![1]);
        assert!(engine.is_pinned());
        assert_eq!(counters.pin_file.load(Ordering::SeqCst), 1);
        // Nothing playing: reconciled with no active file.
        assert_eq!(counters.reconcile_file_priorities.load(Ordering::SeqCst), 1);
        assert_eq!(*counters.last_active_file.lock().unwrap(), None);

        let stats = engine.get_statistics().await;
        assert_eq!(stats.pinned_files, vec![1]);
        assert!(stats.files[1].pinned);
        assert!(!stats.files[0].pinned && !stats.files[2].pinned);
        let json = serde_json::to_value(&stats).unwrap();
        assert_eq!(json["pinnedFiles"], serde_json::json!([1]));
        assert_eq!(json["files"][1]["pinned"], true);
        assert_eq!(
            enginefs.pinned_downloads().await,
            vec![PinnedDownload {
                info_hash: TEST_HASH.to_string(),
                file_idx: 1
            }]
        );

        // Idempotent, and a second pin lists in order.
        enginefs.pin_download(TEST_HASH, 1, None).await.unwrap();
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert_eq!(engine.pinned_file_indices(), vec![0, 1]);
        assert_eq!(enginefs.pinned_downloads().await.len(), 2);

        // With file 2 playing, pinning reconciles around that selection.
        enginefs.on_stream_start(TEST_HASH, 2).await;
        assert_eq!(*counters.last_active_file.lock().unwrap(), Some(2));
        let before = counters.reconcile_file_priorities.load(Ordering::SeqCst);
        enginefs.pin_download(TEST_HASH, 1, None).await.unwrap();
        assert_eq!(
            counters.reconcile_file_priorities.load(Ordering::SeqCst),
            before + 1
        );
        assert_eq!(*counters.last_active_file.lock().unwrap(), Some(2));

        // Unpin forgets the pin and reconciles again around the selection.
        assert!(
            enginefs
                .unpin_download(TEST_HASH, 1, false)
                .await
                .unwrap()
                .unpinned
        );
        assert_eq!(counters.unpin_file.load(Ordering::SeqCst), 1);
        assert_eq!(engine.pinned_file_indices(), vec![0]);
        assert_eq!(
            counters.reconcile_file_priorities.load(Ordering::SeqCst),
            before + 2
        );
        assert_eq!(*counters.last_active_file.lock().unwrap(), Some(2));
        assert!(!engine.get_statistics().await.files[1].pinned);
        // Not pinned (any more) / unknown torrent: false, no reconcile.
        assert!(
            !enginefs
                .unpin_download(TEST_HASH, 1, false)
                .await
                .unwrap()
                .unpinned
        );
        assert!(
            !enginefs
                .unpin_download(&"f".repeat(40), 0, false)
                .await
                .unwrap()
                .unpinned
        );
        assert_eq!(
            counters.reconcile_file_priorities.load(Ordering::SeqCst),
            before + 2
        );
        assert!(
            enginefs
                .unpin_download(TEST_HASH, 0, false)
                .await
                .unwrap()
                .unpinned
        );
        assert!(!engine.is_pinned());
        assert!(enginefs.pinned_downloads().await.is_empty());
    }

    /// `unpin_download(.., delete_files = true)` takes the data with the
    /// pin: only the one file while the torrent has other pins (it keeps
    /// running for them), the whole torrent -- registry entry, backend
    /// record, files and folder -- once the last pin goes. Asking for a
    /// file that is not pinned at all still deletes, and still reports that
    /// no pin was cleared.
    #[tokio::test]
    async fn unpin_download_deletes_the_data_when_asked() {
        let tmp = tempfile::tempdir().unwrap();
        let folder = tmp.path().join("Show");
        std::fs::create_dir_all(&folder).unwrap();
        let file = |idx: usize| folder.join(format!("video-{idx}.mkv"));
        let (enginefs, counters) = test_enginefs_with_file_count(3);
        for idx in 0..3 {
            std::fs::write(file(idx), b"payload").unwrap();
        }
        *counters.output_folder.lock().unwrap() = Some(folder.clone());

        enginefs.pin_download(TEST_HASH, 1, None).await.unwrap();
        enginefs.pin_download(TEST_HASH, 2, None).await.unwrap();

        // One pin of two: that file's bytes go, the torrent stays.
        assert!(
            enginefs
                .unpin_download(TEST_HASH, 1, true)
                .await
                .unwrap()
                .unpinned
        );
        assert!(!file(1).exists(), "the unpinned file's data is deleted");
        assert!(file(2).is_file(), "the other pin's data stays");
        assert!(enginefs.get_engine(TEST_HASH).await.is_some());
        assert!(
            enginefs
                .get_backend()
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty(),
            "a torrent with pins left is never dropped"
        );
        assert_eq!(
            enginefs.pinned_downloads().await,
            vec![PinnedDownload {
                info_hash: TEST_HASH.to_string(),
                file_idx: 2
            }]
        );

        // The last pin: the torrent goes with its files.
        assert!(
            enginefs
                .unpin_download(TEST_HASH, 2, true)
                .await
                .unwrap()
                .unpinned
        );
        assert_eq!(
            *enginefs.get_backend().removed_with_files.lock().unwrap(),
            vec![TEST_HASH.to_string()]
        );
        assert!(enginefs.get_engine(TEST_HASH).await.is_none());
        assert!(enginefs.pinned_downloads().await.is_empty());
        assert_eq!(
            read_pinned_downloads(&enginefs.pinned_downloads_path()),
            serde_json::json!({}),
            "the persisted pin set is empty again"
        );
    }

    /// A per-file delete must both stop the backend writing the file and
    /// actually give the disk back. librqbit holds an open `File` on every
    /// file of a torrent for its lifetime, so unlinking alone leaves the
    /// inode's blocks allocated until the torrent is dropped -- the file is
    /// truncated first. And the want-set is reconciled without the file
    /// whether or not it was pinned, so nothing writes it again.
    #[tokio::test]
    async fn per_file_delete_deselects_the_file_and_frees_its_blocks() {
        let tmp = tempfile::tempdir().unwrap();
        let folder = tmp.path().join("Show");
        std::fs::create_dir_all(&folder).unwrap();
        let file = |idx: usize| folder.join(format!("video-{idx}.mkv"));
        let (enginefs, counters) = test_enginefs_with_file_count(3);
        for idx in 0..3 {
            std::fs::write(file(idx), [7u8; 4096]).unwrap();
        }
        *counters.output_folder.lock().unwrap() = Some(folder.clone());

        // File 2 is pinned, so the torrent keeps running; file 0 is not
        // pinned at all (a pin lost to a crash, or a plain "remove this
        // download") and is the one deleted.
        enginefs.pin_download(TEST_HASH, 2, None).await.unwrap();
        let open = std::fs::File::open(file(0)).unwrap();
        let before = counters.reconcile_file_priorities.load(Ordering::SeqCst);

        assert!(
            !enginefs
                .unpin_download(TEST_HASH, 0, true)
                .await
                .unwrap()
                .unpinned
        );
        assert!(!file(0).exists(), "the file is gone");
        assert!(file(2).is_file(), "the pinned file stays");
        assert!(enginefs.get_engine(TEST_HASH).await.is_some());
        assert_eq!(
            open.metadata().unwrap().len(),
            0,
            "the blocks are released through a handle still open on the file"
        );
        assert_eq!(
            counters.reconcile_file_priorities.load(Ordering::SeqCst),
            before + 1,
            "the want-set is recomputed without the deleted file"
        );
        assert_eq!(
            *counters.dropped_ranges.lock().unwrap(),
            vec![(0..1, crate::backend::AfterRelease::Reselect)],
            "and the backend is told to forget the file's pieces, and to want \
             the range again for the sake of a boundary piece a neighbour shares"
        );
    }

    /// **The interlock, from the cleaner's door.** A piece only leaves the
    /// disk once the backend has agreed to forget it, and only the pieces
    /// it agreed to.
    ///
    /// Unlink behind librqbit's back and the torrent still believes it
    /// holds the piece: it advertises it, and answers a peer's request with
    /// a read past the end of nothing. So the caller names pieces, the
    /// backend says which of them it will give up -- it keeps the ones a
    /// peer is mid-flight on and the ones a live stream is about to read --
    /// and only those are taken. A backend that will not give any of them
    /// up leaves every byte where it is.
    #[tokio::test]
    async fn a_reclaim_takes_only_the_pieces_the_backend_agreed_to_forget() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [3u32, 4, 5] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 4096]).unwrap();
        }
        // Piece 5 is one the backend keeps -- a peer is working on it, or a
        // stream is about to read it.
        *counters.drops_pieces.lock().unwrap() = vec![3, 4];

        assert_eq!(enginefs.release_pieces(TEST_HASH, &[3, 4, 5]).await, 2);
        assert!(!bucket.join("3").exists());
        assert!(!bucket.join("4").exists());
        assert!(
            bucket.join("5").is_file(),
            "the backend still believes it has piece 5, so it is not ours to take"
        );
        assert_eq!(
            *counters.dropped_ranges.lock().unwrap(),
            vec![(3..6, crate::backend::AfterRelease::LeaveDropped)],
            "asked as one range, and left dropped: a piece wanted again the \
             moment it is deleted is a re-download, not a reclaim"
        );

        // And a backend that will not forget anything keeps every byte.
        counters.refuses_drop.store(true, Ordering::SeqCst);
        assert_eq!(enginefs.release_pieces(TEST_HASH, &[5]).await, 0);
        assert!(bucket.join("5").is_file());
    }

    /// **The panel's window is a reading of the disk, not of the policy's
    /// intentions.**
    ///
    /// What a viewer is being told is "you can scrub back this far, and you
    /// have this much in hand". Both halves are therefore what is *on the
    /// disk* on each side of the playhead: the ahead half is read-ahead
    /// that has arrived, and a stream whose read-ahead has not arrived yet
    /// must not report the extent the policy intends to fill as though the
    /// bytes were there.
    #[tokio::test]
    async fn the_window_is_what_the_store_holds_of_the_file_split_at_the_playhead() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        // Four pieces of twenty-five bytes, so a playhead can have pieces on
        // both sides of it.
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        // Two pieces of budget over a four-piece file: a split, so a policy
        // is installed at all.
        enginefs.set_cache_budget(Some(50));

        // Three of the four pieces are on the disk: the fourth is
        // read-ahead that has not arrived.
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 2] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }

        // A reader sixty bytes into the file: piece two.
        engine.note_playhead(0, 60);
        engine.begin_retention(0).await;

        let numbers = enginefs
            .torrent_stream_numbers(TEST_HASH, 0)
            .await
            .expect("the engine exists");
        assert_eq!(
            numbers.window,
            Some(crate::retention::CacheWindow {
                behind_bytes: 50,
                ahead_bytes: 25,
            }),
            "pieces zero and one are behind the playhead; piece two is the one \
             under it and counts as ahead; piece three is not on the disk and \
             is not in hand"
        );
    }

    /// The transfer totals a panel shows are the torrent's own, this
    /// session's, and they reach the answer with the window.
    #[tokio::test]
    async fn a_torrent_stream_reports_what_the_torrent_has_moved() {
        let (enginefs, counters) = test_enginefs_with_file_count(1);
        counters.fetched.store(4_800, Ordering::SeqCst);
        counters.uploaded.store(2_100, Ordering::SeqCst);

        let numbers = enginefs
            .torrent_stream_numbers(TEST_HASH, 0)
            .await
            .expect("the engine exists");
        let transfer = numbers.transfer.expect("a running torrent's counters");
        assert_eq!(transfer.fetched, 4_800);
        assert_eq!(transfer.uploaded, 2_100);
    }

    /// **A torrent that has stopped moving bytes has not moved no bytes.**
    ///
    /// The counters live in the backend's running state, so there is
    /// nothing to read for a torrent that is paused, still checking,
    /// stopped for space or in error -- and every one of those is reachable
    /// while a panel is up: the fastresume check at the start of a stream,
    /// and the idle, background and free-space arms of the reconciler's
    /// verdict. Reporting zero there tells a viewer whose session has moved
    /// gigabytes that it has shared nothing, which is the reading a client
    /// cannot tell from the truth. So the absence is passed on.
    #[tokio::test]
    async fn a_torrent_whose_counters_cannot_be_read_reports_no_totals() {
        let (enginefs, counters) = test_enginefs_with_file_count(1);
        counters.fetched.store(4_800, Ordering::SeqCst);
        counters.uploaded.store(2_100, Ordering::SeqCst);
        enginefs.get_engine(TEST_HASH).await.unwrap();

        counters.paused.store(true, Ordering::SeqCst);
        let numbers = enginefs
            .torrent_stream_numbers(TEST_HASH, 0)
            .await
            .expect("the engine exists");
        assert_eq!(
            numbers.transfer, None,
            "a paused torrent has counters nothing can read, not counters at zero"
        );

        // And they are back the moment it is running again: nothing here
        // was forgotten, it was unreadable.
        counters.paused.store(false, Ordering::SeqCst);
        assert_eq!(
            enginefs
                .torrent_stream_numbers(TEST_HASH, 0)
                .await
                .and_then(|numbers| numbers.transfer)
                .map(|transfer| transfer.fetched),
            Some(4_800)
        );
    }

    /// **The window is this file's, not the torrent's.**
    ///
    /// A season pack whose earlier episodes are still on the disk holds
    /// pieces that have nothing to do with the episode playing. Counting
    /// them would tell a viewer they can scrub back into bytes that belong
    /// to another file -- the store's listing is the whole torrent's, and
    /// only the policy knows which of it is the stream being asked about.
    #[tokio::test]
    async fn the_window_leaves_the_rest_of_the_torrent_out_of_this_files_numbers() {
        let (enginefs, counters) = test_enginefs_with_files(vec![
            ("Show.S01E01.mkv".into(), 100),
            ("Show.S01E02.mkv".into(), 100),
        ]);
        // Four twenty-five byte pieces per file: episode one is pieces
        // 0..4, episode two 4..8.
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));

        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        // The whole of the episode watched last night, which nothing has
        // aged out yet, and two pieces of the one playing now.
        for piece in [0u32, 1, 2, 3, 4, 5] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }

        // A reader twenty-five bytes into the second episode: piece five.
        engine.note_playhead(1, 25);
        engine.begin_retention(1).await;

        let numbers = enginefs
            .torrent_stream_numbers(TEST_HASH, 1)
            .await
            .expect("the engine exists");
        assert_eq!(
            numbers.window,
            Some(crate::retention::CacheWindow {
                behind_bytes: 25,
                ahead_bytes: 25,
            }),
            "piece four is behind the playhead and piece five is under it;              the four pieces of the other episode are not this stream's to              scrub back into"
        );
    }

    /// **A retention pass must not blink the panel's rows out.**
    ///
    /// A pass takes the policy out of its slot for a directory listing and
    /// two awaited backend calls, and it runs on the reconciler's tick for
    /// exactly the stream a panel is asking about. Anything that answered
    /// from the slot itself would say "no window, no committed set" for a
    /// second or so out of every two -- and by this server's own contract
    /// that is not a delay but a statement: it means nothing is bounding
    /// this stream. So the bounds are kept beside the policy and the
    /// reading comes from there.
    #[tokio::test]
    async fn a_pass_in_flight_still_answers_what_is_bounding_the_stream() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));

        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }

        let store = enginefs.piece_store();
        engine.note_playhead(0, 0);
        engine.begin_retention(0).await;
        engine.retain(&store).await.expect("a pass");
        let settled = enginefs
            .torrent_stream_numbers(TEST_HASH, 0)
            .await
            .expect("the engine exists");
        assert!(
            settled.window.is_some() && settled.committed_bytes.is_some(),
            "the stream is bounded between passes: {settled:?}"
        );

        // Now park a pass inside the backend call it makes to announce
        // what the window has released, and ask while it is in there.
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *counters.advertise_gate.lock().unwrap() = Some((entered_tx, release_rx));
        engine.note_playhead(0, 25);
        let running = tokio::spawn({
            let engine = engine.clone();
            let store = store.clone();
            async move { engine.retain(&store).await }
        });
        tokio::time::timeout(Duration::from_secs(10), entered_rx)
            .await
            .expect("the pass reached the backend call")
            .expect("the fake said so");

        let mid_pass = enginefs
            .torrent_stream_numbers(TEST_HASH, 0)
            .await
            .expect("the engine exists");
        assert!(
            mid_pass.window.is_some() && mid_pass.committed_bytes.is_some(),
            "and it is still bounded while the pass that bounds it is              running: {mid_pass:?}"
        );

        release_tx.send(()).expect("the pass is waiting on this");
        running
            .await
            .expect("the pass task")
            .expect("a pass ran to the end");
    }

    /// **Every absence here is a real one, and none of them is a zero.**
    ///
    /// A client draws no row for "there is no such number" and a misleading
    /// one for "the number is zero", so the two must not be spelled the
    /// same way. A torrent this server does not hold has no answer at all; a
    /// torrent no reader has been inside has no playhead, and inventing one
    /// from what is on the disk would put a window round a region nobody
    /// has ever read; a torrent nothing is bounding has no window and no
    /// committed set, whatever it announces.
    #[tokio::test]
    async fn a_stream_with_no_playhead_and_no_policy_reports_no_window() {
        let (enginefs, _counters) = test_enginefs_with_file_count(2);
        assert!(
            enginefs
                .torrent_stream_numbers("f".repeat(40).as_str(), 0)
                .await
                .is_none(),
            "a hash no engine exists for is a stream this server is not holding"
        );

        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(1));
        engine.begin_retention(0).await;
        let numbers = enginefs
            .torrent_stream_numbers(TEST_HASH, 0)
            .await
            .expect("the engine exists");
        assert_eq!(
            (numbers.window, numbers.committed_bytes),
            (None, None),
            "a policy is installed, but no reader has been anywhere in the file"
        );

        // A reader is inside the *other* file: this one's numbers left with
        // it.
        engine.note_playhead(1, 0);
        let numbers = enginefs
            .torrent_stream_numbers(TEST_HASH, 0)
            .await
            .expect("the engine exists");
        assert_eq!((numbers.window, numbers.committed_bytes), (None, None));

        // And a budget that covers the file installs no policy, so there is
        // nothing bounding this stream to have a window or a committed set.
        let (enginefs, _counters) = test_enginefs_with_file_count(1);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(1_000_000));
        engine.note_playhead(0, 0);
        engine.begin_retention(0).await;
        let numbers = enginefs
            .torrent_stream_numbers(TEST_HASH, 0)
            .await
            .expect("the engine exists");
        assert_eq!((numbers.window, numbers.committed_bytes), (None, None));
    }

    /// **The file the panel named is the file the numbers have to be
    /// about.**
    ///
    /// There is one policy per torrent, so opening a reader on a second
    /// file of the same torrent moves what is bounding this torrent to that
    /// file -- and the playhead does not move with it: it is written as
    /// reads return, so until the first byte of the new file goes out it
    /// still names the old one. In that interval the playhead and the
    /// bounds name different files, and answering from the bounds anyway
    /// would take the second file's piece span and piece length and apply
    /// them to an offset in the first: a window and a committed set
    /// measured over somebody else's pieces, handed to a panel that asked
    /// about this file. There is nothing to say about a file nothing is
    /// bounding, so nothing is said.
    #[tokio::test]
    async fn a_policy_that_has_moved_to_another_file_says_nothing_about_this_one() {
        let (enginefs, counters) = test_enginefs_with_files(vec![
            ("Show.S01E01.mkv".into(), 100),
            ("Show.S01E02.mkv".into(), 100),
        ]);
        // Four twenty-five byte pieces per file: episode one is pieces
        // 0..4, episode two 4..8.
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));

        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        // Two pieces of each episode, so the other file's span has numbers
        // of its own to report if anything lets it.
        for piece in [0u32, 1, 4, 5] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }

        // A reader twenty-five bytes into the first episode, which is what
        // the policy is bounding.
        engine.note_playhead(0, 25);
        engine.begin_retention(0).await;
        let numbers = enginefs
            .torrent_stream_numbers(TEST_HASH, 0)
            .await
            .expect("the engine exists");
        assert_eq!(
            numbers.window,
            Some(crate::retention::CacheWindow {
                behind_bytes: 25,
                ahead_bytes: 25,
            }),
            "the file being read is bounded, and these are its pieces"
        );

        // A second reader opens on the next episode. That is what installs
        // a policy, so the bounds are the other file's from here on -- and
        // no byte of it has been read yet, so the playhead is still in this
        // one.
        engine.begin_retention(1).await;
        let numbers = enginefs
            .torrent_stream_numbers(TEST_HASH, 0)
            .await
            .expect("the engine exists");
        assert_eq!(
            (numbers.window, numbers.committed_bytes),
            (None, None),
            "nothing bounds this file any more, and the other file's span \
             over this file's offset is not a window: it is a reading of \
             somebody else's pieces"
        );
    }

    /// **A stream stops being bounded, and the panel has to hear that.**
    ///
    /// The bounds are kept beside the policy so that a pass which has the
    /// policy out of its slot still answers -- but they are a reading *of*
    /// that policy, and a policy that is dropped takes its window and its
    /// committed set with it. Two ordinary things drop one: a pin taken
    /// while the file is playing, which hands the whole file back to the
    /// user and to the swarm, and a budget that has grown to cover the file,
    /// which is a torrent nothing needs to bound. Left standing, the bounds
    /// would go on reporting a window and a promise for a stream that has
    /// neither, which by this server's own contract is a statement and not
    /// a stale number.
    #[tokio::test]
    async fn a_stream_whose_policy_is_dropped_stops_reporting_a_window() {
        /// What this server says is bounding the first file of the fixture.
        async fn bounded(
            enginefs: &BackendEngineFS<FakeBackend>,
        ) -> (Option<crate::retention::CacheWindow>, Option<u64>) {
            let numbers = enginefs
                .torrent_stream_numbers(TEST_HASH, 0)
                .await
                .expect("the engine exists");
            (numbers.window, numbers.committed_bytes)
        }
        let seeded = |enginefs: &BackendEngineFS<FakeBackend>| {
            let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
            std::fs::create_dir_all(&bucket).unwrap();
            for piece in [0u32, 1, 2, 3] {
                std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
            }
        };

        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        seeded(&enginefs);
        engine.note_playhead(0, 25);
        engine.begin_retention(0).await;
        assert_eq!(
            bounded(&enginefs).await,
            (
                Some(crate::retention::CacheWindow {
                    behind_bytes: 25,
                    ahead_bytes: 75,
                }),
                Some(0)
            ),
            "a policy is installed, and this is what it is bounding"
        );

        // The user pins the file they are watching. A pin is a retention
        // property -- those bytes were asked for and are shared like any
        // other bytes we keep -- so the pass drops the policy.
        engine.pinned_files.write().insert(0);
        let store = enginefs.piece_store();
        assert!(
            engine.retain(&store).await.is_none(),
            "a pinned torrent has no retention pass to make"
        );
        assert_eq!(
            bounded(&enginefs).await,
            (None, None),
            "and nothing bounds the stream now, so there is no window and \
             nothing promised: the reading went with the policy it was of"
        );

        // The other way a policy goes: a budget that has grown to cover the
        // file. The reader opens again, and there is nothing to bound.
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        seeded(&enginefs);
        engine.note_playhead(0, 25);
        engine.begin_retention(0).await;
        assert!(
            bounded(&enginefs).await.0.is_some(),
            "bounded to begin with"
        );

        enginefs.set_cache_budget(Some(1_000_000));
        engine.begin_retention(0).await;
        assert_eq!(
            bounded(&enginefs).await,
            (None, None),
            "the budget covers the whole file, so no policy is installed -- \
             and a window left over from the one that was is a claim about a \
             stream nothing is bounding"
        );
    }

    /// The committed bytes are the set the policy has really settled on --
    /// what we have advertised and will not reclaim -- and they grow as
    /// playback walks past pieces, never from what happens to be on disk.
    #[tokio::test]
    async fn the_committed_bytes_are_what_playback_has_walked_past() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));

        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }

        let store = enginefs.piece_store();
        engine.note_playhead(0, 0);
        engine.begin_retention(0).await;
        engine.retain(&store).await.expect("a pass");
        assert_eq!(
            enginefs
                .torrent_stream_numbers(TEST_HASH, 0)
                .await
                .and_then(|numbers| numbers.committed_bytes),
            Some(0),
            "the first pass of a stream commits nothing: no window has \
             released a piece yet"
        );

        // Playback walks on to the second piece, which releases the first.
        engine.note_playhead(0, 25);
        engine.retain(&store).await.expect("a pass");
        assert_eq!(
            enginefs
                .torrent_stream_numbers(TEST_HASH, 0)
                .await
                .and_then(|numbers| numbers.committed_bytes),
            Some(25),
            "one piece of twenty-five bytes is committed for sharing"
        );
    }

    /// **A file's first and last piece are its neighbours' too.**
    ///
    /// Pieces are a fixed length across a whole torrent, so in one without
    /// BEP-47 padding a file that does not begin and end on a piece boundary
    /// shares those two pieces with whatever lies either side of it. The
    /// policy governs one file and its reclaim set includes both, and
    /// nothing under it refuses: librqbit drops a piece it *has* whoever
    /// wants it -- `ChunkTracker::drop_piece` consults the want-set only for
    /// a piece we do not have -- and the store then unlinks the bytes. The
    /// still-selected neighbour fetches the piece again, the next pass finds
    /// it held and outside the window again and reclaims it again: a refetch
    /// loop at the boundary for as long as the neighbour is wanted, paid for
    /// in the neighbour's bytes and the swarm's.
    #[tokio::test]
    async fn the_piece_the_next_file_shares_survives_a_reclaim_of_this_one() {
        let (enginefs, counters) = test_enginefs_with_files(vec![
            ("Show.S01E01.mkv".into(), 100),
            ("Show.S01E02.mkv".into(), 110),
            ("Show.S01E03.mkv".into(), 100),
        ]);
        // Twenty-five byte pieces over three files that do not divide by
        // them: episode two is pieces 4..9 and episode three 8..13, so piece
        // eight holds the last ten bytes of one and the first fifteen of the
        // other.
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        // Two pieces of budget over a five-piece file: a split, so a policy
        // is installed at all.
        enginefs.set_cache_budget(Some(50));

        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [4u32, 5, 8] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }

        // A reader at the top of episode two: piece four is the window, and
        // pieces five and eight are outside it.
        engine.note_playhead(1, 0);
        engine.begin_retention(1).await;
        let store = enginefs.piece_store();
        let pass = engine.retain(&store).await.expect("a pass");

        assert!(
            bucket.join("8").is_file(),
            "the piece the next episode also lies in stays on the disk, \
             because the torrent still wants that episode"
        );
        assert!(
            !bucket.join("5").exists(),
            "a piece of nobody else's still goes: this is a reclaim, not a refusal"
        );
        assert_eq!(pass.reclaimed, 1, "one piece was this file's alone to give");
        assert_eq!(
            *counters.dropped_ranges.lock().unwrap(),
            vec![(5..6, crate::backend::AfterRelease::LeaveDropped)],
            "the backend is never even asked to forget the shared piece: it \
             would agree, and the bytes would go"
        );
    }

    /// The same boundary piece, once nothing else wants it.
    ///
    /// What holds the piece back is the *neighbour*, not the boundary: a
    /// file the torrent has stopped wanting will not fetch the piece again,
    /// so there is no loop to avoid and no data anybody asked for to lose.
    /// A rule that refused every boundary piece instead would leave two
    /// pieces of every file on the disk for ever, and the cache cleaner
    /// walking a volume of them.
    #[tokio::test]
    async fn a_shared_piece_goes_once_the_file_that_shares_it_is_out_of_the_want_set() {
        let (enginefs, counters) = test_enginefs_with_files(vec![
            ("Show.S01E01.mkv".into(), 100),
            ("Show.S01E02.mkv".into(), 110),
            ("Show.S01E03.mkv".into(), 100),
        ]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        // The third episode is out of the want-set -- the viewer never asked
        // for it, or a delete took it -- so nothing of the torrent wants
        // piece eight but the file being reclaimed.
        *counters.wanted_files.lock().unwrap() = Some([0, 1].into_iter().collect());
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));

        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [4u32, 5, 8] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }

        engine.note_playhead(1, 0);
        engine.begin_retention(1).await;
        let store = enginefs.piece_store();
        let pass = engine.retain(&store).await.expect("a pass");

        assert!(
            !bucket.join("8").exists(),
            "nothing else wants the piece, so it is cache like any other"
        );
        assert!(!bucket.join("5").exists());
        assert_eq!(
            pass.reclaimed, 2,
            "both pieces outside the window were this pass's to take"
        );
    }

    /// A piece that becomes announced between the cleaner's reading and its
    /// unlink is not taken.
    ///
    /// The cleaner's gate is collected before a blocking directory walk and
    /// before every delete ahead of this one, so by the time a delete
    /// happens the reading can be minutes old. Two things make a piece
    /// announced on the happy path, both of them ordinary: a retention pass
    /// commits and advertises pieces every couple of seconds, and a reader
    /// moving to another file puts the first file's whole range back. So
    /// the question is asked a second time, against the live policy, inside
    /// the same lock those two take -- `Engine::release_reclaimable`.
    ///
    /// Without that, the invariant the whole design rests on is a
    /// likelihood rather than a rule: we would delete a piece we had told a
    /// peer about.
    #[tokio::test]
    async fn a_piece_announced_since_the_cleaner_looked_is_left_alone() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 4096]).unwrap();
        *counters.drops_pieces.lock().unwrap() = vec![0];
        // A policy is installed only where the budget does not cover the
        // file, so the fixture needs one that does not.
        enginefs.set_cache_budget(Some(1));

        // A policy over file 0 that would give piece 0 up: this is the
        // reading the cleaner takes.
        engine.note_playhead(0, 0);
        engine.begin_retention(0).await;
        let mut gate = crate::retention::ReclaimGate::default();
        engine.gate_entry(&mut gate);
        assert!(
            gate.releases(TEST_HASH, 0),
            "the cleaner's reading says this piece may go"
        );

        // Then the reader moves to the other file, which puts file 0's
        // range back into what we announce -- exactly what happens when a
        // viewer skips to the next episode while a clean pass is walking.
        engine.note_playhead(1, 0);
        engine.begin_retention(1).await;

        assert_eq!(
            enginefs.release_pieces(TEST_HASH, &[0]).await,
            0,
            "the piece is announced now, whatever the cleaner's reading said"
        );
        assert!(
            bucket.join("0").is_file(),
            "and it is still on the disk: we told a peer we had it"
        );
    }

    /// A pin taken while the file is already playing keeps its bytes.
    ///
    /// The pin exemption used to live only in `begin_retention`, which runs
    /// when a reader opens. Pin a file that is already playing and the
    /// policy installed before the pin existed stays installed, and the
    /// retention pass reclaims under it: measured on a real session, half a
    /// 32 MiB file deleted with `is_pinned()` true the whole time. Worse
    /// than the deletion, the policy also holds the range back, so the file
    /// the user asked to keep is announced to nobody while librqbit
    /// re-fetches what was just thrown away.
    ///
    /// A pin is a retention property, so the pass has to ask, not just the
    /// opener.
    #[tokio::test]
    async fn a_pin_taken_mid_playback_stops_the_reclaim_and_puts_the_range_back() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        *counters.drops_pieces.lock().unwrap() = vec![0, 1, 2, 3];
        // A policy is installed only where the budget does not cover the
        // file, so the fixture needs one that does not.
        enginefs.set_cache_budget(Some(1));

        engine.note_playhead(0, 0);
        engine.begin_retention(0).await;
        assert!(
            engine.gate_verdict().releases(0),
            "before the pin, the policy would give this piece up"
        );

        // The user pins the file they are watching.
        engine.pinned_files.write().insert(0);

        let store = enginefs.piece_store();
        assert!(
            engine.retain(&store).await.is_none(),
            "a pinned torrent has no retention pass to make"
        );
        assert!(
            !engine.gate_verdict().releases(0),
            "and nothing of it may be reclaimed any more"
        );
    }

    /// A stream that moves to another file of the same torrent puts the
    /// first file's range back into what we announce before it holds the
    /// second one's back.
    ///
    /// One policy per torrent, so opening a reader on another file replaces
    /// it -- and the range it was holding back would otherwise stay
    /// announced to nobody for the life of the engine, while the cache
    /// cleaner's gate, which reads "no policy for this piece" as "we
    /// announce it", went on calling those same pieces protected. Held back
    /// and protected at once is the one combination that is never right:
    /// bytes we will not share and will not reclaim either.
    #[tokio::test]
    async fn a_reader_moving_to_another_file_gives_the_first_one_back_to_the_swarm() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        // Smaller than either file, so both get a real policy rather than
        // `Shape::Whole` -- which installs nothing and holds nothing back.
        enginefs.set_cache_budget(Some(40));
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        let hash = TEST_HASH.to_lowercase();
        engine.begin_retention(0).await;
        let gate = enginefs.reclaim_verdicts().await.gate;
        assert!(
            gate.releases(&hash, 0),
            "file 0's piece is inside the window and uncommitted, so it may go"
        );

        engine.begin_retention(1).await;
        assert_eq!(
            *counters.advertised.lock().unwrap(),
            vec![(0..1, false), (0..1, true), (1..2, false)],
            "file 0 held back, then given back, and only then file 1 held back"
        );

        // And the gate says the same thing from the other side: the piece
        // we announce again is one nothing may take, and the piece we are
        // now holding back is one a pass may.
        let gate = enginefs.reclaim_verdicts().await.gate;
        assert!(!gate.releases(&hash, 0), "announced again, so protected");
        assert!(gate.releases(&hash, 1), "held back, so reclaimable");
    }

    /// A hash the session runs no torrent for has no have-set for a
    /// deletion to disagree with, so its pieces go straight to the store.
    ///
    /// This is most of what the cache cleaner reclaims: a previous
    /// install's leftovers, and torrents the idle sweep has already taken
    /// out of the session. Refusing them because no backend would vouch for
    /// them would leave a disk full of bytes nothing will ever read.
    #[tokio::test]
    async fn pieces_of_a_torrent_the_session_does_not_run_go_without_a_claim() {
        let (enginefs, counters) = test_enginefs_with_file_count(1);
        let orphan = "fedcba9876543210fedcba9876543210fedcba98";
        let bucket = enginefs.piece_store().torrent_dir(orphan).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("7"), [7u8; 4096]).unwrap();

        assert_eq!(enginefs.release_pieces(orphan, &[7]).await, 1);
        assert!(!bucket.join("7").exists());
        assert!(
            counters.dropped_ranges.lock().unwrap().is_empty(),
            "there was nothing to ask"
        );
    }

    /// The file a per-file delete removes must stop being the file playback
    /// is registered on, or the want-set re-planned right after it puts the
    /// deleted index straight back into `only_files` (the active file is
    /// always unioned in) -- librqbit then keeps writing through the handle
    /// it holds open on the torrent's files and re-allocates blocks on the
    /// unlinked inode, which nothing can reclaim until the process exits.
    #[tokio::test]
    async fn per_file_delete_stops_the_file_counting_as_the_one_playing() {
        let tmp = tempfile::tempdir().unwrap();
        let folder = tmp.path().join("Show");
        std::fs::create_dir_all(&folder).unwrap();
        let file = |idx: usize| folder.join(format!("video-{idx}.mkv"));
        let (enginefs, counters) = test_enginefs_with_file_count(3);
        for idx in 0..3 {
            std::fs::write(file(idx), [7u8; 4096]).unwrap();
        }
        *counters.output_folder.lock().unwrap() = Some(folder.clone());

        // File 2 is pinned, so the torrent keeps running after the delete;
        // file 0 is the one being played, and the one deleted.
        enginefs.pin_download(TEST_HASH, 2, None).await.unwrap();
        enginefs
            .activate_multifile_file_for_playback(TEST_HASH, 0, None, "test-playing")
            .await;
        enginefs
            .active_file_streams
            .write()
            .await
            .insert((TEST_HASH.to_string(), 0), 1);
        assert_eq!(*counters.last_active_file.lock().unwrap(), Some(0));

        assert!(
            !enginefs
                .unpin_download(TEST_HASH, 0, true)
                .await
                .unwrap()
                .unpinned
        );

        assert_eq!(
            *counters.last_active_file.lock().unwrap(),
            None,
            "the deleted file is not planned back into the want-set"
        );
        assert!(
            !enginefs
                .active_multifile_files
                .read()
                .await
                .contains_key(TEST_HASH),
            "and it is no longer the torrent's active selection"
        );
        let key = (TEST_HASH.to_string(), 0);
        assert!(
            !enginefs.active_file_streams.read().await.contains_key(&key),
            "and its stream count"
        );
        assert_eq!(*enginefs.active_file.read().await, None);
        assert!(!file(0).exists());
        assert!(file(2).is_file(), "the pinned file is untouched");

        // A selection naming another file of the same torrent is left
        // alone: that file is still playing.
        enginefs
            .activate_multifile_file_for_playback(TEST_HASH, 2, None, "test-playing")
            .await;
        assert!(
            !enginefs
                .unpin_download(TEST_HASH, 1, true)
                .await
                .unwrap()
                .unpinned
        );
        assert_eq!(*counters.last_active_file.lock().unwrap(), Some(2));
    }

    /// A delete of a file nothing pinned (a pin lost to a crash, or a plain
    /// "remove this download" for a torrent that was only streamed) still
    /// drops the torrent and its data, and reports `false`: no pin cleared.
    #[tokio::test]
    async fn unpin_download_deletes_an_unpinned_file_too() {
        let (enginefs, _counters) = test_enginefs_with_file_count(2);
        assert!(
            !enginefs
                .unpin_download(TEST_HASH, 0, true)
                .await
                .unwrap()
                .unpinned
        );
        assert_eq!(
            *enginefs.get_backend().removed_with_files.lock().unwrap(),
            vec![TEST_HASH.to_string()]
        );
        assert!(enginefs.get_engine(TEST_HASH).await.is_none());
    }

    /// An unpin issued while a pin of the same hash is still in flight --
    /// a magnet add resolving metadata, or a relocation moving files --
    /// must queue behind it and apply to the finished pin. It takes the
    /// same per-hash lock: without one it would find no engine (the hash
    /// is parked in the magnet registry for the length of the add), fall
    /// into the dormant branch, do nothing, and leave the pin to land and
    /// be persisted behind it.
    #[tokio::test]
    async fn unpin_download_queues_behind_an_in_flight_pin() {
        let root = tempfile::tempdir().unwrap();
        let counters = Arc::new(FakeCounters::default());
        let handle = FakeHandle {
            info_hash: TEST_HASH.to_string(),
            counters: counters.clone(),
            files: (0..2)
                .map(|idx| BackendFileInfo {
                    name: format!("video-{idx}.mkv"),
                    length: 100,
                })
                .collect(),
            init: FakeInit::new(true, Duration::from_secs(60)),
        };
        let enginefs = BackendEngineFS::new_with_backend(
            FakeBackend::new(vec![handle]),
            HashMap::new(),
            root.path().join("cache"),
            root.path().join("downloads"),
        );
        std::fs::create_dir_all(root.path().join("downloads")).unwrap();
        enginefs.backend.hold_add.store(true, Ordering::SeqCst);

        // Both handoffs are waited for by state, never by a stopwatch: a
        // loaded runner that missed a fixed sleep would let the unpin run
        // before the pin exists (finding nothing to unpin, so the pin lands
        // and is persisted behind it) and fail the assertions below.
        let unpin = async {
            // The pin is inside the held add once the backend has recorded
            // its placement.
            assert!(
                wait_until(TEST_WAIT_BOUND, || {
                    !enginefs.backend.placements.lock().unwrap().is_empty()
                })
                .await,
                "the pin reached the held backend add"
            );
            assert!(enginefs.get_engine(TEST_HASH).await.is_none());
            enginefs
                .unpin_download(TEST_HASH, 0, true)
                .await
                .unwrap()
                .unpinned
        };
        let release = async {
            // The pin holds the per-hash lock and so does the map; a
            // third reference means the unpin has taken it too and is
            // parked on it -- exactly the interleaving under test.
            assert!(
                wait_until(TEST_WAIT_BOUND, || {
                    enginefs
                        .pin_locks
                        .lock()
                        .get(TEST_HASH)
                        .is_some_and(|lock| Arc::strong_count(lock) >= 3)
                })
                .await,
                "the unpin queued behind the in-flight pin"
            );
            enginefs.backend.add_hold.add_permits(1);
        };
        let (pinned, _unpinned, ()) =
            tokio::join!(enginefs.pin_download(TEST_HASH, 0, None), unpin, release);
        pinned.expect("the pin itself goes through");

        assert!(
            enginefs.pinned_downloads().await.is_empty(),
            "the unpin applies to the pin it queued behind"
        );
        assert_eq!(
            read_pinned_downloads(&enginefs.pinned_downloads_path()),
            serde_json::json!({}),
            "and nothing survives in the persisted set"
        );
    }

    /// A dormant pin (no torrent in the backend) had nothing on disk the
    /// old placement could name -- the torrent lived in the cache root
    /// under a folder named by metadata the pin does not have.
    /// The pin is dropped, nothing is deleted, nothing fails, and the
    /// answer says the data did not go rather than echoing the request.
    #[tokio::test]
    async fn unpin_download_of_a_dormant_pin_deletes_nothing() {
        let root = tempfile::tempdir().unwrap();
        let counters = Arc::new(FakeCounters::default());
        let handle = FakeHandle {
            info_hash: OTHER_HASH.to_string(),
            counters,
            files: vec![BackendFileInfo {
                name: "video-0.mkv".to_string(),
                length: 100,
            }],
            init: FakeInit::new(true, Duration::from_secs(60)),
        };
        let enginefs = BackendEngineFS::new_with_backend(
            FakeBackend::new(vec![handle]),
            HashMap::new(),
            root.path().join("cache"),
            root.path().join("downloads"),
        );
        std::fs::create_dir_all(root.path().join("downloads")).unwrap();
        std::fs::write(
            enginefs.pinned_downloads_path(),
            serde_json::to_vec(&serde_json::json!({ TEST_HASH: [1] })).unwrap(),
        )
        .unwrap();
        assert_eq!(enginefs.restore_pinned_downloads().await, 0);

        assert_eq!(
            enginefs.unpin_download(TEST_HASH, 1, true).await.unwrap(),
            UnpinOutcome {
                unpinned: true,
                deleted_files: false,
            }
        );
        assert!(
            enginefs
                .get_backend()
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            read_pinned_downloads(&enginefs.pinned_downloads_path()),
            serde_json::json!({})
        );
    }

    /// A dormant pin asked to take its data with it: the placement folder
    /// A destructive unpin of one file of a torrent that keeps others has to
    /// take the **piece files**, and until the piece store became the
    /// session's default storage nothing here did.
    ///
    /// `drop_file_pieces` is librqbit's own have-set bookkeeping: it forgets
    /// the pieces and frees not one byte, handing the indices back precisely
    /// so that whoever asked can delete them (`DroppedFilePieces`). While the
    /// session wrote whole files the delete of the file *was* the delete of
    /// the bytes and there was nothing else to do; now the file at the
    /// backend's path is at most a leftover an earlier version wrote, and the
    /// data is the pieces. Without this the caller was answered
    /// `deletedFiles: true` with every piece of the deleted file still in the
    /// store -- and nothing would ever have reclaimed them, since the
    /// torrent's piece directory is protected for as long as it has a pin.
    ///
    /// Which is also why `deleted_files` stops reading "already absent" as
    /// "freed": under this storage the backend's path is *always* absent, so
    /// the old `NotFound => true` would have made the flag a constant true.
    #[tokio::test]
    async fn a_per_file_delete_takes_the_pieces_the_backend_gave_up() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        *counters.output_folder.lock().unwrap() = Some(enginefs.download_dir.join("show"));
        let pieces = enginefs.piece_store().torrent_dir(TEST_HASH);
        let bucket = pieces.join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [3u32, 4, 5] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 4096]).unwrap();
        }
        // A staged copy of one of them, from a re-download that was in
        // flight: half a piece nobody wants is worth as little as all of it.
        std::fs::write(bucket.join("4.part"), [7u8; 512]).unwrap();
        // Piece 5 is the still-pinned neighbour's, and the backend does not
        // give it up.
        *counters.drops_pieces.lock().unwrap() = vec![3, 4];

        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        enginefs.pin_download(TEST_HASH, 1, None).await.unwrap();

        assert_eq!(
            enginefs.unpin_download(TEST_HASH, 0, true).await.unwrap(),
            UnpinOutcome {
                unpinned: true,
                deleted_files: true,
            },
            "the bytes really left the disk"
        );
        assert!(!bucket.join("3").exists());
        assert!(!bucket.join("4").exists());
        assert!(!bucket.join("4.part").exists(), "and the staged copy");
        assert!(
            bucket.join("5").is_file(),
            "the still-pinned file's piece is not the delete's to take"
        );
        assert!(
            enginefs.get_engine(TEST_HASH).await.is_some(),
            "the torrent keeps running for its other pin"
        );

        // And nothing left the disk the second time, so the answer says so
        // rather than echoing the request flag: the pieces are already gone
        // and the backend's path never held anything.
        assert_eq!(
            enginefs.unpin_download(TEST_HASH, 0, true).await.unwrap(),
            UnpinOutcome {
                unpinned: false,
                deleted_files: false,
            }
        );
    }

    /// A dormant pin's bytes are its pieces, and an unpin that asks to take
    /// the data goes and takes them: the store's directory for the hash,
    /// which `protected_paths` holds the cleaner off for as long as the pin
    /// stands -- and the entry leaves `downloads.json` with the pin, so no
    /// client could ask again either. The directory stays while another file
    /// of the same torrent is still pinned: it holds that file's pieces too.
    ///
    /// This used to delete `<downloadsDir>/<info hash>`, and so deleted
    /// nothing at all on an install with no separate downloads directory
    /// configured -- which was every default one, and is now every one.
    #[tokio::test]
    async fn unpin_download_of_a_dormant_pin_deletes_its_pieces() {
        let root = tempfile::tempdir().unwrap();
        let handle = FakeHandle {
            info_hash: OTHER_HASH.to_string(),
            counters: Arc::new(FakeCounters::default()),
            files: vec![BackendFileInfo {
                name: "video-0.mkv".to_string(),
                length: 100,
            }],
            init: FakeInit::new(true, Duration::from_secs(60)),
        };
        let enginefs = BackendEngineFS::new_with_backend(
            FakeBackend::new(vec![handle]),
            HashMap::new(),
            root.path().join("cache"),
            root.path().join("rqbit-downloads"),
        );
        std::fs::create_dir_all(root.path().join("rqbit-downloads")).unwrap();
        let pieces = enginefs.piece_store().path().to_path_buf();
        let folder = pieces.join(TEST_HASH);
        std::fs::create_dir_all(folder.join("0")).unwrap();
        for piece in [1, 2] {
            std::fs::write(folder.join("0").join(piece.to_string()), [7u8; 4096]).unwrap();
        }

        std::fs::write(
            enginefs.pinned_downloads_path(),
            serde_json::to_vec(&serde_json::json!({ TEST_HASH: [1, 2] })).unwrap(),
        )
        .unwrap();
        assert_eq!(enginefs.restore_pinned_downloads().await, 0);

        // File 2 of the same torrent is still pinned, and its data is in
        // that directory.
        assert_eq!(
            enginefs.unpin_download(TEST_HASH, 1, true).await.unwrap(),
            UnpinOutcome {
                unpinned: true,
                deleted_files: false,
            }
        );
        assert!(folder.is_dir(), "the other pin's data stays");

        // The last one takes the pieces with it.
        assert_eq!(
            enginefs.unpin_download(TEST_HASH, 2, true).await.unwrap(),
            UnpinOutcome {
                unpinned: true,
                deleted_files: true,
            }
        );
        assert!(
            !folder.exists(),
            "the pin's bytes go with it instead of waiting on the age rule"
        );
        assert!(pieces.is_dir(), "only the torrent's own directory goes");
        assert!(
            enginefs
                .get_backend()
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty(),
            "there was no torrent to remove"
        );
        assert_eq!(
            read_pinned_downloads(&enginefs.pinned_downloads_path()),
            serde_json::json!({})
        );
    }

    /// The registry is not the session. `remove_engine` drops the registry
    /// entry and leaves the torrent running in the backend -- it is what
    /// the TUI's delete key does, and the idle sweep's own first step --
    /// so an unpin arriving afterwards finds no engine while the torrent is
    /// very much alive. Nothing was pinned and nothing may be deleted: the
    /// bytes in the store belong to a torrent this call has no engine to
    /// reach, and unlinking them by hand is exactly the have-set desync
    /// `delete_download_data` holds a claim across the unlink to avoid.
    #[tokio::test]
    async fn an_unpin_of_a_hash_the_registry_lost_leaves_the_live_torrent_alone() {
        let (enginefs, _counters) = test_enginefs_unmanaged();
        let pieces = enginefs.piece_store().torrent_dir(TEST_HASH);
        std::fs::create_dir_all(pieces.join("0")).unwrap();
        let piece = pieces.join("0").join("1");
        std::fs::write(&piece, [7u8; 100]).unwrap();

        enginefs.get_or_add_engine(TEST_HASH).await.unwrap();
        enginefs.remove_engine(TEST_HASH).await;
        assert!(enginefs.get_engine(TEST_HASH).await.is_none());
        assert!(
            enginefs
                .get_backend()
                .get_torrent(TEST_HASH)
                .await
                .is_some(),
            "the session still has it, which is the whole point"
        );

        assert_eq!(
            enginefs.unpin_download(TEST_HASH, 0, true).await.unwrap(),
            UnpinOutcome {
                unpinned: false,
                deleted_files: false,
            }
        );
        assert!(
            piece.is_file(),
            "no pin was removed, so nothing here has any warrant to delete the torrent's bytes"
        );
        assert!(
            enginefs.backend.removed.lock().unwrap().is_empty()
                && enginefs
                    .backend
                    .removed_with_files
                    .lock()
                    .unwrap()
                    .is_empty(),
            "and the torrent nobody unpinned stays in the session"
        );
    }

    /// The same lost-engine window, but with a dormant pin behind it, so
    /// the delete does have its warrant. The bytes still may not be
    /// unlinked by hand -- the session holds the torrent and would go on
    /// advertising the pieces -- so the delete goes through the backend,
    /// which drops the torrent before its storage releases them.
    #[tokio::test]
    async fn a_dormant_pins_delete_goes_through_the_session_that_still_holds_the_torrent() {
        let (enginefs, _counters) = test_enginefs_unmanaged();
        let pieces = enginefs.piece_store().torrent_dir(TEST_HASH);
        std::fs::create_dir_all(pieces.join("0")).unwrap();
        let piece = pieces.join("0").join("1");
        std::fs::write(&piece, [7u8; 100]).unwrap();
        std::fs::create_dir_all(&enginefs.download_dir).unwrap();
        std::fs::write(
            enginefs.pinned_downloads_path(),
            serde_json::to_vec(&serde_json::json!({ TEST_HASH: [0] })).unwrap(),
        )
        .unwrap();
        // Dormant: the registry has no engine for it, whatever the session
        // holds.
        assert_eq!(enginefs.restore_pinned_downloads().await, 0);

        assert_eq!(
            enginefs.unpin_download(TEST_HASH, 0, true).await.unwrap(),
            UnpinOutcome {
                unpinned: true,
                deleted_files: true,
            }
        );
        assert_eq!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .as_slice(),
            &[TEST_HASH.to_string()],
            "deleted through the session, which takes the torrent out before its storage frees \
             the pieces"
        );
        assert!(
            piece.is_file(),
            "and not by hand behind a torrent that still advertises them"
        );
    }

    /// A magnet add parks its hash outside both the registry and the
    /// backend for as long as metadata takes, and the pin lock does not
    /// cover a *stream's* add. An unpin landing in that window must leave
    /// the store alone: the torrent being added builds its have-set from
    /// the pieces that are there, and a directory removed under it is the
    /// same desync by another route.
    #[tokio::test]
    async fn a_dormant_pins_delete_leaves_an_add_in_flight_its_pieces() {
        let (enginefs, _counters) = test_enginefs_unmanaged();
        let pieces = enginefs.piece_store().torrent_dir(TEST_HASH);
        std::fs::create_dir_all(pieces.join("0")).unwrap();
        let piece = pieces.join("0").join("1");
        std::fs::write(&piece, [7u8; 100]).unwrap();
        std::fs::create_dir_all(&enginefs.download_dir).unwrap();
        std::fs::write(
            enginefs.pinned_downloads_path(),
            serde_json::to_vec(&serde_json::json!({ TEST_HASH: [0] })).unwrap(),
        )
        .unwrap();
        assert_eq!(enginefs.restore_pinned_downloads().await, 0);

        // The session has not got the torrent yet -- it is being added.
        enginefs.backend.hide_torrents.store(true, Ordering::SeqCst);
        enginefs.backend.hold_add.store(true, Ordering::SeqCst);
        let adding = match enginefs.get_or_begin_add_magnet(TEST_HASH, None).await {
            EngineLookup::Adding(pending) => pending,
            _ => panic!("the stream's add should be in flight"),
        };

        assert_eq!(
            enginefs.unpin_download(TEST_HASH, 0, true).await.unwrap(),
            UnpinOutcome {
                unpinned: true,
                deleted_files: false,
            },
            "the pin goes; the bytes the add is about to claim do not"
        );
        assert!(piece.is_file(), "left for the add to find");

        enginefs.backend.hold_add.store(false, Ordering::SeqCst);
        enginefs.backend.add_hold.add_permits(1);
        adding.done.await.expect("the add finishes");
        assert!(
            enginefs.get_engine(TEST_HASH).await.is_some(),
            "and the engine is live immediately after"
        );
        assert!(
            piece.is_file(),
            "holding pieces the session was never told had gone"
        );
    }

    /// A delete for a file index the torrent does not have is refused --
    /// the same 404-shaped `FileNotFound` a pin of it gets. Unvalidated it
    /// would find nothing pinned, conclude the torrent has no pins left and
    /// delete every file of it: a stale index from a client is not a
    /// request to wipe the whole download.
    #[tokio::test]
    async fn unpin_download_with_delete_rejects_an_out_of_range_file() {
        let tmp = tempfile::tempdir().unwrap();
        let folder = tmp.path().join("Show");
        std::fs::create_dir_all(&folder).unwrap();
        let file = |idx: usize| folder.join(format!("video-{idx}.mkv"));
        let (enginefs, counters) = test_enginefs_with_file_count(3);
        for idx in 0..3 {
            std::fs::write(file(idx), b"payload").unwrap();
        }
        *counters.output_folder.lock().unwrap() = Some(folder.clone());
        enginefs.get_or_add_magnet(TEST_HASH, None).await.unwrap();

        let err = match enginefs.unpin_download(TEST_HASH, 3, true).await {
            Ok(outcome) => panic!("index 3 of 3 files must be refused, got {outcome:?}"),
            Err(err) => err,
        };
        assert!(
            matches!(
                err,
                PinDownloadError::FileNotFound {
                    file_idx: 3,
                    file_count: 3
                }
            ),
            "{err:?}"
        );
        for idx in 0..3 {
            assert!(file(idx).is_file(), "file {idx} is untouched");
        }
        assert!(
            enginefs
                .get_backend()
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty(),
            "the torrent is not dropped for an index it does not have"
        );
        assert!(enginefs.get_engine(TEST_HASH).await.is_some());

        // Without `delete_files` there is nothing destructive to guard: an
        // unknown index simply reports that no pin was cleared.
        assert!(
            !enginefs
                .unpin_download(TEST_HASH, 3, false)
                .await
                .unwrap()
                .unpinned
        );
    }

    #[tokio::test]
    async fn pin_download_rejects_out_of_range_file() {
        let (enginefs, counters) = test_enginefs_with_file_count(3);
        let err = match enginefs.pin_download(TEST_HASH, 3, None).await {
            Ok(_) => panic!("index 3 of 3 files must be rejected"),
            Err(err) => err,
        };
        assert!(
            matches!(
                err,
                PinDownloadError::FileNotFound {
                    file_idx: 3,
                    file_count: 3
                }
            ),
            "{err:?}"
        );
        assert_eq!(counters.pin_file.load(Ordering::SeqCst), 0);
        assert!(enginefs.pinned_downloads().await.is_empty());
        assert!(!enginefs.get_engine(TEST_HASH).await.unwrap().is_pinned());
    }

    /// A stopped torrent must download again once one of its files is
    /// pinned -- whoever stopped it, including the process before this one.
    ///
    /// Asserted on the run state, never on a resume counter: on master this
    /// read `if idle_paused.swap(false) && resume()`, so with a stopped
    /// torrent and an empty flag -- every torrent after a restart -- the
    /// pin recorded an offline download that downloaded nothing, and a
    /// counter-based test could not have told the difference.
    #[tokio::test(start_paused = true)]
    async fn pin_download_starts_a_stopped_torrent() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(2);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        stop_torrent(&enginefs, TEST_HASH).await;

        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Live,
            "a pinned download that is not running is not a download"
        );
    }

    /// Single-file torrents are always fully wanted: the pin is recorded
    /// (so the engine is exempt from eviction) but no selection is planned.
    #[tokio::test]
    async fn pin_download_on_single_file_torrent_records_pin_without_reconcile() {
        let (enginefs, counters) = test_enginefs();
        let engine = enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert_eq!(engine.pinned_file_indices(), vec![0]);
        assert_eq!(counters.pin_file.load(Ordering::SeqCst), 1);
        assert_eq!(counters.reconcile_file_priorities.load(Ordering::SeqCst), 0);
        assert!(engine.get_statistics().await.files[0].pinned);
    }

    /// The inactivity sweep must leave a pinned engine alone -- removing the
    /// torrent from the session would stop the offline download -- while
    /// still removing an idle unpinned one; once unpinned, the engine is
    /// ordinary again and the next window removes it.
    #[tokio::test(start_paused = true)]
    async fn idle_sweeper_keeps_pinned_engine_and_removes_unpinned() {
        let TwoEngines {
            enginefs, removed, ..
        } = test_enginefs_with_two_engines();
        enginefs.pin_download(TEST_HASH, 1, None).await.unwrap();
        let present = |hash: &str| {
            let engines = enginefs.engines.clone();
            let hash = hash.to_string();
            async move { engines.read().await.contains_key(&hash) }
        };

        // Nobody touches either engine for a full inactivity window (the
        // sweep runs every 15 s; +30 s covers the tick after the window).
        tokio::time::sleep(INACTIVE_TORRENT_REMOVE_TIMEOUT + Duration::from_secs(30)).await;
        assert!(present(TEST_HASH).await, "pinned engine must survive");
        assert!(
            !present(OTHER_HASH).await,
            "idle unpinned engine is removed"
        );
        assert_eq!(*removed.lock().unwrap(), vec![OTHER_HASH.to_string()]);
        assert_eq!(enginefs.pinned_downloads().await.len(), 1);

        // Still pinned after another window.
        tokio::time::sleep(INACTIVE_TORRENT_REMOVE_TIMEOUT + Duration::from_secs(30)).await;
        assert!(present(TEST_HASH).await);

        // Unpinned: swept on the next idle window like any other engine.
        assert!(
            enginefs
                .unpin_download(TEST_HASH, 1, false)
                .await
                .unwrap()
                .unpinned
        );
        tokio::time::sleep(INACTIVE_TORRENT_REMOVE_TIMEOUT + Duration::from_secs(30)).await;
        assert!(!present(TEST_HASH).await, "unpinned engine is removed");
        assert_eq!(
            *removed.lock().unwrap(),
            vec![OTHER_HASH.to_string(), TEST_HASH.to_string()]
        );
    }

    /// With seeding off the idle arm stops what nobody is watching -- and a
    /// pinned torrent is not that: somebody asked for it offline, and it
    /// keeps downloading.
    ///
    /// Two engines under one tick, so the assertion is a difference rather
    /// than a claim about a machine that had not got round to it: the
    /// unpinned one is stopped on the same pass that leaves the pinned one
    /// running.
    #[tokio::test(start_paused = true)]
    async fn the_idle_arm_stops_the_unpinned_torrent_and_leaves_the_pinned_one() {
        let TwoEngines { mut enginefs, .. } = test_enginefs_with_two_engines();
        if let Some(sweep) = enginefs.take_sweep_task() {
            sweep.abort();
        }
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        tokio::time::advance(INACTIVE_TORRENT_PAUSE_GRACE * 2).await;

        enginefs.reconcile_tick().await;

        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Live,
            "the pinned torrent keeps downloading"
        );
        assert_eq!(
            run_state_of(&enginefs, OTHER_HASH).await,
            RunState::Paused,
            "and the one nobody asked for is stopped on the same pass"
        );
    }

    // --- season-pack episode guessing (server.js guessFileIdx parity) ---

    fn series(season: usize, episode: usize) -> crate::engine::SeriesInfo {
        crate::engine::SeriesInfo {
            season: Some(season),
            episode: Some(episode),
        }
    }

    async fn guess_with(
        files: &[(&str, u64)],
        series: Option<crate::engine::SeriesInfo>,
    ) -> Option<usize> {
        let (enginefs, _counters) = test_enginefs_with_files(
            files
                .iter()
                .map(|(name, length)| (name.to_string(), *length))
                .collect(),
        );
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        engine.guess_file_index(series.as_ref()).await
    }

    #[tokio::test]
    async fn guess_picks_matching_episode_over_larger_files() {
        let files = [
            ("Show.S01E01.1080p.mkv", 5_000),
            ("Show.S01E02.1080p.mkv", 1_000),
            ("Show.S01E03.1080p.mkv", 8_000),
        ];
        assert_eq!(guess_with(&files, Some(series(1, 2))).await, Some(1));
    }

    #[tokio::test]
    async fn guess_matches_1x02_notation() {
        let files = [
            ("show.1x01.mkv", 4_000),
            ("show.1x02.mkv", 100),
            ("show.1x03.mkv", 6_000),
        ];
        assert_eq!(guess_with(&files, Some(series(1, 2))).await, Some(1));
    }

    #[tokio::test]
    async fn guess_is_case_insensitive() {
        let files = [("SHOW.S01E01.MKV", 9_000), ("SHOW.s01E02.MKV", 10)];
        assert_eq!(guess_with(&files, Some(series(1, 2))).await, Some(1));
    }

    #[tokio::test]
    async fn guess_breaks_size_ties_on_lowest_index() {
        let files = [
            ("intro.mkv", 500),
            ("Show.S01E02.CUT-A.mkv", 1_000),
            ("Show.S01E02.CUT-B.mkv", 1_000),
        ];
        assert_eq!(guess_with(&files, Some(series(1, 2))).await, Some(1));
    }

    #[tokio::test]
    async fn guess_prefers_larger_file_among_matching_episodes() {
        let files = [
            ("Show.S01E02.480p.mkv", 1_000),
            ("Show.S01E02.1080p.mkv", 4_000),
        ];
        assert_eq!(guess_with(&files, Some(series(1, 2))).await, Some(1));
    }

    #[tokio::test]
    async fn guess_falls_back_to_largest_media_when_no_episode_matches() {
        let files = [
            ("Show.S01E01.mkv", 2_000),
            ("Show.S01E02.mkv", 7_000),
            ("notes.txt", 90_000),
        ];
        assert_eq!(guess_with(&files, Some(series(9, 9))).await, Some(1));
    }

    #[tokio::test]
    async fn guess_never_picks_non_media_files() {
        let files = [
            ("readme.txt", 900_000),
            ("Show.S01E02.nfo", 800_000),
            ("Show.S01E02.mkv", 10),
        ];
        assert_eq!(guess_with(&files, Some(series(1, 2))).await, Some(2));
        assert_eq!(guess_with(&files, None).await, Some(2));
    }

    #[tokio::test]
    async fn guess_returns_none_without_any_media_file() {
        let files = [("readme.txt", 900_000), ("cover.jpg", 5_000)];
        assert_eq!(guess_with(&files, Some(series(1, 2))).await, None);
        assert_eq!(guess_with(&files, None).await, None);
    }

    #[tokio::test]
    async fn empty_hints_do_not_trigger_episode_tag_matching() {
        // guessFileIdx: {} (movies) must pick the largest media file, not a
        // small file whose name happens to carry a resolution like 1920x1080.
        let files = [
            ("sample.1920x1080.mkv", 100),
            ("Movie.2024.1080p.mkv", 9_000),
        ];
        assert_eq!(
            guess_with(&files, Some(crate::engine::SeriesInfo::default())).await,
            Some(1)
        );
    }

    /// What `GatedBackend::add_torrent` does with the next add.
    #[derive(Clone, Copy)]
    enum AddBehaviour {
        /// Block until `release` is notified, then hand out the fake handle.
        WaitForRelease,
        /// Fail immediately with this message.
        Fail(&'static str),
        /// Panic inside the add task.
        Panic,
    }

    /// A backend whose `add_torrent` blocks until released, standing in for
    /// librqbit resolving a magnet's metadata inside `Session::add_torrent`;
    /// it can also be told to fail or panic instead.
    struct GatedBackend {
        handle: FakeHandle,
        release: Arc<tokio::sync::Notify>,
        adds: Arc<AtomicUsize>,
        behaviour: Arc<Mutex<AddBehaviour>>,
        /// Info hashes `remove_torrent` was asked to drop.
        removed: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl TorrentBackend for GatedBackend {
        type Handle = FakeHandle;

        async fn add_torrent(
            &self,
            _source: TorrentSource,
            _trackers: Vec<String>,
        ) -> Result<Self::Handle> {
            self.adds.fetch_add(1, Ordering::SeqCst);
            let behaviour = *self.behaviour.lock().unwrap();
            match behaviour {
                AddBehaviour::WaitForRelease => {
                    self.release.notified().await;
                    Ok(self.handle.clone())
                }
                AddBehaviour::Fail(message) => Err(anyhow::anyhow!(message)),
                AddBehaviour::Panic => panic!("fake backend add panicked"),
            }
        }

        async fn get_torrent(&self, _info_hash: &str) -> Option<Self::Handle> {
            None
        }

        /// Records the request and answers like librqbit does for a torrent
        /// it never got to insert -- the usual case after a timeout, which
        /// the caller must tolerate.
        async fn remove_torrent(&self, info_hash: &str) -> Result<()> {
            self.removed.lock().unwrap().push(info_hash.to_string());
            Err(anyhow::anyhow!("torrent {info_hash} not found"))
        }

        async fn list_torrents(&self) -> Vec<String> {
            Vec::new()
        }

        async fn memory_diagnostics(&self) -> BackendMemoryDiagnostics {
            BackendMemoryDiagnostics::default()
        }
    }

    struct Gated {
        enginefs: Arc<BackendEngineFS<GatedBackend>>,
        release: Arc<tokio::sync::Notify>,
        adds: Arc<AtomicUsize>,
        behaviour: Arc<Mutex<AddBehaviour>>,
        removed: Arc<Mutex<Vec<String>>>,
        _root: tempfile::TempDir,
    }

    impl Gated {
        fn adds(&self) -> usize {
            self.adds.load(Ordering::SeqCst)
        }

        fn removed(&self) -> Vec<String> {
            self.removed.lock().unwrap().clone()
        }

        fn set_behaviour(&self, behaviour: AddBehaviour) {
            *self.behaviour.lock().unwrap() = behaviour;
        }
    }

    fn gated_enginefs() -> Gated {
        let release = Arc::new(tokio::sync::Notify::new());
        let adds = Arc::new(AtomicUsize::new(0));
        let behaviour = Arc::new(Mutex::new(AddBehaviour::WaitForRelease));
        let removed = Arc::new(Mutex::new(Vec::new()));
        let handle = FakeHandle {
            info_hash: TEST_HASH.to_string(),
            counters: Arc::new(FakeCounters::default()),
            files: vec![BackendFileInfo {
                name: "video.mkv".to_string(),
                length: 100,
            }],
            init: FakeInit::new(true, Duration::from_secs(1)),
        };
        let root = tempfile::tempdir().unwrap();
        let enginefs = BackendEngineFS::new_with_backend(
            GatedBackend {
                handle,
                release: release.clone(),
                adds: adds.clone(),
                behaviour: behaviour.clone(),
                removed: removed.clone(),
            },
            HashMap::new(),
            root.path().join("cache"),
            root.path().join("downloads"),
        );
        Gated {
            enginefs: Arc::new(enginefs),
            release,
            adds,
            behaviour,
            removed,
            _root: root,
        }
    }

    /// While the backend is still adding a magnet there is no engine, but the
    /// add is observable (with its tracker list) and shared: a second request
    /// for the same hash joins it instead of starting a duplicate resolution.
    /// Once the backend returns, the engine is published and the pending entry
    /// is gone.
    #[tokio::test]
    async fn magnet_add_is_observable_and_shared_until_the_backend_returns() {
        let gated = gated_enginefs();
        let enginefs = &gated.enginefs;
        let extra = "udp://extra.invalid:6969/announce".to_string();

        let first = enginefs
            .get_or_begin_add_magnet(&TEST_HASH.to_uppercase(), Some(vec![extra.clone()]))
            .await;
        let EngineLookup::Adding(first) = first else {
            panic!("expected an in-flight add");
        };
        assert!(first.trackers.contains(&extra), "{:?}", first.trackers);
        assert!(enginefs.get_engine(TEST_HASH).await.is_none());
        let pending = enginefs
            .pending_magnet_add(TEST_HASH)
            .await
            .expect("pending add is visible");
        assert_eq!(pending.trackers, first.trackers);

        // Another caller (say, the stream route) joins the same add.
        let second = enginefs
            .get_or_begin_add_magnet(TEST_HASH, Some(vec!["udp://late.invalid/announce".into()]))
            .await;
        let EngineLookup::Adding(second) = second else {
            panic!("expected to join the in-flight add");
        };
        assert!(
            wait_until(TEST_WAIT_BOUND, || gated.adds() == 1).await,
            "backend add started once"
        );

        gated.release.notify_one();
        let engine = first.done.await.expect("add succeeds");
        let joined = second.done.await.expect("shared add succeeds");
        assert!(Arc::ptr_eq(&engine, &joined));
        assert_eq!(engine.info_hash, TEST_HASH);
        assert!(enginefs.get_engine(TEST_HASH).await.is_some());
        // The task clears its pending entry right after publishing the engine.
        let deadline = tokio::time::Instant::now() + TEST_WAIT_BOUND;
        while enginefs.pending_magnet_add(TEST_HASH).await.is_some() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "pending entry is removed after the engine is published"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(gated.adds(), 1);
        assert!(gated.removed().is_empty(), "{:?}", gated.removed());

        // Now the engine is simply found.
        let EngineLookup::Ready(ready) = enginefs.get_or_begin_add_magnet(TEST_HASH, None).await
        else {
            panic!("expected the existing engine");
        };
        assert!(Arc::ptr_eq(&ready, &engine));
    }

    /// The blocking variant used by stream routes: two concurrent waiters get
    /// the same engine from one backend add. Bounded so a regression (say, a
    /// second add started for the second waiter) fails instead of hanging:
    /// `notify_waiters` releases every parked add at once, and the awaits
    /// are under (virtual-time) timeouts.
    #[tokio::test(start_paused = true)]
    async fn concurrent_get_or_add_magnet_waiters_share_one_add() {
        let gated = gated_enginefs();
        let enginefs = gated.enginefs.clone();

        let a = spawn_get_or_add(&enginefs, None);
        let b = spawn_get_or_add(&enginefs, None);
        assert!(
            wait_until(Duration::from_secs(1), || gated.adds() == 1).await,
            "exactly one backend add is started for both waiters"
        );
        // Both waiters are parked on that one add (current-thread runtime:
        // the add task registered its `notified()` in the same poll that
        // bumped the counter, so `notify_waiters` below reaches it).
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!a.is_finished() && !b.is_finished());
        assert_eq!(gated.adds(), 1);
        gated.release.notify_waiters();

        let bound = Duration::from_secs(5);
        let a = tokio::time::timeout(bound, a)
            .await
            .expect("first waiter finishes")
            .unwrap()
            .expect("first waiter");
        let b = tokio::time::timeout(bound, b)
            .await
            .expect("second waiter finishes")
            .unwrap()
            .expect("second waiter");
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(gated.adds(), 1);
    }

    /// `Result::expect_err` without `Engine: Debug`.
    fn expect_add_error(
        result: Result<Arc<Engine<FakeHandle>>, MagnetAddError>,
        why: &str,
    ) -> MagnetAddError {
        match result {
            Err(error) => error,
            Ok(engine) => panic!("{why}: unexpectedly got engine {}", engine.info_hash),
        }
    }

    fn spawn_get_or_add(
        enginefs: &Arc<BackendEngineFS<GatedBackend>>,
        trackers: Option<Vec<String>>,
    ) -> tokio::task::JoinHandle<Result<Arc<Engine<FakeHandle>>, MagnetAddError>> {
        let efs = enginefs.clone();
        tokio::spawn(async move { efs.get_or_add_magnet(TEST_HASH, trackers).await })
    }

    /// An add the backend never answers is given up on after
    /// `METADATA_RESOLVE_TIMEOUT`: waiters get the typed timeout error,
    /// non-blocking lookups a failure record (with the trackers the add ran
    /// with) rather than an eternal in-flight add, and only a blocking caller
    /// starts a fresh attempt.
    #[tokio::test(start_paused = true)]
    async fn magnet_add_times_out_and_leaves_a_retryable_failure_record() {
        let gated = gated_enginefs();
        let enginefs = &gated.enginefs;
        let extra = "udp://extra.invalid:6969/announce".to_string();

        let started = tokio::time::Instant::now();
        let waiter = spawn_get_or_add(enginefs, Some(vec![extra.clone()]));
        assert!(wait_until(Duration::from_secs(1), || gated.adds() == 1).await);

        let error = expect_add_error(waiter.await.unwrap(), "the backend never answers");
        assert!(
            matches!(
                &error,
                MagnetAddError::MetadataTimeout { info_hash, timeout }
                    if info_hash == TEST_HASH && *timeout == METADATA_RESOLVE_TIMEOUT
            ),
            "{error:?}"
        );
        let waited = started.elapsed();
        assert!(
            waited >= METADATA_RESOLVE_TIMEOUT && waited < METADATA_RESOLVE_TIMEOUT * 2,
            "waited {waited:?}"
        );
        assert_eq!(gated.adds(), 1);

        // Pollers see the failure, not a stuck add, and do not retry.
        assert!(enginefs.pending_magnet_add(TEST_HASH).await.is_none());
        let EngineLookup::Failed(failed) = enginefs.get_or_begin_add_magnet(TEST_HASH, None).await
        else {
            panic!("expected the failure record");
        };
        assert!(matches!(
            failed.error,
            MagnetAddError::MetadataTimeout { .. }
        ));
        assert!(failed.trackers.contains(&extra), "{:?}", failed.trackers);
        assert_eq!(
            enginefs
                .failed_magnet_add(TEST_HASH)
                .await
                .map(|f| f.error.to_string()),
            Some(failed.error.to_string())
        );
        assert_eq!(gated.adds(), 1);

        // A blocking caller retries with a fresh backend add, which is shared
        // and observable like the first one.
        let retry = spawn_get_or_add(enginefs, None);
        assert!(wait_until(Duration::from_secs(1), || gated.adds() == 2).await);
        assert!(matches!(
            enginefs.get_or_begin_add_magnet(TEST_HASH, None).await,
            EngineLookup::Adding(_)
        ));
        assert!(enginefs.failed_magnet_add(TEST_HASH).await.is_none());
        gated.release.notify_one();
        let engine = retry.await.unwrap().expect("retry succeeds");
        assert_eq!(engine.info_hash, TEST_HASH);
        assert!(
            wait_until(Duration::from_secs(1), || {
                enginefs
                    .magnet_adds
                    .try_read()
                    .is_ok_and(|adds| adds.is_empty())
            })
            .await,
            "a successful add leaves no registry entry behind"
        );
    }

    /// A superseded add's supervisor must not touch its successor's registry
    /// entry. When the idle sweep aborts an in-flight add (abort + entry
    /// removed under the registry lock), a new lookup for the same hash can
    /// register a fresh add before the old supervisor observes the
    /// cancellation; its `pending.id == id` check is what keeps it from
    /// marking that fresh add failed. Current-thread runtime: neither the
    /// aborted task nor its supervisor runs until this test yields, and the
    /// uncontended lock acquisitions in between do not.
    #[tokio::test(start_paused = true)]
    async fn superseded_add_supervisor_leaves_the_new_entry_alone() {
        let gated = gated_enginefs();
        let enginefs = &gated.enginefs;

        let EngineLookup::Adding(old) = enginefs.get_or_begin_add_magnet(TEST_HASH, None).await
        else {
            panic!("expected an in-flight add");
        };
        assert!(wait_until(Duration::from_secs(1), || gated.adds() == 1).await);

        // What the sweep does to an idle in-flight add.
        old.abort.abort();
        enginefs.magnet_adds.write().await.remove(TEST_HASH);

        // Re-issued before the old supervisor has settled: a new add.
        let EngineLookup::Adding(new) = enginefs.get_or_begin_add_magnet(TEST_HASH, None).await
        else {
            panic!("expected a fresh in-flight add");
        };
        assert_ne!(new.id, old.id);

        // Now let the old supervisor run to completion.
        let error = expect_add_error(old.done.clone().await, "the old add was aborted");
        assert!(
            matches!(&error, MagnetAddError::Cancelled { info_hash } if info_hash == TEST_HASH),
            "{error:?}"
        );

        // The new entry is untouched by it...
        let pending = enginefs
            .pending_magnet_add(TEST_HASH)
            .await
            .expect("the new add is still in flight");
        assert_eq!(pending.id, new.id);
        assert!(enginefs.failed_magnet_add(TEST_HASH).await.is_none());
        assert!(wait_until(Duration::from_secs(1), || gated.adds() == 2).await);

        // ...and completes normally.
        gated.release.notify_one();
        let engine = new.done.clone().await.expect("the new add succeeds");
        assert_eq!(engine.info_hash, TEST_HASH);
        assert!(enginefs.get_engine(TEST_HASH).await.is_some());
        assert!(
            wait_until(Duration::from_secs(1), || {
                enginefs
                    .magnet_adds
                    .try_read()
                    .is_ok_and(|adds| adds.is_empty())
            })
            .await,
            "the successful add leaves no registry entry behind"
        );
    }

    /// librqbit's `add_torrent` is not cancel-safe: the torrent can be sitting
    /// in the session, inserted but never started, when the timeout drops the
    /// add future. The timed-out add must therefore ask the backend to remove
    /// the hash (tolerating "not found", the usual answer) so a retry does not
    /// hit `AlreadyManaged` on a torrent that will never resolve.
    #[tokio::test(start_paused = true)]
    async fn metadata_timeout_removes_the_half_added_torrent_from_the_backend() {
        let gated = gated_enginefs();
        let enginefs = &gated.enginefs;

        let error = expect_add_error(
            enginefs.get_or_add_magnet(TEST_HASH, None).await,
            "the backend never answers",
        );
        assert!(
            matches!(&error, MagnetAddError::MetadataTimeout { .. }),
            "{error:?}"
        );
        assert_eq!(gated.removed(), [TEST_HASH.to_string()]);
        assert!(matches!(
            enginefs.get_or_begin_add_magnet(TEST_HASH, None).await,
            EngineLookup::Failed(_)
        ));
    }

    /// A backend add that panics (debug builds; release aborts the process)
    /// must not leave the hash stuck in `resolvingMetadata` forever: the
    /// waiter gets a typed error, the
    /// registry holds a failure record, and the next blocking attempt starts
    /// a fresh add.
    #[tokio::test]
    async fn panicking_magnet_add_leaves_a_failure_record_instead_of_a_stuck_add() {
        let gated = gated_enginefs();
        let enginefs = &gated.enginefs;
        gated.set_behaviour(AddBehaviour::Panic);

        let error = expect_add_error(
            enginefs.get_or_add_magnet(TEST_HASH, None).await,
            "the add task panicked",
        );
        assert!(
            matches!(&error, MagnetAddError::TaskFailed { info_hash, reason }
                if info_hash == TEST_HASH && reason.contains("panicked")),
            "{error:?}"
        );
        assert!(enginefs.pending_magnet_add(TEST_HASH).await.is_none());
        assert!(matches!(
            enginefs.get_or_begin_add_magnet(TEST_HASH, None).await,
            EngineLookup::Failed(_)
        ));
        assert_eq!(gated.adds(), 1);

        gated.set_behaviour(AddBehaviour::WaitForRelease);
        gated.release.notify_one(); // stored as a permit for the next add
        let engine = enginefs
            .get_or_add_magnet(TEST_HASH, None)
            .await
            .expect("a fresh add is started");
        assert_eq!(engine.info_hash, TEST_HASH);
        assert_eq!(gated.adds(), 2);
    }

    /// Failure records are kept while something keeps asking about the hash
    /// and swept by the eviction loop once nothing has for the inactivity
    /// window, after which a lookup starts over instead of reporting the
    /// stale failure.
    #[tokio::test(start_paused = true)]
    async fn idle_magnet_add_failure_records_are_swept() {
        let gated = gated_enginefs();
        let enginefs = &gated.enginefs;
        gated.set_behaviour(AddBehaviour::Fail("no peers"));

        let error = expect_add_error(
            enginefs.get_or_add_magnet(TEST_HASH, None).await,
            "the backend refuses",
        );
        assert!(
            matches!(&error, MagnetAddError::Backend { .. })
                && error.to_string().contains("no peers"),
            "{error:?}"
        );
        gated.set_behaviour(AddBehaviour::WaitForRelease);

        // Polled again before the window elapses: still the same record.
        tokio::time::sleep(INACTIVE_TORRENT_REMOVE_TIMEOUT - Duration::from_secs(30)).await;
        assert!(matches!(
            enginefs.get_or_begin_add_magnet(TEST_HASH, None).await,
            EngineLookup::Failed(_)
        ));
        assert_eq!(gated.adds(), 1);

        // The poll above refreshed it, so it survives another near-window.
        tokio::time::sleep(INACTIVE_TORRENT_REMOVE_TIMEOUT - Duration::from_secs(30)).await;
        assert!(enginefs.failed_magnet_add(TEST_HASH).await.is_some());

        // Nobody asks for a full window: swept, and the next lookup starts over.
        tokio::time::sleep(INACTIVE_TORRENT_REMOVE_TIMEOUT + Duration::from_secs(30)).await;
        assert!(
            enginefs.magnet_adds.read().await.is_empty(),
            "idle failure record was not swept"
        );
        assert!(matches!(
            enginefs.get_or_begin_add_magnet(TEST_HASH, None).await,
            EngineLookup::Adding(_)
        ));
        assert!(wait_until(Duration::from_secs(1), || gated.adds() == 2).await);
    }

    #[test]
    fn resolving_metadata_stats_describe_a_torrent_without_metadata() {
        let stats =
            EngineStats::resolving_metadata(TEST_HASH, &["udp://one.invalid/announce".to_string()]);
        assert_eq!(stats.info_hash, TEST_HASH);
        assert_eq!(stats.phase, StartupPhase::ResolvingMetadata);
        assert!(!stats.has_metadata);
        assert!(stats.files.is_empty());
        assert_eq!(stats.sources.len(), 1);
        assert_eq!(stats.sources[0].url, "udp://one.invalid/announce");
        // Nothing is in flight before there is a piece map: `null`, not a
        // zeroed piece a client would render as "0 of 0 bytes".
        assert_eq!(stats.in_flight_piece, None);
        let json = serde_json::to_value(&stats).unwrap();
        assert_eq!(json["phase"], "resolvingMetadata");
        assert_eq!(json["hasMetadata"], false);
        assert_eq!(json["streamLen"], 0);
        assert_eq!(json["pieceLength"], serde_json::Value::Null);
        assert_eq!(json["inFlightPiece"], serde_json::Value::Null);
    }
}
