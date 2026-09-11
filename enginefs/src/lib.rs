use crate::engine::Engine;
use anyhow::Result;
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
/// disk with less than this free; the published cap keeps the cache out of
/// it (`cache_budget::cap_to_publish`); and the engine's reconciler stops a
/// torrent that is writing when the volume falls under it
/// ([`reconcile::desired`]'s free-space arm). The third is what makes the
/// other two hold. librqbit's storage writes the whole file it wants and
/// stops only at ENOSPC, which it treats as a fatal torrent error -- so
/// without the reconciler a torrent larger than the free space ran the
/// volume to zero in 40 s at full speed on the television that prompted
/// this, and with the volume at zero every other stream and the OS around
/// them failed too. It looks
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
/// (`Engine::refuse_reads_for_space`). The owners' slack passes normally
/// settle it well inside this -- the tick and the running-low bell both give
/// back everything nobody is playing, and the next reconcile starts the
/// torrent -- so a reader sees a buffering blip, not a failure. This is the
/// bound for a volume nothing here can free: a parked read that nothing will
/// complete is a player spinning for ever.
///
/// Counted per volume rather than per torrent, because what decides whether
/// a parked read has anything coming is the disk, not when this particular
/// torrent happened to be stopped on it.
pub const STOPPED_READ_STALL_BOUND: Duration = Duration::from_secs(20);
/// Free space that must remain on the download volume after a pinned file's
/// missing bytes are written; `pin_download` refuses below it
/// ([`PinDownloadError::InsufficientSpace`]). Re-pinning a complete file
/// needs nothing and is never refused.
pub const PIN_FREE_SPACE_MARGIN: u64 = 500 * 1024 * 1024;

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
/// How long after the reconciler last moved a torrent its *timer* will
/// leave it stopped -- the anti-flap dwell, applied in
/// [`BackendEngineFS::start_if_stopped`] and nowhere else.
///
/// The ladder's own line carries most of the hysteresis: the free-space arm
/// measures a stopped torrent against a higher line than a running one.
/// What is left over is an input that moves for reasons of its own around
/// that line -- a volume a neighbouring process is writing to, a probe that
/// is briefly unreadable -- and each crossing of it costs a swarm: a stop
/// drops every peer and a start re-announces to trackers that enforce a
/// minimum announce interval.
///
/// Fifteen seconds, which is how long this server waits before believing
/// that a torrent's conditions have really changed. It used to be spelt as
/// the idle arm's grace, because it was the same judgement; the idle arm is
/// gone -- what is playing is a value now
/// ([`crate::retention::live`]) and not a clock -- and the dwell keeps the
/// number under its own name. Neither a stop nor a playback start goes
/// through it: see [`BackendEngineFS::start_if_stopped`] for why each is
/// exempt.
pub(crate) const RECONCILE_MIN_DWELL: Duration = Duration::from_secs(15);

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

/// A name no other deletion in this process or an earlier one has used:
/// the time and a counter. See `BackendEngineFS::delete_dormant_download_data`,
/// which renames a directory to it before deleting it -- a clash with a
/// leftover a killed process did not finish would make that rename fail.
fn deletion_nonce() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

/// `Fn(path) -> u64` probe of the volume holding a path: available bytes
/// (`fs4::available_space`), or an identity (`volume_id`) telling two paths
/// on the same volume apart from two on different ones.
type VolumeProbe = Arc<dyn Fn(&std::path::Path) -> std::io::Result<u64> + Send + Sync>;

/// An identity of the volume holding `path`, equal for two paths on the
/// same volume: the device id on Unix; the path prefix (drive letter or
/// UNC share) on Windows, where std exposes no stable volume serial.
///
/// Public because the server asks the same question of the roots it caps --
/// a free-space cap is a statement about a volume, so roots on two volumes
/// cannot share one budget -- and two answers to "are these the same volume"
/// that disagree would be worse than either.
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
    /// Which entity the server is playing: one cell, written by
    /// [`Self::on_stream_start`] and by the proxy's reader, read by the
    /// reconciler's ladder, by every retention pass and by the task that
    /// drops the predecessor's slack. See [`crate::retention::live`].
    ///
    /// It replaced a `Option<(String, usize)>` cell of the same shape that
    /// was written by four callers, cleared by three, rolled back by a
    /// `Drop` and read by a diagnostics field: a *record* of the last file
    /// anything selected. This is the same shape used as a *decision* --
    /// one writer, no rollback, no clearer -- because a stream the server
    /// saw opened really did leave the previous one behind, whether or not
    /// the request that opened it survived.
    live: Arc<crate::retention::live::Live>,
    /// For multi-file torrents, only the latest requested file is allowed to be
    /// wanted at a time. Single-file torrents bypass this selector.
    active_multifile_files: Arc<RwLock<HashMap<String, MultiFileActiveSelection>>>,
    priority_generation: Arc<AtomicU64>,
    /// Optional disk cache for persisting completed files. No constructor
    /// populates it in the librqbit-only build, and nothing reads it either,
    /// so it is dead code today; kept for a future backend that wants it.
    #[allow(dead_code)]
    disk_cache: Option<Arc<disk_cache::DiskCacheManager>>,
    /// The user's sharing setting: with it on this server uploads all the
    /// time, and with it off only while a player is reading from it. See
    /// [`Self::apply_upload_switch`].
    seeding_enabled: Arc<AtomicBool>,
    /// Held across [`Self::apply_upload_switch`]'s reading and its write.
    upload_switch: Arc<tokio::sync::Mutex<()>>,
    /// Magnet adds still inside the backend's `add_torrent`, plus the failure
    /// records of ones that ended without an engine, keyed by info hash. See
    /// [`PendingMagnetAdd`] and [`FailedMagnetAdd`].
    magnet_adds: MagnetAddRegistry<B::Handle>,
    /// One lock per info hash serialising `pin_download` and
    /// `unpin_download` for the same torrent -- a pin that has to add the
    /// torrent must not be raced by an unpin that deletes its data; entries
    /// live only while a call holds or waits for them.
    pin_locks: parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Pins the embedder named for torrents the backend did not have at
    /// startup (see [`Self::apply_pins`]): held here for the life of the
    /// process and applied by whatever next puts an engine for the torrent
    /// in the registry ([`Self::register_engine`] -- a stream's add as much
    /// as a pin's), or dropped by `unpin_download`. Never held across an
    /// `.await`. Shared with [`EngineParts`], which is how the registration
    /// reaches it without `&self`.
    dormant_pins: DormantPins,
    /// Available-bytes probe for the free-space check in `pin_download` and
    /// for the reconciler's arm (`fs4::available_space`; tests substitute
    /// one). Both ask it about one folder: the piece store's root.
    free_space_probe: VolumeProbe,
    /// Epoch of every `*_secs` timestamp this instance and its engines keep.
    clock: Clock,
    /// The housekeeping sweep started by the constructor, kept so its owner
    /// can cancel it. See [`Self::take_sweep_task`].
    sweep_task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Rung by the tick's own reading of the volume whenever it is under
    /// the line, for the owners that hold disposable bytes and have no
    /// tick of their own. See [`crate::retention::SlackBell`].
    slack_bell: Arc<crate::retention::SlackBell>,
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
    /// What the server says the torrent-data volume may hold, written
    /// through [`Self::set_cache_budget`] and shared with every [`Engine`]
    /// this instance makes. Unknown until something has published one: see
    /// [`crate::retention`].
    budget: Arc<crate::retention::RetentionBudget>,
    /// Where the backend's piece stores register once their `init` has
    /// seeded them, and so where a torrent's held set is read and every
    /// unlink of a registered torrent's piece goes: the backend's own
    /// ([`TorrentBackend::store_registry`]), or an empty one over the
    /// download root for a backend that keeps none, so that a hash nothing
    /// registered answers "no store" -- unknown, never empty.
    registry: Arc<crate::piece_store::StoreRegistry>,
    /// Whether the embedder named a pin set at boot. The same `Arc` reaches
    /// every engine and every retention backing, because a condition half
    /// the process believes is worse than either answer: see
    /// [`crate::piece_store::PinsUnknown`].
    pins_unknown: Arc<crate::piece_store::PinsUnknown>,
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
    live: Arc<crate::retention::live::Live>,
    pins_unknown: Arc<crate::piece_store::PinsUnknown>,
    dormant_pins: DormantPins,
}

/// The pins waiting for their torrent to come back: see
/// `BackendEngineFS::dormant_pins`.
type DormantPins = Arc<parking_lot::Mutex<BTreeMap<String, std::collections::BTreeSet<usize>>>>;

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

/// The one file the server is playing, as the diagnostics report it. Read
/// off the liveness cell ([`crate::retention::live`]), which is where that
/// fact lives.
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

/// What the torrent cache holds and what nothing may take from it, as the
/// owners know it -- [`BackendEngineFS::cache_holdings`].
///
/// The replacement for a walk of the tree. Every byte under the piece store
/// is in `total_bytes`: a running store counts its own from the bits it
/// keeps, with no syscall at all, and what belongs to no store is `stat`ed
/// on demand. `protected_bytes` is the part a pin or a live window keeps,
/// which is what a caller shown "over the limit" needs in order to know
/// whether anything can be done about it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheHoldings {
    /// Occupancy of the whole piece store.
    pub total_bytes: u64,
    /// How much of it a pin or a live window keeps.
    pub protected_bytes: u64,
    /// How many files that is.
    pub protected_files: usize,
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
/// **It does not un-switch the liveness cell**, and there is nothing here
/// to roll back: the server saw a stream open on this file, so the file
/// that was playing before really is the one nobody is playing now, and a
/// request that died on its way to the disk gate does not bring it back.
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
                let mut selections = active_multifile_files.write().await;
                if selections
                    .get(&key.0)
                    .is_some_and(|selection| selection.file_idx == key.1)
                {
                    selections.remove(&key.0);
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
        // want-set back -- `apply_pins`, below -- the
        // reconciler must not start one, and the honest way to say so is
        // that it has not been settled yet
        // (`reconcile::Conditions::settled`). Every other engine is made by
        // an add, which carries its want-set with it.
        let restored_unsettled = backend.sets_piece_reclaim();
        let budget = Arc::new(crate::retention::RetentionBudget::default());
        // Nothing is playing in a process that has served nothing, and that
        // is what makes the first tick after a restart stop every unpinned
        // torrent the session restored.
        let live = Arc::new(crate::retention::live::Live::new());
        let pins_unknown: Arc<crate::piece_store::PinsUnknown> = Arc::default();
        let registry = backend.store_registry().unwrap_or_else(|| {
            Arc::new(crate::piece_store::StoreRegistry::new(
                crate::piece_store::StoreRoot::in_download_dir(&download_dir),
            ))
        });
        let mut engines_map = HashMap::new();
        for (hash, handle) in restored_handles {
            let engine = Engine::new_with_handle(
                handle,
                &hash,
                clock,
                volumes.clone(),
                budget.clone(),
                live.clone(),
                pins_unknown.clone(),
            );
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
            live: live.clone(),
            active_multifile_files: Arc::new(RwLock::new(HashMap::new())),
            priority_generation: Arc::new(AtomicU64::new(0)),
            disk_cache: None,
            seeding_enabled: Arc::new(AtomicBool::new(true)),
            upload_switch: Arc::new(tokio::sync::Mutex::new(())),
            magnet_adds: Arc::new(RwLock::new(HashMap::new())),
            pin_locks: parking_lot::Mutex::new(HashMap::new()),
            dormant_pins: Arc::new(parking_lot::Mutex::new(BTreeMap::new())),
            free_space_probe: Arc::new(|path| match declared_volume_space(path) {
                Some(bytes) => Ok(bytes),
                None => fs4::available_space(path),
            }),
            clock,
            sweep_task: parking_lot::Mutex::new(None),
            slack_bell: Arc::default(),
            reconcile_locks: Default::default(),
            volumes,
            budget,
            registry,
            pins_unknown,
        };

        let engines_clone = engines.clone();
        let backend_clone = efs.backend.clone();
        let active_streams_clone = efs.active_streams.clone();
        let active_file_streams_clone = efs.active_file_streams.clone();
        let live_clone = efs.live.clone();
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
                    // Cloned out and the guard dropped before the first
                    // `.await`: the registry is a write-preferring `RwLock`,
                    // and a read guard held across the three activity maps'
                    // locks below parks every writer -- an add, a removal --
                    // and every reader queued behind it, for as long as any
                    // of those maps is held.
                    let engines: Vec<(String, Arc<Engine<B::Handle>>)> = engines_clone
                        .read()
                        .await
                        .iter()
                        .map(|(hash, engine)| (hash.clone(), engine.clone()))
                        .collect();
                    for (hash, engine) in &engines {
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
                        // The torrent being played is never removed, and
                        // this is asked of the same cell the reconciler and
                        // the passes read rather than of a register a
                        // request happened to leave behind: removing the
                        // live torrent's engine takes the entity its window
                        // is drawn round out of the map with it.
                        let live_here = live_clone.is_torrent(hash);
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
                        } else if live_here {
                            Some("playing")
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
                            to_remove.push(engine.clone());
                        }
                    }
                }

                if !to_remove.is_empty() {
                    // Decided above under the read lock, across several
                    // awaits; removed here under the write lock, which is
                    // a later instant. What can happen in between is a
                    // stream opening on the very engine that was found
                    // idle: `on_stream_start` writes the cell, finds the
                    // engine (a `get_engine`, which touches it) and counts
                    // its stream, all of it after the reading above and
                    // before this guard. An unconditional `remove` here
                    // then took the torrent out from under that stream --
                    // its reads failed against a torrent the session no
                    // longer had, and the bytes it had just started reading
                    // went with the directory.
                    //
                    // So the removal is decided again, from the facts as
                    // they stand under the guard. The engine must still be
                    // the one that was read (a re-add meanwhile publishes
                    // another, which this pass knows nothing about), and
                    // still idle by the readings that need no other lock:
                    // the clock every lookup stamps, the engine's own reader
                    // count, the liveness cell and the pin set. Every writer
                    // of the three activity maps read above looks the engine
                    // up first, so a touch is the earliest trace any of them
                    // leaves, and a stamp at or after this sweep's `now`
                    // reads as an age of zero.
                    let mut write = engines_clone.write().await;
                    let mut removed = Vec::with_capacity(to_remove.len());
                    for engine in to_remove {
                        let hash = &engine.info_hash;
                        let current = write
                            .get(hash)
                            .is_some_and(|current| Arc::ptr_eq(current, &engine));
                        let age_secs = now.saturating_sub(
                            engine
                                .last_accessed
                                .load(std::sync::atomic::Ordering::SeqCst),
                        );
                        let still_idle = current
                            && !engine.is_pinned()
                            && engine
                                .active_streams
                                .load(std::sync::atomic::Ordering::SeqCst)
                                == 0
                            && age_secs > INACTIVE_TORRENT_REMOVE_TIMEOUT.as_secs()
                            && !live_clone.is_torrent(hash);
                        if !still_idle {
                            tracing::debug!(
                                info_hash = %hash,
                                current,
                                age_secs,
                                removed = false,
                                "an engine found idle was used before it could be removed; keeping it"
                            );
                            continue;
                        }
                        debug!(info_hash = %hash, "Auto-removing inactive engine");
                        write.remove(hash);
                        removed.push(engine.info_hash.clone());
                    }
                    drop(write);

                    // Actually stop the torrents in the backend session,
                    // and take their bytes with them: an engine nothing has
                    // asked about for five minutes is one whose entities
                    // the slack passes have already emptied, and what it
                    // leaves behind is a directory nothing in this process
                    // has a deleter for once its store is gone.
                    for hash in removed {
                        if let Err(e) = backend_clone.remove_torrent_and_files(&hash).await {
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
    /// **One reading of what is being played, for the whole pass.** The
    /// ladder and the retention pass both consult it, and a value that
    /// moved between the two would have the reconciler start a torrent that
    /// the pass then emptied, or stop one whose window the pass had just
    /// measured. See [`crate::retention::live::Reading`].
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
        // The volume is the session's, not any one torrent's, so the tick
        // reads it before it looks at what there is to decide about -- and
        // reads it even when there is nothing. Taken inside the loop it was
        // skipped entirely by a session with no engines, which is exactly
        // the session the bell it rings exists for: a viewer who only ever
        // proxies, and one whose torrents the sweep has all removed, both
        // hold cache and never open a torrent.
        self.probe_volume(self.volumes.data_folder(), now).await;
        // Where the upload switch turns off: a stream ends in more than one
        // place (the response's end, the last reader's drop), and this
        // reads both registers without either having to remember to ask.
        self.apply_upload_switch().await;
        let mut probed = true;
        let mut decisions = Vec::with_capacity(engines.len());
        // Taken once, before the first engine, for the retention pass: the
        // modes it derives for the files of one tick must agree with each
        // other, so they come off one reading. The ladder does **not** read
        // it -- see `reconcile_engine`, which asks the cell under the hash
        // lock, because a copy taken before the first engine is a tick old
        // by the last one and a ladder that stops from it stops the torrent
        // the viewer has just opened.
        let live = self.live.reading();
        for engine in engines {
            if let Some(decision) = self
                .reconcile_engine(&engine, crate::reconcile::Trigger::Timer, now, &mut probed)
                .await
            {
                decisions.push((engine.info_hash.clone(), decision));
            }
            // The retention pass rides this tick rather than a timer of its
            // own: it is the same interval, over the same engines, and it
            // costs a copy of the store's held bits for a torrent something
            // is actually reading and a `None` for every other.
            self.retain_engine(&engine, &live).await;
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
        self.reconcile_engine(&engine, trigger, now, &mut probed)
            .await
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
    /// only for the one thing that is a statement about the *device*: the
    /// read refusal. The stop call itself is the same call.
    async fn reconcile_engine(
        &self,
        engine: &Arc<Engine<B::Handle>>,
        trigger: crate::reconcile::Trigger,
        now: u64,
        probed: &mut bool,
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
            self.probe_volume(&folder, now).await;
        }
        let conditions = crate::reconcile::Conditions {
            run_state: engine.handle.run_state(),
            settled: engine.is_settled(),
            // Playing is the liveness value, plus the reads still being
            // delivered off this torrent: a body mid-flight is not what a
            // viewer is watching, but stopping the torrent under one stalls
            // it. Neither is a clock, and neither is an activity register a
            // request left behind.
            //
            // **The cell itself, under this hash's lock -- never the tick's
            // copy of it.** The timer takes one reading before its first
            // engine and then spends the tick on the engines before this
            // one, and a pass on the predecessor can hold it inside a
            // backend call for the length of the switch task's unlink.
            // The torrent a viewer opened meanwhile has its cell written
            // (`on_stream_start` does that first) and no byte delivered
            // yet, so `readers()` is 0; read from the copy it is a torrent
            // nobody is playing, the idle arm answers `Stop`, and the
            // timer's next `Run` for it waits out `RECONCILE_MIN_DWELL`
            // -- the episode just started, parked for fifteen seconds.
            playing: self.live.is_torrent(&engine.info_hash) || engine.retention.readers() > 0,
            pinned: engine.is_pinned(),
            has_metadata: engine.handle.has_metadata().await,
            finished: engine.handle.is_finished().await,
            available: self.volumes.available(),
            // One lock read of the state librqbit already holds, like the
            // run state beside it. Only the `Error` arm reads it, and only
            // the error state can make it true.
            out_of_space: engine.handle.is_out_of_space().await,
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
            has_metadata = conditions.has_metadata,
            finished = conditions.finished,
            available = ?conditions.available,
            settled = conditions.settled,
            out_of_space = conditions.out_of_space,
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
                    self.after_stopping_for_space(engine, &conditions, now, stopped_here);
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
            crate::reconcile::Decision::RestartFromError => {
                self.restart_if_the_dwell_allows(engine, &conditions, now)
                    .await;
            }
            // `Error` with no way out of it, and a probe that failed on a
            // timer pass. Both are "no opinion", and lifting a refusal is
            // an opinion: a torrent the backend killed has its refusal
            // lifted by the restart above once the volume clears the line,
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
    /// A torrent that is stopped on a volume that has been short for
    /// [`STOPPED_READ_STALL_BOUND`] has its reads failed
    /// ([`Engine::refuse_reads_for_space`]): a read parked on a piece that
    /// is not being fetched is a player buffering with no end, and the
    /// bound is how long the owners' slack passes get to settle it first.
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
    ) {
        if stopped_here {
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

    /// Act on a [`crate::reconcile::Decision::RestartFromError`]: take the
    /// torrent out of the backend's error state, at most once per
    /// [`RECONCILE_MIN_DWELL`].
    ///
    /// **The dwell is the whole of what keeps this from being a loop**, and
    /// it binds whoever is asking -- a `PlaybackStart` included, which is
    /// the one place this differs from [`Self::start_if_stopped`]. A
    /// restart re-checks the storage and goes back to writing, so a restart
    /// onto a volume that is still filling errors again within seconds; a
    /// player retrying a dead stream would then ask for one restart per
    /// request, each of them a re-check of every piece on disk. Fifteen
    /// seconds between attempts is what the anti-flap dwell is already for,
    /// and the reading it is taken from is the one the backend's state
    /// machine and this engine already share, so there is no clock here of
    /// its own.
    ///
    /// The transition is recorded on success only: a restart the backend
    /// refused moved nothing, and holding the next attempt off for a dwell
    /// because of one would be remembering a failure.
    async fn restart_if_the_dwell_allows(
        &self,
        engine: &Arc<Engine<B::Handle>>,
        conditions: &crate::reconcile::Conditions,
        now: u64,
    ) {
        if let Some(moved_at) = engine.last_transition_at() {
            let since_transition = Duration::from_secs(now.saturating_sub(moved_at));
            if since_transition < RECONCILE_MIN_DWELL {
                debug!(
                    info_hash = %engine.info_hash,
                    since_secs = since_transition.as_secs(),
                    "not restarting an errored torrent this soon after the last time it was moved"
                );
                return;
            }
        }
        match self.restart_from_error(&engine.info_hash).await {
            Ok(true) => {
                engine.record_transition(now);
                tracing::info!(
                    info_hash = %engine.info_hash,
                    available = ?conditions.available,
                    "restarted a torrent a full volume had killed"
                );
            }
            // The state machine settled between the reading and the call:
            // the torrent is not in the error state any more, or the engine
            // is gone. Neither is a failure and neither is a move.
            Ok(false) => debug!(
                info_hash = %engine.info_hash,
                "the torrent was no longer in the backend's error state; nothing restarted"
            ),
            Err(error) => tracing::warn!(
                info_hash = %engine.info_hash,
                error = %format!("{error:#}"),
                "could not restart a torrent a full volume had stopped"
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
    /// a start are both ordinary: the torrent is stopped when the slack goes
    /// and frees the volume, so the reconcile that follows leaves it
    /// stopped and starts nothing; and the user then presses play,
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
    ///
    /// **And rings the running-low bell if the reading is under the line.**
    /// This is the reading every owner acts on, so it is where the noticing
    /// belongs; a second reading taken by whoever wanted to know would be a
    /// second opinion about one device. (The cache budget's minute
    /// publisher takes a reading of its own, and deliberately decides
    /// nothing from it -- it states a cap and rings nothing.) The line is the
    /// resume line rather than the floor, so the bell rings while the
    /// volume is still inside the band -- the slack has to be gone *before*
    /// the floor is reached, not after. A probe that failed rings nothing:
    /// an unreadable volume is not a full one.
    ///
    /// Off the worker ([`Self::free_space_of`]), like every probe here.
    async fn probe_volume(&self, folder: &std::path::Path, now: u64) {
        let available = match self.free_space_of(folder).await {
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
        if available.is_some_and(|available| available < crate::reconcile::resume_line()) {
            self.slack_bell.ring();
        }
        self.volumes.record(available, now);
    }

    /// The free space of the volume under `folder` (or its nearest existing
    /// ancestor), with the `statvfs` taken on the blocking pool.
    ///
    /// Every caller is on an async worker -- the tick, a stream request's
    /// reconcile, a pin -- and the device is the one that is filling up:
    /// a spun-down disk, a network mount that has stopped answering. Taken
    /// inline it parks the worker, and every stream and API task scheduled
    /// on it, for as long as the device does not answer.
    async fn free_space_of(&self, folder: &std::path::Path) -> std::io::Result<u64> {
        let probe = Arc::clone(&self.free_space_probe);
        let folder = folder.to_path_buf();
        tokio::task::spawn_blocking(move || probe_at_existing_ancestor(&*probe, &folder))
            .await
            .unwrap_or_else(|join| {
                Err(std::io::Error::other(format!(
                    "the volume probe task failed: {join}"
                )))
            })
    }

    /// Take a fresh reading of the volume the pieces land on, now, and ring
    /// the bell if it is short ([`Self::probe_volume`]).
    ///
    /// For a caller that has just given bytes back and must not be judged
    /// by the reading taken before it: the recorded one is at most one
    /// [`crate::reconcile::RECONCILE_INTERVAL`] old, which is a whole tick
    /// of a device the caller has just changed. The stream route's disk
    /// gate is the caller -- it drops every owner's slack and then asks
    /// [`Engine::is_stopped_for_space`] again, and that question is
    /// answered from this reading.
    ///
    /// **The `statvfs` is taken on the blocking pool, never inline.** This
    /// runs on a request's own task, and the device it reads is by
    /// construction one that has just run out of room -- a spun-down HDD,
    /// an SMB mount that has stopped answering. Taken where it stands it
    /// would park that worker thread, and with it every other stream and
    /// API task scheduled on it, which is the very thing
    /// `SLACK_DROP_BOUND` had just bounded the request's waiting for.
    pub async fn reread_volume(&self) {
        let folder = self.volumes.data_folder().to_path_buf();
        self.probe_volume(&folder, self.clock.now_secs()).await;
    }

    /// The bell the tick's reading of the volume rings when it is running
    /// low, for the owners that answer by dropping their slack. See
    /// [`crate::retention::SlackBell`].
    pub fn slack_bell(&self) -> &Arc<crate::retention::SlackBell> {
        &self.slack_bell
    }

    /// Whether anything is using this torrent right now: a response body
    /// open on it, a file stream, a multi-file selection, or a reader
    /// parked inside the engine. These were questions asked in three
    /// places -- the housekeeping sweep's idle pause, the per-stream
    /// grace-period task and this -- and the first two are gone: the
    /// ladder is the only thing that asks.
    ///
    /// **Nothing that decides whether a torrent runs reads them.** That was
    /// the reconciler's idle arm, and it is gone: a register is written by
    /// a request and ended by the request's guard, so one that outlived its
    /// request read as "playing" for the life of the process, and one that
    /// was ended too early stopped a torrent under a body still being
    /// delivered. What is playing is a value with one writer and no expiry
    /// ([`crate::retention::live`]); these count responses, for the
    /// activity light and the housekeeping sweep.
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
            live: self.live.clone(),
            pins_unknown: self.pins_unknown.clone(),
            dormant_pins: self.dormant_pins.clone(),
        }
    }

    /// Wrap a backend handle in an `Engine` and publish it, or return the
    /// engine already registered for the same info hash.
    ///
    /// **A new engine is pinned before it is visible.** The pins the
    /// embedder named for this torrent while the backend did not have it
    /// (`dormant_pins`) go into the engine's pin set under the registry's
    /// write lock, so there is no instant at which the registry holds an
    /// unpinned engine for a pinned torrent. There used to be: only
    /// `pin_download` applied them, and a *stream* of the torrent -- the
    /// ordinary way a torrent librqbit did not restore comes back -- went
    /// through here and published an engine whose `is_pinned()` was false.
    /// Its store was then seeded from the kept directory into an engine
    /// nothing protected: the passes reclaimed outside the window, the
    /// idle sweep removed the torrent and its files five minutes after
    /// the stream ended, and `cache_holdings` reported the bytes protected
    /// throughout, because the dormant record still named them.
    ///
    /// The handle's own copy of each pin (`TorrentHandle::pin_file`, what
    /// the want-set planner reads) is applied after the lock is dropped: a
    /// backend call under the registry's write lock parks every reader in
    /// the process behind it. The engine's pin set is what the sweep and
    /// the passes read, and that is what has to be there first; a pin the
    /// handle refuses -- a file index the torrent turns out not to have --
    /// is taken out of the set again, as `apply_pins` drops it.
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
            parts.live,
            parts.pins_unknown,
        ));
        let dormant = parts
            .dormant_pins
            .lock()
            .remove(&info_hash)
            .unwrap_or_default();
        if !dormant.is_empty() {
            engine.pinned_files.write().extend(dormant.iter().copied());
        }
        engines.insert(info_hash.clone(), engine.clone());
        drop(engines);
        for &file_idx in &dormant {
            if let Err(error) = engine.handle.pin_file(file_idx).await {
                tracing::warn!(info_hash, file_idx, %error, "could not re-apply a dormant pin");
                engine.pinned_files.write().remove(&file_idx);
            }
        }
        if !dormant.is_empty() {
            tracing::info!(
                info_hash,
                pinned = ?engine.pinned_file_indices(),
                "dormant pins applied to the torrent that came back"
            );
        }
        engine
    }

    /// Add a torrent from a `.torrent` blob or URL and publish its engine.
    pub async fn add_torrent(
        &self,
        source: TorrentSource,
        extra_trackers: Option<Vec<String>>,
    ) -> Result<Arc<Engine<B::Handle>>> {
        let trackers = self.merged_trackers(extra_trackers).await;
        debug!(count = trackers.len(), "Adding torrent with trackers");
        let handle = self.backend.add_torrent(source, trackers).await?;
        Ok(Self::register_engine(&self.engines, handle, self.engine_parts()).await)
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
                MagnetAddState::Failed(failed) if !retry_failed => {
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
    /// The whole of what bounds the streaming cache: a torrent playing at
    /// 20 MB/s writes a gigabyte a minute, so this runs on the reconciler's
    /// two-second tick, asks the policy where the playhead has left us, and
    /// gives back what the window no longer covers.
    ///
    /// A torrent the backend stopped with an **error** is removed with its
    /// files instead, when nobody is playing it and nobody has pinned it.
    /// An errored torrent holds no storage for a `drop_pieces` to edit, so
    /// the slack pass cannot delete a byte of it: `drop_pieces` bails, the
    /// pieces stay, and every later pass offers them again. Nothing else
    /// will ever come for them either -- the torrent announces nothing and
    /// the next start would rebuild its have-set from exactly those files.
    /// So this is their opportunity, and it is taken whole.
    ///
    /// **That one asks the liveness cell, not the tick's reading of it.**
    /// It is the largest delete in this process -- a torrent and every file
    /// of it, out from under whatever is holding them -- and the reading
    /// the tick started from can be a whole tick old by the time this
    /// engine's turn comes round. A viewer who starts a torrent the backend
    /// stopped with an error has `on_stream_start` write the cell and ask
    /// for a restart, and the restart has not happened yet: the run state
    /// still says `Error`, and a delete decided from the stale reading
    /// takes the files out from under the request that asked for them. The
    /// pass below keeps the reading, because the modes of one tick must
    /// agree with the ladder's; the delete asks again, as every delete
    /// here does.
    ///
    /// **And it holds the hash's pin lock while it removes.** A pin is the
    /// other thing that can say this torrent's bytes are wanted, and
    /// `pin_download` finds the engine in the registry, records the pin on
    /// it and answers `Ok`. Made outside the lock, this removal could be
    /// inside its backend call when that pin landed: the pin went onto an
    /// engine `remove_engine_if_current` then took out of the registry, and
    /// the user was told their download was kept while its torrent left the
    /// session and its files went with it. Under the lock the pin queues
    /// behind the removal, finds no engine, and adds the torrent again as a
    /// pin of an unmanaged torrent does. The pin set and the cell are asked
    /// again under the lock: no await separates the reading above from the
    /// `try_lock`, but a pin or a stream on another worker thread can land
    /// in between, and the lock is free again by the time it is taken.
    /// `try_lock`, because the tick may not wait: a pin holding the lock
    /// is resolving metadata, which is bounded by
    /// `METADATA_RESOLVE_TIMEOUT` and not by the tick, and the removal
    /// nothing is racing comes round again in two seconds.
    async fn retain_engine(
        &self,
        engine: &Arc<Engine<B::Handle>>,
        live: &crate::retention::live::Reading,
    ) {
        if engine.handle.run_state() == RunState::Error
            && !engine.is_pinned()
            && !self.live.is_torrent(&engine.info_hash)
        {
            let lock = self.pin_lock(&engine.info_hash);
            if let Ok(guard) = lock.try_lock() {
                self.remove_errored_engine_locked(engine).await;
                drop(guard);
            } else {
                debug!(
                    info_hash = %engine.info_hash,
                    "a pin or unpin of an errored torrent is in flight; its removal waits for the next tick"
                );
            }
            self.release_pin_lock(&engine.info_hash, lock);
            return;
        }
        let Some(pass) = engine.retain(&self.registry, live).await else {
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

    /// The Error arm of [`Self::retain_engine`], with the hash's pin lock
    /// held: the pin set and the cell asked once more under it, then the
    /// torrent and its files removed and the engine dropped from the
    /// registry -- while it is still this engine.
    async fn remove_errored_engine_locked(&self, engine: &Arc<Engine<B::Handle>>) {
        if engine.is_pinned() || self.live.is_torrent(&engine.info_hash) {
            debug!(
                info_hash = %engine.info_hash,
                "an errored torrent was pinned or opened before its removal took the lock; kept"
            );
            return;
        }
        tracing::info!(
            info_hash = %engine.info_hash,
            "removing an errored torrent nobody is playing and nobody pinned, with its files"
        );
        if let Err(error) = self
            .backend
            .remove_torrent_and_files(&engine.info_hash)
            .await
        {
            tracing::warn!(
                info_hash = %engine.info_hash,
                error = %format!("{error:#}"),
                "could not remove an errored torrent; its bytes stay until the next tick"
            );
            return;
        }
        self.remove_engine_if_current(engine).await;
    }

    /// Every entity nobody is playing and nobody is reading, taken off the
    /// disk now rather than at the next tick; the number of piece files
    /// that left it.
    ///
    /// Called where the answer is wanted at once: the moment a viewer opens
    /// something else (the switch task), the moment the volume runs low,
    /// the disk gate in front of a refused stream, and `POST /cache/clean`.
    /// It is the same pass the tick would run, so there is nothing here a
    /// tick would not have done -- what it buys is the two seconds between
    /// them, which on a switch is the difference between a stream that fits
    /// and a `507`.
    pub async fn drop_slack(&self) -> usize {
        let engines: Vec<_> = self.engines.read().await.values().cloned().collect();
        let mut reclaimed = 0;
        for engine in engines {
            if let Some(pass) = engine.drop_slack(&self.registry).await
                && pass != crate::retention::RetentionPass::default()
            {
                reclaimed += pass.reclaimed;
                debug!(
                    info_hash = %engine.info_hash,
                    reclaimed = pass.reclaimed,
                    "slack dropped"
                );
            }
        }
        reclaimed
    }

    /// The liveness cell: which entity this server is playing. Handed to
    /// the proxy cache, which writes it for a proxied body, and read by the
    /// task that drops the predecessor's slack. See
    /// [`crate::retention::live`].
    pub fn live(&self) -> &Arc<crate::retention::live::Live> {
        &self.live
    }

    /// What the server says the torrent-data volume may hold.
    ///
    /// **Pushed in, never recomputed.** The server's cap is
    /// `min(cacheSize, occupied + available - floor)`; a second reading of
    /// the same volume taken here would disagree with it, and the two
    /// halves of the cache would be sized against different numbers.
    ///
    /// The server states it through [`Self::cache_budget`] and its own
    /// ordered writer (`server::cache_budget::publish`) rather than here,
    /// because a publication that is not ordered against the other
    /// readings of the volume can put a stale cap over a fresh one. This
    /// is the unordered spelling, and what remains of it is the engine's
    /// own tests, which have one reading and no order to keep. `None` is the shape
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
    /// copy is what makes that structural -- there is one place the cap is
    /// written, and everything that reads it reads that.
    pub fn cache_budget(&self) -> Arc<crate::retention::RetentionBudget> {
        self.budget.clone()
    }

    /// What every registered store holds, in bytes, by the bits it keeps:
    /// no syscall, no listing, no walk.
    ///
    /// The disk arm of the published cap is sized from this
    /// (`server::cache_budget`), which is why it may not cost anything: it
    /// is read on a timer, and the figure it replaced was whatever an
    /// eviction pass had last counted -- 0 until the first walk of the root
    /// finished, which on a television with sixteen thousand cache files is
    /// minutes after the first stream opened.
    ///
    /// What it does **not** count is what no store speaks for: strays, and
    /// a torrent held in Error. Those are read by
    /// [`Self::cache_holdings`], on demand, because reading them costs a
    /// `read_dir`.
    pub fn cache_occupancy(&self) -> u64 {
        self.registry.occupancy()
    }

    /// What the piece store holds and what nothing may take from it, whole:
    /// [`Self::cache_occupancy`] plus the bytes no store speaks for, and
    /// the pins and live windows that keep part of it.
    ///
    /// The reading behind `GET /cache.json`. It is the on-demand half of
    /// the pair: one `read_dir` of the store root on the blocking pool for
    /// the unregistered bytes, and a copy-out per engine for the
    /// protections. Nothing here is on a tick, and nothing here walks the
    /// tree.
    ///
    /// Taken over several instants -- the registry's sum, then the
    /// directory listing, then the engines one after another -- so what it
    /// answers is a reading of a moving cache and not a transaction over
    /// it. That is what a usage figure has always been; what it no longer
    /// is, is minutes old.
    pub async fn cache_holdings(&self) -> CacheHoldings {
        let registry = self.registry.clone();
        // A `read_dir` of the root and a `stat` per unadopted directory:
        // filesystem work, and this is called from a request a worker is
        // serving.
        let unregistered = tokio::task::spawn_blocking(move || registry.unregistered_bytes())
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(%error, "the piece store's unregistered bytes could not be read");
                0
            });
        let mut holdings = CacheHoldings {
            total_bytes: self.cache_occupancy() + unregistered,
            ..CacheHoldings::default()
        };
        let engines: Vec<_> = self.engines.read().await.values().cloned().collect();
        for engine in engines {
            // No registration is no reading, never an empty one: a torrent
            // in Error holds no storage, and its bytes are in the
            // unregistered half above with nothing protecting them.
            let Some(held) = self.registry.held(&engine.info_hash.to_lowercase()) else {
                continue;
            };
            let protected = engine.protects(&held).await;
            holdings.protected_bytes += held.bytes_of(&protected.pieces);
            holdings.protected_files += protected.files.len();
        }
        // A dormant pin has no engine to speak for it -- that is what
        // dormant means -- and no store either, so its directory is in the
        // unregistered bytes above and nothing so far has protected a byte
        // of it. Nothing can ever take those bytes while the pin stands
        // (nothing but an unpin can, and the sweep at launch keeps the
        // pin set), so reporting them as
        // reclaimable tells a client a shortfall has a remedy it has not
        // got. The one `stat` per dormant pin is on the same blocking hop
        // the unregistered half already takes.
        let pins = self.dormant_pinned_downloads();
        let dormant: std::collections::BTreeSet<String> = pins
            .iter()
            .map(|pin| pin.info_hash.to_lowercase())
            .collect();
        if !dormant.is_empty() {
            let store = self.piece_store();
            let files = pins.len();
            let bytes = tokio::task::spawn_blocking(move || {
                dormant
                    .iter()
                    .map(|info_hash| store.stat(info_hash).occupancy())
                    .sum::<u64>()
            })
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(%error, "a dormant pin's bytes could not be read");
                0
            });
            holdings.protected_bytes += bytes;
            holdings.protected_files += files;
        }
        holdings
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

    /// The registry the session's piece stores report to -- see the field.
    pub fn store_registry(&self) -> &Arc<crate::piece_store::StoreRegistry> {
        &self.registry
    }

    /// Turn sharing while idle on or off: what the upload switch reads
    /// ([`Self::apply_upload_switch`]), applied before this returns.
    ///
    /// That is all it does. It used to ask the reconciler about every
    /// torrent as well, which decided nothing: the ladder governs whether a
    /// torrent runs, and no arm of it has read this setting since the idle
    /// arm went.
    pub async fn set_seeding_enabled(&self, enabled: bool) {
        self.seeding_enabled.store(enabled, Ordering::Relaxed);
        self.apply_upload_switch().await;
        tracing::info!(seeding_enabled = enabled, "Seeding policy updated");
    }

    pub fn seeding_enabled(&self) -> bool {
        self.seeding_enabled.load(Ordering::Relaxed)
    }

    /// Tell the backend whether to upload, from what is true now: always
    /// while the sharing setting is on, and with it off only while a
    /// player is reading from this server.
    ///
    /// **"A player is reading" is [`Self::playback_is_live`]**, the answer
    /// the activity light is drawn from, so the light can never show an
    /// upload the setting has ruled out: it is lit only while nothing is
    /// playing, and with the setting off that is exactly when nothing
    /// uploads. A player that is paused still holds its response open,
    /// so a paused film keeps sharing. While one is reading, every torrent
    /// uploads -- the one being watched and a title kept offline alike --
    /// because the switch is the session's: it chokes peers, it does not
    /// choose between torrents, and a rule that did would be the retention
    /// owner's advertise mask, which is not a thing to share.
    ///
    /// Only uploading is switched. Nothing is paused and no peer is
    /// dropped, so what a torrent downloads is still the ladder's and the
    /// retention owner's to decide, and turning sharing back on costs one
    /// Unchoke per peer.
    ///
    /// **Recomputed, never remembered**: asked where the answer can turn
    /// true (a stream opening, the setting moving) so sharing starts at
    /// once, and on every reconciler tick, which is where it turns false.
    /// Serialised, because the reading and the write are two steps with an
    /// await between them: a tick that read "nothing playing" just before
    /// a stream registered must not land its "off" after the open's "on".
    /// Under the lock the later caller reads the later registers.
    async fn apply_upload_switch(&self) {
        let _turn = self.upload_switch.lock().await;
        let enabled = self.seeding_enabled() || self.playback_is_live().await;
        self.backend.set_upload_enabled(enabled);
    }

    /// Put the torrent `info_hash` back to work after the backend stopped it
    /// with an **error** -- and only then. `false` when no engine holds that
    /// hash any more (it was swept while space was being reclaimed) and when
    /// the torrent is not in the error state; neither is a failure.
    ///
    /// **The reconciler is the caller**, off
    /// [`crate::reconcile::Decision::RestartFromError`]: an ENOSPC error on
    /// a volume that has room again, held to the dwell
    /// ([`Self::restart_if_the_dwell_allows`]). Restarting re-checks the
    /// torrent's storage and takes it live, which is the one transition the
    /// rest of the ladder cannot make -- `start_torrent` on a torrent in
    /// the error state starts nothing.
    ///
    /// A torrent the *reconciler* stopped is not this method's: it starts
    /// those itself on its next pass, from the volume reading it takes
    /// then, and having a second path unpause them from a reading nobody
    /// rechecked is the shape of bug this whole design is closing. So this
    /// refuses them, and the guard is the state machine's answer rather
    /// than any note about who stopped what.
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
    /// sweep just by looking at it. The window is counted off the held set
    /// the torrent's registered store keeps -- a copy of its bits, no
    /// listing.
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
        // A torrent with no registered store -- one in Error, or one whose
        // `init` has not seeded it -- has no held set to count, and that is
        // no numbers, not a window of zero.
        let held = self.registry.held(info_hash)?;
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
        let active_file =
            self.live
                .reading()
                .torrent()
                .map(|(info_hash, file_idx)| ActiveFileSnapshot {
                    info_hash: info_hash.to_string(),
                    file_idx,
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
        self.start_stream(info_hash, file_idx, true).await;
    }

    /// [`Self::on_stream_start`] without its `PlaybackStart` reconcile, for
    /// a caller that asks the reconciler itself once its disk gate has run
    /// (`routes::stream`, through [`Self::focus_torrent`]). The gate may
    /// free space, and the reconcile after it is the one that can start a
    /// torrent stopped at the floor; one before it as well was a second
    /// ladder and a second volume probe on every request, deciding nothing
    /// the later one would not.
    ///
    /// Cancel-safe and handed over exactly as `on_stream_start` is.
    pub async fn on_stream_start_unreconciled(&self, info_hash: &str, file_idx: usize) {
        self.start_stream(info_hash, file_idx, false).await;
    }

    async fn start_stream(&self, info_hash: &str, file_idx: usize, reconcile: bool) {
        let info_hash = info_hash.to_lowercase();
        // First, before anything this call could fail at: **the server saw
        // a stream open**, and that is the event the liveness cell records.
        // What was playing before is what nobody is playing now, whatever
        // becomes of this request -- the disk gate that refuses it for want
        // of space is refusing it *after* the predecessor became slack,
        // which is what gives it room to be admitted at all.
        let beside = self.switch_to(&info_hash, file_idx).await;
        let mut rollback = StreamStartRollback::armed(self, info_hash.clone(), file_idx);
        let native_lifecycle = self
            .get_engine(&info_hash)
            .await
            .is_some_and(|engine| engine.handle.manages_playback_lifecycle());
        if native_lifecycle {
            if let Some(engine) = self.get_engine(&info_hash).await {
                engine.touch();
            }
        } else {
            self.activate_file(&info_hash, file_idx, beside, "stream")
                .await;
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
        // Registered, so this reads a player: sharing starts with the
        // stream rather than at the next tick.
        self.apply_upload_switch().await;

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
        if reconcile {
            self.reconcile_hash(&info_hash, crate::reconcile::Trigger::PlaybackStart)
                .await;
        }

        // Handed over: from here the caller's guard owns the registration.
        rollback.handed_over();
    }

    /// Move the liveness cell onto `file_idx` of `info_hash`, unless this
    /// open is an aside -- in which case the file being played, which the
    /// open's selection has to keep ([`Self::activate_file`]).
    ///
    /// **The aside rule.** An open on another file of the torrent being
    /// played, while some read of the playing file is still open, is a
    /// subtitle or a side file fetched during playback -- not a viewer
    /// moving on. Without it every subtitle fetch would make the film slack
    /// and the next tick would delete it out from under the player, and
    /// there is no signal in the request to tell the two apart. Once no
    /// read of the playing file is left, the same open *is* a move: the
    /// viewer went to the next episode.
    ///
    /// The two readings it is made of are taken here, before the cell is
    /// written, because the cell's writer may not read a second lock under
    /// it ([`crate::retention::live::Live::open`]).
    ///
    /// **And they are taken after the engine is looked up, with nothing
    /// awaited between them and the write.** The lookup waits on the
    /// registry, and another open can move the cell in that wait. Read
    /// before it, the cell named a file that was no longer the one playing,
    /// and the count of its readers was asked of that file: a subtitle's
    /// open that had read "episode one, nobody reading it" took the cell
    /// off episode two, which had opened and begun delivering meanwhile.
    async fn switch_to(&self, info_hash: &str, file_idx: usize) -> Option<usize> {
        let engine = self.peek_engine(info_hash).await;
        let beside = match self.live.reading().file_of(info_hash) {
            Some(playing) if playing != file_idx => engine
                .is_some_and(|engine| engine.retention.readers_of(&playing) > 0)
                .then_some(playing),
            _ => None,
        };
        let keep_current = beside.is_some();
        let switch = self.live.open(
            crate::retention::live::LiveEntity::Torrent {
                info_hash: info_hash.to_string(),
                file_idx,
            },
            keep_current,
        );
        if let Some(switch) = switch {
            tracing::debug!(
                info_hash = %info_hash,
                file_idx,
                from = ?switch.from,
                "the live entity moved"
            );
        }
        beside
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

    /// `beside` is the file being played when this open is an aside to it
    /// ([`Self::switch_to`]): the selection keeps that file and adds this
    /// one. Planned from the open alone, the want-set was this file and the
    /// pins, so a subtitle fetched during a film took the film out of it --
    /// its window's fetch-ahead stopped, and librqbit called the torrent
    /// finished while the film's pieces were still being written.
    async fn activate_file(
        &self,
        info_hash: &str,
        file_idx: usize,
        beside: Option<usize>,
        source: &'static str,
    ) {
        let mut is_multifile = false;
        if let Some(engine) = self.get_engine(info_hash).await {
            engine.touch();
            if engine.handle.manages_playback_lifecycle() {
                return;
            }
            is_multifile = engine.handle.file_count().await > 1;

            // No resume here any more. Starting a torrent that is stopped
            // is the reconciler's, and it has to be asked *after* the
            // activity this call is part of is registered, or it reads a
            // torrent nobody is watching: the caller therefore asks it
            // itself once it has finished registering (`on_stream_start`).
        }

        if !is_multifile {
            return;
        }
        match beside {
            Some(playing) => {
                // What is fetched beside a film, not what plays: no offset,
                // priority or intent of it is read by any backend, only the
                // file.
                let aside = HotFilePriorityPlan {
                    file_idx,
                    start_offset: 0,
                    priority: 0,
                    intent: crate::backend::priorities::PlaybackIntent::Background,
                    bitrate_bytes_per_sec: None,
                };
                self.activate_multifile_file(info_hash, playing, Some(aside), source)
                    .await;
            }
            None => {
                self.activate_multifile_file(info_hash, file_idx, None, source)
                    .await;
            }
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

        // The other files' stream counts stay. They are counts of responses
        // still open, ended one by one by `on_stream_end`, and a selection
        // is not a response ending: wiping them here -- what this did, from
        // when one file per torrent was the rule -- left a film's count at
        // nothing while its subtitle's open selected it, or while the next
        // episode was opened before the last one's body closed.
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
    /// Not persisted here: the embedder keeps the record and hands the set
    /// back at the next startup, where [`Self::apply_pins`] re-applies it to
    /// the torrents the backend restored (librqbit keeps the file in its
    /// persisted `only_files`, so the download itself resumes; the pin makes
    /// it exempt from eviction again). Pins that found no torrent stay
    /// dormant for the run and come back with the torrent: a pin of it
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
        let AddedMagnet {
            engine,
            started_here,
            joiners,
        } = self
            .add_magnet_placed(info_hash, extra_trackers, placement)
            .await?;
        // Data may be in place unless this pin's own add made the torrent:
        // an engine that was there already, and an add a stream started and
        // this pin joined, both come with whatever the store held -- and
        // measured while they check, a complete file reads as nothing had.
        let checked = self
            .check_pin_preconditions(&engine, file_idx, !started_here)
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
            // earlier session, and is slack for the next pass or the next
            // launch sweep rather than this pin's to delete. That is why
            // nothing here asks where a torrent's data is: it used to take
            // the files whenever the pin's own placement folder had not
            // existed before the add.
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
    /// stands the launch sweep keeps that directory, so this is what takes
    /// it now rather than at the next boot. With no engine **and** no pin
    /// there is nothing this call may delete: the bytes belong to no
    /// download it knows of, and the next launch's sweep takes them. What
    /// was really deleted is reported, not what was asked for
    /// ([`UnpinOutcome`]). A `file_idx` the torrent does not
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
                tracing::info!(info_hash, file_idx, "dormant_download_unpinned");
            }
            // `was_dormant` is the warrant, and there is no other one
            // here. Without an engine and without a pin this layer holds
            // no record tying the hash's bytes to a download at all: they
            // are whatever an earlier stream left in the store, which is
            // the launch sweep's to reclaim, not this call's to unlink.
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
    /// the next launch sweep's; this call is what makes an explicit
    /// `deleteFiles` unpin take effect at once instead of at the next boot,
    /// and what takes the directory out of the pin set with the pin.
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
        // Asked at the door, because every question above it was an
        // `await` ago: a torrent added, restored or restarted since is one
        // whose store registered at `init`, and deleting its directory
        // would take the pieces of a live have-set out from under it. The
        // same guard `crate::retention::unlink` makes before a claimless
        // delete, for the same reason and at the same instant -- and, like
        // that one, the safe direction is to refuse: a directory left
        // behind is swept at the next launch, where bytes deleted under a
        // running check are gone.
        //
        // **And the directory leaves the hash's name before a byte goes.**
        // The door is one instant and `remove_dir_all` is a walk of the
        // whole download: a torrent that registered after the door and
        // before the walk ended found half its pieces, and built a have-set
        // from them. Renamed with nothing between the question and the
        // rename, the hash's directory is either whole or gone, and what is
        // deleted afterwards is under a name no store answers to (and the
        // launch sweep's, if the process dies in the walk: it keeps only
        // pinned hashes' own names).
        enum Moved {
            Registered,
            Absent,
            Out,
            Failed(std::io::Error),
        }
        let folder = self.piece_store().torrent_dir(info_hash);
        let doomed = folder.with_file_name(format!(
            "{}.deleting-{}",
            info_hash.to_ascii_lowercase(),
            deletion_nonce()
        ));
        let moved = {
            let registry = Arc::clone(&self.registry);
            let hash = info_hash.to_string();
            let (folder, doomed) = (folder.clone(), doomed.clone());
            tokio::task::spawn_blocking(move || {
                if registry.is_registered(&hash) {
                    return Moved::Registered;
                }
                match std::fs::rename(&folder, &doomed) {
                    Ok(()) => Moved::Out,
                    // "Nothing there" is not "freed", and under this storage
                    // it is the ordinary answer for a hash nothing ever
                    // downloaded: the flag says what left the disk, never
                    // what was asked for.
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Moved::Absent,
                    Err(error) => Moved::Failed(error),
                }
            })
            .await
            .unwrap_or_else(|join| Moved::Failed(std::io::Error::other(join.to_string())))
        };
        match moved {
            Moved::Out => {}
            Moved::Registered => {
                tracing::warn!(
                    info_hash,
                    file_idx,
                    "a store registered for this torrent while its data was being deleted; \
                     leaving it to the torrent that now holds it"
                );
                return false;
            }
            Moved::Absent => return false,
            Moved::Failed(error) => {
                tracing::warn!(
                    info_hash,
                    file_idx,
                    folder = %folder.display(),
                    %error,
                    "could not take the dormant download's pieces out of its name"
                );
                return false;
            }
        }
        match tokio::fs::remove_dir_all(&doomed).await {
            Ok(()) => {
                tracing::info!(
                    info_hash,
                    file_idx,
                    folder = %folder.display(),
                    "dormant_download_deleted"
                );
                true
            }
            Err(error) => {
                tracing::warn!(
                    info_hash,
                    file_idx,
                    folder = %doomed.display(),
                    %error,
                    "could not delete the dormant download's pieces; the next launch sweep takes what is left"
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
            // stayed there until a tick found the torrent unplayed.
            let pieces_freed = match dropped {
                // The claim goes with the pieces, into the one place that
                // orders the unlink against the have-set
                // ([`crate::retention::take_claimed`]) -- and is released
                // there, once the bytes are gone.
                Some(claim) => {
                    crate::retention::take_claimed(&self.registry, &engine.info_hash, claim).await
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

    /// Apply the embedder's pin set to the engines the backend restored.
    ///
    /// Pins of torrents the backend does not have right now are kept
    /// dormant, in memory: librqbit skips a torrent it cannot re-add at
    /// startup -- its output folder on a volume that is not mounted -- and
    /// brings it back on a later boot, when the pin must still be there; a
    /// [`Self::pin_download`] of the torrent meanwhile applies them, an
    /// unpin drops them. Only a pin of a file the torrent does not have is
    /// dropped. Answers how many were applied. Called once at startup,
    /// after the engines are registered.
    ///
    /// `None` declares the pin set unknown
    /// ([`crate::piece_store::PinsUnknown`]) instead of applying no pins:
    /// no pin is applied either way, but every restored torrent is kept and
    /// reported as pinned for the life of the process. Treating silence as
    /// an empty set is what would delete every offline download the
    /// embedder meant to keep.
    ///
    /// **Boot's alone, and that is why it writes `pinned_files` without the
    /// per-hash pin lock.** [`Self::boot`] calls it before it hands the
    /// engine to anyone, so no pin, unpin or delete can be running: each is
    /// a method on an engine nobody else holds yet. The one task that
    /// exists by then, the idle sweep, writes no pin and removes only an
    /// engine idle for [`INACTIVE_TORRENT_REMOVE_TIMEOUT`], which an engine
    /// restored a moment ago is not. Crate-private so that stays true: a
    /// caller holding a published engine would have to take the lock.
    pub(crate) async fn apply_pins(&self, pins: Option<crate::piece_store::PinSet>) -> usize {
        if pins.is_none() {
            tracing::warn!("nobody named the pin set; treating every restored torrent as pinned");
            self.pins_unknown.set("the embedder named no pin set");
        }
        // Every path yields a pin map, empty where it used to return early.
        // The tail of this function is what tells the reconciler that the
        // want-set is back on every restored torrent
        // (`reconcile::Conditions::settled`), and a boot with nothing pinned
        // -- which is most boots -- must reach it: skipping it would leave
        // every restored torrent stopped for good.
        let pins = pins.unwrap_or_default();
        let mut applied = 0;
        let mut dormant = BTreeMap::new();
        for (info_hash, indices) in &pins {
            let info_hash = info_hash.to_lowercase();
            let Some(engine) = self.get_engine(&info_hash).await else {
                dormant.insert(info_hash, indices.iter().copied().collect());
                continue;
            };
            for &file_idx in indices {
                if let Err(error) = engine.handle.pin_file(file_idx).await {
                    tracing::warn!(info_hash, file_idx, %error, "could not apply pin");
                    continue;
                }
                engine.pinned_files.write().insert(file_idx);
                applied += 1;
            }
            engine.touch();
        }
        let dormant_count = dormant.len();
        if dormant_count > 0 {
            tracing::info!(
                torrents = ?dormant.keys().collect::<Vec<_>>(),
                "keeping pins of torrents the backend did not restore"
            );
            self.dormant_pins.lock().extend(dormant);
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
        tracing::info!(applied, "pins_applied");
        applied
    }

    /// Whether the embedder named no pin set at boot --
    /// [`crate::piece_store::PinsUnknown`], which is what makes
    /// `GET /downloads.json` list every restored torrent's files. While it
    /// holds, every restored torrent is pinned as far as this engine is
    /// concerned.
    pub fn pins_unknown(&self) -> bool {
        self.pins_unknown.is_set()
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

    /// The pins [`Self::apply_pins`] found no torrent for, ordered like
    /// [`Self::pinned_downloads`] -- pins the embedder named of a torrent
    /// the backend did not restore (a `.torrent` that will not parse, an
    /// add that errored). They are not downloading anything: the
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
    /// (`may_have_data_in_place`: this call did not add it -- a restart, a
    /// stream, an add a stream started that the pin joined) is not
    /// measured at all: `downloaded` reads 0
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
        match self.free_space_of(&volume).await {
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
    /// selection and its stream count go. What a delete needs before it reconciles -- the want-set
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
        self.hand_live_on(&info_hash, file_idx).await;

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

    /// The aside rule again, at the moment it can have changed its answer:
    /// the live file `file_idx` of `info_hash` has lost its last read, and
    /// another file of the torrent is still being read. That file is what
    /// is playing, and it takes the cell and the selection.
    ///
    /// The rule is decided at each open from a count of reads, and a player
    /// that opens episode two before the server has seen episode one's
    /// connection close finds episode one still read: the open is an aside,
    /// the cell stays on episode one, and episode two -- the one being
    /// watched -- is protected only while a read of it is open. Its first
    /// seek gap handed its whole extent to the next tick's slack pass.
    ///
    /// **A file is read here while a response of it is open, whether or
    /// not that response has read a byte yet.** Episode two's own read
    /// opens only once its gate, its reconcile and the wait for its first
    /// piece are behind it -- seconds on a file nothing has fetched --
    /// while episode one's connection closes milliseconds after the open:
    /// asked of reads alone, this found nothing else read and left the cell
    /// on episode one for good. And the live file's own open response is a
    /// seek, not a viewer gone: its old read has closed and its new one has
    /// not begun, and handed on, the cell went to a subtitle beside it.
    ///
    /// Read after the engine lookup and under the stream counts' guard,
    /// and written with nothing awaited in between ([`Self::switch_to`]'s
    /// rule), and the write moves the cell only off the file this was
    /// decided about ([`Live::hand_on`]): the end of a read of any other
    /// file moves nothing, and an open that moved the cell meanwhile
    /// stands.
    ///
    /// [`Live::hand_on`]: crate::retention::live::Live::hand_on
    async fn hand_live_on(&self, info_hash: &str, file_idx: usize) {
        let Some(engine) = self.peek_engine(info_hash).await else {
            return;
        };
        let streams = self.active_file_streams.read().await;
        let open = |file: usize| {
            engine.retention.readers_of(&file) > 0
                || streams
                    .get(&(info_hash.to_string(), file))
                    .is_some_and(|count| *count > 0)
        };
        if open(file_idx) {
            return;
        }
        let Some(next) = engine
            .retention
            .keys()
            .into_iter()
            .chain(
                streams
                    .keys()
                    .filter(|(hash, _)| hash == info_hash)
                    .map(|(_, file)| *file),
            )
            .filter(|file| *file != file_idx && open(*file))
            .min()
        else {
            return;
        };
        let torrent = |file_idx| crate::retention::live::LiveEntity::Torrent {
            info_hash: info_hash.to_string(),
            file_idx,
        };
        let switch = self.live.hand_on(&torrent(file_idx), torrent(next));
        drop(streams);
        let Some(switch) = switch else {
            return;
        };
        tracing::debug!(
            info_hash = %info_hash,
            file_idx = next,
            from = ?switch.from,
            "the live file's last read closed while another file of it is read; that one is playing"
        );
        self.activate_multifile_file(info_hash, next, None, "hand-on")
            .await;
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

            // Nothing here touches what is being played. The delayed
            // cleanup is about the want-set -- which file librqbit should
            // prioritise five seconds after a body ended -- and a body that
            // ended is not a stream that has been replaced: the viewer who
            // paused is still watching this file, and what says otherwise
            // is their opening something else.
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

    /// Bring an engine up over `download_dir`, in the one order that keeps
    /// the disk bounded and the pins safe:
    ///
    /// 1. Take the embedder's pin set (`pins`), which is what says which
    ///    torrents' data is a download rather than cache
    ///    ([`crate::piece_store::pin_record`]).
    /// 2. Sweep every piece directory it does not claim
    ///    ([`crate::piece_store::sweep_before_session`]). Everything that is
    ///    not pinned is cache, nothing is playing in a process that has
    ///    served nothing, and this is the only moment at which no store is
    ///    registered and no torrent is mid-check -- so what goes here goes
    ///    with nothing holding a handle on it.
    /// 3. `open_session`, which restores the torrents and registers their
    ///    stores. Their `init` seeds each held set from what the sweep left,
    ///    which is why the sweep has to precede it: seeded first and swept
    ///    after, every deleted piece would stay counted as held for the life
    ///    of the process.
    /// 4. Apply the same set to the torrents that came back.
    ///
    /// The sequence lives here rather than in each constructor because the
    /// order *is* the design; two constructors with two copies of it is two
    /// chances to reverse steps 2 and 3.
    pub(crate) async fn boot<F, Fut>(
        cache_dir: std::path::PathBuf,
        download_dir: std::path::PathBuf,
        tracker_storage: Option<Arc<dyn crate::trackers::TrackerStorage>>,
        pins: Option<crate::piece_store::PinSet>,
        open_session: F,
    ) -> Result<Self>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(B, HashMap<String, B::Handle>)>>,
    {
        crate::piece_store::sweep_before_session(&download_dir, pins.as_ref()).await;
        let (backend, restored) = open_session().await?;
        let efs = Self::new_with_backend_and_storage(
            backend,
            restored,
            cache_dir,
            download_dir,
            tracker_storage,
        );
        efs.apply_pins(pins).await;
        Ok(efs)
    }
}

impl BackendEngineFS<LibrqbitBackend> {
    pub async fn new(
        root_dir: std::path::PathBuf,
        _cache_config: EngineCacheConfig,
    ) -> Result<Self> {
        let download_dir = root_dir.join("rqbit-downloads");
        let resolvers =
            crate::backend::dht_bootstrap::DhtBootstrapDns::default().resolvers_in(&download_dir);
        let session_dir = download_dir.clone();
        Self::boot(
            root_dir.join("cache"),
            download_dir,
            None,
            None,
            move || {
                LibrqbitBackend::new(
                    session_dir,
                    TorrentListenPort::default(),
                    Vec::new(),
                    resolvers,
                )
            },
        )
        .await
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
        pins: Option<crate::piece_store::PinSet>,
    ) -> Result<Self> {
        let download_dir = root_dir.join("rqbit-downloads");
        let resolvers = config.dht_bootstrap_dns.resolvers_in(&download_dir);
        let tuning = crate::backend::librqbit::SessionTuning::from_settings(
            &config.speed_profile,
            &config.privacy,
        );
        let session_dir = download_dir.clone();
        Self::boot(
            root_dir.join("cache"),
            download_dir,
            tracker_storage,
            pins,
            move || {
                LibrqbitBackend::new_with_settings(
                    session_dir,
                    config.listen_port,
                    config.dht_bootstrap_nodes,
                    resolvers,
                    tuning,
                )
            },
        )
        .await
    }

    /// librqbit sessions always persist downloads to disk, so the disk-backed
    /// constructor is the same as the regular one.
    pub async fn new_disk_backed(
        root_dir: std::path::PathBuf,
        config: crate::backend::BackendConfig,
        tracker_storage: Option<Arc<dyn crate::trackers::TrackerStorage>>,
        pins: Option<crate::piece_store::PinSet>,
    ) -> Result<Self> {
        Self::new_with_storage(root_dir, config, tracker_storage, pins).await
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

    /// The claim the fake's `drop_pieces` hands out, which records the
    /// thread that released it.
    ///
    /// Nothing about the claim's *meaning* changes: it is opaque to
    /// everything above the backend, it is held across the deletion, and
    /// dropping it is what ends the release. What it adds is a witness --
    /// the thread the bytes really went on.
    struct ClaimProbe {
        released_on: Arc<Mutex<Option<std::thread::ThreadId>>>,
    }

    impl Drop for ClaimProbe {
        fn drop(&mut self) {
            *self.released_on.lock().unwrap() = Some(std::thread::current().id());
        }
    }

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
        /// The hot file of the last plan, beside the active one.
        last_hot_file: Mutex<Option<usize>>,
        last_generation: AtomicU64,
        /// Test knob: the piece indices `drop_file_pieces` hands back as
        /// the ones the backend has agreed to forget. Empty by default,
        /// which is a backend with nothing had.
        drops_pieces: Mutex<Vec<u32>>,
        /// Test knob: report every file as fully on disk (seeded torrent)
        /// instead of the default half-downloaded state.
        seeded: AtomicBool,
        pin_file: AtomicUsize,
        /// Test knob: park the first `pin_file` call on this handle, the
        /// way `advertise_gate` parks a pass. The fake sends on the first
        /// channel as it enters the call and waits on the second before
        /// returning, so a test can hold a pin-record repair in the middle
        /// of its loop -- the condition still set, half the files pinned,
        /// the record not yet written -- and ask what another pin or unpin
        /// does meanwhile. Runs once and is gone.
        pin_gate: Mutex<
            Option<(
                tokio::sync::oneshot::Sender<()>,
                tokio::sync::oneshot::Receiver<()>,
            )>,
        >,
        unpin_file: AtomicUsize,
        /// The ranges `drop_pieces` was asked to forget and what each was
        /// to do afterwards, in order -- every reclaim must ask before it
        /// removes a byte, and the per-file delete must ask for the
        /// re-select its still-pinned neighbour needs.
        dropped_ranges: Mutex<Vec<(std::ops::Range<u32>, crate::backend::AfterRelease)>>,
        /// The fake's want-set, as what is *out* of it: every piece
        /// `drop_pieces` dropped and nothing has re-selected since. A piece
        /// of a file is wanted unless it is here, which is librqbit's own
        /// reading of a dropped piece.
        dropped: Mutex<std::collections::BTreeSet<u32>>,
        /// Every range `reselect_pieces` was asked to want again, in order.
        reselected: Mutex<Vec<std::ops::Range<u32>>>,
        /// The lookahead every reader was opened with, in order.
        lookaheads: Mutex<Vec<u64>>,
        /// What happens on the first `drop_pieces` of a test, from inside
        /// the call: where a test puts what a user or a reader does while
        /// the pass has one part of a run released and the next still to
        /// ask about. Runs once and is gone.
        on_first_drop: Mutex<Option<Box<dyn FnOnce() + Send>>>,
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
        /// Test knob: the backend will not change what it advertises --
        /// librqbit's answer for a torrent whose state has gone. Every
        /// `set_pieces_advertised` fails and records nothing while it is
        /// set.
        refuses_advertise: AtomicBool,
        /// Which thread released the claim `drop_pieces` handed out, and
        /// `None` until one has been released.
        ///
        /// The claim is what orders the unlink against the have-set, so it
        /// has to outlive the deletion: whichever thread drops it is the
        /// thread the piece files were unlinked on. A retention pass runs
        /// under its file's turn and must not do that unlinking on the
        /// reactor, and this is the only thing a test can read it off --
        /// see `the_pass_unlinks_a_reclaimed_piece_off_the_reactor`.
        claim_released_on: Arc<Mutex<Option<std::thread::ThreadId>>>,
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
            *self.counters.last_hot_file.lock().unwrap() =
                plan.hot_file.as_ref().map(|hot| hot.file_idx);
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
        /// Test knob: park the next `remove_torrent_and_files` call, the
        /// way `FakeCounters::advertise_gate` parks a pass. The fake sends
        /// on the first channel as it enters the call and waits on the
        /// second before returning, so a test can ask what a pin does
        /// while a removal is inside the backend. Runs once and is gone.
        remove_gate: Arc<Mutex<Option<Gate>>>,
        /// The last thing `set_upload_enabled` was told; `None` before the
        /// first call.
        upload_enabled: Arc<Mutex<Option<bool>>>,
    }

    /// A parked backend call's two ends, as the fake holds them: it sends
    /// on the first as it enters the call and waits on the second.
    type Gate = (
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    );

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
                remove_gate: Arc::new(Mutex::new(None)),
                upload_enabled: Arc::new(Mutex::new(None)),
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

        fn set_upload_enabled(&self, enabled: bool) {
            *self.upload_enabled.lock().unwrap() = Some(enabled);
        }

        async fn remove_torrent_and_files(&self, info_hash: &str) -> Result<()> {
            // Parked inside the call, if a test asked for it: see
            // `FakeBackend::remove_gate`.
            let gate = self.remove_gate.lock().unwrap().take();
            if let Some((entered, release)) = gate {
                let _ = entered.send(());
                let _ = release.await;
            }
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
            // Parked inside the call, if a test asked for it: see
            // `FakeCounters::pin_gate`.
            let gate = self.counters.pin_gate.lock().unwrap().take();
            if let Some((entered, release)) = gate {
                let _ = entered.send(());
                let _ = release.await;
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
            if self.counters.refuses_advertise.load(Ordering::SeqCst) {
                anyhow::bail!("this fake will not change what it advertises");
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
            if let Some(hook) = self.counters.on_first_drop.lock().unwrap().take() {
                hook();
            }
            if self.counters.refuses_drop.load(Ordering::SeqCst) {
                anyhow::bail!("this fake will not forget a piece it has");
            }
            let dropped: Vec<u32> = if self.counters.drops_what_it_is_asked.load(Ordering::SeqCst) {
                asked.collect()
            } else {
                self.counters.drops_pieces.lock().unwrap().clone()
            };
            self.counters
                .dropped
                .lock()
                .unwrap()
                .extend(dropped.iter().copied());
            Ok(Some(crate::backend::DroppedFilePieces::new(
                dropped,
                ClaimProbe {
                    released_on: self.counters.claim_released_on.clone(),
                },
            )))
        }

        /// Like librqbit: only a piece that was dropped changes, and the
        /// count is how many did.
        async fn reselect_pieces(&self, pieces: std::ops::Range<u32>) -> Result<usize> {
            self.counters
                .reselected
                .lock()
                .unwrap()
                .push(pieces.clone());
            let mut dropped = self.counters.dropped.lock().unwrap();
            Ok(pieces.filter(|piece| dropped.remove(piece)).count())
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
            lookahead_bytes: u64,
        ) -> Result<Box<dyn FileStreamTrait>> {
            self.gate().await?;
            self.counters.get_file_reader.fetch_add(1, Ordering::SeqCst);
            self.counters
                .lookaheads
                .lock()
                .unwrap()
                .push(lookahead_bytes);
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
            _lookahead_bytes: u64,
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

    /// What is in the download dir, sorted, for the tests that assert a pin
    /// leaves no record of its own there.
    fn download_dir_entries(dir: &std::path::Path) -> Vec<std::ffi::OsString> {
        let mut names: Vec<_> = std::fs::read_dir(dir)
            .expect("the fixture's download dir")
            .map(|entry| entry.expect("an entry").file_name())
            .collect();
        names.sort();
        names
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

    /// The fake torrent's piece store: over the directory the test writes
    /// piece files into, seeded from what is there and registered where the
    /// pass reads, as a real torrent's `init` registers the store librqbit
    /// writes to. The pass reads the registry and nothing else -- no
    /// listing -- so a test that writes piece files by hand tells the
    /// registry about them through this, and again through
    /// `init_for_tests` after any later write. The store has to outlive the
    /// passes: the registry holds a `Weak`.
    fn seeded_store(
        enginefs: &BackendEngineFS<FakeBackend>,
        engine: &Engine<FakeHandle>,
    ) -> crate::piece_store::PieceStore {
        let store = crate::piece_store::PieceStore::under(
            enginefs.store_registry().clone(),
            &engine.info_hash,
            engine
                .handle
                .layout()
                .expect("the fake torrent has a layout"),
        );
        store
            .init_for_tests()
            .expect("seed and register the fake torrent's store");
        store
    }

    /// A scratch root of its own for one fake-engine fixture.
    ///
    /// Never a fixed path under `std::env::temp_dir()`: every fixture's
    /// torrent is `TEST_HASH`, so its piece store is the same path under
    /// `<download dir>/.pieces` in all of them, and the tests run in
    /// parallel -- a shared root has one test writing and unlinking the
    /// piece files another is asserting on. The parent is wiped once per
    /// test process so the roots do not pile up across runs.
    fn fake_engine_root() -> std::path::PathBuf {
        static ROOTS: AtomicUsize = AtomicUsize::new(0);
        static WIPE: std::sync::Once = std::sync::Once::new();
        // Per *process*, not per machine: the wipe below removes the whole
        // parent, and two test binaries running at once -- `cargo test`
        // running enginefs's lib tests beside anything else that links it,
        // or two `cargo test -p enginefs` invocations -- would each wipe the
        // other's roots mid-test, taking piece files their fixtures had just
        // written. Measured: three file-existence failures in a run that was
        // green on its own.
        let parent =
            std::env::temp_dir().join(format!("enginefs-fake-engine-tests-{}", std::process::id()));
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
        // File 0 of the fixture's torrent is the one being played, which is
        // what a stream open writes (`on_stream_start`) and what every pass
        // and every gate reads. Without it the fixture's torrent is one
        // nobody is watching, and the honest thing to do with those is
        // delete them -- so a test that drove a pass by hand would be
        // measuring the delete rather than the window. See `playing`.
        enginefs.live().open(
            crate::retention::live::LiveEntity::Torrent {
                info_hash: TEST_HASH.to_string(),
                file_idx: 0,
            },
            false,
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
        let removed = backend.removed_with_files.clone();
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
        // The selection is the latest request's, and file 1's response --
        // which nothing has ended -- is still counted.
        let mut open: Vec<usize> = snapshot
            .active_file_streams
            .iter()
            .map(|stream| stream.file_idx)
            .collect();
        open.sort_unstable();
        assert_eq!(open, vec![1, 2]);
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
    /// want-set and what a retention pass may take, never a location.
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

    /// A torrent the backend stopped for want of disk space is put back to
    /// work **by the reconciler**: once the volume is over the line a
    /// stopped torrent has to clear, and at most once per dwell.
    ///
    /// The line is the resume line and not the floor, and the dwell is what
    /// keeps the recovery from being a loop. A restart re-checks the
    /// torrent's storage and takes it straight back to writing, so one made
    /// at the floor fills the volume again within seconds -- and a restart
    /// per tick is a re-check of every piece on the disk every two seconds,
    /// for as long as the device is full. It used to be conditional on an
    /// eviction pass having freed *something*, which is neither: a device
    /// that gained a gigabyte by any other means left the torrent dead, and
    /// one freed byte restarted it into the same wall.
    #[tokio::test(start_paused = true)]
    async fn the_reconciler_restarts_an_out_of_space_torrent_over_the_resume_line() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        let available = Arc::new(AtomicU64::new(u64::MAX));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));

        // A healthy torrent is nobody's business.
        assert_eq!(counters.restart_from_error.load(Ordering::SeqCst), 0);

        // The backend kills it: a write hit ENOSPC.
        counters.out_of_space.store(true, Ordering::SeqCst);

        // Inside the band -- over the floor, under the resume line -- it is
        // left where it is: there is not enough room to run into.
        available.store(
            CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN - 1,
            Ordering::SeqCst,
        );
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Leave)]
        );
        assert_eq!(counters.restart_from_error.load(Ordering::SeqCst), 0);

        // Over it, and it goes back to work.
        available.store(
            CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN,
            Ordering::SeqCst,
        );
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::RestartFromError)]
        );
        assert_eq!(counters.restart_from_error.load(Ordering::SeqCst), 1);
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);

        // And it dies again at once, as a torrent on a volume this short
        // will. The tick that sees it is inside the dwell of the restart
        // that put it there, and makes no second attempt.
        counters.out_of_space.store(true, Ordering::SeqCst);
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::RestartFromError)],
            "the ladder still wants it restarted; it is the actuator that waits"
        );
        assert_eq!(
            counters.restart_from_error.load(Ordering::SeqCst),
            1,
            "not twice inside a dwell"
        );

        // Past the dwell, it tries again.
        tokio::time::advance(RECONCILE_MIN_DWELL).await;
        enginefs.reconcile_tick().await;
        assert_eq!(counters.restart_from_error.load(Ordering::SeqCst), 2);

        // A hash no engine holds any more -- swept while space was being
        // reclaimed -- is not an error, and restarts nothing.
        assert!(
            !enginefs
                .restart_from_error("ffffffffffffffffffffffffffffffffffffffff")
                .await
                .unwrap()
        );
        assert_eq!(counters.restart_from_error.load(Ordering::SeqCst), 2);
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
        // The fixture's torrent is the one being played, which is what a
        // stream open writes (`on_stream_start`) and what the ladder's
        // bottom arm reads. Almost every test below is about the
        // free-space arm -- a volume filling under a torrent somebody is
        // watching -- and without this every one of them would be
        // measuring the arm that stops a torrent nobody is watching. The
        // few that are about *that* say so by opening something else.
        enginefs.live().open(
            crate::retention::live::LiveEntity::Torrent {
                info_hash: TEST_HASH.to_string(),
                file_idx: 0,
            },
            false,
        );
        (enginefs, counters)
    }

    /// Nothing is playing this server's torrents any more: a proxied body
    /// is. What a viewer opening something else does to every torrent at
    /// once.
    fn nothing_torrent_is_playing(enginefs: &BackendEngineFS<FakeBackend>) {
        enginefs.live().open(
            crate::retention::live::LiveEntity::Proxy {
                dir: "/elsewhere".into(),
            },
            false,
        );
    }

    /// Every call the reconciler could make and does not, in one place.
    fn assert_nothing_moved(counters: &FakeCounters) {
        assert_eq!(counters.stop_torrent.load(Ordering::SeqCst), 0);
        assert_eq!(counters.start_torrent.load(Ordering::SeqCst), 0);
        assert_eq!(counters.restart_from_error.load(Ordering::SeqCst), 0);
    }

    /// The liveness reading a test's own pass runs under: the file it is
    /// about is the one being played, which is what the stream open the
    /// production path always makes first would have written
    /// (`on_stream_start`). Without it every entity is slack and every pass
    /// is a delete, which is exactly the point of the value.
    fn playing(file_idx: usize) -> crate::retention::live::Reading {
        crate::retention::live::Reading::of(crate::retention::live::LiveEntity::Torrent {
            info_hash: TEST_HASH.to_string(),
            file_idx,
        })
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
        // The torrent is the one being played, so the only arm that moves
        // it is the free-space one -- which is what the dwell is now
        // driven by: nothing else in the ladder oscillates.
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.on_stream_start(TEST_HASH, 0).await;
        counters.start_torrent.store(0, Ordering::SeqCst);

        // The volume falls under the floor: the stop the dwell runs from.
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);

        // And clears, well over the resume margin, at once.
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
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
        enginefs.on_stream_start(TEST_HASH, 0).await;
        counters.start_torrent.store(0, Ordering::SeqCst);

        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);

        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
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

    /// **Nothing but a stream opening writes the liveness cell.**
    ///
    /// A client polling the statistics is *looking at* a torrent, not
    /// watching it; so is a details page asking what a magnet resolved to,
    /// and so is `focus_torrent`, which names a torrent and registers
    /// nothing at all. Each of them used to be able to hold a torrent
    /// running: the grace the idle arm measured came off `last_accessed`,
    /// which every `get_engine` writes, so a details page left open --
    /// which polls every few seconds -- reset it before it could ever run
    /// out, and the torrent downloaded all night with seeding off.
    ///
    /// The cell has one writer now, so the question is not "how long since
    /// something touched this engine" but "did the server see a stream
    /// open". Every reader below is asked whether it moved the cell, and
    /// none of them did.
    #[tokio::test(start_paused = true)]
    async fn looking_at_a_torrent_is_not_playing_it() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));

        // A stream really opens, so there is a reading to be wrongly kept.
        enginefs.on_stream_start(TEST_HASH, 0).await;
        assert_eq!(enginefs.live().reading().file_of(TEST_HASH), Some(0));
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);

        // And is replaced by another entity: from here nothing is playing
        // this torrent, whatever anything asks about it.
        enginefs.live().open(
            crate::retention::live::LiveEntity::Proxy {
                dir: "/elsewhere".into(),
            },
            false,
        );

        // Five minutes of a details page polling every ten seconds. Both
        // halves of what the route does to the engine, because both write
        // `last_accessed`: `routes::system::stats_target` finds it with
        // `get_engine`, and then `engine.get_statistics()` builds the body.
        for _ in 0..30 {
            let engine = enginefs
                .get_engine(TEST_HASH)
                .await
                .expect("the statistics route reaches its engine this way");
            engine.get_statistics().await;
            enginefs.focus_torrent(TEST_HASH).await;
            enginefs.peek_engine(TEST_HASH).await;
            tokio::time::advance(Duration::from_secs(10)).await;
            enginefs.reconcile_tick().await;
        }

        assert!(
            !enginefs.live().is_torrent(TEST_HASH),
            "looking at a torrent did not make it the one being played"
        );
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Paused,
            "nobody is watching it, so there is nothing for it to fetch"
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

    /// **The stream route's registration asks the reconciler nothing.**
    ///
    /// The route asks it once, after its disk gate, through `focus_torrent`
    /// -- that is the reading that can start a torrent the gate has just
    /// made room for. A reconcile inside the registration as well was a
    /// second ladder and a second volume probe on every Range request. The
    /// archive route has no gate and no reconcile of its own, and
    /// `on_stream_start` still asks for it.
    #[tokio::test(start_paused = true)]
    async fn only_the_reconciled_stream_start_asks_the_reconciler() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(CACHE_FREE_SPACE_FLOOR - 1));
        enginefs.on_stream_start_unreconciled(TEST_HASH, 0).await;
        assert_eq!(
            counters.stop_torrent.load(Ordering::SeqCst),
            0,
            "the registration reconciled"
        );
        enginefs.on_stream_start(TEST_HASH, 0).await;
        assert_eq!(counters.stop_torrent.load(Ordering::SeqCst), 1);
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
    /// else is true: seeding off, nobody playing it, and a volume with
    /// nothing left on it.
    #[tokio::test(start_paused = true)]
    async fn a_torrent_without_metadata_is_decided_to_run() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(0);
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);

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

    /// A pin is a promise to have the file offline, so the ladder's bottom
    /// arm does not reach the torrent holding it -- with seeding off and
    /// nothing playing, the pinned torrent still runs while an unpinned one
    /// beside it is stopped.
    #[tokio::test(start_paused = true)]
    async fn a_pinned_torrent_is_decided_to_run() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        nothing_torrent_is_playing(&enginefs);
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Stop)],
            "unpinned and unplayed, it is stopped"
        );
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();

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
        // Both pinned, because only one entity can be the one being played
        // and this test is about every torrent getting a decision. A pin is
        // the other way a torrent is owed one.
        for hash in [TEST_HASH, OTHER_HASH] {
            enginefs
                .get_engine(hash)
                .await
                .expect("the fixture's engine")
                .pinned_files
                .write()
                .insert(0);
        }

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
    /// ENOSPC and librqbit declares it dead. Stopped once, not once per
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

        // A byte under it: stopped, once.
        available.store(CACHE_FREE_SPACE_FLOOR - 1, Ordering::SeqCst);
        enginefs.reconcile_tick().await;
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);
        assert_eq!(counters.stop_torrent.load(Ordering::SeqCst), 1);
        assert!(engine.is_stopped_for_space().await);
        assert!(engine.held_stopped_for_space().await);
        assert!(
            !engine.reads_refused(),
            "its readers wait for the slack to go"
        );

        // Back over the floor but inside the margin: still stopped -- the
        // margin is what it has to see cleared before anything starts it
        // again -- and said so, because the ladder holding a torrent stopped
        // is a condition a client needs to know about. The floor is what
        // decides whether there is room *now*, so it reads clear here.
        available.store(
            CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN - 1,
            Ordering::SeqCst,
        );
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);
        assert!(!engine.is_stopped_for_space().await, "the floor is clear");
        assert!(
            engine.held_stopped_for_space().await,
            "but the ladder is still holding it, and says so"
        );

        // The margin over: started again. Past the dwell as well, which
        // every timer start of a torrent this reconciler stopped has to be.
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
        assert!(!engine.held_stopped_for_space().await);
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

    /// The running-low bell, and the line it is rung at.
    ///
    /// It is rung by the tick's own reading of the volume -- the one
    /// `statvfs` of the session -- while that reading is **inside the
    /// band**: over the floor, under the line a stopped torrent has to
    /// clear. Not at the floor, which is where a stream is already being
    /// refused: the slack has to be given back while there is still room
    /// left to give it into. A reading over the line rings nothing, and so
    /// does a probe that failed -- an unreadable volume is not a full one,
    /// here as everywhere else.
    #[tokio::test(start_paused = true)]
    async fn the_bell_rings_while_the_volume_is_inside_the_band() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        let available = Arc::new(AtomicU64::new(u64::MAX));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| {
            match probe_available.load(Ordering::SeqCst) {
                // The one reading that is not a number: a probe that could
                // not read the volume at all.
                u64::MAX => Err(std::io::Error::other("no reading")),
                bytes => Ok(bytes),
            }
        });
        let rung = || {
            let bell = enginefs.slack_bell().clone();
            async move {
                tokio::time::timeout(Duration::from_millis(10), bell.rung())
                    .await
                    .is_ok()
            }
        };

        // A volume nobody can read rings nothing.
        enginefs.reconcile_tick().await;
        assert!(!rung().await, "an unreadable volume is not a short one");

        // Well over the line: nothing to give back.
        available.store(
            CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN,
            Ordering::SeqCst,
        );
        enginefs.reconcile_tick().await;
        assert!(!rung().await, "a volume with room rings nothing");

        // One byte under it -- inside the band, still over the floor.
        available.store(
            CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN - 1,
            Ordering::SeqCst,
        );
        enginefs.reconcile_tick().await;
        assert!(
            rung().await,
            "the slack is wanted back before the floor is reached"
        );
        assert!(
            !rung().await,
            "and one permit, not a counter: the ring was taken"
        );

        // And the same reading, taken on demand by a caller that has just
        // given bytes back rather than by the tick: the recorded reading
        // moves and the bell rings, without waiting two seconds for a pass
        // that would have answered about the volume as it was.
        available.store(u64::MAX - 1, Ordering::SeqCst);
        enginefs.reread_volume().await;
        assert_eq!(enginefs.volumes.available(), Some(u64::MAX - 1));
        assert!(!rung().await, "a volume with room rings nothing");
        available.store(0, Ordering::SeqCst);
        enginefs.reread_volume().await;
        assert_eq!(enginefs.volumes.available(), Some(0));
        assert!(rung().await, "and a short one does");
    }

    /// The tick reads the volume even when there is no torrent to decide
    /// about, and rings from that reading.
    ///
    /// The bell's own consumer is the proxy cache, and a session that has
    /// only ever proxied has no engines at all -- nor has one whose
    /// torrents the housekeeping sweep has removed. Both hold cached bytes
    /// and neither will ever open a torrent, which makes them exactly the
    /// population the bell exists for: nothing switches, so nothing else
    /// asks the proxy for its slack. Read inside the loop over the
    /// engines, the `statvfs` was never taken for them and the bell could
    /// not ring at all.
    #[tokio::test(start_paused = true)]
    async fn a_tick_with_no_torrents_still_reads_the_volume_and_rings() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        enginefs.remove_engine(TEST_HASH).await;
        assert!(
            enginefs.peek_engine(TEST_HASH).await.is_none(),
            "the session holds no torrent at all"
        );
        enginefs.set_free_space_probe(|_| Ok(0));

        assert!(
            enginefs.reconcile_tick().await.is_empty(),
            "there is nothing to decide about"
        );
        assert_eq!(
            enginefs.volumes.available(),
            Some(0),
            "and the volume was read anyway"
        );
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                enginefs.slack_bell().clone().rung()
            )
            .await
            .is_ok(),
            "so the one owner that answers the bell is told"
        );
    }

    /// The on-demand re-read takes its `statvfs` on the blocking pool.
    ///
    /// It runs on a stream request's own task, and the device it reads is
    /// by construction one that has just run out of room. Taken inline it
    /// would park that worker thread -- and every other stream and API task
    /// scheduled on it -- on a spun-down disk or a mount that has stopped
    /// answering, which is the very wait the request's slack-drop bound had
    /// just ended.
    ///
    /// Asserted on the thread the probe ran on, because that is the claim:
    /// a timing test would only say that this particular probe was quick.
    #[tokio::test]
    async fn a_re_read_of_the_volume_is_taken_off_the_worker_that_asked_for_it() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        let worker = std::thread::current().id();
        let probed_on = Arc::new(parking_lot::Mutex::new(None));
        let recorder = probed_on.clone();
        enginefs.set_free_space_probe(move |_| {
            *recorder.lock() = Some(std::thread::current().id());
            Ok(u64::MAX)
        });

        enginefs.reread_volume().await;

        assert_ne!(
            probed_on.lock().expect("the volume was read"),
            worker,
            "the re-read parked the async worker it was asked on"
        );
    }

    /// **And so is every other reading of the volume**: the tick's, the one
    /// a stream request's reconcile takes, and a pin's.
    ///
    /// Each ran its `statvfs` inline on the worker that asked, and the
    /// device each reads is the one filling up -- a spun-down disk, a mount
    /// that has stopped answering. The request's reconcile is one per Range
    /// request, and the tick's parks whatever else shares its worker.
    #[tokio::test]
    async fn every_reading_of_the_volume_is_taken_off_the_worker_that_asked_for_it() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        let worker = std::thread::current().id();
        let probed_on = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let recorder = probed_on.clone();
        enginefs.set_free_space_probe(move |_| {
            recorder.lock().push(std::thread::current().id());
            Ok(u64::MAX)
        });
        let taken = || std::mem::take(&mut *probed_on.lock());

        enginefs.reconcile_tick().await;
        let tick = taken();
        enginefs
            .reconcile_hash(TEST_HASH, Trigger::PlaybackStart)
            .await;
        let request = taken();
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        let pin = taken();

        for (who, threads) in [
            ("the tick", tick),
            ("a request's reconcile", request),
            ("a pin", pin),
        ] {
            assert!(!threads.is_empty(), "{who} read no volume");
            assert!(
                !threads.contains(&worker),
                "{who} parked the async worker it was asked on"
            );
        }
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

    /// The other half of that: a torrent the backend really did stop with
    /// an error goes back to work on the reconciler's own next pass, from
    /// the volume reading that pass takes -- there is no second owner of
    /// the transition and nothing to wait for.
    ///
    /// The restart also lets its reads park again. A torrent whose volume
    /// was short long enough for the stall bound to fail its readers, and
    /// which then died of the ENOSPC the bound was waiting out, comes back
    /// through here -- and a reader opened on it afterwards must wait for
    /// pieces that are being fetched again rather than be handed
    /// `StorageFull` for a volume that has since cleared.
    #[tokio::test(start_paused = true)]
    async fn the_reconciler_restarts_an_errored_torrent_and_its_reads_park_again() {
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

        // And then the backend kills it outright, on a volume that has
        // room again by the time the next pass reads it.
        available.store(u64::MAX, Ordering::SeqCst);
        counters.out_of_space.store(true, Ordering::SeqCst);
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::RestartFromError)]
        );
        assert_eq!(
            counters.start_torrent.load(Ordering::SeqCst),
            0,
            "and not through the start call, which starts nothing on an errored torrent"
        );
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
    /// reads park again. Leave the whole `Error` state on the `Leave` arm
    /// and that lift never happens: the only other thing that lifts a
    /// refusal is the restart, and the restart will not touch a torrent
    /// whose want-set is not back either, so a reader opened on it is
    /// handed `StorageFull` on a volume with room to spare, for good.
    ///
    /// The volume stays short throughout, which is what keeps this about
    /// the want-set: a settled errored torrent on a volume that has cleared
    /// the resume line is restarted rather than left, which is
    /// `the_reconciler_restarts_an_out_of_space_torrent_over_the_resume_line`.
    ///
    /// Asserted through [`poll_a_read`] rather than on `reads_refused`,
    /// which is the flag the code under test writes; what a player gets is
    /// this.
    #[tokio::test(start_paused = true)]
    async fn an_errored_torrent_is_left_alone_only_once_its_want_set_is_back() {
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
        counters.out_of_space.store(true, Ordering::SeqCst);
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Error);

        // Settled, on a volume with no room to restart it into: it is left
        // where it is, and its refusal stands until the volume clears.
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
        // ladder will not restart either: an unsettled reading is never
        // read as "this should be running". The refusal lapses.
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

    /// A read parked on a piece a stopped torrent will not download is a
    /// player spinning for ever. Refusing reads wakes the parked one to
    /// fail with `StorageFull`, fails a new one at its first poll, and the
    /// reconciler does the refusing itself once the volume has been short
    /// for [`STOPPED_READ_STALL_BOUND`] -- the bound for a volume nothing
    /// here can free.
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

        // Stopped, inside the stall bound: the read stays parked (a slack
        // pass may still give the room back), so a task on it does not end.
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
        // One is being played and the other is pinned, so the only arm that
        // can move either is the free-space one -- which is what the bound
        // is measured under.
        enginefs.live().open(
            crate::retention::live::LiveEntity::Torrent {
                info_hash: TEST_HASH.to_string(),
                file_idx: 0,
            },
            false,
        );
        engines[1].pinned_files.write().insert(0);
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
    /// volume gets its room back; and the user presses play. The playback
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

        // The volume gets its room back; the user presses play.
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
    /// The volume gains room while nobody is playing the torrent, so the
    /// pass that follows answers the ladder's bottom `Stop`: no start, no
    /// call of any kind. The refusal still has to go -- it says the device
    /// has no room, and the device has room.
    #[tokio::test(start_paused = true)]
    async fn a_volume_with_room_lifts_the_refusal_of_a_torrent_nobody_plays() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        let available = Arc::new(AtomicU64::new(0));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        nothing_torrent_is_playing(&enginefs);
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
            "nothing is playing it and nothing pinned it, so it stays stopped"
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
    /// rest of the server is happy with, is not out of disk at the floor --
    /// which is the line every reader that asks "is there room *now*" uses.
    ///
    /// The band between the floor and the resume margin is the *ladder's*
    /// hysteresis: it decides when a stopped torrent may be started again.
    /// Judging "is this torrent stopped for want of space?" there instead
    /// of at the floor answers for every paused torrent on a volume with
    /// 513 MiB free -- which `ensure_download_disk_ready` serves from
    /// without complaint and the published cap treats as fine -- and the
    /// client is shown a torrent error over a device that is fine.
    #[tokio::test(start_paused = true)]
    async fn a_paused_torrent_inside_the_margin_is_reported_but_has_room_at_the_floor() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(CACHE_FREE_SPACE_FLOOR + 1));
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        stop_torrent(&enginefs, TEST_HASH).await;

        // The pass takes the volume's reading; the volume never went under
        // the floor at all.
        enginefs.reconcile_tick().await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Paused);

        // Inside the margin the ladder is holding this torrent stopped, and
        // `after_stopping_for_space` will fail its reads once the volume has
        // been short for `STOPPED_READ_STALL_BOUND`. So the readers that
        // report a condition are asked at the ladder's own line: a client
        // that is about to be told `StorageFull` is not told everything is
        // fine, and the running-low bell has been rung for the room that
        // would end it.
        //
        // This asserted the opposite, on the rule that the hysteresis was
        // the ladder's line and nobody else's. That rule made the band an
        // absorbing state: reads refused and `stats.json` reporting
        // buffering with no error -- and the band is where a volume that
        // has just given its slack back sits by construction, since
        // `CacheLimit::effective` stops the instant `available` reaches the
        // floor. A pinned download stalled at whatever percent it had
        // reached, in silence, for good.
        let stats = engine.get_statistics().await;
        assert_eq!(
            stats.phase,
            StartupPhase::Error,
            "the ladder is holding it stopped, so the client is told so"
        );
        assert!(stats.error.is_some());
        assert!(engine.held_stopped_for_space().await);

        // The floor keeps its own reading, deliberately: whether there is
        // room *now* is not what the ladder is waiting for, and the volume
        // is over the floor.
        assert!(!engine.is_stopped_for_space().await);
    }

    /// **With sharing off, nothing uploads until a player reads, and the
    /// upload stops when the player does.** The switch is recomputed where
    /// it can turn on -- the stream opening, so the first byte the player
    /// asks for is already shared -- and on the tick, which is where it
    /// turns off: nothing on the way out of a stream has to remember to
    /// ask.
    #[tokio::test]
    async fn with_sharing_off_the_upload_switch_follows_the_player() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        let upload = enginefs.backend.upload_enabled.clone();
        let told = || *upload.lock().unwrap();

        enginefs.set_seeding_enabled(false).await;
        assert_eq!(told(), Some(false), "off, and nothing is playing");

        enginefs.on_stream_start(TEST_HASH, 0).await;
        assert_eq!(told(), Some(true), "a player is reading: shared at once");
        enginefs.reconcile_tick().await;
        assert_eq!(told(), Some(true), "and the tick agrees while it reads");

        enginefs.on_stream_end(TEST_HASH, 0).await;
        enginefs.reconcile_tick().await;
        assert_eq!(told(), Some(false), "the player left, and the tick saw it");
    }

    /// **With sharing on, the server uploads with nothing playing**, and
    /// the setting moving is applied when it moves, not at the next tick.
    #[tokio::test]
    async fn with_sharing_on_the_upload_switch_stays_on_with_nothing_playing() {
        let (mut enginefs, _counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        let upload = enginefs.backend.upload_enabled.clone();
        let told = || *upload.lock().unwrap();

        enginefs.reconcile_tick().await;
        assert_eq!(told(), Some(true), "the default is on");
        enginefs.set_seeding_enabled(false).await;
        assert_eq!(told(), Some(false), "turned off with nothing playing");
        enginefs.set_seeding_enabled(true).await;
        assert_eq!(told(), Some(true), "turned on again");
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
    /// rollback -- the per-file counter and the multi-file selection --
    /// covered by nothing. `playback_is_live` is a deliberately narrow
    /// oracle: it reads `engine_active_streams` and `active_streams` and
    /// nothing else, because the selections outlive the stream on purpose
    /// (the want-set is planned from them). And the test above runs over a
    /// single-file torrent, so `activate_file` never reaches
    /// `activate_multifile_file` and the selection branch is not entered at
    /// all.
    ///
    /// So this one is multi-file, and reads the two registers directly.
    /// They used to be read through the reconciler -- a count left behind
    /// made `playing` true for ever, so the idle arm could never fire --
    /// and that oracle is gone with the arm: what the ladder reads now is
    /// the liveness cell, which this rollback deliberately does not touch.
    /// The registers still have to be undone, because nothing ages one out
    /// and the want-set is planned from them: a file still registered as
    /// the active one is unioned back into `only_files` on every later
    /// reconcile of the torrent.
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

        // The undo is a spawned task, so the registers are read until they
        // are empty rather than once. Bounded, so a register left behind
        // fails instead of hanging -- and nothing ages one out, so a
        // failure here is for the life of the process.
        let key = (TEST_HASH.to_string(), 1);
        let deadline = tokio::time::Instant::now() + TEST_WAIT_BOUND;
        loop {
            let file_stream = enginefs
                .active_file_streams
                .read()
                .await
                .get(&key)
                .copied()
                .unwrap_or(0);
            let selection = enginefs
                .active_multifile_files
                .read()
                .await
                .contains_key(TEST_HASH);
            if file_stream == 0 && !selection {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the file the abandoned request selected is still registered:                  stream={file_stream} selection={selection}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // What the rollback does *not* undo, deliberately: the server saw a
        // stream open on file 1, so file 1 is the entity being played and
        // the one the viewer left is slack. A request that died on its way
        // to the disk does not bring the previous film back.
        assert_eq!(
            enginefs.live().reading().file_of(TEST_HASH),
            Some(1),
            "the switch stands"
        );
    }

    /// **`focus_torrent` names a torrent; it does not play one.**
    ///
    /// It writes no register and now no liveness either, so on the ladder's
    /// own conditions the torrent it names is one nobody is watching, and
    /// the answer is `Stop` however loudly the caller asked. That is the
    /// right answer and it used to be the dangerous one: the arm that would
    /// have given it was the timer's alone, precisely because `playing` was
    /// read from registers the caller might still be writing, and the one
    /// production call site was safe only because it runs `on_stream_start`
    /// two lines earlier (`routes::stream`).
    ///
    /// The ordering no longer matters for a different reason: what starts
    /// the torrent is the stream open, and the stream open is what writes
    /// the cell. Focus is a want-set hint that follows it.
    #[tokio::test(start_paused = true)]
    async fn focusing_a_torrent_neither_plays_it_nor_starts_it() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        nothing_torrent_is_playing(&enginefs);
        stop_torrent(&enginefs, TEST_HASH).await;

        enginefs.focus_torrent(TEST_HASH).await;

        assert!(
            !enginefs.live().is_torrent(TEST_HASH),
            "focusing a torrent is not the server seeing a stream open on it"
        );
        assert_eq!(
            run_state_of(&enginefs, TEST_HASH).await,
            RunState::Paused,
            "so there is nothing for it to fetch and it stays stopped"
        );
        assert_eq!(counters.start_torrent.load(Ordering::SeqCst), 0);

        // And the call that really does open a stream starts it, from the
        // same conditions, with the same trigger.
        enginefs.on_stream_start(TEST_HASH, 0).await;
        assert_eq!(run_state_of(&enginefs, TEST_HASH).await, RunState::Live);
        assert_eq!(counters.start_torrent.load(Ordering::SeqCst), 1);
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

    /// **And so is a pin that joined an add a stream started.** The torrent
    /// is the stream's, not the pin's, and what its store holds came with
    /// it -- but the pin found no engine before the add (it was in flight),
    /// took itself for the torrent's adder and measured a checking torrent
    /// whose files read 0: a complete file refused for want of the space to
    /// download it all again.
    #[tokio::test]
    async fn a_pin_that_joined_a_streams_add_is_not_measured_while_it_checks() {
        let (mut enginefs, _counters) = test_enginefs_unmanaged_checking();
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.backend.hold_add.store(true, Ordering::SeqCst);
        assert!(matches!(
            enginefs.get_or_begin_add_magnet(TEST_HASH, None).await,
            EngineLookup::Adding(_)
        ));
        let release = async {
            assert!(
                wait_until(TEST_WAIT_BOUND, || {
                    !enginefs.backend.placements.lock().unwrap().is_empty()
                        && enginefs.pin_locks.lock().contains_key(TEST_HASH)
                })
                .await,
                "the stream's add and the pin both got going"
            );
            enginefs.backend.add_hold.add_permits(1);
        };
        let (result, ()) = tokio::join!(enginefs.pin_download(TEST_HASH, 0, None), release);
        assert!(
            result.is_ok(),
            "a joined add was measured while it checked: {:?}",
            result.err()
        );
        assert_eq!(enginefs.backend.placements.lock().unwrap().len(), 1);
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
    /// backend records are gone. Those bytes are the retention owner's, not
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

    /// What the startup sweep must and must not take, now that a pin is the
    /// only claim there is.
    ///
    /// Everything else under the piece root is cache -- and a restart is the
    /// one moment at which nothing is playing, so none of it is worth a
    /// byte. That goes for a torrent librqbit's own records still name: the
    /// session brings it back in a moment and it downloads again if anyone
    /// asks for it, where the download the embedder named beside it is the
    /// thing the user would have lost.
    #[tokio::test]
    async fn the_sweep_keeps_the_pinned_dirs_and_takes_every_other_one() {
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
        let download_dir = root.join("downloads");
        std::fs::create_dir_all(&download_dir).unwrap();
        // The session's own record of the torrent it restored, which used to
        // be a claim of its own and is not one any more.
        std::fs::write(
            download_dir.join("session.json"),
            serde_json::json!({ "torrents": { "0": { "info_hash": TEST_HASH } } }).to_string(),
        )
        .unwrap();
        std::fs::write(download_dir.join(format!("{TEST_HASH}.bitv")), [0u8; 8]).unwrap();

        let pieces = enginefs.piece_store().path().to_path_buf();
        for hash in [TEST_HASH, OTHER_HASH, ORPHAN_HASH] {
            let dir = pieces.join(hash).join("0");
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("0"), [1u8; 1024]).unwrap();
        }

        // The embedder names one torrent the backend did not restore:
        // dormant, and its data has to survive anyway -- the claim is the
        // embedder's, not the session's.
        let pins = crate::piece_store::PinSet::from([(OTHER_HASH.to_string(), vec![0usize])]);
        let report = crate::piece_store::sweep_before_session(&download_dir, Some(&pins)).await;
        assert_eq!(report.removed, 2, "{report:?}");
        assert!(pieces.join(OTHER_HASH).is_dir(), "the pin's");
        assert!(
            !pieces.join(TEST_HASH).exists(),
            "the session records it and it is not pinned, so it is cache"
        );
        assert!(!pieces.join(ORPHAN_HASH).exists(), "nothing claims this");

        assert_eq!(
            crate::piece_store::sweep_before_session(&download_dir, Some(&pins)).await,
            crate::piece_store::SweepReport::default(),
            "and running it again on the next launch does nothing"
        );
        assert!(pieces.join(OTHER_HASH).is_dir());
        enginefs.apply_pins(Some(pins)).await;
        assert_eq!(enginefs.dormant_pinned_downloads().len(), 1);
    }

    /// The sweep runs *before* the session opens, which is the half of it
    /// that cannot be seen from the outside afterwards.
    ///
    /// Opening the session registers a store per restored torrent and seeds
    /// its held set off the disk; a sweep after that would delete pieces the
    /// store has already counted, and every one of them would stay counted
    /// -- held, protected and never reclaimed -- for the life of the
    /// process. So the assertion is made from inside the step that opens the
    /// session: by the time anything can register a store, the disk is
    /// already what the embedder's pin set says it should be.
    #[tokio::test]
    async fn the_sweep_runs_before_the_session_opens() {
        const ORPHAN_HASH: &str = "1111111111111111111111111111111111111111";
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        let download_dir = root.join("downloads");
        let pieces = crate::piece_store::StoreRoot::in_download_dir(&download_dir)
            .path()
            .to_path_buf();
        std::fs::create_dir_all(&download_dir).unwrap();
        let pins = crate::piece_store::PinSet::from([(TEST_HASH.to_string(), vec![0usize])]);
        for hash in [TEST_HASH, ORPHAN_HASH] {
            let dir = pieces.join(hash).join("0");
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("0"), [1u8; 1024]).unwrap();
        }

        let handle = FakeHandle {
            info_hash: TEST_HASH.to_string(),
            counters: Arc::new(FakeCounters::default()),
            files: vec![BackendFileInfo {
                name: "video-0.mkv".to_string(),
                length: 100,
            }],
            init: FakeInit::new(true, Duration::from_secs(60)),
        };
        let seen = Arc::new(AtomicUsize::new(0));
        let enginefs = BackendEngineFS::boot(
            root.join("cache"),
            download_dir.clone(),
            None,
            Some(pins),
            {
                let pieces = pieces.clone();
                let seen = seen.clone();
                let handle = handle.clone();
                move || async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    assert!(
                        !pieces.join(ORPHAN_HASH).exists(),
                        "the unclaimed directory went before anything could open a store on it"
                    );
                    assert!(
                        pieces.join(TEST_HASH).join("0").join("0").is_file(),
                        "and the pinned one is still whole"
                    );
                    Ok((
                        FakeBackend::new(vec![handle.clone()]),
                        HashMap::from([(TEST_HASH.to_string(), handle)]),
                    ))
                }
            },
        )
        .await
        .expect("boot");
        assert_eq!(seen.load(Ordering::SeqCst), 1, "the session opened once");
        // And the pin the embedder named is back on the engine it was for.
        assert_eq!(
            enginefs.pinned_downloads().await,
            vec![PinnedDownload {
                info_hash: TEST_HASH.to_string(),
                file_idx: 0
            }]
        );
    }

    /// **No pin set from the embedder is not an empty pin set.**
    ///
    /// The embedder is the only thing that can say what the user asked to
    /// keep, so a boot it said nothing to knows nothing about it -- and the
    /// one reading of "nothing" that does not destroy an offline download is
    /// "all of it". Every restored torrent is pinned as far as the owner and
    /// the reconciler are concerned, and no pass takes a byte.
    #[tokio::test]
    async fn an_unnamed_pin_set_keeps_every_torrents_bytes() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 5, 6] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);
        // A budget that does not cover the file, so a pass over the file
        // being played would window it and reclaim what falls outside.
        enginefs.set_cache_budget(Some(1));
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        assert!(
            !engine.standing().await.policies.is_empty(),
            "the fixture is one whose policy would give a piece up"
        );

        assert_eq!(enginefs.apply_pins(None).await, 0);
        assert!(enginefs.pins_unknown(), "and it says so");
        assert!(
            engine.is_pinned(),
            "every restored torrent reads as pinned while the set is unknown"
        );
        assert!(
            engine
                .retain(enginefs.store_registry(), &playing(0))
                .await
                .is_none(),
            "and the owner has no pass to make over a file it cannot prove is unpinned"
        );

        // Nobody is playing it, which is the state that empties a torrent.
        nothing_torrent_is_playing(&enginefs);
        for _ in 0..10 {
            enginefs.reconcile_tick().await;
            enginefs.drop_slack().await;
        }
        for piece in [0u32, 1, 5, 6] {
            assert!(
                bucket.join(piece.to_string()).is_file(),
                "piece {piece} was taken from a torrent nothing could prove was unpinned"
            );
        }
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), crate::reconcile::Decision::Run)],
            "a pin runs, and this is treated as a pin"
        );
    }

    /// **A pin taken at runtime is kept by retention, and written nowhere.**
    ///
    /// The embedder holds the record now, so the only thing a pin does on
    /// this side is take effect: the owner stops reclaiming the file's
    /// bytes from the moment `pinned_files` names it -- that file's, and
    /// not the rest of the torrent's. Nothing under the download dir may
    /// change with it -- a second record here is one
    /// this server would then have to keep in step with the embedder's, and
    /// the boot sweep acts on whichever it finds.
    #[tokio::test]
    async fn a_runtime_pin_is_kept_by_retention_and_written_nowhere() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 5, 6] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);
        enginefs.set_cache_budget(Some(1));
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        assert!(
            !engine.standing().await.policies.is_empty(),
            "the fixture is one whose policy would give a piece up"
        );

        // The embedder named nothing pinned, and then the user taps
        // download for offline.
        assert_eq!(
            enginefs
                .apply_pins(Some(crate::piece_store::PinSet::new()))
                .await,
            0
        );
        assert!(!enginefs.pins_unknown(), "an empty set is an answer");
        let before = download_dir_entries(&enginefs.download_dir);
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert!(engine.is_pinned());
        assert!(
            engine
                .retain(enginefs.store_registry(), &playing(0))
                .await
                .is_none(),
            "the owner has no pass to make over a pinned torrent"
        );

        nothing_torrent_is_playing(&enginefs);
        for _ in 0..10 {
            enginefs.reconcile_tick().await;
            enginefs.drop_slack().await;
        }
        for piece in [0u32, 1] {
            assert!(
                bucket.join(piece.to_string()).is_file(),
                "piece {piece} was taken from the file the user has pinned"
            );
        }
        for piece in [5u32, 6] {
            assert!(
                !bucket.join(piece.to_string()).exists(),
                "piece {piece} is the other file's, which nobody pinned"
            );
        }
        assert_eq!(
            download_dir_entries(&enginefs.download_dir),
            before,
            "the pin wrote no record of its own beside the session"
        );
    }

    /// Two files of four pieces, pieces 0, 1, 5 and 6 on the disk, the pin
    /// set named and empty, and then the first file pinned: what the tests
    /// of a pin's scope start from.
    async fn one_file_of_two_pinned() -> (
        BackendEngineFS<FakeBackend>,
        Arc<FakeCounters>,
        Arc<Engine<FakeHandle>>,
        std::path::PathBuf,
        crate::piece_store::PieceStore,
    ) {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 5, 6] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let store = seeded_store(&enginefs, &engine);
        enginefs
            .apply_pins(Some(crate::piece_store::PinSet::new()))
            .await;
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        (enginefs, counters, engine, bucket, store)
    }

    /// **The tick takes what nobody opened of a pinned torrent, and leaves
    /// the pinned file.**
    ///
    /// A pin is per file. The tick's reclaim of the files nothing opened
    /// used to skip any torrent with a pin, so a season with one episode
    /// kept for offline kept every episode the swarm had filled beside it,
    /// for as long as the pin stood. The reconciler still runs the torrent:
    /// "pinned" in its ladder is about the torrent, which has a download to
    /// finish.
    #[tokio::test]
    async fn the_tick_reclaims_a_pinned_torrents_unpinned_files() {
        let (enginefs, _counters, _engine, bucket, _store) = one_file_of_two_pinned().await;
        nothing_torrent_is_playing(&enginefs);
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), crate::reconcile::Decision::Run)],
            "a torrent with a pin runs"
        );
        for piece in [0u32, 1] {
            assert!(
                bucket.join(piece.to_string()).is_file(),
                "piece {piece} is the pinned file's"
            );
        }
        for piece in [5u32, 6] {
            assert!(
                !bucket.join(piece.to_string()).exists(),
                "piece {piece} is a file nobody pinned or opened"
            );
        }
    }

    /// **And so does a slack drop, reading the pin set at each run.**
    ///
    /// No tick has run, so the pinned file has no entity yet and its pieces
    /// are outside every extent like the unpinned file's: the pin set, read
    /// before every run of the reclaim, is the only thing that tells them
    /// apart.
    #[tokio::test]
    async fn a_slack_drop_reclaims_a_pinned_torrents_unpinned_files_and_not_the_pinned_one() {
        let (enginefs, _counters, engine, bucket, _store) = one_file_of_two_pinned().await;
        assert!(
            engine.retention.holding(&0).is_none(),
            "the fixture's pinned file has no entity"
        );
        nothing_torrent_is_playing(&enginefs);
        assert_eq!(enginefs.drop_slack().await, 2);
        for piece in [0u32, 1] {
            assert!(
                bucket.join(piece.to_string()).is_file(),
                "piece {piece} is the pinned file's"
            );
        }
        for piece in [5u32, 6] {
            assert!(
                !bucket.join(piece.to_string()).exists(),
                "piece {piece} is a file nobody pinned or opened"
            );
        }
    }

    /// **A file streamed beside a pinned one is windowed like any other, and
    /// only the pin is protected whole.**
    ///
    /// The owner asked "is anything on this torrent pinned" of every file,
    /// so the episode being watched beside a pinned one was installed on as
    /// a pin: no policy, fetched whole, nothing reclaimed, and the usage
    /// figure called every byte of it protected because a live entity with
    /// no policy keeps its whole extent. Now it has a window like any
    /// unpinned file's, and the figure is the pinned file plus that window.
    #[tokio::test]
    async fn a_file_streamed_beside_a_pinned_one_is_windowed_and_not_protected_whole() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in 0u32..8 {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);
        enginefs
            .apply_pins(Some(crate::piece_store::PinSet::new()))
            .await;
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        // Two pieces of budget over a four-piece file: a split.
        enginefs.set_cache_budget(Some(50));

        enginefs.on_stream_start(TEST_HASH, 1).await;
        engine.begin_retention(1).await;
        engine.note_playhead(1, 0);
        engine.retain(enginefs.store_registry(), &playing(1)).await;
        let standing = engine.standing().await;
        assert_eq!(
            standing
                .policies
                .iter()
                .map(|policy| policy.file_idx)
                .collect::<Vec<_>>(),
            vec![1],
            "the streamed file is bounded, the pinned one is not"
        );

        let holdings = enginefs.cache_holdings().await;
        assert_eq!(holdings.total_bytes, 200);
        assert_eq!(holdings.protected_files, 2);
        assert!(
            holdings.protected_bytes > 100 && holdings.protected_bytes < 200,
            "the pinned file whole and the streamed file's window: {holdings:?}"
        );
    }

    /// Three episodes of 100, 110 and 100 bytes at 25 bytes a piece: the
    /// second is pieces 4..9 and shares piece 8 with the third. The third
    /// is out of the backend's want-set, which is where a pin stands for
    /// the length of the backend call that selects its file: the engine
    /// records the pin first.
    fn a_neighbour_the_backend_does_not_want_yet()
    -> (BackendEngineFS<FakeBackend>, Arc<FakeCounters>) {
        let (enginefs, counters) = test_enginefs_with_files(vec![
            ("Show.S01E01.mkv".into(), 100),
            ("Show.S01E02.mkv".into(), 110),
            ("Show.S01E03.mkv".into(), 100),
        ]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        *counters.wanted_files.lock().unwrap() = Some([0, 1].into_iter().collect());
        (enginefs, counters)
    }

    /// **The piece a file shares with a pinned neighbour is not this file's
    /// to stop wanting.**
    ///
    /// The boundary rule asks the backend's want-set, and a pin reaches the
    /// engine's pin set before the backend has selected the file. Dropped
    /// from the want-set in that gap, the boundary piece stays dropped: the
    /// pinned file wants its pieces whole once, when it is adopted, and a
    /// piece dropped after that is one it never fetches.
    #[tokio::test]
    async fn a_pinned_neighbours_boundary_piece_is_never_dropped_from_the_want_set() {
        let (enginefs, counters) = a_neighbour_the_backend_does_not_want_yet();
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        engine.pinned_files.write().insert(2);
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("4"), [7u8; 25]).unwrap();
        engine.begin_retention(1).await;
        engine.note_playhead(1, 0);
        let _store = seeded_store(&enginefs, &engine);
        engine
            .retain(enginefs.store_registry(), &playing(1))
            .await
            .expect("a pass");
        let dropped = counters.dropped_ranges.lock().unwrap().clone();
        assert!(
            !dropped.is_empty(),
            "the fixture is one whose want step drops something"
        );
        assert!(
            dropped.iter().all(|(range, _)| !range.contains(&8)),
            "piece eight is the pinned episode's too: {dropped:?}"
        );
    }

    /// **A pin taken on the neighbour while a slack reclaim runs keeps the
    /// piece the two share.**
    ///
    /// The door the reclaim asks before every run answers for the file's
    /// own pin. The plan was made before the pin, so it names the boundary
    /// piece, and only a reading of the pin set at the run keeps it.
    #[tokio::test]
    async fn a_pin_taken_on_the_neighbour_mid_reclaim_keeps_the_piece_they_share() {
        let (enginefs, counters) = a_neighbour_the_backend_does_not_want_yet();
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [5u32, 8] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(1).await;
        // The neighbour is pinned while the first run is being dropped.
        *counters.on_first_drop.lock().unwrap() = Some(Box::new({
            let pinned = engine.pinned_files.clone();
            move || {
                pinned.write().insert(2);
            }
        }));
        nothing_torrent_is_playing(&enginefs);
        enginefs.drop_slack().await;
        assert!(!bucket.join("5").exists(), "the first run went");
        assert!(
            bucket.join("8").is_file(),
            "piece eight is the pinned episode's too"
        );
    }

    /// **And a pin taken on the neighbour under the want step keeps the
    /// boundary piece that arrived under it.**
    ///
    /// The want step unlinks what arrived between its reading of the disk
    /// and its drop, asking the door first. The door is about this file;
    /// the neighbour's pin is read beside it.
    #[tokio::test]
    async fn a_pin_taken_on_the_neighbour_under_the_want_step_keeps_the_boundary_piece() {
        let (enginefs, counters) = a_neighbour_the_backend_does_not_want_yet();
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("4"), [7u8; 25]).unwrap();
        let store = Arc::new(seeded_store(&enginefs, &engine));
        engine.begin_retention(1).await;
        engine.note_playhead(1, 0);
        // Piece 8 completes, and the third episode is pinned, while the
        // backend is forgetting the pieces outside the window.
        *counters.on_first_drop.lock().unwrap() = Some(Box::new({
            let store = store.clone();
            let bucket = bucket.clone();
            let pinned = engine.pinned_files.clone();
            move || {
                std::fs::write(bucket.join("8"), [7u8; 25]).unwrap();
                store.init_for_tests().unwrap();
                pinned.write().insert(2);
            }
        }));
        engine
            .retain(enginefs.store_registry(), &playing(1))
            .await
            .expect("a pass");
        assert!(
            counters
                .dropped_ranges
                .lock()
                .unwrap()
                .first()
                .is_some_and(|(range, _)| range.contains(&8)),
            "the fixture's drop is the one the piece arrived under"
        );
        assert!(
            bucket.join("8").is_file(),
            "piece eight is the pinned episode's too"
        );
    }

    /// **What a file beside a pinned one holds back never hides the piece
    /// the two share.**
    ///
    /// An unpinned file's policy holds its whole extent back when it is
    /// installed, and its slack pass does it again on every tick for as
    /// long as the entity holds anything. The boundary piece a pinned
    /// neighbour shares is in that extent, and the slack pass never takes
    /// it -- it is the pinned file's -- so the entity never empties and the
    /// hold-back is never lifted: one piece of a file the user asked to
    /// keep and share, announced to nobody for as long as the pin stands.
    #[tokio::test]
    async fn a_pinned_neighbours_boundary_piece_stays_announced() {
        let (enginefs, counters) = a_neighbour_the_backend_does_not_want_yet();
        *counters.wanted_files.lock().unwrap() = Some([0, 1, 2].into_iter().collect());
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        engine.pinned_files.write().insert(2);
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in 4u32..9 {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(1).await;
        engine.note_playhead(1, 0);
        engine
            .retain(enginefs.store_registry(), &playing(1))
            .await
            .expect("a pass");
        nothing_torrent_is_playing(&enginefs);
        enginefs.drop_slack().await;
        assert!(
            !bucket.join("4").exists() && bucket.join("8").is_file(),
            "the second episode went, and the piece it shares with the pinned one stayed"
        );
        let advertised = counters.advertised.lock().unwrap().clone();
        assert!(
            advertised
                .iter()
                .any(|(range, on)| !on && range.contains(&4)),
            "the second episode was held back: {advertised:?}"
        );
        assert!(
            advertised
                .iter()
                .rev()
                .find(|(range, _)| range.contains(&8))
                .is_none_or(|(_, on)| *on),
            "piece eight is the pinned episode's, and announced: {advertised:?}"
        );
    }

    /// The set the embedder names is applied at startup to the torrents the
    /// backend restored; the hash is matched however it was cased, and a
    /// pin of a file the torrent does not have is dropped.
    #[tokio::test]
    async fn the_named_pins_are_applied_to_the_restored_torrents() {
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

        // Nothing named: nothing applied, and the set is known all the same.
        let (first, _counters) = make(3);
        assert_eq!(
            first
                .apply_pins(Some(crate::piece_store::PinSet::new()))
                .await,
            0,
            "nothing yet"
        );
        assert!(!first.pins_unknown(), "an empty set is an answer");
        first.pin_download(TEST_HASH, 2, None).await.unwrap();
        first.pin_download(TEST_HASH, 1, None).await.unwrap();
        assert!(
            first
                .unpin_download(TEST_HASH, 2, false)
                .await
                .unwrap()
                .unpinned
        );
        drop(first);

        // "Restart": a new engine over the backend's restored torrent, with
        // the pin the embedder kept handed back in.
        let (second, counters) = make(3);
        assert!(second.pinned_downloads().await.is_empty());
        assert_eq!(
            second
                .apply_pins(Some(crate::piece_store::PinSet::from([(
                    TEST_HASH.to_string(),
                    vec![1usize]
                )])))
                .await,
            1
        );
        let engine = second.get_engine(TEST_HASH).await.unwrap();
        assert_eq!(engine.pinned_file_indices(), vec![1]);
        assert!(engine.is_pinned(), "exempt from eviction again");
        assert_eq!(counters.pin_file.load(Ordering::SeqCst), 1);
        assert_eq!(engine.get_statistics().await.pinned_files, vec![1]);
        drop(second);

        // An index the torrent does not have is dropped; a torrent the
        // backend does not have keeps its pin (dormant, see below); the
        // hash is matched whatever case the embedder spells it in.
        let (third, _counters) = make(3);
        assert_eq!(
            third
                .apply_pins(Some(crate::piece_store::PinSet::from([
                    (OTHER_HASH.to_string(), vec![0usize]),
                    (TEST_HASH.to_uppercase(), vec![0usize, 7]),
                ])))
                .await,
            1
        );
        assert_eq!(
            third.pinned_downloads().await,
            vec![PinnedDownload {
                info_hash: TEST_HASH.to_string(),
                file_idx: 0
            }]
        );
        assert_eq!(third.dormant_pinned_downloads().len(), 1, "the other hash");
    }

    /// A pin whose torrent the backend did not bring back at startup (its
    /// output folder on a volume that was not mounted -- librqbit skips the
    /// torrent but keeps its record) is not lost: it is held dormant for
    /// that run, applied on a later boot that has the torrent, comes along
    /// with a pin of the torrent made before then, and is dropped by an
    /// unpin.
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
        let pinned = |indices: Vec<usize>| {
            crate::piece_store::PinSet::from([(TEST_HASH.to_string(), indices)])
        };

        // Boot without the torrent: nothing applied, nothing lost.
        let (second, _counters) = make(false, OTHER_HASH);
        assert_eq!(second.apply_pins(Some(pinned(vec![1]))).await, 0);
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
        second.pin_download(OTHER_HASH, 0, None).await.unwrap();
        assert_eq!(
            second.dormant_pinned_downloads(),
            vec![PinnedDownload {
                info_hash: TEST_HASH.to_string(),
                file_idx: 1
            }],
            "a pin of another torrent leaves the dormant one where it is"
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
        drop(second);

        // Next boot with the torrent back: applied.
        let (third, _counters) = make(true, TEST_HASH);
        assert_eq!(third.apply_pins(Some(pinned(vec![1]))).await, 1);
        assert_eq!(
            third
                .get_engine(TEST_HASH)
                .await
                .unwrap()
                .pinned_file_indices(),
            vec![1]
        );
        drop(third);

        // Absent at boot, pinned again before the next one (the client's
        // re-pin): the dormant pins come with the new one.
        let (fourth, counters) = make(false, TEST_HASH);
        assert_eq!(fourth.apply_pins(Some(pinned(vec![1, 2]))).await, 0);
        let engine = fourth.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert_eq!(engine.pinned_file_indices(), vec![0, 1, 2]);
        assert_eq!(engine.get_statistics().await.pinned_files, vec![0, 1, 2]);
        assert_eq!(counters.pin_file.load(Ordering::SeqCst), 3);
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
        engine.begin_retention(0).await;
        engine.note_playhead(0, 60);
        let _store = seeded_store(&enginefs, &engine);

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
        engine.begin_retention(1).await;
        engine.note_playhead(1, 25);
        let _store = seeded_store(&enginefs, &engine);

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

    /// **What the cache holds is the store's own count, and what no pass
    /// may take is a pin or a live window -- neither read off the disk.**
    ///
    /// The figure behind `GET /cache.json` used to be an eviction pass's
    /// walk: a `statx` of every file in the tree, so it was as old as the
    /// last pass and absent before the first. The store keeps a bit per
    /// piece it holds and the layout that prices each bit, so the total is
    /// a sum in memory, and the two claims on it are the owners' own --
    /// which is why a file nobody is playing and nobody has pinned protects
    /// nothing, however many bytes of it are on the disk.
    #[tokio::test]
    async fn the_cache_figure_is_the_stores_count_and_the_owners_protections() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        // Two pieces of budget over a four-piece file: a split, so a policy
        // is installed and a window is something less than the whole file.
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);

        let idle = enginefs.cache_holdings().await;
        assert_eq!(idle.total_bytes, 100, "four pieces of twenty-five bytes");
        assert_eq!(
            (idle.protected_bytes, idle.protected_files),
            (0, 0),
            "nobody is playing it and nobody pinned it, so nothing holds it"
        );

        // Now it is the stream being played, with a pass behind it.
        enginefs.on_stream_start(TEST_HASH, 0).await;
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass ran");
        let live = enginefs.cache_holdings().await;
        assert_eq!(live.total_bytes, 100, "a pass that kept its window");
        assert_eq!(live.protected_files, 1);
        assert!(
            live.protected_bytes > 0 && live.protected_bytes < 100,
            "the window and the committed half, not the whole file: {live:?}"
        );

        // And a pin keeps every piece of its file, window or no window.
        engine.pinned_files.write().insert(0);
        let pinned = enginefs.cache_holdings().await;
        assert_eq!(
            (pinned.protected_bytes, pinned.protected_files),
            (100, 1),
            "a pin is the user asking for the file, not for a window of it"
        );

        // And a directory no store speaks for -- a torrent this session
        // holds in Error, or one a previous process left. No bits are kept
        // for it, so it is the one thing here that costs a `stat`; it is on
        // the volume, so it is in the total, and nothing speaks for it, so
        // nothing protects it.
        let orphan = enginefs
            .piece_store()
            .torrent_dir("89abcdef0123456789abcdef0123456789abcdef")
            .join("0");
        std::fs::create_dir_all(&orphan).unwrap();
        std::fs::write(orphan.join("0"), [3u8; 40]).unwrap();
        let stray_bytes =
            crate::chunk_store::occupied_bytes(&std::fs::metadata(orphan.join("0")).unwrap());
        let with_stray = enginefs.cache_holdings().await;
        assert_eq!(with_stray.total_bytes, 100 + stray_bytes);
        assert_eq!(
            (with_stray.protected_bytes, with_stray.protected_files),
            (100, 1),
            "the pin still, and nothing new"
        );
    }

    /// **A dormant pin's bytes are protected, though no engine speaks for
    /// them.**
    ///
    /// A pin the session has not restored a torrent for has no engine and
    /// no registered store, so its directory is in the unregistered half of
    /// the total -- `stat`ed, because nothing holds bits for it -- and
    /// nothing in the engines' loop above ever names it. Nothing can take
    /// those bytes either: the pin stands, every unlink goes through a
    /// registered store and there is none, and the sweep at launch keeps
    /// the pin set. So reporting them as reclaimable tells a client a
    /// shortfall has a remedy it has not got, which is the same wrong
    /// answer as calling slack protected, in the other direction.
    #[tokio::test]
    async fn a_dormant_pins_bytes_are_protected_though_no_engine_speaks_for_them() {
        let (enginefs, _counters) = test_enginefs_unmanaged();
        let pieces = enginefs.piece_store().torrent_dir(TEST_HASH);
        std::fs::create_dir_all(pieces.join("0")).unwrap();
        let piece = pieces.join("0").join("1");
        std::fs::write(&piece, [7u8; 100]).unwrap();
        let bytes = crate::chunk_store::occupied_bytes(&std::fs::metadata(&piece).unwrap());

        let unclaimed = enginefs.cache_holdings().await;
        assert_eq!(
            (unclaimed.total_bytes, unclaimed.protected_bytes),
            (bytes, 0),
            "no store speaks for it and nobody has pinned it: ordinary cache"
        );

        std::fs::create_dir_all(&enginefs.download_dir).unwrap();
        let pins = crate::piece_store::PinSet::from([(TEST_HASH.to_string(), vec![0usize])]);
        // Dormant: the record names it, and no engine came back with it.
        assert_eq!(enginefs.apply_pins(Some(pins)).await, 0);

        let pinned = enginefs.cache_holdings().await;
        assert_eq!(
            (
                pinned.total_bytes,
                pinned.protected_bytes,
                pinned.protected_files
            ),
            (bytes, bytes, 1),
            "the user asked for those bytes and nothing here may take them: {pinned:?}"
        );
    }

    /// **A torrent that comes back through a stream, not a pin, comes back
    /// pinned.**
    ///
    /// librqbit skips a torrent it cannot re-add at startup, the embedder's
    /// record still names its pin, and `apply_pins` keeps that pin dormant
    /// for whatever brings the torrent back. Only `pin_download` used to
    /// apply it. The ordinary way back is a viewer pressing play: the
    /// stream route adds the magnet, the store is seeded from the kept
    /// directory, and the engine that was published for it had no pin --
    /// so the pass reclaimed the pinned file outside the window, the idle
    /// sweep removed the torrent with its files once the stream ended, and
    /// `cache_holdings` reported the bytes protected the whole time,
    /// because the dormant record still spoke for them.
    ///
    /// The pin goes into the engine under the registry's write lock, so
    /// the engine is never visible unpinned; the handle's copy follows.
    #[tokio::test(start_paused = true)]
    async fn a_torrent_streamed_back_with_a_dormant_pin_is_pinned_before_it_is_seen() {
        let (mut enginefs, counters) = test_enginefs_unmanaged();
        if let Some(sweep) = enginefs.take_sweep_task() {
            sweep.abort();
        }
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        // The boot: the record names file 0, and the backend restored no
        // engine for the torrent.
        let pins = crate::piece_store::PinSet::from([(TEST_HASH.to_string(), vec![0usize])]);
        assert_eq!(enginefs.apply_pins(Some(pins)).await, 0);
        assert_eq!(enginefs.dormant_pinned_downloads().len(), 1);

        // The viewer streams it: the stream route's add, not a pin.
        let engine = enginefs
            .get_or_add_magnet(TEST_HASH, None)
            .await
            .expect("the stream's add");
        assert!(
            engine.is_pinned(),
            "the engine the stream published carries the pin the record named"
        );
        assert_eq!(engine.pinned_file_indices(), vec![0]);
        assert_eq!(
            counters.pin_file.load(Ordering::SeqCst),
            1,
            "and the handle's want-set planner was told"
        );
        assert!(enginefs.dormant_pinned_downloads().is_empty());
        assert_eq!(
            enginefs.pinned_downloads().await,
            vec![PinnedDownload {
                info_hash: TEST_HASH.to_string(),
                file_idx: 0,
            }]
        );

        // The kept directory has the whole file; playback opens at its
        // head under a one-piece window. Unpinned, the pass would take
        // every piece the window does not cover.
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);
        enginefs.set_cache_budget(Some(50));
        enginefs.on_stream_start(TEST_HASH, 0).await;
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);

        enginefs.reconcile_tick().await;
        for piece in [0u32, 1, 2, 3] {
            assert!(
                bucket.join(piece.to_string()).is_file(),
                "piece {piece} of the pinned file is still on the disk after the tick"
            );
        }
        assert!(
            counters.dropped_ranges.lock().unwrap().is_empty(),
            "and the backend was not asked to forget a piece of it"
        );
    }

    /// **A file that stopped being the live one protects nothing, though
    /// its policy is still standing.**
    ///
    /// The rule the figure is built on, and the one it would be easiest to
    /// get backwards. A holding does not go when the viewer opens
    /// something else -- the window and the committed half stay in the map
    /// until the slack pass has taken the bytes -- so a protection read off
    /// the holdings alone would report a whole film as unreclaimable for as
    /// long as the entity lived. What that costs is not a wrong number: a
    /// client shown `protected == total` over a cache that is over its
    /// limit is being told the shortfall has no remedy, when the remedy is
    /// the pass that is already running.
    #[tokio::test]
    async fn a_file_that_stopped_being_live_protects_nothing() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);

        enginefs.on_stream_start(TEST_HASH, 0).await;
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass ran");
        let playing = enginefs.cache_holdings().await;
        assert!(
            playing.protected_bytes > 0 && playing.protected_files == 1,
            "the window it is playing inside: {playing:?}"
        );

        // The viewer opened a proxied stream. Nothing about this file's
        // policy changed -- the same window, the same committed half, the
        // same pieces on the disk -- and every one of those bytes is now
        // slack.
        enginefs.live().open(
            crate::retention::live::LiveEntity::Proxy {
                dir: std::path::PathBuf::from("/proxy/other-film"),
            },
            false,
        );
        assert!(
            engine.retention.holding(&0).is_some(),
            "the holding is still there, which is what makes this a test of the rule \
             and not of the map"
        );
        let slack = enginefs.cache_holdings().await;
        assert_eq!(
            (
                slack.total_bytes,
                slack.protected_bytes,
                slack.protected_files
            ),
            (100, 0, 0),
            "every byte of it is still on the disk, and every byte of it is \
             on its way off: {slack:?}"
        );
    }

    /// **A live file nothing bounds keeps the whole of itself.**
    ///
    /// No budget covers it, so no policy is installed and no window exists
    /// -- and while it is live nothing reclaims any of it, so the honest
    /// figure is its whole extent. Reporting 0 here would say a stream
    /// being played is entirely reclaimable, which is the one thing that is
    /// never true of it.
    #[tokio::test]
    async fn a_live_file_nothing_bounds_keeps_the_whole_of_itself() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        // A budget far above the file: there is nothing here to bound.
        enginefs.set_cache_budget(Some(10_000));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);

        enginefs.on_stream_start(TEST_HASH, 0).await;
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        // A pass that concludes nothing: with no budget to measure against
        // there is nothing for it to keep or take.
        engine.retain(enginefs.store_registry(), &playing(0)).await;
        let holding = engine.retention.holding(&0).expect("an entity of its own");
        assert!(
            holding.installed.is_none() && holding.windows.is_empty(),
            "nothing bounds it, so there is no window to report"
        );

        let holdings = enginefs.cache_holdings().await;
        assert_eq!(
            (holdings.protected_bytes, holdings.protected_files),
            (100, 1),
            "the whole of what it holds: {holdings:?}"
        );
    }

    /// **A retention pass must not blink the panel's rows out.**
    ///
    /// A pass used to take the policy out of its slot for a directory
    /// listing and two awaited backend calls, and it runs on the
    /// reconciler's tick for exactly the stream a panel is asking about.
    /// Anything that answered from that slot would have said "no window, no
    /// committed set" for a second or so out of every two -- and by this
    /// server's own contract that is not a delay but a statement: it means
    /// nothing is bounding this stream. The policy is resident now and the
    /// panel reads it live; this pins that a pass in flight does not blink
    /// it out.
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

        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
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
            let registry = enginefs.store_registry().clone();
            async move { engine.retain(&registry, &playing(0)).await }
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

    /// **A retention pass lists no directory.**
    ///
    /// It used to walk the torrent's piece directories on every tick -- one
    /// `read_dir` per thousand pieces, on the blocking pool, holding the
    /// file's turn across it -- to learn what the store already knew. The
    /// store's directory is walked once, when `init` seeds its held set,
    /// and every pass after that reads the set the store keeps: fifty
    /// passes over a store with pieces to commit and reclaim leave the
    /// listing count where the seed put it. The count is every `read_dir`
    /// the chunk store makes under the torrent's directory, by any of its
    /// doors -- the listing the pass used to take went through
    /// `ChunkDir::held`, and a counter on the seed's walk alone would not
    /// have seen it come back.
    #[tokio::test]
    async fn a_retention_pass_walks_no_directory() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        // Two pieces of budget over a four-piece file: a split, so a policy
        // is installed and a pass has something to ask.
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let store = seeded_store(&enginefs, &engine);
        let listings = || {
            crate::chunk_store::LISTINGS
                .lock()
                .get(store.dir())
                .copied()
                .unwrap_or(0)
        };
        let seeded = listings();
        assert!(seeded > 0, "the seed listed the directory");
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);

        let first = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass ran");
        assert!(
            first.reclaimed > 0,
            "the pass read the seeded set and gave the far pieces back: {first:?}"
        );
        engine.note_playhead(0, 75);
        for _ in 0..49 {
            engine.retain(enginefs.store_registry(), &playing(0)).await;
        }
        assert_eq!(
            listings(),
            seeded,
            "fifty passes, and nothing listed the directory after the seed"
        );
    }

    /// **A reader fetches ahead by the smaller of its intent's cap and the
    /// window's reach, and never by nothing.**
    ///
    /// The cap alone (128 MiB for every request after the first) had a
    /// stream ask librqbit for the whole rest of a file the budget covered
    /// a fraction of; librqbit refuses to drop what a stream is about to
    /// read, so the disk sat over the budget by the lookahead for the
    /// stream's life and every pass asked for the same refused pieces again.
    /// The reach is the bytes from the reader's offset to the *start* of the
    /// last piece the window reaches ahead of it -- librqbit rounds the end
    /// of a lookahead up to a whole piece, and a reader drifts inside its
    /// piece as it plays, so a lookahead cut at the edge itself reaches one
    /// piece past it half the time. Forty pieces of five bytes under a
    /// budget of twenty: a ten-piece window, one behind, nine ahead.
    #[tokio::test]
    async fn a_reader_fetches_ahead_by_the_smaller_of_its_cap_and_the_windows_reach() {
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 200)]);
        counters.pieces_per_file.store(40, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(100));
        let cap = crate::backend::priorities::librqbit_stream_lookahead_bytes(
            PlaybackIntent::DirectSeek,
            BufferProfile::Normal,
        );
        assert!(
            cap > 200,
            "the cap is wider than the file, so the window is the bound"
        );

        // At the file's start: the reach is pieces 0..9, and the last piece
        // it reaches starts at byte 40.
        let _at_start = engine
            .try_get_file_with_intent(0, 0, 255, PlaybackIntent::DirectSeek, BufferProfile::Normal)
            .await
            .expect("a reader");
        // Seven bytes in, on piece 1: the reach is 1..10, byte 45, minus
        // the seven already behind.
        let _seven_in = engine
            .try_get_file_with_intent(0, 7, 255, PlaybackIntent::DirectSeek, BufferProfile::Normal)
            .await
            .expect("a reader");
        assert_eq!(
            *counters.lookaheads.lock().unwrap(),
            vec![40, 38],
            "the window's reach, in bytes from where the reader starts"
        );

        // A window of one piece reaches no further than the piece under the
        // reader: the bound is zero bytes, and the stream still reads one.
        enginefs.set_cache_budget(Some(10));
        let _one_piece = engine
            .try_get_file_with_intent(0, 3, 255, PlaybackIntent::DirectSeek, BufferProfile::Normal)
            .await
            .expect("a reader");
        assert_eq!(counters.lookaheads.lock().unwrap().last(), Some(&1));

        // Nothing bounds the file: the cap is the whole of it.
        enginefs.set_cache_budget(None);
        let _unbounded = engine
            .try_get_file_with_intent(0, 0, 255, PlaybackIntent::DirectSeek, BufferProfile::Normal)
            .await
            .expect("a reader");
        assert_eq!(counters.lookaheads.lock().unwrap().last(), Some(&cap));
    }

    /// **The reach is measured in the reader's own file, from the reader's
    /// own offset.** The window's pieces are the torrent's; the bytes a
    /// reader is handed are from where it starts in its file. A file that
    /// begins a hundred bytes into the torrent has a hundred bytes taken off
    /// the piece arithmetic before the reader's offset is -- without that,
    /// every reader of every file after the first would fetch its file's
    /// offset past the window, the whole first episode for the second. And
    /// a reader inside the file's last piece, which the reach cannot go
    /// beyond, is bounded to the one byte the stream insists on.
    #[tokio::test]
    async fn a_reader_in_a_later_file_is_bounded_from_its_own_offset() {
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        let (enginefs, counters) = test_enginefs_with_files(vec![
            ("Show.S01E01.mkv".into(), 100),
            ("Show.S01E02.mkv".into(), 200),
        ]);
        // Twenty-five byte pieces: episode two is pieces 4..12, from byte
        // 100 of the torrent. A budget of a hundred over its two hundred
        // bytes is four pieces, two of them the window.
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(100));

        // At the top of episode two, on piece 4: the reach is 4..6, and
        // piece 5 starts at torrent byte 125 -- byte 25 of the file.
        let _at_start = engine
            .try_get_file_with_intent(1, 0, 255, PlaybackIntent::DirectSeek, BufferProfile::Normal)
            .await
            .expect("a reader");
        // Three bytes in: the same piece, three fewer bytes to it.
        let _three_in = engine
            .try_get_file_with_intent(1, 3, 255, PlaybackIntent::DirectSeek, BufferProfile::Normal)
            .await
            .expect("a reader");
        // Inside the file's last piece, which starts at byte 175 of the
        // file: the reach is that piece alone, and the bound cannot be
        // measured backwards.
        let _at_end = engine
            .try_get_file_with_intent(
                1,
                197,
                255,
                PlaybackIntent::DirectSeek,
                BufferProfile::Normal,
            )
            .await
            .expect("a reader");
        assert_eq!(*counters.lookaheads.lock().unwrap(), vec![25, 22, 1]);
    }

    /// **A pass leaves the backend wanting the window and the committed
    /// set, and nothing else it does not already have.**
    ///
    /// librqbit fetches every selected piece it is not told otherwise
    /// about, so a pass that only reclaimed would have the swarm refill the
    /// file behind it every two seconds. After a pass the fake's want-set --
    /// its pieces minus what was dropped -- is the window, the committed
    /// set and what is on the disk; moving the head re-wants exactly the
    /// pieces the window moved onto. Eight pieces of twenty-five bytes under
    /// a budget of four: a two-piece window and two committed.
    #[tokio::test]
    async fn a_pass_wants_the_window_and_the_committed_set_and_nothing_else_it_lacks() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 200)]);
        counters.pieces_per_file.store(8, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(100));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        let selected = || -> std::collections::BTreeSet<u32> {
            let dropped = counters.dropped.lock().unwrap();
            (0..8).filter(|piece| !dropped.contains(piece)).collect()
        };

        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        assert_eq!(
            selected(),
            std::collections::BTreeSet::from([0, 1]),
            "the window round piece 0; the six pieces beyond it are not wanted"
        );
        assert_eq!(*counters.reselected.lock().unwrap(), vec![0..2]);

        // Playback walks on to piece 2 with pieces 1 and 2 arrived: 0 and 1
        // commit, the window is 2..4.
        for piece in [1u32, 2] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        store.init_for_tests().unwrap();
        engine.note_playhead(0, 50);
        let pass = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        assert_eq!(pass.committed, 2, "{pass:?}");
        assert_eq!(
            selected(),
            std::collections::BTreeSet::from([0, 1, 2, 3]),
            "the window, the committed set and what is on the disk"
        );
        assert_eq!(
            counters.reselected.lock().unwrap().last(),
            Some(&(2..4)),
            "the pieces the window moved onto are wanted again"
        );
    }

    /// **A piece that arrives under the pass, outside the window, does not
    /// outlive the drop that forgot it.**
    ///
    /// The pass read the held set two backend calls before it trims the
    /// want-set, and a piece outside the window can complete in that gap.
    /// librqbit drops it -- it is a have piece -- and a have piece dropped
    /// and left on the disk is one the store counts and the backend has
    /// forgotten, until a later pass offers it again. So what the backend
    /// reports dropped is read against the store now, and the piece that
    /// arrived goes with the claim. The
    /// fixture lands the piece from inside the backend's own drop call,
    /// which is the gap exactly.
    #[tokio::test]
    async fn a_piece_that_arrives_under_the_pass_is_unlinked_with_the_drop_that_forgot_it() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let store = Arc::new(seeded_store(&enginefs, &engine));
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        // Piece 2 completes while the backend is forgetting 1..4.
        *counters.on_first_drop.lock().unwrap() = Some(Box::new({
            let store = store.clone();
            let bucket = bucket.clone();
            move || {
                std::fs::write(bucket.join("2"), [7u8; 25]).unwrap();
                store.init_for_tests().unwrap();
            }
        }));

        let pass = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        assert_eq!(
            pass.reclaimed, 0,
            "nothing was held outside the window when the pass decided: {pass:?}"
        );
        assert!(
            !bucket.join("2").exists(),
            "the piece the backend forgot is not left on the disk"
        );
        assert_eq!(
            enginefs
                .store_registry()
                .held(TEST_HASH)
                .expect("registered")
                .in_range(0..4),
            std::collections::BTreeSet::from([0]),
            "and the set says so"
        );
        assert_eq!(
            *counters.dropped_ranges.lock().unwrap(),
            vec![(1..4, crate::backend::AfterRelease::LeaveDropped)],
            "one ask, the want-set's"
        );
    }

    /// **And a pin taken under the want step keeps the piece that arrived
    /// under it.**
    ///
    /// The unlink above is an unlink, and every unlink of this owner asks
    /// the door at its instant: the pass read "not pinned" at its first
    /// step, two backend calls before the drop, and a pin lands in that gap
    /// as easily as a piece does. Same fixture as the test above with the
    /// pin landing in the same hook: the backend forgets the piece, the door
    /// says take nothing, the claim is released with the bytes still on the
    /// disk and in the set -- and the pin's next pass wants the whole file
    /// again, which downloads that one piece back over itself. A piece
    /// fetched twice, against a piece of a pinned file deleted.
    #[tokio::test]
    async fn a_pin_taken_under_the_want_step_keeps_the_piece_that_arrived_under_it() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let store = Arc::new(seeded_store(&enginefs, &engine));
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        // Piece 2 completes and the user pins the file while the backend is
        // forgetting 1..4.
        *counters.on_first_drop.lock().unwrap() = Some(Box::new({
            let store = store.clone();
            let bucket = bucket.clone();
            let engine = engine.clone();
            move || {
                std::fs::write(bucket.join("2"), [7u8; 25]).unwrap();
                store.init_for_tests().unwrap();
                engine.pinned_files.write().insert(0);
            }
        }));

        let pass = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        assert_eq!(pass.reclaimed, 0, "{pass:?}");
        assert_eq!(
            *counters.dropped_ranges.lock().unwrap(),
            vec![(1..4, crate::backend::AfterRelease::LeaveDropped)],
            "the want-set's one ask, and nothing after the pin"
        );
        assert!(
            bucket.join("2").is_file(),
            "the piece of the file the user has just asked to keep is still here"
        );
        assert_eq!(
            enginefs
                .store_registry()
                .held(TEST_HASH)
                .expect("registered")
                .in_range(0..4),
            std::collections::BTreeSet::from([0, 2]),
            "and the set says so"
        );
        assert!(
            counters.claim_released_on.lock().unwrap().is_some(),
            "the backend's claim was released, not leaked"
        );

        // The pin's pass wants every piece of the file: the one the backend
        // forgot under the pin is fetched again over the bytes it kept.
        engine.retain(enginefs.store_registry(), &playing(0)).await;
        assert_eq!(counters.reselected.lock().unwrap().last(), Some(&(0..4)));
    }

    /// **And a seek onto the piece that arrived keeps it too.** The door's
    /// window is drawn round where playback is *now*, not where the pass
    /// decided: a reader that has moved onto a piece the backend was asked
    /// to forget is about to read it, and the pass leaves it on the disk as
    /// the reclaim would have left a run the window moved into.
    #[tokio::test]
    async fn a_seek_onto_the_piece_that_arrived_under_the_want_step_keeps_it() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let store = Arc::new(seeded_store(&enginefs, &engine));
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        // Piece 2 completes and the reader seeks onto it while the backend
        // is forgetting 1..4.
        *counters.on_first_drop.lock().unwrap() = Some(Box::new({
            let store = store.clone();
            let bucket = bucket.clone();
            let engine = engine.clone();
            move || {
                std::fs::write(bucket.join("2"), [7u8; 25]).unwrap();
                store.init_for_tests().unwrap();
                engine.note_playhead(0, 50);
            }
        }));

        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        assert!(
            bucket.join("2").is_file(),
            "the piece under the reader's new position is not taken from under it"
        );
        assert_eq!(
            enginefs
                .store_registry()
                .held(TEST_HASH)
                .expect("registered")
                .in_range(0..4),
            std::collections::BTreeSet::from([0, 2])
        );
    }

    /// **And a piece that arrives under the want step inside another
    /// reader's window stays.** The door the want step asks is the one the
    /// reclaim asks, and it answers a window per open reader: two players
    /// on one file deliver in turn, so the file's head is whichever byte
    /// went out last and the other player's window is not round it. Both
    /// readers move inside the drop; the piece under the one whose byte was
    /// not the last is kept because it has a head of its own, where a door
    /// that drew one window round the file's head took it.
    #[tokio::test]
    async fn a_piece_that_arrives_under_the_want_step_inside_another_readers_window_stays() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let store = Arc::new(seeded_store(&enginefs, &engine));
        engine.begin_retention(0).await;
        // Held by the test across the pass: the hook below is consumed when
        // it has run, and a reader dropped with it is a reader gone.
        let first = Arc::new(engine.retention.reader_on(&0).expect("the entity"));
        let second = Arc::new(engine.retention.reader_on(&0).expect("the entity"));
        assert!(first.note((0, 0)).is_none());
        // Piece 2 completes while the backend is forgetting 1..4; the
        // second player reads onto it, and then the first delivers again,
        // so the file's head is back on piece 0.
        *counters.on_first_drop.lock().unwrap() = Some(Box::new({
            let store = store.clone();
            let bucket = bucket.clone();
            let first = first.clone();
            let second = second.clone();
            move || {
                std::fs::write(bucket.join("2"), [7u8; 25]).unwrap();
                store.init_for_tests().unwrap();
                assert!(second.note((0, 50)).is_none());
                assert!(first.note((0, 1)).is_none());
            }
        }));

        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        assert!(
            bucket.join("2").is_file(),
            "the piece under the second player was taken from under it"
        );
        assert_eq!(
            enginefs
                .store_registry()
                .held(TEST_HASH)
                .expect("registered")
                .in_range(0..4),
            std::collections::BTreeSet::from([0, 2])
        );
        assert_eq!(
            engine.retention.holding(&0).unwrap().last_position,
            Some((0, 1)),
            "the file's head is the first player's last byte"
        );
        assert_eq!(engine.retention.readers_of(&0), 2);
    }

    /// **A reclaim unlinks through the registered store, so the pieces leave
    /// the held set with their files.**
    ///
    /// The pass reads the set and never the directory, so an unlink that
    /// went round the store -- by path, as every reclaim used to -- would
    /// leave the bits standing over files that had gone: the next pass
    /// would offer the same pieces again, ask the backend to forget them
    /// again, and count a delete of nothing, every tick for the rest of the
    /// session. Through the store, the set is the disk after the reclaim as
    /// it was before it, and the next pass has nothing to ask.
    #[tokio::test]
    async fn a_reclaim_takes_the_pieces_out_of_the_held_set_with_their_files() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);

        let pass = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass ran");
        assert_eq!(pass.reclaimed, 3, "the three outside the window: {pass:?}");
        assert_eq!(
            enginefs
                .store_registry()
                .held(TEST_HASH)
                .expect("registered")
                .in_range(0..4),
            std::collections::BTreeSet::from([0]),
            "the set is the disk: the reclaimed pieces left it with their files"
        );
        assert!(bucket.join("0").is_file() && !bucket.join("1").exists());
        assert_eq!(
            counters.dropped_ranges.lock().unwrap().len(),
            1,
            "one drop, for the one run"
        );

        let pass = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass ran");
        assert_eq!(
            pass,
            crate::retention::RetentionPass::default(),
            "and the next pass finds nothing to give back: {pass:?}"
        );
        assert_eq!(
            *counters.dropped_ranges.lock().unwrap(),
            vec![
                (1..4, crate::backend::AfterRelease::LeaveDropped),
                (1..4, crate::backend::AfterRelease::LeaveDropped),
            ],
            "so it asked the backend to forget nothing more: the second ask is \
             the want-set's, for the three pieces it gave back and does not \
             want fetched again"
        );
    }

    /// **A reclaim from a paused torrent goes through.** Paused is a settled
    /// state: librqbit's `drop_pieces` edits a paused torrent's have-set as
    /// it edits a live one's, and the design's slack pass drops from exactly
    /// such a torrent. Only a check in progress and a torrent with no
    /// storage stop the reclaim; a guard that let only Live through would
    /// leave a paused torrent's disk where it is for as long as it stays
    /// paused.
    #[tokio::test]
    async fn a_reclaim_from_a_paused_torrent_goes_through() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        counters.paused.store(true, Ordering::SeqCst);
        assert_eq!(engine.handle.run_state(), RunState::Paused);

        let pass = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        assert_eq!(
            pass.reclaimed, 3,
            "the three outside the window go from a paused torrent: {pass:?}"
        );
        assert!((1..4).all(|piece| !bucket.join(piece.to_string()).exists()));
        assert!(bucket.join("0").is_file());
    }

    /// **The held set the pass is handed is this file's, not the torrent's.**
    /// The registry answers for the whole torrent; the backing narrows it to
    /// the file's extent before the owner sees it, so a policy over one
    /// episode of a pack is never handed the other episodes' pieces to
    /// decide about.
    #[tokio::test]
    async fn the_held_set_the_backing_hands_the_pass_is_the_files_own() {
        let (enginefs, counters) = test_enginefs_with_files(vec![
            ("Show.S01E01.mkv".into(), 100),
            ("Show.S01E02.mkv".into(), 100),
        ]);
        // Four pieces per file: episode one is 0..4, episode two 4..8.
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in 0..8u32 {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);
        assert_eq!(
            enginefs
                .store_registry()
                .held(TEST_HASH)
                .expect("registered")
                .count(),
            8,
            "the registry holds the whole torrent"
        );
        assert_eq!(
            engine.held_in_file(enginefs.store_registry(), 1).await,
            Some(std::collections::BTreeSet::from([4, 5, 6, 7])),
            "and the backing hands the pass the second episode's four"
        );
        assert_eq!(
            engine.held_in_file(enginefs.store_registry(), 0).await,
            Some(std::collections::BTreeSet::from([0, 1, 2, 3]))
        );
    }

    /// **The panel's numbers for a torrent with no registered store are no
    /// numbers.** A policy stands and a reader is inside the file, but the
    /// torrent is in Error and its store has gone with its storage: there
    /// is no held set to count a window off, and a window of zero would say
    /// the disk holds nothing of the stream, which is a measurement nobody
    /// took. The whole row is absent instead, as it was for a directory
    /// that would not list.
    #[tokio::test]
    async fn the_panels_numbers_are_absent_for_a_torrent_with_no_registered_store() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        engine.begin_retention(0).await;
        engine.note_playhead(0, 25);
        assert!(
            engine.policy_reading(0).is_some(),
            "a policy stands and the reader is inside the file"
        );
        assert!(
            enginefs
                .torrent_stream_numbers(TEST_HASH, 0)
                .await
                .is_none(),
            "no store, so no numbers -- not a window of zero"
        );

        let _store = seeded_store(&enginefs, &engine);
        assert!(
            enginefs
                .torrent_stream_numbers(TEST_HASH, 0)
                .await
                .is_some_and(|numbers| numbers.window.is_some()),
            "with a store registered the same question has a window"
        );
    }

    /// **Nothing of a torrent is dropped or unlinked while it is checking
    /// or in error.**
    ///
    /// The pass decides its reclaim from a held set and a window, and
    /// neither says what the torrent is doing. A torrent under its initial
    /// hash check is reading every piece it means to claim, and a piece
    /// dropped from under it is a have-bit over nothing; one in Error holds
    /// no storage for a drop to edit. So the run state is read where the
    /// drop is decided and not before: `retain` steps aside for a check
    /// before it takes the turn, and the reclaim reads it before every run,
    /// so a torrent that errors between the decision and the unlink loses
    /// nothing. The fake keeps its storage registered through the error,
    /// which the real backend would not -- that is exactly what makes the
    /// reclaim's own reading the one under test here.
    #[tokio::test]
    async fn nothing_is_dropped_or_unlinked_while_the_torrent_is_checking_or_in_error() {
        let (enginefs, counters, init) = test_enginefs_with_init(
            vec![("film.mkv".into(), 100)],
            FakeInit::new(true, Duration::from_secs(60)),
        );
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        let all_here = || (0..4).all(|piece| bucket.join(piece.to_string()).is_file());

        // The check is running again -- a restart out of error re-checks
        // what is on the disk -- and the tick finds the torrent initializing.
        init.ready.store(false, Ordering::SeqCst);
        assert!(
            engine
                .retain(enginefs.store_registry(), &playing(0))
                .await
                .is_none(),
            "a torrent under its check has no pass to make"
        );
        assert!(
            counters.dropped_ranges.lock().unwrap().is_empty() && all_here(),
            "and nothing was asked of the backend or the disk"
        );

        // In error at the instant of the reclaim: the pass ran -- the set is
        // still registered -- and decided on the three pieces outside the
        // one-piece window, and the reclaim read the state before its first
        // run and took nothing.
        init.ready.store(true, Ordering::SeqCst);
        counters.in_error_state.store(true, Ordering::SeqCst);
        let pass = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass over a registered set runs");
        assert_eq!(pass.reclaimed, 0, "nothing reclaimed in error: {pass:?}");
        assert!(
            counters.dropped_ranges.lock().unwrap().is_empty() && all_here(),
            "no drop was asked and no file went"
        );

        // Settled again, the same decision goes through.
        counters.in_error_state.store(false, Ordering::SeqCst);
        let pass = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        assert_eq!(
            pass.reclaimed, 3,
            "the three outside the window go: {pass:?}"
        );
        assert!((1..4).all(|piece| !bucket.join(piece.to_string()).exists()));
        assert!(
            bucket.join("0").is_file(),
            "the piece under the playhead stays"
        );
    }

    /// **And the want-set is left alone in the same states.** A torrent in
    /// error holds no chunk tracker for a re-selection or a drop to edit;
    /// the backend bails, and a bail per run is a warning per run per tick.
    /// The pass reads the state at the want step as the reclaim does, asks
    /// nothing, and asks the moment the torrent is settled again.
    #[tokio::test]
    async fn the_want_set_is_left_alone_while_the_torrent_is_in_error() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        // Only the piece under the playhead: three pieces beyond the window
        // for the want step to stop wanting.
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);

        counters.in_error_state.store(true, Ordering::SeqCst);
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass over a registered set runs");
        assert!(
            counters.reselected.lock().unwrap().is_empty()
                && counters.dropped_ranges.lock().unwrap().is_empty(),
            "nothing was asked of a want-set the torrent does not have"
        );

        counters.in_error_state.store(false, Ordering::SeqCst);
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        assert_eq!(*counters.reselected.lock().unwrap(), vec![0..1]);
        assert_eq!(
            *counters.dropped_ranges.lock().unwrap(),
            vec![(1..4, crate::backend::AfterRelease::LeaveDropped)]
        );

        // And the clear's want-everything is left unasked in error too: a
        // torrent restarted out of error rebuilds its want-set whole.
        counters.in_error_state.store(true, Ordering::SeqCst);
        engine.pinned_files.write().insert(0);
        engine.retain(enginefs.store_registry(), &playing(0)).await;
        assert_eq!(*counters.reselected.lock().unwrap(), vec![0..1]);
    }

    /// **The want-set is trimmed on a paused torrent, and left alone on one
    /// still checking.** A paused torrent keeps its chunk tracker, and the
    /// window it is left wanting is what a restart fetches first; a torrent
    /// under its initial check has nothing settled to edit, and the pass on
    /// it concludes nothing at all.
    #[tokio::test]
    async fn the_want_set_is_trimmed_on_a_paused_torrent_and_left_alone_on_one_still_checking() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        counters.paused.store(true, Ordering::SeqCst);
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass runs on a paused torrent");
        assert_eq!(*counters.reselected.lock().unwrap(), vec![0..1]);
        assert_eq!(
            *counters.dropped_ranges.lock().unwrap(),
            vec![(1..4, crate::backend::AfterRelease::LeaveDropped)]
        );

        let (enginefs, counters, init) = test_enginefs_with_init(
            vec![("film.mkv".into(), 100)],
            FakeInit::new(false, Duration::from_secs(60)),
        );
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        assert!(
            engine
                .retain(enginefs.store_registry(), &playing(0))
                .await
                .is_none(),
            "a torrent under its check has no pass"
        );
        assert!(
            counters.reselected.lock().unwrap().is_empty()
                && counters.dropped_ranges.lock().unwrap().is_empty(),
            "and nothing was asked of its want-set"
        );
        init.mark_ready();
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass, once the check is over");
        assert_eq!(*counters.reselected.lock().unwrap(), vec![0..1]);
    }

    /// **A file left unbounded is wanted whole; a sibling an install retires
    /// is not.** The policy's passes stopped wanting the file beyond the
    /// window, and only an entity that ends with no policy at all wants the
    /// rest again: the budget gone, or a pin. The file a reader has just
    /// left is neither -- it is slack, and its want-set is left as the
    /// passes left it, or the second episode would have the first
    /// downloading whole behind it, a file nobody is reading filling the
    /// disk at the swarm's pace.
    #[tokio::test]
    async fn a_file_left_unbounded_is_wanted_whole_and_a_retired_sibling_is_not() {
        let (enginefs, counters) = test_enginefs_with_files(vec![
            ("Show.S01E01.mkv".into(), 100),
            ("Show.S01E02.mkv".into(), 100),
        ]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        // Half of either file: a one-piece window and one committed.
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        assert_eq!(*counters.reselected.lock().unwrap(), vec![0..1]);
        assert_eq!(
            *counters.dropped_ranges.lock().unwrap(),
            vec![(1..4, crate::backend::AfterRelease::LeaveDropped)]
        );

        // The reader moves on to episode two: episode two is held back, and
        // episode one is touched not at all. Its range is **not** given
        // back -- it is a file nobody is playing, so its pieces are on
        // their way off the disk, and announcing them first would be a Have
        // for bytes that go seconds later (issue (a)).
        engine.begin_retention(1).await;
        assert_eq!(
            *counters.advertised.lock().unwrap(),
            vec![(0..4, false), (4..8, false)],
            "episode two is held back and episode one is left alone"
        );
        assert_eq!(
            *counters.reselected.lock().unwrap(),
            vec![0..1],
            "the file the reader left is not wanted whole"
        );

        // The budget goes: the next install on episode two ends with
        // nothing installed, and the file is wanted whole.
        enginefs.set_cache_budget(None);
        engine.begin_retention(1).await;
        assert_eq!(
            counters.advertised.lock().unwrap().last(),
            Some(&(4..8, true))
        );
        assert_eq!(*counters.reselected.lock().unwrap(), vec![0..1, 4..8]);

        // A pin on a bounded file: its install clears the policy and wants
        // the file whole.
        enginefs.set_cache_budget(Some(50));
        engine.begin_retention(0).await;
        assert_eq!(*counters.reselected.lock().unwrap(), vec![0..1, 4..8]);
        engine.pinned_files.write().insert(0);
        engine.begin_retention(0).await;
        assert_eq!(
            *counters.reselected.lock().unwrap(),
            vec![0..1, 4..8, 0..4],
            "the pinned file is wanted whole"
        );
    }

    /// **A pass measures against where playback is now, not where it was
    /// when the pass began.**
    ///
    /// The pass reads the held set and then the playhead, and between the
    /// two it holds none of the locks `note_playhead` takes -- the reading
    /// used to be a directory walk on the blocking pool with the pass
    /// suspended across it, and it is a memory read now, but the gap is
    /// the same gap and the owner's hook puts playback inside it. A byte
    /// delivered there moves the playhead while the fill writes the
    /// read-ahead the player is about to want. A pass that read the
    /// playhead before the held set draws its window round where
    /// playback *was*: everything written ahead of that is outside the
    /// window, on the disk, and reclaimed, and `AfterRelease::LeaveDropped`
    /// means nothing fetches it back until the player arrives at the hole
    /// and parks. The same reordering was measured on the proxy's identical
    /// pass before it was fixed there: a 16 MB read left an empty directory
    /// under an 8 MB budget after two passes.
    ///
    /// One piece of window over a four-piece file, so the two readings give
    /// disjoint answers and the assertions are opposites rather than
    /// counts: measured at piece 0 the pass keeps 0 and takes 3, measured
    /// at piece 3 it keeps 3 and takes 0.
    #[tokio::test]
    async fn a_pass_reclaims_round_where_playback_got_to_while_it_listed() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));

        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }

        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);

        // Playback, in the gap this pass has: from inside the pass, as it
        // starts its listing. See `Engine::interleave` for why not a queued
        // task.
        *engine.interleave.lock() = Some(Box::new({
            let engine = engine.clone();
            move || engine.note_playhead(0, 75)
        }));

        let pass = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass ran");
        assert!(pass.reclaimed > 0, "the pass gave pieces back: {pass:?}");
        assert!(
            bucket.join("3").is_file(),
            "the piece under the playhead the pass ended at is not the pass's to take"
        );
        assert!(
            !bucket.join("0").exists(),
            "and what playback has left behind is"
        );
    }

    /// **A torrent with no registered store holds unknown, not nothing.**
    ///
    /// The pass reads the held set of the store registered for the hash,
    /// and a torrent in Error has none: librqbit drops its storage, and the
    /// registration goes with it. The answer is then "no store", and an
    /// empty set would have been a very definite measurement instead: the
    /// policy's `advance` keeps only the committed pieces the set names, so
    /// one pass over an empty answer would withdraw every committed piece
    /// from what we announce -- after peers had been told, and there is no
    /// un-Have -- and the next, with the store back, would find them outside
    /// the window and no longer committed and reclaim them. That is what a
    /// directory that would not list once did through the listing this
    /// replaced, and the promise the whole design rests on -- what we
    /// announce is what nothing will ever reclaim -- was broken for the
    /// file. A pass with no store concludes nothing.
    ///
    /// The fixture is the two-pass one above: a second pass commits piece 0.
    /// Then the store goes -- the torrent errored and librqbit dropped its
    /// storage -- and the third pass finds no registration.
    #[tokio::test]
    async fn a_pass_over_a_torrent_with_no_registered_store_withdraws_nothing() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));

        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        std::fs::write(bucket.join("1"), [7u8; 25]).unwrap();
        store.init_for_tests().unwrap();
        engine.note_playhead(0, 25);
        let pass = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        assert_eq!(
            pass.committed, 1,
            "piece 0 is committed and announced: {pass:?}"
        );
        counters.advertised.lock().unwrap().clear();

        // The torrent's storage is gone: the last handle over the store is
        // dropped, and the registration with it.
        drop(store);
        assert!(
            enginefs.store_registry().held(TEST_HASH).is_none(),
            "no store is registered for the hash"
        );

        assert!(
            engine
                .retain(enginefs.store_registry(), &playing(0))
                .await
                .is_none(),
            "a pass with no store concludes nothing"
        );
        assert!(
            counters.advertised.lock().unwrap().is_empty(),
            "and in particular it withdraws nothing from what we announce: {:?}",
            counters.advertised.lock().unwrap()
        );
        assert!(
            bucket.join("0").is_file() && bucket.join("1").is_file(),
            "and reclaims nothing"
        );
    }

    /// **And a byte in another file while the pass listed leaves this
    /// file's pass concluding normally, on this file's own head.**
    ///
    /// Each file has a head of its own and hears only its own bytes. A byte
    /// of the next episode -- the viewer opened it while this file's pass
    /// was at its held reading -- is that file's, and this file's head is
    /// still piece 0 at the re-read: the window is drawn round it, and what
    /// is outside the window goes. When a torrent had one head told to
    /// every file, that byte read here as "the head has left the file" and
    /// stopped the pass cold, for as long as the viewer stayed in the other
    /// file: the window never moved, nothing outside it was reclaimed, and
    /// nothing was committed for sharing.
    #[tokio::test]
    async fn a_byte_in_another_file_while_the_pass_listed_leaves_it_concluding_on_this_files_head()
    {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        // Two pieces of budget over each four-piece file.
        enginefs.set_cache_budget(Some(50));

        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);

        // The next episode is read while the pass lists.
        *engine.interleave.lock() = Some(Box::new({
            let engine = engine.clone();
            move || engine.note_playhead(1, 0)
        }));

        let pass = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a byte of another file stopped this file's pass");
        assert_eq!(
            pass.reclaimed, 3,
            "everything outside the one-piece window round piece 0: {pass:?}"
        );
        assert!(
            bucket.join("0").is_file(),
            "the piece under this file's head"
        );
        for piece in [1u32, 2, 3] {
            assert!(!bucket.join(piece.to_string()).exists());
        }
        let holding = engine.retention.holding(&0).expect("file 0's entity");
        assert_eq!(
            holding.last_position,
            Some((0, 0)),
            "a byte of file 1 moved file 0's head"
        );
        assert_eq!(holding.windows, vec![0..1]);
        assert!(
            engine.retention.holding(&1).is_none(),
            "a byte of a file nothing bounds was remembered"
        );
    }

    /// **A pin taken while the pass is reclaiming keeps its bytes.**
    ///
    /// `Engine::retain` asks whether the torrent is pinned in its
    /// prologue, and by the time the first piece is unlinked that answer is
    /// a disk walk and two awaited backend calls old. `pin_download` writes
    /// `pinned_files` under none of the locks the pass holds, so it lands
    /// in exactly that gap -- and what the pass takes then is worse than
    /// deleted: `AfterRelease::LeaveDropped` leaves the pieces neither held
    /// nor wanted, and the pin's own reconcile short-circuits an unchanged
    /// selection, so nothing re-queues them and the download the user has
    /// just asked for stays short of them until a restart hash-checks the
    /// file off the disk. The same failure with the pin landing a second
    /// earlier was measured at half a 32 MiB file; this is it moved inside
    /// the pass.
    ///
    /// The park is `FakeCounters::advertise_gate`, which holds the pass
    /// inside the call it makes to announce what the window released --
    /// after the decision and before the reclaim, which is the instant this
    /// door is about. What the pass then does with the policy it is holding
    /// is not this test's subject: it puts it back, as it always has.
    #[tokio::test]
    async fn a_pin_taken_while_the_pass_reclaims_keeps_its_bytes() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        // Two pieces of budget over four 25-byte pieces, half of which is
        // the committed set: a one-piece window.
        enginefs.set_cache_budget(Some(50));

        // A first pass with nothing on the disk but the piece under the
        // playhead: it takes nothing, and leaves behind the record that its
        // window covered piece 0. That record is what lets the second pass
        // commit piece 0, and a commit is what parks the pass in the
        // backend call below.
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");

        // Now the rest of the file is on the disk and playback has walked
        // on to piece 1: piece 0 commits, piece 1 is the window, and pieces
        // 2 and 3 are what this pass sets out to reclaim.
        for piece in [1u32, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        store.init_for_tests().unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *counters.advertise_gate.lock().unwrap() = Some((entered_tx, release_rx));
        engine.note_playhead(0, 25);
        let running = tokio::spawn({
            let engine = engine.clone();
            let registry = enginefs.store_registry().clone();
            async move { engine.retain(&registry, &playing(0)).await }
        });
        tokio::time::timeout(Duration::from_secs(10), entered_rx)
            .await
            .expect("the pass reached the call it makes to announce a committed piece")
            .expect("the fake said so");
        // The user taps download for offline while the pass is parked.
        engine.pinned_files.write().insert(0);

        release_tx.send(()).expect("the pass is waiting on this");
        let pass = running
            .await
            .expect("the pass task")
            .expect("a pass ran to the end");
        assert_eq!(
            pass.reclaimed, 0,
            "the pass stopped at the door instead of reclaiming: {pass:?}"
        );
        assert!(
            bucket.join("2").is_file() && bucket.join("3").is_file(),
            "the pieces of the file the user has just asked to keep are still here"
        );
        assert_eq!(
            *counters.dropped_ranges.lock().unwrap(),
            vec![(1..4, crate::backend::AfterRelease::LeaveDropped)],
            "and the pass the pin landed in asked the backend to forget \
             nothing: the one ask is the first pass's, which stopped wanting \
             the three pieces it did not have. A pin on a single-file torrent \
             changes no selection of librqbit's own, so a piece dropped under \
             it would have stayed unwanted until the pass below wants the \
             file whole"
        );
        // The pass that runs under the pin clears the policy, and with it
        // wants the whole file again: what the first pass stopped wanting is
        // fetched for the download the user asked for.
        engine.retain(enginefs.store_registry(), &playing(0)).await;
        assert_eq!(
            counters.reselected.lock().unwrap().last(),
            Some(&(0..4)),
            "the pin's pass wants every piece of the file again, after the \
             windows the two passes before it wanted"
        );
    }

    /// **And the reader that moved while the pass ran narrows the reclaim
    /// rather than stopping it.**
    ///
    /// The playhead the window was drawn round is as old as the pin above:
    /// `note_playhead` runs on every delivered byte and takes none of the
    /// pass's locks, so playback walks on while the pass commits and
    /// withdraws. librqbit refuses to drop what its own live stream is
    /// about to read, but that refusal is the forward lookahead alone (4
    /// MiB), blind to the tenth of the window kept behind the playhead for
    /// a scan back, empty when no stream is open, and promised by no
    /// backend -- the fake here refuses nothing at all. So the pass has to
    /// subtract the window at the reader's current position itself.
    ///
    /// Same fixture and same park as the test above, so the reclaim it sets
    /// out to make is pieces 2 and 3. Moving the playhead onto piece 3
    /// while it is parked puts the one-piece window over piece 3 alone,
    /// which is what makes this an assertion about narrowing and not about
    /// refusing: piece 3 stays, piece 2 still goes.
    #[tokio::test]
    async fn a_run_the_reader_walked_into_while_the_pass_ran_keeps_that_much() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        // Two pieces of budget over four 25-byte pieces, half of which is
        // the committed set: a one-piece window.
        enginefs.set_cache_budget(Some(50));

        // A first pass with nothing on the disk but the piece under the
        // playhead: it takes nothing, and leaves behind the record that its
        // window covered piece 0. That record is what lets the second pass
        // commit piece 0, and a commit is what parks the pass in the
        // backend call below.
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");

        // Now the rest of the file is on the disk and playback has walked
        // on to piece 1: piece 0 commits, piece 1 is the window, and pieces
        // 2 and 3 are what this pass sets out to reclaim.
        for piece in [1u32, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        store.init_for_tests().unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *counters.advertise_gate.lock().unwrap() = Some((entered_tx, release_rx));
        engine.note_playhead(0, 25);
        let running = tokio::spawn({
            let engine = engine.clone();
            let registry = enginefs.store_registry().clone();
            async move { engine.retain(&registry, &playing(0)).await }
        });
        tokio::time::timeout(Duration::from_secs(10), entered_rx)
            .await
            .expect("the pass reached the call it makes to announce a committed piece")
            .expect("the fake said so");
        // Playback, while the pass is parked: piece 3 of 4.
        engine.note_playhead(0, 75);

        release_tx.send(()).expect("the pass is waiting on this");
        let pass = running
            .await
            .expect("the pass task")
            .expect("a pass ran to the end");
        assert_eq!(
            pass.reclaimed, 1,
            "one of the two pieces it set out to take, not both: {pass:?}"
        );
        assert!(
            bucket.join("3").is_file(),
            "the piece the reader walked onto while the pass ran is not the \
             pass's to take"
        );
        assert!(
            !bucket.join("2").exists(),
            "and the run is narrowed rather than refused whole: what playback \
             has left behind still goes"
        );
    }

    /// **And the door is asked before every part of a run, not once for
    /// the run.**
    ///
    /// The window at the door can fall inside a run and cut it in two.
    /// The two parts are released one after the other, and `release` is a
    /// `drop_pieces` plus an unlink batch -- long enough for the pin above
    /// to land between them. Asked once for the run, the pass would give the
    /// second part back against an answer a whole release old: the pieces
    /// of a download the user has just asked to keep, dropped and unlinked
    /// with nothing to fetch them again. This is the door's own defect
    /// shape, a reading trusted one call later, re-created one level down
    /// inside the door.
    ///
    /// Eight pieces and a one-piece window. Parked in the commit, the reader
    /// seeks to piece 4, so the door splits the reclaim run 2..8 round the
    /// window into 2..4 and 5..8. The pin lands from inside the backend
    /// call that drops 2..4. Pieces 5 to 7 must not follow.
    #[tokio::test]
    async fn a_pin_taken_between_the_parts_of_a_split_run_keeps_the_rest() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 200)]);
        counters.pieces_per_file.store(8, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        // Two pieces of budget over eight 25-byte pieces: one committed,
        // one window, as in the two tests above.
        enginefs.set_cache_budget(Some(50));

        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");

        // The whole file is on the disk and playback has walked on to piece
        // 1: piece 0 commits, piece 1 is the window, and the reclaim the
        // pass decides on is the one run 2..8.
        for piece in 1u32..8 {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        store.init_for_tests().unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *counters.advertise_gate.lock().unwrap() = Some((entered_tx, release_rx));
        // The user pins from inside the first drop -- between the parts.
        *counters.on_first_drop.lock().unwrap() = Some(Box::new({
            let engine = engine.clone();
            move || {
                engine.pinned_files.write().insert(0);
            }
        }));
        engine.note_playhead(0, 25);
        let running = tokio::spawn({
            let engine = engine.clone();
            let registry = enginefs.store_registry().clone();
            async move { engine.retain(&registry, &playing(0)).await }
        });
        tokio::time::timeout(Duration::from_secs(10), entered_rx)
            .await
            .expect("the pass reached the call it makes to announce a committed piece")
            .expect("the fake said so");
        // A seek to piece 4 while the pass is parked: the window at the door
        // is now inside the run 2..8.
        engine.note_playhead(0, 4 * 25);

        release_tx.send(()).expect("the pass is waiting on this");
        let pass = running
            .await
            .expect("the pass task")
            .expect("a pass ran to the end");
        let dropped: Vec<std::ops::Range<u32>> = counters
            .dropped_ranges
            .lock()
            .unwrap()
            .iter()
            .map(|(range, _)| range.clone())
            .collect();
        assert_eq!(
            dropped,
            vec![1..8, 2..4],
            "the first pass stopped wanting the seven pieces it did not have; \
             then the part before the window went, the pin landed inside that \
             drop, and nothing was asked for after it: {pass:?}"
        );
        assert!(
            !bucket.join("2").exists() && !bucket.join("3").exists(),
            "the part released before the pin is gone"
        );
        for piece in [5u32, 6, 7] {
            assert!(
                bucket.join(piece.to_string()).is_file(),
                "piece {piece} of the file the user asked to keep is still here"
            );
        }
    }

    /// And the other half of a pass that is not the reactor's to do: the
    /// unlinks.
    ///
    /// A reclaim is one `unlink` per piece, and the claim the backend hands
    /// back has to outlive them -- it is what stops a stream downloading a
    /// piece back into the range being deleted -- so whichever thread
    /// releases the claim is the thread the bytes went on. The pass holds
    /// the file's turn throughout, so doing that work on the reactor stops
    /// request handling for as long as the volume takes.
    ///
    /// A `#[tokio::test]` drives its runtime on the test's own thread, so
    /// "the reactor" here is a thread identity and not a timing.
    #[tokio::test]
    async fn the_pass_unlinks_a_reclaimed_piece_off_the_reactor() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));

        // All four pieces on the disk with the playhead at the start, so
        // the two at the far end are outside the window and are the pass's
        // to give back.
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [0u32, 1, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }

        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        let pass = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass ran");
        assert!(pass.reclaimed > 0, "the pass gave pieces back: {pass:?}");

        let released_on = *counters.claim_released_on.lock().unwrap();
        let released_on = released_on.expect("the pass took the claim it was handed");
        assert_ne!(
            released_on,
            std::thread::current().id(),
            "the unlinks the claim covers did not run on the reactor"
        );
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
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        let numbers = enginefs
            .torrent_stream_numbers(TEST_HASH, 0)
            .await
            .expect("the engine exists");
        assert_eq!((numbers.window, numbers.committed_bytes), (None, None));
    }

    /// **A stream stops being bounded, and the panel has to hear that.**
    ///
    /// The panel's numbers are a reading *of* the policy, and a policy that
    /// is dropped takes its window and its committed set with it. Two ordinary things drop one: a pin taken
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
        let seeded = |enginefs: &BackendEngineFS<FakeBackend>, engine: &Engine<FakeHandle>| {
            let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
            std::fs::create_dir_all(&bucket).unwrap();
            for piece in [0u32, 1, 2, 3] {
                std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
            }
            seeded_store(enginefs, engine)
        };

        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let _store = seeded(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 25);
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
        assert!(
            engine
                .retain(enginefs.store_registry(), &playing(0))
                .await
                .is_none(),
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
        let _store = seeded(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 25);
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

        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
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
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
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
        engine.begin_retention(1).await;
        engine.note_playhead(1, 0);
        let _store = seeded_store(&enginefs, &engine);
        let pass = engine
            .retain(enginefs.store_registry(), &playing(1))
            .await
            .expect("a pass");

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
            vec![
                (6..8, crate::backend::AfterRelease::LeaveDropped),
                (5..6, crate::backend::AfterRelease::LeaveDropped),
            ],
            "the backend is never even asked to forget the shared piece: it \
             would agree, and the bytes would go. The want-set's ask is for \
             the two pieces of this file's own it does not have, the reclaim's \
             for the one it gives back"
        );
    }

    /// **And the want-set is trimmed by the same boundary rule.** A piece
    /// the next episode also lies in, not on the disk and outside this
    /// file's window, stays wanted: dropped, the neighbour would be a piece
    /// short with nothing to fetch it, as it would be a piece short of the
    /// reclaim above. Pieces of this file's own beyond the window are
    /// dropped.
    #[tokio::test]
    async fn the_piece_the_next_file_shares_stays_wanted_when_the_window_leaves_it() {
        let (enginefs, counters) = test_enginefs_with_files(vec![
            ("Show.S01E01.mkv".into(), 100),
            ("Show.S01E02.mkv".into(), 110),
            ("Show.S01E03.mkv".into(), 100),
        ]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));

        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        // Episode two is pieces 4..9; only its first two have arrived.
        for piece in [4u32, 5] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        engine.begin_retention(1).await;
        engine.note_playhead(1, 0);
        let _store = seeded_store(&enginefs, &engine);
        engine
            .retain(enginefs.store_registry(), &playing(1))
            .await
            .expect("a pass");

        let asked: Vec<std::ops::Range<u32>> = counters
            .dropped_ranges
            .lock()
            .unwrap()
            .iter()
            .map(|(range, _)| range.clone())
            .collect();
        assert_eq!(
            asked,
            vec![6..8, 5..6],
            "pieces six and seven are this file's own and not wanted; piece \
             eight is the next episode's too and is never asked about; piece \
             five is the reclaim's"
        );
    }

    /// The same boundary piece, once nothing else wants it.
    ///
    /// What holds the piece back is the *neighbour*, not the boundary: a
    /// file the torrent has stopped wanting will not fetch the piece again,
    /// so there is no loop to avoid and no data anybody asked for to lose.
    /// A rule that refused every boundary piece instead would leave two
    /// pieces of every file on the disk for ever.
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

        engine.begin_retention(1).await;
        engine.note_playhead(1, 0);
        let _store = seeded_store(&enginefs, &engine);
        let pass = engine
            .retain(enginefs.store_registry(), &playing(1))
            .await
            .expect("a pass");

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

        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        assert!(
            !engine.standing().await.policies.is_empty(),
            "before the pin, a policy bounds the file"
        );

        // The user pins the file they are watching.
        engine.pinned_files.write().insert(0);

        assert!(
            engine
                .retain(enginefs.store_registry(), &playing(0))
                .await
                .is_none(),
            "a pinned torrent has no retention pass to make"
        );
    }

    /// **A pass that dies leaves the policy where it was, and the next one
    /// runs.**
    ///
    /// The tick that drives a pass is a task, and a task is dropped at
    /// whatever await it is parked on when the runtime shuts down. The pass
    /// used to take the policy out of its slot for the length of itself and
    /// put it back on the way out, and a future dropped at an await has no
    /// way out: the policy went with it, the gate read the torrent as
    /// announced while its window was still held back, and no later pass
    /// had a policy to run -- the file unbounded until the next open. The
    /// policy is resident now and what the pass holds is the file's turn, a
    /// guard that a dropped future releases like any other local.
    ///
    /// Parked in the commit's backend call, as the door tests are: after the
    /// interleave point and the listing, with the policy advanced in place.
    #[tokio::test]
    async fn a_pass_aborted_in_flight_leaves_the_policy_standing_and_the_next_pass_runs() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));

        // The first pass records that the window covered piece 0, so the
        // second commits it and parks in the call that announces it.
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        for piece in [1u32, 2, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        store.init_for_tests().unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (_release_tx, release_rx) = tokio::sync::oneshot::channel();
        *counters.advertise_gate.lock().unwrap() = Some((entered_tx, release_rx));
        engine.note_playhead(0, 25);
        let running = tokio::spawn({
            let engine = engine.clone();
            let registry = enginefs.store_registry().clone();
            async move { engine.retain(&registry, &playing(0)).await }
        });
        tokio::time::timeout(Duration::from_secs(10), entered_rx)
            .await
            .expect("the pass reached the call it makes to announce a committed piece")
            .expect("the fake said so");

        // The runtime takes the task down mid-pass.
        running.abort();
        assert!(
            running
                .await
                .expect_err("the pass was aborted")
                .is_cancelled(),
            "aborted at the await it was parked on"
        );

        assert!(
            !engine.standing().await.policies.is_empty(),
            "the policy is still bounding the file: it never left its cell"
        );
        let pass = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("the next tick's pass runs: the dead one dropped the file's turn");
        assert_eq!(
            pass.reclaimed, 2,
            "and it gives back what the dead pass had decided to and never did: {pass:?}"
        );
        assert!(!bucket.join("2").exists() && !bucket.join("3").exists());
        assert!(
            bucket.join("1").is_file(),
            "the piece under the playhead stays"
        );
    }

    /// **A pin whose range the backend will not take back keeps its policy
    /// until it can.**
    ///
    /// The clear used to empty the slot first and re-advertise second, so a
    /// backend that refused left the range held back beside an empty slot:
    /// the cleaner's gate read the torrent as announced, nothing shared the
    /// pieces and nothing would ever reclaim them -- held back and protected
    /// at once, the one combination that is never right -- and it was
    /// logged at debug and never retried. The range is advertised back
    /// first now and the policy forgotten only when that succeeded, so a
    /// refusal leaves a policy that still tells the truth about what is
    /// held back, and the next pass under the pin retries.
    #[tokio::test]
    async fn a_pin_whose_range_the_backend_will_not_take_back_keeps_the_policy_until_it_can() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        assert_eq!(
            *counters.advertised.lock().unwrap(),
            vec![(0..4, false)],
            "the file's range is held back"
        );

        // The user pins the file, and the backend will not have the range
        // back.
        engine.pinned_files.write().insert(0);
        counters.refuses_advertise.store(true, Ordering::SeqCst);
        assert!(
            engine
                .retain(enginefs.store_registry(), &playing(0))
                .await
                .is_none(),
            "a pinned torrent has no pass to make"
        );
        assert!(
            engine
                .retention
                .holding(&0)
                .expect("the file has an entity")
                .installed
                .is_some(),
            "and the policy stands, because its range is still held back: a cell \
             that said otherwise would be the held-back-and-announced combination"
        );
        assert_eq!(
            *counters.advertised.lock().unwrap(),
            vec![(0..4, false)],
            "nothing was put back, because the backend refused"
        );

        // The backend can again, and the next pass under the pin retries.
        counters.refuses_advertise.store(false, Ordering::SeqCst);
        assert!(
            engine
                .retain(enginefs.store_registry(), &playing(0))
                .await
                .is_none()
        );
        assert!(
            engine
                .retention
                .holding(&0)
                .expect("the file has an entity")
                .installed
                .is_none(),
            "the range is back in what we announce, and only now is the policy gone"
        );
        assert_eq!(
            *counters.advertised.lock().unwrap(),
            vec![(0..4, false), (0..4, true)]
        );
    }

    /// An eight-piece film of twenty-five byte pieces, all on the disk,
    /// under a budget of two pieces (a one-piece window, one committed),
    /// with a stream open at its start and a second at byte 150 -- a seek
    /// -- each having delivered one byte: heads at piece 0 and piece 6, the
    /// second's the file's last byte. The fixture of the two tests below.
    async fn two_streams_on_one_file() -> (
        BackendEngineFS<FakeBackend>,
        Arc<Engine<FakeHandle>>,
        std::path::PathBuf,
        crate::piece_store::PieceStore,
        crate::files::FileHandle<FakeHandle>,
        crate::files::FileHandle<FakeHandle>,
    ) {
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        use tokio::io::AsyncReadExt;
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 200)]);
        counters.pieces_per_file.store(8, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in 0..8u32 {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let store = seeded_store(&enginefs, &engine);
        let mut at_start = engine
            .try_get_file_with_intent(0, 0, 255, PlaybackIntent::DirectSeek, BufferProfile::Normal)
            .await
            .expect("a stream at the start");
        let mut seek = engine
            .try_get_file_with_intent(
                0,
                150,
                255,
                PlaybackIntent::DirectSeek,
                BufferProfile::Normal,
            )
            .await
            .expect("a stream at byte 150");
        let mut byte = [0u8; 1];
        at_start.read_exact(&mut byte).await.expect("a byte at 0");
        seek.read_exact(&mut byte).await.expect("a byte at 150");
        (enginefs, engine, bucket, store, at_start, seek)
    }

    /// **Two streams on one file at different offsets are two heads at the
    /// door, and a reclaim between them takes neither's piece.**
    ///
    /// A seek is a second response on the file still playing, and each
    /// [`crate::files::FileHandle`] is a reader of the file's entity with a
    /// playhead of its own. The file's head is the last byte either
    /// delivered -- the seek's, at piece 6 -- and the pass concludes a
    /// window round it and one round the other stream's piece 0; the door
    /// answers both at every unlink, so the run between them is cut round
    /// both. With one head per torrent the window at the door was the
    /// seek's alone, and the piece the first stream was inside went with
    /// everything else outside it.
    #[tokio::test]
    async fn two_streams_on_one_file_each_keep_the_piece_under_their_own_head() {
        let (enginefs, engine, bucket, _store, _at_start, _seek) = two_streams_on_one_file().await;
        assert_eq!(engine.retention.readers_of(&0), 2);
        let pass = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        assert_eq!(
            engine.retention.holding(&0).unwrap().windows,
            vec![6..7, 0..1],
            "a window per head, the file's own first"
        );
        assert_eq!(pass.reclaimed, 6, "{pass:?}");
        assert!(
            bucket.join("0").is_file(),
            "the piece under the first stream's head went with the run between the heads"
        );
        assert!(
            bucket.join("6").is_file(),
            "the piece under the seek's head"
        );
        for piece in [1u32, 2, 3, 4, 5, 7] {
            assert!(
                !bucket.join(piece.to_string()).exists(),
                "piece {piece} is outside both windows and stayed"
            );
        }
    }

    /// **Dropping a stream's handle ends its reader, and the next pass has
    /// one head.** The response ended; the piece it was inside is nobody's
    /// to keep once the other stream has delivered again, and the pass
    /// concludes one window and reclaims it.
    #[tokio::test]
    async fn dropping_a_stream_ends_its_reader_and_the_next_pass_has_one_head() {
        use tokio::io::AsyncReadExt;
        let (enginefs, engine, bucket, _store, mut at_start, seek) =
            two_streams_on_one_file().await;
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        assert!(bucket.join("6").is_file());
        drop(seek);
        assert_eq!(
            engine.retention.readers_of(&0),
            1,
            "the dropped stream is still a reader of the file"
        );
        // The stream still open delivers again: the file's head is its.
        let mut byte = [0u8; 1];
        at_start.read_exact(&mut byte).await.expect("a byte at 1");
        let pass = engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        assert_eq!(
            engine.retention.holding(&0).unwrap().windows,
            vec![0..1],
            "one head, one window"
        );
        assert_eq!(pass.reclaimed, 1, "{pass:?}");
        assert!(
            !bucket.join("6").exists(),
            "the piece the ended stream was inside stayed"
        );
        assert!(bucket.join("0").is_file());
        drop(at_start);
        assert_eq!(engine.retention.readers_of(&0), 0);
    }

    /// **The install comes before the stream, and the stream's reader is on
    /// the entity the install made**: the file has a holding the moment
    /// `get_file` returns, before any byte has gone out, and the first byte
    /// lands on it rather than on nothing. This is the ordering that keeps
    /// a torrent file's head from ever being a byte noted into no entity,
    /// which is not remembered -- production order, which the fixtures
    /// above follow.
    #[tokio::test]
    async fn get_file_installs_before_the_first_byte_is_noted() {
        use crate::backend::priorities::{BufferProfile, PlaybackIntent};
        use tokio::io::AsyncReadExt;
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 200)]);
        counters.pieces_per_file.store(8, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        assert!(engine.retention.holding(&0).is_none());
        let mut stream = engine
            .try_get_file_with_intent(
                0,
                25,
                255,
                PlaybackIntent::DirectSeek,
                BufferProfile::Normal,
            )
            .await
            .expect("a stream");
        let holding = engine
            .retention
            .holding(&0)
            .expect("the file has an entity the moment the stream is handed out");
        assert!(holding.installed.is_some(), "and a policy standing on it");
        assert_eq!(holding.last_position, None, "no byte has gone out");
        assert!(!holding.live_playhead);
        assert_eq!(
            engine.retention.readers_of(&0),
            0,
            "a reader that has delivered nothing"
        );
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await.expect("a byte");
        let holding = engine.retention.holding(&0).unwrap();
        assert_eq!(
            holding.last_position,
            Some((0, 26)),
            "the first byte landed on the entity, from the stream's own offset"
        );
        assert!(holding.live_playhead);
        assert_eq!(engine.retention.readers_of(&0), 1);
    }

    /// **A second reader on the same file under the same budget re-holds
    /// nothing back.**
    ///
    /// Every seek is a new range request, so a new reader, so another
    /// `begin_retention` on the file already playing. A policy that already
    /// describes this file under this budget is kept untouched: clearing it
    /// and installing it again would put the window back into what we
    /// announce for the length of two backend calls, and a window piece
    /// that has completed has its Have go out in that gap -- to be
    /// reclaimed a pass later, with no un-Have to take it back.
    #[tokio::test]
    async fn a_second_reader_on_the_same_file_under_the_same_budget_re_holds_nothing_back() {
        let (enginefs, counters) = test_enginefs_with_files(vec![("film.mkv".into(), 100)]);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        assert_eq!(*counters.advertised.lock().unwrap(), vec![(0..4, false)]);

        // The viewer seeks: a second reader opens on the same file.
        engine.begin_retention(0).await;
        assert_eq!(
            *counters.advertised.lock().unwrap(),
            vec![(0..4, false)],
            "the policy already installed describes this file under this budget, \
             so nothing was given back and nothing re-held-back"
        );
        assert!(
            !engine.standing().await.policies.is_empty(),
            "and it still bounds the file"
        );
    }

    /// The one state in which two policies stand on a torrent, staged: file
    /// 0 played and two passes committed its piece 0; the viewer opened
    /// file 1, and the backend would not give file 0's range back at its
    /// retiring but held file 1's back a moment later. The fixture of
    /// `two_standing_policies_are_both_reported_and_each_is_deleted_under_its_own_turn`,
    /// for the tests that go on from there. Pieces are twenty-five bytes,
    /// file 0 is 0..4 and file 1 is 4..8; pieces 0 and 1 are on the disk.
    async fn two_policies_standing() -> (
        BackendEngineFS<FakeBackend>,
        Arc<FakeCounters>,
        Arc<Engine<FakeHandle>>,
        std::path::PathBuf,
        crate::piece_store::PieceStore,
    ) {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));

        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        std::fs::write(bucket.join("1"), [7u8; 25]).unwrap();
        store.init_for_tests().unwrap();
        engine.note_playhead(0, 25);
        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");

        // The viewer opens file 1. Nothing is retired: each file is its own
        // entity with its own head and its own window, so both policies
        // stand from here on.
        engine.begin_retention(1).await;
        assert_eq!(
            *counters.advertised.lock().unwrap(),
            vec![(0..4, false), (0..1, true), (4..8, false)],
            "file 0 held back and its piece 0 committed; file 1 held back;              nothing of file 0 put back"
        );
        assert_eq!(
            engine
                .standing()
                .await
                .policies
                .iter()
                .map(|policy| policy.file_idx)
                .collect::<Vec<_>>(),
            vec![0, 1],
            "two policies stand"
        );
        (enginefs, counters, engine, bucket, store)
    }

    /// **A switch to the next episode makes the first file slack: its
    /// bytes go and its range is never announced again.**
    ///
    /// This is issue (a), and it is what the liveness value is for. What
    /// used to happen when a viewer opened the next episode was that the
    /// install retired the first file -- which put its whole range *back*
    /// into what we announce -- and then nothing deleted the bytes until a
    /// cleaner walk got round to them. A Have per switch for pieces that
    /// were about to go, and a disk that kept every film anybody had opened
    /// this session.
    ///
    /// Now the file the viewer left is [`Mode::Slack`] at the very next
    /// tick: its extent is held back before a single unlink, every piece it
    /// holds is taken, its policy and windows go, its entity is forgotten,
    /// and nothing of it is ever announced again. The file being played
    /// keeps its window through the same tick and commits what the window
    /// releases, as it always did.
    ///
    /// [`Mode::Slack`]: crate::retention::owner::Mode::Slack
    #[tokio::test]
    async fn a_switch_to_the_next_file_makes_the_first_slack_and_takes_its_bytes() {
        let (enginefs, counters, engine, bucket, store) = two_policies_standing().await;
        // The server sees a stream open on file 1: the switch.
        assert_eq!(
            engine.standing().await.policies[0].mode,
            crate::retention::owner::Mode::Live,
            "while file 0 is being played its pass is the live one, which keeps \
             what it committed"
        );
        enginefs.on_stream_start(TEST_HASH, 1).await;
        assert_eq!(enginefs.live().reading().file_of(TEST_HASH), Some(1));
        assert!(
            matches!(
                engine.standing().await.policies[0].mode,
                crate::retention::owner::Mode::Slack { .. }
            ),
            "and the moment the viewer leaves it, its next pass is the slack one: \
             a policy on its way out keeps nothing it committed"
        );
        counters.advertised.lock().unwrap().clear();

        // The viewer reads file 1: piece 4.
        std::fs::write(bucket.join("4"), [7u8; 25]).unwrap();
        store.init_for_tests().unwrap();
        engine.note_playhead(1, 0);
        let live = enginefs.live().reading();
        let pass = engine
            .retain(enginefs.store_registry(), &live)
            .await
            .expect("a pass");

        assert_eq!(
            pass.reclaimed, 2,
            "every held piece of the file left behind: {pass:?}"
        );
        assert!(
            !bucket.join("0").exists() && !bucket.join("1").exists(),
            "including the one it had committed for sharing"
        );
        assert!(
            bucket.join("4").is_file(),
            "and the file being played keeps its window"
        );
        assert!(
            engine.retention.holding(&0).is_none(),
            "the entity that holds nothing and nobody reads is forgotten"
        );
        assert_eq!(
            *counters.advertised.lock().unwrap(),
            vec![(0..4, false)],
            "held back before a byte of it was unlinked, and never put back"
        );

        // And the file being played goes on committing what its window
        // releases.
        std::fs::write(bucket.join("5"), [7u8; 25]).unwrap();
        store.init_for_tests().unwrap();
        engine.note_playhead(1, 25);
        let pass = engine
            .retain(enginefs.store_registry(), &live)
            .await
            .expect("a pass");
        assert_eq!(pass.committed, 1, "{pass:?}");
        assert_eq!(
            counters.advertised.lock().unwrap().last(),
            Some(&(4..5, true)),
            "file 1's committed piece is announced"
        );
    }

    /// **One reading of what is being played, for the whole tick.**
    ///
    /// The ladder and the retention pass both consult it, and they are
    /// asked one after the other over the same engine. A value read twice
    /// could differ between the two readings, and the two decisions it
    /// feeds are opposites: the ladder would keep the torrent running for a
    /// viewer while the pass deleted the window they are inside, or stop it
    /// under a window the pass had just measured and kept. So the tick
    /// takes one copy before its first engine and hands it to both.
    ///
    /// The hook is inside the pass, which is after the ladder: a write
    /// there is a write in the exact gap the two readings would straddle,
    /// and neither may see it.
    #[tokio::test]
    async fn a_live_write_racing_the_tick_is_seen_by_both_halves_or_by_neither() {
        let (mut enginefs, counters) = test_enginefs_with_file_count(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        if let Some(sweep) = enginefs.take_sweep_task() {
            sweep.abort();
        }
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        // Stopped, so the ladder's own `Run` has a call to make -- and that
        // call is what the tick can be parked in, which is the gap between
        // its two halves.
        stop_torrent(&enginefs, TEST_HASH).await;
        let enginefs = Arc::new(enginefs);

        counters.hold_start.store(true, Ordering::SeqCst);
        let tick = tokio::spawn({
            let enginefs = enginefs.clone();
            async move { enginefs.reconcile_tick().await }
        });
        assert!(
            wait_until(TEST_WAIT_BOUND, || counters
                .start_torrent
                .load(Ordering::SeqCst)
                == 1)
            .await,
            "the ladder decided Run and its start is inside the backend"
        );

        // The viewer opens something else, in the gap: after the ladder has
        // read, before the pass has.
        enginefs.live().open(
            crate::retention::live::LiveEntity::Proxy {
                dir: "/elsewhere".into(),
            },
            false,
        );
        counters.hold_start.store(false, Ordering::SeqCst);
        counters.start_gate.notify_one();

        assert_eq!(
            tick.await.expect("the tick"),
            vec![(TEST_HASH.to_string(), Decision::Run)],
            "the ladder read the torrent as the one being played"
        );
        assert!(
            bucket.join("0").is_file(),
            "and so did the pass: a live pass keeps the window"
        );
        assert!(
            engine
                .retention
                .holding(&0)
                .expect("file 0's entity")
                .installed
                .is_some(),
            "the policy stands, which a slack pass would have dropped"
        );

        // And the next tick sees the new value in both halves.
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Stop)]
        );
        assert!(!bucket.join("0").exists(), "and its bytes go");
    }

    /// **The aside rule: a subtitle read during playback is not a switch.**
    ///
    /// A player fetching a side file of the torrent it is playing opens a
    /// stream on another file of that torrent, and there is nothing in the
    /// request to tell that apart from a viewer skipping to the next
    /// episode. What tells them apart is whether anything is still reading
    /// the file that is playing: while a read of it is open the open on the
    /// other file is an aside and the live file does not move, and the
    /// video is not deleted out from under the player at the next tick.
    /// Once no read of it is left, the same open *is* the viewer moving on.
    #[tokio::test]
    async fn an_open_on_another_file_while_the_first_is_being_read_is_not_a_switch() {
        let (enginefs, _counters) = test_enginefs_with_file_count(2);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        engine.begin_retention(0).await;
        assert_eq!(enginefs.live().reading().file_of(TEST_HASH), Some(0));

        // A response is still delivering file 0.
        let reader = engine
            .retention
            .reader_on(&0)
            .expect("file 0 has an entity");
        reader.promises(0..1);

        enginefs.on_stream_start(TEST_HASH, 1).await;
        assert_eq!(
            enginefs.live().reading().file_of(TEST_HASH),
            Some(0),
            "an open on a side file while the video is still being read is an aside"
        );

        // The response ends, and the same open moves the live file.
        drop(reader);
        enginefs.on_stream_start(TEST_HASH, 1).await;
        assert_eq!(
            enginefs.live().reading().file_of(TEST_HASH),
            Some(1),
            "with nothing left reading file 0, this is the viewer moving on"
        );
    }

    /// **An aside keeps the film in the want-set.**
    ///
    /// The subtitle's open leaves the film live, and its selection has to
    /// say the same: planned from the open alone it was the subtitle and
    /// the pins, so librqbit stopped queueing the film's pieces -- its
    /// window's fetch-ahead with them -- and called the torrent finished
    /// while the film was still being written. Once nothing reads the film
    /// the same open is a move, and plans the new file alone.
    #[tokio::test]
    async fn an_aside_open_keeps_the_file_being_played_selected() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        enginefs.on_stream_start(TEST_HASH, 0).await;
        engine.begin_retention(0).await;
        let reader = engine
            .retention
            .reader_on(&0)
            .expect("file 0 has an entity");
        reader.promises(0..1);

        enginefs.on_stream_start(TEST_HASH, 1).await;
        assert_eq!(
            (
                *counters.last_active_file.lock().unwrap(),
                *counters.last_hot_file.lock().unwrap()
            ),
            (Some(0), Some(1)),
            "the subtitle's selection left the film out"
        );

        drop(reader);
        enginefs.on_stream_start(TEST_HASH, 1).await;
        assert_eq!(
            (
                *counters.last_active_file.lock().unwrap(),
                *counters.last_hot_file.lock().unwrap()
            ),
            (Some(1), None)
        );
    }

    /// **A selection leaves the other files' stream counts alone.**
    ///
    /// Selecting a file used to wipe the count of every other file of the
    /// torrent -- a rule from when one file per torrent was playing. The
    /// counts are of responses still open, and each is ended by its own
    /// `on_stream_end`: wiped, a film's count read nothing while its body
    /// was still being delivered, and the delayed cleanup that asks it
    /// thought the film was done.
    #[tokio::test]
    async fn opening_another_file_leaves_the_first_ones_stream_count() {
        let (enginefs, _counters) = test_enginefs_with_file_count(2);
        let count = |file_idx: usize| {
            let enginefs = &enginefs;
            async move {
                enginefs
                    .active_file_streams
                    .read()
                    .await
                    .get(&(TEST_HASH.to_string(), file_idx))
                    .copied()
                    .unwrap_or(0)
            }
        };
        enginefs.on_stream_start(TEST_HASH, 0).await;
        enginefs.on_stream_start(TEST_HASH, 1).await;
        assert_eq!(
            (count(0).await, count(1).await),
            (1, 1),
            "the second file's selection wiped the first one's open response"
        );
        enginefs.on_stream_end(TEST_HASH, 0).await;
        assert_eq!((count(0).await, count(1).await), (0, 1));
    }

    /// **The next episode, opened before the last one's read closed, takes
    /// the cell once that read has closed.**
    ///
    /// A player opens episode two before the server has seen episode one's
    /// connection close, so the open finds episode one still read and is
    /// an aside. Nothing asked again: the cell stayed on episode one, and
    /// episode two, the one being watched, was protected only while a read
    /// of it was open -- its first seek gap handed it to the slack pass.
    /// The end of a read that is not the live file's moves nothing, and
    /// neither does one while the live file is still read elsewhere.
    #[tokio::test]
    async fn the_live_files_last_read_closing_hands_the_cell_to_the_file_still_read() {
        let (enginefs, counters) = test_enginefs_with_file_count(3);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        enginefs.on_stream_start(TEST_HASH, 0).await;
        engine.begin_retention(0).await;
        let first = engine.retention.reader_on(&0).expect("an entity");
        first.promises(0..1);
        let second = engine.retention.reader_on(&0).expect("an entity");
        second.promises(0..1);

        enginefs.on_stream_start(TEST_HASH, 1).await;
        engine.begin_retention(1).await;
        let next = engine.retention.reader_on(&1).expect("an entity");
        next.promises(0..1);
        assert_eq!(enginefs.live().reading().file_of(TEST_HASH), Some(0));

        // A subtitle fetched and done: not the live file's read.
        enginefs.on_stream_start(TEST_HASH, 2).await;
        enginefs.on_stream_end(TEST_HASH, 2).await;
        assert_eq!(enginefs.live().reading().file_of(TEST_HASH), Some(0));

        // One of episode one's reads ends, and another is still open.
        drop(first);
        enginefs.on_stream_end(TEST_HASH, 0).await;
        assert_eq!(enginefs.live().reading().file_of(TEST_HASH), Some(0));

        drop(second);
        enginefs.on_stream_end(TEST_HASH, 0).await;
        assert_eq!(
            enginefs.live().reading().file_of(TEST_HASH),
            Some(1),
            "episode one's last read closed and episode two kept its aside"
        );
        assert_eq!(
            (
                *counters.last_active_file.lock().unwrap(),
                *counters.last_hot_file.lock().unwrap()
            ),
            (Some(1), None),
            "and the selection moved with it"
        );
        drop(next);
    }

    /// **The next episode takes the cell when the last one's read closes,
    /// though no byte of it has been read yet.**
    ///
    /// Episode two's open found episode one still read and was an aside;
    /// its own read opens only once the gate, the reconcile and the wait
    /// for its first piece are behind it -- seconds on a file nothing has
    /// fetched -- and episode one's connection closes milliseconds after
    /// the open. Asked of reads alone, the hand-on found no other file
    /// read at that moment and left the cell on episode one for good. An
    /// open response is the viewer's, whether or not it has read yet.
    #[tokio::test]
    async fn the_live_files_last_read_closing_hands_the_cell_to_a_file_opened_but_not_yet_read() {
        let (enginefs, counters) = test_enginefs_with_file_count(3);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        enginefs.on_stream_start(TEST_HASH, 0).await;
        engine.begin_retention(0).await;
        let first = engine.retention.reader_on(&0).expect("an entity");
        first.promises(0..1);

        enginefs.on_stream_start(TEST_HASH, 1).await;
        assert_eq!(enginefs.live().reading().file_of(TEST_HASH), Some(0));

        drop(first);
        enginefs.on_stream_end(TEST_HASH, 0).await;
        assert_eq!(
            enginefs.live().reading().file_of(TEST_HASH),
            Some(1),
            "episode one's last read closed and episode two, opened but not yet read, kept its aside"
        );
        assert_eq!(
            (
                *counters.last_active_file.lock().unwrap(),
                *counters.last_hot_file.lock().unwrap()
            ),
            (Some(1), None),
            "and the selection moved with it"
        );
    }

    /// **A seek on the live file is not a hand-on**, though its old read
    /// has closed and its new one has not begun: the new response is open,
    /// and the file is still the one being played. Handed on, the cell went
    /// to the subtitle open beside it, and the film was slack until the
    /// subtitle's response closed.
    #[tokio::test]
    async fn a_seek_on_the_live_file_keeps_the_cell_beside_an_open_aside() {
        let (enginefs, _counters) = test_enginefs_with_file_count(3);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        enginefs.on_stream_start(TEST_HASH, 0).await;
        engine.begin_retention(0).await;
        let before = engine.retention.reader_on(&0).expect("an entity");
        before.promises(0..1);
        // A subtitle opened beside the film.
        enginefs.on_stream_start(TEST_HASH, 2).await;
        assert_eq!(enginefs.live().reading().file_of(TEST_HASH), Some(0));

        // The seek: the new response opens, then the old one closes.
        enginefs.on_stream_start(TEST_HASH, 0).await;
        drop(before);
        enginefs.on_stream_end(TEST_HASH, 0).await;
        assert_eq!(
            enginefs.live().reading().file_of(TEST_HASH),
            Some(0),
            "a seek handed the film's cell to the subtitle beside it"
        );
    }

    /// **And the aside rule is asked of the cell as it stands when the open
    /// writes it, not as it stood before the open looked the engine up.**
    ///
    /// Episode one has ended and nothing reads it. The player opens episode
    /// two and, at once, its subtitle. The subtitle's open read the cell --
    /// episode one -- and then waited on the registry; episode two's open
    /// moved the cell and its response began delivering in that wait. The
    /// subtitle then asked whether anything still read *episode one*,
    /// heard no, called itself a move and took the cell off the episode
    /// being watched: the next tick's slack pass is due over it, and takes
    /// its bytes the first time the player is between two responses.
    #[tokio::test(start_paused = true)]
    async fn an_open_asks_the_aside_rule_of_the_cell_it_is_about_to_write() {
        let (enginefs, _counters) = test_enginefs_with_file_count(3);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        engine.begin_retention(0).await;
        engine.begin_retention(1).await;
        let torrent = |file_idx| crate::retention::live::LiveEntity::Torrent {
            info_hash: TEST_HASH.to_string(),
            file_idx,
        };
        enginefs.live().open(torrent(0), false);
        let enginefs = Arc::new(enginefs);

        // The subtitle's open, waiting on the registry behind a writer.
        let registry = enginefs.engines.write().await;
        let subtitle = tokio::spawn({
            let enginefs = enginefs.clone();
            async move { enginefs.switch_to(TEST_HASH, 2).await }
        });
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Episode two opens in that wait, and a response starts on it.
        enginefs.live().open(torrent(1), false);
        let reader = engine
            .retention
            .reader_on(&1)
            .expect("file 1 has an entity");
        reader.promises(0..1);
        drop(registry);
        subtitle.await.expect("the subtitle's open");

        assert_eq!(
            enginefs.live().reading().file_of(TEST_HASH),
            Some(1),
            "an open decided against the episode that had ended took the cell off the one being read"
        );
    }

    /// **A body still being delivered keeps its torrent running and its
    /// file's window whole, even once something else is being played.**
    ///
    /// An open read is not liveness -- it is not what a viewer is watching,
    /// and it buys the torrent nothing once it has ended -- but it is an
    /// in-flight response, and taking its bytes out from under it is a
    /// broken read for the player and a fetch the swarm is paid for twice.
    /// So the ladder keeps such a torrent running and the file being read
    /// runs the live pass, however long its own turn was ago.
    #[tokio::test(start_paused = true)]
    async fn a_body_still_being_delivered_keeps_its_torrent_running_and_its_window() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);

        // The viewer opens something else, and a response is still
        // delivering this file.
        let reader = engine
            .retention
            .reader_on(&0)
            .expect("file 0 has an entity");
        reader.promises(0..1);
        nothing_torrent_is_playing(&enginefs);

        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Run)],
            "the torrent a body is still being delivered out of keeps running"
        );
        assert!(
            bucket.join("0").is_file(),
            "and the file being read keeps its window"
        );

        // The response ends.
        drop(reader);
        assert_eq!(
            enginefs.reconcile_tick().await,
            vec![(TEST_HASH.to_string(), Decision::Stop)]
        );
        assert!(!bucket.join("0").exists(), "and its bytes go");
    }

    /// **A stopped playback keeps its window, for as long as it is stopped.**
    ///
    /// There is no clock anywhere in what decides this. A viewer who pauses
    /// -- or closes the player and comes back in an hour -- is still the
    /// one playing this file until they open something else, so the window
    /// round where they got to is kept and resuming inside it plays off the
    /// disk. A hundred ticks pass here and nothing moves; the install a
    /// resume makes finds the policy already describing this file under
    /// this budget and keeps it untouched, which is what makes the resume
    /// free rather than a fresh hold-back.
    #[tokio::test]
    async fn a_stopped_playback_keeps_its_window_and_a_resume_keeps_the_policy() {
        let (enginefs, counters) = test_enginefs_with_file_count(1);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        let windows = || {
            engine
                .retention
                .holding(&0)
                .expect("file 0's entity")
                .windows
        };

        engine
            .retain(enginefs.store_registry(), &playing(0))
            .await
            .expect("a pass");
        let first = windows();
        assert!(!first.is_empty(), "the pass measured a window");

        for _ in 0..100 {
            engine.retain(enginefs.store_registry(), &playing(0)).await;
        }
        assert!(
            bucket.join("0").is_file(),
            "the piece under the head is still here a hundred ticks later"
        );
        assert_eq!(windows(), first, "and the window is the one it was");
        assert_eq!(
            engine.retention.install(0, 0).await,
            crate::retention::owner::InstallOutcome::Kept,
            "the resume keeps the policy: nothing is re-held-back"
        );
    }

    /// **A torrent nobody is playing loses the files nothing ever opened**,
    /// and the delete stops where it is if the viewer comes back.
    ///
    /// A pass walks entities, and an entity exists for a file something
    /// opened. What a torrent holds of the files nothing ever opened -- the
    /// twelve other episodes the swarm filled around the one that was
    /// watched -- is in no pass's extent, and on a torrent nobody is
    /// playing and nobody has pinned there is nothing that will ever want
    /// it again.
    ///
    /// The second half is the door: the liveness cell is read again before
    /// every run rather than carried in from the tick's reading, so an open
    /// that lands mid-delete stops it at the run it is on and the entity
    /// that open installs keeps the rest.
    #[tokio::test]
    async fn a_torrent_nobody_plays_loses_the_files_nothing_opened() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        // Two runs, so the door is asked twice: pieces 0 and 1 of file 0,
        // and 5 and 6 of file 1. Nothing has opened either file.
        for piece in [0u32, 1, 5, 6] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);
        nothing_torrent_is_playing(&enginefs);

        // The first run's hold-back is parked, and the viewer opens the
        // torrent again while it is.
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *counters.advertise_gate.lock().unwrap() = Some((entered_tx, release_rx));
        let running = tokio::spawn({
            let engine = engine.clone();
            let registry = enginefs.store_registry().clone();
            let live = enginefs.live().reading();
            async move { engine.retain(&registry, &live).await }
        });
        tokio::time::timeout(Duration::from_secs(10), entered_rx)
            .await
            .expect("the reclaim reached the call that holds its first run back")
            .expect("the fake said so");
        enginefs.live().open(
            crate::retention::live::LiveEntity::Torrent {
                info_hash: TEST_HASH.to_string(),
                file_idx: 0,
            },
            false,
        );
        release_tx.send(()).expect("the reclaim is waiting on this");
        let pass = running.await.expect("the task").expect("a pass");

        assert_eq!(pass.reclaimed, 2, "the first run went: {pass:?}");
        assert!(!bucket.join("0").exists() && !bucket.join("1").exists());
        assert!(
            bucket.join("5").is_file() && bucket.join("6").is_file(),
            "and the run after the viewer came back was never asked for"
        );

        // With nobody playing it again, the rest goes.
        nothing_torrent_is_playing(&enginefs);
        let pass = engine
            .retain(enginefs.store_registry(), &enginefs.live().reading())
            .await
            .expect("a pass");
        assert_eq!(pass.reclaimed, 2, "{pass:?}");
        assert!(!bucket.join("5").exists() && !bucket.join("6").exists());
    }

    /// **The switch does not wait for the tick.**
    ///
    /// Two seconds is nothing until the thing waiting for the room is the
    /// stream that caused the switch, so the moment a viewer opens
    /// something else the server runs the same slack passes the tick would
    /// have run. `EngineFS::drop_slack` is what the task watching the
    /// liveness cell calls, and what the running-low bell and
    /// `POST /cache/clean` will call after it.
    #[tokio::test]
    async fn dropping_slack_empties_what_nobody_is_playing_without_a_tick() {
        let (enginefs, counters) = test_enginefs_with_file_count(1);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);

        counters.advertised.lock().unwrap().clear();
        enginefs.drop_slack().await;
        assert!(
            bucket.join("0").is_file(),
            "the file being played is not slack"
        );
        assert!(
            counters.advertised.lock().unwrap().is_empty(),
            "and dropping slack is not a pass over it: what a viewer is \
             watching is not measured, committed or trimmed off the tick"
        );
        assert!(
            engine
                .retention
                .holding(&0)
                .expect("the entity being played")
                .windows
                .is_empty(),
            "nor is a window concluded round it: that is the tick's, and a \
             switch is not a reason to measure the film that is playing"
        );

        nothing_torrent_is_playing(&enginefs);
        enginefs.drop_slack().await;
        assert!(
            !bucket.join("0").exists(),
            "and the moment something else is played, its bytes go"
        );
    }

    /// **A slack pass stops where it is if the viewer comes back to the
    /// file it is emptying.**
    ///
    /// The mode a pass runs in is decided once, by its driver, from one
    /// reading of what is being played -- and the pass then spends seconds
    /// unlinking. A viewer who opens the file again in that window is
    /// inside the window a new policy is about to draw, and the run the
    /// pass is on may be the one their playback is reading. So the door
    /// asks the liveness cell again before every run rather than trusting
    /// the mode it started under, and what is left is what the returning
    /// stream finds.
    #[tokio::test]
    async fn a_slack_pass_stops_when_the_file_is_played_again() {
        let (enginefs, counters) = test_enginefs_with_file_count(1);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        // Two runs: pieces 0 and 1, then piece 3.
        for piece in [0u32, 1, 3] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 25]).unwrap();
        }
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);

        // The viewer opens something else, and comes back while the first
        // run of the pass that follows is being unlinked.
        nothing_torrent_is_playing(&enginefs);
        *counters.on_first_drop.lock().unwrap() = Some(Box::new({
            let live = enginefs.live().clone();
            move || {
                live.open(
                    crate::retention::live::LiveEntity::Torrent {
                        info_hash: TEST_HASH.to_string(),
                        file_idx: 0,
                    },
                    false,
                );
            }
        }));

        let pass = engine
            .retain(enginefs.store_registry(), &enginefs.live().reading())
            .await
            .expect("a pass");

        assert_eq!(pass.reclaimed, 2, "the run it was already on: {pass:?}");
        assert!(!bucket.join("0").exists() && !bucket.join("1").exists());
        assert!(
            bucket.join("3").is_file(),
            "and the run after the viewer came back was never asked for"
        );
        assert_eq!(
            *counters.advertised.lock().unwrap().last().unwrap(),
            (0..4, false),
            "the whole extent was held back before a byte of it went"
        );

        // And the entity the stopped pass left behind has no policy any
        // more -- a slack pass drops it -- yet still holds a piece. The
        // next tick has to walk it all the same, or what a refused delete
        // leaves is left for good.
        assert!(
            engine
                .retention
                .holding(&0)
                .expect("the entity is still here: it holds something")
                .installed
                .is_none(),
            "the slack pass dropped the policy"
        );
        nothing_torrent_is_playing(&enginefs);
        let pass = engine
            .retain(enginefs.store_registry(), &enginefs.live().reading())
            .await
            .expect("a pass");
        assert_eq!(pass.reclaimed, 1, "{pass:?}");
        assert!(!bucket.join("3").exists());
        assert!(
            engine.retention.holding(&0).is_none(),
            "and now it holds nothing, so the entity goes"
        );
    }

    /// **An unpinned torrent in error that nobody is playing is removed
    /// with its files.**
    ///
    /// An errored torrent holds no storage for a `drop_pieces` to edit, so
    /// no slack pass can take a byte of it: the drop bails, the pieces
    /// stay, and every later pass offers them again. Nothing else will ever
    /// come for them either -- the torrent announces nothing, and the next
    /// start would rebuild its have-set from exactly those files. So the
    /// tick that finds it is its opportunity, and it is taken whole.
    ///
    /// A pinned one is not: a pin is kept until it is unpinned, and an
    /// error is the reconciler's to recover from.
    #[tokio::test(start_paused = true)]
    async fn an_errored_torrent_nobody_plays_is_removed_with_its_files() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        nothing_torrent_is_playing(&enginefs);
        counters.in_error_state.store(true, Ordering::SeqCst);
        enginefs
            .get_engine(TEST_HASH)
            .await
            .expect("the fixture's engine")
            .pinned_files
            .write()
            .insert(0);

        enginefs.reconcile_tick().await;
        assert!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty(),
            "a pinned torrent is kept, error or no error"
        );
        assert!(enginefs.peek_engine(TEST_HASH).await.is_some());

        enginefs
            .get_engine(TEST_HASH)
            .await
            .expect("the fixture's engine")
            .pinned_files
            .write()
            .clear();
        enginefs.reconcile_tick().await;
        assert_eq!(
            *enginefs.backend.removed_with_files.lock().unwrap(),
            vec![TEST_HASH.to_string()],
            "and an unpinned one goes with its bytes"
        );
        assert!(
            enginefs.peek_engine(TEST_HASH).await.is_none(),
            "the engine goes with it"
        );
    }

    /// **A stream that opens while the tick is walking keeps everything the
    /// tick was about to take.**
    ///
    /// The tick reads what is being played once, before its first engine,
    /// and then spends the rest of the tick unlinking: that is what makes
    /// the ladder and the passes of one tick agree, and it is also a
    /// reading that is out of date by the time the last engine's last file
    /// is reached. The viewer starting a film in that window has
    /// `on_stream_start` write the cell and `begin_retention` install the
    /// policy, both of them before this file's turn comes round -- and a
    /// pass that carried its mode through would then hold that policy's
    /// window back from the swarm, drop the policy, delete the bytes and
    /// forget the entity, leaving the film playing with no window, no
    /// want-set and no deleter until the next seek.
    ///
    /// So the mode is re-established under the turn. Everything here is
    /// the ordinary case: a viewer pressing play on something the last tick
    /// found nobody watching.
    #[tokio::test]
    async fn a_stream_that_opens_under_the_tick_keeps_the_policy_it_installed() {
        let (enginefs, counters) = test_enginefs_with_file_count(1);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        let _store = seeded_store(&enginefs, &engine);

        // The reading the tick took: nothing of this torrent is playing.
        nothing_torrent_is_playing(&enginefs);
        let tick = enginefs.live().reading();
        // And the viewer presses play, in the gap between that reading and
        // this file's turn.
        enginefs.on_stream_start(TEST_HASH, 0).await;
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        counters.advertised.lock().unwrap().clear();

        engine.retain(enginefs.store_registry(), &tick).await;

        assert!(
            bucket.join("0").is_file(),
            "the film being played kept its bytes"
        );
        assert!(
            engine
                .retention
                .holding(&0)
                .expect("the entity the open installed")
                .installed
                .is_some(),
            "and its policy, which is its window and its want-set"
        );
        assert_eq!(
            *counters.advertised.lock().unwrap(),
            vec![],
            "nothing of it was held back from the swarm"
        );
        assert_eq!(
            engine.standing().await.policies.len(),
            1,
            "so a reading of what it holds names the policy standing on it"
        );
    }

    /// **And a file opened while the tick is inside the pass before it.**
    ///
    /// The tick decides every file's mode from one reading and then walks
    /// them one turn at a time, so the gap the mode has to survive is not
    /// the tick's own two seconds but every unlink of every file before
    /// this one. An open landing in there is not always something the cell
    /// can be asked about, either: an open on another file of the torrent
    /// being played is an aside -- a subtitle fetched while the film runs
    /// -- and deliberately moves nothing, so at the instant its policy goes
    /// in, that file is neither live nor being read.
    ///
    /// What it does do is take the file's turn ([`Retention::install`]), so
    /// counting the opens the driver saw is enough: this one is not in that
    /// count, and any later one cannot start until the pass has let the
    /// turn go.
    ///
    /// [`Retention::install`]: crate::retention::owner::Retention::install
    #[tokio::test]
    async fn a_file_opened_while_the_tick_is_in_the_file_before_it_keeps_its_bytes() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        counters.pieces_per_file.store(4, Ordering::SeqCst);
        counters
            .drops_what_it_is_asked
            .store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        enginefs.set_cache_budget(Some(50));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        // One piece of each file, and an entity on each: two films the
        // viewer has opened this session.
        std::fs::write(bucket.join("0"), [7u8; 25]).unwrap();
        std::fs::write(bucket.join("4"), [7u8; 25]).unwrap();
        let _store = seeded_store(&enginefs, &engine);
        engine.begin_retention(0).await;
        engine.note_playhead(0, 0);
        engine.begin_retention(1).await;
        engine.note_playhead(1, 0);

        // Nothing is playing: the tick's reading makes both files slack.
        nothing_torrent_is_playing(&enginefs);
        let tick = enginefs.live().reading();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *counters.advertise_gate.lock().unwrap() = Some((entered_tx, release_rx));
        let running = tokio::spawn({
            let engine = engine.clone();
            let registry = enginefs.store_registry().clone();
            async move { engine.retain(&registry, &tick).await }
        });
        tokio::time::timeout(Duration::from_secs(10), entered_rx)
            .await
            .expect("the pass reached the call that holds file 0 back")
            .expect("the fake said so");

        // The viewer opens file 1 while file 0 is being emptied.
        engine.begin_retention(1).await;
        release_tx.send(()).expect("the pass is waiting on this");
        let pass = running.await.expect("the task").expect("a pass");

        assert_eq!(pass.reclaimed, 1, "file 0 went: {pass:?}");
        assert!(!bucket.join("0").exists());
        assert!(
            bucket.join("4").is_file(),
            "and the file opened under the pass kept its bytes"
        );
        assert!(
            engine
                .retention
                .holding(&1)
                .expect("the entity the open installed")
                .installed
                .is_some(),
            "and the policy that bounds them"
        );
    }

    /// **A pin that lands while the errored torrent's removal is inside the
    /// backend is not told `Ok` about a torrent that is then gone.**
    ///
    /// `pin_download` runs under the hash's pin lock; the removal used not
    /// to. It found the engine in the registry, recorded the pin on it, told
    /// the handle, and answered `Ok` -- and the removal then finished:
    /// `remove_engine_if_current` took that very engine out of the registry,
    /// the torrent had already left the session, and its files with it. The
    /// user's download was kept, according to the answer they got.
    ///
    /// The removal now holds the pin lock, so the pin queues behind it,
    /// finds no engine, and adds the torrent again -- as a pin of a torrent
    /// the session does not have always has. Whatever the pin answers, the
    /// registry must hold a pinned engine for the hash when it answers `Ok`.
    #[tokio::test(start_paused = true)]
    async fn a_pin_under_an_errored_torrents_removal_is_not_told_ok_about_a_torrent_then_gone() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        nothing_torrent_is_playing(&enginefs);
        counters.in_error_state.store(true, Ordering::SeqCst);
        let enginefs = Arc::new(enginefs);

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *enginefs.backend.remove_gate.lock().unwrap() = Some((entered_tx, release_rx));
        let tick = tokio::spawn({
            let enginefs = enginefs.clone();
            async move { enginefs.reconcile_tick().await }
        });
        tokio::time::timeout(TEST_WAIT_BOUND, entered_rx)
            .await
            .expect("the tick reached the backend's removal")
            .expect("the fake said so");

        // The user taps download for offline while the removal is inside
        // the backend.
        let pin = tokio::spawn({
            let enginefs = enginefs.clone();
            async move { enginefs.pin_download(TEST_HASH, 0, None).await }
        });
        // Give the pin every chance to run ahead of the release.
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        release_tx.send(()).expect("the removal is waiting on this");
        tick.await.expect("the tick finished");
        let pinned = pin
            .await
            .expect("the pin task")
            .expect("the pin was accepted");

        let registered = enginefs
            .peek_engine(TEST_HASH)
            .await
            .expect("a pin that answered Ok left a torrent in the registry");
        assert!(
            Arc::ptr_eq(&registered, &pinned),
            "and the engine it answered with is the one registered"
        );
        assert!(registered.is_pinned(), "pinned, as the answer said");
        assert_eq!(registered.pinned_file_indices(), vec![0]);
    }

    /// **And once it holds the lock, the removal asks the pin set and the
    /// cell again.**
    ///
    /// `retain_engine` reads both before it takes the hash's pin lock. No
    /// await separates that reading from the `try_lock`, but a pin running
    /// on another worker thread can record itself and let go of the lock
    /// inside that gap, and the lock is then free for a removal decided
    /// from a pin set without it: the download the user was told is kept
    /// goes with its files. A stream opening on another thread fits the
    /// same gap. So the removal decides again under the lock, and that
    /// decision is what this calls directly -- a current-thread test cannot
    /// put another thread's pin between two statements.
    #[tokio::test(start_paused = true)]
    async fn an_errored_torrents_removal_asks_again_under_the_lock() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        nothing_torrent_is_playing(&enginefs);
        counters.in_error_state.store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        let removed = || enginefs.backend.removed_with_files.lock().unwrap().clone();

        // A pin that landed after the tick's reading and before its lock.
        engine.pinned_files.write().insert(0);
        enginefs.remove_errored_engine_locked(&engine).await;
        assert!(
            removed().is_empty(),
            "a pin that landed before the lock was taken was removed with its files"
        );
        assert!(enginefs.peek_engine(TEST_HASH).await.is_some());

        // A stream that opened in the same gap.
        engine.pinned_files.write().remove(&0);
        enginefs.live().open(
            crate::retention::live::LiveEntity::Torrent {
                info_hash: TEST_HASH.to_string(),
                file_idx: 0,
            },
            false,
        );
        enginefs.remove_errored_engine_locked(&engine).await;
        assert!(
            removed().is_empty(),
            "a torrent opened before the lock was taken was removed with its files"
        );
        assert!(enginefs.peek_engine(TEST_HASH).await.is_some());

        // With neither, it goes: the two refusals above were the checks'.
        nothing_torrent_is_playing(&enginefs);
        enginefs.remove_errored_engine_locked(&engine).await;
        assert_eq!(removed(), vec![TEST_HASH.to_string()]);
        assert!(enginefs.peek_engine(TEST_HASH).await.is_none());
    }

    /// **And the tick does not wait for a pin in flight: it leaves the
    /// removal to the next one.**
    ///
    /// A pin holds the hash's lock across metadata resolution, which is
    /// bounded by `METADATA_RESOLVE_TIMEOUT` and not by the two-second
    /// tick, and every torrent after this one waits for the tick. Taking
    /// the torrent out under that pin is what the lock is there to prevent.
    #[tokio::test(start_paused = true)]
    async fn an_errored_torrents_removal_steps_round_a_pin_in_flight() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        nothing_torrent_is_playing(&enginefs);
        counters.in_error_state.store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        // A pin of this hash, holding its lock.
        let lock = enginefs.pin_lock(TEST_HASH);
        let guard = lock.lock().await;
        let tick = enginefs.live().reading();
        tokio::time::timeout(TEST_WAIT_BOUND, enginefs.retain_engine(&engine, &tick))
            .await
            .expect("the tick waited on a pin in flight");
        assert!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty(),
            "the torrent went with its files under a pin in flight"
        );
        assert!(enginefs.peek_engine(TEST_HASH).await.is_some());

        // The pin is done; the next tick removes it.
        drop(guard);
        enginefs.release_pin_lock(TEST_HASH, lock);
        enginefs.retain_engine(&engine, &tick).await;
        assert!(enginefs.peek_engine(TEST_HASH).await.is_none());
    }

    /// **And a torrent in error that the viewer has just started is not
    /// removed with its files.**
    ///
    /// This is the largest delete in the process -- a torrent and every
    /// byte of it -- and the state it is decided from is one a playback
    /// start is in the middle of leaving: `on_stream_start` writes the cell
    /// and asks the reconciler for a restart, and until that restart lands
    /// the run state still says `Error`. A removal decided from the tick's
    /// reading takes the files out from under the request that asked for
    /// them. So this one delete asks the cell itself, at the instant, like
    /// every unlink below it.
    #[tokio::test(start_paused = true)]
    async fn an_errored_torrent_started_since_the_reading_is_not_removed() {
        let (mut enginefs, counters) = test_enginefs_for_reconciler(1);
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        nothing_torrent_is_playing(&enginefs);
        counters.in_error_state.store(true, Ordering::SeqCst);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        // The tick's reading, taken before its first engine.
        let tick = enginefs.live().reading();
        // The viewer opens it, and the restart has not happened yet.
        enginefs.live().open(
            crate::retention::live::LiveEntity::Torrent {
                info_hash: TEST_HASH.to_string(),
                file_idx: 0,
            },
            false,
        );
        enginefs.retain_engine(&engine, &tick).await;

        assert!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty(),
            "the torrent the viewer just started was removed with its files"
        );
        assert!(enginefs.peek_engine(TEST_HASH).await.is_some());
    }

    /// **The housekeeping sweep never removes the live torrent's engine.**
    ///
    /// The sweep drops an engine nothing has asked about for five minutes,
    /// and it now takes the torrent's files with it. The entity every
    /// window is drawn round lives in that engine's retention owner, so
    /// removing the one being played would take the window out of the map
    /// under the player -- and the files it is reading with it.
    #[tokio::test(start_paused = true)]
    async fn the_housekeeping_sweep_never_removes_the_live_torrents_engine() {
        let TwoEngines {
            enginefs, removed, ..
        } = test_enginefs_with_two_engines();
        enginefs.live().open(
            crate::retention::live::LiveEntity::Torrent {
                info_hash: TEST_HASH.to_string(),
                file_idx: 0,
            },
            false,
        );
        let present = |hash: &str| {
            let engines = enginefs.engines.clone();
            let hash = hash.to_string();
            async move { engines.read().await.contains_key(&hash) }
        };

        tokio::time::sleep(INACTIVE_TORRENT_REMOVE_TIMEOUT + Duration::from_secs(30)).await;
        assert!(
            present(TEST_HASH).await,
            "the engine of the torrent being played must survive"
        );
        assert!(!present(OTHER_HASH).await, "the idle one is removed");
        assert_eq!(*removed.lock().unwrap(), vec![OTHER_HASH.to_string()]);
    }

    /// **The sweep does not hold the engine registry across its awaits.**
    ///
    /// It read the registry under one read guard for the whole decision,
    /// across the three activity maps' locks, and the registry is a
    /// write-preferring `RwLock`: while any of those maps was held, every
    /// writer on the registry -- an add, a removal -- was parked behind
    /// the sweep, and every reader after it behind that writer.
    #[tokio::test(start_paused = true)]
    async fn the_housekeeping_sweep_does_not_hold_the_registry_across_its_awaits() {
        let (enginefs, _counters) = test_enginefs();
        nothing_torrent_is_playing(&enginefs);
        // The sweep that finds the engine idle parks behind this guard.
        let park = enginefs.active_multifile_files.write().await;
        tokio::time::sleep(INACTIVE_TORRENT_REMOVE_TIMEOUT + Duration::from_secs(30)).await;

        let engines = enginefs.engines.clone();
        let writer = tokio::spawn(async move { drop(engines.write().await) });
        assert!(
            wait_until(TEST_WAIT_BOUND, || writer.is_finished()).await,
            "a writer on the engine registry is parked behind the sweep"
        );
        drop(park);
    }

    /// **A stream that opens between the sweep's decision and its removal
    /// keeps its engine.**
    ///
    /// The sweep decides under the registry's read lock, across several
    /// awaits, and removed under a write lock taken later -- with no second
    /// look. A viewer pressing play in that gap had `on_stream_start` write
    /// the cell, find the engine and count the stream, and then watched the
    /// torrent leave the session under the reads it had just opened, its
    /// directory with it.
    ///
    /// The park is the last lock the decision awaits, the multi-file
    /// selection map: held by the test, it stops the sweep with every
    /// reading taken and the removal still to make. A one-file torrent's
    /// stream open does not touch that map, so the open runs to the end
    /// while the sweep is parked.
    #[tokio::test(start_paused = true)]
    async fn a_stream_opened_between_the_sweeps_decision_and_its_removal_keeps_its_engine() {
        let (enginefs, _counters) = test_enginefs();
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        // The fixture's torrent is the one being played; the viewer moves
        // on, and nobody touches the engine for the inactivity window.
        nothing_torrent_is_playing(&enginefs);

        // The sweep that finds it idle parks behind this guard, its
        // decision made.
        let park = enginefs.active_multifile_files.write().await;
        tokio::time::sleep(INACTIVE_TORRENT_REMOVE_TIMEOUT + Duration::from_secs(30)).await;

        // The viewer opens it while the sweep is parked.
        enginefs.on_stream_start(TEST_HASH, 0).await;
        assert!(
            enginefs.live().is_torrent(TEST_HASH),
            "the open wrote the cell before anything else"
        );
        drop(park);
        // The sweep runs to the end of its pass and goes back to sleep.
        tokio::time::sleep(Duration::from_secs(1)).await;

        let kept = enginefs.peek_engine(TEST_HASH).await;
        assert!(
            kept.is_some_and(|kept| Arc::ptr_eq(&kept, &engine)),
            "the engine the stream is reading through is still the registered one"
        );
        assert!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty(),
            "and the torrent was not taken out of the session under it"
        );
    }

    /// **A restart leaves nothing playing, and the first tick stops every
    /// unpinned torrent the session restored.**
    ///
    /// The liveness cell starts empty, and that is the honest reading: a
    /// process that has served nothing is playing nothing. Everything the
    /// last one left is therefore cache with nobody to speak for it, which
    /// is what the boot sweep and the passes are about -- and what a
    /// running torrent would be fetching into.
    #[tokio::test(start_paused = true)]
    async fn a_restart_leaves_nothing_playing_and_stops_every_restored_torrent() {
        let TwoEngines { mut enginefs, .. } = test_enginefs_with_two_engines();
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        if let Some(sweep) = enginefs.take_sweep_task() {
            sweep.abort();
        }
        assert_eq!(
            enginefs.live().reading(),
            crate::retention::live::Reading::nothing(),
            "nothing is playing in a process that has served nothing"
        );

        let mut decided = enginefs.reconcile_tick().await;
        decided.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            decided,
            vec![
                (TEST_HASH.to_string(), Decision::Stop),
                (OTHER_HASH.to_string(), Decision::Stop),
            ],
            "every restored torrent is one nobody is playing"
        );
    }

    /// **A torrent the viewer opens while the tick is walking is not
    /// stopped from the reading the tick started with.**
    ///
    /// The timer takes one copy of what is playing before its first engine
    /// and then spends the tick on the engines in turn, and the pass on the
    /// predecessor can hold it inside a backend call for as long as the
    /// switch task's unlink takes. A stream that opens meanwhile has its
    /// cell written first (`on_stream_start`) and no byte delivered yet, so
    /// `readers()` is 0: read from the copy it is a torrent nobody is
    /// playing, the idle arm answers `Stop`, and the stop is made -- a stop
    /// is never held by the dwell. The next timer's `Run` for it then *is*
    /// held, for `RECONCILE_MIN_DWELL`: the episode the viewer just started
    /// stands still for fifteen seconds. Roughly every other switch, since
    /// the tick is two seconds and the unlink is of the same order.
    ///
    /// So the ladder asks the cell under the hash lock, and only the pass
    /// keeps the tick's reading (its modes for one tick must agree). The
    /// park here is on the first engine's stop rather than on its pass:
    /// the registry is a `HashMap`, so which engine the tick reaches first
    /// is not the test's to choose, and holding the stop open on both lets
    /// whichever comes first be the predecessor.
    #[tokio::test(start_paused = true)]
    async fn a_stream_opened_under_the_tick_is_not_stopped_from_its_reading() {
        let TwoEngines {
            mut enginefs,
            counters,
            ..
        } = test_enginefs_with_two_engines();
        if let Some(sweep) = enginefs.take_sweep_task() {
            sweep.abort();
        }
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        // The boot's pin pass, with nothing pinned: the want-set is back and
        // the ladder reads its bottom arm for both.
        enginefs.apply_pins(Some(Default::default())).await;
        let enginefs = Arc::new(enginefs);

        // Nothing is playing, so the tick stops the first engine it
        // reaches; the fake holds that stop open.
        for counters in &counters {
            counters.hold_stop.store(true, Ordering::SeqCst);
        }
        let tick = tokio::spawn({
            let enginefs = enginefs.clone();
            async move { enginefs.reconcile_tick().await }
        });
        let stops = |idx: usize| counters[idx].stop_torrent.load(Ordering::SeqCst);
        assert!(
            wait_until(TEST_WAIT_BOUND, || stops(0) + stops(1) == 1).await,
            "the tick is inside the first engine's stop"
        );
        let (first, second) = if stops(0) == 1 {
            (0, OTHER_HASH)
        } else {
            (1, TEST_HASH)
        };
        let second_counters = &counters[1 - first];

        // The viewer opens the other torrent while the tick is parked. Its
        // own reconcile (`PlaybackStart`) finds it running and leaves it.
        second_counters.hold_stop.store(false, Ordering::SeqCst);
        enginefs.on_stream_start(second, 0).await;
        assert_eq!(second_counters.stop_torrent.load(Ordering::SeqCst), 0);

        counters[first].stop_gate.notify_one();
        let decisions = tick.await.expect("the tick finished");
        assert!(
            decisions.contains(&(second.to_string(), Decision::Run)),
            "the torrent the viewer opened under the tick is wanted running: {decisions:?}"
        );
        assert_eq!(
            second_counters.stop_torrent.load(Ordering::SeqCst),
            0,
            "and the tick did not stop it from the reading it started with"
        );
        assert_eq!(run_state_of(&enginefs, second).await, RunState::Live);
        assert_eq!(
            run_state_of(&enginefs, [TEST_HASH, OTHER_HASH][first]).await,
            RunState::Paused,
            "the predecessor, which nobody is playing, is stopped"
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
        let pins = crate::piece_store::PinSet::from([(TEST_HASH.to_string(), vec![1usize])]);
        assert_eq!(enginefs.apply_pins(Some(pins)).await, 0);

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
        // A registered store over those files, as a running torrent has:
        // every unlink in the process goes through the store that holds the
        // held set, and a hash no store is registered for keeps its bytes
        // for the next launch's sweep.
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        let store = seeded_store(&enginefs, &engine);

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
        drop(store);

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

    /// **The same delete, with no store registered for the hash: nothing is
    /// unlinked, and the answer says so.**
    ///
    /// There used to be a second door -- the unlink went by path when the
    /// registry had no store for the hash -- and it was the cache cleaner's:
    /// it walked the root, found a directory the session did not claim, and
    /// deleted the files it could name. With the walk gone that door has no
    /// caller left that is not this one, and leaving it open leaves a way to
    /// unlink a piece behind a live store's back: a held bit standing over a
    /// file that has gone, and an unlinked inode kept alive by the store's
    /// cached descriptor. Every unlink in this process now goes through the
    /// registered store, and a hash that has none keeps its bytes for the
    /// next launch's sweep, which takes every directory no pin claims.
    #[tokio::test]
    async fn a_delete_for_a_hash_with_no_registered_store_frees_nothing() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        *counters.output_folder.lock().unwrap() = Some(enginefs.download_dir.join("show"));
        let bucket = enginefs.piece_store().torrent_dir(TEST_HASH).join("0");
        std::fs::create_dir_all(&bucket).unwrap();
        for piece in [3u32, 4] {
            std::fs::write(bucket.join(piece.to_string()), [7u8; 4096]).unwrap();
        }
        *counters.drops_pieces.lock().unwrap() = vec![3, 4];

        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        enginefs.pin_download(TEST_HASH, 1, None).await.unwrap();
        assert_eq!(
            enginefs.unpin_download(TEST_HASH, 0, true).await.unwrap(),
            UnpinOutcome {
                unpinned: true,
                deleted_files: false,
            },
            "the pin goes, and the answer does not claim bytes that are still there"
        );
        assert!(
            bucket.join("3").is_file() && bucket.join("4").is_file(),
            "no store holds these, so nothing here unlinks them by path"
        );
    }

    /// A dormant pin's bytes are its pieces, and an unpin that asks to take
    /// the data goes and takes them: the store's directory for the hash,
    /// which nothing may take for as long as the pin stands -- and the
    /// entry leaves `downloads.json` with the pin, so no client could ask
    /// again either. The directory stays while another file of the same
    /// torrent is still pinned: it holds that file's pieces too.
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

        let pins = crate::piece_store::PinSet::from([(TEST_HASH.to_string(), vec![1usize, 2])]);
        assert_eq!(enginefs.apply_pins(Some(pins)).await, 0);

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
    }

    /// **The hash's directory is whole or gone, never half-deleted.**
    ///
    /// The registry is asked at the door, and `remove_dir_all` is a walk of
    /// the whole download after it: a torrent whose store registered in
    /// the walk found half its pieces under its name. So the directory is
    /// renamed out of the hash's name before anything is deleted, and a
    /// walk that stops partway -- here a bucket the process may not delete
    /// in -- leaves nothing under that name, only a leftover the launch
    /// sweep takes. A process that can delete through a read-only
    /// directory proves nothing here, and the test says so.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_dormant_delete_takes_the_directory_out_of_the_hashs_name_first() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let enginefs = BackendEngineFS::new_with_backend(
            FakeBackend::new(Vec::new()),
            HashMap::new(),
            root.path().join("cache"),
            root.path().join("rqbit-downloads"),
        );
        let pieces = enginefs.piece_store().path().to_path_buf();
        let folder = pieces.join(TEST_HASH);
        std::fs::create_dir_all(folder.join("0")).unwrap();
        std::fs::write(folder.join("0").join("1"), [7u8; 4096]).unwrap();
        let bucket = folder.join("0");
        std::fs::set_permissions(&bucket, std::fs::Permissions::from_mode(0o555)).unwrap();
        if std::fs::remove_file(bucket.join("1")).is_ok() {
            eprintln!("this process deletes through a read-only directory; nothing to prove");
            return;
        }
        let pins = crate::piece_store::PinSet::from([(TEST_HASH.to_string(), vec![0usize])]);
        enginefs.apply_pins(Some(pins)).await;

        let outcome = enginefs.unpin_download(TEST_HASH, 0, true).await.unwrap();
        let left: Vec<std::path::PathBuf> = std::fs::read_dir(&pieces)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        for dir in &left {
            let _ = std::fs::set_permissions(dir.join("0"), std::fs::Permissions::from_mode(0o755));
        }
        assert!(!outcome.deleted_files, "the walk did not finish");
        assert!(
            !folder.exists(),
            "a half-deleted directory stood under the hash's name"
        );
        assert_eq!(left.len(), 1, "{left:?}");
        assert!(
            left[0]
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&format!("{TEST_HASH}.deleting-"))),
            "{left:?}"
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

    /// **A store that registered while the delete was in flight keeps its
    /// bytes.**
    ///
    /// Every question `delete_dormant_download_data` asks before its
    /// `remove_dir_all` -- is another file pinned, is an add pending, does
    /// the session hold the torrent -- is an `await` old by the time the
    /// directory goes. A torrent added, restored or restarted in that gap
    /// has a store registered at `init` and a have-set built from what is
    /// on the disk, and removing the directory under it is the
    /// advertise-then-serve-a-hole this design exists to prevent, reached
    /// from the one door that never went through the backend.
    ///
    /// So the registry is asked at the door, as `retention::unlink` asks it
    /// before a claimless delete. Refusing is the safe direction: a
    /// directory left behind is swept at the next launch, where bytes taken
    /// from under a running check are gone.
    #[tokio::test]
    async fn a_dormant_delete_refuses_a_torrent_whose_store_registered_since() {
        let root = tempfile::tempdir().unwrap();
        let enginefs = BackendEngineFS::new_with_backend(
            FakeBackend::new(Vec::new()),
            HashMap::new(),
            root.path().join("cache"),
            root.path().join("rqbit-downloads"),
        );
        std::fs::create_dir_all(root.path().join("rqbit-downloads")).unwrap();
        let pieces = enginefs.piece_store().path().to_path_buf();
        let folder = pieces.join(TEST_HASH);
        std::fs::create_dir_all(folder.join("0")).unwrap();
        std::fs::write(folder.join("0").join("1"), [7u8; 4096]).unwrap();

        let pins = crate::piece_store::PinSet::from([(TEST_HASH.to_string(), vec![1usize])]);
        assert_eq!(enginefs.apply_pins(Some(pins)).await, 0);

        // The gap: a torrent for this hash is added while the unpin is on
        // its way to the disk, and its store registers as `init` seeds it.
        let layout = Arc::new(
            crate::piece_store::PieceLayout::new(
                4096,
                4096,
                [crate::piece_store::FileSpec::payload(4096)],
            )
            .expect("a layout"),
        );
        let store = crate::piece_store::PieceStore::under(
            Arc::clone(enginefs.store_registry()),
            TEST_HASH,
            layout,
        );
        store
            .init_for_tests()
            .expect("the store seeds and registers");
        assert!(enginefs.store_registry().is_registered(TEST_HASH));

        assert_eq!(
            enginefs.unpin_download(TEST_HASH, 1, true).await.unwrap(),
            UnpinOutcome {
                unpinned: true,
                deleted_files: false,
            },
            "the pin goes, but its bytes are the registered store's now"
        );
        assert!(
            folder.join("0").join("1").is_file(),
            "the piece a live have-set speaks for is still on the disk"
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
        let pins = crate::piece_store::PinSet::from([(TEST_HASH.to_string(), vec![0usize])]);
        // Dormant: the registry has no engine for it, whatever the session
        // holds.
        assert_eq!(enginefs.apply_pins(Some(pins)).await, 0);

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
        let pins = crate::piece_store::PinSet::from([(TEST_HASH.to_string(), vec![0usize])]);
        assert_eq!(enginefs.apply_pins(Some(pins)).await, 0);

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
    ///
    /// **With its files.** An engine nothing has asked about for five
    /// minutes is one whose entities the slack passes have already emptied;
    /// whatever the removal leaves under the root has no deleter left in
    /// this process, because the store that would answer for it goes with
    /// the torrent.
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
