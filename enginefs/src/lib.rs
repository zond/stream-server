use crate::engine::Engine;
use anyhow::Result;
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::AbortHandle;
use tracing::debug;

pub mod backend;
pub mod cache;
pub mod disk_cache;
pub mod engine;
pub mod files;
pub mod metadata_cache;
pub mod metadata_pins;
pub mod piece_cache;
pub mod piece_waiter;
pub mod tracker_prober;
pub mod trackers;

// Re-export TrackerStorage for use by server crate
pub use trackers::TrackerStorage;

use crate::backend::librqbit::LibrqbitBackend;
use crate::backend::priorities::EngineCacheConfig;

use crate::backend::{
    BackendMemoryDiagnostics, HotFilePriorityPlan, TorrentBackend, TorrentFilePriorityPlan,
    TorrentHandle, TorrentListenPort, TorrentPlacement, TorrentSource,
};

const INACTIVE_TORRENT_REMOVE_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes
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
const INACTIVE_TORRENT_PAUSE_GRACE: Duration = Duration::from_secs(15);
const HLS_PLAYBACK_LEASE_TTL: Duration = Duration::from_secs(300);
const NATIVE_LIFECYCLE_HLS_PLAYBACK_LEASE_TTL: Duration = Duration::from_secs(15);

/// Instance-relative clock for the idle bookkeeping (engine `last_accessed`,
/// playback leases, magnet-add polls). Seconds since the owning
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
    /// The download volume has less than the file's missing bytes plus
    /// [`PIN_FREE_SPACE_MARGIN`] available.
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
    /// backend ones -- whose chains name absolute cache and downloads paths
    /// (`relocating {hash} into {folder}`, librqbit's `error opening
    /// {path}`) -- become a generic sentence, and a failed magnet add
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

/// Bytes a file has allocated on disk (`st_blocks` * 512 on Unix): a
/// pre-sized sparse placeholder has none, a file with data written about
/// its length. 0 where std exposes no block count, or the file cannot be
/// read.
#[cfg(unix)]
fn allocated_bytes(path: &std::path::Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path)
        .map(|metadata| metadata.blocks().saturating_mul(512))
        .unwrap_or(0)
}

#[cfg(not(unix))]
fn allocated_bytes(_path: &std::path::Path) -> u64 {
    0
}

/// `Fn(path) -> u64` probe of the volume holding a path: available bytes
/// (`fs4::available_space`) or an identity (`volume_id`) telling two
/// paths on the same volume apart from two on different ones.
type VolumeProbe = Arc<dyn Fn(&std::path::Path) -> std::io::Result<u64> + Send + Sync>;

/// An identity of the volume holding `path`, equal for two paths on the
/// same volume: the device id on Unix; the path prefix (drive letter or
/// UNC share) on Windows, where std exposes no stable volume serial.
#[cfg(unix)]
fn volume_id(path: &std::path::Path) -> std::io::Result<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).map(|metadata| metadata.dev())
}

#[cfg(windows)]
fn volume_id(path: &std::path::Path) -> std::io::Result<u64> {
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
fn volume_id(_path: &std::path::Path) -> std::io::Result<u64> {
    Err(std::io::Error::other("volume identity unavailable"))
}

/// One pinned offline download, see [`BackendEngineFS::pinned_downloads`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PinnedDownload {
    pub info_hash: String,
    pub file_idx: usize,
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
    /// `None` for an entry that stands for a relocation in progress
    /// (`BackendEngineFS::relocate_engine`): it ends with the move, not
    /// with its pollers, so the sweep leaves it alone.
    abort: Option<AbortHandle>,
}

/// Registry-wide id source for [`PendingMagnetAdd::id`].
static NEXT_ADD_ID: AtomicU64 = AtomicU64::new(1);

impl<H: TorrentHandle + 'static> PendingMagnetAdd<H> {
    /// An entry settled by hand -- the returned sender resolves `done` --
    /// for a torrent that is briefly without an engine while it is moved
    /// (see `BackendEngineFS::relocate_engine`). A sender dropped without
    /// sending fails every waiter with [`MagnetAddError::TaskFailed`].
    fn settled_later(
        info_hash: String,
        trackers: Arc<[String]>,
    ) -> (tokio::sync::oneshot::Sender<MagnetAddResult<H>>, Self) {
        let (settle, settled) = tokio::sync::oneshot::channel();
        let done = settled
            .map(move |received| match received {
                Ok(result) => result,
                Err(_dropped) => Err(MagnetAddError::TaskFailed {
                    info_hash,
                    reason: "relocation ended without settling its waiters".to_string(),
                }),
            })
            .boxed()
            .shared();
        (
            settle,
            Self {
                done,
                trackers,
                id: NEXT_ADD_ID.fetch_add(1, Ordering::Relaxed),
                abort: None,
            },
        )
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

fn hls_playback_lease_ttl_secs() -> u64 {
    HLS_PLAYBACK_LEASE_TTL.as_secs()
}

fn playback_lease_is_active(lease: &PlaybackLease, now: u64) -> bool {
    lease.expires_at_secs > now
}

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
    /// HLS playback is made of short segment reads. A lease keeps the file wanted
    /// while the player is buffered and no response body is currently open.
    active_playback_leases: Arc<RwLock<HashMap<(String, usize), PlaybackLease>>>,
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
    /// Where pinned downloads are placed (`<downloads_dir>/<info hash>`),
    /// see [`Self::set_downloads_dir`]. `None` = the backend's default root.
    downloads_dir: parking_lot::RwLock<Option<std::path::PathBuf>>,
    /// One lock per info hash serialising `pin_download` calls for the same
    /// torrent (a relocation must not be raced by a second pin); entries
    /// live only while a call holds or waits for them.
    pin_locks: parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Persisted pins of torrents the backend did not have at startup
    /// (see [`Self::restore_pinned_downloads`]): kept in the persisted file
    /// and applied by the next `pin_download` of the torrent, or dropped by
    /// `unpin_download`. Never held across an `.await`.
    dormant_pins: parking_lot::Mutex<BTreeMap<String, std::collections::BTreeSet<usize>>>,
    /// Available-bytes probe for the free-space check in `pin_download`
    /// (`fs4::available_space`; tests substitute one).
    free_space_probe: VolumeProbe,
    /// Volume-identity probe telling whether a relocation stays on one
    /// volume (a rename, free) or crosses to another (a copy, which the
    /// free-space check has to size; `volume_id`, tests substitute one).
    volume_id_probe: VolumeProbe,
    /// Epoch of every `*_secs` timestamp this instance and its engines keep.
    clock: Clock,
}

#[derive(Debug, Clone)]
struct PlaybackLease {
    last_seen_secs: u64,
    expires_at_secs: u64,
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
pub struct ActivePlaybackLeaseSnapshot {
    pub info_hash: String,
    pub file_idx: usize,
    pub last_seen_secs: u64,
    pub expires_in_secs: u64,
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
    pub active_playback_leases: Vec<ActivePlaybackLeaseSnapshot>,
    pub active_multifile_selections: Vec<MultiFileActiveSelectionSnapshot>,
    pub idle_paused_torrents: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EngineDiagnosticsSnapshot {
    pub uptime_secs: u64,
    pub streams: StreamActivitySnapshot,
    pub memory: BackendMemoryDiagnostics,
}

pub type EngineFS = BackendEngineFS<LibrqbitBackend>;

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
        let mut engines_map = HashMap::new();
        for (hash, handle) in restored_handles {
            engines_map.insert(
                hash.clone(),
                Arc::new(Engine::new_with_handle(handle, &hash, clock)),
            );
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
            active_playback_leases: Arc::new(RwLock::new(HashMap::new())),
            active_multifile_files: Arc::new(RwLock::new(HashMap::new())),
            priority_generation: Arc::new(AtomicU64::new(0)),
            disk_cache: None,
            seeding_enabled: Arc::new(AtomicBool::new(true)),
            magnet_adds: Arc::new(RwLock::new(HashMap::new())),
            downloads_dir: parking_lot::RwLock::new(None),
            pin_locks: parking_lot::Mutex::new(HashMap::new()),
            dormant_pins: parking_lot::Mutex::new(BTreeMap::new()),
            free_space_probe: Arc::new(|path| fs4::available_space(path)),
            volume_id_probe: Arc::new(volume_id),
            clock,
        };

        let engines_clone = engines.clone();
        let backend_clone = efs.backend.clone();
        let active_streams_clone = efs.active_streams.clone();
        let active_file_streams_clone = efs.active_file_streams.clone();
        let active_file_clone = efs.active_file.clone();
        let active_playback_leases_clone = efs.active_playback_leases.clone();
        let active_multifile_files_clone = efs.active_multifile_files.clone();
        let seeding_flag = efs.seeding_enabled.clone();
        let magnet_adds_clone = efs.magnet_adds.clone();
        let clock = efs.clock;
        tokio::spawn(async move {
            loop {
                // Run fairly frequently so seeding stops promptly after the
                // user disables it; torrent removal is still gated by the much
                // longer inactivity timeout below, so this only changes how
                // quickly the seeding-disabled pause reacts.
                tokio::time::sleep(Duration::from_secs(15)).await;
                let mut to_remove = Vec::new();
                let now = clock.now_secs();

                let expired_leases = {
                    let mut leases = active_playback_leases_clone.write().await;
                    let expired = leases
                        .iter()
                        .filter(|(_, lease)| !playback_lease_is_active(lease, now))
                        .map(|(key, _)| key.clone())
                        .collect::<Vec<_>>();
                    for key in &expired {
                        leases.remove(key);
                    }
                    expired
                };

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
                                // A relocation in progress has no task to
                                // abort and ends on its own.
                                let Some(abort) = &pending.abort else {
                                    return true;
                                };
                                abort.abort();
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

                for (info_hash, file_idx) in expired_leases {
                    tracing::info!(
                        info_hash = %info_hash,
                        file_idx,
                        ttl_secs = hls_playback_lease_ttl_secs(),
                        "HLS playback lease expired"
                    );

                    let still_active = {
                        let streams = active_file_streams_clone.read().await;
                        streams
                            .get(&(info_hash.clone(), file_idx))
                            .copied()
                            .unwrap_or(0)
                            > 0
                    };
                    if still_active {
                        continue;
                    }

                    let active_selection = {
                        let mut selections = active_multifile_files_clone.write().await;
                        match selections.get(&info_hash).cloned() {
                            Some(selection) if selection.file_idx == file_idx => {
                                selections.remove(&info_hash);
                                tracing::info!(
                                    info_hash = %info_hash,
                                    file_idx,
                                    generation = selection.generation,
                                    reason = "hls-lease-expired",
                                    "multifile_active_file_cleared"
                                );
                                None
                            }
                            Some(selection) => Some(selection),
                            None => None,
                        }
                    };

                    {
                        let mut active = active_file_clone.write().await;
                        if let Some((ref h, idx)) = *active
                            && h == &info_hash
                            && idx == file_idx
                        {
                            *active = active_selection
                                .as_ref()
                                .map(|selection| (info_hash.clone(), selection.file_idx));
                        }
                    }

                    let engine = {
                        let engines = engines_clone.read().await;
                        engines.get(&info_hash).cloned()
                    };
                    if let Some(engine) = engine {
                        if engine.handle.manages_playback_lifecycle() {
                            // A native-lifecycle backend expires its own
                            // generation-scoped HLS lease and performs the
                            // acknowledged pause. Shared delayed cleanup must
                            // not race it.
                            continue;
                        }
                        let reconciled = Self::reconcile_multifile_engine(
                            engine.clone(),
                            active_selection
                                .as_ref()
                                .map(|selection| selection.file_idx),
                            None,
                            active_selection
                                .as_ref()
                                .map(|selection| selection.generation)
                                .unwrap_or(0),
                            "hls-lease-expired",
                        )
                        .await;
                        if reconciled {
                            continue;
                        }

                        if let Err(err) = engine.handle.clear_file_streaming(file_idx).await {
                            tracing::warn!(
                                info_hash = %info_hash,
                                file_idx,
                                error = %err,
                                "Failed to clear file priorities after HLS playback lease expired"
                            );
                        } else {
                            tracing::info!(
                                info_hash = %info_hash,
                                file_idx,
                                "Cleared file priorities after HLS playback lease expired"
                            );
                        }
                    }
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
                        let active_playback_lease_count = {
                            let leases = active_playback_leases_clone.read().await;
                            leases
                                .iter()
                                .filter(|((stream_hash, _), lease)| {
                                    stream_hash == hash && playback_lease_is_active(lease, now)
                                })
                                .count()
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
                        } else if active_playback_lease_count > 0 {
                            Some("active_playback_leases")
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
                                active_playback_lease_count,
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
                                active_playback_lease_count,
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

                // Stop all torrent activity when seeding is disabled and no
                // playback is active. A later playback request resumes the
                // torrent before making its requested file wanted.
                if !seeding_flag.load(Ordering::Relaxed) {
                    let read = engines_clone.read().await;
                    for (hash, engine) in read.iter() {
                        if engine.handle.manages_playback_lifecycle() {
                            continue;
                        }
                        // A pinned download must keep downloading; seeding
                        // is stopped for it the moment it completes and is
                        // unpinned, like any other torrent.
                        if engine.is_pinned() {
                            continue;
                        }
                        let hash_active = {
                            let streams = active_streams_clone.read().await;
                            streams.get(hash).copied().unwrap_or(0) > 0
                        };
                        let file_active = {
                            let streams = active_file_streams_clone.read().await;
                            streams
                                .iter()
                                .any(|((stream_hash, _), count)| stream_hash == hash && *count > 0)
                        };
                        let playback_active = {
                            let leases = active_playback_leases_clone.read().await;
                            leases.iter().any(|((stream_hash, _), lease)| {
                                stream_hash == hash && playback_lease_is_active(lease, now)
                            })
                        };
                        let multifile_active = {
                            let selections = active_multifile_files_clone.read().await;
                            selections.contains_key(hash)
                        };
                        let reader_active = engine.active_streams.load(Ordering::SeqCst) > 0;
                        if hash_active
                            || file_active
                            || playback_active
                            || multifile_active
                            || reader_active
                        {
                            continue;
                        }

                        // A magnet that is still fetching its info dictionary
                        // must remain connected to the swarm. Inactive engines
                        // are removed by the separate cleanup policy.
                        if !engine.handle.stats().await.has_metadata {
                            continue;
                        }

                        if engine.idle_paused.swap(true, Ordering::Relaxed) {
                            continue;
                        }

                        tracing::info!(
                            info_hash = %hash,
                            "torrent_paused_idle"
                        );
                        if let Err(e) = engine.handle.pause_torrent().await {
                            tracing::warn!(
                                info_hash = %hash,
                                error = %e,
                                "Failed to pause idle torrent"
                            );
                            engine.idle_paused.store(false, Ordering::Relaxed);
                        }
                    }
                }
            }
        });

        efs
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

    /// Wrap a backend handle in an `Engine` and publish it, or return the
    /// engine already registered for the same info hash.
    async fn register_engine(
        engines: &EngineRegistry<B::Handle>,
        handle: B::Handle,
        clock: Clock,
    ) -> Arc<Engine<B::Handle>> {
        let info_hash = handle.info_hash();
        let mut engines = engines.write().await;
        if let Some(engine) = engines.get(&info_hash) {
            engine.touch();
            return engine.clone();
        }
        let engine = Arc::new(Engine::new_with_handle(handle, &info_hash, clock));
        engines.insert(info_hash, engine.clone());
        engine
    }

    pub async fn add_torrent(
        &self,
        source: TorrentSource,
        extra_trackers: Option<Vec<String>>,
    ) -> Result<Arc<Engine<B::Handle>>> {
        let trackers = self.merged_trackers(extra_trackers).await;
        debug!(count = trackers.len(), "Adding torrent with trackers");
        let handle = self.backend.add_torrent(source, trackers).await?;
        Ok(Self::register_engine(&self.engines, handle, self.clock).await)
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
    /// this call starts. Like the trackers, the placement only counts when
    /// this call is the one that adds the torrent: an existing engine or an
    /// in-flight add is joined as is, wherever it lives -- the caller checks
    /// `TorrentHandle::output_folder` (see `pin_download`, which relocates).
    pub async fn get_or_add_magnet_placed(
        &self,
        info_hash: &str,
        extra_trackers: Option<Vec<String>>,
        placement: TorrentPlacement,
    ) -> Result<Arc<Engine<B::Handle>>, MagnetAddError> {
        match self
            .lookup_or_begin_add_magnet(info_hash, extra_trackers, true, placement)
            .await
        {
            EngineLookup::Ready(engine) => Ok(engine),
            EngineLookup::Adding(pending) => pending.done.await,
            EngineLookup::Failed(failed) => Err(failed.error),
        }
    }

    async fn lookup_or_begin_add_magnet(
        &self,
        info_hash: &str,
        extra_trackers: Option<Vec<String>>,
        retry_failed: bool,
        placement: TorrentPlacement,
    ) -> EngineLookup<B::Handle> {
        let info_hash = info_hash.to_lowercase();
        if let Some(engine) = self.get_engine(&info_hash).await {
            return EngineLookup::Ready(engine);
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
            return EngineLookup::Ready(engine);
        }
        let now = self.clock.now_secs();
        if let Some(entry) = adds.get(&info_hash) {
            entry.touch(now);
            match &entry.state {
                MagnetAddState::Adding(pending) => return EngineLookup::Adding(pending.clone()),
                MagnetAddState::Failed(failed) if !retry_failed => {
                    return EngineLookup::Failed(failed.clone());
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
            self.clock,
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
        EngineLookup::Adding(pending)
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
        clock: Clock,
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
                    Ok(Ok(handle)) => Ok(Self::register_engine(&engines, handle, clock).await),
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
            abort: Some(abort),
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

    pub async fn get_engine(&self, info_hash: &str) -> Option<Arc<Engine<B::Handle>>> {
        let engines = self.engines.read().await;
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

    /// Where the registry's engines write, for the cache cleaner: every
    /// file at the path the backend reports (`TorrentHandle::file_path`, or
    /// the output folder joined with the file's name when the backend knows
    /// the folder but not the path), `<download_dir>/<name>` for a backend
    /// that knows neither. A torrent without a file list yet protects its
    /// output folder (or `<download_dir>/<name>`). Paths outside the
    /// download dir (a torrent placed under the downloads dir) are harmless
    /// to the cleaner, which never walks there.
    pub async fn protected_paths(&self) -> Vec<std::path::PathBuf> {
        let engines: Vec<_> = self.engines.read().await.values().cloned().collect();
        let mut paths = Vec::new();
        for engine in engines {
            let stats = engine.get_statistics().await;
            let folder = engine
                .handle
                .output_folder()
                .filter(|folder| *folder != self.download_dir);
            if stats.files.is_empty() {
                paths.push(folder.unwrap_or_else(|| self.download_dir.join(&stats.name)));
                continue;
            }
            for (idx, file) in stats.files.iter().enumerate() {
                let path = match engine.handle.file_path(idx).await {
                    Some(path) => path,
                    None => folder
                        .as_deref()
                        .unwrap_or(&self.download_dir)
                        .join(&file.path),
                };
                paths.push(path);
            }
        }
        paths
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
        let idle_paused_torrents = engines
            .iter()
            .filter(|(_, engine)| engine.idle_paused.load(Ordering::Relaxed))
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
        let active_playback_leases = self
            .active_playback_leases
            .read()
            .await
            .iter()
            .filter(|(_, lease)| playback_lease_is_active(lease, now))
            .map(
                |((info_hash, file_idx), lease)| ActivePlaybackLeaseSnapshot {
                    info_hash: info_hash.clone(),
                    file_idx: *file_idx,
                    last_seen_secs: lease.last_seen_secs,
                    expires_in_secs: lease.expires_at_secs.saturating_sub(now),
                },
            )
            .collect();
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
            active_playback_leases,
            active_multifile_selections,
            idle_paused_torrents,
        }
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
    pub async fn on_stream_start(&self, info_hash: &str, file_idx: usize) {
        let info_hash = info_hash.to_lowercase();
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
            self.activate_file(&info_hash, file_idx, false, "stream")
                .await;
        }

        // Also update legacy active_streams counter
        {
            let mut streams = self.active_streams.write().await;
            let count = streams.entry(info_hash.clone()).or_insert(0);
            *count += 1;
        }
        {
            let mut streams = self.active_file_streams.write().await;
            let count = streams.entry((info_hash.clone(), file_idx)).or_insert(0);
            *count += 1;
        }

        tracing::debug!(
            "Stream started for {} file_idx={} (shared mode)",
            info_hash,
            file_idx
        );
    }

    async fn activate_file(
        &self,
        info_hash: &str,
        file_idx: usize,
        keep_file_downloading: bool,
        source: &'static str,
    ) {
        let mut is_multifile = false;
        if let Some(engine) = self.get_engine(info_hash).await {
            engine.touch();
            if engine.handle.manages_playback_lifecycle() {
                *self.active_file.write().await = Some((info_hash.to_string(), file_idx));
                return;
            }
            is_multifile = engine.handle.file_count().await > 1;

            // Any new playback activity must resume a torrent that was paused
            // by the idle seeding-disabled policy.
            if let Err(err) = engine.handle.resume_torrent().await {
                tracing::warn!(
                    info_hash = %info_hash,
                    file_idx,
                    error = %err,
                    source,
                    "Failed to resume torrent for active stream"
                );
            } else if engine.idle_paused.swap(false, Ordering::Relaxed) {
                tracing::info!(
                    info_hash = %info_hash,
                    file_idx,
                    source,
                    "torrent_resumed_for_stream"
                );
            }

            if keep_file_downloading
                && !is_multifile
                && let Err(err) = engine.handle.keep_file_downloading(file_idx).await
            {
                tracing::warn!(
                    info_hash = %info_hash,
                    file_idx,
                    error = %err,
                    source,
                    "Failed to keep HLS playback file downloading"
                );
            }
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
            let mut leases = self.active_playback_leases.write().await;
            leases.retain(|(hash, idx), _| hash.as_str() != info_hash || *idx == file_idx);
        }
        {
            let mut streams = self.active_file_streams.write().await;
            streams.retain(|(hash, idx), _| hash.as_str() != info_hash || *idx == file_idx);
        }
        {
            let mut active = self.active_file.write().await;
            *active = Some((info_hash.to_string(), file_idx));
        }

        engine.touch();
        if let Err(err) = engine.handle.resume_torrent().await {
            tracing::warn!(
                info_hash = %info_hash,
                file_idx,
                generation,
                error = %err,
                source,
                "Failed to resume torrent for multi-file active file"
            );
        } else {
            engine.idle_paused.store(false, Ordering::Relaxed);
        }

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
    /// With a downloads dir set ([`Self::set_downloads_dir`]) the torrent
    /// lives in `<downloads_dir>/<info hash>`: a torrent this call adds is
    /// placed there wanting only `file_idx`, and one already managed
    /// elsewhere (streamed first, then pinned) is relocated -- dropped from
    /// the backend keeping its files, files moved, re-added in place
    /// (`TorrentBackend::relocate_torrent`), which re-checks whatever was
    /// downloaded (`checking` phase) and replaces the registry's engine;
    /// readers still open on the old one end, and for the length of the
    /// move the hash is looked up as an in-flight add (see
    /// [`Self::relocate_engine`]), so requests wait for the new engine
    /// rather than reaching the dropped torrent. Without a downloads dir
    /// everything stays in the backend's root.
    ///
    /// Persisted: the pin set is written to `pinned-downloads.json` in the
    /// download dir on every change and re-applied by
    /// [`Self::restore_pinned_downloads`] at startup to the torrents the
    /// backend restored (librqbit keeps the file in its persisted
    /// `only_files` and the folder in its `output_folder`, so the download
    /// itself resumes in place; the pin makes it exempt from eviction again).
    /// Pins the restore found no torrent for stay dormant in that file and
    /// come back with the torrent: a pin of it applies them alongside the
    /// new one.
    ///
    /// Calls for the same info hash run one at a time (`pin_locks`): a
    /// relocation drops the torrent from the backend and re-adds it, and a
    /// second pin racing through that window would find nothing to
    /// relocate, fail, and could tear down the engine the first one has
    /// just published. Serialised, the second caller simply sees the torrent
    /// already in place.
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
        let folder = self.download_folder(info_hash);
        let placement = TorrentPlacement {
            output_folder: folder.clone(),
            only_files: Some(vec![file_idx]),
        };
        let was_managed = self.get_engine(info_hash).await.is_some();
        // Asked before the add, which creates the folder: one that already
        // exists may hold the data of an earlier session whose backend
        // records are gone (a purged cache dir with the downloads dir
        // intact) -- data a refused pin must not delete, and that the
        // free-space check must not count as missing while the backend is
        // still checking it (`downloaded` reads 0 until then).
        let folder_existed = match &folder {
            Some(folder) => tokio::fs::try_exists(folder).await.unwrap_or(false),
            None => false,
        };
        let engine = self
            .get_or_add_magnet_placed(info_hash, extra_trackers.clone(), placement)
            .await?;
        let checked = self
            .check_pin_preconditions(
                &engine,
                file_idx,
                folder.as_deref(),
                was_managed || folder_existed,
            )
            .await;
        if let Err(error) = checked {
            // Torn down only when demonstrably this pin's: no engine
            // existed before the call and the torrent sits in the folder
            // only pins place under. A torrent in the cache root stays for
            // the idle sweeper -- it may be a stream request's in-flight
            // add this call joined (dropping it would fail the stream about
            // to open on it), and without a downloads dir the two cannot be
            // told apart. A folder this add created holds nothing but the
            // placeholder librqbit pre-sized, so it goes too; a folder that
            // was there before keeps whatever it holds.
            let placed_by_this_pin = !was_managed
                && !engine.is_pinned()
                && folder.is_some()
                && engine.handle.output_folder() == folder;
            if placed_by_this_pin {
                self.remove_engine_if_current(&engine).await;
                let dropped = if !folder_existed {
                    self.backend.remove_torrent_and_files(info_hash).await
                } else {
                    self.backend.remove_torrent(info_hash).await
                };
                if let Err(e) = dropped {
                    debug!(info_hash, error = %e, "could not drop the torrent added for a refused pin");
                }
            }
            return Err(error);
        }
        // Pinned before anything slow happens: a relocation can outlast the
        // idle window, and `is_pinned()` is what keeps the sweeper off the
        // engine meanwhile. Undone below if the pin does not go through
        // (unless the file was pinned already -- a re-pin changes nothing).
        let newly_pinned = engine.pinned_files.write().insert(file_idx);
        let relocated = match folder {
            Some(folder)
                if engine
                    .handle
                    .output_folder()
                    .is_some_and(|current| current != folder) =>
            {
                self.relocate_engine(engine.clone(), folder, extra_trackers)
                    .await
            }
            _ => Ok(engine.clone()),
        };
        let engine = match relocated {
            Ok(engine) => engine,
            Err(error) => {
                if newly_pinned {
                    engine.pinned_files.write().remove(&file_idx);
                    // The failure path may have rebuilt the registry's
                    // engine from the old pin set, this file included.
                    if let Some(current) = self.get_engine(info_hash).await {
                        current.pinned_files.write().remove(&file_idx);
                    }
                }
                return Err(error);
            }
        };
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
        if engine.idle_paused.swap(false, Ordering::Relaxed)
            && let Err(err) = engine.handle.resume_torrent().await
        {
            tracing::warn!(
                info_hash = %engine.info_hash,
                file_idx,
                error = %err,
                "Failed to resume idle-paused torrent for pinned download"
            );
            engine.idle_paused.store(true, Ordering::Relaxed);
        }
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
    /// [`Self::delete_download_data`]: the whole torrent when this was its
    /// last pin, only this file while other pins hold. A dormant pin has no
    /// torrent to delete anything of.
    ///
    /// Takes the same per-hash lock as [`Self::pin_download`]: an unpin
    /// issued while a pin of that hash is still resolving metadata or
    /// relocating queues behind it and applies to the finished pin.
    /// Unlocked it would find no engine (the hash is parked in the magnet
    /// registry for the length of the add), report that nothing was pinned,
    /// and leave the pin to land and be persisted behind it.
    pub async fn unpin_download(
        &self,
        info_hash: &str,
        file_idx: usize,
        delete_files: bool,
    ) -> Result<bool> {
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
    ) -> Result<bool> {
        let info_hash = info_hash.to_string();
        let Some(engine) = self.get_engine(&info_hash).await else {
            // No torrent, but maybe a dormant pin waiting for it.
            let was_dormant = {
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
                removed
            };
            if was_dormant {
                self.persist_pinned_downloads().await;
                tracing::info!(info_hash, file_idx, "dormant_download_unpinned");
            }
            if delete_files {
                tracing::info!(
                    info_hash,
                    file_idx,
                    "no torrent to delete the data of; only the pin was dropped"
                );
            }
            return Ok(was_dormant);
        };
        let was_pinned = engine.pinned_files.write().remove(&file_idx);
        engine.handle.unpin_file(file_idx).await?;
        let torrent_dropped = if delete_files {
            self.delete_download_data(&engine, file_idx).await
        } else {
            false
        };
        if was_pinned {
            // A dropped torrent has no handle to reconcile against any more
            // (and nothing left to want).
            if !torrent_dropped {
                self.reconcile_with_active_selection(engine.clone(), "unpin_download")
                    .await;
            }
            self.persist_pinned_downloads().await;
            tracing::info!(
                info_hash = %engine.info_hash,
                file_idx,
                pinned = ?engine.pinned_file_indices(),
                deleted = delete_files,
                "download_unpinned"
            );
        }
        Ok(was_pinned)
    }

    /// Delete what `file_idx` of `engine` occupies on disk, for an unpin
    /// that was asked to take the data with it. With no pin left on the
    /// torrent that is the whole torrent: dropped from the registry and
    /// from the backend with its files and its (then empty) per-torrent
    /// folder ([`TorrentBackend::remove_torrent_and_files`]). While other
    /// files of it stay pinned the torrent must keep running, so only this
    /// file is unlinked -- the unpin's reconcile drops it from the want-set
    /// right after, so the backend does not write it again. Returns whether
    /// the torrent was dropped. Best effort: a failure is logged, the unpin
    /// stands.
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
            match tokio::fs::remove_file(&path).await {
                Ok(()) => tracing::info!(
                    info_hash = %engine.info_hash,
                    file_idx,
                    path = %path.display(),
                    "download_file_deleted"
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => tracing::warn!(
                    info_hash = %engine.info_hash,
                    file_idx,
                    path = %path.display(),
                    %error,
                    "could not delete the download's file"
                ),
            }
            return false;
        }
        self.remove_engine_if_current(engine).await;
        match self
            .backend
            .remove_torrent_and_files(&engine.info_hash)
            .await
        {
            Ok(()) => tracing::info!(
                info_hash = %engine.info_hash,
                file_idx,
                "download_deleted"
            ),
            Err(error) => tracing::warn!(
                info_hash = %engine.info_hash,
                file_idx,
                %error,
                "could not delete the download's torrent"
            ),
        }
        true
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
        let pins = match tokio::fs::read(&path).await {
            Ok(bytes) => match serde_json::from_slice::<BTreeMap<String, Vec<usize>>>(&bytes) {
                Ok(pins) => pins,
                Err(error) => {
                    tracing::warn!(path = %path.display(), %error, "ignoring unreadable pinned downloads file");
                    return 0;
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return 0,
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "could not read pinned downloads file");
                return 0;
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
        tracing::info!(restored, "pinned_downloads_restored");
        restored
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
    /// torrent the backend did not restore (its output folder on a volume
    /// that is not mounted, say). They are not downloading anything: the
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

    /// The file exists, and the volume it is (or will be) written to has
    /// room for what the pin will write there plus [`PIN_FREE_SPACE_MARGIN`]:
    /// the file's missing bytes -- or, when the pin relocates the torrent
    /// onto another volume, the full length of every file the move copies
    /// (the pinned file, and any other file with verified data; see
    /// `TorrentBackend::relocate_torrent`), since a copy writes sparse
    /// files out in full and the pinned file's rest is downloaded there.
    /// A relocation within one volume is a rename and costs nothing beyond
    /// the missing bytes. A volume that cannot be probed is not held against
    /// the pin (logged); volumes whose identity cannot be told apart are
    /// assumed different (the strict side).
    ///
    /// A torrent that stays where it is and is still `checking` data that
    /// may already be there (`may_have_data_in_place`: it was managed
    /// before this call -- a restart, a relocation -- or the pin added it
    /// into a folder that already existed) is not measured at all:
    /// `downloaded` reads 0 until the check ends, so a complete file would
    /// be refused as if it had everything left to write -- and refusing
    /// changes nothing about a download librqbit already wants. A torrent
    /// measured while checking -- one the pin relocates, or added into a
    /// fresh folder -- counts what its files have allocated on disk
    /// instead of `downloaded` (see `allocated_bytes`).
    async fn check_pin_preconditions(
        &self,
        engine: &Arc<Engine<B::Handle>>,
        file_idx: usize,
        folder: Option<&std::path::Path>,
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
        let current = engine.handle.output_folder();
        let relocating_to = match (folder, current.as_deref()) {
            (Some(folder), Some(current)) if folder != current => Some(folder),
            _ => None,
        };
        if may_have_data_in_place
            && relocating_to.is_none()
            && stats.phase == crate::backend::StartupPhase::Checking
        {
            return Ok(());
        }
        let crosses_volumes = match (relocating_to, current.as_deref()) {
            (Some(to), Some(from)) => !self.same_volume(from, to),
            _ => false,
        };
        // While the torrent is checking, `downloaded` reads 0 for every
        // file. Measured anyway (about to be moved -- a pin during a
        // restart's check -- or freshly added), each file counts what it
        // has allocated on disk, the fallback the backend's relocation
        // uses to decide what moves. Nothing allocated, no path, or no
        // block counts on this platform is the strict side: all missing.
        let checking = stats.phase == crate::backend::StartupPhase::Checking;
        let mut have = Vec::with_capacity(stats.files.len());
        for (idx, file) in stats.files.iter().enumerate() {
            have.push(if checking {
                match engine.handle.file_path(idx).await {
                    Some(path) => allocated_bytes(&path).min(file.length),
                    None => 0,
                }
            } else {
                file.downloaded
            });
        }
        let required = if crosses_volumes {
            stats
                .files
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx == file_idx || have[*idx] > 0)
                .map(|(_, file)| file.length)
                .fold(0u64, u64::saturating_add)
        } else {
            stats
                .files
                .get(file_idx)
                .map(|file| file.length.saturating_sub(have[file_idx]))
                .unwrap_or(0)
        };
        if required == 0 {
            return Ok(());
        }
        let volume = folder
            .map(std::path::Path::to_path_buf)
            .or(current)
            .unwrap_or_else(|| self.download_dir.clone());
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

    /// Whether `from` and `to` (or their nearest existing ancestors) are on
    /// the same volume. Unknown counts as different: the free-space check
    /// then sizes a copy rather than a rename.
    fn same_volume(&self, from: &std::path::Path, to: &std::path::Path) -> bool {
        let probe = &*self.volume_id_probe;
        match (
            probe_at_existing_ancestor(probe, from),
            probe_at_existing_ancestor(probe, to),
        ) {
            (Ok(a), Ok(b)) => a == b,
            (from_id, to_id) => {
                debug!(
                    from = %from.display(),
                    to = %to.display(),
                    ?from_id,
                    ?to_id,
                    "volume identity unknown; sizing the relocation as a copy"
                );
                false
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

    #[cfg(test)]
    fn set_volume_id_probe(
        &mut self,
        probe: impl Fn(&std::path::Path) -> std::io::Result<u64> + Send + Sync + 'static,
    ) {
        self.volume_id_probe = Arc::new(probe);
    }

    /// Where pinned downloads go: `<dir>/<info hash>` per torrent, or the
    /// backend's default root with `None`. Applies to pins issued from now
    /// on; torrents already pinned elsewhere are relocated by their next
    /// `pin_download`, not by this call.
    pub fn set_downloads_dir(&self, dir: Option<std::path::PathBuf>) {
        let mut current = self.downloads_dir.write();
        if *current != dir {
            tracing::info!(downloads_dir = ?dir, "downloads_dir_updated");
        }
        *current = dir;
    }

    pub fn downloads_dir(&self) -> Option<std::path::PathBuf> {
        self.downloads_dir.read().clone()
    }

    /// The folder a pinned `info_hash` is placed in under the downloads
    /// dir, `None` without one.
    pub fn download_folder(&self, info_hash: &str) -> Option<std::path::PathBuf> {
        self.downloads_dir
            .read()
            .as_ref()
            .map(|dir| dir.join(info_hash.to_lowercase()))
    }

    /// Move `engine`'s torrent into `folder` (see [`Self::pin_download`]),
    /// wanting its pinned files (the caller has already recorded the pin
    /// being made), and publish the backend's new handle as the registry's
    /// engine for the hash with the pins carried over.
    ///
    /// For the length of the move the hash has no live torrent: the backend
    /// drops it before moving the files, and a handle to the dropped
    /// torrent answers every stream or stats call with an error. So the old
    /// engine leaves the registry first and a [`PendingMagnetAdd`] takes
    /// its place -- requests meanwhile find `EngineLookup::Adding` (stats
    /// report `resolvingMetadata`, a stream waits) exactly as for a magnet
    /// whose metadata is resolving, and nobody starts a second add for the
    /// hash. The entry is settled with the new engine once it is published
    /// (a cross-device copy can take minutes; the sweep leaves the entry
    /// alone).
    ///
    /// On failure the backend may or may not still manage the torrent: the
    /// registry entry is rebuilt from `get_torrent` when it does (and the
    /// waiters get that engine); otherwise there is no engine, the waiters
    /// get the error, and no failure record is left -- the next request
    /// re-adds through the registry instead of using a handle to a torrent
    /// that is gone. An engine somebody else published for the hash
    /// meanwhile is theirs to keep.
    async fn relocate_engine(
        &self,
        engine: Arc<Engine<B::Handle>>,
        folder: std::path::PathBuf,
        extra_trackers: Option<Vec<String>>,
    ) -> Result<Arc<Engine<B::Handle>>, PinDownloadError> {
        let wanted = engine.pinned_files.read().clone();
        let placement = TorrentPlacement {
            output_folder: Some(folder.clone()),
            only_files: Some(wanted.into_iter().collect()),
        };
        let trackers = self.merged_trackers(extra_trackers).await;
        tracing::info!(
            info_hash = %engine.info_hash,
            from = ?engine.handle.output_folder(),
            to = ?folder,
            "download_relocating"
        );
        let (settle, pending) =
            PendingMagnetAdd::settled_later(engine.info_hash.clone(), trackers.clone().into());
        self.begin_relocation(&engine, pending.clone()).await;
        let relocated = self
            .backend
            .relocate_torrent(&engine.info_hash, placement, trackers)
            .await;
        let (result, settled) = match relocated {
            Ok(handle) => {
                let engine = self.replace_engine(&engine, handle).await;
                (Ok(engine.clone()), Ok(engine))
            }
            Err(error) => {
                tracing::warn!(
                    info_hash = %engine.info_hash,
                    error = %format!("{error:#}"),
                    "download_relocate_failed"
                );
                let settled = match self.backend.get_torrent(&engine.info_hash).await {
                    Some(handle) => Ok(self.replace_engine(&engine, handle).await),
                    None => match self.get_engine(&engine.info_hash).await {
                        Some(other) => Ok(other),
                        None => Err(MagnetAddError::Backend {
                            info_hash: engine.info_hash.clone(),
                            error: Arc::new(anyhow::anyhow!(
                                "relocation failed and the torrent is no longer managed: {error:#}"
                            )),
                        }),
                    },
                };
                let error = PinDownloadError::Backend(error.context(format!(
                    "relocating {} into {}",
                    engine.info_hash,
                    folder.display()
                )));
                (Err(error), settled)
            }
        };
        self.end_relocation(&engine.info_hash, &pending).await;
        // Whoever awaited the entry: the engine is published (or the hash is
        // free for a fresh add) by now.
        let _ = settle.send(settled);
        result
    }

    /// Take `engine` out of the registry (only while it still is the
    /// registry's engine) and put `pending` in its place in the magnet-add
    /// registry, atomically for lookups: `lookup_or_begin_add_magnet` takes
    /// the add registry before the engines, as this does.
    async fn begin_relocation(
        &self,
        engine: &Arc<Engine<B::Handle>>,
        pending: PendingMagnetAdd<B::Handle>,
    ) {
        let now = self.clock.now_secs();
        let mut adds = self.magnet_adds.write().await;
        let mut engines = self.engines.write().await;
        if engines
            .get(&engine.info_hash)
            .is_some_and(|current| Arc::ptr_eq(current, engine))
        {
            engines.remove(&engine.info_hash);
        }
        adds.insert(
            engine.info_hash.clone(),
            MagnetAddEntry {
                state: MagnetAddState::Adding(pending),
                last_polled_secs: AtomicU64::new(now),
            },
        );
    }

    /// Drop the relocation's registry entry, if it still is `pending`. The
    /// engine (if any) is published before this, so a lookup between the
    /// two always finds one or the other.
    async fn end_relocation(&self, info_hash: &str, pending: &PendingMagnetAdd<B::Handle>) {
        let mut adds = self.magnet_adds.write().await;
        if matches!(
            adds.get(info_hash).map(|entry| &entry.state),
            Some(MagnetAddState::Adding(current)) if current.id == pending.id
        ) {
            adds.remove(info_hash);
        }
    }

    /// Publish `handle` as the engine for `old`'s hash, carrying the pins.
    async fn replace_engine(
        &self,
        old: &Arc<Engine<B::Handle>>,
        handle: B::Handle,
    ) -> Arc<Engine<B::Handle>> {
        let engine = Arc::new(Engine::new_with_handle(handle, &old.info_hash, self.clock));
        *engine.pinned_files.write() = old.pinned_files.read().clone();
        self.engines
            .write()
            .await
            .insert(old.info_hash.clone(), engine.clone());
        engine
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

    /// Refresh a lease only if playback is already known to be active. This is
    /// used by stats.json so a progress poll cannot create a new download.
    pub async fn refresh_existing_hls_playback(
        &self,
        info_hash: &str,
        file_idx: usize,
        source: &'static str,
    ) -> bool {
        let info_hash = info_hash.to_lowercase();
        let now = self.clock.now_secs();
        let engine = self.get_engine(&info_hash).await;
        let native_lifecycle = engine
            .as_ref()
            .is_some_and(|engine| engine.handle.manages_playback_lifecycle());
        let ttl = if native_lifecycle {
            NATIVE_LIFECYCLE_HLS_PLAYBACK_LEASE_TTL
        } else {
            HLS_PLAYBACK_LEASE_TTL
        };
        let refreshed = {
            let mut leases = self.active_playback_leases.write().await;
            let key = (info_hash.clone(), file_idx);
            match leases.get_mut(&key) {
                Some(lease) if playback_lease_is_active(lease, now) => {
                    lease.last_seen_secs = now;
                    lease.expires_at_secs = now.saturating_add(ttl.as_secs());
                    true
                }
                Some(_) => {
                    leases.remove(&key);
                    false
                }
                None => false,
            }
        };

        if refreshed {
            if let Some(engine) = engine {
                if native_lifecycle {
                    engine.touch();
                    if let Err(error) = engine.handle.refresh_hls_activity(file_idx, source).await {
                        tracing::warn!(
                            info_hash = %info_hash,
                            file_idx,
                            source,
                            %error,
                            "Failed to refresh existing native-lifecycle HLS playback"
                        );
                    }
                } else {
                    self.activate_file(&info_hash, file_idx, true, source).await;
                }
            }
            tracing::debug!(
                info_hash = %info_hash,
                file_idx,
                source,
                "Existing HLS playback lease refreshed"
            );
        }

        refreshed
    }

    /// Called when a stream ends for a torrent file
    pub async fn on_stream_end(&self, info_hash: &str, file_idx: usize) {
        let info_hash = info_hash.to_lowercase();

        let hash_streams_remaining = {
            let mut streams = self.active_streams.write().await;
            if let Some(count) = streams.get_mut(&info_hash) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    streams.remove(&info_hash);
                    0
                } else {
                    *count
                }
            } else {
                0
            }
        };

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

        if !native_lifecycle && hash_streams_remaining == 0 && file_streams_remaining == 0 {
            self.schedule_torrent_pause(info_hash.clone());
        }

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

    /// Promptly pause an idle torrent shortly after the last stream on it ends
    /// when seeding is disabled. A later playback request resumes it before
    /// selecting the requested file.
    fn schedule_torrent_pause(&self, info_hash: String) {
        // Dropping a Tokio JoinHandle detaches the task instead of cancelling it.
        drop(self.schedule_torrent_pause_after(info_hash, INACTIVE_TORRENT_PAUSE_GRACE));
    }

    fn schedule_torrent_pause_after(
        &self,
        info_hash: String,
        delay: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let engines = self.engines.clone();
        let active_streams = self.active_streams.clone();
        let active_file_streams = self.active_file_streams.clone();
        let active_playback_leases = self.active_playback_leases.clone();
        let active_multifile_files = self.active_multifile_files.clone();
        let seeding_enabled = self.seeding_enabled.clone();
        let clock = self.clock;

        tokio::spawn(async move {
            tokio::time::sleep(delay).await;

            if seeding_enabled.load(Ordering::Relaxed) {
                return;
            }

            let hash_active = {
                let streams = active_streams.read().await;
                streams.get(&info_hash).copied().unwrap_or(0) > 0
            };
            let file_active = {
                let streams = active_file_streams.read().await;
                streams
                    .iter()
                    .any(|((hash, _), count)| hash == &info_hash && *count > 0)
            };
            let playback_active = {
                let now = clock.now_secs();
                let leases = active_playback_leases.read().await;
                leases.iter().any(|((hash, _), lease)| {
                    hash == &info_hash && playback_lease_is_active(lease, now)
                })
            };
            let multifile_active = {
                let selections = active_multifile_files.read().await;
                selections.contains_key(&info_hash)
            };
            if hash_active || file_active || playback_active || multifile_active {
                tracing::debug!(
                    info_hash = %info_hash,
                    hash_active,
                    file_active,
                    playback_active,
                    multifile_active,
                    "Skipping idle pause because stream activity resumed"
                );
                return;
            }

            let engine = {
                let engines = engines.read().await;
                engines.get(&info_hash).cloned()
            };
            if let Some(engine) = engine {
                let reader_active = engine.active_streams.load(Ordering::SeqCst) > 0;
                if reader_active {
                    tracing::debug!(
                        info_hash = %info_hash,
                        reader_active,
                        "Skipping idle pause because a file or metadata reader is active"
                    );
                    return;
                }
                if engine.is_pinned() {
                    tracing::debug!(
                        info_hash = %info_hash,
                        "Skipping idle pause because the torrent has a pinned download"
                    );
                    return;
                }
                if !engine.handle.stats().await.has_metadata {
                    tracing::debug!(
                        info_hash = %info_hash,
                        "Skipping idle pause while torrent metadata is unresolved"
                    );
                    return;
                }
                engine.touch();
                if engine.idle_paused.swap(true, Ordering::Relaxed) {
                    return;
                }
                tracing::info!(
                    info_hash = %info_hash,
                    "torrent_paused_idle"
                );
                if let Err(err) = engine.handle.pause_torrent().await {
                    tracing::warn!(
                        info_hash = %info_hash,
                        error = %err,
                        "Failed to pause inactive torrent after grace period"
                    );
                    engine.idle_paused.store(false, Ordering::Relaxed);
                }
            }
        })
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
        let active_playback_leases = self.active_playback_leases.clone();
        let active_multifile_files = self.active_multifile_files.clone();
        let clock = self.clock;
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
            let playback_active = {
                let now = clock.now_secs();
                let leases = active_playback_leases.read().await;
                leases
                    .get(&key)
                    .map(|lease| playback_lease_is_active(lease, now))
                    .unwrap_or(false)
            };
            if playback_active {
                tracing::info!(
                    info_hash = %info_hash,
                    file_idx,
                    "Skipping delayed cleanup because HLS playback lease is active"
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
        let (backend, restored) =
            LibrqbitBackend::new(download_dir.clone(), TorrentListenPort::default()).await?;
        let efs = Self::new_with_backend(backend, restored, root_dir.join("cache"), download_dir);
        efs.restore_pinned_downloads().await;
        Ok(efs)
    }

    /// Only `config.listen_port` is consumed here: librqbit takes the rest of
    /// its settings from the session defaults (see `update_torrent_settings`).
    pub async fn new_with_storage(
        root_dir: std::path::PathBuf,
        config: crate::backend::BackendConfig,
        tracker_storage: Option<Arc<dyn crate::trackers::TrackerStorage>>,
    ) -> Result<Self> {
        let download_dir = root_dir.join("rqbit-downloads");
        let (backend, restored) =
            LibrqbitBackend::new(download_dir.clone(), config.listen_port).await?;
        let efs = Self::new_with_backend_and_storage(
            backend,
            restored,
            root_dir.join("cache"),
            download_dir,
            tracker_storage,
        );
        efs.restore_pinned_downloads().await;
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

    /// librqbit does not support reconfiguring a live session; settings apply
    /// on the next restart.
    pub async fn update_torrent_settings(
        &self,
        _profile: &crate::backend::TorrentSpeedProfile,
        _privacy: &crate::backend::TorrentPrivacyConfig,
    ) {
        tracing::debug!(
            "librqbit backend does not support dynamic session settings; they apply on restart"
        );
    }

    pub fn set_seeding_enabled(&self, enabled: bool) {
        self.seeding_enabled.store(enabled, Ordering::Relaxed);
        self.backend.set_seeding_enabled(enabled);
        tracing::info!(seeding_enabled = enabled, "Seeding policy updated");

        // When seeding is turned back on, resume torrents the seeding-disabled
        // policy had paused so they can seed again. Turning seeding off is
        // handled lazily by the periodic loop / schedule_torrent_pause.
        if enabled {
            let engines = self.engines.clone();
            tokio::spawn(async move {
                let read = engines.read().await;
                for engine in read.values() {
                    if engine.handle.manages_playback_lifecycle() {
                        continue;
                    }
                    if engine.idle_paused.swap(false, Ordering::Relaxed)
                        && let Err(err) = engine.handle.resume_torrent().await
                    {
                        tracing::warn!(
                            info_hash = %engine.info_hash,
                            error = %err,
                            "Failed to resume torrent after re-enabling seeding"
                        );
                        engine.idle_paused.store(true, Ordering::Relaxed);
                    }
                }
            });
        }
    }

    pub fn seeding_enabled(&self) -> bool {
        self.seeding_enabled.load(Ordering::Relaxed)
    }

    /// Mark the torrent as active. librqbit has no session-wide streaming mode,
    /// so this is a best-effort resume of a torrent the idle policy had paused.
    pub async fn focus_torrent(&self, target_info_hash: &str) {
        if let Some(engine) = self.get_engine(&target_info_hash.to_lowercase()).await {
            if engine.handle.manages_playback_lifecycle() {
                return;
            }
            if engine.idle_paused.swap(false, Ordering::Relaxed)
                && let Err(err) = engine.handle.resume_torrent().await
            {
                tracing::warn!(
                    info_hash = %engine.info_hash,
                    error = %err,
                    "Failed to resume torrent on focus"
                );
                engine.idle_paused.store(true, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::librqbit::{DeferredSelection, await_initialized};
    use crate::backend::{
        BackendFileInfo, EngineStats, FileStreamTrait, Growler, PeerDiscovery, PeerSearch,
        PieceReadiness, StartupPhase, StatsFile, StatsOptions, SwarmCap, TorrentFilePriorityPlan,
    };
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    const TEST_HASH: &str = "0123456789abcdef0123456789abcdef01234567";

    #[derive(Default)]
    struct FakeCounters {
        keep_file_downloading: AtomicUsize,
        clear_file_streaming: AtomicUsize,
        resume_torrent: AtomicUsize,
        pause_torrent: AtomicUsize,
        reconcile_file_priorities: AtomicUsize,
        prepare_file_for_streaming: AtomicUsize,
        get_file_reader: AtomicUsize,
        /// Selection updates / reader opens that went through while the fake
        /// torrent was still initializing. The gate must keep this at zero.
        applied_while_initializing: AtomicUsize,
        last_active_file: Mutex<Option<usize>>,
        last_generation: AtomicU64,
        /// Test knob: report every file as fully on disk (seeded torrent)
        /// instead of the default half-downloaded state.
        seeded: AtomicBool,
        pin_file: AtomicUsize,
        unpin_file: AtomicUsize,
        /// The fake handle's own pin set (what the real backend keeps in its
        /// `PinnedFiles` map), reported through `stats()`.
        pinned: Mutex<std::collections::BTreeSet<usize>>,
        /// What `output_folder()` reports; set by the fake backend's
        /// placed add and relocate.
        output_folder: Mutex<Option<std::path::PathBuf>>,
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
        /// Every `relocate_torrent` request (hash, placement), in order.
        relocations: Arc<Mutex<Vec<(String, TorrentPlacement)>>>,
        /// Test knob: make `relocate_torrent` fail (the torrent stays
        /// managed where it was, as the real backend's recovery leaves it).
        fail_relocate: Arc<AtomicBool>,
        /// Test knob: while set, `relocate_torrent` blocks (after recording
        /// the request) until the test adds a permit to `relocate_hold`,
        /// standing in for a slow cross-device move.
        hold_relocate: Arc<AtomicBool>,
        relocate_hold: Arc<tokio::sync::Semaphore>,
        /// Test knob: `get_torrent` finds nothing (the torrent is gone from
        /// the session, as after a relocation that failed to re-add).
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
                relocations: Arc::new(Mutex::new(Vec::new())),
                fail_relocate: Arc::new(AtomicBool::new(false)),
                hold_relocate: Arc::new(AtomicBool::new(false)),
                relocate_hold: Arc::new(tokio::sync::Semaphore::new(0)),
                hide_torrents: Arc::new(AtomicBool::new(false)),
                hold_add: Arc::new(AtomicBool::new(false)),
                add_hold: Arc::new(tokio::sync::Semaphore::new(0)),
            }
        }
    }

    #[async_trait::async_trait]
    impl TorrentBackend for FakeBackend {
        type Handle = FakeHandle;

        async fn add_torrent(
            &self,
            _source: TorrentSource,
            _trackers: Vec<String>,
        ) -> Result<Self::Handle> {
            Ok(self.handles[0].clone())
        }

        async fn add_torrent_placed(
            &self,
            _source: TorrentSource,
            _trackers: Vec<String>,
            placement: TorrentPlacement,
        ) -> Result<Self::Handle> {
            let handle = self.handles[0].clone();
            if placement.output_folder.is_some() {
                *handle.counters.output_folder.lock().unwrap() = placement.output_folder.clone();
            }
            self.placements.lock().unwrap().push(placement);
            if self.hold_add.load(Ordering::SeqCst) {
                self.add_hold.acquire().await.unwrap().forget();
            }
            Ok(handle)
        }

        /// A fresh handle clone reporting the new folder, like the real
        /// backend's re-added torrent.
        async fn relocate_torrent(
            &self,
            info_hash: &str,
            placement: TorrentPlacement,
            _trackers: Vec<String>,
        ) -> Result<Self::Handle> {
            self.relocations
                .lock()
                .unwrap()
                .push((info_hash.to_string(), placement.clone()));
            if self.hold_relocate.load(Ordering::SeqCst) {
                self.relocate_hold.acquire().await.unwrap().forget();
            }
            if self.fail_relocate.load(Ordering::SeqCst) {
                anyhow::bail!("fake relocation failed");
            }
            let handle = self
                .handles
                .iter()
                .find(|h| h.info_hash == info_hash)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("not managed"))?;
            *handle.counters.output_folder.lock().unwrap() = placement.output_folder;
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

    #[async_trait::async_trait]
    impl TorrentHandle for FakeHandle {
        fn info_hash(&self) -> String {
            self.info_hash.clone()
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
                is_finished: seeded,
                has_metadata: !self.files.is_empty(),
                phase,
                checked_bytes: (phase == StartupPhase::Checking).then_some(0),
                check_total_bytes: (phase == StartupPhase::Checking).then_some(total_len),
                initial_window_ready_bytes: None,
                initial_window_bytes: None,
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

        fn output_folder(&self) -> Option<std::path::PathBuf> {
            self.counters.output_folder.lock().unwrap().clone()
        }

        /// Like the real backend: the output folder joined with the file's
        /// name, unknown without a folder.
        async fn file_path(&self, file_idx: usize) -> Option<std::path::PathBuf> {
            let folder = self.output_folder()?;
            Some(folder.join(&self.files.get(file_idx)?.name))
        }

        async fn resume_torrent(&self) -> Result<()> {
            self.counters.resume_torrent.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn pause_torrent(&self) -> Result<()> {
            self.counters.pause_torrent.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn keep_file_downloading(&self, _file_idx: usize) -> Result<()> {
            self.counters
                .keep_file_downloading
                .fetch_add(1, Ordering::SeqCst);
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
        let root = std::env::temp_dir().join("enginefs-hls-lease-tests");
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
        let root = std::env::temp_dir().join("enginefs-two-engine-tests");
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

    // (a) A torrent that is Initializing when the request arrives: the request
    // blocks (no error, no empty body) and succeeds once the torrent is ready.
    #[tokio::test(start_paused = true)]
    async fn get_file_waits_for_initializing_torrent_then_succeeds() {
        use crate::backend::priorities::PlaybackIntent;
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
            .try_get_file_with_intent(1, 0, 1, PlaybackIntent::DirectInitial)
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
        use crate::backend::priorities::PlaybackIntent;
        let init_timeout = Duration::from_secs(3);
        let (enginefs, counters, init) = test_enginefs_initializing(2, init_timeout);
        let engine = enginefs.get_engine(TEST_HASH).await.expect("engine");

        let started = tokio::time::Instant::now();
        // Outer bound is the "no hang" assertion (virtual time auto-advances).
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            engine.try_get_file_with_intent(1, 0, 1, PlaybackIntent::DirectInitial),
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

    /// Insert an active playback lease directly. Production leases are created
    /// elsewhere now; tests that exercise the generic lease/cleanup machinery
    /// seed a lease this way.
    async fn insert_active_lease(enginefs: &BackendEngineFS<FakeBackend>, file_idx: usize) {
        let now = enginefs.clock.now_secs();
        enginefs.active_playback_leases.write().await.insert(
            (TEST_HASH.to_string(), file_idx),
            PlaybackLease {
                last_seen_secs: now,
                expires_at_secs: now.saturating_add(300),
            },
        );
    }

    #[tokio::test]
    async fn refresh_existing_hls_playback_does_not_create_lease() {
        let (enginefs, counters) = test_enginefs();

        let refreshed = enginefs
            .refresh_existing_hls_playback(TEST_HASH, 0, "stats-json")
            .await;

        assert!(!refreshed);
        assert_eq!(counters.keep_file_downloading.load(Ordering::SeqCst), 0);
        assert!(
            enginefs
                .stream_activity_snapshot()
                .await
                .active_playback_leases
                .is_empty()
        );
    }

    #[tokio::test]
    async fn multi_file_selects_only_requested_file() {
        let (enginefs, counters) = test_enginefs_with_file_count(3);

        enginefs.on_stream_start(TEST_HASH, 1).await;

        let snapshot = enginefs.stream_activity_snapshot().await;
        assert_eq!(snapshot.active_multifile_selections.len(), 1);
        assert_eq!(snapshot.active_multifile_selections[0].file_idx, 1);
        assert_eq!(*counters.last_active_file.lock().unwrap(), Some(1));
        assert_eq!(counters.keep_file_downloading.load(Ordering::SeqCst), 0);
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
    /// buffering/ready phases, and ignores out-of-range indices.
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
            pinned: false,
            complete: ready == total,
        };
        let base = EngineStats {
            name: "t".into(),
            info_hash: TEST_HASH.into(),
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
            is_finished: false,
            has_metadata: true,
            phase: StartupPhase::Buffering,
            checked_bytes: None,
            check_total_bytes: None,
            initial_window_ready_bytes: None,
            initial_window_bytes: None,
            peer_discovery: PeerDiscovery::default(),
            error: None,
            pinned_files: Vec::new(),
        };

        let mut stats = base.clone();
        stats.focus_stream_file(0);
        assert_eq!(stats.phase, StartupPhase::Buffering);
        assert_eq!(stats.initial_window_ready_bytes, Some(10));
        assert_eq!(stats.initial_window_bytes, Some(100));

        let mut stats = base.clone();
        stats.focus_stream_file(1);
        assert_eq!(stats.phase, StartupPhase::Ready);
        assert_eq!(stats.initial_window_ready_bytes, Some(60));

        // Out of range: untouched.
        let mut stats = base.clone();
        stats.focus_stream_file(7);
        assert_eq!(stats.phase, StartupPhase::Buffering);
        assert_eq!(stats.initial_window_bytes, None);

        // Not a piece-map phase: untouched even though the file is complete.
        let mut stats = base.clone();
        stats.phase = StartupPhase::Checking;
        stats.focus_stream_file(1);
        assert_eq!(stats.phase, StartupPhase::Checking);
        assert_eq!(stats.initial_window_bytes, None);
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
            serde_json::json!({ "seen": 0, "queued": 0, "connecting": 0, "live": 0 })
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

    #[tokio::test]
    async fn stats_cannot_switch_active_multifile_file() {
        let (enginefs, _counters) = test_enginefs_with_file_count(3);

        enginefs.on_stream_start(TEST_HASH, 1).await;
        insert_active_lease(&enginefs, 1).await;
        let refreshed = enginefs
            .refresh_existing_hls_playback(TEST_HASH, 2, "stats-json")
            .await;

        let snapshot = enginefs.stream_activity_snapshot().await;
        assert!(!refreshed);
        assert_eq!(snapshot.active_multifile_selections.len(), 1);
        assert_eq!(snapshot.active_multifile_selections[0].file_idx, 1);
        assert_eq!(snapshot.active_playback_leases.len(), 1);
        assert_eq!(snapshot.active_playback_leases[0].file_idx, 1);
    }

    #[tokio::test]
    async fn stats_refreshes_current_multifile_file() {
        let (enginefs, _counters) = test_enginefs_with_file_count(3);

        enginefs.on_stream_start(TEST_HASH, 1).await;
        insert_active_lease(&enginefs, 1).await;
        let refreshed = enginefs
            .refresh_existing_hls_playback(TEST_HASH, 1, "stats-json")
            .await;

        let snapshot = enginefs.stream_activity_snapshot().await;
        assert!(refreshed);
        assert_eq!(snapshot.active_multifile_selections.len(), 1);
        assert_eq!(snapshot.active_multifile_selections[0].file_idx, 1);
        assert_eq!(snapshot.active_playback_leases.len(), 1);
        assert_eq!(snapshot.active_playback_leases[0].file_idx, 1);
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn paused_torrent_resumes_for_requested_multifile_file() {
        let (enginefs, counters) = test_enginefs_with_file_count(3);
        {
            let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
            engine.idle_paused.store(true, Ordering::Relaxed);
        }

        enginefs.on_stream_start(TEST_HASH, 2).await;

        let snapshot = enginefs.stream_activity_snapshot().await;
        assert_eq!(snapshot.active_multifile_selections.len(), 1);
        assert_eq!(snapshot.active_multifile_selections[0].file_idx, 2);
        assert_eq!(*counters.last_active_file.lock().unwrap(), Some(2));
        assert!(
            counters.resume_torrent.load(Ordering::SeqCst) > 0,
            "request should resume an idle-paused torrent"
        );
    }

    #[tokio::test]
    async fn idle_pause_skips_active_multifile_selection() {
        let (enginefs, counters) = test_enginefs_with_file_count(3);
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);

        enginefs.on_stream_start(TEST_HASH, 1).await;
        {
            enginefs.active_streams.write().await.clear();
            enginefs.active_file_streams.write().await.clear();
        }
        enginefs
            .schedule_torrent_pause_after(TEST_HASH.to_string(), Duration::from_millis(10))
            .await
            .unwrap();

        assert_eq!(counters.pause_torrent.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn idle_pause_runs_when_no_activity_remains() {
        let (enginefs, counters) = test_enginefs_with_file_count(3);
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);

        enginefs
            .schedule_torrent_pause_after(TEST_HASH.to_string(), Duration::from_millis(10))
            .await
            .unwrap();

        assert_eq!(counters.pause_torrent.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn idle_pause_skips_torrent_awaiting_metadata() {
        let (enginefs, counters) = test_enginefs_with_file_count(0);
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);

        enginefs
            .schedule_torrent_pause_after(TEST_HASH.to_string(), Duration::from_millis(10))
            .await
            .unwrap();

        assert_eq!(counters.pause_torrent.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn single_file_bypasses_multifile_selector() {
        let (enginefs, counters) = test_enginefs();

        enginefs.on_stream_start(TEST_HASH, 0).await;

        let snapshot = enginefs.stream_activity_snapshot().await;
        assert!(snapshot.active_multifile_selections.is_empty());
        assert_eq!(counters.reconcile_file_priorities.load(Ordering::SeqCst), 0);
        assert_eq!(counters.keep_file_downloading.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn active_hls_lease_prevents_delayed_cleanup() {
        let (enginefs, counters) = test_enginefs();

        insert_active_lease(&enginefs, 0).await;
        let cleanup = enginefs
            .schedule_file_cleanup_after(TEST_HASH.to_string(), 0, Duration::from_millis(10))
            .await
            .expect("cleanup task");
        cleanup.await.expect("cleanup task completed");

        assert_eq!(counters.clear_file_streaming.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn expired_hls_lease_allows_delayed_cleanup() {
        let (enginefs, counters) = test_enginefs();

        insert_active_lease(&enginefs, 0).await;
        {
            let mut leases = enginefs.active_playback_leases.write().await;
            leases
                .get_mut(&(TEST_HASH.to_string(), 0))
                .unwrap()
                .expires_at_secs = enginefs.clock.now_secs();
        }
        let cleanup = enginefs
            .schedule_file_cleanup_after(TEST_HASH.to_string(), 0, Duration::from_millis(10))
            .await
            .expect("cleanup task");
        cleanup.await.expect("cleanup task completed");

        assert_eq!(counters.clear_file_streaming.load(Ordering::SeqCst), 1);
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
        let root = std::env::temp_dir().join("enginefs-placement-tests");
        let enginefs = BackendEngineFS::new_with_backend(
            backend,
            HashMap::new(),
            root.join("cache"),
            root.join("downloads"),
        );

        let placement = TorrentPlacement {
            output_folder: Some(root.join("offline").join(TEST_HASH)),
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
            .get_or_add_magnet_placed(
                TEST_HASH,
                None,
                TorrentPlacement {
                    output_folder: Some(root.join("elsewhere")),
                    only_files: None,
                },
            )
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

    // --- downloads dir: placement and relocation of pinned torrents ---

    /// Engine over the fake backend with nothing managed yet: a pin has to
    /// add the torrent, so the placement it uses is observable.
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
        let root = std::env::temp_dir().join("enginefs-downloads-dir-tests");
        let enginefs = BackendEngineFS::new_with_backend(
            FakeBackend::new(vec![handle]),
            HashMap::new(),
            root.join("cache"),
            root.join("downloads"),
        );
        (enginefs, counters)
    }

    /// With a downloads dir, a pin adds an unmanaged torrent straight into
    /// `<dir>/<hash>` wanting only the pinned file -- no relocation needed
    /// afterwards -- and without one the add uses the backend's default
    /// placement plus the want-set.
    #[tokio::test]
    async fn pin_download_places_a_new_torrent_under_the_downloads_dir() {
        let (enginefs, _counters) = test_enginefs_unmanaged();
        let dir = std::path::PathBuf::from("/offline");
        enginefs.set_downloads_dir(Some(dir.clone()));
        assert_eq!(enginefs.downloads_dir(), Some(dir.clone()));
        assert_eq!(
            enginefs.download_folder(&TEST_HASH.to_uppercase()),
            Some(dir.join(TEST_HASH))
        );

        let engine = enginefs.pin_download(TEST_HASH, 1, None).await.unwrap();
        assert_eq!(
            enginefs.backend.placements.lock().unwrap().as_slice(),
            &[TorrentPlacement {
                output_folder: Some(dir.join(TEST_HASH)),
                only_files: Some(vec![1]),
            }]
        );
        assert!(enginefs.backend.relocations.lock().unwrap().is_empty());
        assert_eq!(engine.handle.output_folder(), Some(dir.join(TEST_HASH)));
        assert_eq!(engine.pinned_file_indices(), vec![1]);

        let (enginefs, _counters) = test_enginefs_unmanaged();
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert_eq!(
            enginefs.backend.placements.lock().unwrap().as_slice(),
            &[TorrentPlacement {
                output_folder: None,
                only_files: Some(vec![0]),
            }]
        );
        assert!(enginefs.backend.relocations.lock().unwrap().is_empty());
    }

    /// A torrent managed outside `<dir>/<hash>` (streamed first) is
    /// relocated by the pin: the backend is asked to move it there wanting
    /// its pins plus the new file, the registry's engine is replaced by one
    /// over the backend's new handle with the pins carried, and a later pin
    /// of the same torrent finds it in place. Without a downloads dir, or
    /// when the backend cannot tell where the torrent is, nothing moves.
    #[tokio::test]
    async fn pin_download_relocates_a_torrent_managed_elsewhere() {
        let (enginefs, counters) = test_enginefs_with_file_count(3);
        let dir = std::path::PathBuf::from("/offline");

        // No downloads dir: pinned in place, wherever that is.
        *counters.output_folder.lock().unwrap() = Some("/cache/rqbit-downloads/show".into());
        enginefs.pin_download(TEST_HASH, 2, None).await.unwrap();
        assert!(enginefs.backend.relocations.lock().unwrap().is_empty());

        enginefs.set_downloads_dir(Some(dir.clone()));
        let before = enginefs.get_engine(TEST_HASH).await.unwrap();
        let engine = enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert_eq!(
            enginefs.backend.relocations.lock().unwrap().as_slice(),
            &[(
                TEST_HASH.to_string(),
                TorrentPlacement {
                    output_folder: Some(dir.join(TEST_HASH)),
                    only_files: Some(vec![0, 2]),
                }
            )]
        );
        assert!(
            !Arc::ptr_eq(&before, &engine),
            "the registry holds a new engine over the backend's new handle"
        );
        assert!(Arc::ptr_eq(
            &enginefs.get_engine(TEST_HASH).await.unwrap(),
            &engine
        ));
        assert_eq!(engine.pinned_file_indices(), vec![0, 2]);
        assert_eq!(engine.handle.output_folder(), Some(dir.join(TEST_HASH)));
        assert_eq!(
            engine.get_statistics().await.pinned_files,
            vec![0, 2],
            "the backend's pin set survived the relocation"
        );
        assert!(enginefs.backend.placements.lock().unwrap().is_empty());

        // In place now: another pin relocates nothing.
        enginefs.pin_download(TEST_HASH, 1, None).await.unwrap();
        assert_eq!(enginefs.backend.relocations.lock().unwrap().len(), 1);
        assert_eq!(
            enginefs
                .get_engine(TEST_HASH)
                .await
                .unwrap()
                .pinned_file_indices(),
            vec![0, 1, 2]
        );

        // Unknown whereabouts (a backend without output_folder): no move.
        let (enginefs, _counters) = test_enginefs_with_file_count(2);
        enginefs.set_downloads_dir(Some(dir));
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert!(enginefs.backend.relocations.lock().unwrap().is_empty());
    }

    /// A failed relocation is reported, records no pin, and leaves the
    /// registry consistent with the backend: the engine is rebuilt over
    /// whatever handle the backend still has (pins carried) rather than
    /// kept over a handle to a torrent that may be gone.
    #[tokio::test]
    async fn pin_download_reports_a_failed_relocation_and_rebuilds_the_engine() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        *counters.output_folder.lock().unwrap() = Some("/cache/rqbit-downloads/show".into());
        enginefs.pin_download(TEST_HASH, 1, None).await.unwrap();
        enginefs.set_downloads_dir(Some("/offline".into()));
        enginefs.backend.fail_relocate.store(true, Ordering::SeqCst);

        let before = enginefs.get_engine(TEST_HASH).await.unwrap();
        let err = match enginefs.pin_download(TEST_HASH, 0, None).await {
            Ok(_) => panic!("relocation failure must fail the pin"),
            Err(err) => err,
        };
        assert!(matches!(err, PinDownloadError::Backend(_)), "{err}");
        assert!(err.to_string().contains("relocating"), "{err}");
        let after = enginefs.get_engine(TEST_HASH).await.unwrap();
        assert!(!Arc::ptr_eq(&before, &after));
        assert_eq!(
            after.pinned_file_indices(),
            vec![1],
            "no pin recorded for 0"
        );
        assert_eq!(
            enginefs.pinned_downloads().await,
            vec![PinnedDownload {
                info_hash: TEST_HASH.to_string(),
                file_idx: 1
            }]
        );
    }

    /// Two pins of one torrent issued together (two episodes of a season
    /// pack, the client's re-pin loop) relocate it once: the second waits
    /// for the first, then finds the torrent in place. The pin is recorded
    /// on the engine before the relocation starts and carried to the new
    /// one.
    #[tokio::test]
    async fn concurrent_pins_of_one_torrent_relocate_it_once() {
        let (enginefs, counters) = test_enginefs_with_file_count(3);
        *counters.output_folder.lock().unwrap() = Some("/cache/rqbit-downloads/show".into());
        let dir = std::path::PathBuf::from("/offline");
        enginefs.set_downloads_dir(Some(dir.clone()));
        enginefs.backend.hold_relocate.store(true, Ordering::SeqCst);
        let before = enginefs.get_engine(TEST_HASH).await.unwrap();

        let release = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(
                enginefs.backend.relocations.lock().unwrap().len(),
                1,
                "the second pin waits instead of relocating too"
            );
            assert!(
                enginefs.get_engine(TEST_HASH).await.is_none(),
                "the old engine is out of the lookup path for the move"
            );
            assert!(before.is_pinned(), "pinned before the move");
            assert_eq!(before.pinned_file_indices(), vec![0]);
            enginefs.backend.relocate_hold.add_permits(1);
        };
        let (a, b, ()) = tokio::join!(
            enginefs.pin_download(TEST_HASH, 0, None),
            enginefs.pin_download(TEST_HASH, 1, None),
            release,
        );
        let a = a.expect("first pin");
        let b = b.expect("second pin");
        assert_eq!(enginefs.backend.relocations.lock().unwrap().len(), 1);
        let current = enginefs.get_engine(TEST_HASH).await.unwrap();
        assert!(Arc::ptr_eq(&a, &current));
        assert!(Arc::ptr_eq(&b, &current));
        assert_eq!(current.pinned_file_indices(), vec![0, 1]);
        assert_eq!(current.handle.output_folder(), Some(dir.join(TEST_HASH)));
        assert_eq!(current.get_statistics().await.pinned_files, vec![0, 1]);
        assert!(enginefs.pin_locks.lock().is_empty(), "locks are per call");
    }

    /// When a relocation fails and the torrent is gone from the backend,
    /// only the registry entry the call started from is dropped -- an
    /// engine someone else published for the hash meanwhile stays -- and
    /// the pin that did not go through is not left on either engine.
    #[tokio::test]
    async fn failed_relocation_removes_only_the_engine_it_started_from() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        *counters.output_folder.lock().unwrap() = Some("/cache/rqbit-downloads/show".into());
        enginefs.set_downloads_dir(Some("/offline".into()));
        enginefs.backend.hold_relocate.store(true, Ordering::SeqCst);
        enginefs.backend.fail_relocate.store(true, Ordering::SeqCst);
        enginefs.backend.hide_torrents.store(true, Ordering::SeqCst);
        let started_from = enginefs.get_engine(TEST_HASH).await.unwrap();

        let other = Arc::new(Engine::new_with_handle(
            started_from.handle.clone(),
            TEST_HASH,
            enginefs.clock,
        ));
        let swap = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(started_from.is_pinned());
            enginefs
                .engines
                .write()
                .await
                .insert(TEST_HASH.to_string(), other.clone());
            enginefs.backend.relocate_hold.add_permits(1);
        };
        let (result, ()) = tokio::join!(enginefs.pin_download(TEST_HASH, 0, None), swap);
        assert!(matches!(result, Err(PinDownloadError::Backend(_))));
        let current = enginefs
            .get_engine(TEST_HASH)
            .await
            .expect("the other engine is not removed");
        assert!(Arc::ptr_eq(&current, &other));
        assert!(!started_from.is_pinned(), "failed pin undone");
        assert!(!other.is_pinned());

        // Nobody else in the way: the stale entry itself is dropped.
        enginefs
            .backend
            .hold_relocate
            .store(false, Ordering::SeqCst);
        assert!(
            enginefs.pin_download(TEST_HASH, 1, None).await.is_err(),
            "relocation still fails"
        );
        assert!(enginefs.get_engine(TEST_HASH).await.is_none());
    }

    /// While a torrent is being moved it has no live handle (the backend
    /// dropped it before moving the files), so the hash is looked up as an
    /// in-flight add: `get_engine` finds nothing, the non-blocking lookup
    /// reports `Adding` (no second add is started), and a blocking lookup
    /// waits and gets the relocated engine -- never the dropped one. The
    /// entry is gone once the new engine is published, and a relocation
    /// outlasting the idle window is not swept as an idle add.
    #[tokio::test(start_paused = true)]
    async fn requests_during_a_relocation_wait_for_the_new_engine() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        *counters.output_folder.lock().unwrap() = Some("/cache/rqbit-downloads/show".into());
        let dir = std::path::PathBuf::from("/offline");
        enginefs.set_downloads_dir(Some(dir.clone()));
        enginefs.backend.hold_relocate.store(true, Ordering::SeqCst);
        let old = enginefs.get_engine(TEST_HASH).await.unwrap();

        let observe = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(enginefs.get_engine(TEST_HASH).await.is_none());
            let EngineLookup::Adding(pending) =
                enginefs.get_or_begin_add_magnet(TEST_HASH, None).await
            else {
                panic!("a relocating torrent looks like an in-flight add");
            };
            assert!(pending.abort.is_none(), "nothing to abort");
            assert!(enginefs.pending_magnet_add(TEST_HASH).await.is_some());
            assert!(
                enginefs.backend.placements.lock().unwrap().is_empty(),
                "no second add for the hash"
            );
            // Nobody polls for a full idle window: the entry stays.
            tokio::time::sleep(INACTIVE_TORRENT_REMOVE_TIMEOUT * 2).await;
            assert!(
                enginefs.pending_magnet_add(TEST_HASH).await.is_some(),
                "a relocation is not swept as an idle add"
            );
            enginefs.backend.relocate_hold.add_permits(1);
            pending.done.await
        };
        let waiter = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            enginefs.get_or_add_magnet(TEST_HASH, None).await
        };
        let (pinned, observed, waited) =
            tokio::join!(enginefs.pin_download(TEST_HASH, 0, None), observe, waiter);
        let pinned = pinned.expect("pin");
        let observed = observed.expect("the pending add resolves to the new engine");
        let waited = waited.expect("the blocking lookup resolves to the new engine");
        assert!(!Arc::ptr_eq(&pinned, &old));
        assert!(Arc::ptr_eq(&observed, &pinned));
        assert!(Arc::ptr_eq(&waited, &pinned));
        assert_eq!(pinned.handle.output_folder(), Some(dir.join(TEST_HASH)));
        assert!(Arc::ptr_eq(
            &enginefs.get_engine(TEST_HASH).await.unwrap(),
            &pinned
        ));
        assert!(enginefs.pending_magnet_add(TEST_HASH).await.is_none());
        assert!(enginefs.failed_magnet_add(TEST_HASH).await.is_none());
        assert!(enginefs.magnet_adds.read().await.is_empty());
    }

    /// A failed relocation settles the waiters like the registry does: with
    /// the rebuilt engine when the backend still manages the torrent, with
    /// an error -- and no lingering entry or failure record, so the next
    /// request re-adds -- when it does not.
    #[tokio::test]
    async fn failed_relocation_settles_the_waiters() {
        async fn waited_pin(
            enginefs: &BackendEngineFS<FakeBackend>,
        ) -> Result<Arc<Engine<FakeHandle>>, MagnetAddError> {
            let release = async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                assert!(enginefs.pending_magnet_add(TEST_HASH).await.is_some());
                enginefs.backend.relocate_hold.add_permits(1);
            };
            let waiter = async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                enginefs.get_or_add_magnet(TEST_HASH, None).await
            };
            let (pinned, (), waited) =
                tokio::join!(enginefs.pin_download(TEST_HASH, 0, None), release, waiter);
            assert!(matches!(pinned, Err(PinDownloadError::Backend(_))));
            waited
        }

        // Still managed: the waiter gets the rebuilt engine.
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        *counters.output_folder.lock().unwrap() = Some("/cache/rqbit-downloads/show".into());
        enginefs.set_downloads_dir(Some("/offline".into()));
        enginefs.backend.hold_relocate.store(true, Ordering::SeqCst);
        enginefs.backend.fail_relocate.store(true, Ordering::SeqCst);
        let old = enginefs.get_engine(TEST_HASH).await.unwrap();
        let waited = waited_pin(&enginefs).await.expect("rebuilt engine");
        assert!(!Arc::ptr_eq(&waited, &old));
        assert!(Arc::ptr_eq(
            &waited,
            &enginefs.get_engine(TEST_HASH).await.unwrap()
        ));
        assert!(enginefs.magnet_adds.read().await.is_empty());

        // Gone from the backend: the waiter gets an error and the hash is
        // free for a fresh add.
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        *counters.output_folder.lock().unwrap() = Some("/cache/rqbit-downloads/show".into());
        enginefs.set_downloads_dir(Some("/offline".into()));
        enginefs.backend.hold_relocate.store(true, Ordering::SeqCst);
        enginefs.backend.fail_relocate.store(true, Ordering::SeqCst);
        enginefs.backend.hide_torrents.store(true, Ordering::SeqCst);
        let waited = waited_pin(&enginefs).await;
        assert!(
            matches!(waited, Err(MagnetAddError::Backend { .. })),
            "{:?}",
            waited.as_ref().err()
        );
        assert!(enginefs.get_engine(TEST_HASH).await.is_none());
        assert!(enginefs.magnet_adds.read().await.is_empty());
        assert!(matches!(
            enginefs.get_or_begin_add_magnet(TEST_HASH, None).await,
            EngineLookup::Adding(_)
        ));
    }

    /// The cleaner's protected paths are where the files really are: the
    /// backend's `file_path` (its output folder -- `<root>/<torrent name>`
    /// for a multi-file torrent in the cache root, `<downloadsDir>/<hash>`
    /// for a placed one), not `<root>/<relative name>`, which for a
    /// multi-file torrent names a file that does not exist while the real
    /// one goes unprotected.
    #[tokio::test]
    async fn protected_paths_follow_the_backend_output_folder() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        let root = enginefs.download_dir.clone();

        // Backend without a folder or path: the historical root join.
        assert_eq!(
            enginefs.protected_paths().await,
            vec![root.join("video-0.mkv"), root.join("video-1.mkv")]
        );

        let show = root.join("show");
        *counters.output_folder.lock().unwrap() = Some(show.clone());
        assert_eq!(
            enginefs.protected_paths().await,
            vec![show.join("video-0.mkv"), show.join("video-1.mkv")]
        );

        let placed = std::path::PathBuf::from("/offline").join(TEST_HASH);
        *counters.output_folder.lock().unwrap() = Some(placed.clone());
        assert_eq!(
            enginefs.protected_paths().await,
            vec![placed.join("video-0.mkv"), placed.join("video-1.mkv")]
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
    /// the margin, a torrent the pin placed under the downloads dir is
    /// dropped again, a complete file needs no space, and a volume that
    /// cannot be probed does not block the pin.
    #[tokio::test]
    async fn pin_download_refuses_without_the_free_space_margin() {
        let (mut enginefs, _counters) = test_enginefs_unmanaged();
        // Fake files are 100 bytes, half downloaded: 50 remain.
        let available = Arc::new(AtomicU64::new(PIN_FREE_SPACE_MARGIN + 49));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));
        enginefs.set_downloads_dir(Some("/offline".into()));

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
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .as_slice(),
            &[TEST_HASH.to_string()],
            "the torrent added under the downloads dir for the refused pin is dropped with its placeholder"
        );
        assert!(enginefs.backend.removed.lock().unwrap().is_empty());
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
        assert_eq!(enginefs.backend.removed_with_files.lock().unwrap().len(), 1);
        assert!(enginefs.backend.removed.lock().unwrap().is_empty());

        // No downloads dir: the torrent added into the cache root is an
        // ordinary streamed one -- left to the idle sweeper, since it
        // cannot be told from one a stream request started.
        let (mut enginefs, _counters) = test_enginefs_unmanaged();
        enginefs.set_free_space_probe(|_| Ok(0));
        assert!(matches!(
            enginefs.pin_download(TEST_HASH, 0, None).await,
            Err(PinDownloadError::InsufficientSpace { .. })
        ));
        assert!(enginefs.get_engine(TEST_HASH).await.is_some());
        assert!(enginefs.backend.removed.lock().unwrap().is_empty());
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

    /// A pin that relocates the torrent onto another volume has to fit what
    /// the move copies -- the pinned file in full plus every other file with
    /// verified data (placeholders stay behind) -- not just the pinned
    /// file's missing bytes; on one volume the move is a rename and only the
    /// missing bytes count. Volumes that cannot be told apart are sized as
    /// a copy.
    #[tokio::test]
    async fn relocation_across_volumes_needs_room_for_what_it_copies() {
        // Three 100-byte files, half downloaded each: every file has data,
        // so a cross-volume move copies all 300 bytes.
        let cross_volume = |enginefs: &mut BackendEngineFS<FakeBackend>| {
            enginefs
                .set_volume_id_probe(|path| Ok(if path.starts_with("/offline") { 2 } else { 1 }));
        };
        let (mut enginefs, counters) = test_enginefs_with_file_count(3);
        *counters.output_folder.lock().unwrap() = Some("/cache/rqbit-downloads/show".into());
        enginefs.set_downloads_dir(Some("/offline".into()));
        cross_volume(&mut enginefs);
        let available = Arc::new(AtomicU64::new(PIN_FREE_SPACE_MARGIN + 299));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));

        match enginefs.pin_download(TEST_HASH, 0, None).await {
            Err(PinDownloadError::InsufficientSpace { required, .. }) => {
                assert_eq!(required, PIN_FREE_SPACE_MARGIN + 300);
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("must refuse"),
        }
        assert!(enginefs.backend.relocations.lock().unwrap().is_empty());
        assert!(
            enginefs.get_engine(TEST_HASH).await.is_some(),
            "the streamed torrent stays"
        );
        available.store(PIN_FREE_SPACE_MARGIN + 300, Ordering::SeqCst);
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert_eq!(enginefs.backend.relocations.lock().unwrap().len(), 1);

        // Same volume: a rename, only the pinned file's 50 missing bytes.
        let (mut enginefs, counters) = test_enginefs_with_file_count(3);
        *counters.output_folder.lock().unwrap() = Some("/cache/rqbit-downloads/show".into());
        enginefs.set_downloads_dir(Some("/offline".into()));
        enginefs.set_volume_id_probe(|_| Ok(7));
        enginefs.set_free_space_probe(|_| Ok(PIN_FREE_SPACE_MARGIN + 50));
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert_eq!(enginefs.backend.relocations.lock().unwrap().len(), 1);

        // Unknown volumes: sized as a copy.
        let (mut enginefs, counters) = test_enginefs_with_file_count(3);
        *counters.output_folder.lock().unwrap() = Some("/cache/rqbit-downloads/show".into());
        enginefs.set_downloads_dir(Some("/offline".into()));
        enginefs.set_volume_id_probe(|_| Err(std::io::Error::other("no stat")));
        enginefs.set_free_space_probe(|_| Ok(PIN_FREE_SPACE_MARGIN + 299));
        assert!(matches!(
            enginefs.pin_download(TEST_HASH, 0, None).await,
            Err(PinDownloadError::InsufficientSpace { .. })
        ));

        // Not relocating (no downloads dir): missing bytes only, whatever
        // the volumes.
        let (mut enginefs, counters) = test_enginefs_with_file_count(3);
        *counters.output_folder.lock().unwrap() = Some("/cache/rqbit-downloads/show".into());
        cross_volume(&mut enginefs);
        enginefs.set_free_space_probe(|_| Ok(PIN_FREE_SPACE_MARGIN + 50));
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
    }

    /// Re-pinning a file of a torrent that is still checking its data in
    /// place (a restart, a relocation) is not refused for space: its
    /// `downloaded` reads 0 until the check ends, and the download is
    /// already librqbit's to continue. A torrent added by the pin itself is
    /// measured even while initializing (nothing of it is on disk yet), and
    /// so is a checking torrent the pin would relocate.
    #[tokio::test]
    async fn re_pin_of_a_checking_torrent_in_place_skips_the_space_check() {
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

        // Would relocate: measured (and refused -- everything would move).
        let (mut enginefs, counters, _init) =
            test_enginefs_initializing(2, Duration::from_secs(60));
        *counters.output_folder.lock().unwrap() = Some("/cache/rqbit-downloads/show".into());
        enginefs.set_downloads_dir(Some("/offline".into()));
        enginefs.set_free_space_probe(|_| Ok(0));
        assert!(matches!(
            enginefs.pin_download(TEST_HASH, 1, None).await,
            Err(PinDownloadError::InsufficientSpace { .. })
        ));

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
        let root = std::env::temp_dir().join("enginefs-downloads-dir-tests");
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

    /// A relocation issued while the torrent is still checking (a pin right
    /// after a restart) cannot read `downloaded`, which is 0 for every file
    /// until the check ends; it is sized from what the files have allocated
    /// on disk instead: a complete file moved within one volume needs
    /// nothing, an absent one its length, and a cross-volume copy counts
    /// every file with data plus the pinned one in full.
    #[cfg(unix)]
    #[tokio::test]
    async fn relocation_during_a_check_is_sized_from_the_allocated_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let show = tmp.path().join("show");
        std::fs::create_dir_all(&show).unwrap();
        std::fs::write(show.join("video-0.mkv"), [1u8; 100]).unwrap();
        let offline = tmp.path().join("offline");
        let checking_in = |folder: &std::path::Path| {
            let (enginefs, counters, _init) =
                test_enginefs_initializing(2, Duration::from_secs(60));
            *counters.output_folder.lock().unwrap() = Some(folder.to_path_buf());
            enginefs.set_downloads_dir(Some(offline.clone()));
            enginefs
        };

        // Same volume, file 0 all there: nothing to write.
        let mut enginefs = checking_in(&show);
        enginefs.set_volume_id_probe(|_| Ok(7));
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert_eq!(enginefs.backend.relocations.lock().unwrap().len(), 1);

        // Same volume, file 1 absent: its whole length.
        let mut enginefs = checking_in(&show);
        enginefs.set_volume_id_probe(|_| Ok(7));
        enginefs.set_free_space_probe(|_| Ok(PIN_FREE_SPACE_MARGIN + 99));
        match enginefs.pin_download(TEST_HASH, 1, None).await {
            Err(PinDownloadError::InsufficientSpace { required, .. }) => {
                assert_eq!(required, PIN_FREE_SPACE_MARGIN + 100);
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("must refuse"),
        }

        // Across volumes: file 0 (data) is copied, file 1 (pinned) written.
        let cross_volume = |enginefs: &mut BackendEngineFS<FakeBackend>| {
            let offline = offline.clone();
            enginefs.set_volume_id_probe(move |path| {
                Ok(if path.starts_with(&offline) { 2 } else { 1 })
            });
        };
        let mut enginefs = checking_in(&show);
        cross_volume(&mut enginefs);
        enginefs.set_free_space_probe(|_| Ok(PIN_FREE_SPACE_MARGIN + 199));
        match enginefs.pin_download(TEST_HASH, 1, None).await {
            Err(PinDownloadError::InsufficientSpace { required, .. }) => {
                assert_eq!(required, PIN_FREE_SPACE_MARGIN + 200);
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("must refuse"),
        }
        let mut enginefs = checking_in(&show);
        cross_volume(&mut enginefs);
        enginefs.set_free_space_probe(|_| Ok(PIN_FREE_SPACE_MARGIN + 200));
        enginefs.pin_download(TEST_HASH, 1, None).await.unwrap();
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
        let root = std::env::temp_dir().join("enginefs-downloads-dir-tests");
        let enginefs = BackendEngineFS::new_with_backend(
            FakeBackend::new(vec![handle]),
            HashMap::new(),
            root.join("cache"),
            root.join("downloads"),
        );
        (enginefs, counters)
    }

    /// A fresh pin into a `<dir>/<hash>` folder that already exists (the
    /// data of an earlier session whose backend records are gone) is not
    /// measured while the torrent is still checking that data -- its
    /// `downloaded` reads 0 -- and, refused, drops the torrent but never
    /// the folder's files. Only a folder the pin itself created goes with
    /// a refused pin.
    #[tokio::test]
    async fn fresh_pin_into_an_existing_folder_keeps_its_data() {
        let tmp = tempfile::tempdir().unwrap();
        let folder = tmp.path().join(TEST_HASH);
        std::fs::create_dir_all(&folder).unwrap();
        let data = folder.join("video-0.mkv");
        std::fs::write(&data, [7u8; 100]).unwrap();

        // Still checking, disk reports no room: accepted, unmeasured.
        let (mut enginefs, _counters) = test_enginefs_unmanaged_checking();
        enginefs.set_downloads_dir(Some(tmp.path().to_path_buf()));
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .is_empty()
        );
        assert!(data.is_file());

        // Refused (no such file): the torrent goes, the folder stays.
        let (mut enginefs, _counters) = test_enginefs_unmanaged_checking();
        enginefs.set_downloads_dir(Some(tmp.path().to_path_buf()));
        enginefs.set_free_space_probe(|_| Ok(0));
        assert!(matches!(
            enginefs.pin_download(TEST_HASH, 5, None).await,
            Err(PinDownloadError::FileNotFound { .. })
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
        assert!(data.is_file(), "pre-existing data survives a refused pin");

        // No folder yet: nothing is on disk, so the checking torrent is
        // measured, refused, and dropped with the placeholder it made.
        let (mut enginefs, _counters) = test_enginefs_unmanaged_checking();
        enginefs.set_downloads_dir(Some(tmp.path().join("elsewhere")));
        enginefs.set_free_space_probe(|_| Ok(0));
        assert!(matches!(
            enginefs.pin_download(TEST_HASH, 0, None).await,
            Err(PinDownloadError::InsufficientSpace { .. })
        ));
        assert_eq!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .as_slice(),
            &[TEST_HASH.to_string()]
        );
        assert!(enginefs.backend.removed.lock().unwrap().is_empty());
    }

    /// A pin that joins a magnet add another request started (a stream's
    /// stats poll resolving metadata) and is then refused leaves that
    /// engine alone: the torrent is theirs, in the cache root, and dropping
    /// it would fail the stream about to open on it.
    #[tokio::test]
    async fn refused_pin_leaves_a_joined_stream_add_alone() {
        let (mut enginefs, _counters) = test_enginefs_unmanaged();
        enginefs.set_downloads_dir(Some("/offline".into()));
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.backend.hold_add.store(true, Ordering::SeqCst);
        assert!(matches!(
            enginefs.get_or_begin_add_magnet(TEST_HASH, None).await,
            EngineLookup::Adding(_)
        ));

        let release = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
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
        assert_eq!(engine.handle.output_folder(), None, "in the cache root");
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

        async fn relocate_torrent(
            &self,
            info_hash: &str,
            placement: TorrentPlacement,
            trackers: Vec<String>,
        ) -> Result<Self::Handle> {
            self.inner
                .relocate_torrent(info_hash, placement, trackers)
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

    /// The cache-purge case against the real backend: the session's
    /// records are gone but `<downloadsDir>/<hash>/e1.bin` is complete. A
    /// fresh pin of that file is accepted although the volume reports no
    /// free space (librqbit verifies the data in place; while it does, the
    /// file is not counted as missing), and a pin of the torrent's other
    /// file -- refused once the check shows it missing, or accepted
    /// unmeasured while the check runs -- never takes the folder's data
    /// with it: the folder was not this pin's to empty.
    #[tokio::test]
    async fn fresh_pin_over_pre_seeded_data_survives_with_the_real_backend() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("show");
        std::fs::create_dir_all(&src).unwrap();
        let payload: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
        std::fs::write(src.join("e1.bin"), &payload).unwrap();
        std::fs::write(src.join("e2.bin"), vec![3u8; 16 * 1024]).unwrap();
        let (bytes, hash) = real_torrent(&src).await;
        let offline = tmp.path().join("offline");
        let folder = offline.join(&hash);
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
            enginefs.set_downloads_dir(Some(offline.clone()));
            enginefs.set_free_space_probe(|_| Ok(0));
            enginefs
        };

        let enginefs = make().await;
        let engine = enginefs
            .pin_download(&hash, 0, None)
            .await
            .expect("a complete file in place needs no space");
        assert_eq!(engine.handle.output_folder(), Some(folder.clone()));
        engine.handle.handle.wait_until_initialized().await.unwrap();
        let stats = engine.get_statistics().await;
        assert!(stats.files[0].complete, "verified in place: {stats:?}");
        assert_eq!(engine.pinned_file_indices(), vec![0]);
        enginefs.backend.remove_torrent(&hash).await.unwrap();
        drop(enginefs);

        let enginefs = make().await;
        match enginefs.pin_download(&hash, 1, None).await {
            Ok(engine) => {
                // Still checking when measured: accepted unmeasured, and
                // the data in place is what the check finds.
                engine.handle.handle.wait_until_initialized().await.unwrap();
                assert!(engine.get_statistics().await.files[0].complete);
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

    // --- pin persistence across restarts ---

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
        assert!(first.unpin_download(TEST_HASH, 2, false).await.unwrap());
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
        assert!(second.unpin_download(TEST_HASH, 1, false).await.unwrap());
        assert!(!second.unpin_download(TEST_HASH, 1, false).await.unwrap());
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
        assert!(enginefs.unpin_download(TEST_HASH, 1, false).await.unwrap());
        assert_eq!(counters.unpin_file.load(Ordering::SeqCst), 1);
        assert_eq!(engine.pinned_file_indices(), vec![0]);
        assert_eq!(
            counters.reconcile_file_priorities.load(Ordering::SeqCst),
            before + 2
        );
        assert_eq!(*counters.last_active_file.lock().unwrap(), Some(2));
        assert!(!engine.get_statistics().await.files[1].pinned);
        // Not pinned (any more) / unknown torrent: false, no reconcile.
        assert!(!enginefs.unpin_download(TEST_HASH, 1, false).await.unwrap());
        assert!(
            !enginefs
                .unpin_download(&"f".repeat(40), 0, false)
                .await
                .unwrap()
        );
        assert_eq!(
            counters.reconcile_file_priorities.load(Ordering::SeqCst),
            before + 2
        );
        assert!(enginefs.unpin_download(TEST_HASH, 0, false).await.unwrap());
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
        assert!(enginefs.unpin_download(TEST_HASH, 1, true).await.unwrap());
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
        assert!(enginefs.unpin_download(TEST_HASH, 2, true).await.unwrap());
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

    /// A delete of a file nothing pinned (a pin lost to a crash, or a plain
    /// "remove this download" for a torrent that was only streamed) still
    /// drops the torrent and its data, and reports `false`: no pin cleared.
    #[tokio::test]
    async fn unpin_download_deletes_an_unpinned_file_too() {
        let (enginefs, _counters) = test_enginefs_with_file_count(2);
        assert!(!enginefs.unpin_download(TEST_HASH, 0, true).await.unwrap());
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

        let unpin = async {
            // Long enough for the pin to be inside the held add.
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(enginefs.get_engine(TEST_HASH).await.is_none());
            enginefs.unpin_download(TEST_HASH, 0, true).await.unwrap()
        };
        let release = async {
            tokio::time::sleep(Duration::from_millis(150)).await;
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

    /// A dormant pin (no torrent in the backend) has nothing on disk this
    /// layer can name: the pin is dropped, nothing is deleted, nothing
    /// fails.
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

        assert!(enginefs.unpin_download(TEST_HASH, 1, true).await.unwrap());
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

    /// A torrent the seeding-disabled policy had paused must download again
    /// once one of its files is pinned.
    #[tokio::test]
    async fn pin_download_resumes_idle_paused_torrent() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        engine.idle_paused.store(true, Ordering::Relaxed);

        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        assert!(counters.resume_torrent.load(Ordering::SeqCst) > 0);
        assert!(!engine.idle_paused.load(Ordering::Relaxed));
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
        assert!(enginefs.unpin_download(TEST_HASH, 1, false).await.unwrap());
        tokio::time::sleep(INACTIVE_TORRENT_REMOVE_TIMEOUT + Duration::from_secs(30)).await;
        assert!(!present(TEST_HASH).await, "unpinned engine is removed");
        assert_eq!(
            *removed.lock().unwrap(),
            vec![OTHER_HASH.to_string(), TEST_HASH.to_string()]
        );
    }

    /// With seeding disabled the periodic loop pauses idle torrents; a
    /// pinned one must keep downloading.
    #[tokio::test(start_paused = true)]
    async fn seeding_disabled_loop_skips_pinned_engine() {
        let TwoEngines {
            enginefs,
            counters: [pinned, unpinned],
            ..
        } = test_enginefs_with_two_engines();
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();

        tokio::time::sleep(Duration::from_secs(20)).await;
        assert_eq!(unpinned.pause_torrent.load(Ordering::SeqCst), 1);
        assert_eq!(pinned.pause_torrent.load(Ordering::SeqCst), 0);
        assert!(
            !enginefs
                .get_engine(TEST_HASH)
                .await
                .unwrap()
                .idle_paused
                .load(Ordering::Relaxed)
        );
    }

    /// The post-stream grace-period pause skips a pinned torrent too.
    #[tokio::test]
    async fn idle_pause_after_stream_skips_pinned_torrent() {
        let (enginefs, counters) = test_enginefs_with_file_count(3);
        enginefs.seeding_enabled.store(false, Ordering::Relaxed);
        enginefs.pin_download(TEST_HASH, 2, None).await.unwrap();

        enginefs
            .schedule_torrent_pause_after(TEST_HASH.to_string(), Duration::from_millis(10))
            .await
            .unwrap();

        assert_eq!(counters.pause_torrent.load(Ordering::SeqCst), 0);
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
            wait_until(Duration::from_secs(1), || gated.adds() == 1).await,
            "backend add started once"
        );

        gated.release.notify_one();
        let engine = first.done.await.expect("add succeeds");
        let joined = second.done.await.expect("shared add succeeds");
        assert!(Arc::ptr_eq(&engine, &joined));
        assert_eq!(engine.info_hash, TEST_HASH);
        assert!(enginefs.get_engine(TEST_HASH).await.is_some());
        // The task clears its pending entry right after publishing the engine.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
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
        old.abort.as_ref().expect("a spawned add").abort();
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
        let json = serde_json::to_value(&stats).unwrap();
        assert_eq!(json["phase"], "resolvingMetadata");
        assert_eq!(json["hasMetadata"], false);
        assert_eq!(json["streamLen"], 0);
    }
}
