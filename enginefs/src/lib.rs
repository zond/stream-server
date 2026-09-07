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
pub mod disk_cache;
pub mod engine;
pub mod files;
pub mod http_client;
pub mod metadata_cache;
pub mod metadata_pins;
pub mod piece_cache;
pub mod piece_store;
pub mod piece_waiter;
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
    BackendMemoryDiagnostics, Footprint, HotFilePriorityPlan, TorrentBackend,
    TorrentFilePriorityPlan, TorrentHandle, TorrentListenPort, TorrentPlacement, TorrentSource,
};

const INACTIVE_TORRENT_REMOVE_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes
/// Free space the cache is kept out of on the volume the torrents write to.
///
/// One number, three readers, and it is one number so they cannot drift:
/// the server's stream route refuses to start a stream to disk with less
/// than this free; its cache cleaner evicts the cache back to it; and the
/// engine's free-space watch ([`BackendEngineFS::free_space_watch_tick`])
/// stops a torrent that is writing when the volume falls under it. The
/// third is what makes the other two hold. librqbit's storage writes the
/// whole file it wants and stops only at ENOSPC, which it treats as a fatal
/// torrent error -- so without the watch a torrent larger than the free
/// space ran the volume to zero between two cleaner passes (40 s at full
/// speed on the television that prompted this), and with the volume at
/// zero every other stream and the OS around them failed too. The watch
/// checks every [`FREE_SPACE_WATCH_INTERVAL`], so a torrent can overshoot
/// the floor by that long of writing; the floor is sized to absorb it.
///
/// Offline downloads are the fourth writer and keep their own margin,
/// [`PIN_FREE_SPACE_MARGIN`], checked once when a pin is accepted; a pin
/// can therefore settle the volume under this line by design, and the
/// watch stops it there like anything else.
pub const CACHE_FREE_SPACE_FLOOR: u64 = 512 * 1024 * 1024;
/// How often the free-space watch reads the volume. One `statvfs` per
/// distinct output folder per tick -- microseconds -- so it can afford to
/// be short, and it has to be: a torrent at 20 MB/s writes 40 MB per tick
/// past the floor before the watch sees it.
pub const FREE_SPACE_WATCH_INTERVAL: Duration = Duration::from_secs(2);
/// A torrent the watch stopped is started again by the watch only once the
/// volume has this much *over* the floor -- or by the cache cleaner the
/// moment it has made room, whatever the margin. Without the hysteresis a
/// torrent resumed at the floor writes a few MB, is stopped again, and
/// flaps: each stop drops its peers and each start re-announces.
pub const FREE_SPACE_RESUME_MARGIN: u64 = 64 * 1024 * 1024;
/// How long a torrent may stay stopped for space with its readers parked
/// before the watch fails them (`Engine::refuse_reads_for_space`). The
/// cache cleaner normally settles it well inside this -- the stop notifies
/// it, and its pass either makes room and restarts the torrent or evicts
/// it -- so a reader sees a buffering blip, not a failure. This is the
/// bound for a server whose cleaner is off or stuck: a parked read that
/// nothing will complete is a player spinning for ever.
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

/// Both ends of every relocation in flight, keyed by info hash. Shared with
/// the detached half of the move, which is what removes its entry -- see
/// [`BackendEngineFS::relocate_engine`].
type RelocationRegistry = Arc<parking_lot::Mutex<HashMap<String, Vec<std::path::PathBuf>>>>;

/// The guard on one info hash's pin lock, owned rather than borrowed so it
/// can be handed to a task that outlives the request that took it -- see
/// [`BackendEngineFS::pin_download`] and [`BackendEngineFS::relocate_engine`].
type PinGuard = tokio::sync::OwnedMutexGuard<()>;

/// How the backend's half of a relocation ended, for the half that settles
/// it ([`BackendEngineFS::relocate_engine`]).
enum Relocated<H> {
    /// The files are in their new home; this is the handle to them.
    Moved(H),
    /// The move failed. `still_managed` is the backend's handle for the
    /// torrent if it kept it (its recovery usually leaves it where it was),
    /// asked for in the same task rather than by the supervisor: the
    /// supervisor is what settles this task's panic, which it can only do
    /// while it is not the one making the calls that panic.
    Failed {
        error: anyhow::Error,
        still_managed: Option<H>,
    },
}

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
    /// `None` for an entry that stands for a relocation in progress
    /// (`BackendEngineFS::relocate_engine`): it ends with the move, not
    /// with its pollers, so the sweep leaves it alone.
    abort: Option<AbortHandle>,
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
                joiners: Arc::new(AtomicUsize::new(0)),
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
    /// Relocations in flight, keyed by info hash: every path the move reads
    /// from or writes to, held only for the length of the move. See
    /// [`Self::begin_relocation`] -- the engine leaves the registry before
    /// the backend touches a file, so this is the only thing naming either
    /// end of the copy while it runs.
    relocations: RelocationRegistry,
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
    /// The housekeeping sweep started by the constructor, kept so its owner
    /// can cancel it. See [`Self::take_sweep_task`].
    sweep_task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Rung once per free-space watch tick that stopped a torrent, for the
    /// cache cleaner to run a pass at once rather than on its next poll --
    /// see [`Self::out_of_space_signal`].
    out_of_space_notify: Arc<tokio::sync::Notify>,
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

impl StreamActivitySnapshot {
    /// Whether a player is reading from this server right now.
    ///
    /// Three of the fields here are live and the rest are sticky, and telling
    /// them apart is the whole of the answer:
    ///
    /// * `engine_active_streams` counts open file readers -- up when one is
    ///   handed out, down when the [`crate::files::FileHandle`] is dropped.
    /// * `active_streams` counts the stream responses on top of them
    ///   (`on_stream_start`/`on_stream_end`), which is also what an HLS
    ///   client's segment requests move.
    /// * `active_playback_leases` is already filtered to unexpired leases:
    ///   how a client that reads in short bursts says it is still there
    ///   between them.
    ///
    /// `active_file` and `active_multifile_selections` are the ones to leave
    /// out. They name the file most recently chosen, and they deliberately
    /// outlive the stream, because the want-set is planned from them -- a
    /// light driven by either would come on with the first playback of the
    /// session and never go out again.
    pub fn playback_is_live(&self) -> bool {
        self.engine_active_streams > 0
            || self.active_streams.values().any(|count| *count > 0)
            || !self.active_playback_leases.is_empty()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EngineDiagnosticsSnapshot {
    pub uptime_secs: u64,
    pub streams: StreamActivitySnapshot,
    pub memory: BackendMemoryDiagnostics,
}

/// What the engines tell the cache cleaner about the files they own --
/// `BackendEngineFS::eviction_classes`.
///
/// Every path is one the cleaner may meet on its walk; a path in no class
/// is ordinary cache, evicted by age. The classes are disjoint by
/// construction: an engine's files land in exactly one of them.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EvictionClasses {
    /// May not be evicted: every live engine's files, the placement folder
    /// of every dormant pin, both ends of every relocation in flight -- what
    /// `protected_paths` has always returned.
    pub protected: Vec<std::path::PathBuf>,
    /// Should go before anything else: the files of an unpinned torrent the
    /// backend stopped with an error that is *not* a want of space. Nothing
    /// will restart it, so nothing will ever read these bytes again, and on
    /// a full device they are exactly what keeps the next stream from
    /// starting. Still walked, counted and deleted by the cleaner like any
    /// other file -- the backend's error state holds no open handle on them
    /// -- only sorted to the front of the eviction order.
    pub dead: Vec<std::path::PathBuf>,
    /// Unpinned torrents stopped for want of disk space -- by the free-space
    /// watch, or by librqbit's ENOSPC -- with every path of theirs. Not
    /// protected, but not for the cleaner to unlink either: a paused
    /// torrent holds its files open and its piece map says it has them, so
    /// deleting the bytes alone would leave it resuming over nothing. The
    /// cleaner takes one of these whole, through
    /// `BackendEngineFS::evict_stopped_torrent`, and only when nothing else
    /// can go: while anything else can, evicting *that* lets the stopped
    /// torrent resume with its progress, which is the better outcome for
    /// the person watching it.
    pub stopped_for_space: Vec<StoppedTorrent>,
}

/// A torrent stopped for want of disk space, as [`EvictionClasses`] lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoppedTorrent {
    pub info_hash: String,
    /// Every path its data can be at -- the same set a live torrent would
    /// have protected.
    pub paths: Vec<std::path::PathBuf>,
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
            relocations: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            dormant_pins: parking_lot::Mutex::new(BTreeMap::new()),
            free_space_probe: Arc::new(|path| fs4::available_space(path)),
            volume_id_probe: Arc::new(volume_id),
            clock,
            sweep_task: parking_lot::Mutex::new(None),
            out_of_space_notify: Arc::new(tokio::sync::Notify::new()),
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
        let sweep = tokio::spawn(async move {
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

                        // Already stopped, and by an owner with a better
                        // claim: the free-space watch lifts its own stop
                        // when the volume recovers, and the backend refuses
                        // to pause a torrent twice, so pausing here would
                        // only log a failure every sweep.
                        if engine.is_stopped_for_space() {
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
        *efs.sweep_task.lock() = Some(sweep);

        efs
    }

    /// Take the housekeeping sweep this constructor started -- the loop above
    /// that expires playback leases, prunes the magnet registry and pauses or
    /// removes idle torrents -- for the caller to abort when it shuts down.
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

    /// Start the free-space watch: [`Self::free_space_watch_tick`] every
    /// [`FREE_SPACE_WATCH_INTERVAL`] for as long as this engine exists. The
    /// caller owns the task -- `server::run` puts it with the other forever
    /// loops it aborts on shutdown -- and the task holds the engine weakly,
    /// so an embedder that drops the engine without aborting it ends it
    /// too. Not started by the constructor, unlike the housekeeping sweep:
    /// the tests drive the tick by hand against a probe of their own, and a
    /// watch running behind them against the real volume would stop their
    /// fake torrents whenever the machine happened to be short of disk.
    pub fn start_free_space_watch(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(FREE_SPACE_WATCH_INTERVAL);
            loop {
                interval.tick().await;
                let Some(engine_fs) = weak.upgrade() else {
                    return;
                };
                engine_fs.free_space_watch_tick().await;
            }
        })
    }

    /// One pass of the free-space watch.
    ///
    /// For every engine, read the free space of the volume its output folder
    /// is on (one probe per distinct folder) and:
    ///
    /// * under [`CACHE_FREE_SPACE_FLOOR`], stop a torrent that is still
    ///   writing -- live, unfinished, not idle-paused, not already in the
    ///   error state -- with [`TorrentHandle::stop_for_space`], mark it, and
    ///   ring [`Self::out_of_space_signal`] so the cache cleaner runs a pass
    ///   now. The torrent keeps its files and its piece map; its readers
    ///   stay parked on whatever piece they were waiting for, which is a
    ///   buffering pause for the player while the cleaner decides.
    /// * at [`CACHE_FREE_SPACE_FLOOR`] + [`FREE_SPACE_RESUME_MARGIN`] or
    ///   more, start a torrent this watch stopped again -- the space came
    ///   back by some other route than the cleaner, which restarts what it
    ///   makes room for itself.
    /// * a torrent still stopped after [`STOPPED_READ_STALL_BOUND`] has its
    ///   readers failed ([`Engine::refuse_reads_for_space`]): nothing is
    ///   coming for them, and a player must be told rather than left
    ///   spinning.
    ///
    /// Pinned torrents are stopped like any other -- a pin is a reason to
    /// keep the bytes, not a licence to run the disk to zero -- and the
    /// cleaner's recovery restarts them once it has room. A volume that
    /// cannot be probed is left alone, as everywhere else: an unreadable
    /// reading is not "full".
    pub async fn free_space_watch_tick(&self) {
        let engines: Vec<_> = self.engines.read().await.values().cloned().collect();
        if engines.is_empty() {
            return;
        }
        let now = self.clock.now_secs();
        let mut readings: HashMap<std::path::PathBuf, Option<u64>> = HashMap::new();
        let mut stopped_any = false;
        for engine in engines {
            if engine.handle.manages_playback_lifecycle() {
                continue;
            }
            let folder = engine
                .handle
                .output_folder()
                .unwrap_or_else(|| self.download_dir.clone());
            let available = *readings.entry(folder.clone()).or_insert_with(|| {
                match probe_at_existing_ancestor(&*self.free_space_probe, &folder) {
                    Ok(available) => Some(available),
                    Err(error) => {
                        debug!(
                            folder = %folder.display(),
                            %error,
                            "could not read the output volume's free space; the watch leaves its torrents alone"
                        );
                        None
                    }
                }
            });
            let Some(available) = available else {
                continue;
            };

            if let Some(stopped_for) = engine.stopped_for_space_for(now) {
                if available >= CACHE_FREE_SPACE_FLOOR.saturating_add(FREE_SPACE_RESUME_MARGIN) {
                    match engine.handle.restart_after_error().await {
                        Ok(()) => {
                            engine.clear_space_stop();
                            tracing::info!(
                                info_hash = %engine.info_hash,
                                available,
                                "torrent_resumed_after_space_recovered"
                            );
                        }
                        Err(error) => tracing::warn!(
                            info_hash = %engine.info_hash,
                            error = %format!("{error:#}"),
                            "could not resume a torrent the free-space watch had stopped"
                        ),
                    }
                } else if !engine.reads_refused() && stopped_for >= STOPPED_READ_STALL_BOUND {
                    tracing::warn!(
                        info_hash = %engine.info_hash,
                        stopped_secs = stopped_for.as_secs(),
                        available,
                        "a torrent stopped for want of disk space is still stopped; failing its readers rather than leaving them parked"
                    );
                    engine.refuse_reads_for_space();
                }
                continue;
            }

            if available >= CACHE_FREE_SPACE_FLOOR {
                continue;
            }
            // Nothing to stop: not writing anyway, or the backend has already
            // stopped it (out of space, or dead).
            if engine.idle_paused.load(Ordering::Relaxed)
                || engine.handle.is_in_error_state().await
                || engine.handle.is_finished().await
            {
                continue;
            }
            match engine.handle.stop_for_space().await {
                Ok(()) => {
                    engine.mark_stopped_for_space(now);
                    stopped_any = true;
                    tracing::warn!(
                        info_hash = %engine.info_hash,
                        available,
                        floor = CACHE_FREE_SPACE_FLOOR,
                        pinned = engine.is_pinned(),
                        "torrent_stopped_for_space"
                    );
                }
                Err(error) => debug!(
                    info_hash = %engine.info_hash,
                    error = %format!("{error:#}"),
                    "the backend would not stop the torrent for space"
                ),
            }
        }
        if stopped_any {
            self.out_of_space_notify.notify_one();
        }
    }

    /// Completes once the free-space watch has stopped a torrent since the
    /// last time this completed (or since the engine was made, if a stop
    /// came first). One permit, not a counter: the cache cleaner that awaits
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

    /// [`Self::get_engine`] over the registry rather than `&self`, for
    /// [`Self::end_relocation`]'s reason: the caller outlives the request.
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

    /// What the cache cleaner may not evict.
    ///
    /// Every registry engine's files -- bar a dead one's, see
    /// [`Self::eviction_classes`] -- at the path the backend reports
    /// (`TorrentHandle::file_path`, or the output folder joined with the
    /// file's name when the backend knows the folder but not the path),
    /// `<download_dir>/<name>` for a backend that knows neither. A torrent
    /// without a file list yet protects its output folder (or
    /// `<download_dir>/<name>`).
    ///
    /// Plus the placement folder of every *dormant* pin. Those have no engine
    /// -- that is what dormant means -- so nothing above would name them, and
    /// the cleaner walks the downloads dir now: without this entry a pin whose
    /// torrent the backend did not restore would be aged out from under the
    /// user, which is a worse bug than the orphaned-and-immortal one that made
    /// the cleaner walk there in the first place. It can only name
    /// `<downloads dir>/<info hash>`, the placement this layer chooses itself;
    /// a dormant pin whose data predates a downloads dir lives in the cache
    /// root under a folder named by metadata a dormant pin does not have, and
    /// is protected by nothing.
    ///
    /// Plus, for both, the torrent's directory in the piece store. It does
    /// not exist yet -- nothing hands librqbit a
    /// [`crate::piece_store::PieceStoreFactory`] -- and naming it costs one
    /// path that matches nothing. Leaving it out costs the film somebody is
    /// watching: the store's root is inside the cache root on purpose, so
    /// every piece in it is walked, and a wiring commit that forgot this
    /// would make live piece data evictable mid-playback with nothing to
    /// notice it.
    ///
    /// Plus both ends of every relocation in flight
    /// ([`Self::begin_relocation`]). A relocation is the one window where a
    /// torrent's data has no engine at all speaking for it -- the engine
    /// leaves the registry before the backend is asked to move a byte, and a
    /// cross-device copy takes minutes -- so without those entries the
    /// cleaner would walk the tree being written into and the tree being read
    /// out of, with nothing protecting either.
    pub async fn protected_paths(&self) -> Vec<std::path::PathBuf> {
        self.eviction_classes().await.protected
    }

    /// [`Self::protected_paths`] together with what the same walk of the
    /// engines says the cleaner *should* take: see [`EvictionClasses`].
    ///
    /// A torrent in the backend's error state for a reason that is not a
    /// want of space is dead. Nothing restarts it -- the cleaner's recovery
    /// is for the out-of-space case alone, and the error would only recur
    /// -- so its files are bytes no one will ever read or resume into, and
    /// they used to be protected all the same, because protection was "every
    /// engine in the registry". On the television that prompted this two
    /// torrents that had died of a storage bug held 700 MB between them,
    /// the cleaner reported them protected, and every later stream failed
    /// for the space they held. The third torrent there had died of ENOSPC,
    /// and was protected too: a torrent stopped for space is listed
    /// separately ([`EvictionClasses::stopped_for_space`]), for the cleaner
    /// to evict whole when nothing else can go. A *pinned* torrent stays
    /// protected however it stopped: the user asked for those bytes, and an
    /// unpin is how they say otherwise.
    pub async fn eviction_classes(&self) -> EvictionClasses {
        let engines: Vec<_> = self.engines.read().await.values().cloned().collect();
        let pieces = crate::piece_store::root_in(&self.download_dir);
        let mut classes = EvictionClasses::default();
        for engine in engines {
            let paths = self.engine_paths(&engine).await;
            if engine.is_pinned() {
                classes.protected.extend(paths);
                continue;
            }
            let out_of_space =
                engine.is_stopped_for_space() || engine.handle.is_out_of_space().await;
            if out_of_space {
                classes.stopped_for_space.push(StoppedTorrent {
                    info_hash: engine.info_hash.clone(),
                    paths,
                });
            } else if engine.handle.is_in_error_state().await {
                classes.dead.extend(paths);
            } else {
                classes.protected.extend(paths);
            }
        }
        for pin in self.dormant_pinned_downloads() {
            classes.protected.push(pieces.join(&pin.info_hash));
            if let Some(folder) = self.download_folder(&pin.info_hash) {
                classes.protected.push(folder);
            }
        }
        classes
            .protected
            .extend(self.relocations.lock().values().flatten().cloned());
        classes
    }

    /// Every path `engine`'s data can be at: its directory in the piece store
    /// first (that placement is this layer's own, whatever the backend
    /// reports), then each file at the path the backend gives, or the best
    /// guess from the output folder when it gives none.
    ///
    /// Factored out of [`Self::protected_paths`] because a relocation has to
    /// name exactly this set for an engine that has just left the registry:
    /// the source of the copy is as evictable as the destination while the
    /// move runs, and half of a download arriving is the same loss as none.
    async fn engine_paths(&self, engine: &Arc<Engine<B::Handle>>) -> Vec<std::path::PathBuf> {
        let mut paths =
            vec![crate::piece_store::root_in(&self.download_dir).join(&engine.info_hash)];
        let stats = engine.get_statistics().await;
        let folder = engine
            .handle
            .output_folder()
            .filter(|folder| *folder != self.download_dir);
        if stats.files.is_empty() {
            paths.push(folder.unwrap_or_else(|| self.download_dir.join(&stats.name)));
            return paths;
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
        paths
    }

    /// Info hashes of torrents the backend stopped because the volume they
    /// write to ran out of space, and of torrents the free-space watch
    /// stopped before it could ([`Self::free_space_watch_tick`]).
    ///
    /// A full disk is the one torrent error worth acting on rather than
    /// reporting: the swarm is fine, the torrent is fine, the device is out
    /// of room. The caller that can do something about it is the server's
    /// cache cleaner, which evicts and then calls
    /// [`Self::restart_after_error`] -- this is how it finds out there is
    /// anything to evict *for*. Cheap on purpose (one lock read per engine,
    /// no I/O), because it is asked on a timer.
    pub async fn out_of_space_torrents(&self) -> Vec<String> {
        let engines: Vec<_> = self.engines.read().await.values().cloned().collect();
        let mut hashes = Vec::new();
        for engine in engines {
            if engine.is_stopped_for_space() || engine.handle.is_out_of_space().await {
                hashes.push(engine.handle.info_hash());
            }
        }
        hashes
    }

    /// Put the torrent `info_hash` back to work after the backend stopped it
    /// with an error, or after the free-space watch stopped it. `false` when
    /// no engine holds that hash any more (it was swept while space was
    /// being reclaimed), which is not a failure.
    pub async fn restart_after_error(&self, info_hash: &str) -> Result<bool> {
        let engine = self.engines.read().await.get(info_hash).cloned();
        let Some(engine) = engine else {
            return Ok(false);
        };
        engine.handle.restart_after_error().await?;
        engine.clear_space_stop();
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
            || !(engine.is_stopped_for_space() || engine.handle.is_out_of_space().await)
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
            .map(|(hash, engine)| (hash.clone(), engine.handle.transfer_totals()))
            .collect()
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

    /// [`StreamActivitySnapshot::playback_is_live`] without the snapshot.
    ///
    /// The snapshot is the definition -- which of the fields mean "somebody
    /// is watching" is decided there and tested there -- but building one
    /// to read three of its fields costs six lock acquisitions and four
    /// cloned collections. This is the same three questions asked directly:
    /// the per-engine reader counts, the stream-response counts and the
    /// unexpired leases, three read locks (the `engines` one included, so
    /// this too queues behind an add or remove -- the saving is the other
    /// three locks and the clones, not that wait) and nothing cloned.
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
        if self
            .active_streams
            .read()
            .await
            .values()
            .any(|count| *count > 0)
        {
            return true;
        }
        let now = self.clock.now_secs();
        self.active_playback_leases
            .read()
            .await
            .values()
            .any(|lease| playback_lease_is_active(lease, now))
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
    ///
    /// The guard is owned, and for the length of a relocation it belongs to
    /// the move rather than to this call ([`Self::relocate_engine`]). What
    /// it guards is the window in which the hash has no engine, and the move
    /// outlives the request that asked for it, so the lock has to outlive it
    /// too: a caller that goes away leaves the guard with the move, which
    /// drops it once the successor is published, and one that stays gets it
    /// back with the result and finishes under it.
    pub async fn pin_download(
        &self,
        info_hash: &str,
        file_idx: usize,
        extra_trackers: Option<Vec<String>>,
    ) -> Result<Arc<Engine<B::Handle>>, PinDownloadError> {
        let info_hash = info_hash.to_lowercase();
        let lock = self.pin_lock(&info_hash);
        let mut guard = Some(Arc::clone(&lock).lock_owned().await);
        let result = self
            .pin_download_locked(&info_hash, file_idx, extra_trackers, &mut guard)
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
    /// its own clone, and so does a guard a relocation is still holding,
    /// either of which keeps the entry alive).
    fn release_pin_lock(&self, info_hash: &str, lock: Arc<tokio::sync::Mutex<()>>) {
        let mut locks = self.pin_locks.lock();
        if Arc::strong_count(&lock) == 2 {
            locks.remove(info_hash);
        }
    }

    /// [`Self::pin_download`] with the per-hash lock held. `pin_guard` is
    /// that lock's guard: a relocation takes it for the length of the move
    /// and returns it here if this call is still around to receive it.
    async fn pin_download_locked(
        &self,
        info_hash: &str,
        file_idx: usize,
        extra_trackers: Option<Vec<String>>,
        pin_guard: &mut Option<PinGuard>,
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
        let AddedMagnet {
            engine,
            started_here,
            joiners,
        } = self
            .add_magnet_placed(info_hash, extra_trackers.clone(), placement)
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
            // Torn down only when demonstrably this pin's and nobody
            // else's: this call started the add, nothing joined it while
            // metadata resolved, and the torrent sits in the folder only
            // pins place under. Where it sits is not enough on its own --
            // it is the pin's placement, but a stream request that looked
            // the hash up meanwhile joined this very add and is holding
            // the same engine, and dropping the torrent from the backend
            // fails every read it is about to make (and any it has already
            // opened). A joined torrent stays for the idle sweeper exactly
            // as one in the cache root does: a torrent in the cache root
            // may be a stream request's own add this call joined, and
            // without a downloads dir the two cannot be told apart. A
            // folder this add created holds nothing but the placeholder
            // librqbit pre-sized, so it goes too; a folder that was there
            // before keeps whatever it holds. What this cannot see is a
            // lookup that found the *published* engine between the add
            // finishing and this check -- a window of one free-space probe
            // rather than of a metadata resolution.
            let placed_by_this_pin = started_here
                && joiners == 0
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
                self.relocate_engine(engine.clone(), folder, extra_trackers, pin_guard)
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
    /// torrent to delete anything of beyond its placement folder under the
    /// downloads dir, which this layer named itself and removes
    /// ([`Self::delete_dormant_download_data`]) -- while the pin stands
    /// [`Self::protected_paths`] keeps the cleaner off it, so this is what
    /// takes it now rather than in thirty days. What was
    /// really deleted is reported, not what was asked for
    /// ([`UnpinOutcome`]). A `file_idx` the torrent does not
    /// have is then refused with [`PinDownloadError::FileNotFound`], as
    /// [`Self::pin_download`] refuses it: a stale index must not be read as
    /// "delete the whole torrent".
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
            let deleted_files = delete_files
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
    /// it. That is the placement folder `<downloads dir>/<info hash>`,
    /// which this layer named itself and which holds nothing but that
    /// torrent. Returns whether the data is gone.
    ///
    /// Nothing goes while another file of the same hash is still pinned --
    /// the folder holds that file too -- and nothing can go without a
    /// downloads dir: the torrent then lived in the cache root under a
    /// folder named by metadata a dormant pin does not have. Either way the
    /// bytes are reachable by the cleaner, which walks the downloads dir as
    /// well now; this call is what makes an explicit `deleteFiles` unpin
    /// take effect at once instead of waiting on the age rule, and what
    /// takes the folder out of [`Self::protected_paths`] with the pin.
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
                "other files of the torrent are still pinned; its download folder stays"
            );
            return false;
        }
        let Some(folder) = self.download_folder(info_hash) else {
            tracing::info!(
                info_hash,
                file_idx,
                "no torrent and no downloads dir: nothing this layer can name to delete \
                 (the cache cleaner ages the cache root out on its own)"
            );
            return false;
        };
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
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(error) => {
                tracing::warn!(
                    info_hash,
                    file_idx,
                    folder = %folder.display(),
                    %error,
                    "could not delete the dormant download's folder"
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
    /// file goes -- truncated to nothing and then unlinked, since librqbit
    /// keeps an open `File` on it for the torrent's lifetime and an unlink
    /// alone would not free a byte. The caller reconciles the want-set
    /// without the file first, so the backend does not write it again.
    /// Best effort: a failure is logged, the unpin stands, and the returned
    /// flag says whether the data is actually gone.
    ///
    /// The bytes are one record of the file and the backend's have-set is
    /// the other, and deleting the first does nothing to the second: left
    /// alone, librqbit went on reporting the deleted file complete,
    /// advertising its pieces and answering a peer's request with a read
    /// past the end of nothing, and a re-pin of the file found nothing to
    /// download and declared it finished -- an "offline" episode that is an
    /// immediate read error. So the have-set is edited first
    /// ([`TorrentHandle::drop_file_pieces`]) and the claim it returns is
    /// held until the unlink is done: while it stands nothing can download
    /// a piece back into the file being deleted, and dropping it is what
    /// re-queues the boundary piece the still-pinned neighbour shares. A
    /// backend that cannot drop (a torrent restored at startup -- see the
    /// librqbit handle's doc for why, and for what the next restart does
    /// about it) is a warning and the delete goes ahead: the caller asked
    /// for the disk back, and the stale have-set is the lesser of the two
    /// lies.
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
            let deleted = match tokio::fs::remove_file(&path).await {
                Ok(()) => {
                    tracing::info!(
                        info_hash = %engine.info_hash,
                        file_idx,
                        path = %path.display(),
                        pieces_dropped = dropped.as_ref().map(|d| d.pieces().len()),
                        "download_file_deleted"
                    );
                    true
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
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
            // Released only now that the bytes are gone -- see the doc above
            // for what the claim holds off while they go.
            drop(dropped);
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

    /// Unpause the torrents the backend restored at startup, once their pins
    /// are back.
    ///
    /// A backend that sets piece reclaim
    /// ([`crate::backend::TorrentBackend::sets_piece_reclaim`]) restores
    /// every torrent paused, whatever it was doing when the process died:
    /// the per-session piece-level want-set did not survive the record, so
    /// librqbit forces a restored reclaim torrent paused and wanting every
    /// hole in the storage until the caller re-applies the want-set, or a
    /// seeder reaching it would refill a hole the caller was about to drop.
    /// The want-set this layer re-applies is the pins
    /// ([`Self::restore_pinned_downloads`], run just before this) and the
    /// `only_files` selection librqbit persists with the torrent; there is
    /// no piece-level want-set to re-apply here, because the retention
    /// policy that would compute one is not wired into the session yet (see
    /// [`crate::piece_store`]). So once the pins are back, the torrents are
    /// unpaused -- otherwise a restart would come up with every torrent
    /// stopped, downloading and seeding nothing. The seeding-disabled and
    /// idle policies pause again from here whatever should not be running.
    ///
    /// A no-op unless the backend sets piece reclaim: the shipped session's
    /// filesystem storage cannot reclaim, so its restored torrents were
    /// never force-paused and keep whatever paused state they had.
    ///
    /// Called once at startup, after [`Self::restore_pinned_downloads`] and
    /// before any route can add anything -- the engines here are exactly the
    /// restored torrents.
    pub async fn resume_restored_torrents(&self) {
        if !self.backend.sets_piece_reclaim() {
            return;
        }
        let engines: Vec<_> = self.engines.read().await.values().cloned().collect();
        let mut resumed = 0usize;
        for engine in engines {
            match engine.handle.unpause_restored().await {
                Ok(()) => {
                    engine.idle_paused.store(false, Ordering::Relaxed);
                    resumed += 1;
                }
                Err(error) => {
                    tracing::warn!(
                        info_hash = %engine.info_hash,
                        error = %format!("{error:#}"),
                        "could not resume a restored torrent; it stays paused"
                    );
                }
            }
        }
        if resumed > 0 {
            tracing::info!(resumed, "restored_torrents_resumed");
        }
    }

    /// Reconcile the piece store against what this session actually holds:
    /// every torrent's pieces under [`piece_store::root_in`] that nothing
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
        let root = crate::piece_store::root_in(&self.download_dir);
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
    ///
    /// **The move does not run in the caller's future.** `pin_download`'s
    /// caller is an awaited axum handler, so a client that hangs up drops it
    /// wherever it is, and a cross-device copy is minutes of "wherever". Left
    /// in the caller's future, that drop stopped the copy halfway with the
    /// torrent already out of the backend, and skipped the settling below
    /// entirely: the hash stayed parked as an `Adding` entry nothing retries,
    /// and `relocations` went on naming both ends of the move for the life of
    /// the process -- so the trees it named, which by then no engine and no
    /// persisted pin named either, could never be evicted again.
    ///
    /// So the whole of the move is spawned and a supervisor settles it
    /// however it ends, the same shape and for the same reason as
    /// [`Self::spawn_magnet_add`]'s: the task that calls into the backend is
    /// the one that can be slow or panic, and the task that puts the
    /// registries right touches nothing but maps, so it is still there to run
    /// when the other one is not -- `end_relocation` happens on every outcome
    /// of the move, a panic in it included.
    ///
    /// *The whole* of it: [`Self::begin_relocation`] is the spawned task's
    /// own first act, not the caller's. It records both ends of the move in
    /// `relocations` before the engine leaves the registry, and done on the
    /// caller's side that record was made two lock acquisitions before
    /// anything was spawned -- either of which pends whenever another task
    /// holds the registry, which is where a hangup then left the entry, with
    /// nothing spawned to remove it. Only the paths it records are read here,
    /// before anything is recorded at all: a caller dropped in the middle of
    /// that has begun nothing.
    ///
    /// The per-hash pin lock travels with the move for the same reason: the
    /// caller hands its guard over and takes it back with the result. The
    /// lock is what keeps an unpin out of the window where the hash has no
    /// engine, and an unpin that got in would delete the tree being copied
    /// into as a dormant pin's leftovers -- after which this would publish
    /// the download again, pinned, protected and without its files.
    ///
    /// The caller only waits for the result. A caller that goes away
    /// loses its answer and nothing else -- the move finishes, the engine is
    /// published in its new home carrying the pin that asked for it, and the
    /// waiters parked on the hash get it. What that caller no longer runs is
    /// the rest of `pin_download`: the pin is not written to
    /// `pinned-downloads.json`, so it holds until the process ends and is
    /// forgotten by the next start, which is a download to re-request, not
    /// bytes nothing can reclaim.
    async fn relocate_engine(
        &self,
        engine: Arc<Engine<B::Handle>>,
        folder: std::path::PathBuf,
        extra_trackers: Option<Vec<String>>,
        pin_guard: &mut Option<PinGuard>,
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
        // Asked of the backend here and recorded there: reading an engine's
        // files starts nothing, so a caller dropped in the middle of it
        // leaves no entry, no unprotected tree and no half-moved torrent.
        let mut protected = self.engine_paths(&engine).await;
        protected.push(folder.clone());

        let moving = {
            let backend = self.backend.clone();
            let engines = self.engines.clone();
            let adds = self.magnet_adds.clone();
            let relocations = self.relocations.clone();
            let clock = self.clock;
            let engine = engine.clone();
            let pending = pending.clone();
            tokio::spawn(async move {
                Self::begin_relocation(
                    &engines,
                    &adds,
                    &relocations,
                    clock,
                    &engine,
                    protected,
                    pending,
                )
                .await;
                match backend
                    .relocate_torrent(&engine.info_hash, placement, trackers)
                    .await
                {
                    Ok(handle) => Relocated::Moved(handle),
                    Err(error) => Relocated::Failed {
                        error,
                        still_managed: backend.get_torrent(&engine.info_hash).await,
                    },
                }
            })
        };
        let supervisor = {
            let engines = self.engines.clone();
            let adds = self.magnet_adds.clone();
            let relocations = self.relocations.clone();
            let clock = self.clock;
            let held = pin_guard.take();
            tokio::spawn(async move {
                let relocated = match moving.await {
                    Ok(relocated) => relocated,
                    // A panic only reaches here in a debug build; the release
                    // profile's `panic = "abort"` takes the process instead.
                    // Nothing aborts this handle, so cancellation is not a case.
                    // Settled like a move that failed with the torrent gone:
                    // whatever the task had recorded of the relocation goes
                    // with it, rather than parking the hash for the life of
                    // the process behind an entry nothing retries or sweeps.
                    Err(join_error) => Relocated::Failed {
                        error: anyhow::anyhow!("the relocation task did not finish: {join_error}"),
                        still_managed: None,
                    },
                };
                let (result, settled) = match relocated {
                    Relocated::Moved(handle) => {
                        let engine = Self::replace_engine(&engines, clock, &engine, handle).await;
                        (Ok(engine.clone()), Ok(engine))
                    }
                    Relocated::Failed {
                        error,
                        still_managed,
                    } => {
                        tracing::warn!(
                            info_hash = %engine.info_hash,
                            error = %format!("{error:#}"),
                            "download_relocate_failed"
                        );
                        let settled = match still_managed {
                            Some(handle) => {
                                Ok(Self::replace_engine(&engines, clock, &engine, handle).await)
                            }
                            None => match Self::lookup_engine(&engines, &engine.info_hash).await {
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
                Self::end_relocation(&relocations, &adds, &engine.info_hash, &pending).await;
                // Whoever awaited the entry: the engine is published (or the
                // hash is free for a fresh add) by now.
                let _ = settle.send(settled);
                // The pin lock goes back to the caller with the result, or
                // is released here with this task if the caller is gone.
                (result, held)
            })
        };
        match supervisor.await {
            Ok((result, held)) => {
                *pin_guard = held;
                result
            }
            Err(join_error) => Err(PinDownloadError::Backend(anyhow::anyhow!(
                "the relocation supervisor did not finish: {join_error}"
            ))),
        }
    }

    /// Take `engine` out of the registry (only while it still is the
    /// registry's engine) and put `pending` in its place in the magnet-add
    /// registry, atomically for lookups: `lookup_or_begin_add_magnet` takes
    /// the add registry before the engines, as this does.
    ///
    /// Records `protected` -- both ends of the move, [`Self::engine_paths`]
    /// of the engine that is about to leave plus the folder being written
    /// into -- in `relocations` *first*, so there is no instant in which the
    /// data is walkable and evictable: taking the engine out of the registry
    /// takes it out of [`Self::protected_paths`] too, and the new engine that
    /// would put it back does not exist until the backend has finished
    /// copying. The entry goes with [`Self::end_relocation`], after the
    /// successor is published.
    ///
    /// Over the registries rather than `&self`, and called from the spawned
    /// half of [`Self::relocate_engine`] rather than from the request, for
    /// [`Self::end_relocation`]'s reason and then one more: what this records
    /// is undone by that, and a record made where a hangup can land between
    /// the two is a tree protected for the life of the process. Everything
    /// after the insert here awaits a lock some other task may be holding.
    async fn begin_relocation(
        engines: &EngineRegistry<B::Handle>,
        adds: &MagnetAddRegistry<B::Handle>,
        relocations: &RelocationRegistry,
        clock: Clock,
        engine: &Arc<Engine<B::Handle>>,
        protected: Vec<std::path::PathBuf>,
        pending: PendingMagnetAdd<B::Handle>,
    ) {
        relocations
            .lock()
            .insert(engine.info_hash.clone(), protected);
        let now = clock.now_secs();
        let mut adds = adds.write().await;
        let mut engines = engines.write().await;
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

    /// Drop the relocation's registry entry, if it still is `pending`, and the
    /// paths it was protecting. The engine (if any) is published before this,
    /// so a lookup between the two always finds one or the other, and
    /// [`Self::protected_paths`] never stops naming the data it is holding.
    ///
    /// Over the registries rather than `&self`: the only caller is the
    /// detached supervisor in [`Self::relocate_engine`], which outlives the
    /// request and so cannot borrow the `EngineFS` it came in on. It is also
    /// the reason this must never be skippable -- what it undoes is
    /// [`Self::begin_relocation`], and an entry left behind is a tree the
    /// cache cleaner may not touch and nothing else names.
    async fn end_relocation(
        relocations: &RelocationRegistry,
        adds: &MagnetAddRegistry<B::Handle>,
        info_hash: &str,
        pending: &PendingMagnetAdd<B::Handle>,
    ) {
        relocations.lock().remove(info_hash);
        let mut adds = adds.write().await;
        if matches!(
            adds.get(info_hash).map(|entry| &entry.state),
            Some(MagnetAddState::Adding(current)) if current.id == pending.id
        ) {
            adds.remove(info_hash);
        }
    }

    /// Publish `handle` as the engine for `old`'s hash, carrying the pins.
    /// Over the registry rather than `&self`, for [`Self::end_relocation`]'s
    /// reason: the caller outlives the request.
    async fn replace_engine(
        engines: &EngineRegistry<B::Handle>,
        clock: Clock,
        old: &Arc<Engine<B::Handle>>,
        handle: B::Handle,
    ) -> Arc<Engine<B::Handle>> {
        let engine = Arc::new(Engine::new_with_handle(handle, &old.info_hash, clock));
        *engine.pinned_files.write() = old.pinned_files.read().clone();
        engines
            .write()
            .await
            .insert(old.info_hash.clone(), engine.clone());
        engine
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
        self.active_playback_leases.write().await.remove(&key);
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
                // The free-space watch already stopped it, and lifts its own
                // stop when the volume recovers; the backend refuses a
                // second pause anyway (see `TorrentHandle::stop_for_space`).
                if engine.is_stopped_for_space() {
                    tracing::debug!(
                        info_hash = %info_hash,
                        "Skipping idle pause: the free-space watch has already stopped this torrent"
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
        let (backend, restored) = LibrqbitBackend::new(
            download_dir.clone(),
            TorrentListenPort::default(),
            Vec::new(),
            crate::backend::dht_bootstrap::DhtBootstrapDns::default().resolvers_in(&download_dir),
        )
        .await?;
        let efs = Self::new_with_backend(backend, restored, root_dir.join("cache"), download_dir);
        efs.restore_pinned_downloads().await;
        efs.resume_restored_torrents().await;
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
        efs.resume_restored_torrents().await;
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
        /// Files whose pieces `drop_file_pieces` was asked to forget, in
        /// order -- the delete path must ask before it removes a byte.
        dropped_file_pieces: Mutex<Vec<usize>>,
        /// The fake handle's own pin set (what the real backend keeps in its
        /// `PinnedFiles` map), reported through `stats()`.
        pinned: Mutex<std::collections::BTreeSet<usize>>,
        /// What `output_folder()` reports; set by the fake backend's
        /// placed add and relocate.
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
        /// How many times the torrent was put back to work after that.
        restart_after_error: AtomicUsize,
        /// How many times the free-space watch stopped the torrent.
        stop_for_space: AtomicUsize,
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

        fn transfer_totals(&self) -> crate::backend::TransferTotals {
            crate::backend::TransferTotals {
                fetched: self.counters.fetched.load(Ordering::SeqCst),
                uploaded: self.counters.uploaded.load(Ordering::SeqCst),
            }
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

        async fn drop_file_pieces(
            &self,
            file_idx: usize,
        ) -> Result<Option<crate::backend::DroppedFilePieces>> {
            self.counters
                .dropped_file_pieces
                .lock()
                .unwrap()
                .push(file_idx);
            Ok(Some(crate::backend::DroppedFilePieces::new(vec![], ())))
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

        async fn is_out_of_space(&self) -> bool {
            self.counters.out_of_space.load(Ordering::SeqCst)
        }

        async fn is_finished(&self) -> bool {
            self.counters.seeded.load(Ordering::SeqCst)
        }

        async fn stop_for_space(&self) -> Result<()> {
            self.counters.stop_for_space.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn is_in_error_state(&self) -> bool {
            self.counters.in_error_state.load(Ordering::SeqCst)
                || self.counters.out_of_space.load(Ordering::SeqCst)
        }

        async fn restart_after_error(&self) -> Result<()> {
            self.counters
                .restart_after_error
                .fetch_add(1, Ordering::SeqCst);
            self.counters.out_of_space.store(false, Ordering::SeqCst);
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

    /// Wait for `ready` to hold instead of sleeping for it. A pin reaches
    /// its relocation through `tokio::fs`, a blocking-pool round trip whose
    /// duration a test on a loaded machine may not assume: a fixed sleep
    /// that is long enough here observes the state before the move on a
    /// busy CI runner, and asserts about the wrong moment.
    async fn until(mut ready: impl FnMut() -> bool) {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Holds once [`BackendEngineFS::relocate_engine`] has parked the hash
    /// in the magnet registry and asked the backend to move the files --
    /// `FakeBackend::relocate_torrent` records the request before it blocks
    /// on `relocate_hold`, and `begin_relocation` runs before that.
    fn relocation_started(enginefs: &BackendEngineFS<FakeBackend>) -> bool {
        !enginefs.backend.relocations.lock().unwrap().is_empty()
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

        insert_active_lease(&enginefs, 1).await;
        assert!(
            enginefs.stream_activity_snapshot().await.playback_is_live(),
            "a live playback lease is a client that is still there between reads"
        );
    }

    /// `BackendEngineFS::playback_is_live` is the snapshot's answer without
    /// the snapshot, so it is checked against the snapshot at every state
    /// the three live fields can put the server in -- each one on its own,
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

        // A lease that has not expired, then one that has.
        insert_active_lease(&enginefs, 1).await;
        both_agree(&enginefs, true, "an unexpired playback lease").await;
        enginefs
            .active_playback_leases
            .write()
            .await
            .values_mut()
            .for_each(|lease| lease.expires_at_secs = 0);
        both_agree(&enginefs, false, "an expired lease is a client that left").await;
    }

    /// The light's reading of the connection is a peek over the engines that
    /// exist: it touches no idle clock, looks nothing up, adds nothing, and
    /// an engine that has gone is gone from the sum.
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
        let root = fake_engine_root();
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
        let root = fake_engine_root();
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
            until(|| relocation_started(&enginefs)).await;
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

    /// A relocation is the one window in which a torrent's data has nothing
    /// speaking for it, and both ends of the move are exposed.
    ///
    /// `begin_relocation` takes the engine out of the registry before the
    /// backend is asked to move anything, and `protected_paths` names only the
    /// engines it can see. So for the whole of a copy that can take minutes
    /// the cleaner walks the destination tree -- files whose mtime is *now*,
    /// but which the size rule will happily take -- and the source it is being
    /// copied from, with neither in the protected set. What arrives is then
    /// half a download.
    #[tokio::test]
    async fn a_relocation_protects_both_ends_of_the_move_while_it_runs() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        let show = enginefs.download_dir.join("show");
        *counters.output_folder.lock().unwrap() = Some(show.clone());
        let downloads = enginefs.download_dir.join("offline");
        enginefs.set_downloads_dir(Some(downloads.clone()));
        enginefs.backend.hold_relocate.store(true, Ordering::SeqCst);
        let pieces = crate::piece_store::root_in(&enginefs.download_dir);

        let inspect = async {
            until(|| relocation_started(&enginefs)).await;
            assert!(
                enginefs.get_engine(TEST_HASH).await.is_none(),
                "the engine is out of the registry for the length of the move"
            );
            let protected = enginefs.protected_paths().await;
            assert!(
                protected.contains(&downloads.join(TEST_HASH)),
                "the destination being written into: {protected:?}"
            );
            assert!(
                protected.contains(&show.join("video-0.mkv")),
                "the source being copied out of: {protected:?}"
            );
            assert!(
                protected.contains(&pieces.join(TEST_HASH)),
                "and the pieces, at either end: {protected:?}"
            );
            enginefs.backend.relocate_hold.add_permits(1);
        };
        let (pinned, ()) = tokio::join!(enginefs.pin_download(TEST_HASH, 0, None), inspect);
        pinned.expect("the pin relocates the torrent");

        // And once the new engine is published it is the engine that speaks
        // for the data again -- the relocation's own entry is not left behind
        // to protect a folder nothing is using.
        let after = enginefs.protected_paths().await;
        assert!(after.contains(&downloads.join(TEST_HASH).join("video-0.mkv")));
        assert!(
            !after.contains(&show.join("video-0.mkv")),
            "the source it moved off is cache again: {after:?}"
        );
    }

    /// A move, once begun, is nobody's request any more.
    ///
    /// `POST /{infoHash}/{fileIdx}/download` is an awaited axum handler, so a
    /// client that hangs up drops the whole of `pin_download` wherever it
    /// happens to be -- and a cross-device relocation is minutes of that
    /// "wherever". Dropped between `begin_relocation` and `end_relocation`,
    /// the move stopped halfway with the torrent already out of the backend,
    /// the hash stayed parked in the magnet registry (so every later lookup
    /// found an `Adding` entry that was already settled with a failure and is
    /// never retried), and `relocations` kept naming both ends of the move for
    /// the life of the process -- multi-gigabyte trees that no engine, no
    /// persisted pin and nothing else could ever name again, and that the
    /// cache cleaner was therefore forbidden to reclaim forever.
    ///
    /// So the move does not run in the caller's future at all: it runs
    /// detached and is settled by a supervisor, exactly as a magnet add is,
    /// and the caller only waits for it.
    #[tokio::test]
    async fn a_relocation_outlives_the_request_that_started_it() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        let show = enginefs.download_dir.join("show");
        *counters.output_folder.lock().unwrap() = Some(show.clone());
        let downloads = enginefs.download_dir.join("offline");
        enginefs.set_downloads_dir(Some(downloads.clone()));
        enginefs.backend.hold_relocate.store(true, Ordering::SeqCst);

        {
            let mut pin = std::pin::pin!(enginefs.pin_download(TEST_HASH, 0, None));
            tokio::select! {
                _ = &mut pin => panic!("the move is held; the pin cannot have finished"),
                () = until(|| relocation_started(&enginefs)) => {}
            }
            // The client hangs up here.
        }

        // The backend finishes the move it was asked for, and everything the
        // move parked is settled by the half of it the client never held.
        enginefs.backend.relocate_hold.add_permits(1);
        assert!(
            wait_until(TEST_WAIT_BOUND, || enginefs.relocations.lock().is_empty()).await,
            "the move settled the relocation it began"
        );
        let engine = enginefs
            .get_engine(TEST_HASH)
            .await
            .expect("the relocated engine is published");
        assert_eq!(
            engine.handle.output_folder(),
            Some(downloads.join(TEST_HASH)),
            "in its new home"
        );
        assert!(engine.is_pinned(), "with the pin that asked for the move");
        assert!(
            enginefs.magnet_adds.read().await.is_empty(),
            "and the hash is not left parked as an add nothing will ever retry"
        );
        let protected = enginefs.protected_paths().await;
        assert!(
            !protected.contains(&show.join("video-0.mkv")),
            "the source it moved off is cache again: {protected:?}"
        );
        assert!(
            !protected.contains(&downloads.join(TEST_HASH)),
            "and the destination is the engine's to speak for, not a relocation's: {protected:?}"
        );
    }

    /// The window a dropped request could still leave a relocation in.
    ///
    /// Detaching the move settled every relocation the backend was asked
    /// for, but the bookkeeping that *precedes* the ask -- the `relocations`
    /// entry the cache cleaner reads, put there before the engine leaves the
    /// registry so the data is never unprotected for an instant -- was still
    /// recorded by the request's own future, two lock acquisitions before
    /// anything was spawned. Either lock pends whenever another task holds
    /// it (every `get_engine` takes the engine registry to read, and the
    /// seeding switch holds it across a resume per engine), so a client that
    /// hung up right there dropped the whole call between the entry and the
    /// task that removes it: both ends of a move that never happened,
    /// protected for the life of the process.
    ///
    /// So the recording is the detached half's first act, and the half that
    /// settles it runs whatever becomes of it.
    #[tokio::test]
    async fn a_relocation_the_request_never_lived_to_start_settles_too() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        let show = enginefs.download_dir.join("show");
        *counters.output_folder.lock().unwrap() = Some(show.clone());
        let downloads = enginefs.download_dir.join("offline");
        enginefs.set_downloads_dir(Some(downloads.clone()));

        // A reader is enough to hold the registry's next writer off, and a
        // reader is what every concurrent request has.
        let engines = enginefs.engines.read().await;
        {
            let mut pin = std::pin::pin!(enginefs.pin_download(TEST_HASH, 0, None));
            tokio::select! {
                _ = &mut pin => panic!("the registry is held; the pin cannot have finished"),
                () = until(|| !enginefs.relocations.lock().is_empty()) => {}
            }
            // The client hangs up: the move is recorded and not yet asked
            // for, which is the whole of the window under test.
        }
        assert!(
            enginefs.backend.relocations.lock().unwrap().is_empty(),
            "the backend has not been asked to move anything yet"
        );
        drop(engines);

        assert!(
            wait_until(TEST_WAIT_BOUND, || enginefs.relocations.lock().is_empty()).await,
            "the relocation was settled by the half the client never held"
        );
        let engine = enginefs
            .get_engine(TEST_HASH)
            .await
            .expect("the relocated engine is published");
        assert_eq!(
            engine.handle.output_folder(),
            Some(downloads.join(TEST_HASH)),
            "in its new home"
        );
        assert!(
            enginefs.magnet_adds.read().await.is_empty(),
            "and the hash is not left parked as an add nothing will ever retry"
        );
        let protected = enginefs.protected_paths().await;
        assert!(
            !protected.contains(&show.join("video-0.mkv")),
            "the source it moved off is cache again: {protected:?}"
        );
    }

    /// A remove-download issued while a move nobody is waiting for is still
    /// running waits for it, as it waits for a pin the caller is still
    /// holding.
    ///
    /// The per-hash lock is what makes `unpin_download` apply to the pin it
    /// raced rather than to the hole in the middle of it: for the length of
    /// a relocation the hash has no engine, so an unpin that gets through
    /// finds none, reports that nothing was pinned, and deletes
    /// `<downloadsDir>/<hash>` -- the tree the backend is copying into --
    /// as a dormant pin's leftovers. The move then publishes its successor
    /// with the pin carried over, and the download the user just removed is
    /// live, pinned, protected from the cleaner and missing its files.
    ///
    /// Detaching the move opened exactly that: the lock was held by the
    /// request, and the request was gone. It is held by the move instead.
    #[tokio::test]
    async fn an_unpin_waits_for_a_move_the_request_walked_away_from() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        let show = enginefs.download_dir.join("show");
        *counters.output_folder.lock().unwrap() = Some(show.clone());
        let downloads = enginefs.download_dir.join("offline");
        enginefs.set_downloads_dir(Some(downloads.clone()));
        enginefs.backend.hold_relocate.store(true, Ordering::SeqCst);

        {
            let mut pin = std::pin::pin!(enginefs.pin_download(TEST_HASH, 0, None));
            tokio::select! {
                _ = &mut pin => panic!("the move is held; the pin cannot have finished"),
                () = until(|| relocation_started(&enginefs)) => {}
            }
            // The client hangs up; the backend copies on.
        }

        let mut unpin = std::pin::pin!(enginefs.unpin_download(TEST_HASH, 0, true));
        tokio::select! {
            _ = &mut unpin => {
                panic!("the unpin ran into the middle of the move instead of waiting for it")
            }
            // The map's `Arc` and the move's guard are two; a third means
            // the unpin has taken the lock too and is parked on it, which
            // is the interleaving under test.
            () = until(|| {
                enginefs
                    .pin_locks
                    .lock()
                    .get(TEST_HASH)
                    .is_some_and(|lock| Arc::strong_count(lock) >= 3)
            }) => {}
        }

        enginefs.backend.relocate_hold.add_permits(1);
        let outcome = unpin
            .await
            .expect("the unpin applies once the move is done");
        assert!(
            outcome.unpinned,
            "it found the pin the move carried into the new engine"
        );
        assert!(outcome.deleted_files, "and the data went with it");
        assert_eq!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .as_slice(),
            &[TEST_HASH.to_string()],
            "the torrent left the backend rather than its folder being pulled out from under it"
        );
        assert!(
            enginefs.get_engine(TEST_HASH).await.is_none(),
            "nothing is left running for a download the user removed"
        );
        assert!(
            enginefs.pinned_downloads().await.is_empty(),
            "and nothing is left pinned"
        );
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
            until(|| relocation_started(&enginefs)).await;
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
            // Synchronised on the relocation, not on the clock: a waiter
            // that looks the hash up before the move parks it finds the
            // old engine, which proves nothing about the settlement.
            let (parked, waiter_parked) = tokio::sync::oneshot::channel();
            let waiter = async {
                until(|| relocation_started(enginefs)).await;
                let _ = parked.send(());
                enginefs.get_or_add_magnet(TEST_HASH, None).await
            };
            let release = async {
                // The waiter has joined the entry -- `done` is the next
                // thing it awaits -- so let the move fail under it.
                let _ = waiter_parked.await;
                assert!(enginefs.pending_magnet_add(TEST_HASH).await.is_some());
                enginefs.backend.relocate_hold.add_permits(1);
            };
            let (pinned, waited, ()) =
                tokio::join!(enginefs.pin_download(TEST_HASH, 0, None), waiter, release);
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

    /// A torrent the backend stopped for want of disk space is listed for the
    /// cleaner, and restarting it is what takes it off the list -- so the
    /// cleaner can find it, reclaim space, and put it back to work instead of
    /// leaving playback dead against a healthy swarm.
    #[tokio::test]
    async fn out_of_space_torrents_are_listed_and_can_be_restarted() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);

        // A healthy torrent is nobody's business.
        assert!(enginefs.out_of_space_torrents().await.is_empty());
        assert_eq!(counters.restart_after_error.load(Ordering::SeqCst), 0);

        counters.out_of_space.store(true, Ordering::SeqCst);
        assert_eq!(
            enginefs.out_of_space_torrents().await,
            vec![TEST_HASH.to_string()]
        );

        assert!(enginefs.restart_after_error(TEST_HASH).await.unwrap());
        assert_eq!(counters.restart_after_error.load(Ordering::SeqCst), 1);
        assert!(
            enginefs.out_of_space_torrents().await.is_empty(),
            "a restarted torrent is no longer stopped"
        );

        // A hash no engine holds any more -- swept while space was being
        // reclaimed -- is not an error, and restarts nothing.
        assert!(
            !enginefs
                .restart_after_error("ffffffffffffffffffffffffffffffffffffffff")
                .await
                .unwrap()
        );
        assert_eq!(counters.restart_after_error.load(Ordering::SeqCst), 1);
    }

    /// A torrent the backend stopped with an error nothing will retry is
    /// dead, and its files are the cleaner's to take first -- they used to
    /// be protected like a live engine's, which on a full television kept
    /// 700 MB of two dead torrents' bytes from every later stream. One that
    /// died of a full disk is not dead (the cleaner's recovery restarts it
    /// once there is room), and a pinned one stays protected however it
    /// died: an unpin is how the user gives those bytes up.
    #[tokio::test]
    async fn a_dead_torrents_files_are_the_cleaners_to_take_first() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        let root = enginefs.download_dir.clone();
        let pieces = crate::piece_store::root_in(&root).join(TEST_HASH);
        let files = vec![
            pieces.clone(),
            root.join("video-0.mkv"),
            root.join("video-1.mkv"),
        ];

        let live = enginefs.eviction_classes().await;
        assert_eq!(live.protected, files, "a live torrent is protected");
        assert!(live.dead.is_empty());

        counters.in_error_state.store(true, Ordering::SeqCst);
        let dead = enginefs.eviction_classes().await;
        assert!(dead.protected.is_empty(), "a dead one protects nothing");
        assert_eq!(dead.dead, files, "and its files go first");
        assert!(
            enginefs.protected_paths().await.is_empty(),
            "protected_paths is the same walk"
        );

        // Out of space is not dead: that one is listed whole, for the
        // recovery to restart or the cleaner to take as a last resort.
        counters.out_of_space.store(true, Ordering::SeqCst);
        let stopped = enginefs.eviction_classes().await;
        assert!(stopped.protected.is_empty());
        assert!(stopped.dead.is_empty());
        assert_eq!(stopped.stopped_for_space.len(), 1);
        counters.out_of_space.store(false, Ordering::SeqCst);

        // Pinned and dead: the pin outranks the death.
        enginefs.pin_download(TEST_HASH, 1, None).await.unwrap();
        let pinned = enginefs.eviction_classes().await;
        assert_eq!(pinned.protected, files);
        assert!(pinned.dead.is_empty());
    }

    /// A dormant pin has no engine, so nothing in the engine walk names it --
    /// and the cleaner walks the downloads dir now. Without its folder in the
    /// protected set, an offline download whose torrent the backend did not
    /// restore would be aged out from under the user.
    #[tokio::test]
    async fn protected_paths_cover_a_dormant_pins_download_folder() {
        let (enginefs, _counters) = test_enginefs_with_file_count(1);
        let downloads = enginefs.download_dir.join("offline");
        enginefs.set_downloads_dir(Some(downloads.clone()));
        std::fs::create_dir_all(&downloads).unwrap();
        std::fs::write(
            enginefs.pinned_downloads_path(),
            serde_json::to_vec(&serde_json::json!({ OTHER_HASH: [0] })).unwrap(),
        )
        .unwrap();
        enginefs.restore_pinned_downloads().await;

        let folder = downloads.join(OTHER_HASH);
        let pieces = crate::piece_store::root_in(&enginefs.download_dir);
        assert!(
            enginefs.protected_paths().await.contains(&folder),
            "the dormant pin's folder is protected"
        );
        assert!(
            enginefs
                .protected_paths()
                .await
                .contains(&pieces.join(OTHER_HASH)),
            "and so are its pieces, once there are any"
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
        let after = enginefs.protected_paths().await;
        assert!(!after.contains(&folder));
        assert!(!after.contains(&pieces.join(OTHER_HASH)));
        assert!(
            after.contains(&pieces.join(TEST_HASH)),
            "the live engine's pieces are protected for as long as it runs"
        );
    }

    /// The cleaner's protected paths are where the files really are: the
    /// backend's `file_path` (its output folder -- `<root>/<torrent name>`
    /// for a multi-file torrent in the cache root, `<downloadsDir>/<hash>`
    /// for a placed one), not `<root>/<relative name>`, which for a
    /// multi-file torrent names a file that does not exist while the real
    /// one goes unprotected. The engine's piece-store directory comes first
    /// whatever the backend reports, since the store's placement is this
    /// layer's own and not the backend's.
    #[tokio::test]
    async fn protected_paths_follow_the_backend_output_folder() {
        let (enginefs, counters) = test_enginefs_with_file_count(2);
        let root = enginefs.download_dir.clone();
        let pieces = crate::piece_store::root_in(&root).join(TEST_HASH);

        // Backend without a folder or path: the historical root join.
        assert_eq!(
            enginefs.protected_paths().await,
            vec![
                pieces.clone(),
                root.join("video-0.mkv"),
                root.join("video-1.mkv")
            ]
        );

        let show = root.join("show");
        *counters.output_folder.lock().unwrap() = Some(show.clone());
        assert_eq!(
            enginefs.protected_paths().await,
            vec![
                pieces.clone(),
                show.join("video-0.mkv"),
                show.join("video-1.mkv")
            ]
        );

        let placed = std::path::PathBuf::from("/offline").join(TEST_HASH);
        *counters.output_folder.lock().unwrap() = Some(placed.clone());
        assert_eq!(
            enginefs.protected_paths().await,
            vec![
                pieces,
                placed.join("video-0.mkv"),
                placed.join("video-1.mkv")
            ]
        );
    }

    // --- the free-space watch ---

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

    /// The whole point: a torrent that is writing is stopped when the volume
    /// falls under the floor, before the filesystem stops it with ENOSPC and
    /// librqbit declares it dead -- and it is listed for the cleaner, which
    /// is what makes room for it. Stopped once, not once per tick; started
    /// again by the watch only once the volume is a margin over the floor,
    /// so it does not flap at the line.
    #[tokio::test]
    async fn the_watch_stops_a_writing_torrent_under_the_floor_and_resumes_it_over_the_margin() {
        let (mut enginefs, counters) = test_enginefs_with_file_count(1);
        let available = Arc::new(AtomicU64::new(CACHE_FREE_SPACE_FLOOR));
        let probe_available = available.clone();
        enginefs.set_free_space_probe(move |_| Ok(probe_available.load(Ordering::SeqCst)));
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        // At the floor: fine.
        enginefs.free_space_watch_tick().await;
        assert_eq!(counters.stop_for_space.load(Ordering::SeqCst), 0);
        assert!(!engine.is_stopped_for_space());

        // A byte under it: stopped, once, and the cleaner's business now.
        available.store(CACHE_FREE_SPACE_FLOOR - 1, Ordering::SeqCst);
        enginefs.free_space_watch_tick().await;
        enginefs.free_space_watch_tick().await;
        assert_eq!(counters.stop_for_space.load(Ordering::SeqCst), 1);
        assert!(engine.is_stopped_for_space());
        assert_eq!(
            enginefs.out_of_space_torrents().await,
            vec![TEST_HASH.to_string()]
        );
        assert!(!engine.reads_refused(), "its readers wait for the cleaner");

        // Back over the floor but inside the margin: still stopped.
        available.store(
            CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN - 1,
            Ordering::SeqCst,
        );
        enginefs.free_space_watch_tick().await;
        assert!(engine.is_stopped_for_space());
        assert_eq!(counters.restart_after_error.load(Ordering::SeqCst), 0);

        // The margin over: started again, and off the cleaner's list.
        available.store(
            CACHE_FREE_SPACE_FLOOR + FREE_SPACE_RESUME_MARGIN,
            Ordering::SeqCst,
        );
        enginefs.free_space_watch_tick().await;
        assert_eq!(counters.restart_after_error.load(Ordering::SeqCst), 1);
        assert!(!engine.is_stopped_for_space());
        assert!(enginefs.out_of_space_torrents().await.is_empty());
    }

    /// The watch stops writers. A torrent the idle policy paused, one that
    /// has everything it wants, one the backend already stopped with an
    /// error: none of them is writing, and stopping them would only cost
    /// peers (and, for the finished one, its seeding) for nothing.
    #[tokio::test]
    async fn the_watch_leaves_alone_what_writes_nothing() {
        let (mut enginefs, counters) = test_enginefs_with_file_count(1);
        enginefs.set_free_space_probe(|_| Ok(0));
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        engine.idle_paused.store(true, Ordering::SeqCst);
        enginefs.free_space_watch_tick().await;
        engine.idle_paused.store(false, Ordering::SeqCst);

        counters.seeded.store(true, Ordering::SeqCst);
        enginefs.free_space_watch_tick().await;
        counters.seeded.store(false, Ordering::SeqCst);

        counters.in_error_state.store(true, Ordering::SeqCst);
        enginefs.free_space_watch_tick().await;
        counters.in_error_state.store(false, Ordering::SeqCst);

        assert_eq!(counters.stop_for_space.load(Ordering::SeqCst), 0);
        assert!(!engine.is_stopped_for_space());

        // And a volume it cannot read is not a full one.
        enginefs.set_free_space_probe(|_| Err(std::io::Error::other("no statvfs here")));
        enginefs.free_space_watch_tick().await;
        assert_eq!(counters.stop_for_space.load(Ordering::SeqCst), 0);

        // Whereas the same torrent, writing, is stopped -- pinned or not
        // (the pin is accepted while there is room, as a pin is, and the
        // volume fills under it).
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.free_space_watch_tick().await;
        assert_eq!(counters.stop_for_space.load(Ordering::SeqCst), 1);
        assert!(engine.is_stopped_for_space());
    }

    /// A stop rings the cleaner, and the cleaner's own restart is the other
    /// way a stopped torrent comes back: `restart_after_error` clears the
    /// stop whatever the volume reads, since the cleaner has just made the
    /// room it is restarting into.
    #[tokio::test]
    async fn a_stop_for_space_rings_the_cleaner_and_its_restart_clears_the_stop() {
        let (mut enginefs, counters) = test_enginefs_with_file_count(1);
        enginefs.set_free_space_probe(|_| Ok(0));
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();

        let rung = tokio::time::timeout(Duration::from_millis(10), enginefs.out_of_space_signal());
        assert!(rung.await.is_err(), "nothing has been stopped yet");

        enginefs.free_space_watch_tick().await;
        tokio::time::timeout(TEST_WAIT_BOUND, enginefs.out_of_space_signal())
            .await
            .expect("the stop rang the cleaner");
        assert!(engine.is_stopped_for_space());

        assert!(enginefs.restart_after_error(TEST_HASH).await.unwrap());
        assert_eq!(counters.restart_after_error.load(Ordering::SeqCst), 1);
        assert!(!engine.is_stopped_for_space());
        assert!(enginefs.out_of_space_torrents().await.is_empty());
    }

    /// To the backend a torrent the watch stopped is merely paused, which
    /// the client would show as buffering for ever; the statistics say what
    /// is actually wrong, in the field a torrent error has always used, and
    /// stop saying it when the torrent is back.
    #[tokio::test]
    async fn a_torrent_stopped_for_space_reports_it_in_its_statistics() {
        let (mut enginefs, _counters) = test_enginefs_with_file_count(1);
        enginefs.set_free_space_probe(|_| Ok(0));
        let engine = enginefs.get_engine(TEST_HASH).await.unwrap();
        assert_eq!(engine.get_statistics().await.error, None);

        enginefs.free_space_watch_tick().await;
        let stats = engine.get_statistics().await;
        assert_eq!(stats.phase, StartupPhase::Error);
        assert_eq!(
            stats.error.as_deref(),
            Some(crate::engine::STOPPED_FOR_SPACE_MESSAGE)
        );

        enginefs.restart_after_error(TEST_HASH).await.unwrap();
        let stats = engine.get_statistics().await;
        assert_ne!(stats.phase, StartupPhase::Error);
        assert_eq!(stats.error, None);
    }

    /// A torrent stopped for space is neither protected nor the cleaner's to
    /// unlink: it is listed whole, so the cleaner can take it through the
    /// engine when nothing else can go. Stopped by the watch or by librqbit's
    /// ENOSPC alike; a pinned one stays protected however it stopped.
    #[tokio::test]
    async fn a_torrent_stopped_for_space_is_listed_whole_for_the_cleaner() {
        let (mut enginefs, counters) = test_enginefs_with_file_count(2);
        let root = enginefs.download_dir.clone();
        let files = vec![
            crate::piece_store::root_in(&root).join(TEST_HASH),
            root.join("video-0.mkv"),
            root.join("video-1.mkv"),
        ];
        let whole = StoppedTorrent {
            info_hash: TEST_HASH.to_string(),
            paths: files.clone(),
        };

        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.free_space_watch_tick().await;
        let classes = enginefs.eviction_classes().await;
        assert!(classes.protected.is_empty() && classes.dead.is_empty());
        assert_eq!(classes.stopped_for_space, vec![whole.clone()]);
        assert!(enginefs.protected_paths().await.is_empty());

        // librqbit's own ENOSPC stop reads the same.
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.free_space_watch_tick().await;
        assert!(
            enginefs
                .eviction_classes()
                .await
                .stopped_for_space
                .is_empty()
        );
        counters.out_of_space.store(true, Ordering::SeqCst);
        assert_eq!(
            enginefs.eviction_classes().await.stopped_for_space,
            vec![whole]
        );

        // The pin outranks the stop.
        enginefs.pin_download(TEST_HASH, 0, None).await.unwrap();
        let classes = enginefs.eviction_classes().await;
        assert_eq!(classes.protected, files);
        assert!(classes.stopped_for_space.is_empty());
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
        enginefs.free_space_watch_tick().await;
        assert!(engine.is_stopped_for_space());
        assert!(!enginefs.evict_stopped_torrent(TEST_HASH).await.unwrap());
        enginefs.unpin_download(TEST_HASH, 0, false).await.unwrap();
        assert!(
            engine.is_stopped_for_space(),
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
            !enginefs.restart_after_error(TEST_HASH).await.unwrap(),
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
        assert!(!readded.is_stopped_for_space() && !readded.reads_refused());
    }

    /// A read parked on a piece a stopped torrent will not download is a
    /// player spinning for ever. Refusing reads wakes the parked one to
    /// fail with `StorageFull`, fails a new one at its first poll, and the
    /// watch does the refusing itself once a torrent has been stopped for
    /// [`STOPPED_READ_STALL_BOUND`] -- the bound for a cleaner that is not
    /// there to settle it sooner.
    #[tokio::test(start_paused = true)]
    async fn readers_of_a_torrent_stopped_for_space_are_failed_rather_than_parked() {
        use tokio::io::{AsyncRead, AsyncReadExt};
        let (mut enginefs, _counters) = test_enginefs_with_file_count(1);
        enginefs.set_free_space_probe(|_| Ok(0));
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
        enginefs.free_space_watch_tick().await;
        assert!(engine.is_stopped_for_space());
        let waiting = tokio::spawn(async move {
            let mut buf = [0u8; 16];
            let result = reader.read(&mut buf).await;
            (reader, result)
        });
        tokio::time::advance(STOPPED_READ_STALL_BOUND - Duration::from_secs(1)).await;
        enginefs.free_space_watch_tick().await;
        assert!(!engine.reads_refused());
        assert!(!waiting.is_finished(), "still parked inside the bound");

        // The bound passed: the watch fails the readers, and the parked
        // read is woken to see it.
        tokio::time::advance(Duration::from_secs(1)).await;
        enginefs.free_space_watch_tick().await;
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
        enginefs.set_free_space_probe(|_| Ok(u64::MAX));
        enginefs.free_space_watch_tick().await;
        assert!(!engine.is_stopped_for_space() && !engine.reads_refused());
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
        let root = fake_engine_root();
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

    /// The other order: the pin's add is the one in flight and a stream
    /// request joins *it*. The torrent then sits exactly where a pin places
    /// one, and a teardown keyed on the folder alone removed it from under
    /// the stream -- with its files -- the moment the pin was refused.
    /// Whoever joined the add holds the engine, so the refusal leaves the
    /// torrent for the idle sweeper, as it does in the cache root.
    #[tokio::test]
    async fn refused_pin_leaves_the_torrent_a_stream_joined_its_add_for() {
        let (mut enginefs, _counters) = test_enginefs_unmanaged();
        enginefs.set_downloads_dir(Some("/offline".into()));
        enginefs.set_free_space_probe(|_| Ok(0));
        enginefs.backend.hold_add.store(true, Ordering::SeqCst);
        let folder = enginefs
            .download_folder(TEST_HASH)
            .expect("downloads dir set");

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
                "one add, the pin's, placed under the downloads dir"
            );
            enginefs.backend.add_hold.add_permits(1);
            joined.done.await.expect("the add itself succeeds")
        };
        let (result, streamed) = tokio::join!(enginefs.pin_download(TEST_HASH, 0, None), stream);
        assert!(matches!(
            result,
            Err(PinDownloadError::InsufficientSpace { .. })
        ));
        assert_eq!(
            streamed.handle.output_folder(),
            Some(folder),
            "the engine the stream holds is the pin-placed one"
        );
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

        // Nobody joined: the same refusal takes the torrent with it, so a
        // refused pin does not leave a placeholder tree under the downloads
        // dir either.
        enginefs.remove_engine(TEST_HASH).await;
        enginefs.backend.hold_add.store(false, Ordering::SeqCst);
        assert!(matches!(
            enginefs.pin_download(TEST_HASH, 0, None).await,
            Err(PinDownloadError::InsufficientSpace { .. })
        ));
        assert!(enginefs.get_engine(TEST_HASH).await.is_none());
        assert_eq!(
            enginefs
                .backend
                .removed_with_files
                .lock()
                .unwrap()
                .as_slice(),
            &[TEST_HASH.to_string()]
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
        // A torrent's file order is the filesystem's readdir order, not the
        // order the fixture wrote the files in: look the indices up. (Both
        // files are whole pieces, so neither order shares a boundary piece.)
        let e1 = crate::backend::librqbit::torrent_file_index(&bytes, "e1.bin");
        let e2 = crate::backend::librqbit::torrent_file_index(&bytes, "e2.bin");
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
            .pin_download(&hash, e1, None)
            .await
            .expect("a complete file in place needs no space");
        assert_eq!(engine.handle.output_folder(), Some(folder.clone()));
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
        let offline = tmp.path().join("offline");
        let folder = offline.join(&hash);
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
        enginefs.set_downloads_dir(Some(offline.clone()));
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

        let pieces = crate::piece_store::root_in(&enginefs.download_dir);
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

        let pieces = crate::piece_store::root_in(&enginefs.download_dir);
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
            *counters.dropped_file_pieces.lock().unwrap(),
            vec![0],
            "and the backend is told to forget the file's pieces"
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
        insert_active_lease(&enginefs, 0).await;
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
            !enginefs
                .active_playback_leases
                .read()
                .await
                .contains_key(&key),
            "its playback lease goes with it"
        );
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
            // The pin holds the per-hash lock twice over (the `Arc` it
            // took and the owned guard it can hand to a relocation) and the
            // map holds it once; a fourth reference means the unpin has
            // taken it too and is parked on it -- exactly the interleaving
            // under test.
            assert!(
                wait_until(TEST_WAIT_BOUND, || {
                    enginefs
                        .pin_locks
                        .lock()
                        .get(TEST_HASH)
                        .is_some_and(|lock| Arc::strong_count(lock) >= 4)
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

    /// A dormant pin (no torrent in the backend) with no downloads dir has
    /// nothing on disk this layer can name -- the torrent lived in the
    /// cache root under a folder named by metadata the pin does not have.
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
    /// `<downloadsDir>/<info hash>` is one this layer named itself, so it
    /// goes at once rather than waiting on the cleaner's age rule, which
    /// `protected_paths` holds off for as long as the pin stands -- and the
    /// entry leaves `downloads.json` with the pin, so no client could ask
    /// again either. The folder stays while another file of the same torrent
    /// is still pinned: it holds that file too.
    #[tokio::test]
    async fn unpin_download_of_a_dormant_pin_deletes_its_download_folder() {
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
        let offline = root.path().join("offline");
        enginefs.set_downloads_dir(Some(offline.clone()));
        let folder = offline.join(TEST_HASH);
        std::fs::create_dir_all(&folder).unwrap();
        for idx in [1, 2] {
            std::fs::write(folder.join(format!("video-{idx}.mkv")), [7u8; 4096]).unwrap();
        }

        std::fs::write(
            enginefs.pinned_downloads_path(),
            serde_json::to_vec(&serde_json::json!({ TEST_HASH: [1, 2] })).unwrap(),
        )
        .unwrap();
        assert_eq!(enginefs.restore_pinned_downloads().await, 0);

        // File 2 of the same torrent is still pinned, and its data is in
        // that folder.
        assert_eq!(
            enginefs.unpin_download(TEST_HASH, 1, true).await.unwrap(),
            UnpinOutcome {
                unpinned: true,
                deleted_files: false,
            }
        );
        assert!(folder.is_dir(), "the other pin's data stays");

        // The last one takes the folder with it.
        assert_eq!(
            enginefs.unpin_download(TEST_HASH, 2, true).await.unwrap(),
            UnpinOutcome {
                unpinned: true,
                deleted_files: true,
            }
        );
        assert!(
            !folder.exists(),
            "the placement folder is not left for a cleaner that never walks it"
        );
        assert!(offline.is_dir(), "only the torrent's own folder goes");
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
