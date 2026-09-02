use crate::engine::Engine;
use anyhow::Result;
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
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
pub mod subtitles;
pub mod tracker_prober;
pub mod trackers;

// Re-export TrackerStorage for use by server crate
pub use trackers::TrackerStorage;

use crate::backend::librqbit::LibrqbitBackend;
use crate::backend::priorities::EngineCacheConfig;

use crate::backend::{
    BackendMemoryDiagnostics, HotFilePriorityPlan, TorrentBackend, TorrentFilePriorityPlan,
    TorrentHandle, TorrentSource,
};

const INACTIVE_TORRENT_REMOVE_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes
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

/// Process-relative clock for the idle bookkeeping. A `tokio::time::Instant`
/// so that `now_secs()` follows paused/advanced time under
/// `#[tokio::test(start_paused = true)]` (it is the std clock otherwise).
static START_TIME: OnceLock<tokio::time::Instant> = OnceLock::new();

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
    /// The add task ended abnormally (panicked) before the backend answered.
    #[error("magnet add task for {info_hash} failed: {reason}")]
    TaskFailed { info_hash: String, reason: String },
    /// The backend's `add_torrent` itself failed.
    #[error("{error:#}")]
    Backend {
        info_hash: String,
        error: Arc<anyhow::Error>,
    },
}

/// Outcome of a magnet add shared between every waiter.
pub type MagnetAddResult<H> = Result<Arc<Engine<H>>, MagnetAddError>;

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
    /// panic) and nothing has retried it since. Only the blocking
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
    /// `now_secs()` of the last lookup that returned this entry; the eviction
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

pub fn now_secs() -> u64 {
    START_TIME
        .get_or_init(tokio::time::Instant::now)
        .elapsed()
        .as_secs()
}

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
        let mut engines_map = HashMap::new();
        for (hash, handle) in restored_handles {
            engines_map.insert(
                hash.clone(),
                Arc::new(Engine::new_with_handle(handle, &hash)),
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
        tokio::spawn(async move {
            loop {
                // Run fairly frequently so seeding stops promptly after the
                // user disables it; torrent removal is still gated by the much
                // longer inactivity timeout below, so this only changes how
                // quickly the seeding-disabled pause reacts.
                tokio::time::sleep(Duration::from_secs(15)).await;
                let mut to_remove = Vec::new();
                let now = now_secs();

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

                        let skip_reason = if engine_active_streams > 0 {
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
    ) -> Arc<Engine<B::Handle>> {
        let info_hash = handle.info_hash();
        let mut engines = engines.write().await;
        if let Some(engine) = engines.get(&info_hash) {
            engine.touch();
            return engine.clone();
        }
        let engine = Arc::new(Engine::new_with_handle(handle, &info_hash));
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
        Ok(Self::register_engine(&self.engines, handle).await)
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
        self.lookup_or_begin_add_magnet(info_hash, extra_trackers, false)
            .await
    }

    /// [`Self::get_or_begin_add_magnet`], waiting for an in-flight add and
    /// retrying a failed one.
    pub async fn get_or_add_magnet(
        &self,
        info_hash: &str,
        extra_trackers: Option<Vec<String>>,
    ) -> Result<Arc<Engine<B::Handle>>, MagnetAddError> {
        match self
            .lookup_or_begin_add_magnet(info_hash, extra_trackers, true)
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
        let now = now_secs();
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
            info_hash.clone(),
            trackers,
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
    /// backend error or timeout (entry becomes its failure record), panic or
    /// abort (likewise) -- without depending on any waiter polling `done`.
    /// A stats poller that never awaits therefore still sees the failure.
    fn spawn_magnet_add(
        backend: Arc<B>,
        engines: EngineRegistry<B::Handle>,
        adds: MagnetAddRegistry<B::Handle>,
        info_hash: String,
        trackers: Vec<String>,
    ) -> PendingMagnetAdd<B::Handle> {
        static NEXT_ADD_ID: AtomicU64 = AtomicU64::new(1);
        let id = NEXT_ADD_ID.fetch_add(1, Ordering::Relaxed);
        let trackers: Arc<[String]> = trackers.into();

        let add = {
            let hash = info_hash.clone();
            let trackers = trackers.clone();
            tokio::spawn(async move {
                let source = TorrentSource::Url(format!("magnet:?xt=urn:btih:{hash}"));
                let add = backend.add_torrent(source, trackers.to_vec());
                match tokio::time::timeout(METADATA_RESOLVE_TIMEOUT, add).await {
                    Ok(Ok(handle)) => Ok(Self::register_engine(&engines, handle).await),
                    Ok(Err(error)) => Err(MagnetAddError::Backend {
                        info_hash: hash,
                        error: Arc::new(error),
                    }),
                    Err(_elapsed) => Err(MagnetAddError::MetadataTimeout {
                        info_hash: hash,
                        timeout: METADATA_RESOLVE_TIMEOUT,
                    }),
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
        entry.touch(now_secs());
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
        let now = now_secs();
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
            uptime_secs: now_secs(),
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
        let now = now_secs();
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

    /// Refresh a lease only if playback is already known to be active. This is
    /// used by stats.json so a progress poll cannot create a new download.
    pub async fn refresh_existing_hls_playback(
        &self,
        info_hash: &str,
        file_idx: usize,
        source: &'static str,
    ) -> bool {
        let info_hash = info_hash.to_lowercase();
        let now = now_secs();
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
                let now = now_secs();
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
                let now = now_secs();
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
        let (backend, restored) = LibrqbitBackend::new(download_dir.clone()).await?;
        Ok(Self::new_with_backend(
            backend,
            restored,
            root_dir.join("cache"),
            download_dir,
        ))
    }

    pub async fn new_with_storage(
        root_dir: std::path::PathBuf,
        _config: crate::backend::BackendConfig,
        tracker_storage: Option<Arc<dyn crate::trackers::TrackerStorage>>,
    ) -> Result<Self> {
        let download_dir = root_dir.join("rqbit-downloads");
        let (backend, restored) = LibrqbitBackend::new(download_dir.clone()).await?;
        Ok(Self::new_with_backend_and_storage(
            backend,
            restored,
            root_dir.join("cache"),
            download_dir,
            tracker_storage,
        ))
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
        handle: FakeHandle,
    }

    #[async_trait::async_trait]
    impl TorrentBackend for FakeBackend {
        type Handle = FakeHandle;

        async fn add_torrent(
            &self,
            _source: TorrentSource,
            _trackers: Vec<String>,
        ) -> Result<Self::Handle> {
            Ok(self.handle.clone())
        }

        async fn get_torrent(&self, info_hash: &str) -> Option<Self::Handle> {
            (info_hash == self.handle.info_hash).then(|| self.handle.clone())
        }

        async fn remove_torrent(&self, _info_hash: &str) -> Result<()> {
            Ok(())
        }

        async fn list_torrents(&self) -> Vec<String> {
            vec![self.handle.info_hash.clone()]
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
            let mut offset = 0u64;
            let files = self
                .files
                .iter()
                .map(|file| {
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
            }
        }

        async fn add_trackers(&self, _trackers: Vec<String>) -> Result<()> {
            Ok(())
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

        async fn get_file_path(&self, _file_idx: usize) -> Option<String> {
            None
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
            FakeBackend { handle },
            restored,
            root.join("cache"),
            root.join("downloads"),
        );
        (enginefs, counters, init)
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

        // The Option-returning wrapper degrades to None (and logs), no panic.
        assert!(
            engine
                .get_file_with_intent(1, 0, 1, PlaybackIntent::DirectInitial)
                .await
                .is_none()
        );
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
        let now = now_secs();
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
                .expires_at_secs = now_secs();
        }
        let cleanup = enginefs
            .schedule_file_cleanup_after(TEST_HASH.to_string(), 0, Duration::from_millis(10))
            .await
            .expect("cleanup task");
        cleanup.await.expect("cleanup task completed");

        assert_eq!(counters.clear_file_streaming.load(Ordering::SeqCst), 1);
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

        async fn remove_torrent(&self, _info_hash: &str) -> Result<()> {
            Ok(())
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
        _root: tempfile::TempDir,
    }

    impl Gated {
        fn adds(&self) -> usize {
            self.adds.load(Ordering::SeqCst)
        }

        fn set_behaviour(&self, behaviour: AddBehaviour) {
            *self.behaviour.lock().unwrap() = behaviour;
        }
    }

    fn gated_enginefs() -> Gated {
        let release = Arc::new(tokio::sync::Notify::new());
        let adds = Arc::new(AtomicUsize::new(0));
        let behaviour = Arc::new(Mutex::new(AddBehaviour::WaitForRelease));
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

    /// A backend add that panics must not leave the hash stuck in
    /// `resolvingMetadata` forever: the waiter gets a typed error, the
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
