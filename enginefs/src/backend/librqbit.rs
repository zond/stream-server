use crate::backend::{
    BackendFileInfo, BackendMemoryDiagnostics, EngineStats, FileStreamTrait, Growler, PeerSearch,
    PieceReadiness, StatsFile, StatsOptions, SwarmCap, TorrentBackend, TorrentFilePriorityPlan,
    TorrentHandle, TorrentSource,
};
use anyhow::{Context, Result};
use librqbit::{ManagedTorrent, ManagedTorrentState, Session};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Ports tried in order for librqbit's incoming BitTorrent listener. Mirrors
/// the pre-9.0.1 `listen_port_range: 42000..42010` fallback (ListenerOptions
/// now binds a single address, so the fallback is done in `new()`).
const LISTEN_PORT_RANGE: std::ops::Range<u16> = 42000..42010;

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

pub struct LibrqbitBackend {
    pub session: Arc<Session>,
    download_dir: PathBuf,
    deferred_selections: DeferredSelections,
}

impl LibrqbitBackend {
    pub async fn new(download_dir: PathBuf) -> Result<(Self, HashMap<String, LibrqbitHandle>)> {
        tokio::fs::create_dir_all(&download_dir).await?;
        debug!(path = ?download_dir, "Storing downloads");

        // librqbit 9.0.1's ListenerOptions binds a single address instead of
        // the old `listen_port_range: 42000..42010`, so preserve the previous
        // port-fallback ourselves: try each port in the range and keep the
        // first that binds. This matters when several sessions coexist (e.g.
        // concurrent embed tests, or a second local instance).
        let session = {
            let mut last_err = None;
            let mut session = None;
            for port in LISTEN_PORT_RANGE {
                let session_opts = librqbit::SessionOptions {
                    listen: Some(librqbit::ListenerOptions {
                        listen_addr: (std::net::Ipv6Addr::UNSPECIFIED, port).into(),
                        enable_upnp_port_forwarding: true,
                        ..Default::default()
                    }),
                    persistence: Some(librqbit::SessionPersistenceConfig::Json {
                        folder: Some(download_dir.clone()),
                    }),
                    // Pin the DHT routing-table dump next to the session
                    // state. librqbit's default resolves through
                    // `directories::ProjectDirs` (HOME/XDG), which has no
                    // answer on Android and would fail `Session::new`.
                    dht: Some(librqbit::DhtSessionConfig {
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
                        anyhow::anyhow!("no librqbit listen port available in range")
                    }));
                }
            }
        };
        let deferred_selections: DeferredSelections = Default::default();
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
    /// Exclusive selection of one file for streaming.
    Prepare(usize),
    /// Deselect one file, keeping the rest of the selection.
    Clear(usize),
    /// Want exactly the union of the active and hot files.
    Reconcile {
        active: Option<usize>,
        hot: Option<usize>,
    },
}

/// Pure planner mapping the current `only_files` selection and an operation to
/// the new selection to apply. `None` means "apply nothing".
///
/// Invariants enforced here (unit-tested):
/// - Single-file torrents are always fully wanted: never touch selection.
/// - The result is never an empty set (that would make nothing wanted and
///   starve playback).
/// - `Clear` of a file that is not currently selected (a newer `Prepare`
///   already switched away) is a no-op, so late delayed-cleanup and HLS-lease
///   expiry cannot clobber the active selection.
/// - Out-of-range indices are dropped; a plan left empty by that is a no-op.
fn plan_only_files(
    current: Option<&[usize]>,
    file_count: usize,
    op: SelectionOp,
) -> Option<HashSet<usize>> {
    if file_count <= 1 {
        return None;
    }
    match op {
        SelectionOp::Prepare(idx) => {
            if idx >= file_count {
                return None;
            }
            Some(std::iter::once(idx).collect())
        }
        SelectionOp::Clear(idx) => {
            let current = current?;
            if !current.contains(&idx) {
                return None;
            }
            let remainder: HashSet<usize> = current
                .iter()
                .copied()
                .filter(|i| *i != idx && *i < file_count)
                .collect();
            if remainder.is_empty() {
                None
            } else {
                Some(remainder)
            }
        }
        SelectionOp::Reconcile { active, hot } => {
            let set: HashSet<usize> = active
                .into_iter()
                .chain(hot)
                .filter(|i| *i < file_count)
                .collect();
            if set.is_empty() { None } else { Some(set) }
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
}

#[async_trait::async_trait]
impl TorrentBackend for LibrqbitBackend {
    type Handle = LibrqbitHandle;

    async fn add_torrent(
        &self,
        source: TorrentSource,
        trackers: Vec<String>,
    ) -> Result<Self::Handle> {
        let add_torrent = match source {
            TorrentSource::Url(url) => librqbit::AddTorrent::Url(url.into()),
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
        })
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
        })
    }

    async fn remove_torrent(&self, info_hash: &str) -> Result<()> {
        let id = librqbit::api::TorrentIdOrHash::parse(info_hash)
            .with_context(|| format!("invalid info hash {info_hash}"))?;
        // delete_files=false keeps downloaded data on disk, matching the
        // libtorrent backend's remove_torrent(handle, false).
        self.session
            .delete(id, false)
            .await
            .with_context(|| format!("failed to remove torrent {info_hash}"))?;
        self.deferred_selections.lock().remove(info_hash);
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

        let (peers, queued, unique) = stats
            .live
            .as_ref()
            .map(|l| {
                (
                    l.snapshot.peer_stats.live as u64,
                    l.snapshot.peer_stats.queued as u64,
                    l.snapshot.peer_stats.seen as u64,
                )
            })
            .unwrap_or((0, 0, 0));

        let has_metadata = self.handle.metadata.load().is_some();

        let mut files = Vec::new();
        let mut total_size = 0u64;
        let mut offset = 0u64;
        if let Some(m) = self.handle.metadata.load_full() {
            for (i, f) in m.info.iter_file_details().enumerate() {
                let filename = f.filename.to_string();
                // file_progress is empty while the torrent is Initializing.
                let have = stats.file_progress.get(i).copied().unwrap_or(0);
                let (file_downloaded, file_progress) = file_progress_fields(f.len, have);
                files.push(StatsFile {
                    name: filename.clone(),
                    path: filename,
                    length: f.len,
                    offset,
                    downloaded: file_downloaded,
                    progress: file_progress,
                });
                total_size += f.len;
                offset += f.len;
            }
        }

        EngineStats {
            name: self.name().unwrap_or_else(|| "Unknown".to_string()),
            info_hash: self.info_hash(),
            files,
            sources: vec![],
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
            is_finished: stats.finished,
            has_metadata,
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

    async fn add_trackers(&self, _trackers: Vec<String>) -> Result<()> {
        Ok(())
    }

    async fn get_file_reader(
        &self,
        file_idx: usize,
        _start_offset: u64,
        _priority: u8,
        _bitrate: Option<u64>,
        intent: crate::backend::priorities::PlaybackIntent,
    ) -> Result<Box<dyn FileStreamTrait>> {
        // librqbit's FileStream requires the Paused or Live state; opening it
        // while the torrent is still Initializing fails immediately, which the
        // HTTP route would turn into a failed first play. Block here instead.
        self.await_initialized().await?;
        // Size the per-stream lookahead window by playback intent instead of
        // librqbit's fixed 32 MiB default: a narrow startup window verifies the
        // head pieces faster, while seeks/sequential get generous read-ahead.
        let opts = librqbit::FileStreamOptions {
            lookahead_bytes: crate::backend::priorities::librqbit_stream_lookahead_bytes(intent),
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

    async fn get_file_path(&self, _file_idx: usize) -> Option<String> {
        // A real path is not obtainable through librqbit 8.1.1's public API:
        // the torrent's resolved output folder lives in ManagedTorrentOptions,
        // which is pub(crate). Returning None makes the engine probe through
        // the HTTP loopback stream instead, which blocks correctly on pieces
        // that are not downloaded yet (a sparse local file would not).
        None
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
    /// (sized by `intent` via `librqbit_stream_lookahead_bytes`, matching the
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
            lookahead_bytes: crate::backend::priorities::librqbit_stream_lookahead_bytes(intent),
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

impl LibrqbitHandle {
    /// File count from resolved metadata; None while a magnet is resolving.
    fn file_count_from_metadata(&self) -> Option<usize> {
        self.handle.metadata.load_full().map(|m| m.file_infos.len())
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
        let Some(set) = plan_only_files(current.as_deref(), file_count, op) else {
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::TorrentBackend;

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
        use crate::backend::priorities::PlaybackIntent;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let payload = dir.join("payload.bin");
        write_payload(&payload, 96 * 1024).await;
        let (torrent_bytes, _hash) = make_torrent(&payload).await;
        let (_backend, handle) = backend_with_torrent(&dir, &torrent_bytes).await;
        handle.handle.wait_until_initialized().await.unwrap();

        let r = handle
            .wait_for_piece_ready(0, 0, Duration::from_secs(5), PlaybackIntent::DirectInitial)
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
                Duration::from_secs(5),
                PlaybackIntent::DirectSeek,
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
        use crate::backend::priorities::PlaybackIntent;
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
                crate::backend::priorities::librqbit_stream_lookahead_bytes(intent) > 0,
                "lookahead must be positive for {intent:?}"
            );
            let mut reader = handle
                .get_file_reader(0, 0, 100, None, intent)
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
        use crate::backend::priorities::PlaybackIntent;
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
            .wait_for_piece_ready(0, 0, timeout, PlaybackIntent::DirectInitial)
            .await
            .unwrap();
        assert!(!r.ready);
        assert_eq!(r.reason, "timeout");
        assert!(r.elapsed_ms >= 300, "elapsed_ms = {}", r.elapsed_ms);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wait_for_piece_ready_rejects_bad_targets() {
        use crate::backend::TorrentHandle;
        use crate::backend::priorities::PlaybackIntent;
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
            )
            .await
            .unwrap();
        assert!(!r.ready);
        assert_eq!(r.reason, "piece-out-of-file-range");

        // Bad file index: structural failure -> Err.
        assert!(
            handle
                .wait_for_piece_ready(7, 0, Duration::from_secs(1), PlaybackIntent::DirectInitial)
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
        use crate::backend::priorities::PlaybackIntent;
        let magnet = std::env::var("STREAM_SERVER_TEST_MAGNET")
            .expect("set STREAM_SERVER_TEST_MAGNET to a magnet link");
        let tmp = tempfile::tempdir().unwrap();
        let (backend, _restored) = LibrqbitBackend::new(tmp.path().to_path_buf())
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
        use crate::backend::priorities::PlaybackIntent;
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
            .get_file_reader(1, 0, 1, None, PlaybackIntent::DirectInitial)
            .await
            .expect("reader opens after initialization");
        let mut buf = [0u8; 1];
        assert_eq!(reader.read(&mut buf).await.unwrap(), 1);
        assert_eq!(buf[0], 0);

        // A deferred reconcile that raced initialization must settle to the
        // latest selection state, never leave a stale parked op behind.
        let slot = handle.deferred_selection();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
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
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while handle.handle.only_files() != Some(vec![0]) && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(handle.handle.only_files(), Some(vec![0]));
        assert!(!handle.deferred_selection().has_pending());
    }

    #[test]
    fn plan_only_files_rules() {
        use SelectionOp::*;
        let plan = plan_only_files;
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
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multifile_selection_lifecycle() {
        use crate::backend::priorities::PlaybackIntent;
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
            .wait_for_piece_ready(0, 0, Duration::from_secs(5), PlaybackIntent::DirectInitial)
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
