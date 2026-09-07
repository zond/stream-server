use crate::routes::compat;
use crate::routes::util::parse_range;
use crate::state::AppState;
use axum::{
    body::Body,
    extract::{Path, RawQuery, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};

use axum::http::HeaderMap;
use enginefs::EngineFS;
use enginefs::backend::librqbit::{LibrqbitHandle, TorrentInitError};
use enginefs::backend::{
    HotFilePriorityPlan, TorrentHandle,
    priorities::{BufferProfile, PlaybackIntent},
};
use enginefs::engine::{Engine, GetFileError};
use futures_util::Stream;
use std::path::Path as FsPath;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);

/// How a response body finished, for the stream-end log line.
///
/// A capture of four failing streams had nothing in it about why any of
/// them stopped. The distinction that mattered was invisible: four bodies
/// died after about ten seconds having delivered ~4 MiB of a multi-gigabyte
/// range, i.e. the player hung up, not the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamOutcome {
    /// The body was dropped before the range was delivered: the player
    /// disconnected, or the request was cancelled.
    ClientDisconnect,
    /// The whole requested range was delivered.
    Complete,
    /// The reader failed part-way (see the `error` field).
    ReaderError,
}

impl StreamOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::ClientDisconnect => "client-disconnect",
            Self::Complete => "complete",
            Self::ReaderError => "reader-error",
        }
    }
}

/// What a response body delivered, accumulated as it is polled.
#[derive(Debug, Default)]
struct BodyProgress {
    bytes_sent: u64,
    /// `None` until the body ends by itself; a body dropped before that is
    /// a player that hung up.
    outcome: Option<StreamOutcome>,
    error: Option<String>,
}

impl BodyProgress {
    fn record_chunk(&mut self, len: usize) {
        self.bytes_sent = self.bytes_sent.saturating_add(len as u64);
    }

    /// The reader failed. First error wins: what broke the stream is more
    /// use than whatever the stream said on its way out.
    fn record_error(&mut self, error: &std::io::Error) {
        if self.outcome.is_none() {
            self.outcome = Some(StreamOutcome::ReaderError);
            self.error = Some(error.to_string());
        }
    }

    /// The reader ran out, which for a `take`-limited body means the whole
    /// requested range was delivered.
    fn record_end(&mut self) {
        self.outcome.get_or_insert(StreamOutcome::Complete);
    }

    fn outcome(&self) -> StreamOutcome {
        self.outcome.unwrap_or(StreamOutcome::ClientDisconnect)
    }
}

/// How often an open stream reports what the torrent is doing.
///
/// Once a second, and only while a body is open: the owner's capture of
/// four stalling streams had nothing between "response ready" and silence,
/// so there was no way to tell a dead swarm from a slow one, or either from
/// a window waiting on a piece bigger than itself.
const STREAM_PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(1);

/// Log what the torrent behind an open stream is doing, once a second,
/// until the task is aborted (the body ended) or the engine is gone.
///
/// `peek_engine`, not `get_engine`: watching a stream must not be what
/// keeps its torrent out of the idle sweep -- the open reader already does
/// that, honestly.
fn spawn_stream_progress_log(
    engine: Arc<enginefs::EngineFS>,
    info_hash: String,
    file_idx: usize,
    stream_id: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(STREAM_PROGRESS_LOG_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick is immediate; skip it so a short request does not
        // log a line saying nothing has happened yet.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let Some(engine) = engine.peek_engine(&info_hash).await else {
                return;
            };
            let mut stats = engine.get_statistics().await;
            stats.focus_stream_file(file_idx);
            tracing::info!(
                stream_id,
                info_hash = %info_hash,
                file_idx,
                phase = ?stats.phase,
                download_speed = stats.download_speed,
                peers = stats.peers,
                connected_seeders = stats.connected_seeders,
                swarm_seeders = stats.swarm_seeders,
                initial_window_ready_bytes = stats.initial_window_ready_bytes,
                initial_window_bytes = stats.initial_window_bytes,
                piece_length = stats.piece_length,
                stage = "stream_progress",
                "stream progress"
            );
        }
    })
}

/// Guard that calls on_stream_end when dropped.
struct StreamLifecycleGuard {
    engine: Arc<enginefs::EngineFS>,
    info_hash: String,
    file_idx: usize,
    stream_id: u64,
    notified: bool,
    started: Instant,
    /// Bytes the range asked for; 0 until the body is built.
    requested_len: u64,
    progress: BodyProgress,
    /// The per-second progress logger, aborted when the stream ends.
    progress_log: tokio::task::JoinHandle<()>,
}

impl StreamLifecycleGuard {
    fn new(
        engine: Arc<enginefs::EngineFS>,
        info_hash: String,
        file_idx: usize,
        stream_id: u64,
    ) -> Self {
        crate::diagnostics::logging::direct_stream_started();
        let progress_log =
            spawn_stream_progress_log(engine.clone(), info_hash.clone(), file_idx, stream_id);
        Self {
            engine,
            info_hash,
            file_idx,
            stream_id,
            notified: false,
            started: Instant::now(),
            requested_len: 0,
            progress: BodyProgress::default(),
            progress_log,
        }
    }

    fn notify_end(&mut self) {
        if self.notified {
            return;
        }
        self.notified = true;
        self.progress_log.abort();
        crate::diagnostics::logging::direct_stream_ended();

        let engine = self.engine.clone();
        let info_hash = self.info_hash.clone();
        let file_idx = self.file_idx;
        let stream_id = self.stream_id;

        // INFO, not DEBUG: this is the only record that a playback session
        // ended and the only one that says how much of what was asked for
        // actually left the server.
        tracing::info!(
            stream_id,
            info_hash = %info_hash,
            file_idx,
            bytes_sent = self.progress.bytes_sent,
            requested_len = self.requested_len,
            duration_ms = self.started.elapsed().as_millis() as u64,
            reason = self.progress.outcome().as_str(),
            error = self.progress.error.as_deref().unwrap_or(""),
            stage = "http_stream_end",
            "stream ended"
        );

        tokio::spawn(async move {
            engine.on_stream_end(&info_hash, file_idx).await;
            tracing::debug!(
                stream_id,
                info_hash = %info_hash,
                file_idx,
                "Stream lifecycle guard notified stream end"
            );
        });
    }
}

impl Drop for StreamLifecycleGuard {
    fn drop(&mut self) {
        self.notify_end();
    }
}

/// Guard that keeps the stream lifecycle alive for the response body, and
/// counts what the body delivered so the stream-end line can say how a
/// playback session actually finished.
struct StreamGuard<S> {
    inner: S,
    lifecycle: StreamLifecycleGuard,
}

impl<S> Stream for StreamGuard<S>
where
    S: Stream<Item = std::io::Result<bytes::Bytes>> + Unpin,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let polled = Pin::new(&mut self.inner).poll_next(cx);
        match &polled {
            Poll::Ready(Some(Ok(chunk))) => self.lifecycle.progress.record_chunk(chunk.len()),
            Poll::Ready(Some(Err(error))) => self.lifecycle.progress.record_error(error),
            Poll::Ready(None) => self.lifecycle.progress.record_end(),
            Poll::Pending => {}
        }
        polled
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// Counts a request as active while its magnet metadata and automatic file
/// selection are being resolved. A normal `FileHandle` takes over this count
/// once streaming begins.
struct MetadataResolutionGuard {
    active_readers: Arc<AtomicUsize>,
}

impl MetadataResolutionGuard {
    async fn acquire<H: TorrentHandle>(engine: &Arc<enginefs::engine::Engine<H>>) -> Self {
        let active_readers = engine.active_streams.clone();
        active_readers.fetch_add(1, Ordering::SeqCst);
        let guard = Self { active_readers };

        engine.touch();
        if !engine.handle.manages_playback_lifecycle() && engine.idle_paused.load(Ordering::SeqCst)
        {
            if let Err(error) = engine.handle.resume_torrent().await {
                tracing::warn!(
                    info_hash = %engine.info_hash,
                    %error,
                    "Failed to resume torrent while resolving stream metadata"
                );
            } else {
                engine.idle_paused.store(false, Ordering::SeqCst);
                tracing::info!(
                    info_hash = %engine.info_hash,
                    "torrent_resumed_for_metadata_resolution"
                );
            }
        }

        guard
    }
}

impl Drop for MetadataResolutionGuard {
    fn drop(&mut self) {
        let previous = self.active_readers.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(previous > 0, "metadata activity counter underflowed");
    }
}

#[derive(Default)]
struct PlaybackQuery {
    download: bool,
    filters: Vec<String>,
    /// `buffer=normal|large|maximum`: this playback's read-ahead choice,
    /// overriding the `bufferProfile` setting for this request only. `None`
    /// when the parameter is absent -- and also when it carries a value this
    /// build does not know, because a player that guessed wrong should get
    /// the server's default rather than a failed stream.
    buffer: Option<BufferProfile>,
}

impl PlaybackQuery {
    fn parse(query: Option<&str>) -> Self {
        let mut parsed = Self::default();
        let Some(query) = query else {
            return parsed;
        };

        for field in query.split('&') {
            let (key, raw_value) = field.split_once('=').unwrap_or((field, ""));
            match key {
                "download" => parsed.download = compat::query_value_is_true(raw_value),
                "buffer" => {
                    // Percent-decoded like `f=`: the values are bare words,
                    // but a client that escapes them is not wrong.
                    let decoded = url::form_urlencoded::parse(field.as_bytes())
                        .next()
                        .map(|(_, value)| value.into_owned())
                        .unwrap_or_else(|| raw_value.to_string());
                    parsed.buffer = BufferProfile::parse(&decoded);
                }
                "f" => {
                    if let Some((_, value)) = url::form_urlencoded::parse(field.as_bytes()).next() {
                        parsed.filters.push(value.into_owned());
                    }
                }
                // Tracker values can be numerous and heavily escaped. They are
                // decoded lazily only if this request must create an engine.
                _ => {}
            }
        }
        parsed
    }
}

fn playback_intent_for_request(
    priority: u8,
    start: u64,
    requested_len: u64,
    file_size: u64,
    is_download: bool,
    is_partial: bool,
) -> PlaybackIntent {
    if priority == 255 {
        return PlaybackIntent::InternalProbe;
    }
    if priority == 0 {
        return PlaybackIntent::Background;
    }
    if is_download && (!is_partial || requested_len >= file_size.saturating_sub(start)) {
        return PlaybackIntent::DownloadFull;
    }
    if is_download && is_partial {
        return PlaybackIntent::DownloadRange;
    }
    if enginefs::backend::priorities::is_container_metadata_request(start, requested_len, file_size)
    {
        return PlaybackIntent::ContainerMetadata;
    }

    if start == 0 {
        PlaybackIntent::DirectInitial
    } else {
        PlaybackIntent::DirectSeek
    }
}

fn content_type_for_name(name: &str) -> &'static str {
    if name.ends_with(".mp4") {
        "video/mp4"
    } else if name.ends_with(".mkv") {
        "video/x-matroska"
    } else if name.ends_with(".ts") {
        "video/mp2t"
    } else if name.ends_with(".avi") {
        "video/x-msvideo"
    } else if name.ends_with(".mov") {
        "video/quicktime"
    } else if name.ends_with(".wmv") {
        "video/x-ms-wmv"
    } else if name.ends_with(".webm") {
        "video/webm"
    } else if name.ends_with(".mp3") {
        "audio/mpeg"
    } else if name.ends_with(".m4a") {
        "audio/mp4"
    } else if name.ends_with(".aac") {
        "audio/aac"
    } else if name.ends_with(".flac") {
        "audio/flac"
    } else if name.ends_with(".wav") {
        "audio/wav"
    } else if name.ends_with(".ogg") {
        "audio/ogg"
    } else if name.ends_with(".opus") {
        "audio/opus"
    } else if name.ends_with(".ac3") {
        "audio/ac3"
    } else if name.ends_with(".eac3") || name.ends_with(".ec3") {
        "audio/eac3"
    } else {
        "application/octet-stream"
    }
}

// Refreshing the full disk list is a relatively expensive syscall sweep over
// every mounted volume. A player fires a burst of probe requests at stream
// start, each of which would otherwise re-run it on a tokio worker thread.
// Free space does not change meaningfully between those, so cache the result
// per root with a short TTL.
type DiskSpaceCache =
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, (Instant, Option<u64>)>>;
static DISK_SPACE_CACHE: std::sync::OnceLock<DiskSpaceCache> = std::sync::OnceLock::new();
const DISK_SPACE_CACHE_TTL: Duration = Duration::from_secs(3);

/// The body of every `507` the stream route answers: a fixed sentence,
/// because the conditions behind it (`ensure_download_disk_ready`'s own
/// message, the engine's reason for a stop) name the cache root, and no
/// response may carry a path.
const INSUFFICIENT_DISK_SPACE_BODY: &str =
    "Insufficient disk space for this stream; free some space and retry";

/// Free space a test has declared for a root and everything under it,
/// standing in for the volume probe -- see [`pretend_available_space`].
type DiskSpaceOverrides = std::sync::Mutex<Vec<(std::path::PathBuf, u64)>>;
static DISK_SPACE_OVERRIDES: std::sync::OnceLock<DiskSpaceOverrides> = std::sync::OnceLock::new();

/// Declare how much free space the volume under `root` has, for every
/// readiness check on a path below it from now on.
///
/// A test seam and nothing else: a volume cannot be filled on demand, so
/// without this the low-disk refusal and the cleaner pass in front of it
/// (`ensure_disk_ready_or_refuse`) could not be exercised end to end. Keyed
/// by root so parallel tests, each with its own temp cache root, never see
/// each other's declaration.
#[doc(hidden)]
pub fn pretend_available_space(root: impl Into<std::path::PathBuf>, bytes: u64) {
    let root = root.into();
    if let Ok(mut overrides) = DISK_SPACE_OVERRIDES
        .get_or_init(|| std::sync::Mutex::new(Vec::new()))
        .lock()
    {
        overrides.retain(|(declared, _)| *declared != root);
        overrides.push((root, bytes));
    }
}

/// Drop the cached free-space reading for `path`, so the next check probes
/// the volume again -- for the check that follows a cache-cleaner pass, which
/// must not be judged by the reading taken before it.
fn forget_available_space(path: &FsPath) {
    if let Some(cache) = DISK_SPACE_CACHE.get()
        && let Ok(mut map) = cache.lock()
    {
        map.remove(path);
    }
}

fn available_space_for_path(path: &FsPath) -> Option<u64> {
    if let Some(overrides) = DISK_SPACE_OVERRIDES.get()
        && let Ok(overrides) = overrides.lock()
        && let Some((_, bytes)) = overrides.iter().find(|(root, _)| path.starts_with(root))
    {
        return Some(*bytes);
    }
    let cache =
        DISK_SPACE_CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    if let Ok(map) = cache.lock()
        && let Some((at, value)) = map.get(path)
        && at.elapsed() < DISK_SPACE_CACHE_TTL
    {
        return *value;
    }

    let value = available_space_for_path_uncached(path);
    if let Ok(mut map) = cache.lock() {
        map.insert(path.to_path_buf(), (Instant::now(), value));
    }
    value
}

fn available_space_for_path_uncached(path: &FsPath) -> Option<u64> {
    let disks = sysinfo::Disks::new_with_refreshed_list();
    let mut best_match_len = 0usize;
    let mut best_available = None;

    for disk in disks.list() {
        let mount = disk.mount_point();
        if path.starts_with(mount) {
            let len = mount.as_os_str().len();
            if len >= best_match_len {
                best_match_len = len;
                best_available = Some(disk.available_space());
            }
        }
    }

    best_available
}

fn ensure_download_disk_ready(
    root: &FsPath,
    file_name: &str,
    file_size: u64,
    requested_len: u64,
    is_partial: bool,
) -> Result<(), String> {
    std::fs::create_dir_all(root).map_err(|e| {
        format!(
            "download cache path is not writable: {} ({})",
            root.display(),
            e
        )
    })?;

    let probe_path = root.join(".write-test");
    std::fs::write(&probe_path, b"ok").map_err(|e| {
        format!(
            "download cache path is not writable: {} ({})",
            root.display(),
            e
        )
    })?;
    let _ = std::fs::remove_file(&probe_path);

    let existing_len = root
        .join(file_name)
        .metadata()
        .map(|metadata| metadata.len().min(file_size))
        .unwrap_or(0);
    let remaining = file_size.saturating_sub(existing_len);
    // The same floor the cache cleaner keeps free (`CacheLimit::effective`):
    // below it this check gives up on the disk, so it is the line the cleaner
    // must keep the cache out of.
    let safety_margin = crate::cache_cleaner::CACHE_FREE_SPACE_FLOOR;
    let required = if is_partial {
        requested_len.min(safety_margin)
    } else {
        remaining.saturating_add(safety_margin)
    };

    let available = available_space_for_path(root).ok_or_else(|| {
        format!(
            "could not determine available disk space for download cache: {}",
            root.display()
        )
    })?;

    if available < required {
        return Err(format!(
            "insufficient download cache space: available={} required={} root={}",
            available,
            required,
            root.display()
        ));
    }

    Ok(())
}

fn disk_space_check_treats_as_partial(is_download: bool, is_partial: bool) -> bool {
    !is_download || is_partial
}

/// The readiness check the stream route runs before it streams to disk, and
/// what happens when the disk is not ready: one cache-cleaner pass, the check
/// again, and a `507` if the disk is still short. `Err` is the status and
/// body to answer.
///
/// This used to "degrade the request to memory-only" by re-selecting
/// `state.engine` -- which is the same `Arc<EngineFS>` as
/// `state.download_engine`: `run()` builds one engine and puts it in both
/// fields, librqbit sessions always persist to disk, and there is no
/// memory-only storage anywhere in this server. So the fallback re-fetched
/// the same torrent from the same engine, labelled the log
/// `memoryOnlyLowDiskFallback`, and streamed to the disk the check had just
/// refused; the floor degraded nothing and the torrent ran on until
/// librqbit's ENOSPC fatal error, exactly as if the check did not exist.
///
/// Refusing at once is not right either, on the device the floor is about.
/// The cleaner is what keeps the floor free, by evicting stale cache, but it
/// runs debounced after filesystem events and hourly otherwise, and a
/// refused request writes nothing to arm it with -- a phone with 300 MiB
/// free and gigabytes of old cache would sit refused until the hourly pass.
/// So a failed check runs the cleaner now (`cache_cleaner::clean_cache`, the
/// same pass `POST /cache/clean` takes, with the same protections) and asks
/// again with a fresh free-space reading; only a disk still short after that
/// is answered `507 Insufficient Storage` -- the status the pin route already
/// uses for a full disk -- with a fixed body, because the check's own message
/// names the cache root and no response may carry a path.
#[allow(clippy::too_many_arguments)]
async fn ensure_disk_ready_or_refuse(
    state: &AppState,
    engine_fs: &EngineFS,
    stream_id: u64,
    info_hash: &str,
    file_idx: usize,
    file_name: &str,
    file_size: u64,
    requested_len: u64,
    treat_as_partial: bool,
) -> Result<(), (StatusCode, &'static str)> {
    // The check performs blocking std::fs syscalls (create_dir_all, a write
    // probe, and a metadata stat) that are re-run on every request and every
    // seek. Run them on the blocking pool so they never stall an async worker
    // thread inline -- which would also block any other stream/API task
    // scheduled on that same worker -- e.g. when the cache lives on a
    // spun-down HDD or a slow network/SMB mount.
    let check = || {
        let download_dir = engine_fs.download_dir.clone();
        let file_name = file_name.to_string();
        async move {
            tokio::task::spawn_blocking(move || {
                ensure_download_disk_ready(
                    &download_dir,
                    &file_name,
                    file_size,
                    requested_len,
                    treat_as_partial,
                )
            })
            .await
            .unwrap_or_else(|join_err| Err(format!("disk readiness check task failed: {join_err}")))
        }
    };
    let Err(first) = check().await else {
        return Ok(());
    };
    tracing::warn!(
        stream_id,
        info_hash = %info_hash,
        file_idx,
        error = %first,
        "disk not ready for this stream; running a cache-cleaner pass before giving up on it"
    );
    match crate::cache_cleaner::clean_cache(state).await {
        Ok(report) => tracing::info!(
            stream_id,
            freed = report.freed,
            deleted = report.deleted,
            "cache-cleaner pass on behalf of a stream request"
        ),
        Err(error) => tracing::warn!(
            stream_id,
            error = %format!("{error:#}"),
            "cache-cleaner pass on behalf of a stream request failed"
        ),
    }
    // The free-space reading is cached for a few seconds against a player's
    // burst of probes; a pass that just freed space must not be judged by
    // the reading taken before it.
    forget_available_space(&engine_fs.download_dir);
    match check().await {
        Ok(()) => Ok(()),
        Err(error) => {
            tracing::warn!(
                stream_id,
                info_hash = %info_hash,
                file_idx,
                error = %error,
                "disk still not ready after a cache-cleaner pass; refusing the stream"
            );
            Err((
                StatusCode::INSUFFICIENT_STORAGE,
                INSUFFICIENT_DISK_SPACE_BODY,
            ))
        }
    }
}

/// How a stream request may come by its torrent -- the one thing that
/// differs between the two listeners serving these handlers.
///
/// On loopback a stream URL is how stremio-core starts a torrent at all:
/// the first `GET /{infoHash}/{fileIdx}` for a hash creates the engine,
/// with the request's `tr=` trackers, and everything downstream (stats
/// polls, the pin route) is built on that. The LAN media listener exists
/// for a receiver to fetch what this device is already playing, and a
/// receiver has no business naming torrents: with the creating behaviour
/// mounted there, anyone on the network could start a download, with their
/// own trackers, on this device's disk and connection, unauthenticated.
/// The LAN variant therefore answers only for a torrent the server already
/// has and ignores `tr=` outright -- trackers only count on creation, and
/// it never creates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EngineAccess {
    /// An existing engine, or one created from the info hash with the
    /// request's trackers -- the loopback listener.
    CreateIfMissing,
    /// An existing engine or nothing -- the LAN media listener. An unknown
    /// hash is a `404`, and no magnet add is started or joined.
    ExistingOnly,
}

/// The engine a stream request is about, by [`EngineAccess`]; the error is
/// the status and body to answer instead.
async fn engine_for_request(
    engine_fs: &EngineFS,
    access: EngineAccess,
    info_hash: &str,
    query_str: Option<&str>,
    handler: &'static str,
) -> Result<Arc<Engine<LibrqbitHandle>>, (StatusCode, String)> {
    match access {
        EngineAccess::CreateIfMissing => {
            compat::get_or_create_engine(engine_fs, info_hash, query_str)
                .await
                .map_err(|error| {
                    tracing::error!(info_hash, error = %error, "{handler} failed to create engine");
                    compat::engine_creation_failure(&error)
                })
        }
        EngineAccess::ExistingOnly => engine_fs.get_engine(info_hash).await.ok_or_else(|| {
            tracing::info!(
                info_hash,
                "{handler}: LAN media request for a torrent this server does not have"
            );
            (StatusCode::NOT_FOUND, "Unknown torrent".to_string())
        }),
    }
}

pub async fn head_stream_video(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((info_hash, requested_idx)): Path<(String, String)>,
    RawQuery(query_str): RawQuery,
) -> Response {
    head_stream_video_with(
        state,
        headers,
        info_hash,
        requested_idx,
        query_str,
        EngineAccess::CreateIfMissing,
    )
    .await
}

/// [`head_stream_video`] as the LAN media listener mounts it: existing
/// torrents only (see [`EngineAccess::ExistingOnly`]).
pub async fn lan_head_stream_video(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((info_hash, requested_idx)): Path<(String, String)>,
    RawQuery(query_str): RawQuery,
) -> Response {
    head_stream_video_with(
        state,
        headers,
        info_hash,
        requested_idx,
        query_str,
        EngineAccess::ExistingOnly,
    )
    .await
}

async fn head_stream_video_with(
    state: AppState,
    headers: HeaderMap,
    info_hash: String,
    requested_idx: String,
    query_str: Option<String>,
    access: EngineAccess,
) -> Response {
    let request_start = Instant::now();
    let info_hash = info_hash.to_lowercase();
    let query = PlaybackQuery::parse(query_str.as_deref());
    let is_download = query.download;
    let engine_fs = state.stream_engine();

    let engine = match engine_for_request(
        &engine_fs,
        access,
        &info_hash,
        query_str.as_deref(),
        "head_stream_video",
    )
    .await
    {
        Ok(engine) => engine,
        Err(refusal) => return refusal.into_response(),
    };

    let _metadata_resolution = MetadataResolutionGuard::acquire(&engine).await;
    let files = engine.handle.get_files().await;
    let candidates = files
        .iter()
        .enumerate()
        .map(|(index, file)| compat::FileCandidate {
            index,
            name: file.name.clone(),
            length: file.length,
        })
        .collect::<Vec<_>>();
    let idx = match compat::resolve_file_idx(&requested_idx, &candidates, &query.filters) {
        Ok(idx) => idx,
        Err(err) => {
            tracing::warn!(
                info_hash = %info_hash,
                requested_idx = %requested_idx,
                error = %err,
                "head_stream_video could not resolve file index"
            );
            return (StatusCode::NOT_FOUND, err).into_response();
        }
    };
    let Some(file_info) = files.get(idx) else {
        return (StatusCode::NOT_FOUND, "File not found").into_response();
    };
    let size = file_info.length;
    let name = &file_info.name;

    let range_header = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let (start, end, is_partial) = if let Some(range) = &range_header {
        if let Some((start, end)) = parse_range(range, size) {
            (start, end, true)
        } else {
            return (StatusCode::RANGE_NOT_SATISFIABLE, "Range Not Satisfiable").into_response();
        }
    } else {
        (0, size.saturating_sub(1), false)
    };

    let content_length = end.saturating_sub(start) + 1;
    let mut res_headers = header::HeaderMap::new();
    res_headers.insert(
        header::CONTENT_TYPE,
        content_type_for_name(name).parse().unwrap(),
    );
    res_headers.insert(header::CONTENT_LENGTH, content_length.into());
    res_headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
    if is_partial {
        res_headers.insert(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end, size).parse().unwrap(),
        );
    }
    if is_download {
        res_headers.insert(
            header::CONTENT_DISPOSITION,
            compat::content_disposition_attachment(name),
        );
    }
    compat::add_dlna_headers(&mut res_headers);

    tracing::debug!(
        "head_stream_video: Responded in {:?} for {} idx={} range {}-{}",
        request_start.elapsed(),
        info_hash,
        idx,
        start,
        end
    );

    if is_partial {
        (StatusCode::PARTIAL_CONTENT, res_headers, Body::empty()).into_response()
    } else {
        (StatusCode::OK, res_headers, Body::empty()).into_response()
    }
}

pub async fn stream_video(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((info_hash, requested_idx)): Path<(String, String)>,
    RawQuery(query_str): RawQuery,
) -> Response {
    stream_video_with(
        state,
        headers,
        info_hash,
        requested_idx,
        query_str,
        EngineAccess::CreateIfMissing,
    )
    .await
}

/// [`stream_video`] as the LAN media listener mounts it: existing torrents
/// only (see [`EngineAccess::ExistingOnly`]).
pub async fn lan_stream_video(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((info_hash, requested_idx)): Path<(String, String)>,
    RawQuery(query_str): RawQuery,
) -> Response {
    stream_video_with(
        state,
        headers,
        info_hash,
        requested_idx,
        query_str,
        EngineAccess::ExistingOnly,
    )
    .await
}

async fn stream_video_with(
    state: AppState,
    headers: HeaderMap,
    info_hash: String,
    requested_idx: String,
    query_str: Option<String>,
    access: EngineAccess,
) -> Response {
    let request_start = Instant::now();
    let info_hash = info_hash.to_lowercase();
    let stream_id = NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed);
    let query = PlaybackQuery::parse(query_str.as_deref());
    let is_download = query.download;
    let engine_fs = state.stream_engine();

    tracing::debug!(
        stream_id,
        info_hash = %info_hash,
        file_idx = %requested_idx,
        "stream_video request"
    );

    // Existing engine, or -- on loopback -- one created from the info hash
    // with the request's trackers.
    let engine = match engine_for_request(
        &engine_fs,
        access,
        &info_hash,
        query_str.as_deref(),
        "stream_video",
    )
    .await
    {
        Ok(engine) => {
            tracing::debug!(stream_id, "stream_video engine ready");
            engine
        }
        Err(refusal) => return refusal.into_response(),
    };

    // A torrent the free-space watch has stopped is not downloading, so a
    // reader opened on it would park on its first missing piece for as long
    // as it stays stopped -- a player buffering with no end. The engine has
    // already said what is wrong with the disk; say it to the client, with
    // the body the free-space check uses for the same condition. The cache
    // cleaner either restarts the torrent (and the next request streams) or
    // evicts it (and the next request meets `MagnetAddError::EvictedForSpace`,
    // the same `507`, until the cooling-off period ends).
    if engine.is_stopped_for_space() {
        tracing::warn!(
            stream_id,
            info_hash = %info_hash,
            "stream_video: the torrent is stopped for want of disk space; refusing the stream"
        );
        return (
            StatusCode::INSUFFICIENT_STORAGE,
            INSUFFICIENT_DISK_SPACE_BODY,
        )
            .into_response();
    }

    let _metadata_resolution = MetadataResolutionGuard::acquire(&engine).await;
    let files = engine.handle.get_files().await;
    let candidates = files
        .iter()
        .enumerate()
        .map(|(index, file)| compat::FileCandidate {
            index,
            name: file.name.clone(),
            length: file.length,
        })
        .collect::<Vec<_>>();
    let idx = match compat::resolve_file_idx(&requested_idx, &candidates, &query.filters) {
        Ok(idx) => idx,
        Err(err) => {
            tracing::warn!(
                stream_id,
                info_hash = %info_hash,
                requested_idx = %requested_idx,
                error = %err,
                "stream_video could not resolve file index"
            );
            return (StatusCode::NOT_FOUND, err).into_response();
        }
    };
    let Some(file_info) = files.get(idx) else {
        tracing::warn!(
            stream_id,
            info_hash = %info_hash,
            file_idx = idx,
            "stream_video file index not found before stream start"
        );
        return (StatusCode::NOT_FOUND, "File not found").into_response();
    };
    let size = file_info.length;
    let name = file_info.name.clone();

    let range_header = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let (start, end, is_partial) = if let Some(range) = &range_header {
        if let Some((start, end)) = parse_range(range, size) {
            (start, end, true)
        } else {
            tracing::warn!(
                stream_id,
                info_hash = %info_hash,
                file_idx = idx,
                range = %range,
                "stream_video invalid range header"
            );
            return (StatusCode::RANGE_NOT_SATISFIABLE, "Range Not Satisfiable").into_response();
        }
    } else {
        (0, size.saturating_sub(1), false)
    };
    let requested_content_length = if size == 0 {
        0
    } else {
        end.saturating_sub(start) + 1
    };
    if let Err(refusal) = ensure_disk_ready_or_refuse(
        &state,
        &engine_fs,
        stream_id,
        &info_hash,
        idx,
        &name,
        size,
        requested_content_length,
        disk_space_check_treats_as_partial(is_download, is_partial),
    )
    .await
    {
        return refusal.into_response();
    }
    let start_offset_hint = start;
    // Parse priority from enginefs-prio header
    let priority: u8 = if let Some(prio_val) = headers.get("enginefs-prio") {
        prio_val.to_str().unwrap_or("1").parse().unwrap_or(1)
    } else {
        1
    };
    let playback_intent = playback_intent_for_request(
        priority,
        start,
        requested_content_length,
        size,
        is_download,
        is_partial,
    );
    // The request's own `buffer=` wins; anything else (absent, or a value this
    // build does not know) falls back to the server-wide default.
    let buffer_profile = match query.buffer {
        Some(profile) => profile,
        None => state.settings.read().await.buffer_profile,
    };
    let native_lifecycle = engine.handle.manages_playback_lifecycle();
    if !is_download && !is_partial && start == 0 {
        tracing::info!(
            stream_id,
            info_hash = %info_hash,
            file_idx = idx,
            intent = ?playback_intent,
            "playback request without Range; treating as direct initial, not full download"
        );
    }

    // --- Stream Lifecycle: Notify start only after validation has succeeded. ---
    engine_fs.on_stream_start(&info_hash, idx).await;
    if !native_lifecycle {
        engine_fs
            .activate_multifile_file_for_playback(
                &info_hash,
                idx,
                Some(HotFilePriorityPlan {
                    file_idx: idx,
                    start_offset: start_offset_hint,
                    priority,
                    intent: playback_intent,
                    bitrate_bytes_per_sec: None,
                }),
                "stream-read",
            )
            .await;
    }
    let lifecycle = StreamLifecycleGuard::new(engine_fs.clone(), info_hash.clone(), idx, stream_id);
    if !native_lifecycle {
        engine_fs.focus_torrent(&info_hash).await;
    }

    // Await the async get_file
    tracing::debug!(
        stream_id,
        info_hash = %info_hash,
        file_idx = idx,
        start_offset = start_offset_hint,
        priority,
        intent = ?playback_intent,
        buffer = buffer_profile.as_str(),
        "stream_video calling get_file"
    );
    let mut file = match engine
        .try_get_file_with_intent(
            idx,
            start_offset_hint,
            priority,
            playback_intent,
            buffer_profile,
        )
        .await
    {
        Ok(file) => file,
        Err(err) => return stream_open_failure_response(stream_id, &info_hash, idx, err),
    };
    tracing::debug!(
        stream_id,
        info_hash = %info_hash,
        file_idx = idx,
        size = file.size,
        "stream_video get_file returned success"
    );
    let size = file.size;
    let name = if file.name.is_empty() {
        name
    } else {
        file.name.clone()
    };

    tracing::debug!(
        stream_id,
        info_hash = %info_hash,
        file_idx = idx,
        "stream_video range request: {}-{} (total {})",
        start,
        end,
        size
    );

    if start >= size {
        tracing::warn!(
            stream_id,
            info_hash = %info_hash,
            file_idx = idx,
            start,
            size,
            "stream_video range not satisfiable"
        );
        return (StatusCode::RANGE_NOT_SATISFIABLE, "Range Not Satisfiable").into_response();
    }

    // Seek to the start position
    if start > 0 {
        tracing::debug!(
            stream_id,
            info_hash = %info_hash,
            file_idx = idx,
            start,
            "stream_video seeking"
        );
        if let Err(e) = file.seek(std::io::SeekFrom::Start(start)).await {
            tracing::warn!(
                stream_id,
                info_hash = %info_hash,
                file_idx = idx,
                error = %e,
                "stream_video seek error"
            );
            return (StatusCode::INTERNAL_SERVER_ERROR, "Seek failed").into_response();
        }
        tracing::debug!(stream_id, "stream_video seek complete");
    }

    let content_length = requested_content_length;
    let mut res_headers = header::HeaderMap::new();

    let mime = content_type_for_name(&name);

    // Log detected file type
    tracing::debug!(
        stream_id,
        info_hash = %info_hash,
        file_idx = idx,
        content_type = mime,
        file_name = %name,
        "media file detected"
    );

    res_headers.insert(header::CONTENT_TYPE, mime.parse().unwrap());

    res_headers.insert(header::CONTENT_LENGTH, content_length.into());
    res_headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
    if is_partial {
        res_headers.insert(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end, size).parse().unwrap(),
        );
    }
    if is_download {
        res_headers.insert(
            header::CONTENT_DISPOSITION,
            compat::content_disposition_attachment(&name),
        );
    }
    compat::add_dlna_headers(&mut res_headers);

    // Limit the body to the requested range while the reader waits asynchronously
    // for torrent pieces that have not arrived yet.
    let reader = file.take(content_length);

    // Use ReaderStream to convert AsyncRead to Stream for Axum Body
    // OPTIMIZATION: Use 256KB buffer for improved throughput with large pieces
    // Larger buffer = fewer poll_read calls = less priority calculation overhead
    let base_stream = tokio_util::io::ReaderStream::with_capacity(reader, 262144);

    // Wrap with StreamGuard to notify when stream ends
    let mut lifecycle = lifecycle;
    lifecycle.requested_len = content_length;
    let guarded_stream = StreamGuard {
        inner: base_stream,
        lifecycle,
    };
    let body = Body::from_stream(guarded_stream);

    let response_elapsed_ms = request_start.elapsed().as_millis() as u64;
    if response_elapsed_ms >= 100 {
        tracing::info!(
            stream_id,
            info_hash = %info_hash,
            file_idx = idx,
            elapsed_ms = response_elapsed_ms,
            range_start = start,
            range_end = end,
            partial = is_partial,
            stage = "http_response_ready",
            "startup: direct stream response ready"
        );
    } else {
        tracing::debug!(
            stream_id,
            info_hash = %info_hash,
            file_idx = idx,
            elapsed_ms = response_elapsed_ms,
            range_start = start,
            range_end = end,
            partial = is_partial,
            stage = "http_response_ready",
            "startup: direct stream response ready"
        );
    }

    if is_partial {
        (StatusCode::PARTIAL_CONTENT, res_headers, body).into_response()
    } else {
        (StatusCode::OK, res_headers, body).into_response()
    }
}

/// Map a failed `try_get_file_with_intent` to the HTTP response for a stream
/// request. The backend already blocked (bounded) for the torrent to become
/// streamable, so reaching this means either a bad file index, a torrent that
/// never left its initializing state, or a genuine reader failure.
fn stream_open_failure_response(
    stream_id: u64,
    info_hash: &str,
    file_idx: usize,
    err: GetFileError,
) -> Response {
    let (status, message) = stream_open_failure_status(&err);
    tracing::warn!(
        stream_id,
        info_hash = %info_hash,
        file_idx,
        status = status.as_u16(),
        error = %err,
        "stream_video could not open the file reader after stream start"
    );
    (status, message).into_response()
}

/// Pure status/message mapping for `stream_open_failure_response`. The 502
/// and 500 bodies are fixed, non-leaky strings on purpose: `reason` (from
/// librqbit) and `{err:#}` can carry absolute download-dir paths, which must
/// not be echoed to an HTTP client. The full detail still reaches the logs --
/// `stream_open_failure_response` logs `err` at `warn` for every branch.
fn stream_open_failure_status(err: &GetFileError) -> (StatusCode, String) {
    match err {
        GetFileError::FileNotFound { .. } => (StatusCode::NOT_FOUND, "File not found".to_string()),
        GetFileError::Backend(_) => match err.torrent_init_error() {
            Some(TorrentInitError::TimedOut { timeout_secs, .. }) => (
                StatusCode::GATEWAY_TIMEOUT,
                format!("Torrent is still initializing after {timeout_secs}s; retry shortly"),
            ),
            Some(TorrentInitError::Failed { .. }) => (
                StatusCode::BAD_GATEWAY,
                "Torrent failed to initialize; see server logs for details".to_string(),
            ),
            None => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to open stream reader; see server logs for details".to_string(),
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_open_failure_maps_to_clear_non_2xx_statuses() {
        let (status, msg) = stream_open_failure_status(&GetFileError::FileNotFound {
            file_idx: 3,
            file_count: 2,
        });
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(msg, "File not found");

        let timed_out = GetFileError::Backend(
            anyhow::Error::new(TorrentInitError::TimedOut {
                info_hash: "abc".into(),
                timeout_secs: 60,
            })
            .context("prepare_file_for_streaming"),
        );
        let (status, msg) = stream_open_failure_status(&timed_out);
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert!(msg.contains("still initializing after 60s"), "{msg}");

        let failed = GetFileError::Backend(anyhow::Error::new(TorrentInitError::Failed {
            info_hash: "abc".into(),
            reason: "disk exploded".into(),
        }));
        let (status, msg) = stream_open_failure_status(&failed);
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        // The 502 body must not leak librqbit's `reason` (which can carry an
        // absolute download-dir path) to the HTTP client.
        assert!(
            !msg.contains("disk exploded"),
            "502 body must not echo the backend error detail: {msg}"
        );

        let other = GetFileError::Backend(anyhow::anyhow!("boom").context("get_file_reader"));
        let (status, msg) = stream_open_failure_status(&other);
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        // Same for the 500 body and `{err:#}`.
        assert!(
            !msg.contains("boom"),
            "500 body must not echo the backend error detail: {msg}"
        );
    }

    #[test]
    fn metadata_resolution_guard_balances_the_active_reader_count() {
        let active_readers = Arc::new(AtomicUsize::new(0));
        {
            let _guard = MetadataResolutionGuard {
                active_readers: active_readers.clone(),
            };
            active_readers.fetch_add(1, Ordering::SeqCst);
            assert_eq!(active_readers.load(Ordering::SeqCst), 1);
        }
        assert_eq!(active_readers.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn accepts_small_partial_download_when_cache_root_is_writable() {
        let temp = tempfile::tempdir().expect("temp dir");
        ensure_download_disk_ready(temp.path(), "movie.mkv", 10 * 1024 * 1024, 1, true)
            .expect("writable temp dir should pass partial request safety check");
    }

    #[tokio::test]
    async fn readiness_check_runs_off_the_async_worker_via_spawn_blocking() {
        // stream_video now dispatches the blocking fs probes onto the blocking
        // pool rather than running them inline on a runtime worker. Exercise that
        // exact path: the closure must be Send + 'static and produce the same
        // result it would when called directly.
        let temp = tempfile::tempdir().expect("temp dir");
        let root = temp.path().to_path_buf();
        let name = "movie.mkv".to_string();
        let readiness = tokio::task::spawn_blocking(move || {
            ensure_download_disk_ready(&root, &name, 10 * 1024 * 1024, 1, true)
        })
        .await
        .expect("spawn_blocking join should succeed");
        readiness.expect("writable temp dir should pass partial request safety check");
    }

    #[test]
    fn normal_playback_uses_partial_disk_space_policy() {
        assert!(disk_space_check_treats_as_partial(false, false));
        assert!(disk_space_check_treats_as_partial(false, true));
        assert!(disk_space_check_treats_as_partial(true, true));
        assert!(!disk_space_check_treats_as_partial(true, false));
    }

    #[test]
    fn full_download_uses_download_full_intent() {
        assert_eq!(
            playback_intent_for_request(1, 0, 10_000, 10_000, true, false),
            PlaybackIntent::DownloadFull
        );
    }

    #[test]
    fn ranged_download_uses_download_range_intent() {
        assert_eq!(
            playback_intent_for_request(1, 500, 1, 10_000, true, true),
            PlaybackIntent::DownloadRange
        );
    }

    #[test]
    fn full_file_range_download_uses_download_full_intent() {
        assert_eq!(
            playback_intent_for_request(1, 0, 10_000, 10_000, true, true),
            PlaybackIntent::DownloadFull
        );
    }

    #[test]
    fn resumed_full_remaining_download_uses_download_full_intent() {
        assert_eq!(
            playback_intent_for_request(1, 5_000, 5_000, 10_000, true, true),
            PlaybackIntent::DownloadFull
        );
    }

    #[test]
    fn playback_without_range_is_direct_initial_not_download() {
        assert_eq!(
            playback_intent_for_request(1, 0, 10_000, 10_000, false, false),
            PlaybackIntent::DirectInitial
        );
    }

    #[test]
    fn tail_playback_range_is_container_metadata() {
        let file_size = 100 * 1024 * 1024;
        let tail = enginefs::backend::priorities::container_metadata_start(file_size);
        assert_eq!(
            playback_intent_for_request(1, tail, 1024, file_size, false, true),
            PlaybackIntent::ContainerMetadata
        );
    }

    #[test]
    fn large_tail_playback_range_stays_direct_seek() {
        let file_size = 100 * 1024 * 1024;
        let tail = enginefs::backend::priorities::container_metadata_start(file_size);
        assert_eq!(
            playback_intent_for_request(
                1,
                tail,
                enginefs::backend::priorities::MAX_CONTAINER_METADATA_WINDOW_BYTES + 1,
                file_size,
                false,
                true
            ),
            PlaybackIntent::DirectSeek
        );
    }

    #[test]
    fn small_file_non_tail_range_stays_direct_seek() {
        let file_size = 8 * 1024 * 1024;
        assert_eq!(
            playback_intent_for_request(1, 1024 * 1024, 1024, file_size, false, true),
            PlaybackIntent::DirectSeek
        );
    }

    #[test]
    fn playback_query_extracts_controls_without_decoding_trackers() {
        let query = PlaybackQuery::parse(Some(
            "tr=udp%3A%2F%2Fone&tr=https%3A%2F%2Ftwo&f=Episode+02&download=1",
        ));

        assert!(query.download);
        assert_eq!(query.filters, ["Episode 02"]);
        assert_eq!(query.buffer, None);
    }

    #[test]
    fn playback_query_reads_the_buffer_override() {
        for (raw, expected) in [
            ("buffer=normal", BufferProfile::Normal),
            ("buffer=large", BufferProfile::Large),
            ("buffer=maximum", BufferProfile::Maximum),
            // Case and percent-encoding are a client's business, not ours.
            ("buffer=MAXIMUM", BufferProfile::Maximum),
            ("buffer=%20large%20", BufferProfile::Large),
            ("f=Episode+02&buffer=large&download=1", BufferProfile::Large),
        ] {
            assert_eq!(
                PlaybackQuery::parse(Some(raw)).buffer,
                Some(expected),
                "query {raw}"
            );
        }
    }

    #[test]
    fn an_unknown_buffer_value_falls_back_to_the_default() {
        // `None` is what the handler turns into "use the server's
        // bufferProfile setting"; none of these may fail the request.
        for raw in ["buffer=huge", "buffer=", "buffer", "buffer=2", "download=1"] {
            assert_eq!(PlaybackQuery::parse(Some(raw)).buffer, None, "query {raw}");
        }
    }

    /// A capture of four failing streams said nothing about why any of them
    /// stopped. What mattered was the distinction between a body that
    /// delivered its range and one the player hung up on part-way -- so a
    /// body that never ended by itself must read as a disconnect, and the
    /// first error must survive whatever the stream says afterwards.
    #[test]
    fn body_progress_tells_a_hung_up_player_from_a_delivered_range() {
        let mut dropped = BodyProgress::default();
        dropped.record_chunk(4 * 1024 * 1024);
        assert_eq!(dropped.outcome(), StreamOutcome::ClientDisconnect);
        assert_eq!(dropped.bytes_sent, 4 * 1024 * 1024);
        assert_eq!(dropped.error, None);

        let mut delivered = BodyProgress::default();
        delivered.record_chunk(10);
        delivered.record_chunk(20);
        delivered.record_end();
        assert_eq!(delivered.outcome(), StreamOutcome::Complete);
        assert_eq!(delivered.bytes_sent, 30);

        let mut failed = BodyProgress::default();
        failed.record_chunk(7);
        failed.record_error(&std::io::Error::other("piece read failed"));
        // A stream may still report end-of-stream after erroring; the error
        // is what ended it.
        failed.record_end();
        assert_eq!(failed.outcome(), StreamOutcome::ReaderError);
        assert_eq!(failed.bytes_sent, 7);
        assert_eq!(failed.error.as_deref(), Some("piece read failed"));
    }
}
