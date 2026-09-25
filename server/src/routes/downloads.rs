//! Offline downloads: the control routes `POST /{infoHash}/{fileIdx}/download`,
//! `DELETE /{infoHash}/{fileIdx}/download` and `GET /downloads.json`, and the
//! functions they share with the matching `ServerHandle` methods
//! (`pin_download`, `unpin_download`, `downloads`, `download_path`).

use crate::routes::compat;
use crate::state::AppState;
use axum::{
    extract::{Json, Path, RawQuery, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use enginefs::backend::{EngineStats, StartupPhase, TorrentHandle};
use enginefs::{PinDownloadError, UnpinOutcome};
use serde_json::json;
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// One pinned download as the routes and `ServerHandle` report it.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadInfo {
    pub info_hash: String,
    pub file_idx: usize,
    /// What the torrent backend calls this file, when it knows.
    ///
    /// **A name, not a file.** Torrent data is stored one file per piece
    /// (`enginefs::piece_store`), so no whole file is ever produced at this
    /// path and nothing should try to open it; the bytes come out of the
    /// media routes. The folder in it is librqbit's own -- nothing above the
    /// backend chooses one, for a pinned download no more than for a
    /// streamed torrent -- and it is what `download_path` and the listing
    /// agree on.
    pub path: Option<String>,
    pub name: String,
    pub length: u64,
    pub downloaded: u64,
    pub complete: bool,
    /// The torrent's startup phase (`checking` while librqbit verifies what
    /// is already on disk).
    pub phase: StartupPhase,
    /// Why the download is not progressing, when anything knows: the
    /// engine's error for a failed magnet add or a torrent the backend put
    /// in an error state (a client-safe message either way -- the
    /// backend's own text names server paths and stays in the log),
    /// [`DORMANT_DOWNLOAD_ERROR`] for a pin whose torrent the backend does
    /// not have. `null` for a healthy download.
    pub error: Option<String>,
}

/// What a pin whose torrent the backend did not restore reports as its
/// `error` (`enginefs::BackendEngineFS::dormant_pinned_downloads`): the pin
/// is kept, nothing is downloading, and there is nothing for the client to
/// do about it. It used to blame a folder that might be unavailable, from
/// when a pinned download lived in one of its own; a dormant pin is a
/// torrent the session did not bring back, and where its bytes are is not
/// the question.
pub const DORMANT_DOWNLOAD_ERROR: &str = "the torrent is not managed right now; \
     the pin is kept and applies when it comes back";

/// Pin `file_idx` of `info_hash` as an offline download, exactly what
/// `POST /{infoHash}/{fileIdx}/download` will answer: the engine is created
/// through the magnet registry with `trackers` (normalised like the stats
/// routes' `tr=` values) when the hash is new, and the file is kept wanted
/// and exempt from eviction (see
/// `enginefs::BackendEngineFS::pin_download`). **Nothing moves**: a pin is
/// a retention property, and a torrent that was streamed first is pinned
/// where it already is. Refused with
/// [`PinDownloadError::InsufficientSpace`] below the free-space margin.
pub async fn pin_download(
    state: &AppState,
    info_hash: &str,
    file_idx: usize,
    trackers: Vec<String>,
) -> Result<DownloadInfo, PinDownloadError> {
    let info_hash = info_hash.to_lowercase();
    let trackers = compat::normalize_tracker_sources(trackers);
    let engine = state
        .engine
        .pin_download(&info_hash, file_idx, Some(trackers))
        .await?;
    let stats = engine.get_statistics().await;
    let file = stats
        .files
        .get(file_idx)
        .ok_or_else(|| PinDownloadError::FileNotFound {
            file_idx,
            file_count: stats.files.len(),
        })?;
    if !file.complete {
        spawn_download_progress_log(
            Arc::clone(&state.engine),
            engine.info_hash.clone(),
            file_idx,
        );
    }
    Ok(DownloadInfo {
        info_hash: engine.info_hash.clone(),
        file_idx,
        path: engine.handle.get_file_path(file_idx).await,
        name: file.name.clone(),
        length: file.length,
        downloaded: file.downloaded,
        complete: file.complete,
        phase: stats.phase,
        error: stats.error.clone(),
    })
}

/// How often a pinned download that is not finished says where it stands.
///
/// A stream logs `stream progress` every five seconds while a player is
/// reading; a pin had no line at all after `download_pinned`, so a download
/// that had stopped moving looked exactly like one nobody had asked to move
/// -- a file that sat at 54 % for ten minutes with no peer connected was
/// diagnosed from `/proc/net/tcp`. Ten seconds rather than five: nothing is
/// waiting on this line, and a download runs for the length of a film.
const DOWNLOAD_PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// The files with a progress logger running, so that a second pin of the
/// same file -- a retry, the boot's re-pin of an unfinished download -- adds
/// no second line every ten seconds. A slot is taken before the task is
/// spawned and given back when the task ends ([`LoggerSlot`]).
fn progress_loggers() -> &'static Mutex<HashSet<(String, usize)>> {
    static LOGGERS: OnceLock<Mutex<HashSet<(String, usize)>>> = OnceLock::new();
    LOGGERS.get_or_init(Default::default)
}

/// Takes `key`'s slot in [`progress_loggers`], or answers `None` when a
/// logger already holds it. The slot is released when the value drops.
fn claim_progress_logger(key: (String, usize)) -> Option<LoggerSlot> {
    // The guard is dropped before any `LoggerSlot` exists: a slot's drop
    // takes the same lock, and an eager `then_some` here built one for a
    // refused claim too -- dropped under the guard (a deadlock) and
    // removing the holder's key with it.
    let claimed = progress_loggers()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(key.clone());
    claimed.then(|| LoggerSlot(key))
}

struct LoggerSlot((String, usize));

impl Drop for LoggerSlot {
    fn drop(&mut self) {
        progress_loggers()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.0);
    }
}

/// Bytes moved since the previous reading, and how many readings in a row
/// moved nothing: the two numbers that tell a download that is slow from
/// one that has stopped.
#[derive(Debug, Default)]
struct ProgressTrack {
    last: Option<u64>,
    still: u32,
}

impl ProgressTrack {
    /// Answers `(moved, still)`: bytes since the previous reading -- 0 on
    /// the first, which measures nothing -- and the consecutive readings,
    /// this one included, that moved nothing. A count that went *down* (a
    /// piece dropped, a re-check) is not movement either.
    fn observe(&mut self, downloaded: u64) -> (u64, u32) {
        let moved = self.last.map_or(0, |last| downloaded.saturating_sub(last));
        self.still = match self.last {
            Some(_) if moved == 0 => self.still + 1,
            _ => 0,
        };
        self.last = Some(downloaded);
        (moved, self.still)
    }
}

/// One `download progress` line every [`DOWNLOAD_PROGRESS_LOG_INTERVAL`]
/// for `file_idx` of `info_hash`, until the file is complete, the pin is
/// gone or the torrent has left the session -- each of which is said in a
/// last line. Every field the stream line carries and the ones a stall is
/// argued from: bytes moved since the last line, how long nothing has
/// moved, the peers connected against the addresses queued, dialling and
/// known, and the torrent's run state, so "stopped" and "nobody answers"
/// read differently.
fn spawn_download_progress_log(
    engine: Arc<enginefs::EngineFS>,
    info_hash: String,
    file_idx: usize,
) {
    let Some(slot) = claim_progress_logger((info_hash.clone(), file_idx)) else {
        return;
    };
    tokio::spawn(async move {
        let _slot = slot;
        let mut ticker = tokio::time::interval(DOWNLOAD_PROGRESS_LOG_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick is immediate, and `download_pinned` has just said
        // everything there is to say.
        ticker.tick().await;
        let mut track = ProgressTrack::default();
        loop {
            ticker.tick().await;
            let Some(engine) = engine.peek_engine(&info_hash).await else {
                tracing::info!(
                    info_hash = %info_hash,
                    file_idx,
                    stage = "download_progress",
                    "download progress: the torrent has left the session"
                );
                return;
            };
            if !engine.pinned_file_indices().contains(&file_idx) {
                tracing::info!(
                    info_hash = %info_hash,
                    file_idx,
                    stage = "download_progress",
                    "download progress: the file is no longer pinned"
                );
                return;
            }
            let stats = engine.get_statistics().await;
            let Some(file) = stats.files.get(file_idx) else {
                // No file list right now (a hash check in progress, a
                // torrent the session has not brought back): nothing to
                // measure, and the next tick asks again.
                tracing::debug!(
                    info_hash = %info_hash,
                    file_idx,
                    phase = ?stats.phase,
                    "download progress: no file list to read"
                );
                continue;
            };
            let (moved, still) = track.observe(file.downloaded);
            tracing::info!(
                info_hash = %info_hash,
                file_idx,
                run_state = ?engine.handle.run_state(),
                phase = ?stats.phase,
                downloaded = file.downloaded,
                length = file.length,
                moved,
                still_secs = u64::from(still) * DOWNLOAD_PROGRESS_LOG_INTERVAL.as_secs(),
                download_speed = stats.download_speed,
                peers = stats.peers,
                connected_seeders = stats.connected_seeders,
                queued = stats.queued,
                connecting = stats.peer_discovery.connecting,
                unique = stats.unique,
                known = stats.peer_discovery.known,
                swarm_seeders = stats.swarm_seeders,
                error = stats.error.as_deref().unwrap_or(""),
                stage = "download_progress",
                "download progress"
            );
            if file.complete {
                tracing::info!(
                    info_hash = %info_hash,
                    file_idx,
                    length = file.length,
                    stage = "download_progress",
                    "download complete"
                );
                return;
            }
        }
    });
}

/// Drop the pin on `file_idx` of `info_hash`, exactly what
/// `DELETE /{infoHash}/{fileIdx}/download` answers. [`UnpinOutcome`] says
/// whether a pin was actually cleared (false for an unknown torrent or an
/// unpinned file) and whether data actually went -- which is what the
/// response reports, not the request's own flag.
/// `delete_files` also deletes the data -- the whole torrent when this was
/// its last pin, only that file while other pins hold, and, for a pin whose
/// torrent the backend does not have, its directory in the piece store
/// (see `enginefs::BackendEngineFS::unpin_download`); a `file_idx` the
/// torrent does not have is refused with [`PinDownloadError::FileNotFound`]
/// (404), like [`pin_download`]. Without it the bytes stay where they are
/// and the engine becomes an ordinary, evictable one again.
pub async fn unpin_download(
    state: &AppState,
    info_hash: &str,
    file_idx: usize,
    delete_files: bool,
) -> Result<UnpinOutcome, PinDownloadError> {
    state
        .engine
        .unpin_download(&info_hash.to_lowercase(), file_idx, delete_files)
        .await
}

/// Every pinned download, exactly what `GET /downloads.json` answers:
/// ordered by info hash then file index, the live ones first and the
/// dormant ones (torrent not restored, [`DORMANT_DOWNLOAD_ERROR`]) after
/// them. One stats call per torrent, not per file.
///
/// When the embedder named no pin set
/// ([`enginefs::piece_store::PinsUnknown`]) the list is every file of every
/// torrent the session restored, because that is what is being kept, and
/// reporting the empty in-memory pin set instead would tell the caller the
/// downloads are gone while the bytes are still on the disk.
///
/// **One read of the condition, for the whole listing.** Asking again
/// mid-listing is how a response comes back with a file listed twice: once
/// because the set was unknown, once because a pin taken meanwhile names it.
/// A value read once and trusted for the rest of the answer is the shape
/// `PinsUnknown` is documented against; reading it repeatedly inside one
/// answer is that shape, not its cure.
pub async fn downloads(state: &AppState) -> Vec<DownloadInfo> {
    let engine_fs = state.engine.clone();
    let unknown = engine_fs.pins_unknown();
    // A set, not a list: both sources can name the same file, and a
    // `GET /downloads.json` that returns one download twice is one no
    // client can reconcile against its own list.
    let mut by_hash: BTreeMap<String, std::collections::BTreeSet<usize>> = BTreeMap::new();
    if unknown {
        for info_hash in engine_fs.list_engines().await {
            let Some(engine) = engine_fs.get_engine(&info_hash).await else {
                continue;
            };
            let count = engine.handle.file_count().await;
            by_hash.insert(engine.info_hash.clone(), (0..count).collect());
        }
    }
    for pin in engine_fs.pinned_downloads().await {
        by_hash
            .entry(pin.info_hash)
            .or_default()
            .insert(pin.file_idx);
    }
    let mut downloads = Vec::new();
    for (info_hash, file_indices) in by_hash {
        // The pin came from the registry a moment ago; a torrent that left
        // it since (an unpin racing this listing) simply is not listed.
        let Some(engine) = engine_fs.get_engine(&info_hash).await else {
            continue;
        };
        let stats = engine.get_statistics().await;
        for file_idx in file_indices {
            let path = engine.handle.get_file_path(file_idx).await;
            downloads.push(live_download(&info_hash, file_idx, path, &stats));
        }
    }
    downloads.extend(
        engine_fs
            .dormant_pinned_downloads()
            .into_iter()
            .map(|pin| DownloadInfo {
                info_hash: pin.info_hash,
                file_idx: pin.file_idx,
                path: None,
                name: String::new(),
                length: 0,
                downloaded: 0,
                complete: false,
                phase: StartupPhase::Error,
                error: Some(DORMANT_DOWNLOAD_ERROR.to_string()),
            }),
    );
    downloads
}

/// One pinned file of a live torrent. A torrent still resolving its
/// metadata has no file list yet: the entry then carries the pin and the
/// phase, with the file's own numbers still unknown.
fn live_download(
    info_hash: &str,
    file_idx: usize,
    path: Option<String>,
    stats: &EngineStats,
) -> DownloadInfo {
    let file = stats.files.get(file_idx);
    DownloadInfo {
        info_hash: info_hash.to_string(),
        file_idx,
        path,
        name: file.map(|file| file.name.clone()).unwrap_or_default(),
        length: file.map_or(0, |file| file.length),
        downloaded: file.map_or(0, |file| file.downloaded),
        complete: file.is_some_and(|file| file.complete),
        phase: stats.phase,
        error: stats.error.clone(),
    }
}

/// What the backend calls `file_idx` of `info_hash` -- [`DownloadInfo::path`]
/// on its own, a name and not a file that exists. `None` when the torrent is
/// not managed right now or the backend does not know the path yet (no
/// metadata). Never creates an engine -- unlike [`pin_download`], this only
/// reports.
pub async fn download_path(state: &AppState, info_hash: &str, file_idx: usize) -> Option<String> {
    let engine = state.engine.get_engine(&info_hash.to_lowercase()).await?;
    engine.handle.get_file_path(file_idx).await
}

/// Body of `POST /{infoHash}/{fileIdx}/download`. Every field is optional:
/// an empty body pins with no extra trackers. `trackers` takes a stream's
/// `sources`/`announce` values as they are -- `pin_download` normalises
/// them like the stats routes' `tr=` values -- and, as everywhere else,
/// they only matter when this request is the one that creates the engine.
#[derive(Debug, Default, serde::Deserialize)]
pub struct PinRequest {
    #[serde(default, alias = "sources", alias = "announce")]
    pub trackers: Vec<String>,
}

/// Status and body for a refused pin or unpin: a bad file index is a 404,
/// a full disk a 507 (the client can free space and retry), a failed magnet
/// add whatever `compat::engine_creation_failure` says, a backend refusal a
/// 500. The body is [`PinDownloadError::client_message`], which does not
/// leak the absolute cache/downloads paths the backend errors carry -- the
/// full error goes to the log at the call site.
fn download_failure(error: &PinDownloadError) -> (StatusCode, String) {
    let status = match error {
        PinDownloadError::MagnetAdd(error) => compat::engine_creation_failure(error).0,
        PinDownloadError::FileNotFound { .. } => StatusCode::NOT_FOUND,
        PinDownloadError::InsufficientSpace { .. } => StatusCode::INSUFFICIENT_STORAGE,
        PinDownloadError::Backend(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.client_message())
}

/// The 404 for a route `{fileIdx}` that is not a number at all -- the same
/// answer as for a file the torrent does not have, not a 400: the path
/// shape is the one the stats routes answer 404 for.
fn file_idx_not_found(raw: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": format!("file index {raw:?} is not a file index") })),
    )
        .into_response()
}

pub async fn post_download(
    State(state): State<AppState>,
    Path((info_hash, file_idx)): Path<(String, String)>,
    body: Option<Json<PinRequest>>,
) -> Response {
    let Ok(file_idx) = file_idx.parse::<usize>() else {
        return file_idx_not_found(&file_idx);
    };
    let trackers = body.map(|Json(body)| body.trackers).unwrap_or_default();
    match pin_download(&state, &info_hash, file_idx, trackers).await {
        Ok(info) => Json(info).into_response(),
        Err(error) => {
            tracing::warn!(info_hash, file_idx, error = %format!("{error:#}"), "pin_download_failed");
            let (status, message) = download_failure(&error);
            (status, Json(json!({ "error": message }))).into_response()
        }
    }
}

pub async fn delete_download(
    State(state): State<AppState>,
    Path((info_hash, file_idx)): Path<(String, String)>,
    RawQuery(query): RawQuery,
) -> Response {
    let Ok(file_idx) = file_idx.parse::<usize>() else {
        return file_idx_not_found(&file_idx);
    };
    let delete_files = compat::query_flag(query.as_deref(), "deleteFiles");
    match unpin_download(&state, &info_hash, file_idx, delete_files).await {
        // `deletedFiles` is what happened, not what was asked: a dormant
        // pin whose torrent lived in the cache root has nothing this layer
        // can name to delete, and a failed delete is logged, not raised.
        Ok(outcome) => Json(json!({
            "infoHash": info_hash.to_lowercase(),
            "fileIdx": file_idx,
            "unpinned": outcome.unpinned,
            "deletedFiles": outcome.deleted_files,
        }))
        .into_response(),
        Err(error) => {
            tracing::warn!(info_hash, file_idx, error = %format!("{error:#}"), "unpin_download_failed");
            let (status, message) = download_failure(&error);
            (status, Json(json!({ "error": message }))).into_response()
        }
    }
}

pub async fn get_downloads(State(state): State<AppState>) -> Response {
    Json(downloads(&state).await).into_response()
}

#[cfg(test)]
mod tests {
    use super::{PinRequest, ProgressTrack, claim_progress_logger, download_failure};
    use axum::http::StatusCode;
    use enginefs::PinDownloadError;

    /// The line's two stall fields: a first reading measures nothing,
    /// movement resets the count, and a count that fell is not movement.
    #[test]
    fn progress_track_tells_slow_from_stopped() {
        let mut track = ProgressTrack::default();
        assert_eq!(
            track.observe(100),
            (0, 0),
            "a first reading measures nothing"
        );
        assert_eq!(track.observe(100), (0, 1));
        assert_eq!(track.observe(100), (0, 2));
        assert_eq!(track.observe(160), (60, 0), "movement resets the count");
        assert_eq!(
            track.observe(150),
            (0, 1),
            "a count that fell is not movement"
        );
        assert_eq!(track.observe(151), (1, 0));
    }

    /// A retry or the boot's re-pin of a file already being logged adds no
    /// second logger; once the first ends the slot is free again.
    #[test]
    fn one_progress_logger_per_file() {
        let key = ("one_progress_logger_per_file".to_owned(), 3);
        let first = claim_progress_logger(key.clone()).expect("a free slot is claimed");
        assert!(
            claim_progress_logger(key.clone()).is_none(),
            "the same file is not logged twice over"
        );
        assert!(
            claim_progress_logger((key.0.clone(), 4)).is_some(),
            "another file of the torrent is its own logger"
        );
        drop(first);
        assert!(
            claim_progress_logger(key).is_some(),
            "the slot is given back"
        );
    }

    /// A full disk is a 507 the client can act on, a bad index a 404, and a
    /// backend refusal a 500 whose body never carries the librqbit error
    /// (it names absolute cache and downloads paths).
    #[test]
    fn pin_failures_map_to_actionable_statuses() {
        let (status, message) = download_failure(&PinDownloadError::InsufficientSpace {
            required: 5,
            available: 3,
            margin: 2,
        });
        assert_eq!(status, StatusCode::INSUFFICIENT_STORAGE);
        assert!(message.contains("free space"), "{message}");

        let (status, message) = download_failure(&PinDownloadError::FileNotFound {
            file_idx: 9,
            file_count: 2,
        });
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(message.contains("out of range"), "{message}");

        let (status, message) = download_failure(&PinDownloadError::Backend(anyhow::anyhow!(
            "error opening /home/someone/cache/rqbit-downloads/Show/e1.bin"
        )));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!message.contains("/home/someone"), "{message}");
    }

    /// The body is optional and so is every field in it; a stream's
    /// `sources`/`announce` array is accepted under its own name.
    #[test]
    fn pin_request_accepts_the_streams_own_tracker_field_names() {
        let parse = |json: &str| serde_json::from_str::<PinRequest>(json).unwrap().trackers;
        assert!(parse("{}").is_empty());
        assert_eq!(parse(r#"{"trackers":["udp://a"]}"#), vec!["udp://a"]);
        assert_eq!(parse(r#"{"sources":["udp://b"]}"#), vec!["udp://b"]);
        assert_eq!(parse(r#"{"announce":["udp://c"]}"#), vec!["udp://c"]);
    }
}
