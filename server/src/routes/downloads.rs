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
use enginefs::PinDownloadError;
use enginefs::backend::{EngineStats, StartupPhase, TorrentHandle};
use serde_json::json;
use std::collections::BTreeMap;

/// One pinned download as the routes and `ServerHandle` report it.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadInfo {
    pub info_hash: String,
    pub file_idx: usize,
    /// Where the file is (or will be) on disk, when the engine knows.
    pub path: Option<String>,
    pub name: String,
    pub length: u64,
    pub downloaded: u64,
    pub complete: bool,
    /// The torrent's startup phase (`checking` right after a relocation).
    pub phase: StartupPhase,
    /// Why the download is not progressing, when anything knows: the
    /// engine's error for a failed magnet add or a torrent the backend put
    /// in an error state, [`DORMANT_DOWNLOAD_ERROR`] for a pin whose
    /// torrent the backend does not have. `null` for a healthy download.
    pub error: Option<String>,
}

/// What a pin whose torrent the backend did not restore reports as its
/// `error` (`enginefs::BackendEngineFS::dormant_pinned_downloads`): the pin
/// is kept, nothing is downloading, and there is nothing for the client to
/// fix beyond making the folder available again.
pub const DORMANT_DOWNLOAD_ERROR: &str = "the torrent is not managed right now (its download folder may be unavailable); \
     the pin is kept and applies when it comes back";

/// Pin `file_idx` of `info_hash` as an offline download, exactly what
/// `POST /{infoHash}/{fileIdx}/download` will answer: the engine is created
/// through the magnet registry with `trackers` (normalised like the stats
/// routes' `tr=` values) when the hash is new, placed under
/// `settings.downloadsDir` when one is set -- relocating a torrent already
/// managed in the cache root -- and kept wanted and exempt from eviction
/// (see `enginefs::BackendEngineFS::pin_download`). Refused with
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
        .stream_engine()
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

/// Drop the pin on `file_idx` of `info_hash`, exactly what
/// `DELETE /{infoHash}/{fileIdx}/download` answers. Returns whether a pin
/// was actually cleared (false for an unknown torrent or an unpinned file).
/// `delete_files` also deletes the data -- the whole torrent when this was
/// its last pin, only that file while other pins hold, and nothing at all
/// for a pin whose torrent the backend does not have (see
/// `enginefs::BackendEngineFS::unpin_download`). Without it the bytes stay
/// where they are and the engine becomes an ordinary, evictable one again.
pub async fn unpin_download(
    state: &AppState,
    info_hash: &str,
    file_idx: usize,
    delete_files: bool,
) -> anyhow::Result<bool> {
    state
        .stream_engine()
        .unpin_download(&info_hash.to_lowercase(), file_idx, delete_files)
        .await
}

/// Every pinned download, exactly what `GET /downloads.json` answers:
/// ordered by info hash then file index, the live ones first and the
/// dormant ones (torrent not restored, [`DORMANT_DOWNLOAD_ERROR`]) after
/// them. One stats call per torrent, not per file.
pub async fn downloads(state: &AppState) -> Vec<DownloadInfo> {
    let engine_fs = state.stream_engine();
    let mut by_hash: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for pin in engine_fs.pinned_downloads().await {
        by_hash.entry(pin.info_hash).or_default().push(pin.file_idx);
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

/// Where `file_idx` of `info_hash` is on disk, for handing a finished
/// download to a local player. `None` when the torrent is not managed right
/// now or the backend does not know the path yet (no metadata). Never
/// creates an engine -- unlike [`pin_download`], this only reports.
pub async fn download_path(state: &AppState, info_hash: &str, file_idx: usize) -> Option<String> {
    let engine = state
        .stream_engine()
        .get_engine(&info_hash.to_lowercase())
        .await?;
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

/// Status and body for a refused pin: a bad file index is a 404, a full
/// disk a 507 (the client can free space and retry), a failed magnet add
/// whatever `compat::engine_creation_failure` says, a backend refusal a
/// 500. The body is [`PinDownloadError::client_message`], which does not
/// leak the absolute cache/downloads paths the backend errors carry -- the
/// full error goes to the log at the call site.
fn pin_failure(error: &PinDownloadError) -> (StatusCode, String) {
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
            let (status, message) = pin_failure(&error);
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
        Ok(unpinned) => Json(json!({
            "infoHash": info_hash.to_lowercase(),
            "fileIdx": file_idx,
            "unpinned": unpinned,
            "deletedFiles": delete_files,
        }))
        .into_response(),
        Err(error) => {
            tracing::warn!(info_hash, file_idx, error = %format!("{error:#}"), "unpin_download_failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "could not remove the download; see server logs" })),
            )
                .into_response()
        }
    }
}

pub async fn get_downloads(State(state): State<AppState>) -> Response {
    Json(downloads(&state).await).into_response()
}

#[cfg(test)]
mod tests {
    use super::{PinRequest, pin_failure};
    use axum::http::StatusCode;
    use enginefs::PinDownloadError;

    /// A full disk is a 507 the client can act on, a bad index a 404, and a
    /// backend refusal a 500 whose body never carries the librqbit error
    /// (it names absolute cache and downloads paths).
    #[test]
    fn pin_failures_map_to_actionable_statuses() {
        let (status, message) = pin_failure(&PinDownloadError::InsufficientSpace {
            required: 5,
            available: 3,
            margin: 2,
        });
        assert_eq!(status, StatusCode::INSUFFICIENT_STORAGE);
        assert!(message.contains("free space"), "{message}");

        let (status, message) = pin_failure(&PinDownloadError::FileNotFound {
            file_idx: 9,
            file_count: 2,
        });
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(message.contains("out of range"), "{message}");

        let (status, message) = pin_failure(&PinDownloadError::Backend(anyhow::anyhow!(
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
