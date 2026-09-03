//! Offline downloads: the shared functions behind the download control
//! routes and the matching `ServerHandle` methods (`pin_download` today;
//! the HTTP handlers land with the download control routes).

use crate::routes::compat;
use crate::state::AppState;
use enginefs::PinDownloadError;
use enginefs::backend::{StartupPhase, TorrentHandle};

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
}

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
    })
}
