use crate::routes::compat;
use crate::state::AppState;
use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
};
use enginefs::backend::TorrentHandle;
use enginefs::engine::SeriesInfo;
use hex;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct CreateEngineRequest {
    pub from: Option<String>, // Magnet link or URL
    #[serde(alias = "blob")]
    pub torrent: Option<String>, // Torrent blob (hex encoded) - alias "blob" for stremio-core compat
    pub announce: Option<Vec<String>>,
    #[serde(rename = "peerSearch")]
    pub peer_search: Option<PeerSearchBody>,
    #[serde(rename = "fileMustInclude", default)]
    pub file_must_include: Vec<String>,
    #[serde(rename = "guessFileIdx")]
    pub guess_file_idx: Option<serde_json::Value>,
}

#[derive(Deserialize)]
pub struct PeerSearchBody {
    #[serde(default)]
    pub sources: Vec<String>,
}

pub async fn create_engine(
    State(state): State<AppState>,
    Json(payload): Json<CreateEngineRequest>,
) -> impl IntoResponse {
    let source = if let Some(hex_str) = payload.torrent {
        match hex::decode(hex_str) {
            Ok(bytes) => enginefs::backend::TorrentSource::Bytes(bytes),
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": format!("Invalid hex blob: {}", e) })),
                );
            }
        }
    } else if let Some(from) = payload.from {
        enginefs::backend::TorrentSource::Url(from)
    } else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Missing 'from' or 'torrent' field" })),
        );
    };

    let trackers = merged_trackers(payload.announce, payload.peer_search);
    let file_must_include = payload.file_must_include;
    let guess = parse_guess_file_idx(payload.guess_file_idx.as_ref());

    match state
        .stream_engine()
        .add_torrent(source, Some(trackers))
        .await
    {
        Ok(engine) => {
            let stats = stats_with_guess(&engine, &file_must_include, guess).await;
            (StatusCode::OK, Json(stats))
        }
        // stremio-video's createTorrent.js checks resp.ok before reading the
        // body (createTorrent.js:62); a 200 here on failure leaves
        // guessedFileIdx undefined and produces a broken /{infoHash}/undefined
        // stream URL, so fail with a non-2xx status instead.
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

/// Stremio-core /{infoHash}/create endpoint for magnet links.
///
/// It goes through `EngineFS::get_or_add_magnet`, never
/// `add_torrent`: the hash may already be resolving for a stats poll or a
/// stream request (and vice versa), and only one add per hash may exist or
/// the second one's trackers are silently lost (see
/// `routes::compat::get_or_create_engine`).
#[derive(Deserialize)]
pub struct CreateMagnetRequest {
    pub stream: Option<CreateMagnetStream>,
    #[serde(rename = "peerSearch")]
    pub peer_search: Option<PeerSearchBody>,
    #[serde(rename = "fileMustInclude", default)]
    pub file_must_include: Vec<String>,
    #[serde(rename = "guessFileIdx")]
    pub guess_file_idx: Option<serde_json::Value>,
}

#[derive(Deserialize)]
pub struct CreateMagnetStream {
    #[serde(rename = "infoHash")]
    pub info_hash: Option<String>,
}

pub async fn create_magnet(
    State(state): State<AppState>,
    axum::extract::Path(info_hash): axum::extract::Path<String>,
    Json(payload): Json<CreateMagnetRequest>,
) -> impl IntoResponse {
    // Use the info_hash from path or body
    let ih = payload
        .stream
        .as_ref()
        .and_then(|s| s.info_hash.as_ref())
        .map(|s| s.as_str())
        .unwrap_or(&info_hash);

    let trackers = merged_trackers(None, payload.peer_search);
    let file_must_include = payload.file_must_include;
    let guess = parse_guess_file_idx(payload.guess_file_idx.as_ref());

    match state
        .stream_engine()
        .get_or_add_magnet(ih, Some(trackers))
        .await
    {
        Ok(engine) => {
            let stats = stats_with_guess(&engine, &file_must_include, guess).await;
            (StatusCode::OK, Json(stats))
        }
        // See the matching comment in create_engine: stremio-video's
        // createTorrent.js requires a non-2xx status to detect failure.
        Err(e) => magnet_create_failure(ih, &e),
    }
}

/// Non-2xx JSON error for a failed `/{infoHash}/create`, with the same status
/// mapping (504 on metadata timeout) and non-leaky message as the stream route.
fn magnet_create_failure(
    info_hash: &str,
    error: &enginefs::MagnetAddError,
) -> (StatusCode, Json<serde_json::Value>) {
    tracing::error!(info_hash, %error, "create_magnet failed to create engine");
    let (status, message) = compat::engine_creation_failure(error);
    (status, Json(json!({ "error": message })))
}

fn merged_trackers(
    announce: Option<Vec<String>>,
    peer_search: Option<PeerSearchBody>,
) -> Vec<String> {
    let mut sources = announce.unwrap_or_default();
    if let Some(peer_search) = peer_search {
        sources.extend(peer_search.sources);
    }
    compat::normalize_tracker_sources(sources)
}

/// Parse the request's `guessFileIdx` field, mirroring stremio-core's
/// `CreatedTorrent.guess_file_idx: Option<SeriesInfo>`: `false`/`null`/absent
/// means no guessing; `{}` means guess with no episode hints (movies);
/// `{season, episode}` (stremio-video createTorrent.js:41-53) carries the
/// hints; any other truthy value degrades to a hint-less guess.
fn parse_guess_file_idx(value: Option<&serde_json::Value>) -> Option<SeriesInfo> {
    match value {
        None | Some(serde_json::Value::Bool(false)) | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::Object(obj)) => Some(SeriesInfo {
            season: obj
                .get("season")
                .and_then(serde_json::Value::as_u64)
                .map(|v| v as usize),
            episode: obj
                .get("episode")
                .and_then(serde_json::Value::as_u64)
                .map(|v| v as usize),
        }),
        Some(_) => Some(SeriesInfo::default()),
    }
}

async fn stats_with_guess<H>(
    engine: &Arc<enginefs::engine::Engine<H>>,
    filters: &[String],
    guess: Option<SeriesInfo>,
) -> serde_json::Value
where
    H: TorrentHandle,
{
    let stats = engine.get_statistics().await;
    let mut value = serde_json::to_value(stats).unwrap_or_else(|_| json!({}));

    if filters.is_empty() && guess.is_none() {
        return value;
    }

    let files = engine.handle.get_files().await;

    // fileMustInclude takes precedence: the stream explicitly names its file.
    let mut guessed = files.iter().position(|file| {
        filters
            .iter()
            .any(|filter| compat::file_matches_filter(&file.name, filter))
    });

    // Then the series-aware guess (SxxEyy / NxM episode tags, largest-media
    // fallback) — this is what picks the right episode out of a season pack.
    if guessed.is_none() && guess.is_some() {
        guessed = enginefs::engine::guess_file_index_in(&files, guess.as_ref());
    }

    // Last resort (e.g. no media-extension file at all): largest video file,
    // then largest file of any kind.
    if guessed.is_none() {
        let candidates = files
            .iter()
            .enumerate()
            .map(|(index, file)| compat::FileCandidate {
                index,
                name: file.name.clone(),
                length: file.length,
            })
            .collect::<Vec<_>>();
        guessed = compat::resolve_file_idx("-1", &candidates, &[]).ok();
    }

    if let Some(idx) = guessed
        && let Some(obj) = value.as_object_mut()
    {
        obj.insert("guessedFileIdx".to_string(), json!(idx));
    }

    value
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(value: serde_json::Value) -> Option<SeriesInfo> {
        parse_guess_file_idx(Some(&value))
    }

    #[test]
    fn guess_file_idx_false_null_or_absent_means_no_guess() {
        assert_eq!(parse_guess_file_idx(None), None);
        assert_eq!(parse(json!(false)), None);
        assert_eq!(parse(json!(null)), None);
    }

    #[test]
    fn guess_file_idx_object_carries_season_and_episode() {
        assert_eq!(
            parse(json!({ "season": 2, "episode": 5 })),
            Some(SeriesInfo {
                season: Some(2),
                episode: Some(5),
            })
        );
    }

    #[test]
    fn guess_file_idx_empty_object_guesses_without_hints() {
        assert_eq!(parse(json!({})), Some(SeriesInfo::default()));
    }

    #[test]
    fn guess_file_idx_other_truthy_values_guess_without_hints() {
        assert_eq!(parse(json!(true)), Some(SeriesInfo::default()));
        assert_eq!(parse(json!(1)), Some(SeriesInfo::default()));
    }

    #[test]
    fn guess_file_idx_ignores_non_numeric_hints() {
        assert_eq!(
            parse(json!({ "season": "x", "episode": 5 })),
            Some(SeriesInfo {
                season: None,
                episode: Some(5),
            })
        );
    }
}
