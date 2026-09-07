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

/// What a `POST /create` names, sorted into what has to happen to it.
///
/// stremio-core sends `/create` with a torrent-file `blob` and routes
/// magnets to `/{infoHash}/create`; nothing shipped sends a magnet in
/// `from`. But `from` accepts one -- a `magnet:` link or a bare 40-hex
/// hash, both of which librqbit's `Session::add_torrent` treats as a magnet
/// -- and a magnet may never go through `EngineFS::add_torrent` from a
/// route: librqbit resolves metadata inside that call, with no timeout of
/// its own, and the add is invisible to the registry that bounds every
/// other magnet add at `METADATA_RESOLVE_TIMEOUT` and lets a stats poll for
/// the same hash join it instead of starting a second resolve. (Measured:
/// `{"from": <hash>}` for a hash with no swarm did not answer in two
/// minutes while `/{infoHash}/stats.json` at the same time reported its own,
/// separate `resolvingMetadata` add.) So a magnet is recognised here and
/// sent down the registry path with the link's own `tr=` trackers merged
/// in; a torrent file, by blob or by `http(s)` URL, keeps `add_torrent`.
enum CreateSource {
    /// A `magnet:` link or bare info hash: the registry path.
    Magnet {
        info_hash: String,
        trackers: Vec<String>,
    },
    /// Torrent-file bytes, or a URL the backend fetches them from.
    TorrentFile(enginefs::backend::TorrentSource),
}

impl CreateSource {
    /// Sort `from` the way librqbit's own `Magnet::parse` would: a bare
    /// 40-hex hash is a magnet (the same check `magnet_with_trackers` makes
    /// before it upgrades one), and so is a `magnet:` URL naming a v1 hash
    /// in `xt=urn:btih:`, whose `tr=` values are the link's trackers.
    /// Anything else -- an `http(s)` URL, a v2-only magnet -- is left to
    /// `add_torrent` as before.
    fn from_link(from: String) -> Self {
        if is_info_hash(&from) {
            return Self::Magnet {
                info_hash: from.to_lowercase(),
                trackers: Vec::new(),
            };
        }
        if let Ok(url) = url::Url::parse(&from)
            && url.scheme() == "magnet"
        {
            let mut info_hash = None;
            let mut trackers = Vec::new();
            for (key, value) in url.query_pairs() {
                match key.as_ref() {
                    "xt" => {
                        if let Some(hash) = value.strip_prefix("urn:btih:")
                            && is_info_hash(hash)
                        {
                            info_hash = Some(hash.to_lowercase());
                        }
                    }
                    "tr" => trackers.push(value.into_owned()),
                    _ => {}
                }
            }
            if let Some(info_hash) = info_hash {
                return Self::Magnet {
                    info_hash,
                    trackers,
                };
            }
        }
        Self::TorrentFile(enginefs::backend::TorrentSource::Url(from))
    }
}

/// A 40-digit hex string: a v1 info hash as the routes and the registry
/// spell it.
fn is_info_hash(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

pub async fn create_engine(
    State(state): State<AppState>,
    Json(payload): Json<CreateEngineRequest>,
) -> impl IntoResponse {
    let source = if let Some(hex_str) = payload.torrent {
        match hex::decode(hex_str) {
            Ok(bytes) => CreateSource::TorrentFile(enginefs::backend::TorrentSource::Bytes(bytes)),
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": format!("Invalid hex blob: {}", e) })),
                );
            }
        }
    } else if let Some(from) = payload.from {
        CreateSource::from_link(from)
    } else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Missing 'from' or 'torrent' field" })),
        );
    };

    let mut trackers = merged_trackers(payload.announce, payload.peer_search);
    let file_must_include = payload.file_must_include;
    let guess = parse_guess_file_idx(payload.guess_file_idx.as_ref());

    let engine = match source {
        CreateSource::Magnet {
            info_hash,
            trackers: link_trackers,
        } => {
            trackers = compat::normalize_tracker_sources(
                trackers.into_iter().chain(link_trackers).collect(),
            );
            match state
                .stream_engine()
                .get_or_add_magnet(&info_hash, Some(trackers))
                .await
            {
                Ok(engine) => engine,
                Err(e) => return magnet_create_failure(&info_hash, &e),
            }
        }
        CreateSource::TorrentFile(source) => {
            match state
                .stream_engine()
                .add_torrent(source, Some(trackers))
                .await
            {
                Ok(engine) => engine,
                // stremio-video's createTorrent.js checks resp.ok before
                // reading the body (createTorrent.js:62); a 200 here on
                // failure leaves guessedFileIdx undefined and produces a
                // broken /{infoHash}/undefined stream URL, so fail with a
                // non-2xx status instead. The body is a fixed string like
                // every other torrent-creation failure's: the error is
                // logged here in full and never echoed, since a backend
                // error can carry the cache root's path.
                // The one typed failure this path can produce: the hash was
                // evicted for want of disk space and is inside its
                // cooling-off period. It gets the same 507 and the same
                // words as the magnet path's, or a client that re-creates
                // the torrent from its `.torrent` file would read "Failed
                // to add torrent" for a full disk.
                Err(e) => {
                    if let Some(typed) = e.downcast_ref::<enginefs::MagnetAddError>() {
                        let (status, message) = compat::engine_creation_failure(typed);
                        tracing::warn!(%typed, "create_engine refused the torrent");
                        return (status, Json(json!({ "error": message })));
                    }
                    tracing::error!(error = %format!("{e:#}"), "create_engine failed to add torrent");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({ "error": "Failed to add torrent" })),
                    );
                }
            }
        }
    };
    let stats = stats_with_guess(&engine, &file_must_include, guess).await;
    (StatusCode::OK, Json(stats))
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

    /// A `from` that librqbit would treat as a magnet -- a link, or a bare
    /// info hash -- takes the registry path, with the link's trackers;
    /// anything else is a torrent file to fetch.
    #[test]
    fn create_source_sorts_magnets_from_torrent_files() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        match CreateSource::from_link(format!(
            "magnet:?xt=urn:btih:{hash}&tr=udp%3A%2F%2Fone%3A6969%2Fannounce&dn=x"
        )) {
            CreateSource::Magnet {
                info_hash,
                trackers,
            } => {
                assert_eq!(info_hash, hash);
                assert_eq!(trackers, ["udp://one:6969/announce"]);
            }
            CreateSource::TorrentFile(_) => panic!("a magnet link is a magnet"),
        }
        match CreateSource::from_link(hash.to_uppercase()) {
            CreateSource::Magnet {
                info_hash,
                trackers,
            } => {
                assert_eq!(info_hash, hash, "lowercased, as the registry keys it");
                assert!(trackers.is_empty());
            }
            CreateSource::TorrentFile(_) => panic!("a bare info hash is a magnet to librqbit"),
        }
        for url in [
            "https://example.invalid/film.torrent",
            "ftp://example.invalid/x.torrent",
            "not a url at all",
        ] {
            match CreateSource::from_link(url.to_string()) {
                CreateSource::TorrentFile(enginefs::backend::TorrentSource::Url(kept)) => {
                    assert_eq!(kept, url)
                }
                _ => panic!("{url} is not a magnet"),
            }
        }
    }

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
