use axum::http::{HeaderMap, HeaderValue, StatusCode};
use enginefs::backend::librqbit::LibrqbitHandle;
use enginefs::engine::Engine;
use enginefs::{EngineFS, MagnetAddError};
use regex::RegexBuilder;
use std::sync::Arc;

pub const DLNA_TRANSFER_MODE: &str = "Streaming";
pub const DLNA_CONTENT_FEATURES: &str =
    "DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01700000000000000000000000000000";

#[derive(Debug, Clone)]
pub struct FileCandidate {
    pub index: usize,
    pub name: String,
    pub length: u64,
}

pub fn query_values(query: Option<&str>, name: &str) -> Vec<String> {
    query
        .map(|q| {
            q.split('&')
                .filter(|field| field.split_once('=').unwrap_or((field, "")).0 == name)
                .filter_map(|field| {
                    url::form_urlencoded::parse(field.as_bytes())
                        .next()
                        .map(|(_, value)| value.into_owned())
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Normalise server.js-style peer sources (`tracker:` prefix stripped, `dht:`
/// dropped, blanks removed). Takes the values as they are: `tr=` query values
/// arrive already percent-decoded from `query_values`, and JSON
/// `peerSearch.sources` are never percent-encoded.
pub fn normalize_tracker_sources(sources: Vec<String>) -> Vec<String> {
    sources
        .into_iter()
        .filter_map(|source| {
            let trimmed = source.trim();
            if trimmed.is_empty() || trimmed.starts_with("dht:") {
                None
            } else if let Some(tracker) = trimmed.strip_prefix("tracker:") {
                (!tracker.is_empty()).then(|| tracker.to_string())
            } else {
                Some(trimmed.to_string())
            }
        })
        .collect()
}

/// The addon-supplied trackers of a playback-shaped request: every `tr=` query
/// value, percent-decoded and normalised (`tracker:` prefixes stripped, `dht:`
/// sources dropped) exactly as server.js's `/{infoHash}/{fileIdx}` does.
/// server.js-style truthiness for a query value: `1`, `true` or `yes`
/// (case-insensitively). Anything else -- `0`, empty, absent -- is false.
pub fn query_value_is_true(value: &str) -> bool {
    value == "1" || value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("yes")
}

/// Whether the boolean query flag `name` is set truthily in `query`
/// (`?deleteFiles=1`), by [`query_value_is_true`].
pub fn query_flag(query: Option<&str>, name: &str) -> bool {
    query_values(query, name)
        .iter()
        .any(|value| query_value_is_true(value))
}

pub fn parse_trackers(query_str: Option<&str>) -> Vec<String> {
    normalize_tracker_sources(query_values(query_str, "tr"))
}

/// Look `info_hash` up in `engine_fs`, or create it from a bare magnet with
/// the request's `tr=` trackers merged in, waiting for metadata. Every route
/// that may be the first to touch a torrent (stream, HEAD, both `stats.json`
/// variants) must create the engine through this or
/// `EngineFS::get_or_begin_add_magnet`: the librqbit backend cannot add
/// trackers to a torrent after the fact (see `LibrqbitHandle::add_trackers`),
/// so whichever request arrives first fixes the tracker set for the whole
/// session. A stats poll racing the first stream request must therefore not
/// create a tracker-less engine that the stream request then silently reuses.
///
/// Waits at most `enginefs::METADATA_RESOLVE_TIMEOUT` for metadata; map the
/// error with [`engine_creation_failure`].
pub async fn get_or_create_engine(
    engine_fs: &EngineFS,
    info_hash: &str,
    query_str: Option<&str>,
) -> Result<Arc<Engine<LibrqbitHandle>>, MagnetAddError> {
    engine_fs
        .get_or_add_magnet(info_hash, Some(parse_trackers(query_str)))
        .await
}

/// Status and body for a failed [`get_or_create_engine`]: a metadata timeout
/// is 504 (the swarm did not answer in time; retrying may well succeed), a
/// backend refusal 502, anything else 500. The body is
/// [`MagnetAddError::client_message`] -- the same non-leaking text the stats
/// route reports -- because a librqbit error can carry absolute download-dir
/// paths, which must not be echoed to an HTTP client (log the error at the
/// call site instead).
pub fn engine_creation_failure(error: &MagnetAddError) -> (StatusCode, String) {
    let status = match error {
        MagnetAddError::MetadataTimeout { .. } => StatusCode::GATEWAY_TIMEOUT,
        MagnetAddError::Backend { .. } => StatusCode::BAD_GATEWAY,
        MagnetAddError::Cancelled { .. } | MagnetAddError::TaskFailed { .. } => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    (status, error.client_message())
}

pub fn basename(path: &str) -> &str {
    path.rsplit(['/', '\\'])
        .find(|part| !part.is_empty() && *part != "." && *part != "..")
        .unwrap_or("download")
}

fn clean_filename_component(name: &str) -> String {
    let cleaned = name
        .chars()
        .filter(|ch| !ch.is_control() && *ch != '"' && *ch != '\r' && *ch != '\n')
        .collect::<String>()
        .trim()
        .trim_matches('.')
        .to_string();

    if cleaned.is_empty() {
        "download".to_string()
    } else {
        cleaned
    }
}

fn ascii_fallback(name: &str) -> String {
    let fallback = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_' | ' ') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim()
        .trim_matches('.')
        .to_string();

    if fallback.is_empty() {
        "download".to_string()
    } else {
        fallback
    }
}

pub fn content_disposition_attachment(path: &str) -> HeaderValue {
    let cleaned = clean_filename_component(basename(path));
    let fallback = ascii_fallback(&cleaned);
    let encoded = urlencoding::encode(&cleaned);
    HeaderValue::from_str(&format!(
        "attachment; filename=\"{}\"; filename*=UTF-8''{}",
        fallback, encoded
    ))
    .unwrap_or_else(|_| HeaderValue::from_static("attachment; filename=\"download\""))
}

pub fn content_disposition_inline(path: &str) -> HeaderValue {
    let cleaned = clean_filename_component(basename(path));
    let fallback = ascii_fallback(&cleaned);
    HeaderValue::from_str(&format!("inline; filename=\"{}\"", fallback))
        .unwrap_or_else(|_| HeaderValue::from_static("inline; filename=\"download\""))
}

pub fn add_dlna_headers(headers: &mut HeaderMap) {
    headers.insert(
        "transferMode.dlna.org",
        HeaderValue::from_static(DLNA_TRANSFER_MODE),
    );
    headers.insert(
        "contentFeatures.dlna.org",
        HeaderValue::from_static(DLNA_CONTENT_FEATURES),
    );
}

pub fn resolve_file_idx(
    requested_idx: &str,
    files: &[FileCandidate],
    filters: &[String],
) -> Result<usize, String> {
    if requested_idx != "-1" {
        let idx = requested_idx
            .parse::<usize>()
            .map_err(|_| format!("Invalid file index: {requested_idx}"))?;
        return files
            .iter()
            .any(|file| file.index == idx)
            .then_some(idx)
            .ok_or_else(|| "File index out of bounds".to_string());
    }

    if files.is_empty() {
        return Err("No files available".to_string());
    }

    if !filters.is_empty()
        && let Some(file) = files.iter().find(|file| {
            filters
                .iter()
                .any(|filter| file_matches_filter(&file.name, filter))
        })
    {
        return Ok(file.index);
    }

    files
        .iter()
        .filter(|file| is_video_name(&file.name))
        .max_by_key(|file| file.length)
        .or_else(|| files.iter().max_by_key(|file| file.length))
        .map(|file| file.index)
        .ok_or_else(|| "No playable file found".to_string())
}

pub fn is_video_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    matches!(
        lower.rsplit('.').next(),
        Some("mkv" | "mp4" | "avi" | "webm" | "mov" | "wmv" | "m4v" | "ts")
    )
}

pub fn file_matches_filter(name: &str, filter: &str) -> bool {
    if let Some((pattern, flags)) = parse_regex_filter(filter) {
        return RegexBuilder::new(pattern)
            .case_insensitive(flags.contains('i'))
            .build()
            .map(|regex| regex.is_match(name))
            .unwrap_or(false);
    }

    name.to_ascii_lowercase()
        .contains(&filter.to_ascii_lowercase())
}

fn parse_regex_filter(filter: &str) -> Option<(&str, &str)> {
    if !filter.starts_with('/') {
        return None;
    }
    let last_slash = filter.rfind('/')?;
    (last_slash > 0).then(|| (&filter[1..last_slash], &filter[last_slash + 1..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attachment_header_uses_safe_basename_and_utf8_name() {
        let header = content_disposition_attachment(r#"C:\tmp\..\Movie "Final".mkv"#);
        let value = header.to_str().expect("valid header");

        assert!(value.starts_with("attachment;"));
        assert!(value.contains(r#"filename="Movie Final.mkv""#));
        assert!(value.contains("filename*=UTF-8''Movie%20Final.mkv"));
        assert!(!value.contains(".."));
    }

    /// `?deleteFiles=1` and its spellings; anything else is off, including
    /// a flag that is simply absent.
    #[test]
    fn query_flag_reads_server_js_truthiness() {
        for query in ["deleteFiles=1", "tr=x&deleteFiles=true", "deleteFiles=YES"] {
            assert!(super::query_flag(Some(query), "deleteFiles"), "{query}");
        }
        for query in [
            "deleteFiles=0",
            "deleteFiles=",
            "deleteFiles",
            "other=1",
            "",
        ] {
            assert!(!super::query_flag(Some(query), "deleteFiles"), "{query}");
        }
        assert!(!super::query_flag(None, "deleteFiles"));
    }

    #[test]
    fn parse_trackers_decodes_and_normalises_tr_values() {
        let trackers = parse_trackers(Some(
            "tr=tracker%3Audp%3A%2F%2Fone%3A6969%2Fannounce&f=movie&tr=dht%3Aabc&tr=https%3A%2F%2Ftwo%2Fannounce",
        ));

        assert_eq!(
            trackers,
            ["udp://one:6969/announce", "https://two/announce"]
        );
        assert!(parse_trackers(None).is_empty());
    }

    #[test]
    fn engine_creation_failure_maps_metadata_timeout_to_504_without_leaking() {
        let timeout = MagnetAddError::MetadataTimeout {
            info_hash: "abc".into(),
            timeout: std::time::Duration::from_secs(90),
        };
        let (status, body) = engine_creation_failure(&timeout);
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(body, timeout.to_string());
        assert!(body.contains("90s"), "{body}");

        let (status, body) = engine_creation_failure(&MagnetAddError::Backend {
            info_hash: "abc".into(),
            error: Arc::new(anyhow::anyhow!("cannot open /home/user/downloads/x")),
        });
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(!body.contains("/home/user"), "{body}");

        let (status, body) = engine_creation_failure(&MagnetAddError::TaskFailed {
            info_hash: "abc".into(),
            reason: "panicked at /home/user/src/lib.rs".into(),
        });
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!body.contains("/home/user"), "{body}");
    }

    /// `query_values` already percent-decodes `tr=`; decoding a second time
    /// would corrupt trackers whose URL legitimately contains `%25` (and JSON
    /// `peerSearch.sources` are never percent-encoded to begin with).
    #[test]
    fn parse_trackers_decodes_exactly_once() {
        let trackers = parse_trackers(Some(
            "tr=udp%3A%2F%2Fone.invalid%2Fannounce%3Fkey%3D100%2525",
        ));
        assert_eq!(trackers, ["udp://one.invalid/announce?key=100%25"]);
        assert_eq!(
            normalize_tracker_sources(vec!["tracker:https://two.invalid/a%20b".to_string()]),
            ["https://two.invalid/a%20b"]
        );
    }

    #[test]
    fn resolves_minus_one_to_largest_video() {
        let files = vec![
            FileCandidate {
                index: 0,
                name: "sample.txt".to_string(),
                length: 10_000,
            },
            FileCandidate {
                index: 1,
                name: "movie.mkv".to_string(),
                length: 1_000,
            },
            FileCandidate {
                index: 2,
                name: "feature.mp4".to_string(),
                length: 2_000,
            },
        ];

        assert_eq!(resolve_file_idx("-1", &files, &[]).unwrap(), 2);
    }

    #[test]
    fn resolves_minus_one_with_filter() {
        let files = vec![
            FileCandidate {
                index: 0,
                name: "episode.one.mkv".to_string(),
                length: 1,
            },
            FileCandidate {
                index: 1,
                name: "episode.two.mkv".to_string(),
                length: 1,
            },
        ];

        assert_eq!(
            resolve_file_idx("-1", &files, &["/two/i".to_string()]).unwrap(),
            1
        );
    }
}
