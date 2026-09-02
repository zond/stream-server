//! Stub of the local-files addon that Stremio's default profile ships with.
//!
//! stremio-core's `OFFICIAL_ADDONS` list contains a *protected* descriptor for
//! `http://127.0.0.1:11470/local-addon/manifest.json` with `meta` for
//! `local:`/`bt:` ids and `stream` for `tt` movies/series, so every client
//! running a stock profile asks this server for
//! `/local-addon/stream/{type}/{id}.json` on each details page. The real
//! addon was removed (it had no consumer); without these routes each such
//! request is a `404`, an error group in the client's stream list and an
//! ERROR "unhandled request" log line. The stub answers the manifest with an
//! addon that declares nothing and the stream resource with no streams, so
//! default profiles stay quiet. It is OPEN (media router): legacy clients that
//! know nothing of the bearer token call it too, and it exposes nothing.

use crate::state::AppState;
use axum::{Json, Router, extract::Path, http::StatusCode, response::IntoResponse, routing::get};
use serde_json::{Value, json};

pub const MANIFEST_ID: &str = "org.stremio.local";

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/manifest.json", get(manifest))
        // `{id}` is one path segment, so it matches with or without `.json`.
        .route("/stream/{type}/{id}", get(stream))
        .route("/meta/{type}/{id}", get(meta))
}

/// A minimal valid addon manifest that declares no types, resources or
/// catalogs. Clients keep using the descriptor from their profile for
/// resource matching, hence the `stream` stub below.
pub fn manifest_json() -> Value {
    json!({
        "id": MANIFEST_ID,
        "version": env!("CARGO_PKG_VERSION"),
        "name": "Local Files",
        "description": "Stub kept so default Stremio profiles do not show errors; this server serves no local files.",
        "types": [],
        "resources": [],
        "catalogs": []
    })
}

async fn manifest() -> impl IntoResponse {
    Json(manifest_json())
}

async fn stream(Path((r#type, id)): Path<(String, String)>) -> impl IntoResponse {
    tracing::debug!(
        r#type,
        id = strip_json(&id),
        "local addon stream request (stub: no streams)"
    );
    Json(json!({ "streams": [] }))
}

/// `meta` is only ever asked for `local:`/`bt:` ids, which nothing produces
/// any more; a quiet 404 rather than the ERROR-level unhandled-request log.
async fn meta(Path((r#type, id)): Path<(String, String)>) -> impl IntoResponse {
    tracing::debug!(
        r#type,
        id = strip_json(&id),
        "local addon meta request (stub: not found)"
    );
    StatusCode::NOT_FOUND
}

fn strip_json(id: &str) -> &str {
    id.strip_suffix(".json").unwrap_or(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_is_a_valid_empty_addon() {
        let manifest = manifest_json();
        assert_eq!(manifest["id"], MANIFEST_ID);
        assert_eq!(manifest["name"], "Local Files");
        assert_eq!(manifest["version"], env!("CARGO_PKG_VERSION"));
        for key in ["types", "resources", "catalogs"] {
            assert_eq!(manifest[key], json!([]), "{key}");
        }
    }

    #[test]
    fn strip_json_only_removes_the_suffix() {
        assert_eq!(strip_json("tt0111161.json"), "tt0111161");
        assert_eq!(strip_json("tt0111161:1:2"), "tt0111161:1:2");
        assert_eq!(strip_json("local:json"), "local:json");
    }
}
