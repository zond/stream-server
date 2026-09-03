//! Stub of the local-files addon that Stremio's default profile ships with.
//!
//! stremio-core's `OFFICIAL_ADDONS` list contains a *protected* descriptor for
//! `http://127.0.0.1:11470/local-addon/manifest.json` with `meta` for
//! `local:`/`bt:` ids and `stream` for `tt` movies/series, so every client
//! running a stock profile asks this server for
//! `/local-addon/stream/{type}/{id}.json` on each details page. Profiles
//! synced from an account carry an older descriptor for the same addon that
//! also declares a *catalog* (`other`/`local`), so core additionally requests
//! `/local-addon/catalog/other/local.json` -- and, once the board pages or a
//! filter is applied, `/local-addon/catalog/other/local/{extra}.json`
//! (`AddonHTTPTransport::resource` builds exactly those two shapes).
//!
//! The real addon was removed (it had no consumer); without these routes each
//! such request is a `404`, an error group in the client's stream list or a
//! failed catalog row, and an ERROR "unhandled request" log line. The stub
//! answers the manifest with an addon that declares nothing, the stream
//! resource with no streams and the catalog resource with no metas, so
//! default profiles stay quiet. It is OPEN (media router): legacy clients that
//! know nothing of the bearer token call it too, and it exposes nothing.
//!
//! Everything else under the prefix is a deliberate `404` from
//! [`unsupported`], logged at debug: this stub serves nothing, so a 404 here
//! is the intended answer and not an error worth an ERROR line.

use crate::state::AppState;
use axum::{
    Json, Router,
    extract::Path,
    http::{Method, StatusCode, Uri},
    response::IntoResponse,
    routing::get,
};
use serde_json::{Value, json};

pub const MANIFEST_ID: &str = "org.stremio.local";

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/manifest.json", get(manifest))
        // `{id}` is one path segment, so it matches with or without `.json`.
        .route("/stream/{type}/{id}", get(stream))
        .route("/catalog/{type}/{id}", get(catalog))
        // The extra-args shape: core percent-encodes each name and value, so
        // the whole `name=value&name=value` string is still one segment.
        .route("/catalog/{type}/{id}/{extra}", get(catalog_with_extra))
        .route("/meta/{type}/{id}", get(meta))
        .fallback(unsupported)
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

/// The catalog resource: an empty but *valid* catalog. Core's
/// `ResourceResponse` accepts `{"metas": []}` as `Metas { metas: [] }`, so the
/// row renders as empty instead of failing -- a 404 here used to break the row
/// and log an ERROR on every board refresh.
async fn catalog(Path((r#type, id)): Path<(String, String)>) -> impl IntoResponse {
    tracing::debug!(
        r#type,
        id = strip_json(&id),
        "local addon catalog request (stub: no metas)"
    );
    Json(empty_catalog_json())
}

/// `catalog/{type}/{id}/{extra}.json` -- the same answer; the extra args only
/// ever narrow a catalog that is empty to begin with.
async fn catalog_with_extra(
    Path((r#type, id, extra)): Path<(String, String, String)>,
) -> impl IntoResponse {
    tracing::debug!(
        r#type,
        id,
        extra = strip_json(&extra),
        "local addon catalog request with extra args (stub: no metas)"
    );
    Json(empty_catalog_json())
}

/// The addon-protocol body for a catalog with nothing in it.
pub fn empty_catalog_json() -> Value {
    json!({ "metas": [] })
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

/// Anything else under `/local-addon`: a resource this stub does not
/// implement (`subtitles`, `addon_catalog`, ...), or a misspelled path. The
/// stub deliberately serves nothing beyond the routes above, so the 404 is the
/// answer we intend -- it goes to debug rather than to the ERROR-level
/// unhandled-request fallback in `build_router`, which is for requests we
/// meant to serve and could not.
async fn unsupported(method: Method, uri: Uri) -> impl IntoResponse {
    tracing::debug!(
        method = %method,
        path = uri.path(),
        "local addon request for an unsupported resource (stub: not found)"
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
    fn empty_catalog_is_the_addon_protocol_shape() {
        // stremio-core's `ResourceResponse` deserialises an object with
        // exactly one of its known keys; `metas` makes this `Metas { metas }`
        // with an empty vec, which renders as an empty row, not an error.
        assert_eq!(empty_catalog_json(), json!({ "metas": [] }));
    }

    #[test]
    fn strip_json_only_removes_the_suffix() {
        assert_eq!(strip_json("tt0111161.json"), "tt0111161");
        assert_eq!(strip_json("tt0111161:1:2"), "tt0111161:1:2");
        assert_eq!(strip_json("local:json"), "local:json");
    }
}
