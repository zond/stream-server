//! Cache usage and on-demand cleaning: the control routes `GET /cache.json`
//! and `POST /cache/clean`, and the functions they share with the matching
//! `ServerHandle` methods (`cache_usage`, `clean_cache_now`).
//!
//! `POST /cache/clean` runs exactly the same eviction pass the background
//! cleaner runs on its own schedule (`cache_cleaner::clean_cache`) -- same
//! protections, same occupancy accounting -- only on demand: nothing a live
//! engine is writing or a pin protects is ever touched, however far over
//! the limit the cache is. It exists so a client can offer a "clean now"
//! action without restarting the server just to make the cleaner's
//! start-up tick fire.

use crate::cache_cleaner::{self, CacheUsage, EvictionReport};
use crate::state::AppState;
use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};

/// What the cache currently occupies against its configured limit, exactly
/// what `GET /cache.json` answers. See [`CacheUsage`] for the shape and
/// `cache_cleaner::usage` for the walk it runs (read-only; nothing is
/// evicted or aged out).
pub async fn cache_usage(state: &AppState) -> CacheUsage {
    cache_cleaner::usage(state).await
}

/// Run one eviction pass immediately and report what it freed, exactly
/// what `POST /cache/clean` answers. See [`EvictionReport`] for the shape
/// and `cache_cleaner::clean_cache` for the pass itself, which this shares
/// with the background scheduler.
pub async fn clean_cache_now(state: &AppState) -> anyhow::Result<EvictionReport> {
    cache_cleaner::clean_cache(state).await
}

pub async fn get_cache_usage(State(state): State<AppState>) -> Response {
    Json(cache_usage(&state).await).into_response()
}

pub async fn post_clean_cache(State(state): State<AppState>) -> Response {
    match clean_cache_now(&state).await {
        Ok(report) => Json(report).into_response(),
        Err(error) => {
            tracing::error!(error = %format!("{error:#}"), "cache_clean_failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "cache clean failed" })),
            )
                .into_response()
        }
    }
}
