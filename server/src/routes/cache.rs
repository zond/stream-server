//! Cache usage and on-demand cleaning: the control routes `GET /cache.json`
//! and `POST /cache/clean`, and the functions they share with the matching
//! `ServerHandle` methods (`cache_usage`, `clean_cache_now`).
//!
//! `POST /cache/clean` gives back everything nobody is playing and nobody
//! is reading (`cache_cleaner::drop_slack`) -- the same passes the
//! reconciler's tick and a viewer opening something else run, on demand.
//! Nothing a live engine is writing or a pin protects is ever touched,
//! however far over the limit the cache is. It exists so a client can offer
//! a "clean now" action, and what it can honestly promise is that the
//! disposable bytes are gone by the time it answers.

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
/// `cache_cleaner::usage` for where the figures come from (the owners'
/// own counts; nothing is walked and nothing is deleted).
pub async fn cache_usage(state: &AppState) -> CacheUsage {
    cache_cleaner::usage(state).await
}

/// Drop both owners' slack immediately and report what is left, exactly
/// what `POST /cache/clean` answers. See [`EvictionReport`] for the shape
/// and `cache_cleaner::drop_slack` for the passes themselves, which this
/// shares with the switch task and the reconciler's tick.
///
/// Fallible in its signature and infallible in fact: a pass that cannot
/// unlink a file leaves the bytes for the next one and says so in the
/// figures it reports. The `Result` is the boundary
/// `ServerHandle::clean_cache_now` and the route were built on and it is
/// left as it is, because narrowing it is a change to an API this slice is
/// not about.
pub async fn clean_cache_now(state: &AppState) -> anyhow::Result<EvictionReport> {
    Ok(cache_cleaner::drop_slack(state).await)
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
