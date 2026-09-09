//! `GET /stream-numbers.json?url=<the URL the player is playing>`: what this
//! server holds of that stream.
//!
//! The whole of the route is the question and the dispatch, both of which
//! live in [`crate::stream_numbers`] -- see there for the shape of the
//! answer, what each absence means, and why neither store keeps anything.
//!
//! **A URL this server does not hold is `200 null`, not a `404`.** The
//! client is not asking whether a resource exists here; it is asking what we
//! hold of the stream its player is on, and "nothing" is a complete answer
//! to that. A `404` would have a client showing an error for a stream that
//! is playing perfectly -- an addon's direct link, a local file -- which is
//! the ordinary case for every client this server does not proxy for.

use crate::state::AppState;
use crate::stream_numbers::StreamNumbers;
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};

#[derive(serde::Deserialize)]
pub struct StreamNumbersParams {
    /// The URL the client handed its player. Absolute, or just the path and
    /// query of one.
    pub url: Option<String>,
}

/// The numbers for one playing stream, exactly what `GET
/// /stream-numbers.json` answers. Shared with
/// [`crate::ServerHandle::stream_numbers`], per the library-parity rule.
pub async fn stream_numbers(state: &AppState, url: &str) -> Option<StreamNumbers> {
    crate::stream_numbers::stream_numbers(state, url).await
}

pub async fn get_stream_numbers(
    State(state): State<AppState>,
    Query(params): Query<StreamNumbersParams>,
) -> Response {
    let Some(url) = params.url.filter(|url| !url.is_empty()) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "url is required" })),
        )
            .into_response();
    };
    Json(stream_numbers(&state, &url).await).into_response()
}
