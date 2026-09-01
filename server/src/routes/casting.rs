use crate::state::AppState;
use axum::{
    Json, Router,
    extract::{Path, Query},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
pub struct PlayerParams {
    pub source: Option<String>,
    pub paused: Option<String>,
    pub time: Option<f64>,
    pub volume: Option<f32>,
    pub stop: Option<String>,
    #[serde(rename = "audioTrack")]
    pub audio_track: Option<usize>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_devices))
        .route("/transcode", get(transcode))
        .route("/convert", get(transcode))
        .route("/{devID}", get(get_device))
        .route("/{devID}/player", get(player_control).post(player_control))
}

pub async fn list_devices(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let devices = state.devices.read().await;
    Json(devices.clone())
}

pub async fn get_device(Path(dev_id): Path<String>) -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        format!("Device {} not found", dev_id),
    )
        .into_response()
}

// On-the-fly DLNA transcoding used to shell out to ffmpeg. Transcoding has been
// removed (the server is pure-Rust, direct-play only), so this reports
// not-implemented rather than pretending to serve a transcoded stream.
pub async fn transcode() -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "status": "not_implemented",
            "error": { "message": "Transcoding is not supported" }
        })),
    )
}

pub async fn player_control(
    method: axum::http::Method,
    Path(dev_id): Path<String>,
    Query(query_params): Query<PlayerParams>,
    body: Option<Json<PlayerParams>>,
) -> impl IntoResponse {
    let params = if method == axum::http::Method::POST {
        body.map(|Json(b)| b).unwrap_or(query_params)
    } else {
        query_params
    };

    // stremio-core's play_on_device (models/streaming_server.rs:716-744) POSTs
    // here and treats a successful (2xx) response as `PlayingOnDevice` — a
    // 200 here would make official UIs report success while nothing plays.
    // Casting isn't implemented, so fail visibly instead.
    tracing::warn!(
        device_id = %dev_id,
        "Casting request rejected: device casting is not implemented"
    );
    let response_json = json!({
        "deviceId": dev_id,
        "status": "not_implemented",
        "error": { "message": "Device casting is not implemented" },
        "params": {
            "source": params.source,
            "paused": params.paused,
            "time": params.time,
            "volume": params.volume,
            "stop": params.stop,
            "audio_track": params.audio_track
        }
    });

    (StatusCode::NOT_IMPLEMENTED, Json(response_json))
}
