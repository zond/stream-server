use crate::state::AppState;
use axum::{
    Json, Router,
    extract::Path,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Default, Deserialize)]
pub struct PlayerParams {
    pub source: Option<String>,
    pub paused: Option<String>,
    pub time: Option<f64>,
    pub volume: Option<f32>,
    pub stop: Option<String>,
    #[serde(rename = "audioTrack")]
    pub audio_track: Option<usize>,
}

/// The two casting calls stremio-core makes (models/streaming_server.rs):
/// `GET casting` for the device list and `POST casting/{device}/player`.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_devices))
        .route("/{devID}/player", post(player_control))
}

pub async fn list_devices(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl IntoResponse {
    let devices = state.devices.read().await;
    Json(devices.clone())
}

pub async fn player_control(
    Path(dev_id): Path<String>,
    body: Option<Json<PlayerParams>>,
) -> impl IntoResponse {
    let params = body.map(|Json(b)| b).unwrap_or_default();

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
