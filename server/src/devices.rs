//! The playback devices `GET /casting` reports, which is always none.
//!
//! stremio-core's `StreamingServer` model asks the server for the devices it
//! can cast to, and this is the shape it parses. Nothing here discovers
//! devices, and nothing could be cast to one if it did: `POST
//! /casting/{devID}/player` always answers `501`, on purpose, because
//! stremio-core reads any `2xx` there as "playing on device" and a client
//! would report playback that never started. On Android, the one platform
//! this server actually runs on, M-SEARCH is also multicast the app sandbox
//! is not permitted to send.
//!
//! **The type and the route stay; there is no list behind them.** `/casting`
//! answers an empty list of this type, which is the right answer from a
//! server that cannot cast: the embedder discovers receivers itself (xtremio
//! does, and uses the LAN media listener to feed them) and never asks this
//! route. A `404` instead would be a route stremio-core does not recognise.

/// One playback device, in the shape stremio-core's `StreamingServer` model
/// parses out of `GET /casting`. Nothing constructs one.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Device {
    pub id: String,
    pub name: String,
    pub address: String,
    pub port: u16,
    #[serde(rename = "type")]
    pub device_type: String,
    pub model: Option<String>,
}
