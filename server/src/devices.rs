//! The playback devices `GET /casting` reports, which is always none.
//!
//! stremio-core's `StreamingServer` model asks the server for the devices it
//! can cast to, and this is the shape it parses. The list behind it was
//! filled by an SSDP discovery loop -- an M-SEARCH for `MediaRenderer`, the
//! DIAL service and `ssdp:all` every thirty seconds -- that ran only where
//! `ServerConfig::enable_ssdp_discovery` was set, which is to say only in
//! the deleted daemon. It has been removed with the `ssdp-client` dependency
//! behind it.
//!
//! Nothing could be cast to a device it found in any case: `POST
//! /casting/{devID}/player` has always answered `501`, on purpose, because
//! stremio-core reads any `2xx` there as "playing on device" and a client
//! would report playback that never started. Discovery therefore filled a
//! list whose entries led nowhere -- and on Android, the one platform this
//! server actually runs on, M-SEARCH is multicast the app sandbox is not
//! permitted to send at all.
//!
//! **The type and the list stay.** `AppState::devices` is what `/casting`
//! reads and it is now always empty, which is the right answer from a server
//! that cannot cast: the embedder discovers receivers itself (xtremio does,
//! and uses the LAN media listener to feed them) and never asks this route.
//! A `404` instead would be a route stremio-core does not recognise.

/// One playback device, in the shape stremio-core's `StreamingServer` model
/// parses out of `GET /casting`. Nothing constructs one any more.
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
