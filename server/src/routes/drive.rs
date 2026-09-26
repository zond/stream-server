//! **A paired Google Drive file, as something a player can open.**
//!
//! [`crate::sources::drive::DriveSource`] already reads one by range and
//! renews its own access token; this is what lets anything ask for one.
//! It is the archive layer's shape ([`crate::routes::archive`]) with a
//! single file where a container would be: [`open_file`] opens the file
//! and remembers it under a key, and a `GET /drive/stream/{key}` serves
//! that file's bytes.
//!
//! # Where the credential travels, and where it does not
//!
//! The refresh token is the longest-lived secret this device holds: it
//! does not expire on its own and it reaches every file the account has
//! picked through this OAuth client. So the two halves are split along
//! exactly that line.
//!
//! * **The open is a function call**, [`crate::ServerHandle::open_drive_file`],
//!   and the refresh token is one of its arguments. It is not a request at
//!   all: not a query parameter, not a path segment, not a header and not
//!   a body -- the request line is the half of a request that gets logged
//!   (by the tracing layer here, `routes::util::log_path`, by whatever an
//!   embedder puts in front, and by the diagnostics report the app lets a
//!   viewer copy), and a body is one copy more of the secret in flight.
//!   The `POST /drive/create` that once took it in a JSON body went with
//!   the other control routes the app never used (`control_router` in
//!   `lib.rs` says which remain and why). `/proxy`'s `h=Authorization:...`
//!   overrides are the existing way to put a credential on a relayed fetch
//!   and they are **not** reused here: they ride in the path, and an `h=`
//!   is a header value relayed verbatim, while what a Drive file needs is
//!   a *grant* that is spent for a new header every hour, which is
//!   `DriveCredential`'s whole job and cannot be a value copied into a URL.
//!
//! * **`GET /drive/stream/{key}` is an open media route** and carries a
//!   random key and nothing else. A player cannot send headers, so the URL
//!   handed to mpv has to be usable as it stands -- and this one is
//!   usable, meaningless to anyone who has not been given it, and
//!   unchanged by the token rotating under it. The token never reaches the
//!   player, the diagnostics log, the cast receiver or an `open` line.
//!
//! Nothing in this module writes either token anywhere. The only thing it
//! logs about a file is the file id, which is not a secret (it is the
//! cache key already, see [`DriveSource`]), and the [`DriveError`]
//! sentences, which are written in `sources::drive` rather than echoed
//! from a response body.
//!
//! # A dead pairing arrives as itself
//!
//! [`DriveError::PairAgain`] is terminal: the grant is gone and only a new
//! QR scan brings it back. An app that cannot tell it from "Google is
//! having a moment" shows a spinner for something that will never finish,
//! so the open answers it as a kind of its own
//! ([`DriveOpenError::is_pair_again`], `refused() == Some("pairAgain")`)
//! while everything else that could pass is a sentence. That is the
//! archive layer's own convention (`refused` is a kind the client switches
//! on, `error` is a sentence), and the switch is on the kind, never on the
//! English.
//!
//! The stream route answers the same way, over HTTP because a player is
//! what asks: a session whose pairing has since died is **`401` with
//! `{"refused":"pairAgain"}`** rather than a body that stops, because the
//! player re-opens and the app has to learn why.
//!
//! # Not on the LAN listener
//!
//! [`stream_routes`] is deliberately absent from `crate::lan_media_routes`.
//! The bytes are one account's private file, fetched with this device's
//! grant; a cast receiver is an unauthenticated stranger on the network,
//! and the rule that listener is built on is that a group is added by name
//! before it serves it. Nothing about casting a Drive file is designed
//! yet, so the safe default holds.

use crate::routes::archive::media_body;
use crate::routes::util::{self, MediaRange};
use crate::sources::drive::{DriveError, DrivePairing, DriveSource, GOOGLE_DRIVE_API};
use crate::sources::{ByteSource, ReadHint};
use crate::state::AppState;
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::StreamExt;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use url::Url;
use uuid::Uuid;

/// Where the two services a Drive file needs live, for this server.
///
/// **Neither is a caller's URL, and that is the point.** `DriveSource`'s
/// own note says why the API base is a constant rather than a parameter:
/// a caller who could name the origin would have this server fetch an
/// arbitrary host under a credential it renews and *cache* the answer
/// under the vouch `DriveSource` makes -- which is the open-relay shape
/// `/proxy` has and `DriveSource` deliberately does not. So the origin is
/// fixed here, at the server, and a create carries a file id.
///
/// The refresh endpoint is the embedder's rather than a constant: the
/// client secret lives in whatever pairing service the embedder runs, and
/// this repository ships none. A server configured with none answers
/// [`DriveOpenError::NoPairingService`] and creates nothing.
pub struct DriveEndpoints {
    /// `POST {"refreshToken":...}` -> `{"accessToken","expiresIn"}`, the
    /// service that holds the OAuth client secret.
    refresh: Url,
    /// Where Drive itself is: [`GOOGLE_DRIVE_API`] in a shipped build, a
    /// loopback origin in the tests.
    api_base: Url,
}

impl DriveEndpoints {
    /// The endpoints for a server whose embedder named `refresh_endpoint`,
    /// against Google.
    pub fn at(refresh_endpoint: Url) -> Self {
        Self {
            refresh: refresh_endpoint,
            api_base: Url::parse(GOOGLE_DRIVE_API).expect("a literal URL"),
        }
    }

    /// The same, against another origin entirely -- what a test points at
    /// its fake Drive. `#[doc(hidden)]`: an embedder has no use for it,
    /// and a build that let one through would be handing the vouch above
    /// to whoever set the configuration.
    #[doc(hidden)]
    pub fn against(refresh_endpoint: Url, api_base: Url) -> Self {
        Self {
            refresh: refresh_endpoint,
            api_base,
        }
    }

    /// The pairing for one file under this device's grant.
    ///
    /// **Built already expired**, with no access token at all: the app
    /// keeps only the refresh token (that is what the secure store holds),
    /// so there is none to hand over. The open's first `bearer()` therefore
    /// renews before its first request, which is not a special case -- it
    /// is the ordinary path `DriveCredential` takes a minute before every
    /// expiry, taken once at the start.
    pub(crate) fn pairing(&self, file_id: &str, refresh_token: &str) -> DrivePairing {
        DrivePairing {
            refresh_endpoint: self.refresh.clone(),
            refresh_token: refresh_token.to_string(),
            access_token: String::new(),
            expires_in: Duration::ZERO,
            file_id: file_id.to_string(),
            api_base: self.api_base.clone(),
        }
    }
}

/// One opened Drive file, for as long as anything is reading it.
///
/// It holds the source and nothing else: the credential is inside the
/// source, where the only thing that can reach it is the header supplier.
pub struct DriveSession {
    source: Arc<DriveSource>,
}

impl DriveSession {
    /// Whether the entity the viewer is playing is this file -- the rule
    /// `Sessions::retain` is given at the switch, exactly as a translated
    /// container's is (`crate::run`). A source is the only thing that
    /// knows which retained entity its bytes are.
    pub fn is_live(&self, reading: &enginefs::retention::live::Reading) -> bool {
        self.source.is_live(reading)
    }
}

/// What an open answers: a key, the path a player fetches, and the three
/// facts about the file that were learned by opening it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DriveFileOpened {
    /// The session key. Random, so it is not guessable from the file id.
    pub key: String,
    /// Where the bytes are: `/drive/stream/{key}` as a route answers, and
    /// an absolute URL on this server as
    /// [`crate::ServerHandle::open_drive_file`] answers.
    pub url: String,
    /// The name the caller gave, echoed so a player has something to show.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// What Drive labelled the bytes.
    pub content_type: String,
    /// How long the file is.
    pub length: u64,
}

/// Why a Drive file could not be opened, as this layer answers it.
#[derive(Debug)]
pub enum DriveOpenError {
    /// This server was configured with no pairing service, so there is
    /// nothing to renew a token against. A fact about the build and its
    /// embedder, like the archive layer's `noReader`, and never about the
    /// viewer's account.
    NoPairingService,
    /// Everything the source layer can say, `PairAgain` above all.
    Drive(DriveError),
}

impl std::fmt::Display for DriveOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoPairingService => write!(
                f,
                "this build has no pairing service to renew a Google Drive token against"
            ),
            Self::Drive(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for DriveOpenError {}

impl From<DriveError> for DriveOpenError {
    fn from(error: DriveError) -> Self {
        Self::Drive(error)
    }
}

impl DriveOpenError {
    /// Whether this is the terminal one: the grant is gone and only a new
    /// pairing brings it back. **The one question a caller asks without
    /// matching English.**
    pub fn is_pair_again(&self) -> bool {
        matches!(self, Self::Drive(DriveError::PairAgain))
    }

    /// The machine-readable kind an app switches on, for the refusals that
    /// have one. `None` is a plain failure and carries a sentence instead.
    pub fn refused(&self) -> Option<&'static str> {
        match self {
            Self::NoPairingService => Some("noPairingService"),
            Self::Drive(DriveError::PairAgain) => Some("pairAgain"),
            Self::Drive(_) => None,
        }
    }
}

/// Open the file `file_id` under `refresh_token` and remember it, or say
/// why it could not be opened.
///
/// The one place an open happens, behind
/// [`crate::ServerHandle::open_drive_file`] -- which is how the app asks,
/// since the app never speaks HTTP to this server.
pub(crate) async fn open_file(
    state: &AppState,
    file_id: &str,
    refresh_token: &str,
    name: Option<String>,
) -> Result<DriveFileOpened, DriveOpenError> {
    let endpoints = state
        .drive
        .as_ref()
        .ok_or(DriveOpenError::NoPairingService)?;
    // A finished download first, and off the disk: it is opened without
    // the origin and without spending the grant, which is what lets it
    // play on a device that is offline (`crate::proxy_downloads`). The
    // ordinary open below probes Drive, and would refuse exactly then.
    if let Some(downloaded) =
        crate::proxy_downloads::complete_drive_download(state, file_id, name.clone()).await
    {
        tracing::info!(file = %file_id, "a Google Drive file is open from its finished download");
        return Ok(downloaded);
    }
    let pairing = endpoints.pairing(file_id, refresh_token);
    Ok(open_pairing(state, pairing, name).await?)
}

/// [`open_file`] from a pairing already built -- the seam the tests reach,
/// since a shipped build's endpoints are not a caller's to name.
pub(crate) async fn open_pairing(
    state: &AppState,
    pairing: DrivePairing,
    name: Option<String>,
) -> Result<DriveFileOpened, DriveError> {
    // The id, and never the token: the id names content and is the cache
    // key already, while the token is the account itself.
    let file_id = pairing.file_id.clone();
    let source = DriveSource::open(state, pairing).await?;
    let source = match &name {
        Some(name) => source.named(name.clone()),
        None => source,
    };
    let opened = DriveFileOpened {
        key: Uuid::new_v4().to_string(),
        url: String::new(),
        name: source.name().map(str::to_string),
        content_type: source.content_type().to_string(),
        length: source.len(),
    };
    tracing::info!(
        file = %file_id,
        length = opened.length,
        "a Google Drive file is open and readable by range"
    );
    let opened = DriveFileOpened {
        url: stream_path(&opened.key),
        ..opened
    };
    drop(state.drive_files.insert(
        opened.key.clone(),
        DriveSession {
            source: Arc::new(source),
        },
    ));
    Ok(opened)
}

/// Where a key's bytes are, as this server's own path.
pub(crate) fn stream_path(key: &str) -> String {
    format!("/drive/stream/{}", urlencoding::encode(key))
}

/// The one route: the byte-serving half, **open**, because a player cannot
/// send a bearer header, and safe to be open because the URL carries a
/// random key and no credential at all. (The open itself is
/// [`open_file`], reached through the embed API and no route.)
pub fn stream_routes() -> Router<AppState> {
    Router::new().route("/drive/stream/{key}", get(stream_drive_file))
}

/// The one shape a dead pairing has, wherever it is found. `401` because
/// the credential is what is no good, and a kind beside the sentence
/// because the app's answer is a new QR and it must not have to read
/// English to know that.
fn pair_again_response() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({
            "refused": "pairAgain",
            "message": DriveError::PairAgain.to_string(),
        })),
    )
        .into_response()
}

/// One Drive file, as a range of bytes.
///
/// **Nothing about it differs from a plain file over HTTP**: the framing
/// is `util::MediaRange`, the same one the torrent stream route and the
/// archive member route use, so `Content-Length`, `Content-Range`,
/// `206`/`416` and `HEAD` are the same answers here as there.
async fn stream_drive_file(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: header::HeaderMap,
) -> Response {
    let Some(session) = state.drive_files.get(&key) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // Asked before a byte is framed. The grant can die between the create
    // and a seek an hour later, and a player's answer to a body that stops
    // is to open again -- so what it opens into has to be the reason.
    if session.source.needs_pairing_again() {
        return pair_again_response();
    }
    let size = session.source.len();
    let range = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok());
    let Some(framing) = MediaRange::of(range, size) else {
        return util::range_not_satisfiable(size);
    };
    let wanted = framing.content_length(size);
    let reader = match session
        .source
        .open(framing.start, ReadHint::of(wanted))
        .await
    {
        Ok(reader) => reader,
        Err(error) => return read_error_response(&error),
    };

    let mut res_headers = header::HeaderMap::new();
    res_headers.insert(
        header::CONTENT_TYPE,
        session
            .source
            .content_type()
            .parse()
            .unwrap_or_else(|_| header::HeaderValue::from_static("application/octet-stream")),
    );
    framing.write_headers(size, &mut res_headers);
    crate::routes::compat::add_dlna_headers(&mut res_headers);

    // The lease rides inside the body, so the session cannot be evicted or
    // dropped at a switch while a player is still reading it -- the same
    // way an archive member's body holds its session.
    let body = Body::from_stream(media_body(reader.take(wanted)).map(move |chunk| {
        let _in_use = &session;
        chunk
    }));
    (framing.status(), res_headers, body).into_response()
}

/// What a read that failed before the body began answers. A dead pairing
/// is itself here too; anything else is a gateway that would not serve us.
fn read_error_response(error: &std::io::Error) -> Response {
    if let Some(DriveError::PairAgain) = DriveError::in_read(error) {
        return pair_again_response();
    }
    tracing::warn!(%error, "a Google Drive file could not be read");
    (
        StatusCode::BAD_GATEWAY,
        Json(serde_json::json!({ "error": "that file could not be read from Google Drive" })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stream_path_carries_the_key_and_nothing_else() {
        let path = stream_path("a b/c");
        assert_eq!(path, "/drive/stream/a%20b%2Fc");
    }

    /// The kinds an app switches on, stated once so a rename has to come
    /// past a test.
    #[test]
    fn a_dead_pairing_has_a_kind_of_its_own() {
        assert!(DriveOpenError::Drive(DriveError::PairAgain).is_pair_again());
        assert_eq!(
            DriveOpenError::Drive(DriveError::PairAgain).refused(),
            Some("pairAgain")
        );
        assert!(!DriveOpenError::NoPairingService.is_pair_again());
        assert_eq!(
            DriveOpenError::Drive(DriveError::Refused(500)).refused(),
            None
        );
    }
}
