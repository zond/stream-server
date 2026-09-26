//! **A file in somebody's Google Drive as a [`ByteSource`]**, read by
//! range under an access token this server renews for itself.
//!
//! The shape the design named (`docs/translated-sources.md`): "`DriveSource`
//! (Google Drive; ranged `GET` with the addon's OAuth header, which is a
//! `ProxySource` with a header supplier that refreshes)". That is what this
//! is. Drive serves `files/{id}?alt=media` with a `206` for a `Range`, so
//! everything the archive layer already does over a link -- one request per
//! span, a seek that is a new request, the proxy cache's narrowing and
//! `If-Range` -- works over a Drive file unchanged. The one thing Drive
//! needs that a link does not is the header, and the whole of this module
//! is about that header.
//!
//! # The token, and why the renewal is here rather than in the app
//!
//! A pairing is made once, elsewhere: a phone signs in, picks a file, and
//! the television collects a refresh token, an access token and the file's
//! id ([`DrivePairing`]). Access tokens last about an hour. Films do not.
//! So the credential cannot be a value captured at construction, and the
//! renewal cannot be a call back into the app: a token expiring in the
//! middle of a byte range must not need a round trip across FFI on a
//! device whose frame budget is already spent. [`DriveCredential`] is a
//! [`OwnGrant`] -- asked for headers once per request, on this side of
//! that boundary, in Rust.
//!
//! Four things it is careful about, each of which is a stall or a leak if
//! it is got wrong:
//!
//! * **Renewed before it bites.** A read that `401`s mid-film is a stall
//!   the viewer sees and then a retry they wait through. The credential
//!   knows when the token expires and renews while there is still
//!   [`RENEW_MARGIN`] left, so the origin is never handed one it will
//!   refuse.
//! * **A renewal is a wait, not a failure.** Reads already streaming carry
//!   the token they left with and are untouched: Google does not revoke a
//!   response it has begun framing. A read that has *not* yet built its
//!   request waits on the one renewal -- one round trip, measured under a
//!   second -- and then goes with the new token. Nothing is cancelled and
//!   nothing is retried. The margin exists precisely so that this wait
//!   falls between two reads rather than inside a body.
//! * **One refresh at a time.** The token is behind a `Mutex` and the
//!   freshness check is *inside* it, so several reads meeting one expiry
//!   make one request: the first renews, the rest find its token and go.
//!   A renewal that fails for a reason that could pass -- the pairing
//!   service unreachable -- leaves the old token in place and the lock
//!   open, so the read that met the failure fails and the next read tries
//!   again. One failure does not poison the source.
//! * **`pairAgain` is terminal.** `invalid_grant` means the grant is gone
//!   and no retry can bring it back; only a new QR scan can. It is
//!   recorded ([`DriveCredential::is_dead`]) and every later read fails at
//!   once with [`DriveError::PairAgain`] rather than asking again, so the
//!   app shows a code instead of a spinner and Google is not hammered by
//!   a player that keeps seeking.
//!
//! # What is never written down
//!
//! Neither token reaches a log, a `describe`, or an error string.
//! [`ByteSource::describe`] is `ProxySource`'s -- the origin and nothing
//! else, so not even the file id -- and every [`DriveError`] is a sentence
//! written here rather than a body echoed back, because the body of a
//! failed refresh is somebody else's text and may contain anything that
//! was sent to them. `DriveCredential` derives no `Debug` for the same
//! reason: a `{:?}` in a trace is the commonest way a secret gets filed.

use super::proxy::{Credentials, OwnGrant, ProxySource, ProxySourceError, Vouch};
use super::{ByteSource, ReadHint, SeekableReader};
use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::Instant;
use url::Url;

/// Where Drive serves file bytes from. A constant because it is not a
/// caller's URL: this server builds it from a file id, which is what keeps
/// `DriveSource` out of the open-relay shape `/proxy` has.
pub const GOOGLE_DRIVE_API: &str = "https://www.googleapis.com/";

/// How long before an access token expires the credential renews it.
///
/// A minute. What it buys is that the wait for a renewal lands *between*
/// two reads rather than inside one: a read issued at the last moment
/// still has the better part of a minute of validity when it reaches
/// Google, which is more than a ranged `GET` over domestic wifi takes to
/// be accepted. What it costs is one extra refresh an hour at most, which
/// is one request against a token that lasts 3599 seconds.
///
/// It is deliberately much larger than any plausible clock skew between
/// this device and Google, and deliberately much smaller than the token's
/// life, so it can never make a fresh token look stale.
const RENEW_MARGIN: Duration = Duration::from_secs(60);

/// How long a refresh may take before it is a failure. The pairing service
/// answers in well under a second; a request still open after this is a
/// network that is not going to come back inside a viewer's patience, and
/// a read waiting on it is a spinner.
const REFRESH_TIMEOUT: Duration = Duration::from_secs(10);

/// The most of a refresh answer that is read. It is a two-field JSON
/// object; anything past this is not one, and reading it whole would let
/// whatever is on the other end of the pairing URL decide how much memory
/// this process uses.
const MAX_REFRESH_BODY: usize = 8 * 1024;

/// Why a Drive file could not be read. Each is a sentence an app can show,
/// and **none of them carries a token**: the wording is written here, not
/// taken from a response body.
#[derive(Debug)]
pub enum DriveError {
    /// **The pairing is dead.** The refresh token has been revoked or has
    /// expired, the pairing service said so (`invalid_grant` ->
    /// `pairAgain`), and no amount of retrying will change it: a grant
    /// that is gone cannot be renewed into existence. The only thing that
    /// makes this file readable again is a new pairing -- a QR on the
    /// television, a phone that signs in.
    ///
    /// **Terminal, and distinct for that reason.** An app that cannot tell
    /// this from "the network is having a moment" shows a spinner for
    /// something that will never finish.
    PairAgain,
    /// The pairing service could not be reached at all, or did not answer
    /// in time. Worth trying again; the grant may be perfectly good.
    Unreachable(String),
    /// It was reached and answered something that is not a token.
    Refused(u16),
    /// The grant is fine and Drive would not serve the file as a source.
    Source(ProxySourceError),
}

impl std::fmt::Display for DriveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PairAgain => write!(
                f,
                "this device is no longer linked to that Google account, so scan the code again \
                 to link it"
            ),
            Self::Unreachable(error) => write!(
                f,
                "the service that keeps this device linked could not be reached: {error}"
            ),
            Self::Refused(status) => write!(
                f,
                "the service that keeps this device linked answered {status}"
            ),
            Self::Source(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for DriveError {}

impl DriveError {
    /// The typed reason a *read* failed, when it failed for this module's
    /// sake -- above all [`Self::PairAgain`], which a caller has to be
    /// able to tell apart without matching English.
    ///
    /// A read answers `io::Error` because that is what a [`ByteSource`]
    /// answers; the error a mint failed with is boxed inside it, and this
    /// is how it comes back out.
    pub fn in_read(error: &io::Error) -> Option<&Self> {
        error.get_ref()?.downcast_ref::<Self>()
    }
}

impl From<ProxySourceError> for DriveError {
    fn from(error: ProxySourceError) -> Self {
        // A mint that failed inside `ProxySource::open` carries the typed
        // error it failed with, so the terminal case survives the probe as
        // itself rather than arriving as a sentence about a link.
        if let ProxySourceError::Credentials(inner) = &error
            && let Some(DriveError::PairAgain) = DriveError::in_read(inner)
        {
            return Self::PairAgain;
        }
        Self::Source(error)
    }
}

/// Everything one paired file needs, as the pairing service hands it over
/// once and the app keeps it.
///
/// **Nothing in this repository is ever an instance of this.** A refresh
/// token and a file id belong to whoever's Drive they name; they arrive at
/// run time from the app and the tests build their own against a fake
/// origin.
pub struct DrivePairing {
    /// Where a refresh is asked for: a `POST` of `{"refreshToken": ...}`
    /// that answers `{"accessToken", "expiresIn"}`, or `401` with
    /// `{"pairAgain": true}` when the grant is gone. The service, not
    /// Google: the client secret lives there and never on the device.
    pub refresh_endpoint: Url,
    /// The long-lived grant. Secret.
    pub refresh_token: String,
    /// The access token the pairing arrived with, already valid. Secret.
    pub access_token: String,
    /// How much of that token's life was left when it was measured.
    /// **A duration and not an instant on purpose**: an absolute
    /// wall-clock time would be compared against a television's own clock,
    /// which is set by whatever DHCP handed it and is routinely wrong by
    /// hours, and a token believed expired is a refresh per read while one
    /// believed fresh is the `401` this exists to avoid.
    pub expires_in: Duration,
    /// Which file in that Drive. Not a secret, and **it is what the cache
    /// key is**: the id names the content, whoever happens to be entitled
    /// to fetch it, which is the assertion [`DriveSource`] vouches with.
    pub file_id: String,
    /// Where Drive itself is. [`GOOGLE_DRIVE_API`] in every shipped build;
    /// a loopback origin in the tests, which is how they read a Drive file
    /// without one existing.
    pub api_base: Url,
}

impl DrivePairing {
    /// A pairing against Google, which is every real one.
    pub fn new(
        refresh_endpoint: Url,
        refresh_token: impl Into<String>,
        access_token: impl Into<String>,
        expires_in: Duration,
        file_id: impl Into<String>,
    ) -> Self {
        Self {
            refresh_endpoint,
            refresh_token: refresh_token.into(),
            access_token: access_token.into(),
            expires_in,
            file_id: file_id.into(),
            api_base: Url::parse(GOOGLE_DRIVE_API).expect("a literal URL"),
        }
    }

    /// Where this file's bytes are asked for.
    pub(crate) fn media_url(&self) -> Result<Url, DriveError> {
        let mut url = self.api_base.clone();
        url.path_segments_mut()
            .map_err(|()| DriveError::Unreachable("the Drive API base cannot be a base".into()))?
            .pop_if_empty()
            .extend(["drive", "v3", "files", &self.file_id]);
        url.set_query(Some("alt=media"));
        Ok(url)
    }
}

/// The access token as it stands, and when it stops standing.
struct Token {
    bearer: String,
    expires_at: Instant,
}

impl Token {
    /// Whether this token is still worth sending: not merely unexpired,
    /// but with enough life left that the request it goes on will still be
    /// accepted when it lands. See [`RENEW_MARGIN`].
    fn usable_at(&self, now: Instant) -> bool {
        self.expires_at > now + RENEW_MARGIN
    }
}

/// **The one thing in this process that holds a Google refresh token**, and
/// the only thing that turns it into an `Authorization`.
///
/// No `Debug`, by intent: see the module docs.
pub struct DriveCredential {
    refresh_endpoint: Url,
    refresh_token: String,
    /// The current token. The lock is what makes several reads meeting one
    /// expiry into one refresh, and the freshness check lives *inside* it
    /// -- a check outside would let every waiter decide to renew before
    /// any of them had.
    token: tokio::sync::Mutex<Token>,
    /// Set once the pairing service has said the grant is gone. Read
    /// before the lock is taken, so a dead pairing costs a seeking player
    /// an atomic load per read rather than a queue behind a mutex.
    dead: AtomicBool,
}

/// The two fields a successful refresh answers with.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RefreshedToken {
    access_token: String,
    expires_in: u64,
}

/// And what a refusal answers with. `pairAgain` is the one field that
/// matters: it is the service saying the grant is gone rather than that
/// this request went wrong.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RefreshRefusal {
    #[serde(default)]
    pair_again: bool,
}

impl DriveCredential {
    /// Hold a pairing's grant, with the access token it arrived with.
    pub fn new(
        refresh_endpoint: Url,
        refresh_token: String,
        access_token: String,
        valid_for: Duration,
    ) -> Self {
        Self {
            refresh_endpoint,
            refresh_token,
            token: tokio::sync::Mutex::new(Token {
                bearer: access_token,
                expires_at: Instant::now() + valid_for,
            }),
            dead: AtomicBool::new(false),
        }
    }

    /// Whether the pairing has been found dead. Once true, always true.
    pub fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Relaxed)
    }

    /// Age the token in hand to the end of its life, as an hour of real
    /// time would.
    ///
    /// The test hook the alternatives are worse than: the clock this
    /// compares against is `tokio::time::Instant`, and a paused runtime
    /// auto-advances whenever it is idle -- which a fake origin on its own
    /// blocking threads makes it, repeatedly and unpredictably -- so
    /// `advance()` would decide for itself how many expiries a test saw.
    /// Waiting out the real margin is a minute per test. This states the
    /// one thing the test is about.
    #[cfg(test)]
    async fn expire_now(&self) {
        self.token.lock().await.expires_at = Instant::now();
    }

    /// The access token to send now, renewing first if the one in hand is
    /// near enough its end to be refused by the time it lands.
    ///
    /// **Serialised, and that is the feature.** Everything between taking
    /// the lock and putting the new token back is one refresh; a second
    /// read that arrives during it waits, and then finds a token that is
    /// good for another hour rather than deciding to fetch one of its own.
    pub async fn bearer(&self) -> Result<String, DriveError> {
        if self.is_dead() {
            return Err(DriveError::PairAgain);
        }
        let mut held = self.token.lock().await;
        if held.usable_at(Instant::now()) {
            return Ok(held.bearer.clone());
        }
        // Asked again under the lock: the waiter in front may have been the
        // one that discovered the grant is gone, and a second request to
        // find that out again is a request that can only be refused.
        if self.is_dead() {
            return Err(DriveError::PairAgain);
        }
        let renewed = self.renew().await?;
        *held = renewed;
        Ok(held.bearer.clone())
    }

    /// One `POST` to the pairing service. The refresh token goes in the
    /// body and never in a URL: a query string is the half of a request
    /// that gets logged by everything it passes.
    async fn renew(&self) -> Result<Token, DriveError> {
        let client = crate::routes::proxy::http_client()
            .ok_or_else(|| DriveError::Unreachable("no HTTP client".to_string()))?;
        let response = client
            .post(self.refresh_endpoint.clone())
            .timeout(REFRESH_TIMEOUT)
            .json(&serde_json::json!({ "refreshToken": self.refresh_token }))
            .send()
            .await
            // The reqwest error's own text names the URL and nothing else
            // of ours -- there is no secret in a pairing endpoint -- but
            // the body is never in it, which is what matters.
            .map_err(|error| DriveError::Unreachable(error.to_string()))?;
        let status = response.status();
        let body = read_capped(response, MAX_REFRESH_BODY).await?;
        if status.is_success() {
            let RefreshedToken {
                access_token,
                expires_in,
            } = serde_json::from_slice(&body).map_err(|_| DriveError::Refused(status.as_u16()))?;
            return Ok(Token {
                bearer: access_token,
                expires_at: Instant::now() + Duration::from_secs(expires_in),
            });
        }
        // `pairAgain` and not the status: the service is what knows
        // whether `invalid_grant` was behind this, and a `401` without it
        // is an answer about this request rather than about the grant.
        if serde_json::from_slice::<RefreshRefusal>(&body).is_ok_and(|refusal| refusal.pair_again) {
            self.dead.store(true, Ordering::Relaxed);
            return Err(DriveError::PairAgain);
        }
        Err(DriveError::Refused(status.as_u16()))
    }
}

/// A response body read whole but never past `max`: refused on a declared
/// length over it, and otherwise at the first chunk that crosses it.
///
/// `Response::bytes` would buffer whatever the far end sends before anyone
/// could look at the length, so a cap checked afterwards bounds nothing.
async fn read_capped(mut response: reqwest::Response, max: usize) -> Result<Vec<u8>, DriveError> {
    let too_large = || DriveError::Unreachable("the refresh answer was too large".to_string());
    if response
        .content_length()
        .is_some_and(|len| len > max as u64)
    {
        return Err(too_large());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| DriveError::Unreachable(error.to_string()))?
    {
        if body.len() + chunk.len() > max {
            return Err(too_large());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[async_trait::async_trait]
impl OwnGrant for DriveCredential {
    async fn headers(&self) -> io::Result<BTreeMap<String, String>> {
        let bearer = self.bearer().await.map_err(io::Error::other)?;
        Ok(BTreeMap::from([(
            "Authorization".to_string(),
            format!("Bearer {bearer}"),
        )]))
    }
}

/// **A Drive file, read by range.**
///
/// A [`ProxySource`] over `files/{id}?alt=media` with a
/// [`DriveCredential`] supplying its header, which is the whole of it:
/// every question about ranges, seeks, caching and retention is already
/// answered one layer down, and the answers do not change because the
/// authorisation is ours instead of a caller's.
///
/// # Cached, and this is the source that vouches for it
///
/// `ProxyCache::entry` refuses every credentialed read and is not relaxed
/// by any of this. What is here instead is the assertion that rule has no
/// way of making for itself: **`files/{id}?alt=media` identifies the
/// bytes**. The same id is the same file for everyone entitled to it, so
/// the credential authorises the *fetch* and does not determine the
/// *result* -- which is what lets the key be the URL, with no token in it
/// and nothing that churns when the token rotates every hour.
///
/// So this source passes `Vouch::UrlIdentifiesBytes`, in as many words, at
/// the one place a source is constructed. A source that does not say so
/// is not cached. The full cost of saying so -- including who can read
/// those bytes back out of the store without being authorised for them,
/// and why the owner accepted that -- is written where the key is minted:
/// `crate::proxy_cache::ProxyCache::entry_for_vouched_url`. Read it before
/// vouching for anything else.
///
/// What a viewer gets for it: a backward seek inside what the cache still
/// holds is a disk read rather than a round trip to Google over the
/// television's wifi. The player keeps an 8 MiB back-window and nothing
/// more, so without this every seek back beyond a few seconds of film is a
/// new request and a re-download of bytes this device already had.
///
/// # The generation, which is a separate question
///
/// A vouch says the URL names the file. It does not say *which version* of
/// it, and the cache never revalidates -- so a file edited in Drive under
/// the same id must not be served from the old bytes. That is the entity
/// directory's job, not the key's: it is named for the origin's own
/// validator, a fill that finds a different one drops what was held of the
/// old generation, and **an origin that names neither `ETag` nor
/// `Last-Modified` is read and never kept at all**
/// (`routes::proxy::cacheable_entity`).
///
/// Which of those Drive does is the one thing here that has not been
/// measured against the real API, and it is deliberately not guessed:
/// [`Self::validator`] answers it per file at open, and a file opened
/// without one logs that it will not be kept. If Drive turns out to name
/// nothing, the honest fix is to make this source supply a validator from
/// the file's own `md5Checksum`/`modifiedTime` metadata -- one extra JSON
/// call at open -- rather than to keep bytes nothing can date.
pub struct DriveSource {
    inner: ProxySource,
    credential: Arc<DriveCredential>,
    /// The file's name as the pairing stated it, for a caller that has to
    /// say what is being played. Never part of `describe`.
    name: Option<String>,
}

impl DriveSource {
    /// Open a paired file as a source, or say why it could not be one.
    ///
    /// The token is minted **before** the probe rather than inside it, so
    /// that a dead pairing is [`DriveError::PairAgain`] at the first
    /// attempt and not a sentence about a link that could not be
    /// authorised. Everything after that is `ProxySource`'s: one ranged
    /// `GET` of `bytes=0-0`, which is how the entity's length, type and
    /// validator are learned and how an origin that will not range is
    /// refused.
    ///
    /// It takes the server rather than a cache because a `ProxyCache` is
    /// not part of the embeddable API -- the same reason
    /// [`ProxySource::open`] is `pub(crate)` -- and what a caller has in
    /// its hand is the server.
    pub async fn open(state: &crate::AppState, pairing: DrivePairing) -> Result<Self, DriveError> {
        Self::open_against(state.proxy_cache.clone(), state.http_addr, pairing).await
    }

    /// The same, against a cache and a listener directly: what the tests
    /// hold, and what a route would pass if it ever had the two without
    /// the state around them.
    pub(crate) async fn open_against(
        cache: Arc<crate::proxy_cache::ProxyCache>,
        self_addr: SocketAddr,
        pairing: DrivePairing,
    ) -> Result<Self, DriveError> {
        let url = pairing.media_url()?;
        let credential = Arc::new(DriveCredential::new(
            pairing.refresh_endpoint,
            pairing.refresh_token,
            pairing.access_token,
            pairing.expires_in,
        ));
        credential.bearer().await?;
        let inner = ProxySource::open(
            cache,
            self_addr,
            url,
            Credentials::Own {
                grant: credential.clone(),
                // **The vouch, made here and nowhere else.** A Drive file
                // id names one file's content for everyone entitled to
                // fetch it, so the URL identifies the bytes and the
                // credential only buys the right to ask for them. This
                // line is the whole of why these reads are cached; see
                // the note on [`DriveSource`] for what it costs.
                vouch: Vouch::UrlIdentifiesBytes {
                    vouched_by: "DriveSource",
                },
            },
        )
        .await?;
        if inner.validator().is_none() {
            // Not a failure -- the file reads perfectly well -- but the
            // one operational fact worth knowing, because it decides
            // whether the vouch buys anything at all. See the note on
            // [`DriveSource`].
            tracing::info!(
                origin = %ByteSource::describe(&inner),
                "Drive named no validator for this file, so nothing of it will be kept"
            );
        }
        Ok(Self {
            inner,
            credential,
            name: None,
        })
    }

    /// The name to show the file under, when the pairing carried one.
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// What the pairing called the file, if anything.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// The type Drive labelled the bytes with.
    pub fn content_type(&self) -> &str {
        self.inner.content_type()
    }

    /// How Drive identifies this generation of the file, or `None` for a
    /// response that identified it by nothing -- in which case nothing of
    /// it is kept. See the caching note on [`DriveSource`].
    pub fn validator(&self) -> Option<&str> {
        self.inner.validator()
    }

    /// Whether the pairing behind this source has been found dead. The
    /// same fact a failed read carries as [`DriveError::PairAgain`], asked
    /// of the source instead of an error.
    pub fn needs_pairing_again(&self) -> bool {
        self.credential.is_dead()
    }
}

impl DriveSource {
    /// The proxy source underneath, quiet, for a download's filler
    /// ([`crate::proxy_downloads`]): the same media URL, the same renewing
    /// credential, the same vouched cache key.
    pub(crate) fn filling_source(&self) -> ProxySource {
        self.inner.for_filling()
    }

    /// The key directory the file is cached under: see
    /// [`ProxySource::key_dir`].
    pub(crate) fn key_dir(&self) -> Option<std::path::PathBuf> {
        self.inner.key_dir()
    }
}

#[async_trait::async_trait]
impl ByteSource for DriveSource {
    fn len(&self) -> u64 {
        self.inner.len()
    }

    /// The origin and nothing else -- so not the token, and not the file
    /// id either. It is `ProxySource`'s, which is `log_origin` of the
    /// media URL.
    fn describe(&self) -> String {
        self.inner.describe()
    }

    fn is_live(&self, reading: &enginefs::retention::live::Reading) -> bool {
        self.inner.is_live(reading)
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read_at(offset, buf).await
    }

    async fn open(&self, offset: u64, hint: ReadHint) -> io::Result<Box<dyn SeekableReader>> {
        self.inner.open(offset, hint).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    /// A string that exists nowhere but in this test's own secrets, so a
    /// leak of one is a substring search away.
    const REFRESH_TOKEN: &str = "refresh-tok-3f9a-never-log-me";
    /// The token the pairing arrives with. The fake Drive rejects it once
    /// a refresh has happened, which is how a test proves a renewal really
    /// went out rather than merely that a read succeeded.
    const FIRST_ACCESS_TOKEN: &str = "access-tok-0001-never-log-me";

    const FILE_LENGTH: usize = 512 * 1024;

    /// The film's bytes: a pattern, so a range can be checked to have come
    /// from the offset it claims and not merely to be the right length.
    fn byte_at(offset: usize) -> u8 {
        (offset.wrapping_mul(7) % 251) as u8
    }

    /// How the fake pairing service answers a refresh.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Refreshes {
        /// A new access token every time.
        Yes,
        /// `401` with `pairAgain`, and -- to prove the error text is
        /// written here rather than echoed -- the refresh token in the
        /// body, which is what a naive service would do.
        PairAgain,
    }

    /// One loopback listener standing in for both Google and the pairing
    /// service: `/refresh` mints tokens, `/drive/v3/files/...` serves
    /// ranges to whoever holds the current one.
    struct Fake {
        addr: SocketAddr,
        /// Refreshes asked for.
        refreshes: Arc<AtomicUsize>,
        /// Ranged reads Drive answered with a `206`.
        reads: Arc<AtomicUsize>,
        /// Ranged reads Drive refused for a token it never issued.
        rejections: Arc<AtomicUsize>,
        tokens: Arc<Mutex<Tokens>>,
    }

    /// The tokens this fake has handed out, and the one the last read
    /// arrived with.
    ///
    /// **Every issued token stays valid**, which is Google's own behaviour:
    /// a refresh mints a new access token and does not revoke the one in
    /// flight. A fake that revoked would make the "one refresh" test pass
    /// for the wrong reason -- the reads would be *forced* to serialise --
    /// and would model something Drive does not do. What a test asserts
    /// instead is `last_bearer`: which token a read actually went out
    /// under.
    struct Tokens {
        issued: Vec<String>,
        last_bearer: Option<String>,
    }

    impl Fake {
        fn start(mode: Refreshes) -> Fake {
            let listener = TcpListener::bind(("127.0.0.1", 0)).expect("a loopback port");
            let addr = listener.local_addr().expect("the bound address");
            let refreshes = Arc::new(AtomicUsize::new(0));
            let reads = Arc::new(AtomicUsize::new(0));
            let rejections = Arc::new(AtomicUsize::new(0));
            let tokens = Arc::new(Mutex::new(Tokens {
                issued: vec![FIRST_ACCESS_TOKEN.to_string()],
                last_bearer: None,
            }));
            let fake = Fake {
                addr,
                refreshes: refreshes.clone(),
                reads: reads.clone(),
                rejections: rejections.clone(),
                tokens: tokens.clone(),
            };
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(stream) = stream else { break };
                    let tokens = tokens.clone();
                    let (refreshes, reads, rejections) =
                        (refreshes.clone(), reads.clone(), rejections.clone());
                    // A thread per connection: the concurrency test has
                    // several reads open at once and a serial fake would
                    // turn its question into a queue.
                    std::thread::spawn(move || {
                        serve(stream, mode, &tokens, &refreshes, &reads, &rejections)
                    });
                }
            });
            fake
        }

        /// What the last request Drive answered was authorised with, minus
        /// the `Bearer `: how a test says *which* token a read went out
        /// under rather than merely that it worked.
        fn last_bearer(&self) -> Option<String> {
            self.tokens.lock().expect("the tokens").last_bearer.clone()
        }

        fn refresh_endpoint(&self) -> Url {
            Url::parse(&format!("http://{}/refresh", self.addr)).expect("a literal URL")
        }

        fn pairing(&self, valid_for: Duration) -> DrivePairing {
            let mut pairing = DrivePairing::new(
                self.refresh_endpoint(),
                REFRESH_TOKEN,
                FIRST_ACCESS_TOKEN,
                valid_for,
                "a-file-id",
            );
            pairing.api_base =
                Url::parse(&format!("http://{}/", self.addr)).expect("a literal URL");
            pairing
        }

        fn refreshes(&self) -> usize {
            self.refreshes.load(Ordering::SeqCst)
        }

        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }

        fn rejections(&self) -> usize {
            self.rejections.load(Ordering::SeqCst)
        }
    }

    fn serve(
        mut stream: std::net::TcpStream,
        mode: Refreshes,
        tokens: &Mutex<Tokens>,
        refreshes: &AtomicUsize,
        reads: &AtomicUsize,
        rejections: &AtomicUsize,
    ) {
        let mut reader = BufReader::new(stream.try_clone().expect("a second handle"));
        let mut request_line = String::new();
        if reader.read_line(&mut request_line).expect("a request line") == 0 {
            return;
        }
        let mut range = None;
        let mut authorization = None;
        let mut length = 0usize;
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).expect("a header line") == 0 {
                break;
            }
            if line == "\r\n" || line == "\n" {
                break;
            }
            let lower = line.to_ascii_lowercase();
            if let Some(value) = lower.strip_prefix("range: ") {
                range = Some(value.trim().to_string());
            } else if let Some(value) = lower.strip_prefix("content-length: ") {
                length = value.trim().parse().unwrap_or(0);
            }
            // The value's case is the caller's, so it is taken from the
            // original line and only the name from the lowered one.
            if lower.starts_with("authorization: ") {
                authorization = Some(line["authorization: ".len()..].trim().to_string());
            }
        }
        if request_line.starts_with("POST /refresh") {
            let mut body = vec![0u8; length];
            std::io::Read::read_exact(&mut reader, &mut body).expect("the posted body");
            refreshes.fetch_add(1, Ordering::SeqCst);
            match mode {
                Refreshes::Yes => {
                    let minted = format!("access-tok-{:04}", refreshes.load(Ordering::SeqCst) + 1);
                    tokens
                        .lock()
                        .expect("the tokens")
                        .issued
                        .push(minted.clone());
                    let json = format!("{{\"accessToken\":\"{minted}\",\"expiresIn\":3599}}");
                    respond(&mut stream, "200 OK", "application/json", json.as_bytes());
                }
                Refreshes::PairAgain => {
                    // A service that echoes what it was sent. Nothing this
                    // module says about the failure may come from here.
                    let json = format!(
                        "{{\"error\":\"invalid_grant for {REFRESH_TOKEN}\",\"pairAgain\":true}}"
                    );
                    respond(
                        &mut stream,
                        "401 Unauthorized",
                        "application/json",
                        json.as_bytes(),
                    );
                }
            }
            return;
        }
        // Drive: bytes to whoever holds a token this service issued.
        let bearer = authorization
            .as_deref()
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::to_string);
        let known = {
            let mut tokens = tokens.lock().expect("the tokens");
            tokens.last_bearer = bearer.clone();
            bearer.is_some_and(|bearer| tokens.issued.contains(&bearer))
        };
        if !known {
            rejections.fetch_add(1, Ordering::SeqCst);
            respond(
                &mut stream,
                "401 Unauthorized",
                "application/json",
                b"{\"error\":{\"code\":401}}",
            );
            return;
        }
        let (first, last) = match range.as_deref() {
            Some(header) => {
                let (first, last) = header
                    .trim_start_matches("bytes=")
                    .split_once('-')
                    .expect("a range");
                let first: usize = first.parse().expect("a first byte");
                let last: usize = if last.is_empty() {
                    FILE_LENGTH - 1
                } else {
                    last.parse::<usize>()
                        .expect("a last byte")
                        .min(FILE_LENGTH - 1)
                };
                (first, last)
            }
            None => (0, FILE_LENGTH - 1),
        };
        reads.fetch_add(1, Ordering::SeqCst);
        let body: Vec<u8> = (first..=last).map(byte_at).collect();
        let head = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Type: video/x-matroska\r\nETag: \
             \"the-film\"\r\nAccept-Ranges: bytes\r\nContent-Range: bytes {first}-{last}/\
             {FILE_LENGTH}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(head.as_bytes());
        let _ = stream.write_all(&body);
        let _ = stream.flush();
    }

    fn respond(stream: &mut std::net::TcpStream, status: &str, content_type: &str, body: &[u8]) {
        let head = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(head.as_bytes());
        let _ = stream.write_all(body);
        let _ = stream.flush();
    }

    /// Where the server under test is listening. The fake is somewhere
    /// else, which is what an origin is.
    const SELF_ADDR: SocketAddr = SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        11470,
    );

    fn cache() -> (tempfile::TempDir, Arc<crate::proxy_cache::ProxyCache>) {
        let dir = tempfile::tempdir().expect("a scratch root");
        let cache = crate::proxy_cache::ProxyCache::new(dir.path(), Arc::default(), Arc::default());
        (dir, Arc::new(cache))
    }

    /// An hour of token life: enough that nothing in a test renews by
    /// accident.
    const FRESH: Duration = Duration::from_secs(3600);

    /// **A ranged read returns the bytes it asked for**, from the offset it
    /// asked for -- which is the whole claim a `ByteSource` over Drive
    /// makes, and the thing every translator above it depends on.
    #[tokio::test]
    async fn a_ranged_read_returns_exactly_the_bytes_it_asked_for() {
        let fake = Fake::start(Refreshes::Yes);
        let (_root, cache) = cache();
        let source = DriveSource::open_against(cache, SELF_ADDR, fake.pairing(FRESH))
            .await
            .expect("a paired file");
        assert_eq!(source.len(), FILE_LENGTH as u64);
        assert_eq!(source.content_type(), "video/x-matroska");

        let at = 1024 * 300;
        let mut buf = vec![0u8; 1024];
        assert_eq!(
            source.read_at(at as u64, &mut buf).await.unwrap(),
            buf.len()
        );
        assert_eq!(buf, (at..at + 1024).map(byte_at).collect::<Vec<_>>());

        // And the end of the file is short rather than wrong.
        let mut tail = vec![0u8; 4096];
        let near_end = (FILE_LENGTH - 100) as u64;
        assert_eq!(source.read_at(near_end, &mut tail).await.unwrap(), 100);
        assert_eq!(
            tail[..100],
            (FILE_LENGTH - 100..FILE_LENGTH)
                .map(byte_at)
                .collect::<Vec<_>>()[..]
        );
        assert_eq!(fake.refreshes(), 0, "a fresh token was renewed anyway");
    }

    /// **An expiring token is renewed before the read, not after a
    /// `401`.** The margin is the point: a token with less life left than
    /// [`RENEW_MARGIN`] is replaced while it is still good, so the origin
    /// never refuses anything and the viewer never sees the stall a
    /// refused range would be.
    ///
    /// The fake proves the renewal really happened rather than that the
    /// read merely worked: it records which token each read arrived with,
    /// so the assertion is that the read went out under the *new* one and
    /// not merely that it succeeded.
    #[tokio::test]
    async fn an_expiring_token_is_renewed_before_the_read_and_the_read_succeeds() {
        let fake = Fake::start(Refreshes::Yes);
        let (_root, cache) = cache();
        // **Thirty seconds, written out and not `RENEW_MARGIN / 2`.** A
        // test phrased in terms of the constant it is about cannot fail
        // when the constant changes: with the margin set to zero -- which
        // is precisely the regression of renewing *after* a `401` instead
        // of before it -- half of it is still expired, and the test would
        // pass having proved nothing.
        const NEARLY_GONE: Duration = Duration::from_secs(30);
        const _: () = assert!(
            RENEW_MARGIN.as_secs() > NEARLY_GONE.as_secs(),
            "a token with NEARLY_GONE left has to be inside the margin for this test to be about it"
        );
        let source = DriveSource::open_against(cache, SELF_ADDR, fake.pairing(NEARLY_GONE))
            .await
            .expect("a paired file");
        assert_eq!(
            fake.refreshes(),
            1,
            "the token was sent with less than the margin left"
        );

        let mut buf = vec![0u8; 2048];
        assert_eq!(source.read_at(4096, &mut buf).await.unwrap(), buf.len());
        assert_eq!(buf, (4096..4096 + 2048).map(byte_at).collect::<Vec<_>>());
        assert_eq!(
            fake.rejections(),
            0,
            "a request went out under a token the origin refused"
        );
        // The renewed one, not the one the pairing arrived with: the token
        // was replaced *before* it was sent, which is the claim.
        assert_ne!(
            fake.last_bearer().as_deref(),
            Some(FIRST_ACCESS_TOKEN),
            "the read went out under the token that was about to expire"
        );
        assert_eq!(fake.last_bearer().as_deref(), Some("access-tok-0002"));
        // And the renewed token is good for an hour, so reading on does
        // not renew again.
        assert_eq!(fake.refreshes(), 1);
    }

    /// **A token that expires between two reads is renewed between them**,
    /// which is the claim the header being minted per request rather than
    /// held on the source exists to make. A film runs longer than an
    /// access token, so the read that matters is not the first one -- it is
    /// the one an hour in, and there is no opening it can piggyback on.
    ///
    /// The source is opened with an hour of life, reads (no refresh), is
    /// aged as an hour of real time would age it, and reads again.
    #[tokio::test]
    async fn a_token_that_expires_between_two_reads_is_renewed_between_them() {
        let fake = Fake::start(Refreshes::Yes);
        let (_root, cache) = cache();
        let credential = Arc::new(DriveCredential::new(
            fake.refresh_endpoint(),
            REFRESH_TOKEN.to_string(),
            FIRST_ACCESS_TOKEN.to_string(),
            FRESH,
        ));
        let url = fake.pairing(FRESH).media_url().expect("a media URL");
        let source = ProxySource::open(
            cache,
            SELF_ADDR,
            url,
            Credentials::Own {
                grant: credential.clone(),
                vouch: Vouch::UrlIdentifiesBytes {
                    vouched_by: "the expiry test",
                },
            },
        )
        .await
        .expect("an origin that ranges");

        let mut buf = vec![0u8; 512];
        source
            .read_at(1024, &mut buf)
            .await
            .expect("the first read");
        assert_eq!(fake.refreshes(), 0, "a fresh token was renewed");
        assert_eq!(fake.last_bearer().as_deref(), Some(FIRST_ACCESS_TOKEN));

        // An hour passes.
        credential.expire_now().await;
        let at = 9 * 1024;
        source
            .read_at(at, &mut buf)
            .await
            .expect("the read after the expiry");
        assert_eq!(
            buf,
            (at as usize..at as usize + 512)
                .map(byte_at)
                .collect::<Vec<_>>()
        );
        assert_eq!(fake.refreshes(), 1, "the expired token was not renewed");
        assert_eq!(
            fake.last_bearer().as_deref(),
            Some("access-tok-0002"),
            "the second read went out under the token that had expired"
        );
        assert_eq!(fake.rejections(), 0);
    }

    /// **Several reads meeting one expiry cause exactly one refresh.**
    /// Not "few": one. A player seeking makes a handful of reads in the
    /// same instant, and a refresh each would be a burst of identical
    /// requests, several new tokens, and -- because each mints and
    /// invalidates the last -- reads going out under tokens that were
    /// current when they were built and stale when they landed.
    #[tokio::test]
    async fn concurrent_reads_across_an_expiry_cause_one_refresh() {
        let fake = Fake::start(Refreshes::Yes);
        let (_root, cache) = cache();
        let credential = Arc::new(DriveCredential::new(
            fake.refresh_endpoint(),
            REFRESH_TOKEN.to_string(),
            FIRST_ACCESS_TOKEN.to_string(),
            FRESH,
        ));
        let url = fake.pairing(FRESH).media_url().expect("a media URL");
        let source = Arc::new(
            ProxySource::open(
                cache,
                SELF_ADDR,
                url,
                Credentials::Own {
                    grant: credential.clone(),
                    vouch: Vouch::UrlIdentifiesBytes {
                        vouched_by: "the concurrency test",
                    },
                },
            )
            .await
            .expect("an origin that ranges"),
        );
        // **The expiry has to come after the probe, not before it.** A
        // source opened on an already-expired token renews during the
        // probe and hands the reads an hour-old-nothing, so the reads meet
        // no expiry at all and the test passes whatever the lock does --
        // which is how this test first passed while holding the lock
        // across the renewal and while dropping it.
        assert_eq!(
            fake.refreshes(),
            0,
            "a fresh token was renewed at the probe"
        );
        credential.expire_now().await;

        let mut reading = Vec::new();
        for nth in 0..16u64 {
            let source = source.clone();
            reading.push(tokio::spawn(async move {
                // Distinct offsets inside the file, so no read can be
                // answered from the cache by another's bytes and skip its
                // own mint -- and none of them is a short read at the end,
                // which would prove nothing about a token.
                let at = nth * 16 * 1024;
                let mut buf = vec![0u8; 512];
                let read = source.read_at(at, &mut buf).await.expect("a ranged read");
                assert_eq!(read, buf.len());
                assert_eq!(
                    buf,
                    (at as usize..at as usize + 512)
                        .map(byte_at)
                        .collect::<Vec<_>>()
                );
            }));
        }
        for read in reading {
            read.await.expect("a read that did not panic");
        }
        assert_eq!(
            fake.refreshes(),
            1,
            "sixteen reads across one expiry started more than one refresh"
        );
        assert_eq!(fake.rejections(), 0, "a read went out under a stale token");
    }

    /// **`pairAgain` is terminal and is never retried.** The grant is
    /// gone; asking again can only be refused again, and a player that
    /// keeps seeking would turn that into a request per seek. The service
    /// is asked exactly once, however many reads follow.
    #[tokio::test]
    async fn invalid_grant_is_terminal_and_is_not_retried() {
        let fake = Fake::start(Refreshes::PairAgain);
        let (_root, cache) = cache();
        let refused =
            DriveSource::open_against(cache.clone(), SELF_ADDR, fake.pairing(Duration::ZERO))
                .await
                .err()
                .expect("a dead pairing was opened");
        assert!(
            matches!(refused, DriveError::PairAgain),
            "a dead pairing surfaced as something else: {refused}"
        );
        assert_eq!(fake.refreshes(), 1);

        // A second attempt on the same grant asks nobody. The credential
        // is what remembers, so this holds across every source built on
        // it.
        let credential = Arc::new(DriveCredential::new(
            fake.refresh_endpoint(),
            REFRESH_TOKEN.to_string(),
            FIRST_ACCESS_TOKEN.to_string(),
            Duration::ZERO,
        ));
        assert!(matches!(
            credential.bearer().await,
            Err(DriveError::PairAgain)
        ));
        assert!(credential.is_dead());
        assert_eq!(fake.refreshes(), 2, "the second grant asked once");
        for _ in 0..8 {
            assert!(matches!(
                credential.bearer().await,
                Err(DriveError::PairAgain)
            ));
        }
        assert_eq!(
            fake.refreshes(),
            2,
            "a dead pairing was asked again after it had answered"
        );
        // And what a *read* fails with carries the same fact, typed: a
        // `ByteSource` can only answer `io::Error`, so the terminal reason
        // has to survive inside one or an app is left matching English.
        // This is the very error a read would get, taken from the mint a
        // read makes.
        let failed = OwnGrant::headers(&*credential)
            .await
            .expect_err("a dead pairing minted a header");
        assert!(matches!(
            DriveError::in_read(&failed),
            Some(DriveError::PairAgain)
        ));
        assert_eq!(fake.refreshes(), 2, "the mint asked a dead pairing again");
    }

    /// **Nothing a token could reach is written down.** Not the describe
    /// string, not any error this module can produce -- including the one
    /// made from a refusal whose *body* carried the refresh token, which
    /// is what a naive pairing service answers and what proves the
    /// sentences here are written rather than echoed.
    #[tokio::test]
    async fn no_token_reaches_a_describe_string_or_an_error() {
        let fake = Fake::start(Refreshes::PairAgain);
        let (_root, cache) = cache();
        let refused =
            DriveSource::open_against(cache.clone(), SELF_ADDR, fake.pairing(Duration::ZERO))
                .await
                .err()
                .expect("a dead pairing");
        let mut written = vec![refused.to_string(), format!("{refused:?}")];

        // Every other error this module makes, rendered both ways.
        for error in [
            DriveError::PairAgain,
            DriveError::Unreachable("connection refused".to_string()),
            DriveError::Refused(500),
            DriveError::Source(ProxySourceError::WillNotRange),
            DriveError::Source(ProxySourceError::Credentials(io::Error::other(
                DriveError::PairAgain,
            ))),
        ] {
            written.push(error.to_string());
            written.push(format!("{error:?}"));
        }

        // And what a working source says about itself.
        let working = Fake::start(Refreshes::Yes);
        let source = DriveSource::open_against(cache, SELF_ADDR, working.pairing(FRESH))
            .await
            .expect("a paired file");
        written.push(source.describe());
        written.push(source.len().to_string());

        for line in &written {
            assert!(
                !line.contains(REFRESH_TOKEN),
                "the refresh token is in {line:?}"
            );
            assert!(
                !line.contains(FIRST_ACCESS_TOKEN),
                "an access token is in {line:?}"
            );
            assert!(!line.contains("Bearer"), "a bearer header is in {line:?}");
        }

        // The describe is the origin and nothing more -- not the file id,
        // which is not ours to write down either.
        assert_eq!(source.describe(), format!("http://{}", working.addr));
        assert!(!source.describe().contains("a-file-id"));
    }

    /// **A vouched read is kept, and the key is the URL with no token in
    /// it.** Both halves matter. The first is the point of vouching at
    /// all: a second read of a range is the disk's, so a backward seek is
    /// not a round trip to Google. The second is what makes it survive the
    /// hour: the same range read again *after the token has rotated* is
    /// still the disk's, which a key with a credential in it could never
    /// be.
    #[tokio::test]
    async fn a_vouched_read_is_kept_under_a_key_with_no_token_in_it() {
        let fake = Fake::start(Refreshes::Yes);
        let (_root, cache) = cache();
        let source = DriveSource::open_against(cache.clone(), SELF_ADDR, fake.pairing(FRESH))
            .await
            .expect("a paired file");
        assert_eq!(
            source.validator(),
            Some("etag:\"the-film\""),
            "without a validator nothing would be filed at all"
        );

        // A whole chunk, so what is read fills one and is committed.
        let chunk = crate::proxy_cache::CHUNK_BYTES as usize;
        let mut first = vec![0u8; chunk];
        assert_eq!(source.read_at(0, &mut first).await.unwrap(), chunk);
        cache.settled().await;
        let fetched = fake.reads();

        let mut again = vec![0u8; chunk];
        assert_eq!(source.read_at(0, &mut again).await.unwrap(), chunk);
        assert_eq!(again, first);
        assert_eq!(
            fake.reads(),
            fetched,
            "the second read of the same range went back to Drive"
        );

        // And a second pairing -- a different grant, the same file id --
        // finds those bytes. **This is the decision, asserted**: the key
        // is the URL, the credential authorises the fetch and does not
        // determine the content, so the store does not churn when a token
        // does and does not hold a copy per account. What it means is
        // written where the key is minted: an entry made with
        // authorisation can be read without it, which the owner accepted
        // knowingly for a loopback-only server.
        let mut other = fake.pairing(FRESH);
        other.refresh_token = "a-quite-different-grant".to_string();
        // A different account's access token, which this fake -- like
        // Google -- has never issued, so the pairing has to renew before
        // it can read at all.
        other.access_token = "a-quite-different-access-token".to_string();
        other.expires_in = Duration::ZERO;
        let theirs = DriveSource::open_against(cache.clone(), SELF_ADDR, other)
            .await
            .expect("a second pairing");
        let after_probe = fake.reads();
        let mut mine = vec![0u8; chunk];
        assert_eq!(theirs.read_at(0, &mut mine).await.unwrap(), chunk);
        assert_eq!(mine, first);
        assert_eq!(
            fake.reads(),
            after_probe,
            "the second grant re-fetched bytes the store already held"
        );
    }

    /// **A source that mints its own header and vouches for nothing is not
    /// cached**, so caching is something a source opts into by saying so
    /// and never something it acquires by being credentialed. Same URL,
    /// same grant, one word different.
    #[tokio::test]
    async fn an_unvouched_grant_is_read_and_never_kept() {
        let fake = Fake::start(Refreshes::Yes);
        let (_root, cache) = cache();
        let credential = Arc::new(DriveCredential::new(
            fake.refresh_endpoint(),
            REFRESH_TOKEN.to_string(),
            FIRST_ACCESS_TOKEN.to_string(),
            FRESH,
        ));
        let url = fake.pairing(FRESH).media_url().expect("a media URL");
        let source = ProxySource::open(
            cache.clone(),
            SELF_ADDR,
            url,
            Credentials::Own {
                grant: credential,
                vouch: Vouch::Unvouched,
            },
        )
        .await
        .expect("an origin that ranges");

        let chunk = crate::proxy_cache::CHUNK_BYTES as usize;
        let mut buf = vec![0u8; chunk];
        source.read_at(0, &mut buf).await.unwrap();
        cache.settled().await;
        let fetched = fake.reads();
        source.read_at(0, &mut buf).await.unwrap();
        assert_eq!(
            fake.reads(),
            fetched + 1,
            "an unvouched credentialed read was answered from the store"
        );
    }

    /// The media URL is Drive's, built here from a file id rather than
    /// taken from a caller -- which is the whole reason these reads have
    /// no open relay in them.
    #[test]
    fn a_pairing_names_the_drive_media_endpoint() {
        let pairing = DrivePairing::new(
            Url::parse("https://example.invalid/refresh").expect("a literal URL"),
            "grant",
            "access",
            FRESH,
            "1AbC-dEf",
        );
        assert_eq!(
            pairing.media_url().expect("a media URL").as_str(),
            "https://www.googleapis.com/drive/v3/files/1AbC-dEf?alt=media"
        );
    }
}
