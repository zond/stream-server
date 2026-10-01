//! **Cast by published token**: the one route the LAN media listener
//! serves (`docs/design/media-pipeline.md` §2.7).
//!
//! The app publishes a [`MediaId`] for a cast
//! ([`crate::ServerHandle::publish`]) and hands the receiver
//! `<lan base>/cast/<token>`; `GET` and `HEAD` there serve what the id
//! resolved to, with the range framing every media route shares
//! (`routes::util::MediaRange`). Nothing else is on the LAN: no torrent
//! route, no archive route, no `/proxy`, no control route. **The listener
//! serves published tokens and nothing else**, so what a stranger on the
//! network can reach is exactly what the app chose to cast, for exactly as
//! long as it chose to -- and nothing it asks for can make this device
//! fetch, add or open anything the app did not.
//!
//! The rules a token keeps:
//!
//! * **Random, never derived**: 128 bits from the OS, hex. A receiver that
//!   saw one learns nothing about another, or about the id.
//! * **Memory only.** A restart forgets every one. Publishing needs the
//!   listener running, and stopping it (`set_lan_media(false)`, the
//!   `lanMediaEnabled` veto revoked, the server's own stop) unpublishes
//!   every token.
//! * **A publication holds a lease on its id**, as an open reader does, so
//!   the id is not evicted while it is cast.
//! * **Unpublishing cuts the bytes.** Every body served under a token is
//!   wrapped (`CastBody`) and polls the token's cut before each chunk,
//!   so an unpublish -- or the listener's stop, which is every token's --
//!   ends a response in flight at once, with an error, not a clean end the
//!   receiver would read as the film being over.
//! * **Never logged.** `routes::util::log_path` elides `/cast/<token>` to
//!   `/cast`, and nothing here writes one down: a token in a log file is a
//!   URL into this device for as long as it is published.
//!
//! **What a body is.** A `GET` opens the id's source the way
//! [`crate::ServerHandle::open_reader`] does -- the same resolve, the same
//! play rules -- and reads it on the runtime with no reader task: with the
//! publication's [`PlayToken`] the receiver's reads are the viewer's
//! playback (the session moves, the file shares as the app's own player's
//! would), without one they are an aside. A torrent source registers its
//! stream at the body's open and ends it when the body is dropped, as the
//! HTTP torrent route's body does.
//!
//! **Renditions** (`docs/design/renditions.md`, `crate::rendition`) are
//! publications too ([`crate::ServerHandle::publish_rendition`]), with the
//! same token rules and the same cut, and one progressive fragmented MP4
//! at `/cast/{token}/stream.mp4` (`?from=<ms>` for a start other than the
//! spec's): the init segment and the media segments in order, sent as
//! they are made. A plain publication has no `stream.mp4` (`404`); a
//! rendition's token serves it and, like a plain one, the source as it is
//! at `/cast/{token}`.

use crate::media::registry::Entry;
use crate::media::{MediaId, PlayToken, Refusal};
use crate::rendition::{
    NotServed, Producer, Rendition, RenditionSpec, RenditionState, RenditionTuning,
};
use crate::routes::{compat, util};
use crate::sources::ReadHint;
use crate::state::AppState;
use crate::translators::session::Lease;
use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncReadExt;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

/// A published cast: 128 random bits, hex, naming one [`MediaId`] for as
/// long as it is published. **Not the id and not derived from it.**
///
/// Its `Debug` says nothing of the token, so a struct that derives `Debug`
/// around one cannot put it in a log line.
#[derive(Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct CastToken(String);

impl CastToken {
    fn random() -> anyhow::Result<Self> {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(|err| {
            anyhow::anyhow!("failed to draw random bytes for a cast token: {err}")
        })?;
        Ok(Self(hex::encode(bytes)))
    }

    /// The token as it travels: the last segment of the cast URL.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for CastToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CastToken(..)")
    }
}

impl From<String> for CastToken {
    /// A token the app kept as a string, handed back. One this server did
    /// not publish is simply one it serves nothing under.
    fn from(token: String) -> Self {
        Self(token)
    }
}

/// What one token names: the id, the viewer's play if the cast is one, the
/// lease that keeps the id, and the cut every body under it polls.
struct Publication {
    id: MediaId,
    play: Option<PlayToken>,
    cut: CancellationToken,
    /// The stream behind the token, for a rendition. Its runs' stops
    /// are children of `cut`, so the cut ends them; its ring goes with the
    /// last holder -- the map, a request in flight, a run task -- each of
    /// which lets go at the cut.
    rendition: Option<Arc<Rendition>>,
    _lease: Lease<Entry>,
}

/// Every token published and not unpublished, and what a rendition needs
/// to be published at all. Held by [`crate::lan_media::LanMedia`], whose
/// stop unpublishes all of them.
#[derive(Default)]
pub(crate) struct Casts {
    published: std::sync::Mutex<HashMap<String, Arc<Publication>>>,
    /// The embedder's producer (`ServerHandle::install_producer`): without
    /// one, a rendition is refused.
    producer: std::sync::Mutex<Option<Arc<dyn Producer>>>,
    /// The release period and the speed window renditions published from
    /// now on run by.
    tuning: std::sync::Mutex<RenditionTuning>,
}

/// Why a rendition was not published: the sentence of the error
/// `publish_rendition` answers when no producer is installed.
pub(crate) const NO_PRODUCER: &str =
    "no rendition producer is installed (noProducer); install one with install_producer first";

impl Casts {
    fn published(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<Publication>>> {
        self.published
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Publish `id` under a fresh token, if `running` says the listener is
    /// up -- asked under this map's lock, which is what makes a publish
    /// racing a stop either land before the stop's unpublish (and be
    /// unpublished by it) or see the listener down.
    pub(crate) fn publish(
        &self,
        running: impl FnOnce() -> bool,
        lease: Lease<Entry>,
        id: MediaId,
        play: Option<PlayToken>,
        rendition: Option<RenditionSpec>,
    ) -> anyhow::Result<CastToken> {
        let token = CastToken::random()?;
        let cut = CancellationToken::new();
        let rendition = match rendition {
            None => None,
            Some(spec) => {
                let producer = self
                    .producer
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!(NO_PRODUCER))?;
                let tuning = *self
                    .tuning
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                Some(Arc::new(Rendition::new(
                    id.clone(),
                    play.clone(),
                    spec,
                    producer,
                    tuning,
                    cut.clone(),
                )?))
            }
        };
        let mut published = self.published();
        anyhow::ensure!(
            running(),
            "the LAN media listener is not running; start it with set_lan_media(true) first"
        );
        let is_rendition = rendition.is_some();
        published.insert(
            token.0.clone(),
            Arc::new(Publication {
                id,
                play,
                cut,
                rendition,
                _lease: lease,
            }),
        );
        tracing::info!(
            rendition = is_rendition,
            published = published.len(),
            played = published
                .values()
                .filter(|publication| publication.play.is_some())
                .count(),
            "cast published"
        );
        Ok(token)
    }

    /// Unpublish `token`: nothing new is served under it, and every body
    /// being served under it ends now. Whether it was published.
    pub(crate) fn unpublish(&self, token: &CastToken) -> bool {
        let Some(publication) = self.published().remove(token.as_str()) else {
            return false;
        };
        publication.cut.cancel();
        tracing::info!("cast unpublished");
        true
    }

    /// Unpublish every token, cutting every body: the listener stopping.
    pub(crate) fn unpublish_all(&self) -> usize {
        let all: Vec<_> = self.published().drain().map(|(_, cast)| cast).collect();
        for publication in &all {
            publication.cut.cancel();
        }
        if !all.is_empty() {
            tracing::info!(unpublished = all.len(), "every cast unpublished");
        }
        all.len()
    }

    fn get(&self, token: &str) -> Option<Arc<Publication>> {
        self.published().get(token).cloned()
    }

    /// Install the embedder's producer, replacing any before it.
    /// Renditions already published keep the one they were published with.
    pub(crate) fn install_producer(&self, producer: Arc<dyn Producer>) {
        *self
            .producer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(producer);
    }

    pub(crate) fn set_tuning(&self, tuning: RenditionTuning) {
        *self
            .tuning
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = tuning;
    }

    /// Where the rendition under `token` is: [`RenditionState::Ended`] for a
    /// token that is not published, or not a rendition.
    pub(crate) fn rendition_state(&self, token: &CastToken) -> RenditionState {
        self.get(token.as_str())
            .and_then(|publication| publication.rendition.clone())
            .map_or(RenditionState::Ended, |rendition| rendition.state())
    }

    pub(crate) fn rendition_probe(
        &self,
        token: &CastToken,
    ) -> Option<crate::rendition::RenditionProbe> {
        self.get(token.as_str())
            .and_then(|publication| publication.rendition.clone())
            .map(|rendition| rendition.probe())
    }

    /// The rendition published as `token`, if it is one.
    pub(crate) fn rendition(&self, token: &CastToken) -> Option<Arc<Rendition>> {
        self.get(token.as_str())
            .and_then(|publication| publication.rendition.clone())
    }
}

/// The LAN listener's routes: `/cast/{token}`, and a rendition's stream,
/// `/cast/{token}/stream.mp4`. Nothing else.
pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/cast/{token}", get(cast_get).head(cast_head))
        .route(
            "/cast/{token}/stream.mp4",
            get(stream_get).head(stream_head),
        )
}

/// Where a rendition's stream starts: `?from=<ms>` on the film's clock.
#[derive(serde::Deserialize)]
struct StreamQuery {
    from: Option<u64>,
}

async fn stream_get(
    State(state): State<AppState>,
    Path(token): Path<String>,
    Query(query): Query<StreamQuery>,
) -> Response {
    serve_stream(&state, &token, query.from, true).await
}

async fn stream_head(
    State(state): State<AppState>,
    Path(token): Path<String>,
    Query(query): Query<StreamQuery>,
) -> Response {
    serve_stream(&state, &token, query.from, false).await
}

/// What a rendition answers when it has no bytes for the request: `404`
/// past the end, `503` with `{refused, message}` once it has failed or
/// while it is being cut -- never a clean empty body a receiver would read
/// as the film being over.
fn not_served(reason: NotServed) -> Response {
    match reason {
        NotServed::NotFound => StatusCode::NOT_FOUND.into_response(),
        NotServed::Failed(message) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "refused": "renditionFailed", "message": message })),
        )
            .into_response(),
        NotServed::Cut => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "refused": "unpublished",
                "message": "the cast was unpublished",
            })),
        )
            .into_response(),
    }
}

/// **A rendition's stream**: one progressive fragmented MP4 -- the init
/// segment, then segment after segment from the one `from` falls in (the
/// spec's start without it) to the film's end -- sent as the segments are
/// made. A receiver plays it as a file it reads forward (`<video src>`),
/// not through Media Source.
///
/// The answer waits for the first segment and the init segment, so a
/// rendition that cannot start is a status (`503`, as [`not_served`] says)
/// rather than a body that breaks. After that it is `200`, `video/mp4`,
/// **no length and no ranges**: the length of what is not made yet is not
/// known, and offering ranges would invite a seek by bytes that a stream
/// made from a time cannot answer. A `Range` header is not read; the
/// answer is the stream from its start. A seek is a new stream from
/// another `from`.
///
/// Each segment after the first is asked for as the receiver takes the
/// last -- the body is polled only as the socket drains -- so a receiver
/// that pauses stops asking, and the run's lookahead holds the producer.
/// No timer ends a stream: a segment that is slow to come is waited for.
/// The cut (unpublish, the listener's stop) breaks the body with an error,
/// as it does a plain cast's, and so does a rendition that fails partway;
/// only the film's end is a clean end.
async fn serve_stream(state: &AppState, token: &str, from: Option<u64>, body: bool) -> Response {
    let Some(publication) = state.lan_media.casts().get(token) else {
        tracing::info!("cast request for a token that is not published");
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(rendition) = publication.rendition.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut res_headers = HeaderMap::new();
    res_headers.insert(
        header::CONTENT_TYPE,
        "video/mp4".parse().expect("a MIME type"),
    );
    res_headers.insert(
        header::CACHE_CONTROL,
        "no-store".parse().expect("a header value"),
    );
    if !body {
        return (StatusCode::OK, res_headers, Body::empty()).into_response();
    }
    let start_ms = rendition.stream_start_ms(from);
    if rendition.stream_begins(start_ms) {
        // The receiver could not seek in the stream and is playing it again
        // from its start; the app is told (`rendition_restarts`) and puts it
        // back where it was. Logged without the token.
        tracing::info!(
            restarts = rendition.restarts(),
            stage = "rendition_stream_restart",
            "a receiver fetched a rendition's stream again from its start"
        );
    }
    let first = rendition.first_segment(start_ms);
    // The first segment starts the run there (or joins one), and the init
    // segment is frozen by the time it is out; either failing is the one
    // answer, since a failure fails the whole rendition.
    let begun = async {
        let first_bytes = rendition.segment(state, first).await?;
        Ok::<_, NotServed>((rendition.init(state).await?, first_bytes))
    };
    let (init, first_bytes) = match begun.await {
        Ok(begun) => begun,
        Err(reason) => return not_served(reason),
    };
    state.lan_media.record_body();
    rendition.stream_sent(start_ms, 1);
    let count = rendition.count();
    let later = {
        let state = state.clone();
        let rendition = rendition.clone();
        futures_util::stream::unfold(Some(first + 1), move |next| {
            let state = state.clone();
            let rendition = rendition.clone();
            async move {
                let segment = next.filter(|segment| *segment < count)?;
                match rendition.segment(&state, segment).await {
                    Ok(bytes) => {
                        rendition.stream_sent(start_ms, segment - first + 1);
                        Some((Ok(bytes), Some(segment + 1)))
                    }
                    // Past the film's end, as a run that reached it found.
                    Err(NotServed::NotFound) => None,
                    Err(NotServed::Failed(sentence)) => {
                        Some((Err(std::io::Error::other(sentence)), None))
                    }
                    Err(NotServed::Cut) => {
                        Some((Err(std::io::Error::other("the cast was unpublished")), None))
                    }
                }
            }
        })
    };
    let chunks = futures_util::stream::iter([Ok(init), Ok(first_bytes)])
        .chain(later)
        .flat_map(|piece: std::io::Result<Bytes>| {
            let pieces: Vec<std::io::Result<Bytes>> = match piece {
                Ok(bytes) => bytes
                    .chunks(64 * 1024)
                    .map(|chunk| Ok(bytes.slice_ref(chunk)))
                    .collect(),
                Err(error) => vec![Err(error)],
            };
            futures_util::stream::iter(pieces)
        });
    let body = CastBody {
        chunks: Box::pin(chunks),
        cut: Box::pin(publication.cut.clone().cancelled_owned()),
        ended: false,
        cut_seen: false,
        delivered: 0,
        length: 0,
        kind: "rendition",
        _held: Box::new(()),
    };
    (StatusCode::OK, res_headers, Body::from_stream(body)).into_response()
}

async fn cast_get(
    State(state): State<AppState>,
    Path(token): Path<String>,
    headers: HeaderMap,
) -> Response {
    serve(&state, &token, &headers, true).await
}

async fn cast_head(
    State(state): State<AppState>,
    Path(token): Path<String>,
    headers: HeaderMap,
) -> Response {
    serve(&state, &token, &headers, false).await
}

/// A refusal as a receiver is told it: a status, and the
/// `{refused, message}` body the archive routes answer with.
fn refused(refusal: Refusal) -> Response {
    let status = match &refusal {
        Refusal::UnknownId | Refusal::NoSuchFile(_) | Refusal::UnrecognisedUrl => {
            StatusCode::NOT_FOUND
        }
        Refusal::Translated(crate::translators::Refusal::Malformed(_)) => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        Refusal::Translated(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
        Refusal::NoRanges | Refusal::NoReader(_) | Refusal::NotYet { .. } => {
            StatusCode::NOT_IMPLEMENTED
        }
        Refusal::InsufficientDiskSpace => StatusCode::INSUFFICIENT_STORAGE,
        Refusal::ServerStopped => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::BAD_GATEWAY,
    };
    tracing::info!(refused = refusal.kind(), %status, "cast refused");
    (status, Json(refusal)).into_response()
}

async fn serve(state: &AppState, token: &str, headers: &HeaderMap, body: bool) -> Response {
    let Some(publication) = state.lan_media.casts().get(token) else {
        tracing::info!("cast request for a token that is not published");
        return StatusCode::NOT_FOUND.into_response();
    };
    let resolved = match state.media.resolve(state, &publication.id).await {
        Ok(resolved) => resolved,
        Err(refusal) => return refused(refusal),
    };
    // An origin that will not range can be read forward by a player
    // through `/proxy` on loopback, never by a receiver: nothing here can
    // seek it.
    if !resolved.in_process {
        return refused(Refusal::NoRanges);
    }
    let size = resolved.len;
    let range = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok());
    let Some(framing) = util::MediaRange::of(range, size) else {
        return util::range_not_satisfiable(size);
    };
    let mut res_headers = HeaderMap::new();
    if let Ok(content_type) = resolved.content_type.parse() {
        res_headers.insert(header::CONTENT_TYPE, content_type);
    }
    framing.write_headers(size, &mut res_headers);
    compat::add_dlna_headers(&mut res_headers);
    if !body {
        return (framing.status(), res_headers, Body::empty()).into_response();
    }

    // The same open a reader over the id makes, with the publication's play:
    // the session moves and the stream registers here, before a byte.
    let (entry, source) = match state
        .media
        .open_source(state, &publication.id, publication.play.clone())
        .await
    {
        Ok(opened) => opened,
        Err(refusal) => return refused(refusal),
    };
    let length = framing.content_length(size);
    let reader = match source
        .bytes()
        .open(framing.start, ReadHint::of(length))
        .await
    {
        Ok(reader) => reader,
        Err(error) => return refused(crate::media::reader::refusal_of(&error)),
    };
    state.lan_media.record_body();
    tracing::info!(
        source = source.kind(),
        start = framing.start,
        length,
        played = publication.play.is_some(),
        stage = "cast_body_start",
        "cast body"
    );
    let body = CastBody {
        chunks: Box::pin(crate::routes::archive::media_body(reader.take(length))),
        cut: Box::pin(publication.cut.clone().cancelled_owned()),
        ended: false,
        cut_seen: false,
        delivered: 0,
        length,
        kind: source.kind(),
        _held: Box::new((source, entry)),
    };
    (framing.status(), res_headers, Body::from_stream(body)).into_response()
}

type Chunks = Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send>>;

/// A cast response's body, endable by its token's unpublish: the cut is
/// polled first on every poll, so a body parked on a piece nobody has is
/// woken by the cut itself, and yields an **error** -- hyper then drops the
/// connection, and the receiver reads a broken source rather than a file
/// that ended early (`proxy_streams::ClosableStream`'s rule, for the same
/// reason). It holds the source and the id's lease for as long as it is
/// read (nothing, for a rendition's file, which is bytes in memory);
/// dropped, the source's stream ends.
struct CastBody {
    chunks: Chunks,
    cut: Pin<Box<WaitForCancellationFutureOwned>>,
    ended: bool,
    cut_seen: bool,
    delivered: u64,
    length: u64,
    kind: &'static str,
    _held: Box<dyn Send>,
}

impl Stream for CastBody {
    type Item = std::io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        if this.cut.as_mut().poll(cx).is_ready() {
            this.ended = true;
            this.cut_seen = true;
            return Poll::Ready(Some(Err(std::io::Error::other("the cast was unpublished"))));
        }
        let next = this.chunks.as_mut().poll_next(cx);
        match &next {
            Poll::Ready(Some(Ok(chunk))) => this.delivered += chunk.len() as u64,
            Poll::Ready(None) | Poll::Ready(Some(Err(_))) => this.ended = true,
            Poll::Pending => {}
        }
        next
    }
}

impl Drop for CastBody {
    fn drop(&mut self) {
        tracing::info!(
            source = self.kind,
            delivered = self.delivered,
            length = self.length,
            cut = self.cut_seen,
            stage = "cast_body_end",
            "cast body ended"
        );
    }
}
