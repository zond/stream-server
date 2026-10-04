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
//! at `/cast/{token}/stream.mp4`: a file with a length and ranges whose
//! every byte is fixed before it is made (header, then one padded slot per
//! segment), so a receiver seeks in it by bytes. A plain publication has
//! no `stream.mp4` (`404`); a rendition's token serves it and, like a
//! plain one, the source as it is at `/cast/{token}`.

use crate::media::registry::Entry;
use crate::media::{MediaId, PlayToken, Refusal};
use crate::rendition::{
    Ask, NotServed, Producer, Rendition, RenditionReadiness, RenditionSpec, RenditionState,
    RenditionTuning,
};
use crate::routes::{compat, util};
use crate::sources::ReadHint;
use crate::state::AppState;
use crate::translators::session::Lease;
use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
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
    /// **The cast's hold on its torrent** (`enginefs::retention::holds`),
    /// for an id a torrent is behind: from publish to unpublish the torrent
    /// runs and keeps its bytes, whatever the receiver is reading and
    /// whatever else a stream opens. Taken while the player's own hold is
    /// still there, so the hand-over has no gap; let go by the unpublish.
    hold: std::sync::Mutex<Option<enginefs::retention::holds::TorrentHold>>,
    _lease: Lease<Entry>,
}

impl Publication {
    /// Let go of the torrent: unpublished, now -- not when the last request
    /// still holding the publication finishes.
    fn release(&self) {
        self.hold
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
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
    /// Preparations waiting now (`ServerHandle::prepare_rendition`): what a
    /// test reads to see an unpublish end one.
    preparing: Arc<std::sync::atomic::AtomicUsize>,
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
        holds: &enginefs::retention::holds::Holds,
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
        let hold = lease
            .torrent_now()
            .map(|(info_hash, files)| holds.hold(&info_hash, files));
        published.insert(
            token.0.clone(),
            Arc::new(Publication {
                id,
                play,
                cut,
                rendition,
                hold: std::sync::Mutex::new(hold),
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
        publication.release();
        publication.cut.cancel();
        tracing::info!("cast unpublished");
        true
    }

    /// Unpublish every token, cutting every body: the listener stopping.
    pub(crate) fn unpublish_all(&self) -> usize {
        let all: Vec<_> = self.published().drain().map(|(_, cast)| cast).collect();
        for publication in &all {
            publication.release();
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

    /// How far the rendition under `token` has got towards its receiver's
    /// start: [`RenditionReadiness::Ended`] for a token that is not
    /// published, or not a rendition.
    pub(crate) fn rendition_readiness(&self, token: &CastToken) -> RenditionReadiness {
        self.rendition(token)
            .map_or(RenditionReadiness::Ended, |rendition| rendition.readiness())
    }

    /// How many preparations are waiting now: each holds this while it does.
    pub(crate) fn preparing(&self) -> Arc<std::sync::atomic::AtomicUsize> {
        self.preparing.clone()
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

async fn stream_get(
    State(state): State<AppState>,
    Path(token): Path<String>,
    headers: HeaderMap,
) -> Response {
    serve_stream(&state, &token, &headers, true).await
}

async fn stream_head(
    State(state): State<AppState>,
    Path(token): Path<String>,
    headers: HeaderMap,
) -> Response {
    serve_stream(&state, &token, &headers, false).await
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

/// Zeros for a slot's padding, handed out without allocating.
static ZEROS: [u8; 64 * 1024] = [0; 64 * 1024];

/// A rendition file's bytes from `next` to `end` (inclusive), piece by
/// piece: the header, then each slot's fragment and padding, the
/// fragments asked of the rendition as the body gets to them.
struct FileReader {
    state: AppState,
    rendition: Arc<Rendition>,
    layout: Arc<crate::rendition::layout::Layout>,
    next: u64,
    end: u64,
    /// The first slot's fragment, when the answer waited for it.
    first: Option<(u64, Bytes)>,
    /// What the next fragment is asked as: the range's first slot is the
    /// receiver's request wherever the range began, the rest are read on
    /// into.
    ask: Ask,
}

impl FileReader {
    /// The next piece, `None` at the range's end.
    async fn piece(&mut self) -> Option<std::io::Result<Bytes>> {
        if self.next > self.end {
            return None;
        }
        let header = self.layout.header.len() as u64;
        if self.next < header {
            let to = (self.end + 1).min(header);
            let piece = self.layout.header.slice(self.next as usize..to as usize);
            self.next = to;
            return Some(Ok(piece));
        }
        let slot = self.layout.slot_at(self.next)?;
        let place = self.layout.slots[slot as usize];
        let from = self.next - place.offset;
        let to = (self.end - place.offset).min(place.size - 1);
        if place.in_tail(from) {
            let len = (to + 1 - from) as usize;
            self.next += len as u64;
            // A tail is at most TAIL_ZEROS long.
            return Some(Ok(Bytes::from_static(&ZEROS[..len])));
        }
        let fragment = match self.first.take() {
            Some((first, fragment)) if first == slot => fragment,
            _ => {
                let asked = self.rendition.slot(&self.state, slot, self.ask).await;
                match asked {
                    Ok(fragment) => fragment,
                    Err(reason) => {
                        // Nothing after an error.
                        self.next = u64::MAX;
                        return Some(Err(std::io::Error::other(match reason {
                            NotServed::Cut => "the cast was unpublished".to_string(),
                            NotServed::Failed(sentence) => sentence,
                            NotServed::NotFound => "past the rendition's last slot".to_string(),
                        })));
                    }
                }
            }
        };
        self.ask = self.ask.then();
        let bytes = slot_bytes(&fragment, place.size, from, to);
        self.next += to + 1 - from;
        Some(Ok(bytes))
    }
}

/// Bytes `from..=to` of a slot `size` long holding `fragment`: the
/// fragment, a `free` box header, zeros.
fn slot_bytes(fragment: &Bytes, size: u64, from: u64, to: u64) -> Bytes {
    let len = fragment.len() as u64;
    let pad = size - len;
    let header = crate::rendition::layout::free_header(pad);
    let mut out = Vec::with_capacity((to + 1 - from) as usize);
    let mut at = from;
    while at <= to {
        if at < len {
            let upto = (to + 1).min(len);
            out.extend_from_slice(&fragment[at as usize..upto as usize]);
            at = upto;
        } else if at < len + 8 {
            let upto = (to + 1).min(len + 8);
            out.extend_from_slice(&header[(at - len) as usize..(upto - len) as usize]);
            at = upto;
        } else {
            out.resize(out.len() + (to + 1 - at) as usize, 0);
            at = to + 1;
        }
    }
    Bytes::from(out)
}

/// **A rendition's file**: one progressive fragmented MP4 with a length
/// and ranges (`docs/design/renditions.md`, "Seeking by bytes") -- the
/// header (`ftyp` + `moov` + `sidx`), then one slot per segment, each its
/// fragment padded with a `free` box, every byte the same however often
/// and in whatever order it is asked for. A receiver plays it as a file
/// (`<video src>`), not through Media Source, and seeks in it by bytes:
/// it finds the time in the `sidx` and asks for a `Range` there.
///
/// The length is known once the first run has reported the source's
/// formats and index, so the answer -- a `HEAD` too -- waits for that
/// (starting the run, from the film's start, if none is live). Then the
/// range framing every media route shares: `200` or `206` with
/// `Content-Range`, `416` for a range past the end. A range that begins in
/// a slot waits for that slot's fragment before it answers, so a rendition
/// that cannot make it is a status (`503`, as [`not_served`] says) rather
/// than a body that breaks; one that begins in the header answers at once.
///
/// Each later slot is asked for as the receiver takes the bytes before it,
/// so a receiver that pauses stops asking and the run's lookahead holds
/// the producer. The first slot a range asks for may move the run there --
/// a read from the header on into slot 0 included: Chrome's demuxer probes
/// it before it seeks, and a read that waits for nobody waits forever; the slots a range reads on
/// into never move a live run ([`Ask`]: the latest asker wins). The last bytes of a slot are always zeros and are
/// answered without asking for anything. No timer ends a body: a slot that
/// is slow to come is waited for. The cut (unpublish, the listener's stop)
/// and a rendition that fails partway break the body with an error.
async fn serve_stream(state: &AppState, token: &str, headers: &HeaderMap, body: bool) -> Response {
    let Some(publication) = state.lan_media.casts().get(token) else {
        tracing::info!("cast request for a token that is not published");
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(rendition) = publication.rendition.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let layout = match rendition.layout(state).await {
        Ok(layout) => layout,
        Err(reason) => return not_served(reason),
    };
    let size = layout.total;
    let range = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok());
    let Some(framing) = util::MediaRange::of(range, size) else {
        return util::range_not_satisfiable(size);
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
    framing.write_headers(size, &mut res_headers);
    if !body {
        return (framing.status(), res_headers, Body::empty()).into_response();
    }
    // A range that begins in a slot -- and not in its zero tail -- waits for
    // that slot's fragment: the receiver's seek.
    let first = match layout.slot_at(framing.start) {
        Some(slot)
            if !layout.slots[slot as usize]
                .in_tail(framing.start - layout.slots[slot as usize].offset) =>
        {
            match rendition.slot(state, slot, Ask::Seek).await {
                Ok(fragment) => Some((slot, fragment)),
                Err(reason) => return not_served(reason),
            }
        }
        _ => None,
    };
    state.lan_media.record_body();
    // The first slot a range reaches is one it asked for, wherever the
    // range began: a read from the header on into slot 0 (Chrome's demuxer
    // probing the first fragment) is as much the receiver's request as a
    // jump to its start.
    let ask = Ask::Seek;
    let reader = FileReader {
        state: state.clone(),
        rendition,
        layout,
        next: framing.start,
        end: framing.end,
        first,
        ask,
    };
    let chunks = futures_util::stream::unfold(reader, |mut reader| async move {
        let piece = reader.piece().await?;
        Some((piece, reader))
    })
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
    let length = framing.content_length(size);
    let body = CastBody {
        chunks: Box::pin(chunks),
        cut: Box::pin(publication.cut.clone().cancelled_owned()),
        ended: false,
        cut_seen: false,
        delivered: 0,
        length,
        kind: "rendition",
        _held: Box::new(()),
    };
    (framing.status(), res_headers, Body::from_stream(body)).into_response()
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
