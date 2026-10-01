use crate::routes::compat;
use crate::routes::util::{self, MediaRange};
use crate::sources::proxy::ProxySourceError;
use crate::sources::{ByteSource, ProxySource, TorrentSource};
use crate::state::AppState;
use crate::translators::session::{Lease, SessionSources, TranslatedSession};
use crate::translators::{Body as MemberBody, Refusal, Translator};
use axum::{
    Json, Router,
    body::Body,
    extract::{Extension, Path, Query, State},
    http::{Method, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
    routing::get,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

#[derive(Serialize)]
struct CreateResponse {
    key: String,
}

#[derive(Deserialize)]
struct StreamParams {
    key: String,
    file: Option<String>,
}

#[derive(Deserialize)]
struct CreateQuery {
    lz: Option<String>,
}

/// How much a member body asks its reader for per chunk.
///
/// `ReaderStream::new` reads 4 KiB at a time, and for an archive member
/// that is 4 KiB per read of whatever holds the container's bytes -- a
/// seek of the piece store, a ranged read through the proxy cache -- per
/// boxed future, per socket write; measured at some fourteen times the CPU
/// per byte of a 256 KiB read on a desktop core, on what is the streaming
/// hot path of the weakest devices this runs on. Every media body reads
/// with it -- the torrent stream route, the archive members and `/ftp` --
/// at 256 KiB per active stream.
pub(crate) const MEDIA_BODY_CHUNK_BYTES: usize = 256 * 1024;

/// The response body for a media reader: [`MEDIA_BODY_CHUNK_BYTES`] per read.
pub(crate) fn media_body<R: tokio::io::AsyncRead>(reader: R) -> ReaderStream<R> {
    ReaderStream::with_capacity(reader, MEDIA_BODY_CHUNK_BYTES)
}

/// Which container format a URL prefix names -- the prefix is what says
/// *which translator reads this* (`crate::translators`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Rar,
    Zip,
    SevenZ,
    Tar,
    /// `.tar.gz`, which is a refusal rather than a container: see
    /// [`crate::translators::TarGz`].
    TarGz,
    /// A disc image, ISO 9660 or UDF: see [`crate::translators::iso`].
    Iso,
}

impl Format {
    /// The format a URL's first path segment names, as `crate::archive_prefixes`
    /// mounts them.
    pub(crate) fn of_prefix(prefix: &str) -> Option<Self> {
        match prefix {
            "rar" => Some(Self::Rar),
            "zip" => Some(Self::Zip),
            "7zip" => Some(Self::SevenZ),
            "tar" => Some(Self::Tar),
            "tgz" => Some(Self::TarGz),
            "iso" => Some(Self::Iso),
            _ => None,
        }
    }

    /// The translator for this format -- `None` only for RAR in a build
    /// without the `rar` feature, which has no reader for it at all and
    /// answers `rar_disabled_response` instead.
    fn translator(self) -> Option<Box<dyn Translator>> {
        match self {
            Self::Zip => Some(Box::new(crate::translators::zip::Zip)),
            Self::Tar => Some(Box::new(crate::translators::tar::Tar)),
            Self::TarGz => Some(Box::new(crate::translators::TarGz)),
            Self::Iso => Some(Box::new(crate::translators::iso::Iso)),
            Self::SevenZ => Some(Box::new(crate::translators::sevenz::SevenZ)),
            #[cfg(feature = "rar")]
            Self::Rar => Some(Box::new(crate::translators::rar::Rar)),
            #[cfg(not(feature = "rar"))]
            Self::Rar => None,
        }
    }
}

/// What a refused member is answered with, in **one** place: `415` for a
/// member this server will not serve by range (§3 of the design), `422`
/// for a container that contradicts itself. The body carries the kind the
/// client switches on and the sentence it shows -- xtremio's
/// `archive_sniff` already has the place for it.
fn refusal_response(refusal: &Refusal) -> Response {
    let status = match refusal {
        Refusal::Compressed { .. }
        | Refusal::Encrypted
        | Refusal::Solid
        | Refusal::NoRandomAccess { .. }
        | Refusal::Unsupported { .. } => StatusCode::UNSUPPORTED_MEDIA_TYPE,
        Refusal::Malformed(_) => StatusCode::UNPROCESSABLE_ENTITY,
    };
    (
        status,
        Json(serde_json::json!({
            "refused": refusal.kind(),
            "message": refusal.to_string(),
        })),
    )
        .into_response()
}

/// What a URL that cannot be a source is answered with. **An origin that
/// will not serve ranges is `501`**, because it is this server that
/// declines to do the work: serving a member out of it would mean
/// downloading the whole archive first, which is the thing the design
/// exists to stop.
///
/// Shared with `routes::drive`, which opens a `ProxySource` of its own and
/// owes a client the same four answers about one.
pub(crate) fn source_error_response(error: &ProxySourceError) -> Response {
    // `noRanges` is a *refusal*, in the same shape as a translator's, and
    // not an `error`: it is a thing about this source that the viewer can
    // be told in the viewer's own words ("this link will not serve the
    // film in pieces"), and a client that has to tell it from the other
    // `501` -- a build with no reader for the format, which is a sentence
    // for whoever built the app and never for a television -- would
    // otherwise have to match English. See [`no_reader_response`].
    match error {
        ProxySourceError::WillNotRange => (
            StatusCode::NOT_IMPLEMENTED,
            Json(serde_json::json!({
                "refused": "noRanges",
                "message": error.to_string(),
            })),
        )
            .into_response(),
        ProxySourceError::Origin(StatusCode::NOT_FOUND) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        // `Credentials` cannot arise on the archive routes -- a
        // `/{fmt}/create` relays the caller's own `h=`, which is a value
        // and not a grant to renew -- and it is answered rather than
        // `unreachable!()`d, because the arm that says "this cannot
        // happen" is how a later source that mints its own header gets a
        // panic instead of a status -- and `/drive` is exactly that
        // source: it shares this function, and its one terminal case --
        // a grant that is gone -- never reaches here, because
        // `DriveError` lifts `pairAgain` out of a `Credentials` before
        // answering (`sources::drive`). A `502` is right for what is left
        // either way: the far end would not authorise us.
        ProxySourceError::Origin(_)
        | ProxySourceError::Fetch(_)
        | ProxySourceError::Credentials(_) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

/// What a `/create` asks for: the volume list, and which member.
#[derive(Debug, Clone)]
pub(crate) struct ArchiveCreateRequest {
    pub(crate) urls: Vec<String>,
    pub(crate) file_idx: Option<usize>,
    pub(crate) file_must_include: Vec<String>,
}

/// The whole archive API under one format prefix: [`session_router`] and
/// [`stream_router`] together, which is what the loopback listener mounts.
pub fn router(format: Format) -> Router<AppState> {
    stream_router(format).merge(session_router(format))
}

/// The session-creating half: `/create` takes a container by URL -- the
/// volume list, from wherever the caller names -- reads its index off it
/// and remembers that, with the member chosen, under a key. Loopback only:
/// it reaches out to a caller-named address, which is why the LAN listener
/// never mounts any of this: it serves published cast tokens alone
/// (`crate::cast`).
pub fn session_router(format: Format) -> Router<AppState> {
    Router::new()
        .route(
            "/create",
            get(create_session_auto).post(create_session_auto),
        )
        .route(
            "/create/{key}",
            get(create_session_with_key).post(create_session_with_key),
        )
        // The prefix these routes are mounted under, as the handlers read
        // it: `crate::archive_prefixes` mounts one of these per format.
        .layer(Extension(format))
}

/// The byte-serving half: a member read out of a session [`session_router`]
/// already created. Nothing here fetches, opens or names anything -- an
/// unknown key is a `404`.
pub fn stream_router(format: Format) -> Router<AppState> {
    Router::new()
        .route("/stream", get(stream_content_query))
        .route("/stream/{key}", get(stream_redirection))
        .route("/stream/{key}/{*file}", get(stream_content_path))
        .layer(Extension(format))
}

/// What a build without the `rar` cargo feature says about a RAR. The
/// feature is off in the MIT build, because `unrar-rs` is GPL-3.0 (see
/// `server/Cargo.toml` and AGENTS.md) -- so such a build has no RAR
/// reader at all, and says so rather than failing as some other error.
#[cfg(not(feature = "rar"))]
const RAR_DISABLED_ERROR: &str =
    "RAR support is not compiled into this build (rebuild with the \"rar\" cargo feature)";

/// 501 JSON response returned for RAR requests when the "rar" cargo feature
/// is not compiled into this build.
#[cfg(not(feature = "rar"))]
fn rar_disabled_response() -> Response {
    tracing::warn!("RAR request rejected: RAR support is not compiled into this build");
    no_reader_response(RAR_DISABLED_ERROR)
}

/// **This build has no reader for the format**, which is a fact about the
/// build and not about the source: `noReader`, so a client shows its own
/// sentence ("this build cannot read RAR archives") rather than the one
/// here, which names a cargo feature and is for whoever builds the app.
/// The `message` travels all the same, for a log and for a developer.
fn no_reader_response(message: &str) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(serde_json::json!({
            "refused": "noReader",
            "message": message,
        })),
    )
        .into_response()
}

pub(crate) fn parse_create_request(
    lz: Option<String>,
    body: &axum::body::Bytes,
) -> Result<ArchiveCreateRequest, String> {
    let value = if let Some(lz) = lz {
        let utf16 = lz_str::decompress_from_encoded_uri_component(&lz)
            .ok_or_else(|| "Failed to decompress lz payload".to_string())?;
        let json =
            String::from_utf16(&utf16).map_err(|_| "Invalid UTF-16 in lz payload".to_string())?;
        serde_json::from_str::<serde_json::Value>(&json)
            .map_err(|err| format!("Invalid lz JSON payload: {err}"))?
    } else if body.is_empty() {
        return Err("Missing archive create payload".to_string());
    } else {
        serde_json::from_slice::<serde_json::Value>(body)
            .map_err(|err| format!("Invalid JSON payload: {err}"))?
    };

    archive_request_from_value(&value)
}

fn archive_request_from_value(value: &serde_json::Value) -> Result<ArchiveCreateRequest, String> {
    if let Some(items) = value.as_array() {
        let urls = items
            .iter()
            .filter_map(|item| {
                item.get("url")
                    .and_then(|url| url.as_str())
                    .or_else(|| item.as_str())
                    .map(str::to_string)
            })
            .collect::<Vec<_>>();
        return Ok(ArchiveCreateRequest {
            urls,
            file_idx: None,
            file_must_include: Vec::new(),
        });
    }

    let Some(obj) = value.as_object() else {
        return Err("Archive create payload must be an object or array".to_string());
    };

    let urls = obj
        .get("urls")
        .and_then(|urls| urls.as_array())
        .map(|urls| {
            urls.iter()
                .filter_map(archive_url_from_value)
                .collect::<Vec<_>>()
        })
        .or_else(|| {
            obj.get("url")
                .and_then(|url| url.as_str())
                .map(|url| vec![url.to_string()])
        })
        .unwrap_or_default();

    let file_idx = obj
        .get("fileIdx")
        .and_then(|idx| idx.as_u64())
        .map(|idx| idx as usize);
    let file_must_include = obj
        .get("fileMustInclude")
        .and_then(|filters| filters.as_array())
        .map(|filters| {
            filters
                .iter()
                .filter_map(|filter| filter.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    Ok(ArchiveCreateRequest {
        urls,
        file_idx,
        file_must_include,
    })
}

fn archive_url_from_value(value: &serde_json::Value) -> Option<String> {
    value.as_str().map(str::to_string).or_else(|| {
        value
            .as_array()
            .and_then(|parts| parts.first())
            .and_then(|url| url.as_str())
            .map(str::to_string)
    })
}

async fn create_session_auto(
    State(state): State<AppState>,
    Extension(format): Extension<Format>,
    method: Method,
    Query(query): Query<CreateQuery>,
    body: axum::body::Bytes,
) -> Response {
    let key = Uuid::new_v4().to_string();
    create_session_internal(state, format, key, method, query, body).await
}

async fn create_session_with_key(
    State(state): State<AppState>,
    Extension(format): Extension<Format>,
    Path(key): Path<String>,
    method: Method,
    Query(query): Query<CreateQuery>,
    body: axum::body::Bytes,
) -> Response {
    create_session_internal(state, format, key, method, query, body).await
}

async fn create_session_internal(
    state: AppState,
    format: Format,
    key: String,
    method: Method,
    query: CreateQuery,
    body: axum::body::Bytes,
) -> Response {
    let payload = match parse_create_request(query.lz, &body) {
        Ok(payload) => payload,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let translator = match translator_for(format) {
        Ok(translator) => translator,
        Err(error) => return error.response(),
    };
    let (session, selected) =
        match create_session(&state, translator.as_ref(), key.clone(), &payload).await {
            Ok(made) => made,
            Err(error) => return error.response(),
        };
    let selected_name = selected
        .and_then(|at| session.index().members.get(at))
        .map(|member| member.name.clone());
    drop(session);

    if method == Method::GET
        && let Some(file) = selected_name
    {
        return Redirect::temporary(&format!(
            "./stream/{}/{}",
            urlencoding::encode(&key),
            encode_path_segments(&file)
        ))
        .into_response();
    }
    Json(CreateResponse { key }).into_response()
}

/// Why a container's session could not be had -- made by a `/create`, found
/// under a key, or indexed off a torrent's file for the `torrent:` form.
///
/// **One set of reasons, two framings.** The archive routes answer each with
/// the status and body they always have ([`Self::response`]); a media id
/// naming a member is refused with the same reason, typed
/// (`crate::media::Refusal::of_session`). Neither framing lives in the
/// other: the work is the same function for both.
#[derive(Debug)]
pub(crate) enum SessionError {
    /// The create's payload, or a URL in it, cannot be taken (`400`, with
    /// the sentence).
    BadRequest(String),
    /// A create under a key another archive's session holds (`409`).
    KeyInUse,
    /// A link that cannot be a source: `noRanges`, a `404`, a far end that
    /// would not answer ([`source_error_response`]).
    Source(ProxySourceError),
    /// What the container says: a member or a whole container this server
    /// will not serve by range (`415`/`422`).
    Refused(Refusal),
    /// The create's `fileIdx`/`fileMustInclude` matched no member (`404`).
    NoMember,
    /// No session under a key that no create or `torrent:` form can make
    /// one from (`404`).
    NoSession,
    /// A `torrent:` key with no path in it (`400`).
    BadKey,
    /// The `torrent:` key names a torrent this engine does not hold, or a
    /// file it does not have (`404`).
    NotInTorrent,
    /// This build has no reader for the format ([`no_translator_response`]).
    NoReader(Format),
}

impl SessionError {
    /// The archive routes' answer, exactly as each has always been.
    pub(crate) fn response(&self) -> Response {
        match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message.clone()).into_response(),
            Self::KeyInUse => (
                StatusCode::CONFLICT,
                "That session key is in use for another archive",
            )
                .into_response(),
            Self::Source(error) => source_error_response(error),
            Self::Refused(refusal) => refusal_response(refusal),
            Self::NoMember => {
                (StatusCode::NOT_FOUND, "Failed to select archive file").into_response()
            }
            Self::NoSession | Self::NotInTorrent => StatusCode::NOT_FOUND.into_response(),
            Self::BadKey => StatusCode::BAD_REQUEST.into_response(),
            Self::NoReader(format) => no_translator_response(*format),
        }
    }
}

/// The translator for `format`, or why this build has none.
pub(crate) fn translator_for(format: Format) -> Result<Box<dyn Translator>, SessionError> {
    format.translator().ok_or(SessionError::NoReader(format))
}

/// The sentence a build with no reader for `format` says, for a client
/// that is not answered with [`no_translator_response`]'s body.
pub(crate) fn no_reader_message(format: Format) -> &'static str {
    #[cfg(not(feature = "rar"))]
    if format == Format::Rar {
        return RAR_DISABLED_ERROR;
    }
    let _ = format;
    "this build has no reader for that format"
}

/// What a format this build has no reader for answers.
///
/// There is exactly one: RAR in a build without the `rar` cargo feature,
/// which leaves `unrar-rs` out for its licence (`server/Cargo.toml`) and
/// so has no RAR reader at all. Every other format the router mounts is
/// compiled in, so the arm after it is unreachable -- answered rather than
/// panicked, because a route that answers beats a process that goes down.
fn no_translator_response(format: Format) -> Response {
    #[cfg(not(feature = "rar"))]
    if format == Format::Rar {
        return rar_disabled_response();
    }
    tracing::error!(?format, "this build mounted a format it cannot read");
    no_reader_response(no_reader_message(format))
}

/// How a set of volumes is named as one session.
///
/// **Every** URL, and not the first: `rarUrls` is a list of volumes, two
/// sets can share a `.part1.rar` and differ after it, and a session found
/// by the first URL alone would answer one set's index over the other's
/// bytes. A newline cannot occur inside a URL, so the join is unambiguous.
/// A single-volume archive's origin is its URL, exactly as before.
fn set_origin(urls: &[String]) -> String {
    urls.join("\n")
}

/// `/{zip|tar|rar}/create`: every URL becomes a [`ProxySource`], the
/// translator indexes them **in the order they were given**, and what is
/// remembered under the key is the index -- no file, nothing on disk,
/// nothing to sweep but memory. Answers the session, leased, and the
/// member the request picked.
///
/// The list is the volume list. For RAR that is the ordinary case: from an
/// addon, `rarUrls` *is* `.part1.rar`, `.part2.rar`, ... in order, and the
/// extents a member is made of name the volume each part is in
/// (`docs/design/translated-sources.md` §2.2). A format that does not come in
/// sets reads `sources[0]` and says so about the rest, which is what every
/// translator but RAR does.
///
/// The route's `/create` and a media id naming the same URL
/// (`crate::media`) both make their session here.
pub(crate) async fn create_session(
    state: &AppState,
    translator: &dyn Translator,
    key: String,
    payload: &ArchiveCreateRequest,
) -> Result<(Lease<TranslatedSession>, Option<usize>), SessionError> {
    let Some(url) = payload.urls.first() else {
        return Err(SessionError::BadRequest(
            "No archive URL provided".to_string(),
        ));
    };
    // Only what the route is named for: archives at web addresses. This
    // route is open to any loopback caller -- on Android, every app on the
    // device, and any page in a browser on it -- so a URL that was taken
    // as a local path served the members of any archive this process could
    // read, its own private storage included. Every volume is checked, not
    // just the first: a set is read whole, so a local path anywhere in the
    // list would be read.
    if !payload
        .urls
        .iter()
        .all(|url| url.starts_with("http://") || url.starts_with("https://"))
    {
        return Err(SessionError::BadRequest(
            "Failed to resolve archive URL".to_string(),
        ));
    }
    let origin = set_origin(&payload.urls);
    // A key the caller chose (`/{fmt}/create/{key}`) may name a session
    // that already exists, and replacing it points every later
    // `/{fmt}/stream/{key}/...` at a different archive: the player that
    // was reading one file seeks and reads another's bytes. A repeat of
    // the same create is the ordinary case (a re-play sends it again).
    if let Some(existing) = state.translated_archives.get(&key)
        && existing.origin() != origin
    {
        tracing::warn!(
            key = %key,
            "a create under an existing session's key named a different archive; refused"
        );
        return Err(SessionError::KeyInUse);
    }

    // A session that already holds this URL has already probed the origin
    // and read the index off it; a re-play sends the same create again and
    // should cost neither.
    let indexed = match state
        .translated_archives
        .find(|session| session.origin() == origin)
    {
        Some(existing) => {
            tracing::info!(
                origin = %util::log_origin(url),
                "reusing the index an existing session holds"
            );
            let SessionSources::Held(sources) = existing.sources() else {
                // Only a `torrent:` session is not `Held`, and its origin
                // is its key, which is never a URL.
                unreachable!("a session found by URL holds its sources")
            };
            (sources.clone(), existing.index().clone())
        }
        None => {
            // One source per volume, in the order they were given: an
            // `Extent`'s `source` is an index into this list, so a set
            // probed out of order would serve every part of the film from
            // the wrong volume.
            let mut sources: Vec<Arc<ProxySource>> = Vec::with_capacity(payload.urls.len());
            for url in &payload.urls {
                let parsed = match url::Url::parse(url) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        tracing::warn!(
                            origin = %util::log_origin(url),
                            %error,
                            "the archive URL does not parse"
                        );
                        return Err(SessionError::BadRequest(
                            "Failed to resolve archive URL".to_string(),
                        ));
                    }
                };
                let source = match ProxySource::open(
                    state.proxy_cache.clone(),
                    state.http_addr,
                    parsed,
                    // No `h=` of any kind on this route yet; the caller's
                    // headers are what would go here, which is what makes
                    // these reads the relay the cache refuses to key on.
                    std::collections::BTreeMap::new(),
                )
                .await
                {
                    Ok(source) => source,
                    Err(error) => {
                        tracing::warn!(
                            origin = %util::log_origin(url),
                            %error,
                            "the archive URL cannot be read by range"
                        );
                        return Err(SessionError::Source(error));
                    }
                };
                sources.push(Arc::new(source));
            }
            match translator.index(&as_byte_sources(&sources)).await {
                Ok(index) => (sources, index),
                Err(refusal) => {
                    tracing::warn!(
                        origin = %util::log_origin(url),
                        volumes = payload.urls.len(),
                        %refusal,
                        "the archive could not be indexed"
                    );
                    return Err(SessionError::Refused(refusal));
                }
            }
        }
    };
    let (sources, index) = indexed;

    let selected = select_member(&index, payload.file_idx, &payload.file_must_include)?;
    // A create that named a member this server will not serve says so now
    // rather than at the first byte: the player has a sentence to show and
    // no session to clean up.
    if let Some(member) = selected.and_then(|at| index.members.get(at))
        && let MemberBody::Opaque(refusal) = &member.body
    {
        tracing::info!(
            origin = %util::log_origin(url),
            member = %member.name,
            %refusal,
            "the selected member cannot be served by range"
        );
        return Err(SessionError::Refused(refusal.clone()));
    }
    let session = state.translated_archives.insert(
        key,
        TranslatedSession::new(origin, SessionSources::Held(sources), index, selected),
    );
    Ok((session, selected))
}

/// Held proxy sources as the sources a translator and a view read.
fn as_byte_sources(sources: &[Arc<ProxySource>]) -> Vec<Arc<dyn ByteSource>> {
    sources
        .iter()
        .map(|source| source.clone() as Arc<dyn ByteSource>)
        .collect()
}

/// Which member of `index` a request picked, by the `fileIdx` /
/// `fileMustInclude` contract stremio-core's `rarUrls`/`zipUrls` build.
pub(crate) fn select_member(
    index: &crate::translators::Index,
    file_idx: Option<usize>,
    file_must_include: &[String],
) -> Result<Option<usize>, SessionError> {
    let files = index
        .members
        .iter()
        .enumerate()
        .map(|(at, member)| compat::FileCandidate {
            index: at,
            name: member.name.clone(),
            length: member.len,
        })
        .collect::<Vec<_>>();
    if files.is_empty() {
        return Ok(None);
    }
    let requested_idx = file_idx
        .map(|idx| idx.to_string())
        .unwrap_or_else(|| "-1".to_string());
    compat::resolve_file_idx(&requested_idx, &files, file_must_include)
        .map(Some)
        .map_err(|err| {
            tracing::warn!(error = %err, "failed to resolve archive file");
            SessionError::NoMember
        })
}

async fn stream_content_query(
    State(state): State<AppState>,
    Extension(format): Extension<Format>,
    headers: header::HeaderMap,
    Query(params): Query<StreamParams>,
) -> Response {
    stream_member(
        &state,
        format,
        &params.key,
        params.file.as_deref(),
        &headers,
    )
    .await
}

async fn stream_content_path(
    State(state): State<AppState>,
    Extension(format): Extension<Format>,
    headers: header::HeaderMap,
    Path((key, file)): Path<(String, String)>,
) -> Response {
    stream_member(&state, format, &key, Some(&file), &headers).await
}

async fn stream_redirection(
    State(state): State<AppState>,
    Extension(format): Extension<Format>,
    Path(key): Path<String>,
    Query(selection): Query<MemberSelection>,
) -> Response {
    let translator = match translator_for(format) {
        Ok(translator) => translator,
        Err(error) => return error.response(),
    };
    // **The session first, which for a `torrent:` key is what indexes it.**
    // A container inside a torrent has no `/create` to be made at, and its
    // member names are known only to its index -- so indexing it here is
    // what lets a client ask for a member of a torrent's archive without
    // being told its name first.
    let session = match session_for(&state, translator.as_ref(), &key).await {
        Ok(session) => session,
        Err(error) => return error.response(),
    };
    let selected = match chosen_member(&session, selection.file_idx, &selection.file_must_include())
    {
        Ok(selected) => selected,
        Err(error) => return error.response(),
    };
    match selected.as_deref() {
        Some(file) => Redirect::temporary(&format!(
            "./{}/{}",
            urlencoding::encode(&key),
            encode_path_segments(file)
        ))
        .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// The member `session` plays when no member is named: the one its create
/// selected, or -- for a `torrent:` session, which was made by no create
/// and carries none -- the one the same rule `/create` uses picks, `-1`
/// and no filters unless the caller states them, which is the contract
/// `/create` has.
pub(crate) fn chosen_member(
    session: &TranslatedSession,
    file_idx: Option<usize>,
    file_must_include: &[String],
) -> Result<Option<String>, SessionError> {
    if let Some(member) = session.selected() {
        return Ok(Some(member.name.clone()));
    }
    Ok(
        select_member(session.index(), file_idx, file_must_include)?.and_then(|at| {
            session
                .index()
                .members
                .get(at)
                .map(|member| member.name.clone())
        }),
    )
}

/// Which member a redirect should pick, for a session that has not chosen
/// one -- the same two things `/create`'s request carries, as query
/// parameters, so a `torrent:` container can be pointed at a file the way a
/// link-borne one can. `f` is the spelling the stream route already uses for
/// its filters; `fileIdx` is `/create`'s.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MemberSelection {
    pub(crate) file_idx: Option<usize>,
    #[serde(default, alias = "f")]
    pub(crate) file_must_include: Option<String>,
}

impl MemberSelection {
    pub(crate) fn file_must_include(&self) -> Vec<String> {
        self.file_must_include
            .iter()
            .flat_map(|value| value.split(','))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect()
    }
}

/// One member of one session, as a range of bytes.
async fn stream_member(
    state: &AppState,
    format: Format,
    key: &str,
    file: Option<&str>,
    headers: &header::HeaderMap,
) -> Response {
    let translator = match translator_for(format) {
        Ok(translator) => translator,
        Err(error) => return error.response(),
    };
    stream_translated(state, translator.as_ref(), key, file, headers).await
}

/// A member of a translated container, served as byte ranges of whatever
/// holds the container's own bytes.
///
/// **Nothing about a member's HTTP behaviour differs from a plain file's**:
/// the framing is `util::MediaRange`, which is the torrent stream route's
/// own, so `Content-Length`, `Content-Range`, `206`/`416` and `HEAD` are
/// the same answers here as there.
async fn stream_translated(
    state: &AppState,
    translator: &dyn Translator,
    key: &str,
    file: Option<&str>,
    headers: &header::HeaderMap,
) -> Response {
    let session = match session_for(state, translator, key).await {
        Ok(session) => session,
        Err(error) => return error.response(),
    };
    let Some(member) = (match file {
        Some(file) => session.member(file),
        None => session.selected(),
    }) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if let MemberBody::Opaque(refusal) = &member.body {
        return refusal_response(refusal);
    }
    let name = member.name.clone();
    let sources = match sources_for(state, &session).await {
        Ok(sources) => sources,
        Err(error) => return error.response(),
    };
    let view = match session.view(member, sources) {
        Ok(view) => view,
        Err(error) => {
            tracing::error!(member = %name, %error, "a member's extents do not fit its sources");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let size = view.len();
    let range = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok());
    let Some(framing) = MediaRange::of(range, size) else {
        return util::range_not_satisfiable(size);
    };
    let mut res_headers = header::HeaderMap::new();
    res_headers.insert(
        header::CONTENT_TYPE,
        mime_guess::from_path(&name)
            .first_or_octet_stream()
            .as_ref()
            .parse()
            .unwrap(),
    );
    framing.write_headers(size, &mut res_headers);
    compat::add_dlna_headers(&mut res_headers);

    // The reader starts where the range does and stops where it ends; the
    // session's lease rides inside the body, so the session is in use for
    // as long as the player reads and cannot be taken under it -- not by
    // the cap and not by the viewer opening something else, which is what
    // ends an unleased one (`translators::session`). For a torrent-backed
    // member the source inside the view holds the stream registration, and
    // it goes the same way.
    let reader = view
        .reader_at(framing.start)
        .take(framing.content_length(size));
    let body = Body::from_stream(media_body(reader).map(move |chunk| {
        let _in_use = &session;
        chunk
    }));
    (framing.status(), res_headers, body).into_response()
}

/// `key` in the one spelling the session registry files it under.
///
/// A `torrent:<info hash>/<path>` key names its torrent by a hash, and a
/// hash is hex whatever its case: the engine folds it before it looks a
/// torrent up (`TorrentSource`), so `torrent:ABC…/film.rar` and
/// `torrent:abc…/film.rar` name one archive -- filed as written, they would
/// be two sessions over it, each indexing it and each holding a lease. Only
/// the hash is folded; the path is the torrent's own and its case means
/// something. Any other key is the registry's own name for a `/create`d
/// session and is returned as it came.
fn canonical_key(key: &str) -> Cow<'_, str> {
    match key
        .strip_prefix("torrent:")
        .and_then(|rest| rest.split_once('/'))
    {
        Some((hash, path)) if hash.bytes().any(|byte| byte.is_ascii_uppercase()) => {
            Cow::Owned(format!("torrent:{}/{path}", hash.to_ascii_lowercase()))
        }
        _ => Cow::Borrowed(key),
    }
}

/// The session under `key`, leased -- creating it for the `torrent:` form,
/// which has no `/create` of its own. What the archive routes and a media
/// id naming a member (`crate::media`) both find a session with.
pub(crate) async fn session_for(
    state: &AppState,
    translator: &dyn Translator,
    key: &str,
) -> Result<Lease<TranslatedSession>, SessionError> {
    let key = canonical_key(key);
    let key = key.as_ref();
    if let Some(session) = state.translated_archives.get(key) {
        return Ok(session);
    }
    // `torrent:<info hash>/<path in the torrent>`: the archive is a file
    // of a torrent this server already has, and the first request for a
    // member of it is what indexes it.
    let Some(rest) = key.strip_prefix("torrent:") else {
        return Err(SessionError::NoSession);
    };
    let Some((info_hash, path)) = rest.split_once('/') else {
        return Err(SessionError::BadKey);
    };
    // The named file may be one volume of a set, and for RAR it usually
    // is: the rest of the set is the files beside it in the torrent, and
    // the translator's own naming rules say which and in what order
    // (`translators::rar::volume_set`). A format that does not come in
    // sets answers with the one file, which is the trait's default.
    let siblings = TorrentSource::file_names(&state.engine, info_hash)
        .await
        .map_err(|error| {
            tracing::warn!(%info_hash, %error, "no such torrent in this engine");
            SessionError::NotInTorrent
        })?;
    let paths = translator.volumes(path, &siblings).map_err(|refusal| {
        // A set with a hole in it is `Malformed`, naming the volume it
        // wanted -- `422`, and said at the index rather than as a short
        // read in the middle of a film.
        tracing::warn!(%info_hash, archive = path, %refusal, "the set is not all there");
        SessionError::Refused(refusal)
    })?;
    // Held before the first volume opens: the opens below are what would
    // move the live entity, and the hold is what makes an open of the
    // second volume beside the first rather than a move off it.
    let hold = (paths.len() > 1).then(|| {
        let files = paths
            .iter()
            .filter_map(|volume| siblings.iter().position(|name| name == volume))
            .collect();
        state.engine.live().hold_set(info_hash, files)
    });
    let mut sources: Vec<Arc<dyn ByteSource>> = Vec::with_capacity(paths.len());
    for volume in &paths {
        let source = TorrentSource::open(state.engine.clone(), info_hash, volume)
            .await
            .map_err(|error| {
                tracing::warn!(%info_hash, archive = %volume, %error, "no such archive in that torrent");
                SessionError::NotInTorrent
            })?;
        sources.push(Arc::new(source));
    }
    let index = translator.index(&sources).await.map_err(|refusal| {
        tracing::warn!(%info_hash, archive = path, %refusal, "the archive could not be indexed");
        SessionError::Refused(refusal)
    })?;
    // The sources are **not** kept: see `SessionSources::Torrent`. The
    // insert leases what it made, so the read that follows cannot find it
    // gone: indexing the container is itself what moved the live entity,
    // and a look-up after the insert would be a second chance for the cell
    // to move again in between. Insert-if-absent, because a player opens a
    // member with several requests at once: two that both missed above
    // both indexed, and the first one in is the session every later request
    // gets -- the other index is dropped rather than replacing a session
    // somebody already holds a lease on.
    Ok(state.translated_archives.get_or_insert_with(key, || {
        TranslatedSession::new(
            key,
            SessionSources::Torrent {
                info_hash: info_hash.to_string(),
                paths,
                hold,
            },
            index,
            None,
        )
    }))
}

/// The sources a body of this session reads through -- the ones it holds,
/// or a torrent file opened for this read alone.
async fn sources_for(
    state: &AppState,
    session: &TranslatedSession,
) -> Result<Vec<Arc<dyn ByteSource>>, SessionError> {
    match session.sources() {
        SessionSources::Held(sources) => Ok(as_byte_sources(sources)),
        SessionSources::Torrent {
            info_hash, paths, ..
        } => {
            // Every volume, in the order the index was read in: an
            // `Extent`'s `source` indexes this list.
            let mut sources: Vec<Arc<dyn ByteSource>> = Vec::with_capacity(paths.len());
            for path in paths {
                let source = TorrentSource::open(state.engine.clone(), info_hash, path)
                    .await
                    .map_err(|error| {
                        tracing::warn!(%info_hash, archive = %path, %error, "the torrent this archive is in is gone");
                        SessionError::NotInTorrent
                    })?;
                sources.push(Arc::new(source));
            }
            Ok(sources)
        }
    }
}

fn encode_path_segments(path: &str) -> String {
    path.split('/')
        .map(|segment| urlencoding::encode(segment).into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    /// **A `torrent:` key's hash is folded, and nothing else is.** The
    /// session registry is keyed by the string, so two spellings of one
    /// hash would otherwise be two sessions over one archive.
    #[test]
    fn a_torrent_key_is_filed_under_its_lowercase_hash() {
        const UPPER: &str = "torrent:0123456789ABCDEF0123456789ABCDEF01234567/Film.Part1.RAR";
        const LOWER: &str = "torrent:0123456789abcdef0123456789abcdef01234567/Film.Part1.RAR";
        assert_eq!(canonical_key(UPPER), canonical_key(LOWER));
        assert_eq!(canonical_key(UPPER), LOWER, "the path keeps its case");
        assert!(matches!(canonical_key(LOWER), Cow::Borrowed(_)));
        // A `/create`d session's key is the registry's own, and a key
        // with no path is refused later; neither is rewritten.
        for key in ["0123ABCD", "torrent:ABCDEF"] {
            assert_eq!(canonical_key(key), key);
        }
    }

    /// A body over a reader that can fill whatever it is handed is read in
    /// media-sized chunks, not the 4 KiB `ReaderStream::new` would ask for.
    #[tokio::test]
    async fn a_member_body_is_read_in_media_sized_chunks() {
        let data = vec![7u8; 3 * MEDIA_BODY_CHUNK_BYTES + 1];
        let mut body = media_body(std::io::Cursor::new(data));
        let first = body.next().await.expect("a chunk").expect("no error");
        assert_eq!(first.len(), MEDIA_BODY_CHUNK_BYTES);
    }
}
