use crate::routes::compat;
use crate::routes::util::{self, MediaRange};
use crate::sources::proxy::ProxySourceError;
use crate::sources::{ByteSource, ProxySource, TorrentFileSource};
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
/// hot path of the weakest devices this runs on. The same figure the
/// torrent stream route reads with (`routes::stream`), and the same
/// 256 KiB per active stream it costs.
pub(crate) const MEDIA_BODY_CHUNK_BYTES: usize = 256 * 1024;

/// The response body for a media reader: [`MEDIA_BODY_CHUNK_BYTES`] per read.
pub(crate) fn media_body<R: tokio::io::AsyncRead>(reader: R) -> ReaderStream<R> {
    ReaderStream::with_capacity(reader, MEDIA_BODY_CHUNK_BYTES)
}

/// Which container format a URL prefix names.
///
/// The prefix used to be decorative: one handler set served every format
/// and worked out which it was from the file's suffix. It is the format
/// now, because that is what says *which translator reads this*
/// (`crate::translators`).
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
    /// The translator for this format -- `None` only for RAR in a build
    /// without the `rar` feature, which has no reader for it at all and
    /// answers [`rar_disabled_response`] instead.
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
fn source_error_response(error: &ProxySourceError) -> Response {
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
        ProxySourceError::Origin(_) | ProxySourceError::Fetch(_) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

#[derive(Debug)]
struct ArchiveCreateRequest {
    urls: Vec<String>,
    file_idx: Option<usize>,
    file_must_include: Vec<String>,
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
/// never mounts this half (see `crate::lan_media_routes`).
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
/// unknown key is a `404` -- which is what lets the LAN listener mount it.
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

fn parse_create_request(
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
    let Some(translator) = format.translator() else {
        return no_translator_response(format);
    };
    create_translated(&state, translator.as_ref(), key, method, payload).await
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
    no_reader_response("this build has no reader for that format")
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
/// nothing to sweep but memory.
///
/// The list is the volume list. For RAR that is the ordinary case: from an
/// addon, `rarUrls` *is* `.part1.rar`, `.part2.rar`, ... in order, and the
/// extents a member is made of name the volume each part is in
/// (`docs/translated-sources.md` §2.2). A format that does not come in
/// sets reads `sources[0]` and says so about the rest, which is what every
/// translator but RAR does.
async fn create_translated(
    state: &AppState,
    translator: &dyn Translator,
    key: String,
    method: Method,
    payload: ArchiveCreateRequest,
) -> Response {
    let Some(url) = payload.urls.first() else {
        return (StatusCode::BAD_REQUEST, "No archive URL provided").into_response();
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
        return (StatusCode::BAD_REQUEST, "Failed to resolve archive URL").into_response();
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
        return (
            StatusCode::CONFLICT,
            "That session key is in use for another archive",
        )
            .into_response();
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
            let mut sources: Vec<Arc<dyn ByteSource>> = Vec::with_capacity(payload.urls.len());
            for url in &payload.urls {
                let parsed = match url::Url::parse(url) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        tracing::warn!(
                            origin = %util::log_origin(url),
                            %error,
                            "the archive URL does not parse"
                        );
                        return (StatusCode::BAD_REQUEST, "Failed to resolve archive URL")
                            .into_response();
                    }
                };
                let source = match ProxySource::open(
                    state.proxy_cache.clone(),
                    state.http_addr,
                    parsed,
                    Default::default(),
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
                        return source_error_response(&error);
                    }
                };
                sources.push(Arc::new(source));
            }
            match translator.index(&sources).await {
                Ok(index) => (sources, index),
                Err(refusal) => {
                    tracing::warn!(
                        origin = %util::log_origin(url),
                        volumes = payload.urls.len(),
                        %refusal,
                        "the archive could not be indexed"
                    );
                    return refusal_response(&refusal);
                }
            }
        }
    };
    let (sources, index) = indexed;

    let selected = match select_member(&index, &payload) {
        Ok(selected) => selected,
        Err(response) => return *response,
    };
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
        return refusal_response(refusal);
    }
    let selected_name = selected
        .and_then(|at| index.members.get(at))
        .map(|member| member.name.clone());
    drop(state.translated_archives.insert(
        key.clone(),
        TranslatedSession::new(origin, SessionSources::Held(sources), index, selected),
    ));

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

/// Which member of `index` the request picked, by the `fileIdx` /
/// `fileMustInclude` contract stremio-core's `rarUrls`/`zipUrls` build.
fn select_member(
    index: &crate::translators::Index,
    request: &ArchiveCreateRequest,
) -> Result<Option<usize>, Box<Response>> {
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
    let requested_idx = request
        .file_idx
        .map(|idx| idx.to_string())
        .unwrap_or_else(|| "-1".to_string());
    compat::resolve_file_idx(&requested_idx, &files, &request.file_must_include)
        .map(Some)
        .map_err(|err| {
            tracing::warn!(error = %err, "failed to resolve archive file");
            Box::new((StatusCode::NOT_FOUND, "Failed to select archive file").into_response())
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
    let Some(translator) = format.translator() else {
        return no_translator_response(format);
    };
    // **The session first, which for a `torrent:` key is what indexes it.**
    // This only looked a session up, so a container inside a torrent -- which
    // has no `/create` to be made at, and whose member names only its index
    // knows -- was a `404` to every request a client could make: the one
    // route that hands out a member name never reached the one function that
    // creates the session. A client cannot ask for a member of a torrent's
    // archive without being told its name first, so this is where it is told.
    let session = match session_for(&state, translator.as_ref(), &key).await {
        Ok(session) => session,
        Err(response) => return *response,
    };
    // A session made by `/create` carries the member its request selected; a
    // `torrent:` session was just made and carries none, so the same rule
    // `/create` uses picks one -- `-1` and no filters unless the caller
    // states them here, which is the contract `/create` has.
    let selected = match session.selected().map(|member| member.name.clone()) {
        Some(name) => Some(name),
        None => {
            let request = ArchiveCreateRequest {
                urls: Vec::new(),
                file_idx: selection.file_idx,
                file_must_include: selection.file_must_include(),
            };
            match select_member(session.index(), &request) {
                Ok(at) => at.and_then(|at| {
                    session
                        .index()
                        .members
                        .get(at)
                        .map(|member| member.name.clone())
                }),
                Err(response) => return *response,
            }
        }
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

/// Which member a redirect should pick, for a session that has not chosen
/// one -- the same two things `/create`'s request carries, as query
/// parameters, so a `torrent:` container can be pointed at a file the way a
/// link-borne one can. `f` is the spelling the stream route already uses for
/// its filters; `fileIdx` is `/create`'s.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct MemberSelection {
    file_idx: Option<usize>,
    #[serde(default, alias = "f")]
    file_must_include: Option<String>,
}

impl MemberSelection {
    fn file_must_include(&self) -> Vec<String> {
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
    let Some(translator) = format.translator() else {
        return no_translator_response(format);
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
        Err(response) => return *response,
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
        Err(response) => return *response,
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
    // as long as the player reads and its idle clock starts when the body
    // is dropped. For a torrent-backed member the source inside the view
    // holds the stream registration, and it goes the same way.
    let reader = view
        .reader_at(framing.start)
        .take(framing.content_length(size));
    let body = Body::from_stream(media_body(reader).map(move |chunk| {
        let _in_use = &session;
        chunk
    }));
    (framing.status(), res_headers, body).into_response()
}

/// The session under `key`, leased -- creating it for the `torrent:` form,
/// which has no `/create` of its own.
async fn session_for(
    state: &AppState,
    translator: &dyn Translator,
    key: &str,
) -> Result<Lease<TranslatedSession>, Box<Response>> {
    if let Some(session) = state.translated_archives.get(key) {
        return Ok(session);
    }
    // `torrent:<info hash>/<path in the torrent>`: the archive is a file
    // of a torrent this server already has, and the first request for a
    // member of it is what indexes it.
    let Some(rest) = key.strip_prefix("torrent:") else {
        return Err(Box::new(StatusCode::NOT_FOUND.into_response()));
    };
    let Some((info_hash, path)) = rest.split_once('/') else {
        return Err(Box::new(StatusCode::BAD_REQUEST.into_response()));
    };
    // The named file may be one volume of a set, and for RAR it usually
    // is: the rest of the set is the files beside it in the torrent, and
    // the translator's own naming rules say which and in what order
    // (`translators::rar::volume_set`). A format that does not come in
    // sets answers with the one file, which is the trait's default.
    let siblings = TorrentFileSource::file_names(&state.engine, info_hash)
        .await
        .map_err(|error| {
            tracing::warn!(%info_hash, %error, "no such torrent in this engine");
            Box::new(StatusCode::NOT_FOUND.into_response())
        })?;
    let paths = translator.volumes(path, &siblings).map_err(|refusal| {
        // A set with a hole in it is `Malformed`, naming the volume it
        // wanted -- `422`, and said at the index rather than as a short
        // read in the middle of a film.
        tracing::warn!(%info_hash, archive = path, %refusal, "the set is not all there");
        Box::new(refusal_response(&refusal))
    })?;
    let mut sources: Vec<Arc<dyn ByteSource>> = Vec::with_capacity(paths.len());
    for volume in &paths {
        let source = TorrentFileSource::open(state.engine.clone(), info_hash, volume)
            .await
            .map_err(|error| {
                tracing::warn!(%info_hash, archive = %volume, %error, "no such archive in that torrent");
                Box::new(StatusCode::NOT_FOUND.into_response())
            })?;
        sources.push(Arc::new(source));
    }
    let index = translator.index(&sources).await.map_err(|refusal| {
        tracing::warn!(%info_hash, archive = path, %refusal, "the archive could not be indexed");
        Box::new(refusal_response(&refusal))
    })?;
    // The sources are **not** kept: see `SessionSources::Torrent`. The
    // insert leases what it made, so the read that follows cannot find it
    // gone: indexing the container is itself what moved the live entity,
    // and a look-up after the insert would be a second chance for the cell
    // to move again in between.
    Ok(state.translated_archives.insert(
        key.to_string(),
        TranslatedSession::new(
            key,
            SessionSources::Torrent {
                info_hash: info_hash.to_string(),
                paths,
            },
            index,
            None,
        ),
    ))
}

/// The sources a body of this session reads through -- the ones it holds,
/// or a torrent file opened for this read alone.
async fn sources_for(
    state: &AppState,
    session: &TranslatedSession,
) -> Result<Vec<Arc<dyn ByteSource>>, Box<Response>> {
    match session.sources() {
        SessionSources::Held(sources) => Ok(sources.clone()),
        SessionSources::Torrent { info_hash, paths } => {
            // Every volume, in the order the index was read in: an
            // `Extent`'s `source` indexes this list.
            let mut sources: Vec<Arc<dyn ByteSource>> = Vec::with_capacity(paths.len());
            for path in paths {
                let source = TorrentFileSource::open(state.engine.clone(), info_hash, path)
                    .await
                    .map_err(|error| {
                        tracing::warn!(%info_hash, archive = %path, %error, "the torrent this archive is in is gone");
                        Box::new(StatusCode::NOT_FOUND.into_response())
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
