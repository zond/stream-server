use crate::archives::sessions::Lease;
use crate::archives::{self, ArchiveSession, ArchiveSource, CacheConfig};
use crate::routes::compat;
use crate::routes::util::{self, MediaRange};
use crate::sources::proxy::ProxySourceError;
use crate::sources::{ByteSource, ProxySource, TorrentFileSource};
use crate::state::AppState;
use crate::translators::session::{SessionSources, TranslatedSession};
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
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
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
/// `ReaderStream::new` reads 4 KiB at a time, and for an archive member that
/// is 4 KiB per `spawn_blocking` round trip through the progressive cache's
/// `tokio::fs::File`, per boxed future, per socket write -- measured at some
/// fourteen times the CPU per byte of a 256 KiB read on a desktop core, on
/// what is the streaming hot path of the weakest devices this runs on. The
/// same figure the torrent stream route reads with (`routes::stream`), and
/// the same 256 KiB per active stream it costs.
pub(crate) const MEDIA_BODY_CHUNK_BYTES: usize = 256 * 1024;

/// The response body for a media reader: [`MEDIA_BODY_CHUNK_BYTES`] per read.
pub(crate) fn media_body<R: tokio::io::AsyncRead>(reader: R) -> ReaderStream<R> {
    ReaderStream::with_capacity(reader, MEDIA_BODY_CHUNK_BYTES)
}

/// Which container format a URL prefix names.
///
/// The prefix used to be decorative: one handler set served every format
/// and worked out which it was from the file's suffix. It is the format
/// now, because that is what says *which translator reads this* -- and
/// which of the two layers the request belongs to while both exist. ZIP
/// and TAR are translated (`crate::translators`); RAR and 7z are still
/// read by the old extracting handlers, until steps 3 and 5 of
/// `docs/translated-sources.md` convert them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Rar,
    Zip,
    SevenZ,
    Tar,
    /// `.tar.gz`, which is a refusal rather than a container: see
    /// [`crate::translators::TarGz`].
    TarGz,
}

impl Format {
    /// The translator for this format, or `None` for one the old
    /// extracting path still owns.
    fn translator(self) -> Option<Box<dyn Translator>> {
        match self {
            Self::Zip => Some(Box::new(crate::translators::zip::Zip)),
            Self::Tar => Some(Box::new(crate::translators::tar::Tar)),
            Self::TarGz => Some(Box::new(crate::translators::TarGz)),
            Self::Rar | Self::SevenZ => None,
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
        | Refusal::NoRandomAccess { .. } => StatusCode::UNSUPPORTED_MEDIA_TYPE,
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
    let status = match error {
        ProxySourceError::WillNotRange => StatusCode::NOT_IMPLEMENTED,
        ProxySourceError::Origin(StatusCode::NOT_FOUND) => StatusCode::NOT_FOUND,
        ProxySourceError::Origin(_) | ProxySourceError::Fetch(_) => StatusCode::BAD_GATEWAY,
    };
    (
        status,
        Json(serde_json::json!({ "error": error.to_string() })),
    )
        .into_response()
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

/// The session-creating half: `/create` takes an archive by URL (fetched
/// whole, from wherever the caller names) or by torrent file, opens it and
/// remembers the choice under a key. Loopback only -- see
/// `crate::lan_media_routes` for why the LAN listener never mounts this
/// half.
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

/// 501 JSON response returned for RAR requests when the "rar" cargo feature
/// is not compiled into this build.
#[cfg(not(feature = "rar"))]
fn rar_disabled_response() -> Response {
    tracing::warn!("RAR request rejected: RAR support is not compiled into this build");
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(serde_json::json!({ "error": crate::archives::RAR_DISABLED_ERROR })),
    )
        .into_response()
}

/// True when `path` points at a RAR archive that this build cannot handle.
#[cfg(not(feature = "rar"))]
fn is_unsupported_rar(path: &std::path::Path) -> bool {
    path.to_string_lossy().to_lowercase().ends_with(".rar")
}

/// Where the archive handlers write, from the live settings: the cache root
/// (see `CacheConfig::scratch_dir` for what goes under it).
async fn archive_cache_config(state: &AppState) -> CacheConfig {
    let settings = state.settings.read().await;
    CacheConfig {
        cache_dir: PathBuf::from(&settings.cache_root),
        _cache_size: crate::routes::system::cache_size_bytes(settings.cache_size),
    }
}

/// The archive `url` names, as a source a session can own: the one an
/// existing session already holds when there is one, else a fresh download
/// (http/https). Nothing else is fetched: see the refusal below.
///
/// Sharing is by the origin string. A player that re-plays a title sends the
/// same `/create` again, and before this every send downloaded the whole
/// archive again beside the last copy.
async fn resolve_source(
    state: &AppState,
    url: &str,
    cache_config: CacheConfig,
) -> Result<Arc<ArchiveSource>, StatusCode> {
    if let Some(existing) = state
        .archive_cache
        .find(|session| session.source.origin() == url)
    {
        tracing::info!(
            origin = %util::log_origin(url),
            "reusing the archive an existing session holds"
        );
        return Ok(existing.source.clone());
    }
    // Only what the route is named for: an archive at a web address. This
    // route is open to any loopback caller -- on Android, every app on the
    // device, and any page in a browser on it -- so a URL that was taken as
    // a local path served the members of any archive this process could
    // read, its own private storage included. Stremio's own server takes
    // these from an add-on's `rarUrls`/`zipUrls`, which are web addresses.
    // `/ftp` was closed the same way.
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(StatusCode::BAD_REQUEST);
    }
    download_archive(url, cache_config, crate::cache_budget::available_space)
        .await
        .map(Arc::new)
}

/// How much of a download is read before its file is named: enough to hold
/// every signature `archives::archive_suffix_from_magic` looks for.
const SNIFF_BYTES: usize = 512;

/// How long a download may take to connect, and how long it may then go
/// without a byte, before it is given up. There is no bound on the whole:
/// an archive is gigabytes over whatever link the origin has.
const DOWNLOAD_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const DOWNLOAD_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// How much a download writes before it reads the volume again. Everything
/// else on the device writes to the same volume in the meantime -- torrents
/// above all -- so one reading at the start is not enough for gigabytes.
const DOWNLOAD_RECHECK_BYTES: u64 = 64 * 1024 * 1024;

/// What a download may still write under the cache root: the volume's free
/// space above the floor, read on the blocking pool and again every
/// [`DOWNLOAD_RECHECK_BYTES`].
///
/// A download goes under the cache root, on the volume the torrent cache
/// and the proxy cache are capped against, and it was written whole with
/// nothing asking the volume anything: a large enough archive went through
/// the floor the rest of the server keeps, to ENOSPC. An unreadable volume
/// is not a full one, as everywhere else.
struct DownloadRoom<P> {
    probe: P,
    dir: PathBuf,
    /// Bytes above the floor at the last reading; `None` when it failed.
    room: Option<u64>,
    written_since: u64,
}

impl<P> DownloadRoom<P>
where
    P: Fn(&std::path::Path) -> Option<u64> + Clone + Send + 'static,
{
    async fn read(probe: P, dir: PathBuf) -> Self {
        let mut room = Self {
            probe,
            dir,
            room: None,
            written_since: 0,
        };
        room.reread().await;
        room
    }

    async fn reread(&mut self) {
        let probe = self.probe.clone();
        let dir = self.dir.clone();
        self.room = tokio::task::spawn_blocking(move || {
            // The floor is the volume's own, read beside its free space.
            let floor = enginefs::free_space_floor(enginefs::volume_total(&dir));
            probe(&dir).map(|available| available.saturating_sub(floor))
        })
        .await
        .ok()
        .flatten();
        self.written_since = 0;
    }

    /// Whether `len` bytes would fit above the floor as of the last reading,
    /// booking nothing: for a length an origin states up front, which is
    /// refused before a byte of it is fetched.
    fn fits(&self, len: u64) -> bool {
        self.room
            .is_none_or(|room| self.written_since.saturating_add(len) <= room)
    }

    /// Whether `len` more bytes fit above the floor, booking them if so.
    async fn take(&mut self, len: u64) -> bool {
        if self.written_since >= DOWNLOAD_RECHECK_BYTES {
            self.reread().await;
        }
        match self.room {
            None => true,
            Some(room) if self.written_since.saturating_add(len) > room => false,
            Some(_) => {
                self.written_since += len;
                true
            }
        }
    }
}

/// Fetch `url` whole into a scratch file under the cache root and hand it
/// back owned, so it is deleted with the last session holding it.
///
/// The file needs an archive suffix, because that is how the reader is
/// chosen -- a download that went without one (every download used to)
/// could never be opened, and the create failed after the whole transfer.
/// The URL's own suffix is used when it has a recognised one; otherwise the
/// first bytes of the body say what it is, and a body that is neither is
/// `415` before it is stored. Nothing is kept on any error: the scratch
/// file is a `NamedTempFile` until the source takes it.
async fn download_archive<P>(
    url: &str,
    cache_config: CacheConfig,
    probe: P,
) -> Result<ArchiveSource, StatusCode>
where
    P: Fn(&std::path::Path) -> Option<u64> + Clone + Send + 'static,
{
    // The origin, never the URL: an archive link is the caller's and may
    // carry credentials in its query (see `util::log_origin`).
    tracing::info!(origin = %util::log_origin(url), "downloading an archive");
    // Without these a download that stalled -- an origin that stopped
    // sending, a link that dropped without a reset -- held its `/create`
    // open for as long as the socket lived.
    let client = enginefs::http_client_builder()
        .connect_timeout(DOWNLOAD_CONNECT_TIMEOUT)
        .read_timeout(DOWNLOAD_READ_TIMEOUT)
        .build()
        .map_err(|e| {
            tracing::error!("Failed to build HTTP client: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let response = client.get(url).send().await.map_err(|e| {
        // `without_url`, because reqwest's `Display` names the URL it was
        // fetching -- the caller's, credentials and all.
        tracing::error!(
            origin = %util::log_origin(url),
            error = %e.without_url(),
            "failed to fetch the archive"
        );
        StatusCode::BAD_REQUEST
    })?;

    if !response.status().is_success() {
        tracing::error!(
            origin = %util::log_origin(url),
            status = %response.status(),
            "the archive's origin refused"
        );
        return Err(StatusCode::NOT_FOUND);
    }

    // The volume the cache root is on, read before a byte is stored; a
    // length the origin states that will not fit is refused before one is
    // even fetched.
    let mut room = DownloadRoom::read(probe, cache_config.cache_dir.clone()).await;
    let too_big = || {
        tracing::warn!(
            origin = %util::log_origin(url),
            "the archive would take the cache volume under its free-space floor"
        );
        StatusCode::INSUFFICIENT_STORAGE
    };
    if let Some(length) = response.content_length()
        && !room.fits(length)
    {
        return Err(too_big());
    }

    let mut content = response.bytes_stream();
    let mut head = Vec::with_capacity(SNIFF_BYTES);
    while head.len() < SNIFF_BYTES {
        match content.next().await {
            Some(chunk) => head.extend_from_slice(&chunk.map_err(|e| {
                tracing::error!(error = %e.without_url(), "download stream error");
                StatusCode::BAD_GATEWAY
            })?),
            None => break,
        }
    }

    let suffix = archives::archive_suffix(&url_file_name(url))
        .or_else(|| archives::archive_suffix_from_magic(&head))
        .ok_or_else(|| {
            tracing::warn!(
                origin = %util::log_origin(url),
                "the URL names no archive format and the bytes are not one"
            );
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        })?;

    let file = archives::scratch_file(&cache_config, suffix).map_err(|e| {
        tracing::error!("Failed to create archive scratch file: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let std_handle = file.as_file().try_clone().map_err(|e| {
        tracing::error!("Failed to open archive scratch file: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let mut async_file = tokio::fs::File::from_std(std_handle);

    let write_error = |e: std::io::Error| {
        tracing::error!("Failed to write archive scratch file: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    };
    let mut pending = Some(bytes::Bytes::from(head));
    loop {
        let chunk = match pending.take() {
            Some(head) => head,
            None => match content.next().await {
                Some(chunk) => chunk.map_err(|e| {
                    tracing::error!(error = %e.without_url(), "download stream error");
                    StatusCode::BAD_GATEWAY
                })?,
                None => break,
            },
        };
        if !room.take(chunk.len() as u64).await {
            return Err(too_big());
        }
        async_file.write_all(&chunk).await.map_err(write_error)?;
    }
    async_file.flush().await.map_err(write_error)?;

    tracing::info!(
        origin = %util::log_origin(url),
        path = ?file.path(),
        "downloaded an archive"
    );
    Ok(ArchiveSource::downloaded(
        file,
        url.to_string(),
        cache_config,
    ))
}

/// The last path segment of `url`, percent-decoded -- what a suffix would
/// be on, without the query a `?dl=1` would otherwise end it with -- or the
/// whole URL when it does not parse.
fn url_file_name(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|parsed| {
            parsed.path_segments().and_then(|mut segments| {
                segments.next_back().map(|segment| {
                    urlencoding::decode(segment)
                        .map(|decoded| decoded.into_owned())
                        .unwrap_or_else(|_| segment.to_string())
                })
            })
        })
        .unwrap_or_else(|| url.to_string())
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

async fn select_archive_file(
    source: &ArchiveSource,
    request: &ArchiveCreateRequest,
) -> Result<Option<String>, StatusCode> {
    let path = source.path();
    let reader = source.reader().await.map_err(|err| {
        tracing::error!(path = %path.display(), error = %err, "failed to create archive reader");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let entries = reader.list_files().await.map_err(|err| {
        tracing::error!(path = %path.display(), error = %err, "failed to list archive files");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let files = entries
        .iter()
        .filter(|entry| !entry.is_dir)
        .enumerate()
        .map(|(index, entry)| compat::FileCandidate {
            index,
            name: entry.path.clone(),
            length: entry.size,
        })
        .collect::<Vec<_>>();

    if files.is_empty() {
        return Ok(None);
    }

    let requested_idx = request
        .file_idx
        .map(|idx| idx.to_string())
        .unwrap_or_else(|| "-1".to_string());
    let selected_idx = compat::resolve_file_idx(&requested_idx, &files, &request.file_must_include)
        .map_err(|err| {
            tracing::warn!(error = %err, "failed to resolve archive file");
            StatusCode::NOT_FOUND
        })?;
    Ok(files
        .into_iter()
        .find(|file| file.index == selected_idx)
        .map(|file| file.name))
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
    match format.translator() {
        Some(translator) => {
            create_translated(&state, translator.as_ref(), key, method, payload).await
        }
        None => create_downloaded(state, key, method, payload).await,
    }
}

/// `/{zip|tar|tgz}/create`: every URL becomes a [`ProxySource`], the
/// translator indexes them, and what is remembered under the key is the
/// index -- no file, nothing on disk, nothing to sweep but memory.
async fn create_translated(
    state: &AppState,
    translator: &dyn Translator,
    key: String,
    method: Method,
    payload: ArchiveCreateRequest,
) -> Response {
    if payload.urls.len() > 1 {
        // A set of volumes is RAR's case, and step 3 of the design is
        // where it lands. One archive in several parts is not a zip or a
        // tar.
        tracing::warn!(
            key = %key,
            url_count = payload.urls.len(),
            "multi-volume archive compatibility requested but not implemented"
        );
        return (
            StatusCode::NOT_IMPLEMENTED,
            "Multi-volume archive streaming is not implemented",
        )
            .into_response();
    }
    let Some(url) = payload.urls.first() else {
        return (StatusCode::BAD_REQUEST, "No archive URL provided").into_response();
    };
    // Only what the route is named for: an archive at a web address. This
    // route is open to any loopback caller -- on Android, every app on the
    // device, and any page in a browser on it -- so a URL that was taken
    // as a local path served the members of any archive this process could
    // read, its own private storage included.
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return (StatusCode::BAD_REQUEST, "Failed to resolve archive URL").into_response();
    }
    // A key the caller chose (`/{fmt}/create/{key}`) may name a session
    // that already exists, and replacing it points every later
    // `/{fmt}/stream/{key}/...` at a different archive: the player that
    // was reading one file seeks and reads another's bytes. A repeat of
    // the same create is the ordinary case (a re-play sends it again).
    if let Some(existing) = state.translated_archives.get(&key)
        && existing.origin() != url.as_str()
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
        .find(|session| session.origin() == url.as_str())
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
            let sources: Vec<Arc<dyn ByteSource>> = vec![Arc::new(source)];
            match translator.index(&sources).await {
                Ok(index) => (sources, index),
                Err(refusal) => {
                    tracing::warn!(
                        origin = %util::log_origin(url),
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
    state.translated_archives.insert(
        key.clone(),
        TranslatedSession::new(url.clone(), SessionSources::Held(sources), index, selected),
    );

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

/// `/{rar|7zip}/create`: the archive is fetched whole into
/// `<cacheRoot>/.archives` and read as a file. **The old shape**, kept
/// only for the two formats whose translators have not landed yet (steps
/// 3 and 5 of `docs/translated-sources.md`); it goes with them.
async fn create_downloaded(
    state: AppState,
    key: String,
    method: Method,
    payload: ArchiveCreateRequest,
) -> Response {
    if payload.urls.len() > 1 {
        tracing::warn!(
            key = %key,
            url_count = payload.urls.len(),
            "multi-volume archive compatibility requested but not implemented"
        );
        return (
            StatusCode::NOT_IMPLEMENTED,
            "Multi-volume archive streaming is not implemented",
        )
            .into_response();
    }

    let Some(url) = payload.urls.first() else {
        return (StatusCode::BAD_REQUEST, "No archive URL provided").into_response();
    };

    // A key the caller chose (`/{fmt}/create/{key}`) may name a session
    // that already exists, and replacing it points every later
    // `/{fmt}/stream/{key}/...` at a different archive: the player that was
    // reading one file seeks and reads another's bytes. A repeat of the
    // same `/create` is the ordinary case (a re-play sends it again) and
    // still lands; one that names a different archive is refused, and the
    // caller can have a key of its own from `/{fmt}/create`.
    if let Some(existing) = state.archive_cache.get(&key)
        && existing.source.origin() != url.as_str()
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

    let cache_config = archive_cache_config(&state).await;
    let source = match resolve_source(&state, url, cache_config).await {
        Ok(source) => source,
        Err(status) => return (status, "Failed to resolve archive URL").into_response(),
    };

    #[cfg(not(feature = "rar"))]
    if is_unsupported_rar(source.path()) {
        return rar_disabled_response();
    }

    // A failure from here on drops `source`, and with it a download nothing
    // else holds -- the file goes, rather than staying on disk with no
    // session to name it.
    let selected_file = match select_archive_file(&source, &payload).await {
        Ok(file) => file,
        Err(status) => return (status, "Failed to select archive file").into_response(),
    };

    state.archive_cache.insert(
        key.clone(),
        ArchiveSession {
            source,
            selected_file: selected_file.clone(),
        },
    );

    if method == Method::GET
        && let Some(file) = selected_file
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
) -> Response {
    let selected = if format.translator().is_some() {
        state
            .translated_archives
            .get(&key)
            .and_then(|session| session.selected().map(|member| member.name.clone()))
    } else {
        state
            .archive_cache
            .get(&key)
            .and_then(|session| session.selected_file.clone())
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

/// One member of one session, as a range of bytes: the translated path for
/// the formats that have one, the old extracting path for the two that do
/// not yet.
async fn stream_member(
    state: &AppState,
    format: Format,
    key: &str,
    file: Option<&str>,
    headers: &header::HeaderMap,
) -> Response {
    if let Some(translator) = format.translator() {
        return stream_translated(state, translator.as_ref(), key, file, headers).await;
    }
    // The old path names its member in the URL, or takes the one the
    // create chose.
    let file = match file {
        Some(file) => file.to_string(),
        None => {
            let Some(session) = state.archive_cache.get(key) else {
                return StatusCode::NOT_FOUND.into_response();
            };
            let Some(file) = session.selected_file.clone() else {
                return StatusCode::NOT_FOUND.into_response();
            };
            file
        }
    };
    match stream_file(state, key, &file, headers).await {
        Ok(response) => response,
        Err(status) => status.into_response(),
    }
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
    let source = TorrentFileSource::open(state.engine.clone(), info_hash, path)
        .await
        .map_err(|error| {
            tracing::warn!(%info_hash, archive = path, %error, "no such archive in that torrent");
            Box::new(StatusCode::NOT_FOUND.into_response())
        })?;
    let sources: Vec<Arc<dyn ByteSource>> = vec![Arc::new(source)];
    let index = translator.index(&sources).await.map_err(|refusal| {
        tracing::warn!(%info_hash, archive = path, %refusal, "the archive could not be indexed");
        Box::new(refusal_response(&refusal))
    })?;
    // The sources are **not** kept: see `SessionSources::Torrent`.
    state.translated_archives.insert(
        key.to_string(),
        TranslatedSession::new(
            key,
            SessionSources::Torrent {
                info_hash: info_hash.to_string(),
                path: path.to_string(),
            },
            index,
            None,
        ),
    );
    state
        .translated_archives
        .get(key)
        .ok_or_else(|| Box::new(StatusCode::INTERNAL_SERVER_ERROR.into_response()))
}

/// The sources a body of this session reads through -- the ones it holds,
/// or a torrent file opened for this read alone.
async fn sources_for(
    state: &AppState,
    session: &TranslatedSession,
) -> Result<Vec<Arc<dyn ByteSource>>, Box<Response>> {
    match session.sources() {
        SessionSources::Held(sources) => Ok(sources.clone()),
        SessionSources::Torrent { info_hash, path } => {
            let source = TorrentFileSource::open(state.engine.clone(), info_hash, path)
                .await
                .map_err(|error| {
                    tracing::warn!(%info_hash, archive = %path, %error, "the torrent this archive is in is gone");
                    Box::new(StatusCode::NOT_FOUND.into_response())
                })?;
            Ok(vec![Arc::new(source)])
        }
    }
}

fn encode_path_segments(path: &str) -> String {
    path.split('/')
        .map(|segment| urlencoding::encode(segment).into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// Whether `error` is the volume's free-space floor refusing an
/// extraction ([`crate::archives::cache::VolumeRoom`]).
fn is_storage_full(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::StorageFull)
    })
}

/// A member of a downloaded archive, extracted if the format needs it:
/// **the old path**, and only for RAR and 7z. See [`create_downloaded`].
async fn stream_file(
    state: &AppState,
    key: &str,
    file_path_in_archive: &str,
    headers: &header::HeaderMap,
) -> Result<Response, StatusCode> {
    // The `torrent:` form belongs to the translated path now. It never
    // worked for these two formats anyway -- a RAR handler reads a
    // `std::fs::File` and 7z's decoder wants a seekable one -- so this is
    // the same refusal under the same status, said before the torrent is
    // looked at rather than after a stream has been registered on it.
    if let Some(rest) = key.strip_prefix("torrent:") {
        let extension = rest
            .rsplit_once('.')
            .map(|(_, extension)| extension)
            .unwrap_or("");
        return Ok((
            StatusCode::NOT_IMPLEMENTED,
            format!("Archives of type .{extension} cannot be read from inside a torrent"),
        )
            .into_response());
    }

    // The session this request reads from, leased for as long as the
    // response body lives (see `archives::sessions`).
    let session = state.archive_cache.get(key).ok_or(StatusCode::NOT_FOUND)?;

    #[cfg(not(feature = "rar"))]
    if is_unsupported_rar(session.source.path()) {
        return Ok(rar_disabled_response());
    }

    // One extraction per member per archive, whatever the number of
    // requests on it (see `ArchiveSource::open_member`).
    let mut reader = session
        .source
        .open_member(file_path_in_archive)
        .await
        .map_err(|e| {
            tracing::warn!(
                archive = %session.source.path().display(),
                member = file_path_in_archive,
                error = %e,
                "archive member could not be opened"
            );
            // A member that will not fit above the volume's free-space
            // floor is not a missing one: the extraction is refused, and
            // `507` says which of the two it was (the download half of
            // this route answers the same status for the same reason).
            if is_storage_full(&e) {
                StatusCode::INSUFFICIENT_STORAGE
            } else {
                StatusCode::NOT_FOUND
            }
        })?;
    let session_in_use = Some(session);

    // The same framing as every other media response (`util::MediaRange`).
    let size = reader
        .seek(tokio::io::SeekFrom::End(0))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let range = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok());
    let Some(framing) = MediaRange::of(range, size) else {
        return Ok(util::range_not_satisfiable(size));
    };
    reader
        .seek(tokio::io::SeekFrom::Start(framing.start))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let limited_reader = reader.take(framing.content_length(size));

    // The session lease rides inside the body: the session is in use for
    // as long as the player reads, and its idle clock starts when the body
    // is dropped.
    let body = Body::from_stream(media_body(limited_reader).map(move |chunk| {
        let _in_use = &session_in_use;
        chunk
    }));

    let mut res_headers = header::HeaderMap::new();
    res_headers.insert(
        header::CONTENT_TYPE,
        mime_guess::from_path(file_path_in_archive)
            .first_or_octet_stream()
            .as_ref()
            .parse()
            .unwrap(),
    );
    framing.write_headers(size, &mut res_headers);
    compat::add_dlna_headers(&mut res_headers);
    Ok((framing.status(), res_headers, body).into_response())
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

    /// How an [`origin`] answers.
    #[derive(Clone, Copy)]
    enum Answer {
        /// The whole body, with its length stated.
        Stated,
        /// The whole body, delimited by the close alone.
        Unstated,
        /// A stated length, and then nothing: the body never comes.
        StatedThenStall,
    }

    /// An origin that answers every request with `body_len` bytes -- a
    /// zip's signature and then filler -- as `answer` says.
    fn origin(body_len: usize, answer: Answer) -> String {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                std::thread::spawn(move || {
                    // The whole request head, so nothing unread is left on
                    // the socket to turn the close into a reset.
                    let mut request = Vec::new();
                    let mut byte = [0u8; 1];
                    while !request.ends_with(b"\r\n\r\n") {
                        match stream.read(&mut byte) {
                            Ok(1) => request.push(byte[0]),
                            _ => return,
                        }
                    }
                    let length = match answer {
                        Answer::Unstated => String::new(),
                        _ => format!("Content-Length: {body_len}\r\n"),
                    };
                    let head = format!("HTTP/1.1 200 OK\r\n{length}Connection: close\r\n\r\n");
                    let _ = stream.write_all(head.as_bytes());
                    if let Answer::StatedThenStall = answer {
                        std::thread::sleep(std::time::Duration::from_secs(120));
                        return;
                    }
                    let mut body = b"PK\x03\x04".to_vec();
                    body.resize(body_len, 0);
                    let _ = stream.write_all(&body);
                });
            }
        });
        format!("http://{addr}/archive.zip")
    }

    fn scratch(root: &std::path::Path) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(root.join(archives::SCRATCH_DIR_NAME))
            .map(|entries| entries.map(|entry| entry.unwrap().path()).collect())
            .unwrap_or_default()
    }

    /// Room above the free-space floor for `bytes`, whatever the volume.
    fn room_for(bytes: u64) -> impl Fn(&std::path::Path) -> Option<u64> + Clone + Send + 'static {
        move |_: &std::path::Path| Some(crate::cache_budget::CACHE_FREE_SPACE_FLOOR + bytes)
    }

    /// The `507` an extraction refused for want of room answers with is
    /// decided by this (review #18): a member that will not fit is not a
    /// missing one.
    #[test]
    fn a_storage_full_extraction_is_told_apart_from_a_missing_member() {
        let full: anyhow::Error = std::io::Error::new(
            std::io::ErrorKind::StorageFull,
            "the extraction needs more than the volume has above its floor",
        )
        .into();
        assert!(is_storage_full(&full.context("opening the member")));
        let missing: anyhow::Error =
            std::io::Error::new(std::io::ErrorKind::NotFound, "no such member").into();
        assert!(!is_storage_full(&missing));
        assert!(!is_storage_full(&anyhow::anyhow!("not an io error at all")));
    }

    fn config(root: &std::path::Path) -> CacheConfig {
        CacheConfig {
            cache_dir: root.to_path_buf(),
            _cache_size: 0,
        }
    }

    /// A download whose stated length the volume has no room for above the
    /// floor is refused with 507 before its body is read -- this origin
    /// never sends one.
    #[tokio::test]
    async fn a_stated_length_with_no_room_is_refused_before_the_body() {
        let root = tempfile::tempdir().unwrap();
        let url = origin(1024 * 1024, Answer::StatedThenStall);
        let refused = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            download_archive(&url, config(root.path()), room_for(512 * 1024)),
        )
        .await
        .expect("the refusal waited for a body");
        assert_eq!(refused.err(), Some(StatusCode::INSUFFICIENT_STORAGE));
        assert!(scratch(root.path()).is_empty());
    }

    /// A body that says nothing of its length is refused at the byte that
    /// would take the volume under the floor, and what it had written goes
    /// with the refusal; with room it is stored.
    #[tokio::test]
    async fn a_body_that_would_cross_the_floor_is_refused_and_leaves_nothing() {
        let root = tempfile::tempdir().unwrap();
        let url = origin(1024 * 1024, Answer::Unstated);
        let refused = download_archive(&url, config(root.path()), room_for(512 * 1024)).await;
        assert_eq!(refused.err(), Some(StatusCode::INSUFFICIENT_STORAGE));
        assert!(
            scratch(root.path()).is_empty(),
            "{:?}",
            scratch(root.path())
        );

        for answer in [Answer::Unstated, Answer::Stated] {
            let url = origin(1024 * 1024, answer);
            let stored =
                download_archive(&url, config(root.path()), room_for(2 * 1024 * 1024)).await;
            assert!(stored.is_ok());
        }
    }

    #[test]
    fn the_file_name_is_the_last_path_segment_without_the_query() {
        assert_eq!(
            url_file_name("http://h/dl/Show.S01.rar?token=abc&dl=1"),
            "Show.S01.rar"
        );
        assert_eq!(url_file_name("http://h/dl/a%20b.zip"), "a b.zip");
        assert_eq!(url_file_name("http://h/download?id=1"), "download");
        assert_eq!(url_file_name("not a url"), "not a url");
    }
}
