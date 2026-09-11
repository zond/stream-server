use crate::archives::{self, ArchiveSession, ArchiveSource, CacheConfig};
use crate::routes::compat;
use crate::routes::util::parse_range;
use crate::state::AppState;
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{Method, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
    routing::get,
};
use enginefs::backend::TorrentHandle;
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

#[derive(Debug)]
struct ArchiveCreateRequest {
    urls: Vec<String>,
    file_idx: Option<usize>,
    file_must_include: Vec<String>,
}

/// The whole archive API under one format prefix: [`session_router`] and
/// [`stream_router`] together, which is what the loopback listener mounts.
pub fn router() -> Router<AppState> {
    stream_router().merge(session_router())
}

/// The session-creating half: `/create` takes an archive by URL (fetched
/// whole, from wherever the caller names) or by torrent file, opens it and
/// remembers the choice under a key. Loopback only -- see
/// `crate::lan_media_routes` for why the LAN listener never mounts this
/// half.
pub fn session_router() -> Router<AppState> {
    Router::new()
        .route(
            "/create",
            get(create_session_auto).post(create_session_auto),
        )
        .route(
            "/create/{key}",
            get(create_session_with_key).post(create_session_with_key),
        )
}

/// The byte-serving half: a member read out of a session [`session_router`]
/// already created. Nothing here fetches, opens or names anything -- an
/// unknown key is a `404` -- which is what lets the LAN listener mount it.
pub fn stream_router() -> Router<AppState> {
    Router::new()
        .route("/stream", get(stream_content_query))
        .route("/stream/{key}", get(stream_redirection))
        .route("/stream/{key}/{*file}", get(stream_content_path))
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
/// (http/https) or the local path itself.
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
            origin = url,
            "reusing the archive an existing session holds"
        );
        return Ok(existing.source.clone());
    }
    if url.starts_with("http://") || url.starts_with("https://") {
        download_archive(url, cache_config, crate::cache_budget::available_space)
            .await
            .map(Arc::new)
    } else {
        let path = PathBuf::from(url);
        if !path.exists() {
            // It might be a valid local path for some setups, but we generally expect existence
            // For torrent relative paths, this function is used by 'create' which assumes local or http.
            return Err(StatusCode::NOT_FOUND);
        }
        Ok(Arc::new(ArchiveSource::local(
            path,
            url.to_string(),
            cache_config,
        )))
    }
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
        self.room = tokio::task::spawn_blocking(move || probe(&dir))
            .await
            .ok()
            .flatten()
            .map(|available| available.saturating_sub(crate::cache_budget::CACHE_FREE_SPACE_FLOOR));
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
    tracing::info!("Downloading archive from URL: {}", url);
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
        tracing::error!("Failed to fetch URL {}: {}", url, e);
        StatusCode::BAD_REQUEST
    })?;

    if !response.status().is_success() {
        tracing::error!("URL {} returned status {}", url, response.status());
        return Err(StatusCode::NOT_FOUND);
    }

    // The volume the cache root is on, read before a byte is stored; a
    // length the origin states that will not fit is refused before one is
    // even fetched.
    let mut room = DownloadRoom::read(probe, cache_config.cache_dir.clone()).await;
    let too_big = || {
        tracing::warn!(
            url,
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
                tracing::error!("Download stream error: {}", e);
                StatusCode::BAD_GATEWAY
            })?),
            None => break,
        }
    }

    let suffix = archives::archive_suffix(&url_file_name(url))
        .or_else(|| archives::archive_suffix_from_magic(&head))
        .ok_or_else(|| {
            tracing::warn!(
                url,
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
                    tracing::error!("Download stream error: {}", e);
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

    tracing::info!("Downloaded {} to {:?}", url, file.path());
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
    method: Method,
    Query(query): Query<CreateQuery>,
    body: axum::body::Bytes,
) -> Response {
    let key = Uuid::new_v4().to_string();
    create_session_internal(state, key, method, query, body).await
}

async fn create_session_with_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
    method: Method,
    Query(query): Query<CreateQuery>,
    body: axum::body::Bytes,
) -> Response {
    create_session_internal(state, key, method, query, body).await
}

async fn create_session_internal(
    state: AppState,
    key: String,
    method: Method,
    query: CreateQuery,
    body: axum::body::Bytes,
) -> Response {
    let payload = match parse_create_request(query.lz, &body) {
        Ok(payload) => payload,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

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
    headers: header::HeaderMap,
    Query(params): Query<StreamParams>,
) -> Result<Response, StatusCode> {
    let file = if let Some(file) = params.file {
        file
    } else {
        let session = state
            .archive_cache
            .get(&params.key)
            .ok_or(StatusCode::NOT_FOUND)?;
        session.selected_file.clone().ok_or(StatusCode::NOT_FOUND)?
    };
    stream_file(&state, &params.key, &file, &headers).await
}

async fn stream_content_path(
    State(state): State<AppState>,
    headers: header::HeaderMap,
    Path((key, file)): Path<(String, String)>,
) -> Result<Response, StatusCode> {
    stream_file(&state, &key, &file, &headers).await
}

async fn stream_redirection(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Response, StatusCode> {
    let session = state.archive_cache.get(&key).ok_or(StatusCode::NOT_FOUND)?;
    if let Some(file) = &session.selected_file {
        Ok(Redirect::temporary(&format!(
            "./{}/{}",
            urlencoding::encode(&key),
            encode_path_segments(file)
        ))
        .into_response())
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

fn encode_path_segments(path: &str) -> String {
    path.split('/')
        .map(|segment| urlencoding::encode(segment).into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// Registers a stream on a torrent for as long as the archive response
/// body reading from it lives.
///
/// The `torrent:` form of [`stream_file`] opens a file reader on a live
/// torrent, and until this existed it registered nothing at all: no
/// `on_stream_start`, no reconcile, no entry in any activity register.
/// Two things follow from that, and both are the failure the
/// `PlaybackStart` reconcile in `EngineFS::on_stream_start` was written to
/// prevent.
///
/// * The reconciler's own tick reads `playing` from those registers, so
///   with seeding off and the grace elapsed it pauses the torrent this
///   response body is streaming from -- mid-body, dropping its peers,
///   while a reader is still being served out of it.
/// * A request arriving on a torrent an earlier pass already stopped asks
///   the reconciler nothing, and `LibrqbitBackend::get_file_reader` accepts
///   a paused torrent and hands back a reader: the read then parks on
///   pieces nobody is fetching, with no end and no error.
///
/// This is the sibling of the stream route's call site, and it registers
/// the same way for the same reasons -- including the handover:
/// `on_stream_start` and the guard that ends it are one call with no await
/// between them, because a cancel can only land at an await and a
/// registration nobody holds is never ended (see
/// `crate::routes::stream::StreamLifecycleGuard::start`).
struct TorrentMemberStream {
    engine: Arc<enginefs::EngineFS>,
    info_hash: String,
    file_idx: usize,
}

impl TorrentMemberStream {
    async fn start(engine: Arc<enginefs::EngineFS>, info_hash: String, file_idx: usize) -> Self {
        engine.on_stream_start(&info_hash, file_idx).await;
        Self {
            engine,
            info_hash,
            file_idx,
        }
    }
}

impl Drop for TorrentMemberStream {
    fn drop(&mut self) {
        let engine = self.engine.clone();
        let info_hash = std::mem::take(&mut self.info_hash);
        let file_idx = self.file_idx;
        // Spawned because the registers are behind async locks and a
        // `Drop` cannot await one.
        tokio::spawn(async move {
            engine.on_stream_end(&info_hash, file_idx).await;
            tracing::debug!(
                info_hash = %info_hash,
                file_idx,
                "archive member stream ended"
            );
        });
    }
}

// New implementation of stream_file
async fn stream_file(
    state: &AppState,
    key: &str,
    file_path_in_archive: &str,
    headers: &header::HeaderMap,
) -> Result<Response, StatusCode> {
    let cache_config = archive_cache_config(state).await;
    // The session this request reads from, leased for as long as the
    // response body lives (see `archives::sessions`); `None` for the
    // torrent-backed form, which has no session.
    let mut session_in_use = None;
    // And the other half of the same idea for the torrent-backed form,
    // which has a torrent instead of a session: the stream registration
    // that keeps the reconciler from pausing the torrent this body reads
    // from. `None` for the session form, which reads from a file.
    let mut torrent_stream_in_use = None;

    // 1. Determine Input Source, and open the member in it
    let mut reader: Box<dyn crate::archives::AsyncSeekableReader> = if key.starts_with("torrent:") {
        // Format: torrent:<info_hash>/path/to/archive
        let parts: Vec<&str> = key.splitn(3, '/').collect();
        if parts.len() < 2 {
            return Err(StatusCode::BAD_REQUEST);
        }
        let hash_part = parts[0].strip_prefix("torrent:").unwrap();
        // The path part inside the torrent:
        let archive_internal_path = parts.iter().skip(1).copied().collect::<Vec<_>>().join("/");
        // The reader is chosen by the archive's extension, not its whole
        // path -- and asked for before the torrent is looked at. A format
        // no stream reader exists for (7z needs a seekable file; its handler
        // refused a stream after the fact) used to go through the lookup,
        // register a stream -- which starts a torrent the reconciler had
        // stopped -- and open a reader, only to answer 404 for a member
        // that may well be there.
        let extension = archive_internal_path
            .rsplit_once('.')
            .map(|(_, extension)| extension)
            .unwrap_or("");
        if !crate::archives::streams_from_a_reader(extension) {
            return Ok((
                StatusCode::NOT_IMPLEMENTED,
                format!("Archives of type .{extension} cannot be read from inside a torrent"),
            )
                .into_response());
        }

        let engine = &state.engine;
        // EngineFS uses string info_hash
        // let sha_hash = crate::engine::SHA1::from_hex(&hash_part).map_err(|_| StatusCode::BAD_REQUEST)?;

        if let Some(engine_instance) = engine.get_engine(hash_part).await {
            // engine_instance is Arc<Engine<H>>
            // We need to find the file inside this engine.
            // Engine has `handle`.
            let handle = &engine_instance.handle;

            // The file list alone: `stats()` builds the whole stats
            // snapshot -- per-file progress, trackers, a scrape scheduled --
            // for a name lookup.
            let files = handle.get_files().await;

            // Find index
            if let Some(idx) = files.iter().position(|f| f.name == archive_internal_path) {
                // Before the reader, not after it: what the reconciler is
                // being told is that a read is about to start, and the
                // reconcile it makes is what starts a torrent an earlier
                // pass left stopped. See `TorrentMemberStream`.
                torrent_stream_in_use = Some(
                    TorrentMemberStream::start(state.engine.clone(), hash_part.to_lowercase(), idx)
                        .await,
                );
                // get_file_reader(idx, offset, priority). The intent's cap
                // alone: this path installs no retention policy, so there is
                // no window to cut the lookahead to.
                let reader = handle
                    .get_file_reader(
                        idx,
                        0,
                        7,
                        None,
                        enginefs::backend::priorities::librqbit_stream_lookahead_bytes(
                            enginefs::backend::priorities::PlaybackIntent::DirectInitial,
                            // Archive members are read whole, sequentially,
                            // from a session the viewer never gets a buffer
                            // choice for.
                            enginefs::backend::priorities::BufferProfile::Normal,
                        ),
                    )
                    .await // 7 = high priority
                    .map_err(|e| {
                        tracing::error!("Failed to get file stream: {}", e);
                        StatusCode::INTERNAL_SERVER_ERROR
                    })?;

                // Wrapper bridging enginefs::backend::FileStreamTrait (AsyncRead +
                // AsyncSeek + Unpin + Send) to this module's AsyncSeekableReader.
                struct BackendStreamWrapper(Box<dyn enginefs::backend::FileStreamTrait>);

                // Wrapper impls
                impl tokio::io::AsyncRead for BackendStreamWrapper {
                    fn poll_read(
                        mut self: std::pin::Pin<&mut Self>,
                        cx: &mut std::task::Context<'_>,
                        buf: &mut tokio::io::ReadBuf<'_>,
                    ) -> std::task::Poll<std::io::Result<()>> {
                        std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
                    }
                }
                impl tokio::io::AsyncSeek for BackendStreamWrapper {
                    fn start_seek(
                        mut self: std::pin::Pin<&mut Self>,
                        position: std::io::SeekFrom,
                    ) -> std::io::Result<()> {
                        std::pin::Pin::new(&mut self.0).start_seek(position)
                    }
                    fn poll_complete(
                        mut self: std::pin::Pin<&mut Self>,
                        cx: &mut std::task::Context<'_>,
                    ) -> std::task::Poll<std::io::Result<u64>> {
                        std::pin::Pin::new(&mut self.0).poll_complete(cx)
                    }
                }
                // If we need Sync and trait doesn't provide it, we are stuck unless we relax requirement or wrap in Mutex.
                // Mutex provides Sync. Use tokio::sync::Mutex? No, AsyncRead needs &mut.
                // std::sync::Mutex? Blocks.
                // Let's modify `AsyncSeekableReader` to NOT require Sync.

                let wrapped_reader = Box::new(BackendStreamWrapper(reader));

                // We need to ensure wrapped_reader is `AsyncSeekableReader`.
                // Ideally `ArchiveReader` accepts `Box<dyn AsyncSeekableReader>`.

                let archive_reader = crate::archives::get_archive_reader_from_stream(
                    wrapped_reader,
                    extension,
                    cache_config,
                )
                .map_err(|e| {
                    tracing::error!("Failed to create stream reader: {}", e);
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
                // No session to keep the extraction in: this form pays for
                // one per request.
                archive_reader
                    .open_file(file_path_in_archive)
                    .await
                    .map_err(|_| StatusCode::NOT_FOUND)?
                    .into_reader()
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            } else {
                return Err(StatusCode::NOT_FOUND);
            }
        } else {
            return Err(StatusCode::NOT_FOUND);
        }
    } else {
        // Local Session
        let session = state.archive_cache.get(key).ok_or(StatusCode::NOT_FOUND)?;

        #[cfg(not(feature = "rar"))]
        if is_unsupported_rar(session.source.path()) {
            return Ok(rar_disabled_response());
        }

        // One extraction per member per archive, whatever the number of
        // requests on it (see `ArchiveSource::open_member`).
        let reader = session
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
                StatusCode::NOT_FOUND
            })?;
        session_in_use = Some(session);
        reader
    };

    // 3. Determine Content Length
    let file_size = reader
        .seek(tokio::io::SeekFrom::End(0))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    reader
        .seek(tokio::io::SeekFrom::Start(0))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 4. Handle Range Requests. A `Range` this member cannot satisfy is a
    // `416` naming the length, as the torrent stream route answers it: it
    // used to be ignored, and the player that asked for bytes past the end
    // got the whole member under a `200` it had not asked for.
    let range = match headers.get(header::RANGE) {
        None => None,
        Some(value) => match value.to_str().ok().and_then(|v| parse_range(v, file_size)) {
            Some(range) => Some(range),
            None => {
                return Ok(Response::builder()
                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                    .header(header::CONTENT_RANGE, format!("bytes */{file_size}"))
                    .body(Body::empty())
                    .unwrap());
            }
        },
    };
    let is_partial = range.is_some();
    // `(start, len)`. An empty member is `(0, 0)`: the inclusive `end` it
    // used to be computed from saturated to 0 and made a length of one --
    // a `Content-Length: 1` over a body that ends at once, which a client
    // reads as a truncated response.
    let (start, len) = match range {
        Some((start, end)) => (start, end - start + 1),
        None => (0, file_size),
    };

    // Seek to start
    reader
        .seek(tokio::io::SeekFrom::Start(start))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Limit reader
    let limited_reader = reader.take(len);

    // Convert to Body stream. The session lease rides inside it: the
    // session is in use for as long as the player reads, and its idle clock
    // starts when the body is dropped.
    let body = Body::from_stream(media_body(limited_reader).map(move |chunk| {
        let _in_use = &session_in_use;
        // The same for the torrent-backed form: the stream stays
        // registered until the body is dropped, so nothing pauses the
        // torrent underneath a player that is still reading.
        let _streaming = &torrent_stream_in_use;
        chunk
    }));

    // 5. Build Response
    let mime = mime_guess::from_path(file_path_in_archive).first_or_octet_stream();
    let mut builder = Response::builder()
        .header(header::CONTENT_TYPE, mime.as_ref())
        .header(header::ACCEPT_RANGES, "bytes")
        .header("transferMode.dlna.org", compat::DLNA_TRANSFER_MODE)
        .header("contentFeatures.dlna.org", compat::DLNA_CONTENT_FEATURES)
        .header(header::CONTENT_LENGTH, len);

    if is_partial {
        builder = builder.status(StatusCode::PARTIAL_CONTENT).header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, start + len - 1, file_size),
        );
    } else {
        builder = builder.status(StatusCode::OK);
    }

    Ok(builder.body(body).unwrap())
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
