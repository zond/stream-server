//! **Offline downloads of what is not a torrent**: an addon URL, a Google
//! Drive file -- anything that reads through the proxy cache.
//!
//! The principle is `docs/translated-sources.md`'s: only the thing that
//! downloads a file may store it, and for everything that is not a torrent
//! that is the proxy cache. A download of one of them is therefore not a new
//! store but two things the torrent side has and the proxy side did not
//! (`docs/generic-downloads.md`):
//!
//! * **A pin** -- a claim on a key directory that outlives the process. The
//!   embedder names the set at boot ([`crate::ServerConfig::proxy_pins`],
//!   the piece store's rule that `None` is "nobody told me" and not an empty
//!   set), the launch sweep keeps what it names, and the retention owner
//!   answers `keeps_everything` from it
//!   ([`crate::proxy_retention::ProxyRetention::set_pins`]).
//! * **A filler** -- something that fetches the bytes when no player is
//!   asking for them ([`Filler`]). It is a quiet reader over the same
//!   `ProxySource` a player would read through, walked from the first byte
//!   to the last: a range the disk holds is served from the disk and a hole
//!   is one narrowed, conditional fetch that lands in the cache, which is
//!   exactly what `/proxy` does for a player and is why nothing here writes
//!   a chunk itself. Quiet, because a reader that claimed the live entity
//!   would tell the reconciler the viewer had moved on.
//!
//! What a pin names is the identity the cache already keys on: the final
//! target URL with the `h=` request headers for an addon link, the vouched
//! media URL for a Drive file. Credentials are not in it -- a Drive pin
//! carries a file id, never a token -- so a Drive download resumes only once
//! the app has re-pinned it with a fresh pairing, which the app does for
//! every unfinished download at boot. The bytes are kept meanwhile.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use tokio::io::AsyncReadExt;
use url::Url;

use crate::proxy_cache::Entry;
use crate::sources::{ByteSource, ProxySource, ReadHint};

/// A pinned proxy download as the embedder names it, and persists it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ProxyPinKey {
    /// An addon link: the URL stremio-core puts in `d=` plus the path, and
    /// the `h=` request headers, which is the `/proxy` cache key.
    #[serde(rename_all = "camelCase")]
    Url {
        target: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
    /// A Google Drive file, by id. The vouched cache key is derived from the
    /// media URL the configured Drive endpoint makes of it.
    #[serde(rename_all = "camelCase")]
    Drive { file_id: String },
}

impl ProxyPinKey {
    /// A short name for a listing when the pin carries none: the target's
    /// last path segment, or the file id.
    pub fn default_name(&self) -> String {
        match self {
            Self::Url { target, .. } => Url::parse(target)
                .ok()
                .and_then(|url| {
                    url.path_segments()
                        .and_then(|mut segments| segments.next_back().map(str::to_string))
                })
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| target.clone()),
            Self::Drive { file_id } => file_id.clone(),
        }
    }

    /// The cache entry this pin names, derived exactly as the routes derive
    /// it, so a download and a play of the same stream share chunks.
    /// `None` when the key cannot be made: a URL that does not parse, one
    /// that names this server, or a Drive pin with no Drive endpoint
    /// configured.
    pub(crate) fn entry(&self, state: &crate::AppState) -> Option<Entry> {
        match self {
            Self::Url { target, headers } => state.proxy_cache.entry(
                &axum::http::Method::GET,
                &Url::parse(target).ok()?,
                headers,
                &axum::http::HeaderMap::new(),
                state.http_addr,
            ),
            Self::Drive { file_id } => {
                let endpoints = state.drive.as_ref()?;
                let media = endpoints.pairing(file_id, "").media_url().ok()?;
                state
                    .proxy_cache
                    .entry_for_vouched_url(&media, state.http_addr)
            }
        }
    }
}

/// Why a proxy download could not be pinned.
#[derive(Debug)]
pub enum ProxyPinError {
    /// The origin refused, will not range, or could not be reached.
    Source(crate::sources::proxy::ProxySourceError),
    /// The Drive file could not be opened (no pairing service, pair again,
    /// or its source's own refusal).
    Drive(crate::routes::drive::DriveOpenError),
    /// Nothing the cache can key: a URL that does not parse or names this
    /// server, a Drive pin with no Drive endpoint.
    Unkeyable(&'static str),
}

impl std::fmt::Display for ProxyPinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(error) => write!(f, "{error}"),
            Self::Drive(error) => write!(f, "{error}"),
            Self::Unkeyable(why) => write!(f, "this link cannot be a download: {why}"),
        }
    }
}

impl std::error::Error for ProxyPinError {}

impl From<crate::sources::proxy::ProxySourceError> for ProxyPinError {
    fn from(error: crate::sources::proxy::ProxySourceError) -> Self {
        Self::Source(error)
    }
}

impl From<crate::routes::drive::DriveOpenError> for ProxyPinError {
    fn from(error: crate::routes::drive::DriveOpenError) -> Self {
        Self::Drive(error)
    }
}

/// The bytes moved per ranged request the filler makes. Small enough that a
/// stall shows within one `download progress` line, large enough that a
/// film is not thousands of requests.
const FILL_STRIDE: u64 = 32 * 1024 * 1024;
/// How long a filler waits after a read failed before it asks again.
const FILL_RETRY: Duration = Duration::from_secs(15);

/// One pinned proxy download the process knows more about than its key:
/// what it is called, and the filler running for it, if one is.
struct Pinned {
    key: ProxyPinKey,
    name: String,
    filler: Option<tokio::task::AbortHandle>,
}

/// The pinned proxy downloads, by key directory.
#[derive(Default)]
pub struct ProxyDownloads {
    pinned: Mutex<HashMap<PathBuf, Pinned>>,
}

impl ProxyDownloads {
    fn table(&self) -> std::sync::MutexGuard<'_, HashMap<PathBuf, Pinned>> {
        self.pinned
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Registers the boot-time pin set: the keys the embedder named, resolved
    /// to key directories. Pins the resolved set on the retention owner
    /// (`None` stays `None`), and answers the directories for the sweep to
    /// keep. No filler starts here: a URL pin's filler starts when the
    /// embedder pins it again with its request, a Drive pin's when it comes
    /// with a pairing.
    pub(crate) fn install(
        &self,
        state: &crate::AppState,
        keys: Option<&[ProxyPinKey]>,
    ) -> Option<HashSet<PathBuf>> {
        let keys = keys?;
        let mut dirs = HashSet::new();
        let mut pinned = self.table();
        for key in keys {
            let Some(entry) = key.entry(state) else {
                tracing::warn!(
                    ?key,
                    "a proxy pin names nothing this server can key; it is dropped"
                );
                continue;
            };
            let dir = entry.dir().to_path_buf();
            dirs.insert(dir.clone());
            pinned.entry(dir).or_insert_with(|| Pinned {
                key: key.clone(),
                name: key.default_name(),
                filler: None,
            });
        }
        state.proxy_cache.retention().set_pins(Some(dirs.clone()));
        Some(dirs)
    }

    /// Pins `key` and starts (or keeps) its filler over `source`. Answers the
    /// key directory. Idempotent: a second pin of a running download changes
    /// nothing but the name.
    fn pin(
        &self,
        state: &crate::AppState,
        key: ProxyPinKey,
        name: Option<String>,
        entry: Entry,
        source: Arc<ProxySource>,
    ) -> PathBuf {
        let dir = entry.dir().to_path_buf();
        state.proxy_cache.retention().pin(dir.clone());
        let mut pinned = self.table();
        let record = pinned.entry(dir.clone()).or_insert_with(|| Pinned {
            name: name.clone().unwrap_or_else(|| key.default_name()),
            key: key.clone(),
            filler: None,
        });
        if let Some(name) = name {
            record.name = name;
        }
        let running = record
            .filler
            .as_ref()
            .is_some_and(|filler| !filler.is_finished());
        if !running {
            let filler = Filler {
                entry: entry.quiet(),
                source,
                retention: state.proxy_cache.retention().clone(),
                dir: dir.clone(),
            };
            record.filler = Some(tokio::spawn(filler.run()).abort_handle());
        }
        dir
    }

    /// Drops the pin on `dir`, aborts its filler, and with `delete_files`
    /// removes everything under the key. Answers `(was pinned, bytes freed)`.
    pub(crate) fn unpin(
        &self,
        state: &crate::AppState,
        dir: &PathBuf,
        delete_files: bool,
    ) -> (bool, u64) {
        let record = self.table().remove(dir);
        if let Some(filler) = record.as_ref().and_then(|record| record.filler.as_ref()) {
            filler.abort();
        }
        let was_pinned = state.proxy_cache.retention().unpin(dir) || record.is_some();
        let freed = if delete_files {
            state
                .proxy_cache
                .entry_for_key_dir(dir.clone(), Arc::from(""))
                .map(|entry| entry.remove_all())
                .unwrap_or(0)
        } else {
            0
        };
        (was_pinned, freed)
    }

    /// Every pinned proxy download as a listing sees it: `(key directory,
    /// key, name, whether a filler is running)`.
    pub(crate) fn snapshot(&self) -> Vec<(PathBuf, ProxyPinKey, String, bool)> {
        self.table()
            .iter()
            .map(|(dir, pinned)| {
                (
                    dir.clone(),
                    pinned.key.clone(),
                    pinned.name.clone(),
                    pinned
                        .filler
                        .as_ref()
                        .is_some_and(|filler| !filler.is_finished()),
                )
            })
            .collect()
    }
}

/// Pins an addon URL as a download. Probes the origin the way a translated
/// source does (`ProxySource::open`: one `bytes=0-0`, which is where an
/// origin that will not range is refused), pins the key, starts the filler.
pub(crate) async fn pin_url(
    state: &crate::AppState,
    target: &str,
    headers: BTreeMap<String, String>,
    name: Option<String>,
) -> Result<PathBuf, ProxyPinError> {
    let url = Url::parse(target).map_err(|_| ProxyPinError::Unkeyable("the URL does not parse"))?;
    let key = ProxyPinKey::Url {
        target: url.as_str().to_string(),
        headers: headers.clone(),
    };
    let entry = key.entry(state).ok_or(ProxyPinError::Unkeyable(
        "the URL names this server, or carries a credential the cache will not key",
    ))?;
    let source =
        ProxySource::open(state.proxy_cache.clone(), state.http_addr, url, headers).await?;
    let source = Arc::new(source.for_filling());
    Ok(state.proxy_downloads.pin(state, key, name, entry, source))
}

/// Pins a Google Drive file as a download, under the pairing the app
/// holds: the token renews in Rust for the length of the fill, as it does
/// for a stream.
pub(crate) async fn pin_drive(
    state: &crate::AppState,
    file_id: &str,
    refresh_token: &str,
    name: Option<String>,
) -> Result<PathBuf, ProxyPinError> {
    let endpoints = state
        .drive
        .as_ref()
        .ok_or(crate::routes::drive::DriveOpenError::NoPairingService)?;
    let pairing = endpoints.pairing(file_id, refresh_token);
    let key = ProxyPinKey::Drive {
        file_id: file_id.to_string(),
    };
    let entry = key.entry(state).ok_or(ProxyPinError::Unkeyable(
        "the Drive file's media URL names this server",
    ))?;
    let source = crate::sources::drive::DriveSource::open(state, pairing)
        .await
        .map_err(crate::routes::drive::DriveOpenError::Drive)?;
    let name = name.or_else(|| source.name().map(str::to_string));
    let source = Arc::new(source.filling_source());
    Ok(state.proxy_downloads.pin(state, key, name, entry, source))
}

/// The task that fetches a pinned download's holes until it is whole.
struct Filler {
    entry: Entry,
    source: Arc<ProxySource>,
    retention: Arc<crate::proxy_retention::ProxyRetention>,
    dir: PathBuf,
}

impl Filler {
    async fn run(self) {
        let total = self.source.len();
        let mut pos = 0u64;
        let mut sink = vec![0u8; 256 * 1024];
        loop {
            if !self.retention.is_pinned(&self.dir) {
                tracing::info!(dir = %self.dir.display(), "download filler: the pin is gone; stopping");
                return;
            }
            if pos >= total {
                break;
            }
            let until = (pos + FILL_STRIDE).min(total);
            match self.fill(pos, until, &mut sink).await {
                Ok(read) => pos += read,
                Err(error) => {
                    tracing::warn!(
                        dir = %self.dir.display(),
                        origin = %ByteSource::describe(&*self.source),
                        offset = pos,
                        %error,
                        "download filler: a read failed; asking again shortly"
                    );
                    tokio::time::sleep(FILL_RETRY).await;
                }
            }
        }
        let whole = tokio::task::spawn_blocking({
            let entry = self.entry.clone();
            move || entry.held_facts().is_some_and(|facts| facts.complete)
        })
        .await
        .unwrap_or(false);
        tracing::info!(
            dir = %self.dir.display(),
            origin = %ByteSource::describe(&*self.source),
            length = total,
            whole,
            "download filler: reached the end"
        );
    }

    /// One stride: open the quiet source at `from` and read to `until`. A
    /// range the disk holds is served from the disk (no origin request);
    /// a hole is fetched and lands in the cache on its way through. Answers
    /// the bytes read.
    async fn fill(&self, from: u64, until: u64, sink: &mut [u8]) -> std::io::Result<u64> {
        let mut reader = self.source.open(from, ReadHint::of(until - from)).await?;
        let mut read = 0u64;
        while from + read < until {
            let want = ((until - from - read) as usize).min(sink.len());
            let n = reader.read(&mut sink[..want]).await?;
            if n == 0 {
                break;
            }
            read += n as u64;
        }
        Ok(read)
    }
}
