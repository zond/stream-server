//! **The container sniff in `resolve`** (`docs/design/media-pipeline.md`
//! §2.9): a plain file whose first bytes carry a container's signature is
//! answered as the member a container URL would have named.
//!
//! What the head is asked is `enginefs::retention::sniff::containers` --
//! the same signatures that decide a torrent file's play session shares
//! nothing -- and what it names is only which readers to try: each
//! translator's `index` verifies its own format (7z's signature header, a
//! RAR's through `unrar`, a ZIP's end record at the tail, a TAR header's
//! checksum, a disc image's descriptors), and the first that indexes wins.
//! A signature nothing indexes is that translator's refusal, never a film.
//!
//! **The head read is bounded** ([`SNIFF_BOUND`]): a torrent whose first
//! pieces are not here yet, a link whose origin is slow, answer the plain
//! file unsniffed after it, so a playable file is never a wait or a
//! refusal because of the sniff; [`crate::media::Resolved::sniffed`] tells
//! the app the server did not get to look.

use crate::routes::archive::{self, Format};
use crate::sources::ByteSource;
use crate::sources::drive::DriveSource;
use crate::sources::held::HeldSource;
use crate::sources::local::LocalSource;
use crate::sources::{ProxySource, TorrentSource};
use crate::state::AppState;
use crate::translators::Index;
use crate::translators::session::{Lease, SessionSources, TranslatedSession};
use enginefs::retention::sniff::{self, Container, HEAD_BYTES};
use std::sync::Arc;
use std::time::Duration;

use super::Refusal;

/// How long `resolve` waits for a file's head before it answers the plain
/// file unsniffed: the stream route's own patience with a disk or an
/// origin that has stopped answering (`SLACK_DROP_BOUND`), so the sniff
/// never holds a player longer than a stalled start would. A head that is
/// held answers at once; this is spent only on one being fetched.
pub(crate) const SNIFF_BOUND: Duration = Duration::from_secs(10);

/// The readers `head` names, in the order they are tried: 7z, RAR, ZIP,
/// TAR, ISO/UDF. Empty for a film.
pub(super) fn formats(head: &[u8]) -> Vec<Format> {
    sniff::containers(head)
        .into_iter()
        .map(|container| match container {
            Container::SevenZ => Format::SevenZ,
            Container::Rar => Format::Rar,
            Container::Zip => Format::Zip,
            Container::Tar => Format::Tar,
            Container::DiscImage => Format::Iso,
        })
        .collect()
}

/// `source`'s first [`HEAD_BYTES`] (all of it, for a shorter file), or
/// `None` if they could not be read within `bound`.
pub(super) async fn head_of(source: &dyn ByteSource, bound: Duration) -> Option<Vec<u8>> {
    let read = async {
        let want = source.len().min(HEAD_BYTES) as usize;
        let mut head = vec![0u8; want];
        let mut filled = 0;
        while filled < want {
            match source.read_at(filled as u64, &mut head[filled..]).await {
                Ok(0) => break,
                Ok(read) => filled += read,
                Err(error) => {
                    tracing::debug!(
                        source = %source.describe(),
                        %error,
                        "the head could not be read for the container sniff"
                    );
                    return None;
                }
            }
        }
        head.truncate(filled);
        Some(head)
    };
    tokio::time::timeout(bound, read).await.ok().flatten()
}

/// File `file_idx` of a torrent's head, read through an aside -- which
/// registers a stream, so a stopped torrent is started for it, and reads
/// the store's held pieces or asks the swarm for the rest -- within
/// `bound`.
pub(super) async fn torrent_head(
    state: &AppState,
    info_hash: &str,
    file_idx: usize,
    bound: Duration,
) -> Option<Vec<u8>> {
    let read = async {
        let source = TorrentSource::aside(state.engine.clone(), info_hash, file_idx)
            .await
            .ok()?;
        head_of(&source, bound).await
    };
    let head = tokio::time::timeout(bound, read).await.ok().flatten();
    if head.is_none() {
        tracing::info!(
            info_hash,
            file_idx,
            bound_secs = bound.as_secs_f32(),
            "the file's head is not here yet; answering it unsniffed"
        );
    }
    head
}

/// A container found in a file that is not a torrent's, kept with the id's
/// answer so its session can be made again once the archive map has let
/// it go: there is no create to run again and no `torrent:` key to index.
#[derive(Clone)]
pub(crate) enum Sniffed {
    /// A link, read through the proxy cache with the `/proxy` id's `h=`.
    Link(Arc<ProxySource>),
    /// A Google Drive file, under its renewing grant.
    Drive(Arc<DriveSource>),
    /// A link or Drive file downloaded whole, read off the disk.
    Held(Arc<HeldSource>),
    /// A file on this device.
    Local(Arc<LocalSource>),
}

impl Sniffed {
    /// The sources a session over this container holds: a link and a Drive
    /// file as `/create`'s are held, so a play reads ahead of them; a file
    /// already on this device as itself.
    pub(super) fn session_sources(&self) -> SessionSources {
        match self {
            Self::Link(source) => SessionSources::Held(vec![source.clone()]),
            Self::Drive(source) => SessionSources::Held(vec![Arc::new(source.proxy_source())]),
            Self::Held(source) => SessionSources::Kept(vec![source.clone()]),
            Self::Local(source) => SessionSources::Kept(vec![source.clone()]),
        }
    }

    /// The file itself, as its head is read.
    pub(super) fn source(&self) -> Arc<dyn ByteSource> {
        match self {
            Self::Link(source) => source.clone(),
            Self::Drive(source) => source.clone(),
            Self::Held(source) => source.clone(),
            Self::Local(source) => source.clone(),
        }
    }

    /// The one volume, as a translator reads it.
    fn volumes(sources: &SessionSources) -> Vec<Arc<dyn ByteSource>> {
        match sources {
            SessionSources::Held(sources) => sources
                .iter()
                .map(|source| source.clone() as Arc<dyn ByteSource>)
                .collect(),
            SessionSources::Kept(sources) => sources.clone(),
            SessionSources::Torrent { .. } => Vec::new(),
        }
    }

    /// The container indexed by the first of `formats` whose translator
    /// takes it, as a session under a key of its own -- or the first
    /// refusal, when none does.
    pub(super) async fn index(
        &self,
        state: &AppState,
        formats: &[Format],
    ) -> Result<(Format, String, Lease<TranslatedSession>), Refusal> {
        let sources = self.session_sources();
        let (format, index) = first_index(formats, &Self::volumes(&sources)).await?;
        let key = uuid::Uuid::new_v4().to_string();
        let session = state.translated_archives.insert(
            key.clone(),
            TranslatedSession::new(origin_of(&key), sources, index, None),
        );
        Ok((format, key, session))
    }

    /// The session under `key` made again, by `format`'s translator over
    /// this container: what [`Self::index`] made, after the archive map
    /// let it go.
    pub(super) async fn index_again(
        &self,
        state: &AppState,
        format: Format,
        key: &str,
    ) -> Result<Lease<TranslatedSession>, Refusal> {
        let sources = self.session_sources();
        let (_, index) = first_index(&[format], &Self::volumes(&sources)).await?;
        Ok(state.translated_archives.get_or_insert_with(key, || {
            TranslatedSession::new(origin_of(key), sources, index, None)
        }))
    }

    /// Whether this answer has gone bad, as the plain file's would have: a
    /// Drive grant found dead, a download no longer pinned.
    pub(super) fn is_stale(&self, state: &AppState) -> bool {
        match self {
            Self::Drive(source) => source.needs_pairing_again(),
            Self::Held(source) => !state.proxy_cache.retention().is_pinned(source.key_dir()),
            Self::Link(_) | Self::Local(_) => false,
        }
    }
}

/// A sniffed session's origin: never a URL, so a `/create` naming one
/// never finds it (`routes::archive::create_session` reuses by origin).
fn origin_of(key: &str) -> String {
    format!("sniffed:{key}")
}

/// `sources` indexed by the first of `formats` whose translator takes them.
/// When none does, the first one's refusal: a build with no reader for it
/// (`noReader`), or what its translator said.
pub(super) async fn first_index(
    formats: &[Format],
    sources: &[Arc<dyn ByteSource>],
) -> Result<(Format, Index), Refusal> {
    let mut first_refusal = None;
    for format in formats {
        let refusal = match archive::translator_for(*format) {
            Ok(translator) => match translator.index(sources).await {
                Ok(index) => return Ok((*format, index)),
                Err(refusal) => Refusal::Translated(refusal),
            },
            Err(error) => Refusal::of_session(error),
        };
        first_refusal.get_or_insert(refusal);
    }
    Err(first_refusal.unwrap_or_else(|| {
        Refusal::Translated(crate::translators::Refusal::Malformed(
            "no container signature to read".to_string(),
        ))
    }))
}

/// The session over a torrent's file `path` by the first of `formats`
/// whose translator indexes it, made as the `torrent:` form makes one
/// (`routes::archive::session_for`): by the file's name, so a RAR's
/// sibling volumes are found by the naming rules. The format and the
/// session's key with it; the first refusal when none indexes it.
pub(super) async fn torrent_session(
    state: &AppState,
    info_hash: &str,
    path: &str,
    formats: &[Format],
) -> Result<(Format, String, Lease<TranslatedSession>), Refusal> {
    let key = format!("torrent:{}/{path}", info_hash.to_lowercase());
    let mut first_refusal = None;
    for format in formats {
        let made = match archive::translator_for(*format) {
            Ok(translator) => archive::session_for(state, translator.as_ref(), &key).await,
            Err(error) => Err(error),
        };
        match made {
            Ok(session) => return Ok((*format, key, session)),
            Err(error) => {
                first_refusal.get_or_insert(Refusal::of_session(error));
            }
        }
    }
    Err(first_refusal.unwrap_or(Refusal::NoSuchFile(
        "the torrent's file carries no container signature".to_string(),
    )))
}
