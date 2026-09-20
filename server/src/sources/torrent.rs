//! One file of a torrent as a [`ByteSource`].
//!
//! The piece store already holds these bytes, or fetches them from the
//! swarm the moment a reader asks; a seek in it is the one thing the store
//! and its retention were built to serve well. So a container inside a
//! torrent -- a film in a stored ZIP, a RAR set across three volumes -- is
//! read by asking the torrent for the byte ranges the member occupies, and
//! nothing is extracted, copied or written anywhere.
//!
//! Two things this gets right that the `torrent:` archive route did not:
//!
//! * **`Fetching::Streaming`, not `Download`.** That route opens its reader
//!   with the download intent's 256 MiB lookahead, on the reasoning that an
//!   archive member is read whole and sequentially. It is not: a player
//!   seeks in it, and a 256 MiB read-ahead in front of a seeking player is
//!   the swarm fetching a quarter of a gigabyte nobody is about to watch,
//!   and the retention owner unable to keep any of it.
//! * **The stream registration lasts as long as the source.** Every route
//!   that opens a reader on a torrent registers a stream first (AGENTS.md),
//!   because the reconciler pauses a torrent nobody is playing -- mid-body,
//!   dropping its peers -- and because a request arriving on a torrent an
//!   earlier pass stopped gets a reader that parks on pieces nobody is
//!   fetching. A source holds that registration for its whole life, so
//!   every read through it, and every index read between reads, is inside
//!   it.

use super::{ByteSource, ReadHint, SeekableReader, read_filling};
use enginefs::backend::TorrentHandle;
use enginefs::backend::priorities::{BufferProfile, Fetching};
use std::io::{self, SeekFrom};
use std::sync::Arc;
use tokio::io::AsyncSeekExt;

/// What a player's read is worth to the engine: the same priority
/// `routes::stream` gives a request that names none, which is what an
/// archive member read through a player is. 255 is the reconciler's probe
/// and 0 a background fetch; neither is this.
const PLAYBACK_PRIORITY: u8 = 1;

/// Registers a stream on a torrent for as long as whatever reads from it
/// lives: an archive response body (`routes::archive`'s `torrent:` form)
/// or a [`TorrentFileSource`].
///
/// The `torrent:` form of `routes::archive::stream_file` opens a file
/// reader on a live torrent, and until this existed it registered nothing
/// at all: no `on_stream_start`, no reconcile, no entry in any activity
/// register. Two things follow from that, and both are the failure the
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
pub(crate) struct TorrentMemberStream {
    engine: Arc<enginefs::EngineFS>,
    info_hash: String,
    file_idx: usize,
}

impl TorrentMemberStream {
    pub(crate) async fn start(
        engine: Arc<enginefs::EngineFS>,
        info_hash: String,
        file_idx: usize,
    ) -> Self {
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

/// One file of one torrent, as a source of bytes.
pub struct TorrentFileSource {
    engine: Arc<enginefs::EngineFS>,
    info_hash: String,
    file_idx: usize,
    len: u64,
    name: String,
    /// One reader kept open for the scattered reads an index is made of,
    /// seeked under the lock rather than reopened: the piece store serves
    /// a seek cheaply, and opening a reader is a `prepare_file_for_
    /// streaming` and a retention install each time.
    index_reader: tokio::sync::Mutex<Option<Box<dyn SeekableReader>>>,
    /// Ended when this source is dropped, and not before -- see
    /// [`TorrentMemberStream`].
    _stream: TorrentMemberStream,
}

impl TorrentFileSource {
    /// The file at `path_in_torrent` of the torrent `info_hash`.
    pub async fn open(
        engine: Arc<enginefs::EngineFS>,
        info_hash: &str,
        path_in_torrent: &str,
    ) -> io::Result<Self> {
        let info_hash = info_hash.to_lowercase();
        let files = Self::files(&engine, &info_hash).await?;
        let file_idx = files
            .iter()
            .position(|file| file.name == path_in_torrent)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("{path_in_torrent} is not a file of torrent {info_hash}"),
                )
            })?;
        Self::at(engine, info_hash, file_idx, &files).await
    }

    /// The file at `file_idx`, for a caller that resolved the index itself
    /// (`routes::compat::resolve_file_idx`, a sibling volume of a set).
    pub async fn open_index(
        engine: Arc<enginefs::EngineFS>,
        info_hash: &str,
        file_idx: usize,
    ) -> io::Result<Self> {
        let info_hash = info_hash.to_lowercase();
        let files = Self::files(&engine, &info_hash).await?;
        Self::at(engine, info_hash, file_idx, &files).await
    }

    /// The names of the torrent's files, in the torrent's own order --
    /// what a translator that comes in sets is handed to pick its sibling
    /// volumes out of (`Translator::volumes`). The order is the list's,
    /// not the set's: the naming rules put the volumes in order, and a
    /// torrent lists what it lists.
    pub async fn file_names(
        engine: &Arc<enginefs::EngineFS>,
        info_hash: &str,
    ) -> io::Result<Vec<String>> {
        let files = Self::files(engine, &info_hash.to_lowercase()).await?;
        Ok(files.into_iter().map(|file| file.name).collect())
    }

    /// The torrent's file list. `get_files` and not `stats()`: the latter
    /// builds the whole snapshot -- per-file progress, trackers, a scrape
    /// scheduled -- for a name lookup.
    async fn files(
        engine: &Arc<enginefs::EngineFS>,
        info_hash: &str,
    ) -> io::Result<Vec<enginefs::backend::BackendFileInfo>> {
        let torrent = engine.get_engine(info_hash).await.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no torrent {info_hash} in this engine"),
            )
        })?;
        Ok(torrent.handle.get_files().await)
    }

    async fn at(
        engine: Arc<enginefs::EngineFS>,
        info_hash: String,
        file_idx: usize,
        files: &[enginefs::backend::BackendFileInfo],
    ) -> io::Result<Self> {
        let file = files.get(file_idx).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "torrent {info_hash} has {} files, not {}",
                    files.len(),
                    file_idx + 1
                ),
            )
        })?;
        // Before any reader, and it has to be before: what the reconciler
        // is being told is that a read is about to start, and the
        // reconcile it makes is what starts a torrent an earlier pass left
        // stopped.
        let stream = TorrentMemberStream::start(engine.clone(), info_hash.clone(), file_idx).await;
        Ok(Self {
            engine,
            info_hash,
            file_idx,
            len: file.length,
            name: file.name.clone(),
            index_reader: tokio::sync::Mutex::new(None),
            _stream: stream,
        })
    }

    /// Which file of which torrent this is, for a caller that has to name
    /// it to the engine.
    pub fn file_idx(&self) -> usize {
        self.file_idx
    }

    /// A reader on this file at `offset`, at the streaming intent.
    ///
    /// **How far ahead the swarm is asked for is the engine's to decide**,
    /// not this call's: `try_get_file_with_intent` takes the intent and
    /// works the lookahead out from the film's measured bitrate, the
    /// viewer's buffer profile and what the retention budget can keep --
    /// which is the whole reason to go through it rather than through
    /// `get_file_reader`, whose lookahead argument installs no policy to
    /// keep what it pulls. A [`ReadHint`] is therefore spent on nothing
    /// at all here (see [`ByteSource::open`]).
    async fn reader(&self, offset: u64) -> io::Result<Box<dyn SeekableReader>> {
        let torrent = self
            .engine
            .get_engine(&self.info_hash)
            .await
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no torrent {} in this engine", self.info_hash),
                )
            })?;
        let handle = torrent
            .try_get_file_with_intent(
                self.file_idx,
                offset,
                PLAYBACK_PRIORITY,
                // **Streaming, never Download.** A player seeks in a
                // member; the download intent's 256 MiB lookahead in front
                // of a seeking player is a quarter of a gigabyte the swarm
                // fetches and the retention owner cannot keep.
                Fetching::Streaming,
                BufferProfile::default(),
            )
            .await
            .map_err(io::Error::other)?;
        Ok(Box::new(handle))
    }
}

#[async_trait::async_trait]
impl ByteSource for TorrentFileSource {
    fn len(&self) -> u64 {
        self.len
    }

    /// The entity being played is this source's own when **any file of
    /// this torrent** is it.
    ///
    /// Per torrent rather than per file, for the same reason the retention
    /// window is not: a container made of several files of one torrent --
    /// a RAR set -- is read by a body that crosses from one to the next,
    /// and the cell names whichever of them the body is inside. See
    /// `crate::translators::session::TranslatedSession::is_live`, which is
    /// what asks.
    fn is_live(&self, reading: &enginefs::retention::live::Reading) -> bool {
        reading.is_torrent(&self.info_hash)
    }

    fn describe(&self) -> String {
        format!("{} in torrent {}", self.name, self.info_hash)
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        if offset >= self.len || buf.is_empty() {
            return Ok(0);
        }
        let mut held = self.index_reader.lock().await;
        if held.is_none() {
            *held = Some(self.reader(offset).await?);
        }
        let reader = held.as_mut().expect("just opened");
        reader.seek(SeekFrom::Start(offset)).await?;
        let want = (buf.len() as u64).min(self.len - offset) as usize;
        read_filling(reader, &mut buf[..want]).await
    }

    async fn open(&self, offset: u64, _hint: ReadHint) -> io::Result<Box<dyn SeekableReader>> {
        if offset >= self.len {
            return Ok(Box::new(std::io::Cursor::new(Vec::new())));
        }
        // **The offset is told twice, and has to be.** The engine takes it
        // as where the read is about to be, which is what the intent
        // prioritises around; the handle itself still starts at the top of
        // the file, exactly as `routes::stream` finds it. A body served
        // without this seek is the archive's first bytes under the
        // member's name.
        //
        // The hint is spent by the engine and not here: it works the
        // lookahead out from the intent, the film's measured bitrate and
        // what the retention budget can keep, which is a better answer
        // than a caller has. See [`ReadHint`].
        let mut reader = self.reader(offset).await?;
        reader.seek(SeekFrom::Start(offset)).await?;
        Ok(reader)
    }
}
