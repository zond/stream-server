//! One file of a torrent as a [`ByteSource`]: [`TorrentSource`].
//!
//! The piece store already holds these bytes, or fetches them from the
//! swarm the moment a reader asks; a seek in it is the one thing the store
//! and its retention were built to serve well. So a container inside a
//! torrent -- a film in a stored ZIP, a RAR set across three volumes -- is
//! read by asking the torrent for the byte ranges the member occupies, and
//! nothing is extracted, copied or written anywhere.
//!
//! **One source, with or without a player behind it**
//! (`docs/design/media-pipeline.md` §2.2). With a [`Play`], an open is the
//! stream route's own open, factored out and called from both
//! (`crate::routes::stream::open_torrent_stream`): the viewer's play
//! session moved, the stream registered, the disk gate, the focus, and the
//! shared reader for the current screen of a file that shares. Without
//! one it is an aside -- an archive's index and member reads today, a
//! subtitle -- which registers a stream, opens every reader unshared and
//! touches no play session.
//!
//! Two things this gets right that the `torrent:` archive route once did
//! not:
//!
//! * **`Fetching::Streaming`, not `Download`.** That route opened its
//!   reader with the download intent's 256 MiB lookahead, on the reasoning
//!   that an archive member is read whole and sequentially. It is not: a
//!   player seeks in it, and a 256 MiB read-ahead in front of a seeking
//!   player is the swarm fetching a quarter of a gigabyte nobody is about
//!   to watch, and the retention owner unable to keep any of it.
//! * **The stream registration lasts as long as the source, not the
//!   handle.** Every route that opens a reader on a torrent registers a
//!   stream first (AGENTS.md), because the reconciler stops a torrent
//!   nothing holds, and a registered stream is what holds it while a body
//!   is delivered -- without one, a torrent nothing else holds stops
//!   mid-body, dropping its peers -- and because a
//!   request arriving on a torrent an earlier pass stopped gets a reader
//!   that parks on pieces nobody is fetching. A source holds that
//!   registration (and, with a play, the play session's move) for its
//!   whole life, so every read through it, and every index read between
//!   reads, is inside it; a file handle is per position, and a seek is a
//!   new one at the new offset, which neither ends the stream nor moves the
//!   session again.

use super::{ByteSource, ReadHint, SeekableReader, read_filling};
use crate::routes::stream::{
    Consumer, PLAYER_PRIORITY, Player, TorrentOpen, TorrentStream, next_stream_id,
    open_torrent_stream,
};
use enginefs::backend::TorrentHandle;
use enginefs::backend::priorities::{BufferProfile, Fetching};
use std::io::{self, SeekFrom};
use std::sync::Arc;
use tokio::io::AsyncSeekExt;

/// The viewer's playback behind a [`TorrentSource`]: what `p=` and
/// `buffer=` carry on the stream route.
#[derive(Clone, Debug)]
pub struct Play {
    /// The player token, `<viewer>.<screen>`, as `p=` carries it
    /// (`enginefs::retention::sessions::PlayerToken` is its parse).
    pub token: String,
    /// The viewer's read-ahead choice for this playback.
    pub buffer: BufferProfile,
    /// Whether the file may share at all: false for a container file
    /// played by its name, which shares nothing.
    pub shares: bool,
    /// Where the member being played lies, when what plays is a member of
    /// a container (`crate::media`): its byte extent in this file, or its
    /// bytes in every volume of a set. The play session's draw is then
    /// sized from the member's length and made inside it. `None` for the
    /// file played as itself.
    pub member: Option<MemberExtent>,
}

/// Where a member a media id plays lies in the torrent ([`Play::member`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemberExtent {
    /// Its byte extent in the one file that is its container: the play
    /// session is on that file, sharing the member's pieces
    /// (`enginefs::retention::sessions::Played::Torrent`'s `member`).
    In(std::ops::Range<u64>),
    /// Its bytes in each volume of a multi-volume set, in the member's
    /// order: the play session is on the set, one thing played whichever
    /// volume the reader is in, sharing the member's pieces of all of them
    /// (`enginefs::retention::sessions::Played::Set`). Played with
    /// [`Play::shares`] true: a set's session shares.
    Across(Vec<enginefs::retention::sessions::Volume>),
}

/// A [`Play`] and what its open needs beyond the engine, with the stream
/// it registered once the first read made one.
struct Played {
    play: Play,
    /// The read-ahead choice the first open is made with: `play.buffer`
    /// until [`TorrentSource::set_buffer`] says otherwise. After the first
    /// open the stream holds its own (`TorrentStream::set_buffer`).
    buffer: std::sync::Mutex<BufferProfile>,
    /// Whose slack the disk gate drops beside the torrents'.
    proxy_cache: Arc<crate::proxy_cache::ProxyCache>,
    /// `None` until the first reader: the open is the route's, and the
    /// route's open is at an offset. Held from then on, for the source's
    /// life -- the registration and the play session are the source's,
    /// the handle is the position's.
    stream: tokio::sync::Mutex<Option<TorrentStream>>,
}

/// One file of one torrent, as a source of bytes. With a [`Play`] its reads
/// are the viewer's playback; without one they are an aside (a subtitle,
/// an index read).
pub struct TorrentSource {
    engine: Arc<enginefs::EngineFS>,
    info_hash: String,
    file_idx: usize,
    len: u64,
    name: String,
    play: Option<Played>,
    /// One reader kept open for the scattered reads an index is made of,
    /// seeked under the lock rather than reopened: the piece store serves
    /// a seek cheaply, and opening a reader is a `prepare_file_for_
    /// streaming` and a retention install each time.
    index_reader: tokio::sync::Mutex<Option<Box<dyn SeekableReader>>>,
    /// The engine's [`enginefs::EngineFS::clear_generation`] when this
    /// source was made: a cache clear since stopped what it was streaming,
    /// and it opens nothing more ([`Self::reader`]).
    cleared_at: u64,
}

impl TorrentSource {
    /// The file at `path_in_torrent` of the torrent `info_hash`, as an
    /// aside: registered as a stream for as long as the source lives, read
    /// unshared, no play session touched.
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
        Self::aside_in(engine, info_hash, file_idx, &files).await
    }

    /// File `file_idx` of the torrent `info_hash`, as an aside: what
    /// [`Self::open`] makes once it has found the file by its path, and
    /// what a reader over an id without a play makes by index
    /// (`crate::media`).
    pub(crate) async fn aside(
        engine: Arc<enginefs::EngineFS>,
        info_hash: &str,
        file_idx: usize,
    ) -> io::Result<Self> {
        let info_hash = info_hash.to_lowercase();
        let files = Self::files(&engine, &info_hash).await?;
        Self::aside_in(engine, info_hash, file_idx, &files).await
    }

    /// [`Self::aside`] over the file list already in hand.
    async fn aside_in(
        engine: Arc<enginefs::EngineFS>,
        info_hash: String,
        file_idx: usize,
        files: &[enginefs::backend::BackendFileInfo],
    ) -> io::Result<Self> {
        let file = Self::file(&info_hash, file_idx, files)?;
        let (len, name) = (file.length, file.name.clone());
        // Before any reader, and it has to be before: what the reconciler
        // is being told is that a read is about to start, and the
        // reconcile it makes is what starts a torrent an earlier pass left
        // stopped.
        //
        // The sibling of the stream route's registration, for the same
        // reasons -- including the handover: `on_stream_start` undoes its
        // own registration if it is dropped before it returns, and from the
        // instant it returns the source below owns ending it, with no await
        // in between (see `crate::routes::stream::StreamLifecycleGuard::
        // start`). Without it the reconciler's tick reads nobody playing and
        // pauses the torrent under the read, and a request arriving on a
        // torrent an earlier pass stopped gets a reader that parks on
        // pieces nobody is fetching.
        engine.on_stream_start(&info_hash, file_idx).await;
        Ok(Self {
            cleared_at: engine.clear_generation(),
            engine,
            info_hash,
            file_idx,
            len,
            name,
            play: None,
            index_reader: tokio::sync::Mutex::new(None),
        })
    }

    /// File `file_idx` of the torrent `info_hash`, read as the viewer's
    /// playback: its first read makes the stream route's open
    /// (`open_torrent_stream`), and the stream and the play session it
    /// moved are held until the source is dropped.
    pub(crate) async fn played(
        state: &crate::state::AppState,
        info_hash: &str,
        file_idx: usize,
        play: Play,
    ) -> io::Result<Self> {
        let info_hash = info_hash.to_lowercase();
        let files = Self::files(&state.engine, &info_hash).await?;
        let file = Self::file(&info_hash, file_idx, &files)?;
        Ok(Self {
            cleared_at: state.engine.clear_generation(),
            engine: state.engine.clone(),
            len: file.length,
            name: file.name.clone(),
            info_hash,
            file_idx,
            play: Some(Played {
                buffer: std::sync::Mutex::new(play.buffer),
                play,
                proxy_cache: state.proxy_cache.clone(),
                stream: tokio::sync::Mutex::new(None),
            }),
            index_reader: tokio::sync::Mutex::new(None),
        })
    }

    /// Make the next open with `buffer`: the viewer's read-ahead choice,
    /// changed mid-playback. The lookahead is worked out at an open, so it
    /// takes effect at the next one -- a seek -- and never in a handle
    /// already reading. Nothing for an aside, whose reads are opened at
    /// the default whatever a viewer chose.
    pub(crate) async fn set_buffer(&self, buffer: BufferProfile) {
        let Some(played) = &self.play else {
            return;
        };
        *played
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = buffer;
        if let Some(stream) = played.stream.lock().await.as_mut() {
            stream.set_buffer(buffer);
        }
    }

    /// The read-ahead choice the stream's opens are made with, once the
    /// first one has been; `None` for an aside or a played source not yet
    /// opened. What a test of [`Self::set_buffer`] can read, since what the
    /// profile changes -- how far ahead the engine fetches -- is not a
    /// number this crate is shown.
    pub(crate) async fn buffer_in_use(&self) -> Option<BufferProfile> {
        let played = self.play.as_ref()?;
        played
            .stream
            .lock()
            .await
            .as_ref()
            .map(TorrentStream::buffer)
    }

    /// Whether this source's reads are the viewer's playback.
    pub(crate) fn is_played(&self) -> bool {
        self.play.is_some()
    }

    /// **Ask the swarm for `steps` of this file, in that order, ahead of
    /// the player** (`crate::media::prewant`), as a future that owns what it
    /// needs, so the asking runs on a task of its own and never holds up an
    /// open or a read. `None` for an aside, which has no playback to ask
    /// ahead of.
    pub(crate) fn prewant(
        &self,
        steps: Vec<std::ops::Range<u64>>,
    ) -> Option<impl std::future::Future<Output = Option<enginefs::engine::PreWant>> + Send + 'static>
    {
        self.play.as_ref()?;
        let (engine, info_hash, file_idx) =
            (self.engine.clone(), self.info_hash.clone(), self.file_idx);
        Some(async move {
            let torrent = Self::torrent(&engine, &info_hash).await.ok()?;
            torrent.prewant(file_idx, steps).await
        })
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
        Ok(Self::torrent(engine, info_hash)
            .await?
            .handle
            .get_files()
            .await)
    }

    async fn torrent(
        engine: &Arc<enginefs::EngineFS>,
        info_hash: &str,
    ) -> io::Result<Arc<enginefs::engine::Engine<enginefs::backend::librqbit::LibrqbitHandle>>>
    {
        engine.get_engine(info_hash).await.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no torrent {info_hash} in this engine"),
            )
        })
    }

    fn file<'a>(
        info_hash: &str,
        file_idx: usize,
        files: &'a [enginefs::backend::BackendFileInfo],
    ) -> io::Result<&'a enginefs::backend::BackendFileInfo> {
        files.get(file_idx).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "torrent {info_hash} has {} files, not {}",
                    files.len(),
                    file_idx + 1
                ),
            )
        })
    }

    /// A reader on this file, the engine told the read is about to be at
    /// `offset`, at the streaming intent. A new handle every call: the
    /// handle is per position, the registration is the source's.
    ///
    /// **How far ahead the swarm is asked for is the engine's to decide**,
    /// not this call's: `try_get_file_with_intent` (and its unshared twin)
    /// takes the intent and works the lookahead out from the film's
    /// measured bitrate, the viewer's buffer profile and what the retention
    /// budget can keep -- which is the whole reason to go through it rather
    /// than through `get_file_reader`, whose lookahead argument installs no
    /// policy to keep what it pulls. A [`ReadHint`] is therefore spent on
    /// nothing at all here (see [`ByteSource::open`]).
    async fn reader(&self, offset: u64) -> io::Result<Box<dyn SeekableReader>> {
        // **A source a cache clear stopped stays stopped.** The clear failed
        // the reads it had open (`enginefs::Engine::cut_reads`); a seek is a
        // new open, and one that succeeded would fetch again behind a
        // player that was told its stream had ended -- on a torrent the
        // clear stopped, for a stream it no longer counts. Whoever wants it
        // again opens a new source, which is a new ask.
        if self.engine.clear_generation() != self.cleared_at {
            return Err(enginefs::files::cleared_error());
        }
        let torrent = Self::torrent(&self.engine, &self.info_hash).await?;
        let Some(played) = &self.play else {
            // **Unshared.** An aside's read is not a player's: it draws
            // nothing.
            let handle = torrent
                .try_get_file_unshared(
                    self.file_idx,
                    offset,
                    PLAYER_PRIORITY,
                    // **Streaming, never Download.** A player seeks in a
                    // member; the download intent's 256 MiB lookahead in
                    // front of a seeking player is a quarter of a gigabyte
                    // the swarm fetches and the retention owner cannot
                    // keep.
                    Fetching::Streaming,
                    BufferProfile::default(),
                )
                .await
                .map_err(io::Error::other)?;
            return Ok(Box::new(handle));
        };
        let mut stream = played.stream.lock().await;
        if let Some(stream) = stream.as_ref() {
            // A seek, or a second reader: the stream is registered and the
            // session moved already, so only the handle is new.
            let handle = stream.open_at(offset).await.map_err(io::Error::other)?;
            return Ok(Box::new(handle));
        }
        // Read out before the open: the guard is not held across an await.
        let buffer = *played
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (opened, handle) = open_torrent_stream(TorrentOpen {
            engine_fs: &self.engine,
            proxy_cache: &played.proxy_cache,
            engine: &torrent,
            info_hash: &self.info_hash,
            file_idx: self.file_idx,
            stream_id: next_stream_id(),
            offset,
            intent: Fetching::Streaming,
            buffer,
            player: Some(Player {
                token: &played.play.token,
                shares: played.play.shares,
                member: played.play.member.clone(),
            }),
            // A played source is read by a reader, never by a response
            // body: its end line says so rather than reporting a body that
            // never existed.
            consumer: Consumer::Reader,
        })
        .await?;
        *stream = Some(opened);
        Ok(Box::new(handle))
    }
}

impl Drop for TorrentSource {
    /// An aside ends the stream its open registered. A played source's
    /// stream is its `TorrentStream`'s to end, and goes with it.
    fn drop(&mut self) {
        if self.play.is_some() {
            return;
        }
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

#[async_trait::async_trait]
impl ByteSource for TorrentSource {
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
