//! **The reader task**: a blocking reader over async sources
//! (`docs/design/media-pipeline.md` §2.4).
//!
//! A [`MediaReader`] is what a foreign thread holds -- mpv's cookie. Its
//! calls block that thread and never poll a future on it: each one is a
//! [`Command`] to the one runtime task that owns the reader, answered over
//! a oneshot. The module docs of [`crate::media`] state who owns what; in
//! this file's terms:
//!
//! * the task owns the [`Source`], the [`SeekableReader`] open on it, the
//!   position, and the lease on the registry entry, and drops all of them
//!   on the runtime when the command channel ends;
//! * the [`MediaReader`] owns the sender, the [`CancellationToken`], the
//!   length and the runtime's `Handle` -- nothing whose drop spawns;
//! * a cancel is the token, never a command, and every read and seek the
//!   task makes is raced against it.

use super::Refusal;
use super::registry::Entry;
use crate::sources::{ByteSource, MemberView, ReadHint, SeekableReader, TorrentSource};
use crate::translators::session::{Lease, TranslatedSession};
use bytes::Bytes;
use enginefs::backend::priorities::BufferProfile;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// The most one read answers, whatever it asks for: one allocation per
/// read, and a player asks for far less.
const MAX_READ: usize = 1 << 20;

/// What the foreign side asks of the task. One in flight at a time: the
/// channel holds one, and a [`MediaReader`]'s calls take it by `&mut`.
pub enum Command {
    /// Up to `max` bytes from the current position; the position advances
    /// by what is returned. `Ok(empty)` is end of file. Whatever has
    /// arrived, at least one byte -- not a filled buffer -- so a slow
    /// torrent delivers as it arrives, as an HTTP body does.
    Read {
        max: usize,
        reply: oneshot::Sender<io::Result<Bytes>>,
    },
    /// Move to `offset`; answers the offset it is at. A reopen of the
    /// source there, for every source, unless it is where the reader
    /// already is.
    Seek {
        offset: u64,
        reply: oneshot::Sender<io::Result<u64>>,
    },
}

/// The source a reader task reads, owned by the task.
pub(crate) enum Source {
    /// One per reader: its stream registration and play are this reader's.
    Torrent(Box<TorrentSource>),
    /// Shared with the registry entry: a proxied or Drive entity, whose
    /// source holds no registration of its own.
    Shared(Arc<dyn ByteSource>),
    /// A member of a container: the view over its volumes, the volumes
    /// opened as the viewer's playback (none for an aside or a container
    /// behind links), and the lease on the container's session -- held for
    /// the reader's life, so the session, and a set's volume hold with it,
    /// is not let go under a reader (§2.4).
    Member {
        view: MemberView,
        played: Vec<Arc<TorrentSource>>,
        _session: Lease<TranslatedSession>,
    },
}

impl Source {
    fn bytes(&self) -> &dyn ByteSource {
        match self {
            Self::Torrent(source) => source.as_ref(),
            Self::Shared(source) => source.as_ref(),
            Self::Member { view, .. } => view,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Torrent(_) => "torrent",
            Self::Shared(_) => "http",
            Self::Member { .. } => "member",
        }
    }

    /// The torrent files this source reads as the viewer's playback, whose
    /// opens take the viewer's read-ahead choice.
    fn played(&self) -> Vec<&TorrentSource> {
        match self {
            Self::Torrent(source) => vec![source.as_ref()],
            Self::Member { played, .. } => played.iter().map(AsRef::as_ref).collect(),
            Self::Shared(_) => Vec::new(),
        }
    }

    /// A reader at `offset`, to the end of the file.
    async fn open(&self, offset: u64) -> io::Result<Box<dyn SeekableReader>> {
        self.bytes().open(offset, ReadHint::REST).await
    }
}

/// What the task shows the foreign side besides its answers: whether a
/// read or seek is being awaited now (a test's probe), and the read-ahead
/// choice the torrent stream's last open was made with.
#[derive(Default)]
struct Shown {
    waiting: AtomicBool,
    opened_with: std::sync::Mutex<Option<BufferProfile>>,
}

/// A blocking reader over a registered id: what mpv's `stream_cb` and a
/// JNI export hold. See the module docs.
///
/// **Not for a runtime thread.** Every call blocks the calling thread
/// until the task answers; one made from inside a tokio runtime answers an
/// error at once rather than blocking a worker.
pub struct MediaReader {
    commands: mpsc::Sender<Command>,
    cancel: CancellationToken,
    len: u64,
    /// The runtime the task runs on. Entered while the reader is dropped,
    /// so nothing a drop does could ever spawn off a runtime.
    runtime: tokio::runtime::Handle,
    shown: Arc<Shown>,
}

/// A handle that cancels a [`MediaReader`] from another thread while one
/// of its calls blocks the thread that holds it: what mpv's `cancel_fn`
/// is. It does not block.
#[derive(Clone)]
pub struct Canceller {
    cancel: CancellationToken,
    shown: Arc<Shown>,
}

impl Canceller {
    /// Cancel the call in flight, and every later one: each answers
    /// [`io::ErrorKind::Interrupted`]. Sticky, as mpv's contract says.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Whether the task is awaiting a read or a seek right now. A probe for
    /// the tests of what a cancel wakes; nothing decides anything from it.
    #[doc(hidden)]
    pub fn is_waiting(&self) -> bool {
        self.shown.waiting.load(Ordering::SeqCst)
    }
}

fn interrupted() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "the read was cancelled")
}

fn gone() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "the reader has closed: the server has stopped",
    )
}

impl MediaReader {
    /// Up to `buf.len()` bytes from the current position, `Ok(0)` at the
    /// end of the file. Blocks until some have arrived, the reader is
    /// cancelled ([`io::ErrorKind::Interrupted`]) or the server stops (an
    /// error).
    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let (reply, answer) = oneshot::channel();
        self.send(Command::Read {
            max: buf.len(),
            reply,
        })?;
        let bytes = Self::wait(answer)??;
        buf[..bytes.len()].copy_from_slice(&bytes);
        Ok(bytes.len())
    }

    /// Move to `offset` and answer it. A seek to where the reader is
    /// already costs nothing -- mpv makes one right after every open to
    /// learn whether the stream seeks.
    pub fn seek(&mut self, offset: u64) -> io::Result<u64> {
        let (reply, answer) = oneshot::channel();
        self.send(Command::Seek { offset, reply })?;
        Self::wait(answer)?
    }

    /// Cancel the call in flight and every later one. Does not block.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// A handle that cancels this reader from another thread.
    pub fn canceller(&self) -> Canceller {
        Canceller {
            cancel: self.cancel.clone(),
            shown: self.shown.clone(),
        }
    }

    /// The file's length.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the file has no bytes.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The read-ahead choice the torrent stream's last open was made with,
    /// `None` for anything that is not a played torrent. A probe for the
    /// test of `set_buffer`: what the profile changes -- how far ahead the
    /// engine fetches -- is not a number this crate is shown.
    #[doc(hidden)]
    pub fn opened_with_buffer(&self) -> Option<BufferProfile> {
        *self
            .shown
            .opened_with
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn send(&self, command: Command) -> io::Result<()> {
        // A cancelled reader is answered by the task, which checks the token
        // before it touches the source: at once, and in one place.
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(io::Error::other(
                "a MediaReader blocks its caller, and was called from inside a tokio runtime",
            ));
        }
        self.commands
            .try_send(command)
            .map_err(|error| match error {
                // Unreachable while calls take `&mut self`: the one call in
                // flight has had its answer before another can be made.
                mpsc::error::TrySendError::Full(_) => {
                    io::Error::other("a call is already in flight")
                }
                mpsc::error::TrySendError::Closed(_) => gone(),
            })
    }

    fn wait<T>(answer: oneshot::Receiver<T>) -> io::Result<T> {
        answer.blocking_recv().map_err(|_| gone())
    }
}

impl Drop for MediaReader {
    /// Close: the sender goes, and the task, seeing its channel end, drops
    /// the reader on the runtime and exits. Nothing here waits for that.
    fn drop(&mut self) {
        let _entered = self.runtime.enter();
        let (closed, _) = mpsc::channel(1);
        drop(std::mem::replace(&mut self.commands, closed));
    }
}

/// Open `source` at the top and hand the reader to a task of its own.
/// Called on the runtime: the registration a torrent's first open makes is
/// made here, before the caller has its reader.
pub(crate) async fn open(entry: Lease<Entry>, source: Source) -> Result<MediaReader, Refusal> {
    let reader = source.open(0).await.map_err(|error| refusal_of(&error))?;
    let len = source.bytes().len();
    let shown = Arc::new(Shown::default());
    let task = Task {
        entry,
        source,
        reader,
        position: 0,
        len,
        delivered: 0,
        seeks: 0,
        shown: shown.clone(),
    };
    show_buffer(&task.source, &task.shown).await;
    let (commands, received) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    tokio::spawn(task.serve(received, cancel.clone()));
    Ok(MediaReader {
        commands,
        cancel,
        len,
        runtime: tokio::runtime::Handle::current(),
        shown,
    })
}

/// Why an open failed, as a refusal: the disk gate's refusal and a dead
/// Drive grant as themselves.
pub(super) fn refusal_of(error: &io::Error) -> Refusal {
    if error.kind() == io::ErrorKind::StorageFull {
        return Refusal::InsufficientDiskSpace;
    }
    if let Some(crate::sources::drive::DriveError::PairAgain) =
        crate::sources::drive::DriveError::in_read(error)
    {
        return Refusal::PairAgain;
    }
    Refusal::OpenFailed(error.to_string())
}

/// Everything one open reader holds, owned by its task.
struct Task {
    /// The registry entry, leased: an id being read is not evicted.
    entry: Lease<Entry>,
    source: Source,
    reader: Box<dyn SeekableReader>,
    position: u64,
    len: u64,
    /// Bytes answered, for the close line.
    delivered: u64,
    /// Reopens, for the close line.
    seeks: u64,
    shown: Arc<Shown>,
}

impl Task {
    async fn serve(mut self, mut commands: mpsc::Receiver<Command>, cancel: CancellationToken) {
        while let Some(command) = commands.recv().await {
            match command {
                Command::Read { max, reply } => {
                    // Biased: a cancelled token answers before the source
                    // is touched, so every call after a cancel is
                    // `Interrupted` at once -- the stickiness is the token's.
                    self.shown.waiting.store(true, Ordering::SeqCst);
                    let answer = tokio::select! {
                        biased;
                        () = cancel.cancelled() => Err(interrupted()),
                        read = self.read(max) => read,
                    };
                    self.shown.waiting.store(false, Ordering::SeqCst);
                    let _ = reply.send(answer);
                }
                Command::Seek { offset, reply } => {
                    // Biased: a cancelled token answers before the source
                    // is touched, so every call after a cancel is
                    // `Interrupted` at once -- the stickiness is the token's.
                    self.shown.waiting.store(true, Ordering::SeqCst);
                    let answer = tokio::select! {
                        biased;
                        () = cancel.cancelled() => Err(interrupted()),
                        seek = self.seek(offset) => seek,
                    };
                    self.shown.waiting.store(false, Ordering::SeqCst);
                    let _ = reply.send(answer);
                }
            }
        }
        // The channel has ended: the reader was dropped. Everything the
        // task holds goes here, on the runtime -- the file handle, the
        // stream registration and its spawned end, the entry's lease.
        tracing::info!(
            source = self.source.kind(),
            delivered = self.delivered,
            seeks = self.seeks,
            len = self.len,
            cancelled = cancel.is_cancelled(),
            stage = "media_reader_closed",
            "media reader closed"
        );
    }

    async fn read(&mut self, max: usize) -> io::Result<Bytes> {
        if self.position >= self.len {
            return Ok(Bytes::new());
        }
        let want = max.min(MAX_READ).min((self.len - self.position) as usize);
        let mut buf = vec![0u8; want];
        let read = self.reader.read(&mut buf).await?;
        buf.truncate(read);
        self.position += read as u64;
        self.delivered += read as u64;
        Ok(Bytes::from(buf))
    }

    async fn seek(&mut self, offset: u64) -> io::Result<u64> {
        if offset == self.position {
            return Ok(offset);
        }
        if offset > self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("a seek to {offset} is past the end of {} bytes", self.len),
            ));
        }
        // The viewer's read-ahead choice, if it has changed: the lookahead
        // is worked out at an open, and this is one.
        let buffer = *self
            .entry
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(buffer) = buffer {
            for source in self.source.played() {
                source.set_buffer(buffer).await;
            }
        }
        // Opened before the old one goes, so a failed reopen leaves the
        // reader where it was and still reading.
        self.reader = self.source.open(offset).await?;
        self.position = offset;
        self.seeks += 1;
        show_buffer(&self.source, &self.shown).await;
        Ok(offset)
    }
}

/// Show the read-ahead choice `source`'s torrent stream opens with. Takes
/// the two pieces rather than the task: the task's reader is not `Sync`,
/// so a borrow of the whole task cannot be held across the await.
async fn show_buffer(source: &Source, shown: &Shown) {
    let mut buffer = None;
    for played in source.played() {
        buffer = buffer.or(played.buffer_in_use().await);
    }
    *shown
        .opened_with
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = buffer;
}
