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
//!   length, the source's kind, the counters it shares with the task and
//!   the runtime's `Handle` -- nothing whose drop spawns;
//! * a cancel is the token, never a command, and every read and seek the
//!   task makes is raced against it.

use super::prewant::Tracker;
use super::registry::Entry;
use super::{ReadWait, Refusal};
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
    /// Shared with the registry entry: a link, a Drive file, a finished
    /// download read off the cache, or a file on this device -- whose
    /// source holds no registration of its own. Its kind is `http` for
    /// every one of them.
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
    /// The bytes: what a reader task opens, and what a cast body opens at
    /// its range (`crate::cast`).
    pub(crate) fn bytes(&self) -> &dyn ByteSource {
        match self {
            Self::Torrent(source) => source.as_ref(),
            Self::Shared(source) => source.as_ref(),
            Self::Member { view, .. } => view,
        }
    }

    pub(crate) fn kind(&self) -> &'static str {
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
    /// The time the foreign side has spent blocked in a read or a seek:
    /// how long the source kept a rendition's producer
    /// (`crate::rendition::speed`), which its speed leaves out.
    waits: Arc<crate::rendition::WaitClock>,
    /// Where the task counts what it reads and how often it reopens, once
    /// somebody asked ([`MediaReader::count_into`]): a cast's panel.
    tally: std::sync::OnceLock<Arc<SourceTally>>,
}

/// **What a source has been asked for**, summed over every reader counted
/// into it ([`MediaReader::count_into`]) -- every run of a rendition, every
/// body of a plain cast: the bytes read, the opens, and the seeks (each a
/// reopen at another offset). Monotonic; the caller makes the rates.
#[derive(Default)]
pub(crate) struct SourceTally {
    kind: std::sync::OnceLock<&'static str>,
    read: std::sync::atomic::AtomicU64,
    opens: std::sync::atomic::AtomicU64,
    seeks: std::sync::atomic::AtomicU64,
}

impl SourceTally {
    /// One open of a source of `kind` (`torrent`, `http`, `member`).
    pub(crate) fn opened(&self, kind: &'static str) {
        let _ = self.kind.set(kind);
        self.opens.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn read(&self, bytes: u64) {
        self.read.fetch_add(bytes, Ordering::Relaxed);
    }

    fn seeked(&self) {
        self.seeks.fetch_add(1, Ordering::Relaxed);
    }

    /// The kind of the first source opened, `None` before one was.
    pub(crate) fn kind(&self) -> Option<&'static str> {
        self.kind.get().copied()
    }

    /// Bytes read, opens, seeks.
    pub(crate) fn counts(&self) -> (u64, u64, u64) {
        (
            self.read.load(Ordering::Relaxed),
            self.opens.load(Ordering::Relaxed),
            self.seeks.load(Ordering::Relaxed),
        )
    }
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
    /// What the source is (`Source::kind`).
    kind: &'static str,
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

/// A played torrent reader's pre-want, as the registry planned it: when to
/// let it go, and what to ask for, in order (`super::prewant`).
pub(crate) struct Planned {
    pub(crate) tracker: Tracker,
    pub(crate) steps: Vec<std::ops::Range<u64>>,
}

/// **What an id's reader tasks show of their reads**, kept on its registry
/// entry: which of them is waiting on a read and since when
/// ([`ReadWait`]), where the viewer's playback was last served to, and the
/// window a pre-want is asking for. Written by the tasks, read by the
/// handle's cheap questions; no clock is read here but the one handed in.
#[derive(Default)]
pub(crate) struct ReadWatch {
    /// The reads in flight: the task's number, since when, and where.
    waiting: std::sync::Mutex<Vec<(u64, std::time::Instant, u64)>>,
    /// The end of the last read served to a played reader.
    served: std::sync::Mutex<Option<u64>>,
    /// What is being pre-wanted now: its steps, in the order they are
    /// asked for.
    prewant: Arc<std::sync::Mutex<Option<Vec<std::ops::Range<u64>>>>>,
}

/// Numbers the reader tasks, so each can take its own entry out of
/// [`ReadWatch::waiting`].
static NEXT_TASK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl ReadWatch {
    fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn enter(&self, task: u64, now: std::time::Instant, offset: u64) {
        Self::lock(&self.waiting).push((task, now, offset));
    }

    fn leave(&self, task: u64) {
        Self::lock(&self.waiting).retain(|(waiting, ..)| *waiting != task);
    }

    fn served(&self, end: u64) {
        *Self::lock(&self.served) = Some(end);
    }

    /// The oldest wait in flight, as of `now`.
    pub(crate) fn wait(&self, now: std::time::Instant) -> ReadWait {
        let waiting = Self::lock(&self.waiting);
        let Some((_, since, offset)) = waiting.iter().min_by_key(|(_, since, _)| *since) else {
            return ReadWait::default();
        };
        ReadWait {
            waiting_ms: Some(now.saturating_duration_since(*since).as_millis() as u64),
            offset: Some(*offset),
        }
    }

    /// Where a played reader was last served to.
    pub(crate) fn last_served(&self) -> Option<u64> {
        *Self::lock(&self.served)
    }

    /// What is being pre-wanted now, in the order it is asked for.
    pub(crate) fn prewanting(&self) -> Option<Vec<std::ops::Range<u64>>> {
        Self::lock(&self.prewant).clone()
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
        let bytes = self.timed(answer)??;
        buf[..bytes.len()].copy_from_slice(&bytes);
        Ok(bytes.len())
    }

    /// Move to `offset` and answer it. A seek to where the reader is
    /// already costs nothing -- mpv makes one right after every open to
    /// learn whether the stream seeks.
    pub fn seek(&mut self, offset: u64) -> io::Result<u64> {
        let (reply, answer) = oneshot::channel();
        self.send(Command::Seek { offset, reply })?;
        self.timed(answer)?
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

    /// [`Self::wait`], with the time spent in it on the reader's wait clock.
    fn timed<T>(&self, answer: oneshot::Receiver<T>) -> io::Result<T> {
        self.shown.waits.enter(std::time::Instant::now());
        let answered = Self::wait(answer);
        self.shown.waits.leave(std::time::Instant::now());
        answered
    }

    /// The time this reader's caller has spent blocked in it, for a
    /// rendition's speed (`crate::rendition::speed`).
    pub(crate) fn waits(&self) -> Arc<crate::rendition::WaitClock> {
        self.shown.waits.clone()
    }

    /// Count this reader's open, and from now on its reads and seeks, into
    /// `tally`. Once: a second tally is ignored. Called before the reader
    /// is handed on, so no read goes uncounted.
    pub(crate) fn count_into(&self, tally: Arc<SourceTally>) {
        tally.opened(self.kind);
        let _ = self.shown.tally.set(tally);
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
///
/// `prewant` is the played torrent reader's pre-want, planned by the
/// registry: **asked for here, at the open**, beside the head the caller
/// is about to read -- the file is open, so its entity is there to promise
/// on -- and let go by the task as its reads are served (see [`Tracker`]).
pub(crate) async fn open(
    entry: Lease<Entry>,
    source: Source,
    prewant: Option<Planned>,
) -> Result<MediaReader, Refusal> {
    let reader = source.open(0).await.map_err(|error| refusal_of(&error))?;
    let len = source.bytes().len();
    let kind = source.kind();
    let shown = Arc::new(Shown::default());
    let (prewant, steps) = match prewant {
        Some(Planned { tracker, steps }) => (Some(tracker), steps),
        None => (None, Vec::new()),
    };
    let mut task = Task {
        number: NEXT_TASK.fetch_add(1, Ordering::Relaxed),
        watch: entry.watch.clone(),
        entry,
        source,
        reader,
        position: 0,
        len,
        delivered: 0,
        seeks: 0,
        shown: shown.clone(),
        prewant,
        prewanting: None,
    };
    if task.prewant.is_some() {
        task.ask_ahead(steps);
    }
    show_buffer(&task.source, &task.shown).await;
    let (commands, received) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    tokio::spawn(task.serve(received, cancel.clone()));
    Ok(MediaReader {
        commands,
        cancel,
        len,
        kind,
        runtime: tokio::runtime::Handle::current(),
        shown,
    })
}

/// Why an open failed, as a refusal: the disk gate's refusal and a dead
/// Drive grant as themselves.
pub(crate) fn refusal_of(error: &io::Error) -> Refusal {
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
    /// This task's number in [`ReadWatch::waiting`].
    number: u64,
    /// The entry's [`ReadWatch`], which this task writes.
    watch: Arc<ReadWatch>,
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
    /// When to let the pre-want go; `None` for a reader with nothing to
    /// pre-want.
    prewant: Option<Tracker>,
    /// The pre-want standing now: dropping the guard cancels the task that
    /// holds it, which lets the engine's ask go.
    prewanting: Option<tokio_util::sync::DropGuard>,
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
                    let begin = self.position;
                    self.watch
                        .enter(self.number, std::time::Instant::now(), begin);
                    let answer = tokio::select! {
                        biased;
                        () = cancel.cancelled() => Err(interrupted()),
                        read = self.read(max) => read,
                    };
                    self.watch.leave(self.number);
                    self.shown.waiting.store(false, Ordering::SeqCst);
                    let end = match &answer {
                        Ok(bytes) => begin + bytes.len() as u64,
                        Err(_) => begin,
                    };
                    let _ = reply.send(answer);
                    // After the answer: the player has its bytes before
                    // anything is asked of the swarm on their account.
                    self.served(begin, end);
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

    /// **Ask the swarm for `steps` of the file now**, at the open: the
    /// pre-want (`super::prewant`). On a task of its own, since the engine's
    /// answer can wait on a torrent still checking and an open must not;
    /// the guard kept in [`Self::prewanting`] ends it: what the engine
    /// holds for it is let go, at once if the answer comes after the end.
    fn ask_ahead(&mut self, steps: Vec<std::ops::Range<u64>>) {
        let Source::Torrent(torrent) = &self.source else {
            return;
        };
        let Some(asking) = torrent.prewant(steps.clone()) else {
            return;
        };
        let shown = self.watch.prewant.clone();
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        tokio::spawn(async move {
            let Some(held) = asking.await else {
                return;
            };
            let first = steps.first().cloned().unwrap_or_default();
            *ReadWatch::lock(&shown) = Some(steps);
            tracing::info!(
                first_start = first.start,
                first_end = first.end,
                stage = "prewant_started",
                "asking for the resume point beside the head"
            );
            stopped.cancelled().await;
            drop(held);
            *ReadWatch::lock(&shown) = None;
            tracing::info!(
                stage = "prewant_ended",
                "the resume point is no longer asked for ahead of the player"
            );
        });
        self.prewanting = Some(stop.drop_guard());
    }

    /// A read was served from `begin` to `end`: where the viewer's playback
    /// has got to, and whether the pre-want is let go for it.
    fn served(&mut self, begin: u64, end: u64) {
        if end <= begin {
            return;
        }
        let Source::Torrent(torrent) = &self.source else {
            return;
        };
        if !torrent.is_played() {
            return;
        }
        self.watch.served(end);
        if self
            .prewant
            .as_mut()
            .is_some_and(|tracker| tracker.served(begin, end))
        {
            self.prewanting = None;
        }
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
        if let Some(tally) = self.shown.tally.get() {
            tally.read(read as u64);
        }
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
        if let Some(tally) = self.shown.tally.get() {
            tally.seeked();
        }
        if let Some(tracker) = self.prewant.as_mut() {
            tracker.seeked();
        }
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
