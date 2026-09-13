use crate::backend::{FileStreamTrait, TorrentHandle};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncSeek};

/// A read that waited at least this long before returning data is logged.
///
/// Reads here legitimately block for tens of seconds -- a read at an offset
/// whose piece is not on disk parks until the whole piece verifies, which
/// on a 16 MiB piece at a few hundred kB/s is a minute. Nothing logged that
/// at all, so a player timing out on a slow read looked from the server
/// side like nothing happening.
pub const BLOCKED_READ_LOG_THRESHOLD: Duration = Duration::from_secs(1);

/// Where the reader is and how long its current read has been parked.
///
/// Split out from [`FileHandle`] so the arithmetic behind the blocked-read
/// line -- which offset, therefore which piece, and how long the wait
/// really was -- is testable without a torrent session.
#[derive(Debug)]
struct ReadCursor {
    /// Where the next read starts. Seeded with the offset the reader was
    /// opened at, advanced by what each read delivers, and reset by a seek.
    position: u64,
    /// When the read in flight first returned `Pending`. Kept across the
    /// intermediate polls of one read, so the reported wait is the whole
    /// wait and not the gap since the last wake-up.
    pending_since: Option<Instant>,
    /// When the read in flight was first polled -- when the consumer asked
    /// for it, as against when it parked.
    ///
    /// Latched on the first poll and kept across the rest, for the same
    /// reason `pending_since` is: `poll_read` runs many times for one read,
    /// once per wake-up, and a stamp overwritten on each of them says the
    /// read arrived at the moment it was about to return. A read blocked
    /// four seconds would then report having taken no time at all, which is
    /// the one number the consumer actually felt.
    arrived: Option<Instant>,
}

/// What one served read turned out to be.
///
/// Returned by [`ReadCursor::resume`] rather than read off the cursor
/// afterwards, because `resume` is what advances the cursor: asked after
/// it, `position` is the offset the *next* read starts at, and every
/// caller wanting the offset this one ran from would have to subtract what
/// it delivered. `log_blocked_read` did not, and named the wrong piece.
#[derive(Debug, Clone, Copy)]
struct Served {
    /// The offset this read ran from -- the piece it parked on, when it
    /// parked.
    begin: u64,
    /// The offset it reached, which is where the next read starts.
    end: u64,
    /// When the consumer asked for it.
    arrived: Instant,
    /// When it came back.
    returned: Instant,
    /// How long it spent parked, if it parked at all.
    waited: Option<Duration>,
}

impl Served {
    /// How long the consumer waited for this read, parked or not.
    ///
    /// Not [`Served::waited`], which is time spent parked on a missing
    /// piece and is `None` for a read served from the disk. This is the
    /// whole of it, and it is what a player experiences.
    fn took(&self) -> Duration {
        self.returned.saturating_duration_since(self.arrived)
    }
}

impl ReadCursor {
    fn new(position: u64) -> Self {
        Self {
            position,
            pending_since: None,
            arrived: None,
        }
    }

    /// Whether the read in flight has already been stamped.
    fn has_arrived(&self) -> bool {
        self.arrived.is_some()
    }

    /// The consumer asked. Call under [`ReadCursor::has_arrived`], so the
    /// clock is read once per read rather than once per poll.
    fn arrive(&mut self, now: Instant) {
        self.arrived.get_or_insert(now);
    }

    /// The read could not be served yet.
    fn park(&mut self, now: Instant) {
        self.pending_since.get_or_insert(now);
    }

    /// The read returned. Advances the cursor by what it delivered and
    /// says what the read was.
    fn resume(&mut self, now: Instant, delivered: u64) -> Served {
        let begin = self.position;
        self.position = self.position.saturating_add(delivered);
        let arrived = self.arrived.take();
        debug_assert!(
            arrived.is_some(),
            "a read returned without having been polled"
        );
        Served {
            begin,
            end: self.position,
            arrived: arrived.unwrap_or(now),
            returned: now,
            waited: self
                .pending_since
                .take()
                .map(|since| now.saturating_duration_since(since)),
        }
    }

    fn seek_to(&mut self, position: u64) {
        self.position = position;
        self.pending_since = None;
        self.arrived = None;
    }

    /// The absolute torrent piece `offset` sits in, `None` without a piece
    /// length (no metadata, or a backend without pieces). `file_start` is
    /// the file's offset within the torrent.
    ///
    /// Takes the offset rather than reading the cursor, because the caller
    /// that matters asks *after* [`ReadCursor::resume`] has moved it, about
    /// the offset the read ran from.
    fn piece_of(offset: u64, file_start: u64, piece_length: Option<u64>) -> Option<u64> {
        piece_length
            .filter(|len| *len > 0)
            .map(|len| (file_start.saturating_add(offset)) / len)
    }
}

pub struct FileHandle<H: TorrentHandle> {
    pub size: u64,
    pub name: String,
    pub stream: Box<dyn FileStreamTrait>,
    pub engine: Arc<crate::engine::Engine<H>>,
    /// Which file of the torrent, for the blocked-read log.
    file_idx: usize,
    cursor: ReadCursor,
    /// This reader's key in the engine's waker registry, so a torrent that
    /// is stopped for space can wake a read parked on a piece it will not
    /// download -- see [`Engine::refuse_reads_for_space`].
    ///
    /// [`Engine::refuse_reads_for_space`]: crate::engine::Engine::refuse_reads_for_space
    reader_id: u64,
    /// This read, as the retention owner sees it: its own playhead on the
    /// file's entity, moved by every byte that really goes out and gone
    /// when this handle is. Two handles on one file -- a seek is a second
    /// response on the file still playing -- are two heads, each with a
    /// window at the pass's door, where one head per torrent had the pass
    /// take the piece the other response was inside. `None` for a file the
    /// owner has no entity for ([`Retention::reader_on`]): nothing could
    /// bound it, and its bytes are not remembered.
    ///
    /// [`Retention::reader_on`]: crate::retention::owner::Retention::reader_on
    reader: Option<crate::retention::owner::Reader<crate::engine::TorrentBacking<H>>>,
}

/// What one read of one file is: where in the torrent, where in the file,
/// what the read is for, and how far ahead the backend granted it.
///
/// A struct rather than four more parameters because the last two arrived
/// together and mean nothing apart: the intent is what the owner is told
/// the read is *for* ([`PlaybackIntent::reading`]), and the lookahead is
/// what the backend really granted it, which the owner keeps as the floor
/// no later budget may size the window under.
///
/// [`PlaybackIntent::reading`]: crate::backend::priorities::PlaybackIntent::reading
#[derive(Debug, Clone, Copy)]
pub struct Opening {
    /// Which file of the torrent.
    pub file_idx: usize,
    /// The offset in that file the read starts at.
    pub start_offset: u64,
    /// What the read is for.
    pub intent: crate::backend::priorities::PlaybackIntent,
    /// The stream lookahead the backend granted, in bytes: already the
    /// smaller of the intent's cap and the window's forward reach. See
    /// [`crate::piece_store::Buffering`].
    pub lookahead_bytes: u64,
    /// The viewer's read-ahead choice, which is also how many seconds of
    /// the stream the retention window may buy
    /// ([`BufferProfile::window_seconds`]).
    ///
    /// [`BufferProfile::window_seconds`]: crate::backend::priorities::BufferProfile::window_seconds
    pub buffer: crate::backend::priorities::BufferProfile,
}

impl<H: TorrentHandle> FileHandle<H> {
    /// A handle over `stream`, reading `file_idx` from `start_offset`.
    /// Opened after the engine has installed the file's retention policy
    /// (`Engine::try_get_file_with_intent` does, before it asks the backend
    /// for the stream), so the reader this takes on the file's entity is
    /// there before the first byte is noted to it.
    ///
    /// `intent` reaches the owner as what the read is *for*
    /// ([`PlaybackIntent::reading`]). The owner needs it: a player's read of
    /// the container index at the tail delivers bytes exactly like the
    /// response playing the film, and only the intent tells them apart
    /// before the damage -- the probe's byte claiming the file's head and
    /// the window sliding to the end of the file under a player still at
    /// 0:00.
    ///
    /// `lookahead_bytes` is what the backend really granted this stream --
    /// already the smaller of the intent's cap and the window's forward
    /// reach -- and the owner keeps the largest one open on the file as the
    /// floor no later budget may size the window under
    /// ([`crate::piece_store::Buffering`]).
    ///
    /// And `start_offset` reaches it too, so the read has a head from the
    /// open rather than from its first delivered byte: a reader parked on
    /// the piece it is waiting for is the one the file is being buffered
    /// for, and an entity with no head is one no pass draws a window for.
    ///
    /// [`PlaybackIntent::reading`]: crate::backend::priorities::PlaybackIntent::reading
    pub fn new(
        size: u64,
        name: String,
        stream: Box<dyn FileStreamTrait>,
        engine: Arc<crate::engine::Engine<H>>,
        opening: Opening,
    ) -> Self {
        let Opening {
            file_idx,
            start_offset,
            intent,
            lookahead_bytes,
            buffer,
        } = opening;
        let reader_id = engine.next_reader_id();
        let reader = engine.retention.reader_on(
            &file_idx,
            (file_idx, start_offset),
            intent.reading(),
            crate::piece_store::Buffering {
                lookahead_bytes,
                window_seconds: buffer.window_seconds(),
                committed_seconds: Some(crate::backend::priorities::COMMITTED_SECONDS),
                // The entity's, from its size and the film's length, not
                // this read's to ask for: see `Buffering::bytes_per_second`.
                bytes_per_second: None,
                // The entity's, not this read's: every read of one file
                // shares the same pieces. See `Retention::reader_on`.
                seed: 0,
            },
        );
        Self {
            size,
            name,
            stream,
            engine,
            file_idx,
            cursor: ReadCursor::new(start_offset),
            reader_id,
            reader,
        }
    }

    /// The error a read on a torrent stopped for want of disk space fails
    /// with -- `StorageFull`, so a caller that looks at the kind sees the
    /// device's problem and not the torrent's.
    pub fn stopped_for_space_error() -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::StorageFull,
            "the torrent was stopped for want of disk space",
        )
    }

    /// Log a read that had to wait, once it finally returns.
    ///
    /// Deliberately reported on completion rather than while pending:
    /// `poll_read` is only called when the reader is woken, so a "it has
    /// been blocked for a second" check would fire whenever the runtime
    /// happened to poll again and never for the reads that block longest.
    /// The completion line always fires and carries the real wait.
    fn log_blocked_read(&self, served: Served, waited: Duration) {
        let piece_length = self.engine.handle.piece_length();
        tracing::info!(
            info_hash = %self.engine.info_hash,
            file_idx = self.file_idx,
            offset = served.begin,
            piece = ReadCursor::piece_of(served.begin, 0, piece_length),
            piece_length,
            waited_ms = waited.as_millis() as u64,
            took_ms = served.took().as_millis() as u64,
            stage = "blocked_read",
            "read waited for a piece"
        );
    }
}

impl<H: TorrentHandle> AsyncRead for FileHandle<H> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // A torrent stopped for space is not downloading the piece this
        // read may be about to park on, and the backend wakes a parked read
        // only when its piece arrives. So the engine says whether reads are
        // to fail instead, and a read that does park leaves its waker where
        // the engine can reach it.
        // Stamped before anything can return, so every path out of this
        // function has an arrival to clear and none inherits the last
        // read's. Latched, not assigned: this runs again on every wake-up
        // of a read that parked.
        if !self.cursor.has_arrived() {
            self.cursor.arrive(Instant::now());
        }
        if self.engine.reads_refused() {
            self.cursor.resume(Instant::now(), 0);
            return Poll::Ready(Err(Self::stopped_for_space_error()));
        }
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.stream).poll_read(cx, buf);
        match polled {
            Poll::Pending => {
                self.engine
                    .register_read_waker(self.reader_id, cx.waker().clone());
                self.cursor.park(Instant::now());
                // **What this read is blocked on**, which is the one thing
                // the swarm should be fetching before anything else.
                //
                // A parked read is waiting for exactly one piece: the one
                // under its cursor. Said here, it becomes a promise the pass
                // may not unlink and -- more to the point -- the want-set
                // the pass orders ahead of every window it holds. Nothing
                // said it before: `promises` had one production caller in
                // the whole workspace, the proxy's, so on the torrent side
                // the pass knew what readers were *near* and never what any
                // of them was actually stuck on.
                if let Some(reader) = &self.reader {
                    reader.promises_at((self.file_idx, self.cursor.position));
                }
            }
            Poll::Ready(ref result) => {
                let delivered = if result.is_ok() {
                    buf.filled().len().saturating_sub(before) as u64
                } else {
                    0
                };
                let served = self.cursor.resume(Instant::now(), delivered);
                if let Some(waited) = served.waited
                    && waited >= BLOCKED_READ_LOG_THRESHOLD
                {
                    self.log_blocked_read(served, waited);
                }
                // **Phase A**: what the reads of this file look like, which
                // nothing obeys yet. Fed from here because this is the one
                // place that sees every served read with both of its
                // timestamps, and fed to the engine rather than to anything
                // on this handle because a stream survives the response
                // that was carrying it.
                if delivered > 0 {
                    self.engine.note_read(
                        self.file_idx,
                        self.reader_id,
                        crate::retention::streams::Read {
                            begin: served.begin,
                            end: served.end,
                            arrived: served.arrived,
                            returned: served.returned,
                        },
                    );
                }
                // Where a byte really reached a player from, which is what
                // the retention policy calls the playhead: this read's own,
                // on the file's entity. Written after the read rather than
                // before it, so a read that failed or parked moves nothing.
                if delivered > 0
                    && let Some(reader) = &self.reader
                {
                    let claim = reader.note((self.file_idx, served.end));
                    debug_assert!(
                        claim.is_none(),
                        "a delivered byte claimed a torrent file's turn; the tick is its trigger"
                    );
                }
            }
        }
        polled
    }
}

impl<H: TorrentHandle> Drop for FileHandle<H> {
    fn drop(&mut self) {
        self.engine.forget_read_waker(self.reader_id);
        self.engine.active_streams.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<H: TorrentHandle> AsyncSeek for FileHandle<H> {
    fn start_seek(mut self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut self.stream).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        let polled = Pin::new(&mut self.stream).poll_complete(cx);
        if let Poll::Ready(Ok(position)) = polled {
            self.cursor.seek_to(position);
        }
        polled
    }
}

#[cfg(test)]
mod tests {
    use super::{BLOCKED_READ_LOG_THRESHOLD, ReadCursor};
    use std::time::{Duration, Instant};

    /// The blocked-read line has to name the offset the read is parked on,
    /// which means the cursor has to track what each read delivered -- and
    /// the wait has to be measured from the moment the read first parked,
    /// not from the last time the runtime happened to poll it. A read of a
    /// missing 16 MiB piece waits a minute across many wake-ups.
    #[test]
    fn read_cursor_reports_the_whole_wait_at_the_offset_it_parked_on() {
        let piece = 16 * 1024 * 1024u64;
        let mut cursor = ReadCursor::new(piece);
        assert_eq!(
            ReadCursor::piece_of(cursor.position, 0, Some(piece)),
            Some(1)
        );

        // A served read advances the cursor and reports no wait.
        let t0 = Instant::now();
        cursor.arrive(t0);
        let served = cursor.resume(t0, 4096);
        assert_eq!(served.waited, None);
        assert_eq!(served.begin, piece);
        assert_eq!(served.end, piece + 4096);
        assert_eq!(cursor.position, piece + 4096);

        // A read that parks, is polled again while still parked, and only
        // then completes reports the wait from the first park -- and the
        // offset it ran from, which the cursor has moved past by the time
        // anyone asks.
        cursor.arrive(t0);
        cursor.park(t0);
        cursor.park(t0 + Duration::from_secs(20));
        let served = cursor.resume(t0 + Duration::from_secs(28), 4096);
        let waited = served.waited.expect("the read waited");
        assert_eq!(waited, Duration::from_secs(28));
        assert!(waited >= BLOCKED_READ_LOG_THRESHOLD);
        assert_eq!(
            served.begin,
            piece + 4096,
            "the offset it parked on, not the one the next read starts at"
        );
        assert_eq!(
            ReadCursor::piece_of(served.begin, 0, Some(piece)),
            Some(1),
            "and so the piece it was actually waiting for"
        );
        assert_eq!(cursor.position, piece + 8192);

        // And the next read starts unparked.
        cursor.arrive(Instant::now());
        assert_eq!(cursor.resume(Instant::now(), 0).waited, None);
    }

    /// **One read is stamped once, however many times it is polled.**
    ///
    /// `poll_read` runs again on every wake-up of a read that parked, so a
    /// stamp assigned rather than latched says the read arrived at the
    /// moment it was about to return: a read blocked four seconds reports
    /// having taken none, which is the one number the consumer actually
    /// felt. It is also what a rate measured from the gap between reads
    /// needs, since an arrival that moves puts our own stall inside the
    /// previous read's gap.
    #[test]
    fn one_read_is_stamped_once_however_often_it_is_polled() {
        let t0 = Instant::now();
        let mut cursor = ReadCursor::new(0);

        assert!(!cursor.has_arrived(), "nothing is in flight yet");
        cursor.arrive(t0);
        assert!(cursor.has_arrived());
        // The re-polls of the same read.
        cursor.arrive(t0 + Duration::from_secs(2));
        cursor.arrive(t0 + Duration::from_secs(4));

        let served = cursor.resume(t0 + Duration::from_secs(4), 4096);
        assert_eq!(served.arrived, t0, "the first poll, not the last");
        assert_eq!(served.returned, t0 + Duration::from_secs(4));
        assert_eq!(served.took(), Duration::from_secs(4));
        assert!(
            !cursor.has_arrived(),
            "and the read is done, so the next one stamps its own arrival"
        );
    }

    /// A seek abandons the arrival with the wait: what the consumer asked
    /// for before it seeked is not what it is asking for now, and a stamp
    /// carried across would measure the next read from a read that never
    /// happened.
    #[test]
    fn read_cursor_forgets_an_arrival_on_a_seek() {
        let mut cursor = ReadCursor::new(0);
        cursor.arrive(Instant::now());
        cursor.seek_to(4_000_000_000);
        assert!(!cursor.has_arrived());
    }

    /// A seek moves the cursor outright and abandons any wait: the offset
    /// the previous read parked on is not where the next one will.
    #[test]
    fn read_cursor_follows_a_seek() {
        let mut cursor = ReadCursor::new(0);
        cursor.arrive(Instant::now());
        cursor.park(Instant::now());
        cursor.seek_to(4_000_000_000);
        cursor.arrive(Instant::now());
        assert_eq!(cursor.resume(Instant::now(), 0).waited, None);
        assert_eq!(cursor.position, 4_000_000_000);
        // The piece index is absolute: the file's own offset in the torrent
        // counts, not just the offset within the file.
        let at = cursor.position;
        assert_eq!(
            ReadCursor::piece_of(at, 1_000, Some(1_000_000)),
            Some(4_000)
        );
        assert_eq!(
            ReadCursor::piece_of(at, 0, None),
            None,
            "no metadata, no piece"
        );
        assert_eq!(
            ReadCursor::piece_of(at, 0, Some(0)),
            None,
            "never divides by zero"
        );
    }
}
