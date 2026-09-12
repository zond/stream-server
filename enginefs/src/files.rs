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
}

impl ReadCursor {
    fn new(position: u64) -> Self {
        Self {
            position,
            pending_since: None,
        }
    }

    /// The read could not be served yet.
    fn park(&mut self, now: Instant) {
        self.pending_since.get_or_insert(now);
    }

    /// The read returned. Advances the cursor by what it delivered and
    /// yields how long it waited, if it waited at all.
    fn resume(&mut self, now: Instant, delivered: u64) -> Option<Duration> {
        self.position = self.position.saturating_add(delivered);
        self.pending_since
            .take()
            .map(|since| now.saturating_duration_since(since))
    }

    fn seek_to(&mut self, position: u64) {
        self.position = position;
        self.pending_since = None;
    }

    /// The absolute torrent piece the cursor sits in, `None` without a
    /// piece length (no metadata, or a backend without pieces). `file_start`
    /// is the file's offset within the torrent.
    fn piece(&self, file_start: u64, piece_length: Option<u64>) -> Option<u64> {
        piece_length
            .filter(|len| *len > 0)
            .map(|len| (file_start.saturating_add(self.position)) / len)
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
                // Measured, not asked for: see `Buffering::bytes_per_second`.
                bytes_per_second: None,
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
    fn log_blocked_read(&self, waited: Duration) {
        let piece_length = self.engine.handle.piece_length();
        tracing::info!(
            info_hash = %self.engine.info_hash,
            file_idx = self.file_idx,
            offset = self.cursor.position,
            piece = self.cursor.piece(0, piece_length),
            piece_length,
            waited_ms = waited.as_millis() as u64,
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
            }
            Poll::Ready(ref result) => {
                let delivered = if result.is_ok() {
                    buf.filled().len().saturating_sub(before) as u64
                } else {
                    0
                };
                if let Some(waited) = self.cursor.resume(Instant::now(), delivered)
                    && waited >= BLOCKED_READ_LOG_THRESHOLD
                {
                    self.log_blocked_read(waited);
                }
                // Where a byte really reached a player from, which is what
                // the retention policy calls the playhead: this read's own,
                // on the file's entity. Written after the read rather than
                // before it, so a read that failed or parked moves nothing.
                if delivered > 0
                    && let Some(reader) = &self.reader
                {
                    let claim = reader.note((self.file_idx, self.cursor.position));
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
        assert_eq!(cursor.piece(0, Some(piece)), Some(1));

        // A served read advances the cursor and reports no wait.
        assert_eq!(cursor.resume(Instant::now(), 4096), None);
        assert_eq!(cursor.position, piece + 4096);

        // A read that parks, is polled again while still parked, and only
        // then completes reports the wait from the first park.
        let t0 = Instant::now();
        cursor.park(t0);
        cursor.park(t0 + Duration::from_secs(20));
        let waited = cursor
            .resume(t0 + Duration::from_secs(28), 4096)
            .expect("the read waited");
        assert_eq!(waited, Duration::from_secs(28));
        assert!(waited >= BLOCKED_READ_LOG_THRESHOLD);
        assert_eq!(cursor.position, piece + 8192);

        // And the next read starts unparked.
        assert_eq!(cursor.resume(Instant::now(), 0), None);
    }

    /// A seek moves the cursor outright and abandons any wait: the offset
    /// the previous read parked on is not where the next one will.
    #[test]
    fn read_cursor_follows_a_seek() {
        let mut cursor = ReadCursor::new(0);
        cursor.park(Instant::now());
        cursor.seek_to(4_000_000_000);
        assert_eq!(cursor.resume(Instant::now(), 0), None);
        assert_eq!(cursor.position, 4_000_000_000);
        // The piece index is absolute: the file's own offset in the torrent
        // counts, not just the offset within the file.
        assert_eq!(cursor.piece(1_000, Some(1_000_000)), Some(4_000));
        assert_eq!(cursor.piece(0, None), None, "no metadata, no piece");
        assert_eq!(cursor.piece(0, Some(0)), None, "never divides by zero");
    }
}
