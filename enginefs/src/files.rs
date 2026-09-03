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
}

impl<H: TorrentHandle> FileHandle<H> {
    pub fn new(
        size: u64,
        name: String,
        stream: Box<dyn FileStreamTrait>,
        engine: Arc<crate::engine::Engine<H>>,
        file_idx: usize,
        start_offset: u64,
    ) -> Self {
        Self {
            size,
            name,
            stream,
            engine,
            file_idx,
            cursor: ReadCursor::new(start_offset),
        }
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
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.stream).poll_read(cx, buf);
        match polled {
            Poll::Pending => self.cursor.park(Instant::now()),
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
            }
        }
        polled
    }
}

impl<H: TorrentHandle> Drop for FileHandle<H> {
    fn drop(&mut self) {
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
