use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use tokio::fs::{File, OpenOptions};
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, watch};

#[derive(Clone, Debug)]
struct StateSnapshot {
    written_bytes: u64,
    is_complete: bool,
    error: Option<String>,
}

/// How long an extraction goes on writing with nobody reading before it
/// gives up (see [`Readers`]).
///
/// Long enough to bridge a seek -- the player drops one connection and
/// opens the next a moment later, and an extraction that stopped in
/// between would have to start over from the archive -- and long enough
/// for a player that pauses with its connection closed to come back from a
/// short pause. Short enough that a player that has gone for good costs
/// half a minute of decoding and no more, rather than a whole member's
/// worth.
pub const ABANDONED_AFTER: Duration = Duration::from_secs(30);

/// How many [`ProgressiveReader`]s are open on a cache right now. The
/// writers watch it: an extraction nobody is reading is an extraction
/// nobody wants, and once it has been that way for [`ABANDONED_AFTER`] the
/// next write fails and the extraction unwinds. Before this a member was
/// decoded to its end however early the player left -- the whole of a
/// 4 GB member, on a slow SoC, for a request that lasted a second.
#[derive(Default)]
struct Readers(AtomicUsize);

impl Readers {
    fn any(&self) -> bool {
        self.0.load(Ordering::SeqCst) > 0
    }
}

/// The writer's side of the readers count: when it last saw a reader, and
/// how long it waits for one.
struct Abandonment {
    readers: Arc<Readers>,
    unread_since: Option<Instant>,
    after: Duration,
}

impl Abandonment {
    fn new(readers: Arc<Readers>, after: Duration) -> Self {
        Self {
            readers,
            unread_since: None,
            after,
        }
    }

    /// `Err` once nobody has been reading for `after`.
    fn check(&mut self) -> io::Result<()> {
        if self.readers.any() {
            self.unread_since = None;
            return Ok(());
        }
        let since = *self.unread_since.get_or_insert_with(Instant::now);
        if since.elapsed() >= self.after {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "extraction abandoned: nothing has read it for a while",
            ))
        } else {
            Ok(())
        }
    }
}

/// The core cache controller
///
/// The backing file is a [`NamedTempFile`] owned by the cache alone (shared
/// only among clones of it), so its name is unlinked from the directory the
/// moment the last clone drops -- with the `ArchiveSource` that kept it, in
/// practice. The [`CacheWriter`] does not own the name: it holds a handle of
/// its own onto the file (see its `handle` field), which is all a writer
/// moved into a background extraction task that may not start until later
/// needs, and which is why a name can be gone while its bytes are still
/// being written. The archive handlers hand the cache to the caller, who
/// keeps it for as long as the member should stay extracted (`ArchiveSource`
/// keeps one per member for the life of the session) and takes readers from
/// it.
#[derive(Clone)]
pub struct ProgressiveCache {
    state_rx: watch::Receiver<StateSnapshot>,
    temp_path: PathBuf,
    /// The one owner of the file's name: unlinked when the last clone drops.
    _temp_file_handle: Arc<NamedTempFile>,
    total_size: Option<u64>,
    notify: Arc<Notify>,
    readers: Arc<Readers>,
}

impl ProgressiveCache {
    /// Create a new ProgressiveCache in a specific directory -- the archive
    /// scratch directory under the cache root (`CacheConfig::scratch_dir`),
    /// created if missing. There is deliberately no constructor for the
    /// system temp dir: what is written here is cache, and it belongs on
    /// the volume this server counts and caps rather than on one it knows
    /// nothing about.
    pub async fn new_in_dir(
        dir: &std::path::Path,
        total_size: Option<u64>,
    ) -> io::Result<(Self, CacheWriter)> {
        // On the blocking pool: a `stat`, a `mkdir` and an `open` on the
        // cache volume, which on the flash of a slow device or a mount that
        // has stopped answering park whatever reactor worker runs them.
        let dir = dir.to_path_buf();
        let temp_file = tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&dir)?;
            tempfile::Builder::new()
                .prefix("archive_extract_")
                .tempfile_in(&dir)
        })
        .await
        .map_err(io::Error::other)??;
        Self::from_temp_file(temp_file, total_size).await
    }

    async fn from_temp_file(
        temp_file: NamedTempFile,
        total_size: Option<u64>,
    ) -> io::Result<(Self, CacheWriter)> {
        let temp_path = temp_file.path().to_path_buf();
        let writer_handle = temp_file.as_file().try_clone()?;

        let initial_state = StateSnapshot {
            written_bytes: 0,
            is_complete: false,
            error: None,
        };

        let (tx, rx) = watch::channel(initial_state);
        let notify = Arc::new(Notify::new());
        let readers = Arc::new(Readers::default());

        // Open async file for writer
        let writer_file = OpenOptions::new()
            .write(true)
            .create(false) // Created by NamedTempFile
            .open(&temp_path)
            .await?;

        let writer = CacheWriter {
            state_tx: tx.clone(),
            file: writer_file,
            _video_file_size: total_size,
            notify: notify.clone(),
            handle: writer_handle,
            abandonment: Abandonment::new(readers.clone(), ABANDONED_AFTER),
        };

        Ok((
            ProgressiveCache {
                state_rx: rx,
                temp_path,
                _temp_file_handle: Arc::new(temp_file),
                total_size,
                notify,
                readers,
            },
            writer,
        ))
    }

    /// A reader from the start of the member. Counted: while it lives the
    /// extraction is wanted (see [`Readers`]).
    pub async fn reader(&self) -> io::Result<ProgressiveReader> {
        let file = File::open(&self.temp_path).await?;
        self.readers.0.fetch_add(1, Ordering::SeqCst);
        Ok(ProgressiveReader {
            state_rx: self.state_rx.clone(),
            file,
            pos: 0,
            total_size: self.total_size,
            notify: self.notify.clone(),
            wait: None,
            readers: self.readers.clone(),
        })
    }

    /// Whether the extraction ended in an error -- decoding failed, or it
    /// was abandoned -- so a reader taken now would only be told so. A
    /// holder keeping caches around replaces one that has.
    pub fn is_failed(&self) -> bool {
        self.state_rx.borrow().error.is_some()
    }
}

pub struct CacheWriter {
    state_tx: watch::Sender<StateSnapshot>,
    file: File,
    _video_file_size: Option<u64>,
    notify: Arc<Notify>,
    /// The writer's own handle onto the file, dup'd from the temp file when
    /// the cache was made, and the source of handle dups for
    /// [`Self::try_clone_sync`]. A handle and not a share in the
    /// [`NamedTempFile`]: the writer is typically moved into a background
    /// task that starts after the cache (and often the reader) has been
    /// dropped, and it must not find its file gone -- a handle it already
    /// holds cannot be -- but it must not keep the *name* either. A writer
    /// that co-owned the name kept it in the directory until its task
    /// unwound, which is after `finish` has told every reader the member is
    /// complete, so a holder that dropped the cache the instant its readers
    /// were done could still find the file listed (a test did, on a loaded
    /// CI runner). Now the name goes with the cache, synchronously; a writer
    /// still running writes into an unlinked file until the readers count
    /// tells it nobody is reading ([`ABANDONED_AFTER`]), and the bytes are
    /// reclaimed when it drops the handle.
    handle: std::fs::File,
    abandonment: Abandonment,
}

impl AsyncWrite for CacheWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Err(abandoned) = self.abandonment.check() {
            self.set_error(abandoned.to_string());
            return Poll::Ready(Err(abandoned));
        }
        let poll = Pin::new(&mut self.file).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = poll
            && n > 0
        {
            self.state_tx.send_modify(|state| {
                state.written_bytes += n as u64;
            });
            self.notify.notify_waiters();
        }
        poll
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_shutdown(cx)
    }
}

impl CacheWriter {
    /// Mark the stream complete.
    ///
    /// This flushes the writer's tokio `File` FIRST, then publishes
    /// `is_complete`. The flush is load-bearing: `poll_write` bumps
    /// `written_bytes` as soon as tokio *accepts* (buffers) a write, before the
    /// data has been handed to the OS and made visible to the reader's separate
    /// `File` handle. If we set `is_complete` while the tail was still buffered,
    /// a reader that observed `is_complete` with `pos < written_bytes` would hit
    /// EOF on bytes it cannot yet see and silently truncate the stream. Flushing
    /// before completion guarantees every counted byte is physically visible by
    /// the time `is_complete` is published.
    ///
    /// The reader leans on that guarantee rather than only on the
    /// notification below: a zero-byte read taken after completion can only
    /// be a stale answer, so it retries instead of waiting -- see
    /// `ProgressiveReader::poll_read`, and
    /// `a_reader_promised_invisible_bytes_waits_on_the_disk_not_on_the_writer`
    /// for what waiting costs once this notification has been sent.
    pub async fn finish(&mut self) {
        if let Err(e) = self.file.flush().await {
            self.state_tx.send_modify(|state| {
                state.error = Some(format!("flush on finish failed: {e}"));
                state.is_complete = true;
            });
            self.notify.notify_waiters();
            return;
        }
        self.state_tx.send_modify(|state| {
            state.is_complete = true;
        });
        self.notify.notify_waiters();
    }

    pub fn set_error(&self, err: String) {
        self.state_tx.send_modify(|state| {
            state.error = Some(err);
            state.is_complete = true;
        });
        self.notify.notify_waiters();
    }

    /// Create a synchronous writer that shares the same state.
    /// Useful for legacy/sync libraries like 7z or unrar.
    ///
    /// The file handle is duplicated from the one this writer holds rather
    /// than re-opened by path. Re-opening by path raced the cache's drop: a
    /// blocking extraction task that started after `open_file` had returned
    /// (and dropped the `ProgressiveCache`) found the temp file already
    /// unlinked — `No such file or directory` on Linux, `Access is denied`
    /// on Windows where the delete is pending on the open handles. A dup of
    /// an open handle cannot fail that way, whether or not the name is still
    /// there. (The dup shares its file offset with the writer's handle,
    /// which nothing else writes through.)
    pub fn try_clone_sync(&self) -> io::Result<SyncCacheWriter> {
        let file = self.handle.try_clone()?;

        Ok(SyncCacheWriter {
            state_tx: self.state_tx.clone(),
            file,
            notify: self.notify.clone(),
            abandonment: Abandonment::new(self.abandonment.readers.clone(), self.abandonment.after),
        })
    }

    /// How long this writer (and sync clones taken after this) keeps writing
    /// with no reader before giving up; [`ABANDONED_AFTER`] unless a test
    /// says otherwise.
    #[cfg(test)]
    pub fn abandon_after(&mut self, after: Duration) {
        self.abandonment.after = after;
    }
}

/// A synchronous writer that updates the Async Cache state
pub struct SyncCacheWriter {
    state_tx: watch::Sender<StateSnapshot>,
    file: std::fs::File,
    notify: Arc<Notify>,
    abandonment: Abandonment,
}

impl SyncCacheWriter {
    /// `Err` -- and the cache failed with it -- once nobody has read the
    /// cache for [`ABANDONED_AFTER`], for work towards the member that
    /// writes nothing: a decoder draining the entries stored before it.
    /// Every write checks the same thing itself.
    pub fn check_abandoned(&mut self) -> io::Result<()> {
        if let Err(abandoned) = self.abandonment.check() {
            self.set_error(abandoned.to_string());
            return Err(abandoned);
        }
        Ok(())
    }

    pub fn finish(&self) {
        self.state_tx.send_modify(|state| {
            state.is_complete = true;
        });
        self.notify.notify_waiters();
    }

    pub fn set_error(&self, err: String) {
        self.state_tx.send_modify(|state| {
            state.error = Some(err);
            state.is_complete = true;
        });
        self.notify.notify_waiters();
    }
}

impl std::io::Write for SyncCacheWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.check_abandoned()?;
        let n = self.file.write(buf)?;
        if n > 0 {
            self.state_tx.send_modify(|state| {
                state.written_bytes += n as u64;
            });
            self.notify.notify_waiters();
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

pub struct ProgressiveReader {
    state_rx: watch::Receiver<StateSnapshot>,
    file: File,
    pos: u64,
    total_size: Option<u64>,
    notify: Arc<Notify>,
    /// In-flight wait for a writer notification. This future owns a clone of
    /// `notify` and MUST be kept across `poll_read` calls that return
    /// `Poll::Pending`: dropping a `Notified` future deregisters its waker, so
    /// a locally created-and-dropped future would miss every
    /// `notify_waiters()` from the writer and the reader would hang forever.
    wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    /// Counted in by `ProgressiveCache::reader`, out on drop.
    readers: Arc<Readers>,
}

impl Drop for ProgressiveReader {
    fn drop(&mut self) {
        self.readers.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl AsyncRead for ProgressiveReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            // 1. Arm a waiter BEFORE snapshotting state, so a notification
            // that fires between the snapshot and returning Pending re-wakes
            // this task instead of being lost.
            if self.wait.is_none() {
                let notify = self.notify.clone();
                self.wait = Some(Box::pin(async move { notify.notified().await }));
            }
            let mut wait = self.wait.take().expect("wait future just set");
            let wait_ready = wait.as_mut().poll(cx).is_ready();
            if !wait_ready {
                // Keep the registration alive across Pending returns.
                self.wait = Some(wait);
            }

            // 2. Snapshot state
            let current_state = self.state_rx.borrow().clone();

            // Check errors
            if let Some(err) = current_state.error {
                return Poll::Ready(Err(io::Error::other(err)));
            }

            // Check if data available
            if self.pos < current_state.written_bytes {
                let available = current_state.written_bytes - self.pos;
                let needed = buf.remaining().min(available as usize);

                if needed == 0 && buf.remaining() == 0 {
                    return Poll::Ready(Ok(()));
                }

                // Read from file
                // We must be careful: if we read more than is flushed to disk, we might get 0 bytes or blocking?
                // But `written_bytes` is updated after write success.
                // However, internal buffering of `File` might mean it's not on disk yet?
                // `File` (tokio) is usually unbuffered direct syscalls (mostly).
                // Let's assume it's safe.

                let mut sub_buf = buf.take(needed);
                let poll = Pin::new(&mut self.file).poll_read(cx, &mut sub_buf);

                match poll {
                    Poll::Ready(Ok(())) => {
                        let bytes_read = sub_buf.filled().len();
                        // `sub_buf` borrows the parent's unfilled memory, but
                        // filling it does not advance the parent `buf`, so
                        // propagate the progress manually.
                        // SAFETY: the file read initialized `bytes_read` bytes
                        // of the parent's unfilled region through `sub_buf`.
                        unsafe { buf.assume_init(bytes_read) };
                        buf.advance(bytes_read);
                        if bytes_read == 0 && needed > 0 {
                            // The state said data was available and the file
                            // gave nothing. That zero is not a statement
                            // about the file: the read behind it can have
                            // been issued before the writer's buffered bytes
                            // landed and be answered from that operation
                            // now, so it describes a file that no longer
                            // exists.
                            //
                            // Once the writer is complete, retry rather than
                            // wait. `finish` flushes before it publishes
                            // completion, so every counted byte is
                            // physically there and one fresh read finds it
                            // -- while waiting is waiting for a notification
                            // that has already been sent, since the
                            // `notify_waiters` beside that completion is the
                            // last word the writer ever says and a waiter
                            // armed after it hears nothing again. That is a
                            // permanent hang, and it is what
                            // `finish_right_after_buffered_write_reads_full_tail`
                            // saw once in 250 workspace runs as a reader
                            // that timed out.
                            if current_state.is_complete {
                                continue;
                            }
                            // While the writer is still going there is
                            // always another notification coming -- its next
                            // write, its finish, or its error -- so the wait
                            // below is answered, and retrying instead would
                            // only spin against the disk until the buffered
                            // write lands.
                        } else {
                            self.pos += bytes_read as u64;
                            return Poll::Ready(Ok(()));
                        }
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }

            // We reach here with no readable bytes: either `pos >=
            // written_bytes` (nothing produced beyond what we've read) or `pos <
            // written_bytes` but the file read returned 0 (the writer counted
            // bytes into `written_bytes` before they became visible on our File
            // handle).
            //
            // EOF is only correct once the reader has consumed EVERY byte the
            // writer has produced. Signal it only when `pos >= written_bytes`
            // AND the writer is complete. While `pos < written_bytes`, the tail
            // exists but isn't visible yet; `finish()` flushes before marking
            // the stream complete, so those bytes are guaranteed to appear —
            // wait for the writer's next notification and retry instead of
            // returning a premature EOF that truncates the still-invisible tail.
            if self.pos >= current_state.written_bytes && current_state.is_complete {
                return Poll::Ready(Ok(()));
            }

            // Wait for a writer notification. The waiter armed at the top of
            // the loop is already registered; if it fired, retry immediately,
            // otherwise park until the writer's next notify_waiters().
            if wait_ready {
                continue;
            }
            return Poll::Pending;
        }
    }
}

impl AsyncSeek for ProgressiveReader {
    fn start_seek(mut self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        let pos = match position {
            io::SeekFrom::Start(p) => p,
            io::SeekFrom::End(p) => {
                if let Some(total) = self.total_size {
                    if p < 0 {
                        total.saturating_sub(p.unsigned_abs())
                    } else {
                        total + p as u64
                    }
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "SeekFrom::End requires known total size",
                    ));
                }
            }
            io::SeekFrom::Current(p) => {
                let current = self.pos as i64;
                let new_p = current + p;
                if new_p < 0 {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "Negative seek"));
                }
                new_p as u64
            }
        };

        Pin::new(&mut self.file).start_seek(io::SeekFrom::Start(pos))
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let poll = Pin::new(&mut self.file).poll_complete(cx);
        if let Poll::Ready(Ok(new_pos)) = poll {
            self.pos = new_pos;
        }
        poll
    }
}

#[cfg(test)]
mod tests {
    use super::ProgressiveCache;
    use std::io::SeekFrom;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    #[tokio::test]
    async fn reads_all_written_bytes_in_order() {
        let dir = tempfile::tempdir().unwrap();
        // Writer produces the whole stream (flushed so it is physically on disk)
        // and finishes; the reader must return every byte in order and then hit
        // EOF. Driven sequentially in one task for a simple, deterministic check
        // (finish() flushes before publishing completion, so no explicit flush is
        // required for correctness here).
        let (cache, mut writer) = ProgressiveCache::new_in_dir(dir.path(), None)
            .await
            .unwrap();
        let mut reader = cache.reader().await.unwrap();

        writer.write_all(b"hello ").await.unwrap();
        writer.write_all(b"world").await.unwrap();
        writer.flush().await.unwrap();
        writer.finish().await;

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"hello world");
    }

    #[tokio::test]
    async fn finish_right_after_buffered_write_reads_full_tail() {
        let dir = tempfile::tempdir().unwrap();
        // Regression for the tail-truncation race: the async writer bumps
        // `written_bytes` when tokio *buffers* a write, before those bytes are
        // flushed to disk. When `finish()` lands right after such a buffered
        // write and a reader is racing to catch up, the reader could observe
        // `is_complete` while `pos < written_bytes`, hit EOF on the not-yet-
        // visible tail, and silently drop the ending of the stream.
        //
        // This test deliberately does NOT flush before finish() — finish() must
        // make the buffered tail visible — and runs the writer and reader
        // concurrently across many iterations to reliably surface the ~1-in-N
        // interleaving. It must read back the FULL payload with no truncation
        // and no UnexpectedEof.
        const PAYLOAD_LEN: usize = 512 * 1024;
        let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i % 251) as u8).collect();

        for iter in 0..250 {
            let (cache, mut writer) =
                ProgressiveCache::new_in_dir(dir.path(), Some(PAYLOAD_LEN as u64))
                    .await
                    .unwrap();
            let mut reader = cache.reader().await.unwrap();

            let payload_for_writer = payload.clone();
            let writer_task = tokio::spawn(async move {
                writer.write_all(&payload_for_writer).await.unwrap();
                // No explicit flush here on purpose: finish() is responsible for
                // making every counted byte visible before completing.
                writer.finish().await;
            });

            let reading = tokio::spawn(async move {
                let mut out = Vec::new();
                reader.read_to_end(&mut out).await.map(|_| out)
            });

            // The writer runs to its own end and not to a clock. It is the
            // only thing that can still produce a byte or a completion, and
            // how long it takes on a loaded machine says nothing about the
            // race under test.
            writer_task.await.unwrap();

            // So whatever the reader is still waiting for is a wake-up and
            // nothing else. The bound below guards against one that never
            // comes -- which is a real thing this reader could do, and did:
            // see
            // `a_reader_promised_invisible_bytes_waits_on_the_disk_not_on_the_writer`.
            // It is not a measure of how long the machine took, and the
            // assertions are the payload.
            let out = tokio::time::timeout(std::time::Duration::from_secs(10), reading)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "iteration {iter}: the writer has finished and the reader is still \
                         waiting, so it is waiting for a wake-up that is not coming"
                    )
                })
                .unwrap()
                .unwrap_or_else(|e| panic!("iteration {iter}: read failed: {e}"));

            assert_eq!(
                out.len(),
                PAYLOAD_LEN,
                "iteration {iter}: tail truncated ({} of {PAYLOAD_LEN} bytes)",
                out.len(),
            );
            assert_eq!(out, payload, "iteration {iter}: content mismatch");
        }
    }

    /// A waker a test can wait on, so "parked on something that will fire"
    /// can be told from "parked for ever". `notify_one` and not
    /// `notify_waiters`, because a wake that lands before the test gets
    /// round to waiting has to keep.
    #[derive(Default)]
    struct Woken(tokio::sync::Notify);

    impl std::task::Wake for Woken {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.notify_one();
        }

        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.0.notify_one();
        }
    }

    /// **A reader promised bytes it cannot see yet waits on the disk, not on
    /// the writer.**
    ///
    /// `written_bytes` counts a write the moment tokio *buffers* it, so a
    /// reader is routinely promised bytes its own handle cannot see -- once
    /// per iteration of the tail test above, measured. The read it issues
    /// for them can be answered with zero bytes by an operation that was
    /// spawned before those bytes landed, and that zero is not a statement
    /// about the file: it is an observation from before the flush.
    ///
    /// Treating it as a reason to wait for the writer's next notification is
    /// only safe while there is going to be one. `finish` flushes and then
    /// publishes completion, and its `notify_waiters` is the last word the
    /// writer ever says -- a waiter armed after it hears nothing, ever, and
    /// the read hangs for as long as the process lives. That is what the
    /// tail test above caught once in 250 workspace runs as "reader timed
    /// out", ten seconds into a read whose worst honest time in the same
    /// binary is ten milliseconds.
    ///
    /// The state is built by hand because the interleaving that produces it
    /// is a few instructions wide -- the writer's flush landing between the
    /// reader consuming its last notification and the reader consuming the
    /// stale zero -- and no scheduler can be asked for it.
    #[tokio::test]
    async fn a_reader_promised_invisible_bytes_waits_on_the_disk_not_on_the_writer() {
        use std::io::Write as _;

        let dir = tempfile::tempdir().unwrap();
        let (cache, writer) = ProgressiveCache::new_in_dir(dir.path(), None)
            .await
            .unwrap();
        let mut reader = cache.reader().await.unwrap();

        // The writer's last word: five bytes counted, the stream complete,
        // and the notification for both already sent -- to nobody, since no
        // reader has armed a waiter yet. The bytes are not on the disk,
        // which is exactly what a counted-but-buffered write looks like from
        // the reader's side.
        writer.state_tx.send_modify(|state| {
            state.written_bytes = 5;
            state.is_complete = true;
        });
        writer.notify.notify_waiters();

        let woken = std::sync::Arc::new(Woken::default());
        let waker = std::task::Waker::from(woken.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        let mut dst = [0u8; 5];

        // The first read goes to the disk and finds nothing there.
        let mut buf = tokio::io::ReadBuf::new(&mut dst);
        assert!(
            std::pin::Pin::new(&mut reader)
                .poll_read(&mut cx, &mut buf)
                .is_pending(),
            "there is nothing on the disk to read yet"
        );
        woken.0.notified().await;

        // And the second consumes its answer: zero bytes, for a file the
        // writer says has five in it.
        let mut buf = tokio::io::ReadBuf::new(&mut dst);
        assert!(
            std::pin::Pin::new(&mut reader)
                .poll_read(&mut cx, &mut buf)
                .is_pending(),
            "a stale zero is not the end of a stream that has five bytes in it"
        );

        // The bytes land, with no notification at all -- which is what a
        // flush completing after the writer's last `notify_waiters` looks
        // like from here.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&cache.temp_path)
            .unwrap()
            .write_all(b"hello")
            .unwrap();

        // A reader parked on the writer would never hear about them. This
        // one is parked on a read of its own, so every wake is one it asked
        // for and it gets the tail it was promised. Bounded because the
        // failure being guarded against is a wake that never comes, and the
        // wait is the only way to see one; the assertion is the five bytes.
        loop {
            tokio::time::timeout(std::time::Duration::from_secs(10), woken.0.notified())
                .await
                .expect(
                    "the reader is parked on a notification the writer has already sent, \
                     so nothing will ever wake it again",
                );
            let mut buf = tokio::io::ReadBuf::new(&mut dst);
            if let std::task::Poll::Ready(res) =
                std::pin::Pin::new(&mut reader).poll_read(&mut cx, &mut buf)
            {
                res.unwrap();
                assert_eq!(buf.filled(), b"hello", "the promised tail, in full");
                break;
            }
        }
    }

    #[tokio::test]
    async fn reader_reads_appended_bytes_after_catching_up_to_eof() {
        let dir = tempfile::tempdir().unwrap();
        // The reader drains all currently-written bytes (reaching the file's
        // physical end), then the writer appends more and finishes. The reader
        // must go on to read the appended tail rather than stopping at the
        // earlier end. Guards the grow-after-EOF continuation. (The dedicated
        // finish_right_after_buffered_write_reads_full_tail test covers the
        // finish-without-explicit-flush path that the truncation fix resolved.)
        let (cache, mut writer) = ProgressiveCache::new_in_dir(dir.path(), None)
            .await
            .unwrap();
        let mut reader = cache.reader().await.unwrap();

        writer.write_all(b"12345").await.unwrap();
        writer.flush().await.unwrap();
        let mut first = [0u8; 5];
        reader.read_exact(&mut first).await.unwrap();
        assert_eq!(&first, b"12345");

        writer.write_all(b"67890").await.unwrap();
        writer.flush().await.unwrap();
        writer.finish().await;

        let mut second = [0u8; 5];
        reader.read_exact(&mut second).await.unwrap();
        assert_eq!(&second, b"67890");
    }

    /// The extraction files under `dir`.
    fn extractions(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect()
    }

    #[tokio::test]
    async fn sync_writer_outlives_dropped_cache() {
        let dir = tempfile::tempdir().unwrap();
        // Regression for the "Failed to open cache writer: No such file or
        // directory / Access is denied" flake: a caller with nothing to keep
        // the cache in (`OpenedMember::into_reader`) takes the reader and
        // drops the `ProgressiveCache` immediately, while the writer is moved
        // into a blocking extraction task that may start later. The writer
        // must still be able to produce a sync clone and stream into the file
        // after the cache is gone -- through a handle of its own, because the
        // name is the cache's alone and is unlinked with it, not kept until
        // the writer's task happens to unwind.
        let (cache, writer) = ProgressiveCache::new_in_dir(dir.path(), Some(5))
            .await
            .unwrap();
        let mut reader = cache.reader().await.unwrap();
        assert_eq!(extractions(dir.path()).len(), 1);
        drop(cache);
        assert!(
            extractions(dir.path()).is_empty(),
            "the name goes with the cache, whatever the writer is up to"
        );

        let mut sync = writer
            .try_clone_sync()
            .expect("sync writer clone after the cache was dropped");
        std::io::Write::write_all(&mut sync, b"hello").unwrap();
        sync.finish();

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"hello");
    }

    #[tokio::test]
    async fn set_error_propagates_to_reader() {
        let dir = tempfile::tempdir().unwrap();
        let (cache, writer) = ProgressiveCache::new_in_dir(dir.path(), None)
            .await
            .unwrap();
        let mut reader = cache.reader().await.unwrap();

        writer.set_error("boom".into());

        let mut buf = [0u8; 4];
        let err = reader.read(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert!(err.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn seek_from_end_lands_and_reads_from_offset() {
        let dir = tempfile::tempdir().unwrap();
        let (cache, mut writer) = ProgressiveCache::new_in_dir(dir.path(), Some(10))
            .await
            .unwrap();
        let mut reader = cache.reader().await.unwrap();

        writer.write_all(b"0123456789").await.unwrap();
        writer.flush().await.unwrap();
        writer.finish().await;

        let pos = reader.seek(SeekFrom::End(-4)).await.unwrap();
        assert_eq!(pos, 6, "SeekFrom::End(-4) with total 10 lands at 6");

        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(
            &buf, b"6789",
            "pos tracked so the read starts at the sought offset"
        );
    }

    #[tokio::test]
    async fn seek_from_end_without_total_size_errors() {
        let dir = tempfile::tempdir().unwrap();
        let (cache, _writer) = ProgressiveCache::new_in_dir(dir.path(), None)
            .await
            .unwrap();
        let mut reader = cache.reader().await.unwrap();

        let err = reader.seek(SeekFrom::End(-1)).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn seek_current_negative_past_zero_errors() {
        let dir = tempfile::tempdir().unwrap();
        let (cache, _writer) = ProgressiveCache::new_in_dir(dir.path(), None)
            .await
            .unwrap();
        let mut reader = cache.reader().await.unwrap();

        let err = reader.seek(SeekFrom::Current(-5)).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    /// A short grace for the abandonment tests: real time, since the sync
    /// writer runs off the runtime and cannot see a paused clock. The loops
    /// below are bounded, not timed -- a regression fails, never hangs.
    const GRACE: std::time::Duration = std::time::Duration::from_millis(50);
    const BOUND: std::time::Duration = std::time::Duration::from_secs(30);

    /// An extraction whose last reader has gone stops: once the grace has
    /// passed the next write fails, through both the async writer and a
    /// sync clone of it, and the cache reports the failure so a holder
    /// replaces it rather than handing out readers onto a dead extraction.
    #[tokio::test]
    async fn a_writer_nobody_reads_from_gives_up_after_the_grace() {
        let dir = tempfile::tempdir().unwrap();
        let (cache, mut writer) = ProgressiveCache::new_in_dir(dir.path(), None)
            .await
            .unwrap();
        writer.abandon_after(GRACE);
        let reader = cache.reader().await.unwrap();
        writer.write_all(b"read").await.expect("a reader is there");
        drop(reader);
        assert!(!cache.is_failed());

        let deadline = std::time::Instant::now() + BOUND;
        let err = loop {
            match writer.write_all(b"unread").await {
                Ok(()) => assert!(
                    std::time::Instant::now() < deadline,
                    "the writer never gave up"
                ),
                Err(err) => break err,
            }
            tokio::time::sleep(GRACE / 5).await;
        };
        assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
        assert!(cache.is_failed(), "the failure is recorded on the cache");

        // A sync clone watches the same readers with a clock of its own: its
        // first write starts the grace, and with nobody reading it gives up
        // the same way.
        let mut sync = writer.try_clone_sync().unwrap();
        let deadline = std::time::Instant::now() + BOUND;
        let err = loop {
            match std::io::Write::write_all(&mut sync, b"still unread") {
                Ok(()) => assert!(
                    std::time::Instant::now() < deadline,
                    "the sync writer never gave up"
                ),
                Err(err) => break err,
            }
            std::thread::sleep(GRACE / 5);
        };
        assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
    }

    /// While a reader is open the writer keeps going however slowly the
    /// reader reads, and a reader that arrives before the grace is up
    /// resets it -- a seek, which is one connection closing and another
    /// opening, does not cost the extraction.
    #[tokio::test]
    async fn a_reader_keeps_the_writer_going() {
        let dir = tempfile::tempdir().unwrap();
        let (cache, mut writer) = ProgressiveCache::new_in_dir(dir.path(), None)
            .await
            .unwrap();
        writer.abandon_after(GRACE);
        let reader = cache.reader().await.unwrap();
        for _ in 0..4 {
            tokio::time::sleep(GRACE).await;
            writer.write_all(b"x").await.expect("a reader is open");
        }

        // Gone, then back before the grace is up.
        drop(reader);
        tokio::time::sleep(GRACE / 2).await;
        let _next = cache.reader().await.unwrap();
        tokio::time::sleep(GRACE).await;
        writer.write_all(b"y").await.expect("the new reader counts");
        assert!(!cache.is_failed());
    }
}
