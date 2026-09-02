use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
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

/// The core cache controller
#[derive(Clone)]
pub struct ProgressiveCache {
    state_rx: watch::Receiver<StateSnapshot>,
    temp_path: PathBuf,
    // Keep the temp file struct alive
    _temp_file_handle: Arc<NamedTempFile>,
    total_size: Option<u64>,
    notify: Arc<Notify>,
}

impl ProgressiveCache {
    /// Create a new ProgressiveCache using system temp directory
    pub async fn new(total_size: Option<u64>) -> io::Result<(Self, CacheWriter)> {
        // We use std NamedTempFile to create the path and handle, but open with tokio
        let temp_file = NamedTempFile::new()?;
        Self::from_temp_file(temp_file, total_size).await
    }

    /// Create a new ProgressiveCache in a specific directory
    /// This allows respecting the user's cache_root setting
    pub async fn new_in_dir(
        dir: &std::path::Path,
        total_size: Option<u64>,
    ) -> io::Result<(Self, CacheWriter)> {
        // Ensure directory exists
        if !dir.exists() {
            std::fs::create_dir_all(dir)?;
        }
        let temp_file = tempfile::Builder::new()
            .prefix("archive_extract_")
            .tempfile_in(dir)?;
        Self::from_temp_file(temp_file, total_size).await
    }

    async fn from_temp_file(
        temp_file: NamedTempFile,
        total_size: Option<u64>,
    ) -> io::Result<(Self, CacheWriter)> {
        let temp_path = temp_file.path().to_path_buf();
        let handle = Arc::new(temp_file);

        let initial_state = StateSnapshot {
            written_bytes: 0,
            is_complete: false,
            error: None,
        };

        let (tx, rx) = watch::channel(initial_state);
        let notify = Arc::new(Notify::new());

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
            path: temp_path.clone(),
        };

        Ok((
            ProgressiveCache {
                state_rx: rx,
                temp_path,
                _temp_file_handle: handle,
                total_size,
                notify,
            },
            writer,
        ))
    }

    pub async fn reader(&self) -> io::Result<ProgressiveReader> {
        let file = File::open(&self.temp_path).await?;
        Ok(ProgressiveReader {
            state_rx: self.state_rx.clone(),
            file,
            pos: 0,
            total_size: self.total_size,
            notify: self.notify.clone(),
            wait: None,
        })
    }
}

pub struct CacheWriter {
    state_tx: watch::Sender<StateSnapshot>,
    file: File,
    _video_file_size: Option<u64>,
    notify: Arc<Notify>,
    path: PathBuf, // kept for sync writer creation
}

impl AsyncWrite for CacheWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
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
    pub fn try_clone_sync(&self) -> io::Result<SyncCacheWriter> {
        let file = std::fs::OpenOptions::new().write(true).open(&self.path)?;

        Ok(SyncCacheWriter {
            state_tx: self.state_tx.clone(),
            file,
            notify: self.notify.clone(),
        })
    }
}

/// A synchronous writer that updates the Async Cache state
pub struct SyncCacheWriter {
    state_tx: watch::Sender<StateSnapshot>,
    file: std::fs::File,
    notify: Arc<Notify>,
}

impl SyncCacheWriter {
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
                            // The state said data was available but the file
                            // returned EOF (e.g. write not yet visible). Fall
                            // through to the completion check / waiter below.
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
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    #[tokio::test]
    async fn reads_all_written_bytes_in_order() {
        // Writer produces the whole stream (flushed so it is physically on disk)
        // and finishes; the reader must return every byte in order and then hit
        // EOF. Driven sequentially in one task for a simple, deterministic check
        // (finish() flushes before publishing completion, so no explicit flush is
        // required for correctness here).
        let (cache, mut writer) = ProgressiveCache::new(None).await.unwrap();
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
            let (cache, mut writer) = ProgressiveCache::new(Some(PAYLOAD_LEN as u64))
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

            let mut out = Vec::new();
            let read_res = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                reader.read_to_end(&mut out),
            )
            .await;

            writer_task.await.unwrap();
            read_res
                .unwrap_or_else(|_| panic!("iteration {iter}: reader timed out"))
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

    #[tokio::test]
    async fn reader_reads_appended_bytes_after_catching_up_to_eof() {
        // The reader drains all currently-written bytes (reaching the file's
        // physical end), then the writer appends more and finishes. The reader
        // must go on to read the appended tail rather than stopping at the
        // earlier end. Guards the grow-after-EOF continuation. (The dedicated
        // finish_right_after_buffered_write_reads_full_tail test covers the
        // finish-without-explicit-flush path that the truncation fix resolved.)
        let (cache, mut writer) = ProgressiveCache::new(None).await.unwrap();
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

    #[tokio::test]
    async fn set_error_propagates_to_reader() {
        let (cache, writer) = ProgressiveCache::new(None).await.unwrap();
        let mut reader = cache.reader().await.unwrap();

        writer.set_error("boom".into());

        let mut buf = [0u8; 4];
        let err = reader.read(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert!(err.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn seek_from_end_lands_and_reads_from_offset() {
        let (cache, mut writer) = ProgressiveCache::new(Some(10)).await.unwrap();
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
        let (cache, _writer) = ProgressiveCache::new(None).await.unwrap();
        let mut reader = cache.reader().await.unwrap();

        let err = reader.seek(SeekFrom::End(-1)).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn seek_current_negative_past_zero_errors() {
        let (cache, _writer) = ProgressiveCache::new(None).await.unwrap();
        let mut reader = cache.reader().await.unwrap();

        let err = reader.seek(SeekFrom::Current(-5)).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
}
