use super::{
    ArchiveEntry, ArchiveReader, AsyncSeekableReader, CacheConfig, OpenedMember,
    cache::{CacheWriter, ProgressiveCache, SyncCacheWriter},
};
use anyhow::{Result, anyhow};
use async_zip::tokio::read::seek::ZipFileReader;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio_util::compat::TokioAsyncReadCompatExt;

use std::sync::Arc;
use tokio::sync::Mutex;

/// How much of the archive is read, and of the member written, at a time:
/// each is a blocking-pool round trip through a tokio `File`, which at
/// `tokio::io::copy`'s 8 KiB was some quarter of a million per gigabyte.
const INFLATE_CHUNK_BYTES: usize = 256 * 1024;

type Archive = ZipFileReader<BufReader<Box<dyn AsyncSeekableReader>>>;

/// Copy member `index` of `archive` into `out`, for a thread of its own.
async fn inflate_to_sync(
    archive: &mut Archive,
    index: usize,
    out: &mut SyncCacheWriter,
) -> Result<()> {
    use std::io::Write;
    use tokio::io::AsyncReadExt;
    use tokio_util::compat::FuturesAsyncReadCompatExt;
    let mut entry = archive
        .reader_with_entry(index)
        .await
        .map_err(|e| anyhow!("Failed to open entry: {e}"))?
        .compat();
    let mut buf = vec![0u8; INFLATE_CHUNK_BYTES];
    loop {
        let n = entry.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        out.write_all(&buf[..n])?;
    }
}

/// The same, for a task: `out` is the async writer.
async fn inflate_to_async(
    archive: &mut Archive,
    index: usize,
    out: &mut CacheWriter,
) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::compat::FuturesAsyncReadCompatExt;
    let mut entry = archive
        .reader_with_entry(index)
        .await
        .map_err(|e| anyhow!("Failed to open entry: {e}"))?
        .compat();
    let mut buf = vec![0u8; INFLATE_CHUNK_BYTES];
    loop {
        let n = entry.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        out.write_all(&buf[..n]).await?;
    }
}

pub struct ZipHandler {
    path: Option<PathBuf>,
    // Wrap in Arc<Mutex<Option>> to allow taking it out in `open_file` (one-shot)
    reader: Arc<Mutex<Option<Box<dyn AsyncSeekableReader>>>>,
    cache_config: CacheConfig,
}

impl ZipHandler {
    pub fn new(path: PathBuf, cache_config: CacheConfig) -> Self {
        Self {
            path: Some(path),
            reader: Arc::new(Mutex::new(None)),
            cache_config,
        }
    }

    pub fn new_with_reader(
        reader: Box<dyn AsyncSeekableReader>,
        cache_config: CacheConfig,
    ) -> Self {
        Self {
            path: None,
            reader: Arc::new(Mutex::new(Some(reader))),
            cache_config,
        }
    }
}

#[async_trait::async_trait]
impl ArchiveReader for ZipHandler {
    async fn list_files(&self) -> Result<Vec<ArchiveEntry>> {
        // Limitation: list_files destroys the reader if we use `with_ident`?
        // `with_ident` takes ownership of reader.
        // We can't list files then open file with the same reader if it's a stream that can't be cloned.
        // BUT `stream_file` in `routes/archive.rs` calls `get_archive_reader` then `open_file`. It does NOT call `list_files`.
        // `list_files` is used by `list_archive_content` route which opens a NEW reader.

        let mut reader_guard = self.reader.lock().await;

        if let Some(path) = &self.path {
            // Local file: Open fresh
            let file = File::open(path).await?;
            let archive = ZipFileReader::new(BufReader::new(file).compat()).await?;
            let entries = archive
                .file()
                .entries()
                .iter()
                .map(|e| ArchiveEntry {
                    path: e.filename().as_str().unwrap_or_default().to_string(),
                    size: e.uncompressed_size(),
                    is_dir: e.dir().unwrap_or(false),
                })
                .collect();
            Ok(entries)
        } else if let Some(reader) = reader_guard.take() {
            // We TAKE the reader. It is consumed.
            // This works for "One Shot" listing.
            // But if we want to list then open?
            // We can't put it back easily because `ZipFileReader` consumes it.
            // unless we use `into_inner()`?

            let archive = ZipFileReader::new(BufReader::new(reader).compat()).await?;
            let entries = archive
                .file()
                .entries()
                .iter()
                .map(|e| ArchiveEntry {
                    path: e.filename().as_str().unwrap_or_default().to_string(),
                    size: e.uncompressed_size(),
                    is_dir: e.dir().unwrap_or(false),
                })
                .collect();

            // Put reader back?
            // `archive.into_inner()`             // Recover reader
            let returned_reader = archive.into_inner().into_inner().into_inner();
            *reader_guard = Some(returned_reader);

            Ok(entries)
        } else {
            Err(anyhow!("No source available or already consumed"))
        }
    }

    async fn open_file(&self, path: &str) -> Result<OpenedMember> {
        let mut reader_guard = self.reader.lock().await;

        let reader_box: Box<dyn AsyncSeekableReader> = if let Some(p) = &self.path {
            Box::new(File::open(p).await?)
        } else if let Some(r) = reader_guard.take() {
            r
        } else {
            return Err(anyhow!("Archive source already consumed"));
        };

        let mut archive =
            ZipFileReader::new(BufReader::with_capacity(INFLATE_CHUNK_BYTES, reader_box).compat())
                .await?;

        let index = archive
            .file()
            .entries()
            .iter()
            .position(|e| e.filename().as_str().unwrap_or_default() == path)
            .ok_or(anyhow!("File not found in archive"))?;

        let entry = archive.file().entries().get(index).unwrap();
        let size = entry.uncompressed_size();

        // Use ProgressiveCache for robust seeking. The extracted member lands
        // in the archive scratch dir, which nothing counts and whose session
        // unlinks it (see `archives::SCRATCH_DIR_NAME`).
        let (cache, writer) =
            ProgressiveCache::new_in_dir(&self.cache_config.scratch_dir(), Some(size)).await?;

        if self.path.is_none() {
            // A reader over a torrent's file: its reads wait on pieces, and
            // a thread parked in one when the runtime goes down is never
            // woken, so this form stays a task, which the runtime drops.
            // What it gets is the larger copies (see `INFLATE_CHUNK_BYTES`).
            let mut writer = writer;
            tokio::spawn(async move {
                match inflate_to_async(&mut archive, index, &mut writer).await {
                    Ok(()) => writer.finish().await,
                    Err(e) => writer.set_error(e.to_string()),
                }
            });
            return Ok(OpenedMember::Extracted(cache));
        }

        // A file on disk: inflated on a thread of its own, into the sync
        // writer. It used to be a `tokio::spawn` running `tokio::io::copy`
        // between two tokio `File`s: the decompression ran on a reactor
        // worker, taking it from every other request for as long as a
        // member took, and every 8 KiB of it was a blocking-pool round trip
        // on each side -- some quarter of a million per gigabyte. Here the
        // writes are plain `write`s and the archive is read in
        // `INFLATE_CHUNK_BYTES` fills. The async archive reader is polled
        // with the runtime's `block_on`, which a thread outside it may call.
        // A plain thread and not the blocking pool, because the runtime
        // waits for the pool's tasks when it shuts down, and this one runs
        // until the member is done or `ABANDONED_AFTER` has passed without
        // a reader; once the runtime is gone its file reads fail and it
        // ends.
        let runtime = tokio::runtime::Handle::current();
        let mut out = writer.try_clone_sync()?;
        std::thread::spawn(move || {
            match runtime.block_on(inflate_to_sync(&mut archive, index, &mut out)) {
                Ok(()) => out.finish(),
                Err(e) => out.set_error(e.to_string()),
            }
        });

        Ok(OpenedMember::Extracted(cache))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn content(len: usize) -> Vec<u8> {
        (0..len as u32)
            .map(|i| (i.wrapping_mul(31) % 251) as u8)
            .collect()
    }

    /// A zip holding one deflated member.
    async fn write_zip(dir: &std::path::Path, name: &str, data: &[u8]) -> PathBuf {
        let path = dir.join("fixture.zip");
        let file = File::create(&path).await.unwrap();
        let mut writer = async_zip::base::write::ZipFileWriter::with_tokio(file);
        writer
            .write_entry_whole(
                async_zip::ZipEntryBuilder::new(name.into(), async_zip::Compression::Deflate),
                data,
            )
            .await
            .unwrap();
        writer.close().await.unwrap();
        path
    }

    /// The member is inflated while the runtime's only thread is held and
    /// never yields: the decompression has a thread of its own, rather than
    /// being a task on a reactor worker, where it held that worker from
    /// every other request for as long as a member took.
    #[tokio::test(flavor = "current_thread")]
    async fn a_member_is_inflated_while_the_runtime_thread_is_busy() {
        let root = tempfile::tempdir().unwrap();
        let member = content(2 * 1024 * 1024);
        let archive = write_zip(root.path(), "movie.bin", &member).await;
        let handler = ZipHandler::new(
            archive,
            CacheConfig {
                cache_dir: root.path().to_path_buf(),
                _cache_size: 0,
            },
        );
        let OpenedMember::Extracted(cache) = handler.open_file("movie.bin").await.unwrap() else {
            panic!("a zip member is extracted");
        };

        let scratch = root.path().join(crate::archives::SCRATCH_DIR_NAME);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            // Each size is read through the path, not off the directory
            // listing: on Windows a listing reports the size in the
            // directory entry, which is not updated while the file is still
            // open for writing, so the inflate read as 0 bytes for as long
            // as it ran.
            let written: u64 = std::fs::read_dir(&scratch)
                .unwrap()
                .map(|entry| std::fs::metadata(entry.unwrap().path()).unwrap().len())
                .sum();
            if written == member.len() as u64 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{written} of {} bytes inflated while the runtime thread was held",
                member.len()
            );
            // Blocks the runtime's only thread: nothing on it can run.
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        let mut data = Vec::new();
        cache
            .reader()
            .await
            .unwrap()
            .read_to_end(&mut data)
            .await
            .unwrap();
        assert_eq!(data, member);
    }
}
