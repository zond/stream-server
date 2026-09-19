use super::{
    ArchiveEntry, ArchiveReader, AsyncSeekableReader, CacheConfig, OpenedMember,
    cache::{CacheWriter, ProgressiveCache, SyncCacheWriter},
    window::MemberWindow,
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

/// The fixed part of a ZIP local file header: signature, version, flags,
/// method, time, date, CRC, the two sizes and the two length fields.
const LOCAL_HEADER_BYTES: u64 = 30;
const LOCAL_HEADER_SIGNATURE: u32 = 0x0403_4b50;
/// General-purpose bit 0: the member's bytes are encrypted.
const ENCRYPTED_FLAG: u16 = 1;

/// Where the bytes of the member whose local header is at `header_offset`
/// begin, or `None` when they are not plainly there to be read.
///
/// The central directory's own `header_size` is deliberately not used: the
/// spec lets a member's extra field differ in length between the central
/// directory and the local header, and an offset wrong by a few bytes is a
/// film that will not decode. The local header is the one that describes
/// the bytes that follow it, so it is the one that is read -- thirty bytes
/// and the two lengths in them.
async fn stored_member_offset(
    reader: &mut Box<dyn AsyncSeekableReader>,
    header_offset: u64,
) -> Result<Option<u64>> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    reader.seek(std::io::SeekFrom::Start(header_offset)).await?;
    let mut header = [0u8; LOCAL_HEADER_BYTES as usize];
    reader.read_exact(&mut header).await?;
    let field = |at: usize| u16::from_le_bytes([header[at], header[at + 1]]);
    if u32::from_le_bytes([header[0], header[1], header[2], header[3]]) != LOCAL_HEADER_SIGNATURE {
        return Ok(None);
    }
    // An encrypted member's bytes are not the member's, whatever its
    // compression method says; it goes the way every other member this
    // cannot serve directly does.
    if field(6) & ENCRYPTED_FLAG != 0 {
        return Ok(None);
    }
    let name_and_extra = u64::from(field(26)) + u64::from(field(28));
    Ok(Some(header_offset + LOCAL_HEADER_BYTES + name_and_extra))
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
        let stored = entry.compression() == async_zip::Compression::Stored;
        let header_offset = entry.header_offset();

        // A stored member is a byte range of the archive, so there is
        // nothing to decode and nothing to write: the member is served from
        // wherever the archive is read from, which for the torrent form is
        // the torrent itself. That is the case worth having -- a film put in
        // a ZIP is normally stored, since it does not compress -- and it
        // costs no extraction, no second copy under the cache root and no
        // re-extraction per range request, because there is no extraction to
        // repeat. A header that does not say what it should (no local
        // signature, an encrypted member) falls through to the extraction
        // below rather than being served as bytes nobody has checked.
        if stored {
            let mut source = archive.into_inner().into_inner().into_inner();
            match stored_member_offset(&mut source, header_offset).await? {
                Some(offset) => {
                    return Ok(OpenedMember::Direct(Box::new(
                        MemberWindow::new(source, offset, size).await?,
                    )));
                }
                None => {
                    tracing::debug!(
                        member = path,
                        "the local header of a stored member does not describe plain bytes; \
                         extracting it"
                    );
                    // The archive reader was consumed to get at the source;
                    // build another over the same source to extract from.
                    archive = ZipFileReader::new(
                        BufReader::with_capacity(INFLATE_CHUNK_BYTES, source).compat(),
                    )
                    .await?;
                }
            }
        }

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
        write_zip_with(dir, name, data, async_zip::Compression::Deflate).await
    }

    async fn write_zip_with(
        dir: &std::path::Path,
        name: &str,
        data: &[u8],
        compression: async_zip::Compression,
    ) -> PathBuf {
        let path = dir.join("fixture.zip");
        let file = File::create(&path).await.unwrap();
        let mut writer = async_zip::base::write::ZipFileWriter::with_tokio(file);
        writer
            .write_entry_whole(
                async_zip::ZipEntryBuilder::new(name.into(), compression),
                data,
            )
            .await
            .unwrap();
        writer.close().await.unwrap();
        path
    }

    /// **A stored member is a byte range of the archive**, so it is served
    /// from the archive itself: no decoding, nothing written under the cache
    /// root, and a range read out of the middle of it is that range. A
    /// member that *is* compressed still goes through an extraction.
    #[tokio::test]
    async fn a_stored_member_is_served_from_the_archive_itself() {
        let root = tempfile::tempdir().unwrap();
        let member = content(512 * 1024);
        let archive = write_zip_with(
            root.path(),
            "movie.bin",
            &member,
            async_zip::Compression::Stored,
        )
        .await;
        let config = CacheConfig {
            cache_dir: root.path().to_path_buf(),
            _cache_size: 0,
        };
        let handler = ZipHandler::new(archive, config.clone());

        let OpenedMember::Direct(mut reader) = handler.open_file("movie.bin").await.unwrap() else {
            panic!("a stored member needs no extraction");
        };
        use tokio::io::AsyncSeekExt;
        assert_eq!(
            reader.seek(std::io::SeekFrom::End(0)).await.unwrap(),
            member.len() as u64
        );
        reader
            .seek(std::io::SeekFrom::Start(256 * 1024))
            .await
            .unwrap();
        let mut middle = [0u8; 4096];
        reader.read_exact(&mut middle).await.unwrap();
        assert_eq!(middle.as_slice(), &member[256 * 1024..256 * 1024 + 4096]);
        assert!(
            !root.path().join(crate::archives::SCRATCH_DIR_NAME).exists(),
            "nothing was written for it"
        );

        // The compressed member has to be decoded, so it is.
        let deflated = write_zip(root.path(), "movie.bin", &member).await;
        let handler = ZipHandler::new(deflated, config);
        assert!(matches!(
            handler.open_file("movie.bin").await.unwrap(),
            OpenedMember::Extracted(_)
        ));
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
