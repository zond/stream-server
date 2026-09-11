use super::{
    ArchiveEntry, ArchiveReader, CacheConfig, OpenedMember,
    cache::{ProgressiveCache, SyncCacheWriter},
};
use anyhow::{Result, anyhow};
use sevenz_rust2::{Archive, ArchiveReader as SevenZReader, BlockDecoder, Password};
use std::path::{Path, PathBuf};

/// 7z archive handler backed by the pure-Rust `sevenz-rust2` crate.
///
/// Decompression is synchronous, so all archive work runs on the blocking
/// thread pool. Extracted data is streamed into a [`ProgressiveCache`] whose
/// reader supports the range/seek semantics the HTTP layer expects.
pub struct SevenZHandler {
    path: PathBuf,
    cache_config: CacheConfig,
}

impl SevenZHandler {
    pub fn new_with_config(path: PathBuf, cache_config: CacheConfig) -> Self {
        Self { path, cache_config }
    }
}

/// What a drain's error says when the cache it was draining towards was
/// abandoned.
const SKIP_ABANDONED: &str = "abandoned while skipping to the member";

/// A sink for the bytes stored before the member, that fails once nobody
/// reads the cache the member is for.
struct Skip<'a>(&'a mut SyncCacheWriter);

impl std::io::Write for Skip<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .check_abandoned()
            .map_err(|e| std::io::Error::new(e.kind(), format!("{SKIP_ABANDONED}: {e}")))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Decompress a single entry into the progressive cache's sync writer.
///
/// Runs on the blocking pool. Only the block containing the target entry is
/// decoded, so unrelated blocks are never decompressed. Within a solid block
/// the compressed stream is strictly sequential, so entries stored before the
/// target are decoded and drained to reach the target's data at the correct
/// stream offset.
fn extract_entry(archive_path: &Path, entry_name: &str, out: &mut SyncCacheWriter) -> Result<()> {
    let archive = Archive::open(archive_path).map_err(|e| anyhow!("Failed to open 7z: {}", e))?;

    let file_index = archive
        .files
        .iter()
        .position(|f| !f.is_directory() && f.name() == entry_name)
        .ok_or_else(|| anyhow!("File not found in archive: {}", entry_name))?;

    // Empty entries have no associated block; there is nothing to decode.
    if let Some(block_index) = archive.stream_map.file_block_index[file_index] {
        let mut source =
            std::fs::File::open(archive_path).map_err(|e| anyhow!("Failed to open 7z: {}", e))?;
        let password = Password::empty();
        let target = &archive.files[file_index];

        let mut extracted = false;
        let mut write_err: Option<std::io::Error> = None;

        BlockDecoder::new(1, block_index, &archive, &password, &mut source)
            .for_each_entries(&mut |entry, entry_reader| {
                if std::ptr::eq(entry, target) {
                    extracted = true;
                    match std::io::copy(entry_reader, out) {
                        Ok(_) => Ok(false), // Done, stop iterating
                        Err(e) => {
                            write_err = Some(e);
                            Ok(false)
                        }
                    }
                } else {
                    // A preceding entry in a solid block: its bytes come first
                    // in the shared stream and must be fully drained, or the
                    // target would be read from the wrong offset. Drained
                    // through the writer's abandonment check, because a
                    // drain writes nothing to the cache: a player that left
                    // while gigabytes of earlier members were decoded used
                    // to keep the decoder going to the member, and only its
                    // first write gave up.
                    std::io::copy(entry_reader, &mut Skip(out))?;
                    Ok(true)
                }
            })
            .map_err(|e| anyhow!("7z decompression failed: {}", e))?;

        if let Some(e) = write_err {
            return Err(anyhow!("Failed to write decompressed data: {}", e));
        }
        if !extracted {
            return Err(anyhow!("Entry missing from its 7z block: {}", entry_name));
        }
    }

    use std::io::Write;
    out.flush()?;
    Ok(())
}

#[async_trait::async_trait]
impl ArchiveReader for SevenZHandler {
    async fn list_files(&self) -> Result<Vec<ArchiveEntry>> {
        let path = self.path.clone();

        tokio::task::spawn_blocking(move || {
            let reader = SevenZReader::open(&path, Password::empty())
                .map_err(|e| anyhow!("Failed to open 7z: {}", e))?;

            Ok(reader
                .archive()
                .files
                .iter()
                .map(|e| ArchiveEntry {
                    path: e.name().to_string(),
                    size: e.size(),
                    is_dir: e.is_directory(),
                })
                .collect())
        })
        .await?
    }

    async fn open_file(&self, path: &str) -> Result<OpenedMember> {
        let archive_path = self.path.clone();
        let target = path.to_string();

        // Metadata pass: verify the entry exists and get its uncompressed size
        // so the progressive cache knows the total length up front.
        let meta_path = archive_path.clone();
        let meta_target = target.clone();
        let file_size = tokio::task::spawn_blocking(move || -> Result<u64> {
            let reader = SevenZReader::open(&meta_path, Password::empty())
                .map_err(|e| anyhow!("Failed to open 7z: {}", e))?;
            let entry = reader
                .archive()
                .files
                .iter()
                .find(|e| !e.is_directory() && e.name() == meta_target)
                .ok_or_else(|| anyhow!("File not found in archive: {}", meta_target))?;
            Ok(entry.size())
        })
        .await??;

        // The extracted member lands in the archive scratch dir, which
        // nothing counts and whose session unlinks it (see
        // `archives::SCRATCH_DIR_NAME`).
        let (cache, writer) =
            ProgressiveCache::new_in_dir(&self.cache_config.scratch_dir(), Some(file_size)).await?;

        // Decompress in the background on the blocking pool, streaming into the
        // progressive cache so the returned reader can serve data immediately.
        tokio::task::spawn_blocking(move || {
            let mut out = match writer.try_clone_sync() {
                Ok(out) => out,
                Err(e) => {
                    tracing::error!("7z extraction failed to open cache writer: {}", e);
                    writer.set_error(format!("Failed to open cache writer: {}", e));
                    return;
                }
            };

            match extract_entry(&archive_path, &target, &mut out) {
                Ok(()) => out.finish(),
                Err(e) => {
                    tracing::error!("7z extraction failed: {}", e);
                    out.set_error(e.to_string());
                }
            }
        });

        Ok(OpenedMember::Extracted(cache))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sevenz_rust2::{ArchiveEntry as SevenZEntry, ArchiveWriter, SourceReader};
    use std::io::Cursor;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    const FIRST_CONTENT: &[u8] = b"hello from the first entry\n";

    /// Deterministic, mildly incompressible content large enough to span
    /// several reads.
    fn second_content() -> Vec<u8> {
        (0..64 * 1024u32)
            .map(|i| (i.wrapping_mul(31) % 251) as u8)
            .collect()
    }

    /// Build a small 7z archive on disk with two file entries.
    fn write_fixture(dir: &Path) -> PathBuf {
        let path = dir.join("fixture.7z");
        let mut writer = ArchiveWriter::create(&path).expect("create 7z writer");
        writer
            .push_archive_entry(
                SevenZEntry::new_file("first.txt"),
                Some(Cursor::new(FIRST_CONTENT.to_vec())),
            )
            .expect("push first entry");
        writer
            .push_archive_entry(
                SevenZEntry::new_file("videos/second.bin"),
                Some(Cursor::new(second_content())),
            )
            .expect("push second entry");
        writer.finish().expect("finish 7z archive");
        path
    }

    /// Build a solid 7z archive (the 7-Zip CLI default): both entries share
    /// one compressed block, so the second entry's data sits behind the
    /// first's in the same stream.
    fn write_solid_fixture(dir: &Path) -> PathBuf {
        let path = dir.join("solid.7z");
        let mut writer = ArchiveWriter::create(&path).expect("create 7z writer");
        writer
            .push_archive_entries(
                vec![
                    SevenZEntry::new_file("first.txt"),
                    SevenZEntry::new_file("videos/second.bin"),
                ],
                vec![
                    SourceReader::new(Cursor::new(FIRST_CONTENT.to_vec())),
                    SourceReader::new(Cursor::new(second_content())),
                ],
            )
            .expect("push solid entries");
        writer.finish().expect("finish 7z archive");

        // Pin the fixture shape: both files must live in a single block, or
        // this fixture no longer exercises the solid-archive path.
        let archive = Archive::open(&path).expect("reopen solid fixture");
        assert_eq!(archive.blocks.len(), 1, "fixture must be a solid archive");
        assert_eq!(archive.files.len(), 2);

        path
    }

    fn handler_for(dir: &tempfile::TempDir, archive: PathBuf) -> SevenZHandler {
        SevenZHandler::new_with_config(
            archive,
            CacheConfig {
                cache_dir: dir.path().to_path_buf(),
                _cache_size: 0,
            },
        )
    }

    #[tokio::test]
    async fn lists_entries() {
        let dir = tempfile::tempdir().unwrap();
        let archive = write_fixture(dir.path());
        let handler = handler_for(&dir, archive);

        let mut entries = handler.list_files().await.expect("list files");
        entries.sort_by(|a, b| a.path.cmp(&b.path));

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, "first.txt");
        assert_eq!(entries[0].size, FIRST_CONTENT.len() as u64);
        assert!(!entries[0].is_dir);
        assert_eq!(entries[1].path, "videos/second.bin");
        assert_eq!(entries[1].size, second_content().len() as u64);
        assert!(!entries[1].is_dir);
    }

    #[tokio::test]
    async fn reads_full_entry() {
        let dir = tempfile::tempdir().unwrap();
        let archive = write_fixture(dir.path());
        let handler = handler_for(&dir, archive);

        let mut reader = handler
            .open_file("videos/second.bin")
            .await
            .expect("open entry")
            .into_reader()
            .await
            .expect("open reader");
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await.expect("read entry");

        assert_eq!(data, second_content());

        // The smaller entry decodes correctly too.
        let mut reader = handler
            .open_file("first.txt")
            .await
            .expect("open entry")
            .into_reader()
            .await
            .expect("open reader");
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await.expect("read entry");
        assert_eq!(data, FIRST_CONTENT);
    }

    #[tokio::test]
    async fn reads_partial_range_after_seek() {
        let dir = tempfile::tempdir().unwrap();
        let archive = write_fixture(dir.path());
        let handler = handler_for(&dir, archive);

        let expected = second_content();
        let mut reader = handler
            .open_file("videos/second.bin")
            .await
            .expect("open entry")
            .into_reader()
            .await
            .expect("open reader");

        // Seek into the middle of the entry and read a bounded range, the way
        // an HTTP Range request is served.
        let offset = 40_000u64;
        let len = 1_000usize;
        reader
            .seek(std::io::SeekFrom::Start(offset))
            .await
            .expect("seek");
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await.expect("read range");
        assert_eq!(buf, expected[offset as usize..offset as usize + len]);

        // SeekFrom::End works because the cache knows the total size.
        reader
            .seek(std::io::SeekFrom::End(-(len as i64)))
            .await
            .expect("seek from end");
        let mut tail = vec![0u8; len];
        reader.read_exact(&mut tail).await.expect("read tail");
        assert_eq!(tail, expected[expected.len() - len..]);
    }

    #[tokio::test]
    async fn reads_entry_from_solid_block() {
        let dir = tempfile::tempdir().unwrap();
        let archive = write_solid_fixture(dir.path());
        let handler = handler_for(&dir, archive);

        // The target is NOT the first file in the solid block, so extraction
        // must decode and drain "first.txt" before copying the target, or the
        // bytes come from the wrong stream offset (CRC failure or corruption).
        let mut reader = handler
            .open_file("videos/second.bin")
            .await
            .expect("open entry")
            .into_reader()
            .await
            .expect("open reader");
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await.expect("read entry");
        assert_eq!(data, second_content());

        // The first entry of the block still extracts correctly.
        let mut reader = handler
            .open_file("first.txt")
            .await
            .expect("open entry")
            .into_reader()
            .await
            .expect("open reader");
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await.expect("read entry");
        assert_eq!(data, FIRST_CONTENT);
    }

    #[tokio::test]
    async fn two_handlers_read_the_same_archive_concurrently() {
        // Two handlers over byte-identical archives, each with its own cache
        // root, extracting both entries at the same time. Extraction runs on
        // the blocking pool and the handler drops its `ProgressiveCache` as
        // soon as `open_file` returns, so a late-starting extraction task must
        // still find its cache file (it used to be unlinked by then, failing
        // with "Failed to open cache writer" when the pool was busy).
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let archive_a = write_fixture(dir_a.path());
        let archive_b = dir_b.path().join("copy.7z");
        std::fs::copy(&archive_a, &archive_b).expect("copy fixture bytes");

        let handler_a = Arc::new(handler_for(&dir_a, archive_a));
        let handler_b = Arc::new(handler_for(&dir_b, archive_b));

        let expected_second = second_content();
        let mut tasks = Vec::new();
        for _ in 0..4 {
            for handler in [&handler_a, &handler_b] {
                let handler = Arc::clone(handler);
                tasks.push(tokio::spawn(async move {
                    let mut reader = handler
                        .open_file("videos/second.bin")
                        .await
                        .expect("open second entry")
                        .into_reader()
                        .await
                        .expect("open reader");
                    let mut second = Vec::new();
                    reader.read_to_end(&mut second).await.expect("read second");

                    let mut reader = handler
                        .open_file("first.txt")
                        .await
                        .expect("open first")
                        .into_reader()
                        .await
                        .expect("open reader");
                    let mut first = Vec::new();
                    reader.read_to_end(&mut first).await.expect("read first");
                    (first, second)
                }));
            }
        }

        for task in tasks {
            let (first, second) = task.await.expect("task panicked");
            assert_eq!(first, FIRST_CONTENT);
            assert_eq!(second, expected_second);
        }
    }

    /// A solid block's drain towards the member stops once nobody reads the
    /// cache, rather than decoding every earlier member for a player that
    /// has gone.
    #[tokio::test]
    async fn a_drain_nobody_waits_for_stops() {
        let dir = tempfile::tempdir().unwrap();
        let archive = write_solid_fixture(dir.path());
        let (cache, mut writer) = ProgressiveCache::new_in_dir(&dir.path().join("scratch"), None)
            .await
            .unwrap();
        writer.abandon_after(std::time::Duration::ZERO);
        let mut out = writer.try_clone_sync().unwrap();
        drop(cache);
        let err = tokio::task::spawn_blocking(move || {
            extract_entry(&archive, "videos/second.bin", &mut out)
        })
        .await
        .unwrap()
        .expect_err("an abandoned extraction finished");
        assert!(err.to_string().contains(SKIP_ABANDONED), "{err}");
    }

    #[tokio::test]
    async fn missing_entry_errors() {
        let dir = tempfile::tempdir().unwrap();
        let archive = write_fixture(dir.path());
        let handler = handler_for(&dir, archive);

        let err = match handler.open_file("does-not-exist.bin").await {
            Ok(_) => panic!("opening a missing entry should fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("File not found in archive"));
    }
}
