use super::{
    ArchiveEntry, ArchiveReader, AsyncSeekableReader, CacheConfig,
    cache::{ProgressiveCache, SyncCacheWriter},
};
use anyhow::{Result, anyhow};
use sevenz_rust2::{Archive, ArchiveReader as SevenZReader, BlockDecoder, Password};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

/// 7z archive handler backed by the pure-Rust `sevenz-rust2` crate.
///
/// Decompression is synchronous, so all archive work runs on the blocking
/// thread pool. Extracted data is streamed into a [`ProgressiveCache`] whose
/// reader supports the range/seek semantics the HTTP layer expects.
pub struct SevenZHandler {
    path: Option<PathBuf>,
    _reader: Arc<Mutex<Option<Box<dyn AsyncSeekableReader>>>>,
    cache_config: CacheConfig,
}

impl SevenZHandler {
    #[allow(dead_code)]
    pub fn new(path: PathBuf) -> Self {
        Self {
            path: Some(path),
            _reader: Arc::new(Mutex::new(None)),
            cache_config: CacheConfig::default(),
        }
    }

    pub fn new_with_config(path: PathBuf, cache_config: CacheConfig) -> Self {
        Self {
            path: Some(path),
            _reader: Arc::new(Mutex::new(None)),
            cache_config,
        }
    }

    pub fn new_with_reader(reader: Box<dyn AsyncSeekableReader>) -> Self {
        Self {
            path: None,
            _reader: Arc::new(Mutex::new(Some(reader))),
            cache_config: CacheConfig::default(),
        }
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
                    // target would be read from the wrong offset.
                    std::io::copy(entry_reader, &mut std::io::sink())?;
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
        let Some(path) = self.path.clone() else {
            // Stream-based access would require a sync Read + Seek bridge over
            // the async reader; the server always goes through a local path.
            return Err(anyhow!(
                "Listing files from generic stream not supported yet"
            ));
        };

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

    async fn open_file(&self, path: &str) -> Result<Box<dyn AsyncSeekableReader>> {
        let Some(archive_path) = self.path.clone() else {
            // Stream-based access not yet supported (see list_files).
            return Err(anyhow!("Streaming 7z from remote source not supported yet"));
        };
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

        // Create cache in the configured cache directory
        let cache_dir = self.cache_config.get_dir_or_temp();
        let (cache, writer) = ProgressiveCache::new_in_dir(&cache_dir, Some(file_size)).await?;

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

        let reader = cache.reader().await?;
        Ok(Box::new(reader))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sevenz_rust2::{ArchiveEntry as SevenZEntry, ArchiveWriter, SourceReader};
    use std::io::Cursor;
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
                cache_dir: Some(dir.path().to_path_buf()),
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
            .expect("open entry");
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await.expect("read entry");

        assert_eq!(data, second_content());

        // The smaller entry decodes correctly too.
        let mut reader = handler.open_file("first.txt").await.expect("open entry");
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
            .expect("open entry");

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
            .expect("open entry");
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await.expect("read entry");
        assert_eq!(data, second_content());

        // The first entry of the block still extracts correctly.
        let mut reader = handler.open_file("first.txt").await.expect("open entry");
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await.expect("read entry");
        assert_eq!(data, FIRST_CONTENT);
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
