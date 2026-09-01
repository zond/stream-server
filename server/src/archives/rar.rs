use super::{
    ArchiveEntry, ArchiveReader, AsyncSeekableReader, CacheConfig,
    cache::{ProgressiveCache, SyncCacheWriter},
};
use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};
use unrar_rs::{ExtractOptions, RarArchive, StaticVolumeProvider};

/// RAR archive handler backed by the pure-Rust `unrar-rs` crate.
///
/// LICENSING: `unrar-rs` is GPL-3.0-or-later, so a build that links it (the
/// default build) is GPL-3.0-or-later. The repo source stays MIT; see the dep
/// comment in `server/Cargo.toml`.
///
/// Decompression is synchronous, so all archive work runs on the blocking
/// thread pool. Extracted data is streamed into a [`ProgressiveCache`] whose
/// reader supports the range/seek semantics the HTTP layer expects.
///
/// Hardening: this handler never unwraps/expects on archive-derived data. A
/// malformed or malicious archive yields a clean `Err`, never a panic — the
/// release profile is `panic = "abort"`, so a panic here would kill the whole
/// server.
pub struct RarHandler {
    path: PathBuf,
    cache_config: CacheConfig,
}

impl RarHandler {
    #[allow(dead_code)]
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            cache_config: CacheConfig::default(),
        }
    }

    pub fn new_with_config(path: PathBuf, cache_config: CacheConfig) -> Self {
        Self { path, cache_config }
    }
}

/// Open and parse a RAR archive from a local path. Header parsing decompresses
/// nothing, so this is cheap even for large archives.
fn open_archive(path: &Path) -> Result<RarArchive> {
    let file = std::fs::File::open(path).map_err(|e| anyhow!("Failed to open RAR: {}", e))?;
    RarArchive::open(file).map_err(|e| anyhow!("Failed to parse RAR: {}", e))
}

/// Resolve a listed entry name to its extraction index (into the archive's
/// member list) and uncompressed size.
///
/// `list_files` exposes each member's sanitized name, so a follow-up
/// `open_file` call arrives with that sanitized name; match it the same way.
/// `find_member` (raw header name) is a fallback for callers that kept the raw
/// name.
fn resolve_entry(archive: &RarArchive, target: &str) -> Result<(usize, u64)> {
    let index = archive
        .find_member_sanitized(target)
        .or_else(|| archive.find_member(target))
        .ok_or_else(|| anyhow!("File not found in archive: {}", target))?;

    let info = archive
        .member_info(index)
        .ok_or_else(|| anyhow!("File not found in archive: {}", target))?;

    if info.is_directory {
        return Err(anyhow!("Cannot stream a directory entry: {}", target));
    }

    // The progressive cache needs the total length up front so the HTTP layer
    // can answer `SeekFrom::End` / Range requests. A split member's size is
    // only known once its final volume's header is seen; without it we cannot
    // serve ranges, so fail cleanly rather than guessing.
    let size = info.unpacked_size.ok_or_else(|| {
        anyhow!(
            "Uncompressed size for entry is not available (incomplete volume set?): {}",
            target
        )
    })?;

    Ok((index, size))
}

/// Decompress a single member into the progressive cache's sync writer.
///
/// Runs on the blocking pool. `extract_member_streaming` decodes directly into
/// the writer without buffering the whole member in memory, and handles solid
/// and non-solid archives alike (for a solid member it replays the preceding
/// members internally). `verify: true` turns a CRC32/BLAKE2sp mismatch into an
/// error instead of silently serving corrupt bytes.
fn extract_entry(archive_path: &Path, index: usize, out: &mut SyncCacheWriter) -> Result<()> {
    let mut archive = open_archive(archive_path)?;
    let provider = StaticVolumeProvider::from_ordered(vec![archive_path.to_path_buf()]);
    let options = ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };

    archive
        .extract_member_streaming(index, &options, &provider, out)
        .map_err(|e| anyhow!("RAR extraction failed: {}", e))?;

    use std::io::Write;
    out.flush()?;
    Ok(())
}

#[async_trait::async_trait]
impl ArchiveReader for RarHandler {
    async fn list_files(&self) -> Result<Vec<ArchiveEntry>> {
        let path = self.path.clone();

        tokio::task::spawn_blocking(move || {
            let archive = open_archive(&path)?;
            Ok(archive
                .metadata()
                .members
                .into_iter()
                .map(|m| ArchiveEntry {
                    path: m.name,
                    size: m.unpacked_size.unwrap_or(0),
                    is_dir: m.is_directory,
                })
                .collect())
        })
        .await?
    }

    async fn open_file(&self, path: &str) -> Result<Box<dyn AsyncSeekableReader>> {
        let archive_path = self.path.clone();
        let target = path.to_string();

        // Metadata pass: resolve the entry's extraction index and uncompressed
        // size so the progressive cache knows the total length up front.
        let meta_path = archive_path.clone();
        let meta_target = target.clone();
        let (index, file_size) = tokio::task::spawn_blocking(move || -> Result<(usize, u64)> {
            let archive = open_archive(&meta_path)?;
            resolve_entry(&archive, &meta_target)
        })
        .await??;

        // Create the cache in the configured cache directory.
        let cache_dir = self.cache_config.get_dir_or_temp();
        let (cache, writer) = ProgressiveCache::new_in_dir(&cache_dir, Some(file_size)).await?;

        // Decompress in the background on the blocking pool, streaming into the
        // progressive cache so the returned reader can serve data immediately.
        tokio::task::spawn_blocking(move || {
            let mut out = match writer.try_clone_sync() {
                Ok(out) => out,
                Err(e) => {
                    tracing::error!("RAR extraction failed to open cache writer: {}", e);
                    writer.set_error(format!("Failed to open cache writer: {}", e));
                    return;
                }
            };

            match extract_entry(&archive_path, index, &mut out) {
                Ok(()) => out.finish(),
                Err(e) => {
                    tracing::error!("RAR extraction failed: {}", e);
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
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    const FIRST_CONTENT: &[u8] = b"hello from the first entry\n";

    /// Deterministic, mildly incompressible content large enough to span
    /// several reads and exercise a mid-file range request.
    fn second_content() -> Vec<u8> {
        (0..64 * 1024u32)
            .map(|i| (i.wrapping_mul(31) % 251) as u8)
            .collect()
    }

    // ---- Minimal store-method RAR5 archive builder ---------------------------
    //
    // The `rar` CLI (archive creator) is not guaranteed to exist on build
    // machines and `unrar-rs` intentionally exposes no writer, so tests
    // construct a store-method RAR5 archive by hand. The layout follows
    // RARLAB's RAR5 technote: signature, main header (type 1), one file header
    // (type 2) + raw data area per entry, end header (type 5).

    /// Encode a u64 as a RAR5 variable-length integer.
    fn encode_vint(mut value: u64) -> Vec<u8> {
        let mut result = Vec::new();
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            result.push(byte);
            if value == 0 {
                break;
            }
        }
        result
    }

    /// Standard reflected CRC-32 (ISO-HDLC / zlib), as RAR5 stores for headers
    /// and file data. Table-less so the test pulls in no extra dependency.
    fn crc32(data: &[u8]) -> u32 {
        let mut crc: u32 = 0xFFFF_FFFF;
        for &b in data {
            crc ^= b as u32;
            for _ in 0..8 {
                let mask = (crc & 1).wrapping_neg();
                crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
            }
        }
        !crc
    }

    /// Build a generic RAR5 header: `crc32(header_size_vint || body) ||
    /// header_size_vint || body`, where body is `type_vint || flags_vint ||
    /// type_body`.
    fn build_header(header_type: u64, common_flags: u64, type_body: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(header_type));
        body.extend_from_slice(&encode_vint(common_flags));
        body.extend_from_slice(type_body);

        let header_size_bytes = encode_vint(body.len() as u64);

        let mut crc_input = header_size_bytes.clone();
        crc_input.extend_from_slice(&body);
        let crc = crc32(&crc_input);

        let mut result = Vec::new();
        result.extend_from_slice(&crc.to_le_bytes());
        result.extend_from_slice(&header_size_bytes);
        result.extend_from_slice(&body);
        result
    }

    /// Build a type-2 file header for a stored (uncompressed) file. The common
    /// flags carry DATA_AREA (0x0002) with the data size; the raw content is
    /// appended by the caller immediately after.
    fn build_file_header(filename: &str, content: &[u8]) -> Vec<u8> {
        let data_crc = crc32(content);

        // File flags: CRC32_PRESENT (0x0004).
        let file_flags: u64 = 0x0004;

        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_vint(file_flags));
        type_body.extend_from_slice(&encode_vint(content.len() as u64)); // unpacked_size
        type_body.extend_from_slice(&encode_vint(0)); // attributes
        type_body.extend_from_slice(&data_crc.to_le_bytes()); // data CRC32
        type_body.extend_from_slice(&encode_vint(0)); // compression: v0, store, 128KB dict
        type_body.extend_from_slice(&encode_vint(1)); // host OS: Unix
        type_body.extend_from_slice(&encode_vint(filename.len() as u64));
        type_body.extend_from_slice(filename.as_bytes());

        // Common flags: DATA_AREA present (0x0002), with data size before the
        // type body.
        let common_flags: u64 = 0x0002;
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(2)); // header type = File
        body.extend_from_slice(&encode_vint(common_flags));
        body.extend_from_slice(&encode_vint(content.len() as u64)); // data area size
        body.extend_from_slice(&type_body);

        let header_size_bytes = encode_vint(body.len() as u64);
        let mut crc_input = header_size_bytes.clone();
        crc_input.extend_from_slice(&body);
        let crc = crc32(&crc_input);

        let mut result = Vec::new();
        result.extend_from_slice(&crc.to_le_bytes());
        result.extend_from_slice(&header_size_bytes);
        result.extend_from_slice(&body);
        result
    }

    /// Assemble a complete single-volume store-method RAR5 archive.
    fn build_stored_rar5(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut archive = Vec::new();
        // RAR5 signature.
        archive.extend_from_slice(&[0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00]);
        // Main archive header (type 1): archive_flags = 0.
        archive.extend_from_slice(&build_header(1, 0, &encode_vint(0)));
        // File headers + data areas.
        for (name, content) in entries {
            archive.extend_from_slice(&build_file_header(name, content));
            archive.extend_from_slice(content);
        }
        // End of archive header (type 5): end_flags = 0 (no more volumes).
        archive.extend_from_slice(&build_header(5, 0, &encode_vint(0)));
        archive
    }

    fn write_fixture(dir: &Path) -> PathBuf {
        let path = dir.join("fixture.rar");
        let second = second_content();
        let bytes =
            build_stored_rar5(&[("first.txt", FIRST_CONTENT), ("videos/second.bin", &second)]);
        std::fs::write(&path, bytes).expect("write rar fixture");
        path
    }

    fn handler_for(dir: &tempfile::TempDir, archive: PathBuf) -> RarHandler {
        RarHandler::new_with_config(
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

    #[tokio::test]
    async fn malformed_archive_errors_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broken.rar");
        // Valid signature, then garbage where headers should be.
        let mut bytes = vec![0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00];
        bytes.extend_from_slice(&[0xFF; 64]);
        std::fs::write(&path, bytes).unwrap();
        let handler = handler_for(&dir, path);

        // Must return an error, not panic (release is panic=abort).
        assert!(handler.list_files().await.is_err() || handler.open_file("x").await.is_err());
    }
}
