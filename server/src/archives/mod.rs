use anyhow::Result;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncRead, AsyncSeek};

pub mod bridge;
pub mod cache;
pub mod nzb;
#[cfg(feature = "rar")]
pub mod rar;
pub mod sessions;
pub mod sevenz;
pub mod source;
pub mod tar;
pub mod tgz;
pub mod zip;

pub use source::{ArchiveSession, ArchiveSource};

/// How long an archive or NZB session outlives its last use before it is
/// swept, with what it owns (see [`sessions`]).
///
/// A use is a request, or a response body still being read. The clock
/// therefore starts when the player has closed every connection to the
/// session, and what the timeout has to cover is the player that comes back
/// after that: one that fetches by fixed-size range and closes between
/// fetches, paused, or one restarting after an error. Its session key is in
/// the URL it holds and nothing else can mint that key again, so a session
/// swept under it is a failed resume. Ten minutes is long for a pause that
/// stays paused and short against what a session costs while it waits --
/// scratch files under the cache root that **only this sweep** unlinks
/// (see [`SCRATCH_DIR_NAME`]: nothing else in the process speaks for
/// them), and for NZB a pool of idle connections the news server is
/// likelier to close first.
pub const SESSION_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10 * 60);

/// Error message used when a RAR archive is requested but this binary was
/// built without the "rar" cargo feature.
#[cfg(not(feature = "rar"))]
pub const RAR_DISABLED_ERROR: &str =
    "RAR support is not compiled into this build (rebuild with the \"rar\" cargo feature)";

/// Represents a file inside an archive
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ArchiveEntry {
    pub path: String, // Internal path in archive
    pub size: u64,
    pub is_dir: bool,
}

/// Where the archive handlers write: the cache root, and under it the one
/// directory everything of theirs goes in.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// The cache root, from `settings.cacheRoot`. The archive scratch dir
    /// goes directly under it ([`CacheConfig::scratch_dir`]) -- beside the
    /// torrent-data root, not inside it.
    pub cache_dir: PathBuf,
    /// Maximum cache size in bytes (0 = disabled)
    pub _cache_size: u64,
}

/// The directory under the cache root that holds what the archive routes put
/// on disk: archives downloaded whole, and members extracted from them.
///
/// Under the cache root, and not the system temp dir, on purpose. Nothing
/// here is precious -- every byte can be fetched or extracted again from
/// what the session names -- so it belongs on the volume the server sizes
/// its cap against rather than on one it knows nothing about. The system
/// temp dir gave none of that and, on Android, is not writable by an app
/// at all.
///
/// **It is beside the torrent-data root, not inside it.** `cache_dir` is
/// `settings.cacheRoot`, and the store's root is
/// `<cacheRoot>/rqbit-downloads/.pieces`, so nothing that counts the cache
/// has ever counted a byte of this: not `GET /cache.json`, and not the
/// walk that used to run over `<cacheRoot>/rqbit-downloads` before it was
/// deleted -- that walk never came in here either.
///
/// **What is under it owns itself while the process lives, and the next
/// launch takes the rest.** Every file here is a `tempfile::NamedTempFile`
/// -- the download's and the extraction's alike -- so it is unlinked when
/// the session holding it drops, which is when the idle sweep takes that
/// session ([`SESSION_IDLE_TIMEOUT`], `crate::archives::sessions`). No
/// retention owner speaks for these bytes: they are neither a torrent's
/// pieces nor a proxied entity. That deleter is process memory, and a
/// process that is killed -- which Android's low-memory killer does as a
/// matter of course -- drops nothing: every archive played since the last
/// clean exit stayed here, about twice its size (the download and the
/// extraction), counted by nobody. So [`sweep_scratch`] empties the
/// directory at launch, before the router can open a session: session keys
/// are minted per process, so nothing a previous one left here is
/// addressable by any request this one can receive.
pub const SCRATCH_DIR_NAME: &str = ".archives";

/// Delete everything a previous process left under `<cache_dir>/.archives`
/// -- see [`SCRATCH_DIR_NAME`] for why there is anything to delete, and why
/// none of it can be wanted. A directory that is not there is the sweep's
/// own result, not a failure.
pub fn sweep_scratch(cache_dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(cache_dir.join(SCRATCH_DIR_NAME)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

impl CacheConfig {
    /// `<cache root>/.archives` -- see [`SCRATCH_DIR_NAME`].
    pub fn scratch_dir(&self) -> PathBuf {
        self.cache_dir.join(SCRATCH_DIR_NAME)
    }
}

/// The suffixes the readers are chosen by, longest first so `.tar.gz` is
/// found before `.gz` would not be.
const ARCHIVE_SUFFIXES: [&str; 7] = [".tar.gz", ".tgz", ".zip", ".rar", ".7z", ".tar", ".nzb"];

/// The recognised archive suffix `name` ends with (case-insensitively), as
/// written in [`ARCHIVE_SUFFIXES`], or `None` when it has none. `name` may
/// be a whole path or URL path; only its end is looked at.
pub fn archive_suffix(name: &str) -> Option<&'static str> {
    let lower = name.to_lowercase();
    ARCHIVE_SUFFIXES
        .iter()
        .copied()
        .find(|suffix| lower.ends_with(suffix))
}

/// The archive suffix the first bytes of a file say it should have, by the
/// signatures the formats put at their start (tar's `ustar` is at offset
/// 257, so `head` should be at least that long to find one).
///
/// A download is named by the URL it came from, and a URL that ends in an
/// id rather than a filename says nothing about the format -- while the
/// bytes always do. NZB is XML and has no signature to find here.
pub fn archive_suffix_from_magic(head: &[u8]) -> Option<&'static str> {
    if head.starts_with(b"Rar!\x1a\x07") {
        Some(".rar")
    } else if head.starts_with(b"PK\x03\x04") {
        Some(".zip")
    } else if head.starts_with(b"7z\xbc\xaf\x27\x1c") {
        Some(".7z")
    } else if head.starts_with(b"\x1f\x8b") {
        Some(".tar.gz")
    } else if head.len() >= 262 && &head[257..262] == b"ustar" {
        Some(".tar")
    } else {
        None
    }
}

/// A new, uniquely named file in the scratch directory with the given
/// archive suffix, for a download to land in. Deleted when dropped, so a
/// download that fails part way leaves nothing behind.
pub fn scratch_file(
    cache_config: &CacheConfig,
    suffix: &str,
) -> std::io::Result<tempfile::NamedTempFile> {
    let dir = cache_config.scratch_dir();
    std::fs::create_dir_all(&dir)?;
    tempfile::Builder::new()
        .prefix("archive_")
        .suffix(suffix)
        .tempfile_in(dir)
}

/// Trait combining AsyncRead, AsyncSeek, Send, Sync, and Unpin for trait objects
pub trait AsyncSeekableReader: AsyncRead + AsyncSeek + Unpin + Send {}
impl<T: AsyncRead + AsyncSeek + Unpin + Send> AsyncSeekableReader for T {}

/// A member opened by [`ArchiveReader::open_file`].
///
/// Most formats have to decode the member, and do so into a
/// [`cache::ProgressiveCache`] that any number of readers can be taken from
/// while the decoding runs. The cache is what `open_file` returns, not a
/// reader from it, so the caller can keep it and serve every later request
/// for the same member -- a player's range requests, one per seek -- from
/// the one extraction rather than starting another (see
/// `ArchiveSource::open_member`). A format that needs no decoding (a stored
/// TAR member is a slice of the archive) returns a reader over the archive
/// itself; there is nothing to keep.
pub enum OpenedMember {
    Extracted(cache::ProgressiveCache),
    Direct(Box<dyn AsyncSeekableReader>),
}

impl OpenedMember {
    /// One reader, for a caller with nothing to keep the cache in.
    pub async fn into_reader(self) -> Result<Box<dyn AsyncSeekableReader>> {
        match self {
            Self::Extracted(cache) => Ok(Box::new(cache.reader().await?)),
            Self::Direct(reader) => Ok(reader),
        }
    }
}

/// Trait for Archive implementations
#[async_trait]
pub trait ArchiveReader: Send + Sync {
    /// List all files in the archive
    async fn list_files(&self) -> Result<Vec<ArchiveEntry>>;

    /// Open a specific file inside the archive -- see [`OpenedMember`].
    async fn open_file(&self, path: &str) -> Result<OpenedMember>;
}

/// Create an archive reader with custom cache configuration
pub async fn get_archive_reader_with_config(
    path: &Path,
    cache_config: CacheConfig,
) -> Result<Box<dyn ArchiveReader>> {
    // The reader is chosen by suffix, which is why a download has to be
    // given one (`scratch_file`).
    match archive_suffix(&path.to_string_lossy()) {
        Some(".zip") => {
            tracing::info!("Archive detected: ZIP at {:?}", path);
            Ok(Box::new(zip::ZipHandler::new(
                path.to_path_buf(),
                cache_config,
            )))
        }
        Some(".rar") => {
            #[cfg(feature = "rar")]
            {
                tracing::info!(
                    "Archive detected: RAR at {:?}, cache_dir={:?}",
                    path,
                    cache_config.cache_dir
                );
                Ok(Box::new(rar::RarHandler::new_with_config(
                    path.to_path_buf(),
                    cache_config,
                )))
            }
            #[cfg(not(feature = "rar"))]
            {
                tracing::warn!(
                    "RAR archive requested but RAR support is not compiled in: {:?}",
                    path
                );
                Err(anyhow::anyhow!(RAR_DISABLED_ERROR))
            }
        }
        Some(".7z") => {
            tracing::info!(
                "Archive detected: 7z at {:?}, cache_dir={:?}",
                path,
                cache_config.cache_dir
            );
            Ok(Box::new(sevenz::SevenZHandler::new_with_config(
                path.to_path_buf(),
                cache_config,
            )))
        }
        Some(".tar") => {
            tracing::info!("Archive detected: TAR at {:?}", path);
            Ok(Box::new(tar::TarHandler::new(path.to_path_buf()))) // TODO: Async Tar
        }
        Some(".tar.gz" | ".tgz") => {
            tracing::info!("Archive detected: TGZ at {:?}", path);
            Ok(Box::new(tgz::TgzHandler::new(
                path.to_path_buf(),
                cache_config,
            ))) // TODO: Async Tgz
        }
        Some(".nzb") => {
            tracing::info!("Archive detected: NZB at {:?}", path);
            Ok(Box::new(nzb::NzbHandler::new(path.to_path_buf())))
        }
        Some(_) | None => {
            tracing::info!("Normal file detected (not an archive): {:?}", path);
            Err(anyhow::anyhow!(
                "Unsupported archive type: {:?}",
                path.extension()
            ))
        }
    }
}

pub fn get_archive_reader_from_stream(
    reader: Box<dyn AsyncSeekableReader>,
    extension: &str,
    cache_config: CacheConfig,
) -> Result<Box<dyn ArchiveReader>> {
    let ext = extension.to_lowercase();
    if ext == "zip" {
        Ok(Box::new(zip::ZipHandler::new_with_reader(
            reader,
            cache_config,
        )))
    } else if ext == "7z" {
        Ok(Box::new(sevenz::SevenZHandler::new_with_reader(
            reader,
            cache_config,
        )))
    } else {
        Err(anyhow::anyhow!(
            "Unsupported archive type for streaming: .{}",
            ext
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_suffix_is_found_at_the_end_whatever_the_case_or_the_prefix() {
        assert_eq!(archive_suffix("Movie.RAR"), Some(".rar"));
        assert_eq!(archive_suffix("/tmp/archive_x9.7z"), Some(".7z"));
        assert_eq!(archive_suffix("/dl/show.tar.gz"), Some(".tar.gz"));
        assert_eq!(archive_suffix("show.tgz"), Some(".tgz"));
        assert_eq!(archive_suffix("index.nzb"), Some(".nzb"));
        assert_eq!(archive_suffix("/download?id=1"), None);
        assert_eq!(archive_suffix("movie.mkv"), None);
        assert_eq!(archive_suffix("archive.zip.txt"), None);
    }

    #[test]
    fn the_first_bytes_name_the_format_a_suffixless_url_did_not() {
        assert_eq!(
            archive_suffix_from_magic(b"Rar!\x1a\x07\x01\x00rest"),
            Some(".rar")
        );
        assert_eq!(archive_suffix_from_magic(b"PK\x03\x04rest"), Some(".zip"));
        assert_eq!(
            archive_suffix_from_magic(b"7z\xbc\xaf\x27\x1c\x00\x04"),
            Some(".7z")
        );
        assert_eq!(archive_suffix_from_magic(b"\x1f\x8b\x08"), Some(".tar.gz"));
        let mut tar = vec![0u8; 512];
        tar[257..262].copy_from_slice(b"ustar");
        assert_eq!(archive_suffix_from_magic(&tar), Some(".tar"));
        assert_eq!(archive_suffix_from_magic(b"<html>"), None);
        assert_eq!(archive_suffix_from_magic(b""), None);
    }

    /// A scratch file lands under `<cache root>/.archives` with the suffix
    /// the reader dispatch needs, and goes when dropped.
    #[test]
    fn a_scratch_file_is_under_the_cache_root_and_is_deleted_on_drop() {
        let root = tempfile::tempdir().unwrap();
        let config = CacheConfig {
            cache_dir: root.path().to_path_buf(),
            _cache_size: 0,
        };
        let file = scratch_file(&config, ".zip").unwrap();
        let path = file.path().to_path_buf();
        assert_eq!(path.parent(), Some(root.path().join(".archives").as_path()));
        assert_eq!(archive_suffix(&path.to_string_lossy()), Some(".zip"));
        assert!(path.exists());
        drop(file);
        assert!(!path.exists());
    }
}
