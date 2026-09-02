use anyhow::Result;
use async_trait::async_trait;
use std::path::Path;
use tokio::io::{AsyncRead, AsyncSeek};

pub mod bridge;
pub mod cache;
pub mod nzb;
#[cfg(feature = "rar")]
pub mod rar;
pub mod sevenz;
pub mod tar;
pub mod tgz;
pub mod zip;

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

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ArchiveSession {
    pub path: std::path::PathBuf,
    pub selected_file: Option<String>,
    pub created: std::time::Instant,
}

/// Configuration for archive caching behavior
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Directory where cache files should be written
    pub cache_dir: Option<std::path::PathBuf>,
    /// Maximum cache size in bytes (0 = disabled)
    pub _cache_size: u64,
}

impl CacheConfig {
    /// Get the cache directory, falling back to system temp if not configured
    pub fn get_dir_or_temp(&self) -> std::path::PathBuf {
        self.cache_dir.clone().unwrap_or_else(std::env::temp_dir)
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            cache_dir: None,
            _cache_size: 10 * 1024 * 1024 * 1024, // 10GB default
        }
    }
}

/// Trait combining AsyncRead, AsyncSeek, Send, Sync, and Unpin for trait objects
pub trait AsyncSeekableReader: AsyncRead + AsyncSeek + Unpin + Send {}
impl<T: AsyncRead + AsyncSeek + Unpin + Send> AsyncSeekableReader for T {}

/// Trait for Archive implementations
#[async_trait]
pub trait ArchiveReader: Send + Sync {
    /// List all files in the archive
    async fn list_files(&self) -> Result<Vec<ArchiveEntry>>;

    /// Open a stream for a specific file inside the archive
    async fn open_file(&self, path: &str) -> Result<Box<dyn AsyncSeekableReader>>;
}

/// Create an archive reader with custom cache configuration
pub async fn get_archive_reader_with_config(
    path: &Path,
    cache_config: CacheConfig,
) -> Result<Box<dyn ArchiveReader>> {
    let path_str = path.to_string_lossy().to_lowercase();

    // For local files, we open them here to pass into the new async handlers if possible,
    // OR the handlers themselves open the file (backward compat logic).
    // But since we are rewriting handlers to be async, let's try to unify.
    // However, some handlers (like unrar wrapper) might still want a path.
    // For now, we'll keep the path-based factory but implementation might change.

    if path_str.ends_with(".zip") {
        tracing::info!("Archive detected: ZIP at {:?}", path);
        Ok(Box::new(zip::ZipHandler::new(path.to_path_buf())))
    } else if path_str.ends_with(".rar") {
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
    } else if path_str.ends_with(".7z") {
        tracing::info!(
            "Archive detected: 7z at {:?}, cache_dir={:?}",
            path,
            cache_config.cache_dir
        );
        Ok(Box::new(sevenz::SevenZHandler::new_with_config(
            path.to_path_buf(),
            cache_config,
        )))
    } else if path_str.ends_with(".tar") {
        tracing::info!("Archive detected: TAR at {:?}", path);
        Ok(Box::new(tar::TarHandler::new(path.to_path_buf()))) // TODO: Async Tar
    } else if path_str.ends_with(".tar.gz") || path_str.ends_with(".tgz") {
        tracing::info!("Archive detected: TGZ at {:?}", path);
        Ok(Box::new(tgz::TgzHandler::new(path.to_path_buf()))) // TODO: Async Tgz
    } else if path_str.ends_with(".nzb") {
        tracing::info!("Archive detected: NZB at {:?}", path);
        Ok(Box::new(nzb::NzbHandler::new(path.to_path_buf())))
    } else {
        tracing::info!("Normal file detected (not an archive): {:?}", path);
        Err(anyhow::anyhow!(
            "Unsupported archive type: {:?}",
            path.extension()
        ))
    }
}

pub fn get_archive_reader_from_stream(
    reader: Box<dyn AsyncSeekableReader>,
    extension: &str,
) -> Result<Box<dyn ArchiveReader>> {
    let ext = extension.to_lowercase();
    if ext == "zip" {
        Ok(Box::new(zip::ZipHandler::new_with_reader(reader)))
    } else if ext == "7z" {
        Ok(Box::new(sevenz::SevenZHandler::new_with_reader(reader)))
    } else {
        Err(anyhow::anyhow!(
            "Unsupported archive type for streaming: .{}",
            ext
        ))
    }
}
