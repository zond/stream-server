//! What an archive session owns, and for how long.
//!
//! An archive session used to be a path and a chosen member. When the path
//! was a download, nothing owned the file: `NamedTempFile::keep` handed it
//! to the filesystem and the session forgot it, so every `/create` of an
//! archive by URL left the whole archive in the system temp dir until
//! reboot -- once per attempt, since nothing looked for an earlier copy --
//! and a failed create (which every one of them was: the file had no suffix
//! for the reader dispatch to go by) left it there without even a session
//! to name it.
//!
//! [`ArchiveSource`] is the archive as a thing with an owner. A downloaded
//! source holds its `NamedTempFile`, so the file is deleted when the last
//! `Arc<ArchiveSource>` is -- that is, when every session sharing it has
//! been swept (see [`super::sessions`]) and every response body reading from
//! it has ended. Sessions created for the same origin share one source
//! through [`ArchiveSession::source`], which is the deduplication: the
//! second `/create` of a URL costs no second download.

use super::{ArchiveReader, CacheConfig};
use anyhow::Result;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;

/// An archive on disk: where it is, what it was created from, and -- when
/// this server fetched it -- the file itself, deleted with the last owner.
pub struct ArchiveSource {
    path: PathBuf,
    /// The URL or local path the client named. Two sessions naming the same
    /// origin share one source.
    origin: String,
    /// `Some` for a download: owning it is what deletes the file on drop.
    _download: Option<NamedTempFile>,
    cache_config: CacheConfig,
}

impl ArchiveSource {
    /// An archive that was already on disk; nothing here owns the file.
    pub fn local(path: PathBuf, origin: String, cache_config: CacheConfig) -> Self {
        Self {
            path,
            origin,
            _download: None,
            cache_config,
        }
    }

    /// An archive this server downloaded into `file` (from
    /// [`super::scratch_file`], so it has the suffix the reader is chosen
    /// by). The file lives as long as the source does.
    pub fn downloaded(file: NamedTempFile, origin: String, cache_config: CacheConfig) -> Self {
        Self {
            path: file.path().to_path_buf(),
            origin,
            _download: Some(file),
            cache_config,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// A reader over the archive, chosen by the path's suffix.
    pub async fn reader(&self) -> Result<Box<dyn ArchiveReader>> {
        super::get_archive_reader_with_config(&self.path, self.cache_config.clone()).await
    }
}

/// One `/create`: the archive and the member chosen for it.
pub struct ArchiveSession {
    pub source: Arc<ArchiveSource>,
    pub selected_file: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archives::sessions::Sessions;
    use crate::archives::{SESSION_IDLE_TIMEOUT, scratch_file};

    fn config(root: &Path) -> CacheConfig {
        CacheConfig {
            cache_dir: root.to_path_buf(),
            _cache_size: 0,
        }
    }

    /// A downloaded archive stays on disk while a session, or a second
    /// session sharing the source, or a reader's clone of the source is
    /// alive, and goes with the last of them -- through the registry's
    /// sweep, the way the server lets go of it.
    #[tokio::test(start_paused = true)]
    async fn the_download_is_deleted_when_the_last_session_sharing_it_goes() {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let file = scratch_file(&config, ".zip").unwrap();
        let path = file.path().to_path_buf();
        let source = Arc::new(ArchiveSource::downloaded(
            file,
            "http://example.invalid/a.zip".into(),
            config,
        ));

        let sessions = Sessions::new(SESSION_IDLE_TIMEOUT);
        sessions.insert(
            "first".into(),
            ArchiveSession {
                source: source.clone(),
                selected_file: None,
            },
        );
        sessions.insert(
            "second".into(),
            ArchiveSession {
                source: source.clone(),
                selected_file: Some("x".into()),
            },
        );
        drop(source);
        assert!(path.exists());

        // The first session goes idle and is swept; the second is in use.
        let in_use = sessions.get("second").unwrap();
        tokio::time::advance(SESSION_IDLE_TIMEOUT).await;
        sessions.sweep(tokio::time::Instant::now());
        assert!(sessions.get("first").is_none());
        assert!(path.exists(), "the second session still owns the file");

        drop(in_use);
        tokio::time::advance(SESSION_IDLE_TIMEOUT).await;
        sessions.sweep(tokio::time::Instant::now());
        assert!(sessions.is_empty());
        assert!(!path.exists(), "nothing owns it any more");
    }

    /// A source over an archive the caller already had on disk owns nothing
    /// and deletes nothing.
    #[test]
    fn a_local_source_leaves_the_file_alone() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("mine.zip");
        std::fs::write(&path, b"PK").unwrap();
        let source = ArchiveSource::local(
            path.clone(),
            path.to_string_lossy().into_owned(),
            config(root.path()),
        );
        drop(source);
        assert!(path.exists());
    }
}
