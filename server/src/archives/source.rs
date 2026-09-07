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

use super::cache::ProgressiveCache;
use super::{ArchiveReader, AsyncSeekableReader, CacheConfig, OpenedMember};
use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::sync::Mutex;

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
    /// The members extracted so far, by name -- see [`Self::open_member`].
    /// Held for the life of the source, so their files are too; the mutex
    /// is held across an open so two requests racing for a member that is
    /// not there yet start one extraction, not two.
    members: Mutex<HashMap<String, ProgressiveCache>>,
}

impl ArchiveSource {
    /// An archive that was already on disk; nothing here owns the file.
    pub fn local(path: PathBuf, origin: String, cache_config: CacheConfig) -> Self {
        Self {
            path,
            origin,
            _download: None,
            cache_config,
            members: Mutex::new(HashMap::new()),
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
            members: Mutex::new(HashMap::new()),
        }
    }

    /// A reader over `member`, from the one extraction of it this source
    /// keeps.
    ///
    /// Every HTTP request used to be its own extraction: the handler decoded
    /// the whole member into a fresh scratch file per `open_file`, so a
    /// player's ordinary opening -- the head, then the tail for its index,
    /// then one request per seek -- ran that many decodings at once and put
    /// that many copies of the member on disk, for minutes of a slow SoC and
    /// several times the member in flash. Here the first request's
    /// extraction is kept in `members` and every later request takes a
    /// reader from it, whether it is still being written or finished.
    ///
    /// Two things replace an entry. A cache that has failed -- decoding
    /// failed, or nothing read it for long enough that the writer gave up
    /// (`cache::ABANDONED_AFTER`) -- would only tell a new reader so, and is
    /// extracted again. And a cache whose file is gone: the file is under
    /// the cache root, where the cleaner is free to evict it, and a reader
    /// opens it by path.
    pub async fn open_member(&self, member: &str) -> Result<Box<dyn AsyncSeekableReader>> {
        let mut members = self.members.lock().await;
        members.retain(|_, cache| !cache.is_failed());
        if let Some(cache) = members.get(member) {
            match cache.reader().await {
                Ok(reader) => return Ok(Box::new(reader)),
                Err(error) => {
                    tracing::debug!(
                        archive = %self.path.display(),
                        member,
                        %error,
                        "extracted member is gone from disk; extracting again"
                    );
                    members.remove(member);
                }
            }
        }
        match self.reader().await?.open_file(member).await? {
            OpenedMember::Extracted(cache) => {
                let reader = cache.reader().await?;
                members.insert(member.to_string(), cache);
                Ok(Box::new(reader))
            }
            OpenedMember::Direct(reader) => Ok(reader),
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

    /// A two-member 7z on disk, for tests that open members.
    fn write_7z(dir: &Path) -> PathBuf {
        use sevenz_rust2::{ArchiveEntry, ArchiveWriter};
        let path = dir.join("fixture.7z");
        let mut writer = ArchiveWriter::create(&path).expect("create 7z writer");
        writer
            .push_archive_entry(
                ArchiveEntry::new_file("first.txt"),
                Some(std::io::Cursor::new(b"first".to_vec())),
            )
            .expect("push first entry");
        writer
            .push_archive_entry(
                ArchiveEntry::new_file("second.bin"),
                Some(std::io::Cursor::new(second_content())),
            )
            .expect("push second entry");
        writer.finish().expect("finish 7z archive");
        path
    }

    fn second_content() -> Vec<u8> {
        (0..64 * 1024u32)
            .map(|i| (i.wrapping_mul(31) % 251) as u8)
            .collect()
    }

    /// The extraction files under the scratch directory.
    fn extractions(root: &Path) -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir(root.join(crate::archives::SCRATCH_DIR_NAME)) else {
            return Vec::new();
        };
        entries
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("archive_extract_"))
            })
            .collect()
    }

    async fn read_all(mut reader: Box<dyn AsyncSeekableReader>) -> Vec<u8> {
        use tokio::io::AsyncReadExt;
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await.expect("read member");
        data
    }

    /// Three opens of one member -- a player's head, tail and seek -- are
    /// one extraction and one file on disk, and every reader gets the
    /// member; a different member is its own extraction. Dropping the
    /// source drops them all.
    #[tokio::test]
    async fn a_member_is_extracted_once_per_source() {
        let root = tempfile::tempdir().unwrap();
        let archive = write_7z(root.path());
        let source = ArchiveSource::local(
            archive.clone(),
            archive.to_string_lossy().into_owned(),
            config(root.path()),
        );

        let a = source.open_member("second.bin").await.expect("open");
        let b = source.open_member("second.bin").await.expect("open");
        let c = source.open_member("second.bin").await.expect("open");
        assert_eq!(extractions(root.path()).len(), 1, "one extraction");
        for reader in [a, b, c] {
            assert_eq!(read_all(reader).await, second_content());
        }
        assert_eq!(
            extractions(root.path()).len(),
            1,
            "kept after the readers are done, for the next request"
        );

        let first = source.open_member("first.txt").await.expect("open");
        assert_eq!(read_all(first).await, b"first");
        assert_eq!(extractions(root.path()).len(), 2);

        drop(source);
        assert!(extractions(root.path()).is_empty(), "gone with the source");
    }

    /// The cleaner may evict an extraction file from under the source; the
    /// next request extracts again rather than answering 404 for the rest
    /// of the session.
    #[tokio::test]
    async fn an_evicted_extraction_is_redone() {
        let root = tempfile::tempdir().unwrap();
        let archive = write_7z(root.path());
        let source = ArchiveSource::local(
            archive.clone(),
            archive.to_string_lossy().into_owned(),
            config(root.path()),
        );

        let reader = source.open_member("second.bin").await.expect("open");
        assert_eq!(read_all(reader).await, second_content());
        for file in extractions(root.path()) {
            std::fs::remove_file(file).unwrap();
        }

        let again = source.open_member("second.bin").await.expect("open again");
        assert_eq!(read_all(again).await, second_content());
        assert_eq!(extractions(root.path()).len(), 1);
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
