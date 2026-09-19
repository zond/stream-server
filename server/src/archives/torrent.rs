//! An archive inside a torrent, as something with an owner.
//!
//! The `torrent:` form of the archive stream route
//! (`crate::routes::archive::stream_file`) names an archive by the torrent
//! it is in and the path it has inside it, and has no `/create` and so no
//! session: until this module every request opened a reader on the live
//! torrent, built an archive reader over it and extracted the member again.
//! A player seeking in a film therefore paid a whole extraction per range
//! request -- held to the volume's free-space floor since review #18, but
//! paid again and again, each one a second copy of the film written under
//! the cache root and each one reading the torrent from the archive's start.
//!
//! [`TorrentArchives`] gives that form the ownership the URL form has had
//! all along. One [`TorrentArchive`] per (info hash, path inside the
//! torrent) holds the extractions of its members
//! ([`super::source::MemberCaches`], the same one the URL form uses), so the
//! first request extracts and every later one -- the tail read, the seek
//! back into bytes already extracted -- reads the extraction that is there.
//!
//! **What bounds it.** Disk: an extraction is a `ProgressiveCache` under
//! `<cacheRoot>/.archives`, held to the volume's free-space floor by
//! `cache::VolumeRoom`, and there is now one per member rather than one per
//! request. Lifetime: a session is leased for as long as a response body
//! reads from it and swept once nothing has for
//! [`super::SESSION_IDLE_TIMEOUT`] (`super::sessions`), and the sweep drops
//! the caches, which unlinks the files. Beyond that the scratch directory's
//! own two rules apply unchanged: a process that is killed leaves files that
//! the next launch's `super::sweep_scratch` deletes before any route can
//! run, and an extraction nothing reads for `cache::ABANDONED_AFTER` gives
//! up rather than decoding a whole member for a player that has gone.
//!
//! **What it does not outlive.** Nothing here holds a torrent, a reader on
//! one or any engine state: the route looks the torrent up and registers its
//! stream on every request, before it comes here, so a torrent that has been
//! removed or an engine that has been swept for idleness is a `404` with the
//! session untouched -- and then idle, and then swept. The session can hold
//! an extraction of a member of a torrent that is gone for as long as that
//! sweep takes; the bytes are the same disposable scratch bytes the URL
//! form's are, under the same floor, and nothing can address them but a
//! request naming a torrent the engine does not have.

use super::sessions::{Lease, Sessions};
use super::source::MemberCaches;
use super::{AsyncSeekableReader, CacheConfig};
use anyhow::Result;
use std::future::Future;
use std::time::Duration;

/// One archive inside one torrent: the extractions of its members.
#[derive(Default)]
pub struct TorrentArchive {
    members: MemberCaches,
}

/// The archives inside torrents that are being read right now, swept when
/// idle. One per server, in `crate::state::AppState`.
#[derive(Clone)]
pub struct TorrentArchives {
    sessions: Sessions<TorrentArchive>,
}

impl TorrentArchives {
    pub fn new(idle_timeout: Duration) -> Self {
        Self {
            sessions: Sessions::new(idle_timeout),
        }
    }

    /// A reader over `member` of the archive at `path` inside the torrent
    /// `info_hash`, and the session lease it came from.
    ///
    /// The lease is the caller's to hold for as long as the response body
    /// reads: while it lives the session cannot be swept, and the idle clock
    /// starts when it is dropped (see [`Lease`]).
    ///
    /// `open_reader` is called only when there is no extraction to read
    /// from, so the reader on the torrent -- and the read of the archive's
    /// central directory that follows it -- is paid once per member rather
    /// than once per request. `extension` chooses the archive reader, as
    /// everywhere else in this module.
    pub async fn open_member<F, Fut>(
        &self,
        info_hash: &str,
        path: &str,
        member: &str,
        extension: &str,
        cache_config: CacheConfig,
        open_reader: F,
    ) -> Result<(Box<dyn AsyncSeekableReader>, Lease<TorrentArchive>)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Box<dyn AsyncSeekableReader>>>,
    {
        let key = format!("{info_hash}/{path}");
        let session = self
            .sessions
            .get_or_insert_with(&key, TorrentArchive::default);
        let extension = extension.to_string();
        let reader = session
            .members
            .open(member, &key, || async move {
                let source = open_reader().await?;
                super::get_archive_reader_from_stream(source, &extension, cache_config)
            })
            .await?;
        Ok((reader, session))
    }

    /// Remove every session nothing has read for the idle timeout, as of
    /// `now` -- what the registry's own janitor does on its schedule.
    #[cfg(test)]
    pub fn sweep(&self, now: tokio::time::Instant) {
        self.sessions.sweep(now);
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.sessions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archives::SESSION_IDLE_TIMEOUT;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    fn config(root: &Path) -> CacheConfig {
        CacheConfig {
            cache_dir: root.to_path_buf(),
            _cache_size: 0,
        }
    }

    fn content(len: usize) -> Vec<u8> {
        (0..len as u32)
            .map(|i| (i.wrapping_mul(31) % 251) as u8)
            .collect()
    }

    /// A zip holding one member, written with `compression`.
    async fn zip_bytes(name: &str, data: &[u8], compression: async_zip::Compression) -> Vec<u8> {
        let mut writer = async_zip::base::write::ZipFileWriter::new(Vec::new());
        writer
            .write_entry_whole(
                async_zip::ZipEntryBuilder::new(name.into(), compression),
                data,
            )
            .await
            .unwrap();
        writer.close().await.unwrap()
    }

    /// The archive as a torrent would hand it over: a reader that counts
    /// how many times one was opened on the torrent, and how many bytes
    /// were read out of it.
    #[derive(Default)]
    struct TorrentReads {
        readers: AtomicUsize,
        bytes: AtomicUsize,
    }

    struct CountingReader {
        inner: std::io::Cursor<Vec<u8>>,
        reads: Arc<TorrentReads>,
    }

    impl tokio::io::AsyncRead for CountingReader {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let before = buf.filled().len();
            let poll = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
            if let std::task::Poll::Ready(Ok(())) = poll {
                let read = buf.filled().len() - before;
                self.reads.bytes.fetch_add(read, Ordering::SeqCst);
            }
            poll
        }
    }

    impl tokio::io::AsyncSeek for CountingReader {
        fn start_seek(
            mut self: std::pin::Pin<&mut Self>,
            position: std::io::SeekFrom,
        ) -> std::io::Result<()> {
            std::pin::Pin::new(&mut self.inner).start_seek(position)
        }
        fn poll_complete(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<u64>> {
            std::pin::Pin::new(&mut self.inner).poll_complete(cx)
        }
    }

    /// The extraction files under the scratch directory.
    fn extractions(root: &Path) -> Vec<std::path::PathBuf> {
        let Ok(entries) = std::fs::read_dir(root.join(crate::archives::SCRATCH_DIR_NAME)) else {
            return Vec::new();
        };
        entries.map(|entry| entry.unwrap().path()).collect()
    }

    async fn read_range(
        reader: &mut Box<dyn AsyncSeekableReader>,
        start: u64,
        len: usize,
    ) -> Vec<u8> {
        reader.seek(std::io::SeekFrom::Start(start)).await.unwrap();
        let mut out = vec![0u8; len];
        reader.read_exact(&mut out).await.unwrap();
        out
    }

    /// **Two ranged requests for one member are one extraction** (review
    /// #18's leftover). Before this the `torrent:` form had no session, so
    /// each request opened its own reader on the torrent, read the
    /// archive's directory again and decoded the whole member again -- a
    /// seek in a film cost a second copy of the film.
    #[tokio::test]
    async fn a_second_range_on_a_member_costs_no_second_extraction() {
        let root = tempfile::tempdir().unwrap();
        let member = content(512 * 1024);
        let archive = zip_bytes("videos/film.bin", &member, async_zip::Compression::Deflate).await;
        let reads = Arc::new(TorrentReads::default());
        let archives = TorrentArchives::new(SESSION_IDLE_TIMEOUT);

        let open = || {
            let archive = archive.clone();
            let reads = reads.clone();
            move || {
                let archive = archive.clone();
                let reads = reads.clone();
                async move {
                    reads.readers.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::new(CountingReader {
                        inner: std::io::Cursor::new(archive),
                        reads,
                    }) as Box<dyn AsyncSeekableReader>)
                }
            }
        };
        let hash = "ab".repeat(20);

        // The head of the film, as a player asks for it first.
        let (mut first, _lease) = archives
            .open_member(
                &hash,
                "Film.zip",
                "videos/film.bin",
                "zip",
                config(root.path()),
                open(),
            )
            .await
            .expect("the member opens");
        assert_eq!(read_range(&mut first, 0, 4096).await, member[..4096]);
        assert_eq!(reads.readers.load(Ordering::SeqCst), 1);
        assert_eq!(extractions(root.path()).len(), 1);

        // And then a range from the middle of it: the same session, the
        // same extraction, and nothing opened on the torrent for it.
        let (mut second, _lease) = archives
            .open_member(
                &hash,
                "Film.zip",
                "videos/film.bin",
                "zip",
                config(root.path()),
                open(),
            )
            .await
            .expect("the member opens again");
        assert_eq!(
            read_range(&mut second, 256 * 1024, 4096).await,
            member[256 * 1024..256 * 1024 + 4096]
        );
        assert_eq!(
            reads.readers.load(Ordering::SeqCst),
            1,
            "the second range opened a second reader on the torrent"
        );
        assert_eq!(
            extractions(root.path()).len(),
            1,
            "the second range extracted the member again"
        );
        assert_eq!(archives.len(), 1);

        // A different archive in the same torrent is its own session, and a
        // different member its own extraction.
        let other = zip_bytes("other.bin", &content(1024), async_zip::Compression::Deflate).await;
        let reads_other = Arc::new(TorrentReads::default());
        let (mut other_reader, _lease) = archives
            .open_member(
                &hash,
                "Other.zip",
                "other.bin",
                "zip",
                config(root.path()),
                || async move {
                    Ok(Box::new(CountingReader {
                        inner: std::io::Cursor::new(other),
                        reads: reads_other,
                    }) as Box<dyn AsyncSeekableReader>)
                },
            )
            .await
            .expect("the other archive opens");
        assert_eq!(read_range(&mut other_reader, 0, 1024).await, content(1024));
        assert_eq!(archives.len(), 2);
        assert_eq!(extractions(root.path()).len(), 2);
    }

    /// **A stored member is read from the torrent itself.** No extraction,
    /// nothing under the cache root, and a range request reads the archive
    /// around that range rather than from its start -- which is what a film
    /// put in a ZIP looks like, since a film does not compress.
    #[tokio::test]
    async fn a_stored_member_is_served_from_the_torrent_with_no_extraction() {
        let root = tempfile::tempdir().unwrap();
        let member = content(512 * 1024);
        let archive = zip_bytes("videos/film.bin", &member, async_zip::Compression::Stored).await;
        let reads = Arc::new(TorrentReads::default());
        let archives = TorrentArchives::new(SESSION_IDLE_TIMEOUT);
        let hash = "cd".repeat(20);

        let (mut reader, _lease) = archives
            .open_member(
                &hash,
                "Film.zip",
                "videos/film.bin",
                "zip",
                config(root.path()),
                {
                    let archive = archive.clone();
                    let reads = reads.clone();
                    move || async move {
                        Ok(Box::new(CountingReader {
                            inner: std::io::Cursor::new(archive),
                            reads,
                        }) as Box<dyn AsyncSeekableReader>)
                    }
                },
            )
            .await
            .expect("the member opens");

        // The member's own length and coordinates, out of a window over the
        // archive.
        assert_eq!(
            reader.seek(std::io::SeekFrom::End(0)).await.unwrap(),
            member.len() as u64
        );
        let middle = read_range(&mut reader, 256 * 1024, 4096).await;
        assert_eq!(middle, member[256 * 1024..256 * 1024 + 4096]);

        assert!(
            extractions(root.path()).is_empty(),
            "a stored member is not extracted: {:?}",
            extractions(root.path())
        );
        assert!(
            reads.bytes.load(Ordering::SeqCst) < member.len(),
            "the whole member was read out of the torrent for a 4 KiB range"
        );
    }

    /// A session nothing reads goes, and its extraction goes with it; one a
    /// body still holds a lease on stays.
    #[tokio::test(start_paused = true)]
    async fn an_idle_session_is_swept_and_its_extraction_unlinked() {
        let root = tempfile::tempdir().unwrap();
        let member = content(64 * 1024);
        let archive = zip_bytes("film.bin", &member, async_zip::Compression::Deflate).await;
        let archives = TorrentArchives::new(SESSION_IDLE_TIMEOUT);
        let hash = "ef".repeat(20);

        let (mut reader, lease) = archives
            .open_member(&hash, "Film.zip", "film.bin", "zip", config(root.path()), {
                let archive = archive.clone();
                move || async move {
                    Ok(Box::new(CountingReader {
                        inner: std::io::Cursor::new(archive),
                        reads: Arc::new(TorrentReads::default()),
                    }) as Box<dyn AsyncSeekableReader>)
                }
            })
            .await
            .expect("the member opens");
        let mut read = Vec::new();
        reader.read_to_end(&mut read).await.unwrap();
        assert_eq!(read, member);
        assert_eq!(extractions(root.path()).len(), 1);

        // A body is still reading: the session is in use however long it
        // has been.
        tokio::time::advance(SESSION_IDLE_TIMEOUT * 2).await;
        archives.sweep(tokio::time::Instant::now());
        assert_eq!(archives.len(), 1, "a lease is a use");
        assert_eq!(extractions(root.path()).len(), 1);

        // The body ends, and the idle clock runs from there.
        drop(reader);
        drop(lease);
        tokio::time::advance(SESSION_IDLE_TIMEOUT * 2).await;
        archives.sweep(tokio::time::Instant::now());
        assert_eq!(archives.len(), 0);
        assert!(
            extractions(root.path()).is_empty(),
            "the extraction goes with the session that owned it"
        );
    }
}
