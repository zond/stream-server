use super::{
    ArchiveEntry, ArchiveReader, CacheConfig, OpenedMember,
    cache::{ProgressiveCache, SyncCacheWriter},
};
use anyhow::{Result, anyhow};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

use flate2::read::GzDecoder;

pub struct TgzHandler {
    path: PathBuf,
    cache_config: CacheConfig,
}

impl TgzHandler {
    pub fn new(path: PathBuf, cache_config: CacheConfig) -> Self {
        Self { path, cache_config }
    }
}

/// What a scan's error says when it stopped because nobody is waiting for
/// its answer any more.
const SCAN_ABANDONED: &str = "tgz scan abandoned: nobody is waiting for the member any more";

/// The opener's end of a scan: where the member's size goes once its header
/// is found. `None` once it has been handed over.
type Opener = Arc<Mutex<Option<oneshot::Sender<Result<u64>>>>>;

fn take_opener(opener: &Opener) -> Option<oneshot::Sender<Result<u64>>> {
    opener.lock().unwrap_or_else(|e| e.into_inner()).take()
}

/// The compressed archive, refused while the scan is looking for the
/// member's header and nobody is waiting for it.
///
/// A gzip stream can only be read from its start, so reaching a member deep
/// in it means decompressing everything before it -- minutes on a slow SoC
/// for a large archive, most of it inside the entries being skipped, where
/// no check between entries would reach. A request that went away while
/// that ran used to leave it running to the end regardless. Once the
/// header is found and handed over, what stops a copy nobody reads is the
/// cache writer's own abandonment check, and this lets every read through.
struct WhileAwaited<R> {
    inner: R,
    opener: Opener,
}

impl<R: Read> Read for WhileAwaited<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let gone = self
            .opener
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|tx| tx.is_closed());
        if gone {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                SCAN_ABANDONED,
            ));
        }
        self.inner.read(buf)
    }
}

/// Find `target` in the archive at `archive_path`, tell the opener its size,
/// and copy it into the writer the opener sends back.
///
/// One pass over the gzip stream does both. The size has to be known before
/// the cache is made: the route seeks from the end for the length it
/// answers with, and a cache made without a size refuses that seek -- which
/// is what every tgz member got for an answer, a 500. The size is in the
/// member's tar header, so the scan stops there, hands the size over, and
/// copies the member's bytes from where it already is rather than
/// decompressing the archive a second time to get back to them.
///
/// An error before the handover is the opener's to report (and is returned
/// for it); one during the copy is the cache's.
fn extract(
    archive_path: &Path,
    target: &str,
    opener: Opener,
    writer: oneshot::Receiver<SyncCacheWriter>,
) -> Result<()> {
    let file = std::fs::File::open(archive_path)?;
    let mut archive = tar::Archive::new(GzDecoder::new(WhileAwaited {
        inner: file,
        opener: opener.clone(),
    }));
    for entry in archive.entries()? {
        let mut entry = entry?;
        if entry.path()?.to_string_lossy() != target {
            continue;
        }
        let Some(tx) = take_opener(&opener) else {
            return Ok(());
        };
        if tx.send(Ok(entry.size())).is_err() {
            // The opener went between the last read and the handover.
            return Ok(());
        }
        // An `Err` here is the opener failing to make the cache after all;
        // it reports that itself.
        let Ok(mut out) = writer.blocking_recv() else {
            return Ok(());
        };
        match std::io::copy(&mut entry, &mut out) {
            Ok(_) => out.finish(),
            Err(e) => out.set_error(e.to_string()),
        }
        return Ok(());
    }
    Err(anyhow!("File not found in TGZ"))
}

#[async_trait::async_trait]
impl ArchiveReader for TgzHandler {
    async fn list_files(&self) -> Result<Vec<ArchiveEntry>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(&path)?;
            let tar = GzDecoder::new(file);
            let mut archive = tar::Archive::new(tar);
            let mut entries = Vec::new();

            for file in archive.entries()? {
                let file = file?;
                entries.push(ArchiveEntry {
                    path: file.path()?.to_string_lossy().to_string(),
                    size: file.size(),
                    is_dir: file.header().entry_type().is_dir(),
                });
            }
            Ok(entries)
        })
        .await?
    }

    async fn open_file(&self, path: &str) -> Result<OpenedMember> {
        let archive_path = self.path.clone();
        let target = path.to_string();
        let (size_tx, size_rx) = oneshot::channel();
        let (writer_tx, writer_rx) = oneshot::channel();
        let opener: Opener = Arc::new(Mutex::new(Some(size_tx)));

        // A plain OS thread, not the blocking pool: a scan can run for
        // minutes, and the runtime waits for blocking-pool tasks when it
        // shuts down.
        std::thread::spawn(move || {
            if let Err(e) = extract(&archive_path, &target, opener.clone(), writer_rx) {
                tracing::debug!(archive = %archive_path.display(), member = %target, error = %e, "tgz extraction ended");
                if let Some(tx) = take_opener(&opener) {
                    let _ = tx.send(Err(e));
                }
            }
        });

        let size = size_rx
            .await
            .map_err(|_| anyhow!("tgz scan ended without an answer"))??;
        // The extracted member lands in the archive scratch dir, which
        // nothing counts and whose session unlinks it (see
        // `archives::SCRATCH_DIR_NAME`).
        let (cache, writer) =
            ProgressiveCache::new_in_dir(&self.cache_config.scratch_dir(), Some(size)).await?;
        // The sync writer, because the copy runs on a plain thread that
        // cannot await the async one's `finish()`. It writes to an
        // unbuffered `std::fs::File`, so its bytes are visible to the
        // readers' handles as soon as it has written them.
        let out = writer.try_clone_sync()?;
        writer_tx
            .send(out)
            .map_err(|_| anyhow!("tgz extraction ended before its member could be copied"))?;
        Ok(OpenedMember::Extracted(cache))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    fn content(len: usize, seed: u32) -> Vec<u8> {
        (0..len as u32)
            .map(|i| (i.wrapping_mul(31).wrapping_add(seed) % 251) as u8)
            .collect()
    }

    /// A `.tar.gz` with the given members, in order.
    fn write_tgz(dir: &Path, members: &[(&str, Vec<u8>)]) -> PathBuf {
        let path = dir.join("fixture.tar.gz");
        let file = std::fs::File::create(&path).unwrap();
        let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
        let mut builder = tar::Builder::new(encoder);
        for (name, data) in members {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, name, data.as_slice())
                .unwrap();
        }
        builder.into_inner().unwrap().finish().unwrap();
        path
    }

    fn handler(root: &Path, archive: PathBuf) -> TgzHandler {
        TgzHandler::new(
            archive,
            CacheConfig {
                cache_dir: root.to_path_buf(),
                _cache_size: 0,
            },
        )
    }

    /// A member's reader knows its length before the extraction has written
    /// it, so the route's seek from the end -- how it learns the length it
    /// answers with -- lands, rather than failing every request with a 500.
    #[tokio::test]
    async fn a_member_knows_its_length_before_it_is_extracted() {
        let root = tempfile::tempdir().unwrap();
        let second = content(200 * 1024, 7);
        let archive = write_tgz(
            root.path(),
            &[
                ("first.txt", content(1000, 1)),
                ("videos/second.bin", second.clone()),
            ],
        );
        let mut reader = handler(root.path(), archive)
            .open_file("videos/second.bin")
            .await
            .expect("open member")
            .into_reader()
            .await
            .expect("reader");
        let len = reader
            .seek(std::io::SeekFrom::End(0))
            .await
            .expect("seek from end");
        assert_eq!(len, second.len() as u64);
        reader.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await.unwrap();
        assert_eq!(data, second);
    }

    #[tokio::test]
    async fn a_missing_member_is_an_error_from_the_open() {
        let root = tempfile::tempdir().unwrap();
        let archive = write_tgz(root.path(), &[("first.txt", content(10, 1))]);
        let err = match handler(root.path(), archive).open_file("nope.bin").await {
            Ok(_) => panic!("a missing member opened"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("File not found"), "{err}");
    }

    /// A scan whose opener has gone stops reading the archive, even in the
    /// middle of an entry it is skipping, rather than decompressing on to
    /// the member for nobody.
    #[test]
    fn a_scan_nobody_waits_for_stops() {
        let root = tempfile::tempdir().unwrap();
        let archive = write_tgz(
            root.path(),
            &[
                ("big.bin", content(4 * 1024 * 1024, 3)),
                ("target.bin", content(10, 1)),
            ],
        );
        let (size_tx, size_rx) = oneshot::channel();
        let (_writer_tx, writer_rx) = oneshot::channel();
        drop(size_rx);
        let err = extract(
            &archive,
            "target.bin",
            Arc::new(Mutex::new(Some(size_tx))),
            writer_rx,
        )
        .expect_err("the scan went on for nobody");
        assert!(err.to_string().contains(SCAN_ABANDONED), "{err}");
    }
}
