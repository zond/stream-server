//! The startup sweep: delete piece data nothing has adopted.
//!
//! Android kills a backgrounded app without ceremony, so the process can and
//! does die between a torrent being added and anything recording that it
//! exists -- and between a torrent being removed and its pieces being deleted.
//! Cleanup that only runs on the way out is cleanup that does not run. What is
//! left behind is not visible as a download, is not counted by anything that
//! asks the engine what it holds, and is never reclaimed: exactly the
//! invisible disk usage this design exists to stop producing.
//!
//! So the store is reconciled against the session once, at launch, before the
//! first add: every directory under the piece root whose info hash no torrent
//! and no pin claims is removed outright. Running before the first add is what
//! makes that safe -- a torrent being added concurrently would have a
//! directory and no claim yet.
//!
//! It is idempotent by construction: it deletes what is not claimed, and a
//! second pass finds the same claims and nothing left to delete.

use std::collections::HashSet;
use std::path::Path;

/// What one sweep did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SweepReport {
    /// Torrent directories removed because nothing claimed them.
    pub removed: usize,
    /// What they occupied, in bytes as the volume counts them -- the same
    /// `st_blocks` accounting the cache cleaner uses, so a sparse or
    /// partly-written piece is reported at what deleting it actually frees.
    pub freed_bytes: u64,
    /// Entries that could not be removed. Logged, never fatal: a sweep that
    /// trips over one directory must still do the rest.
    pub errors: usize,
}

/// Remove every torrent's pieces under `root` except those whose lowercase
/// info hash is in `adopted`.
///
/// Strays that are not directories, and directories that are not named like an
/// info hash, are removed too: the root belongs to this store alone, so
/// anything in it that is not a torrent's pieces is debris from an interrupted
/// write.
pub fn sweep_unadopted(root: &Path, adopted: &HashSet<String>) -> SweepReport {
    let mut report = SweepReport::default();
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        // No piece root yet is the ordinary first-launch state, not a problem.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return report,
        Err(error) => {
            tracing::warn!(root = %root.display(), %error, "could not read the piece store root");
            report.errors += 1;
            return report;
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::warn!(root = %root.display(), %error, "could not read a piece store entry");
                report.errors += 1;
                continue;
            }
        };
        let path = entry.path();
        let is_dir = entry.file_type().is_ok_and(|t| t.is_dir());
        let claimed = is_dir
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| adopted.contains(name));
        if claimed {
            continue;
        }
        let freed = if is_dir { occupancy(&path) } else { 0 };
        let removed = if is_dir {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        match removed {
            Ok(()) => {
                tracing::info!(path = %path.display(), freed, "swept unadopted piece data");
                report.removed += 1;
                report.freed_bytes += freed;
            }
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "could not sweep piece data");
                report.errors += 1;
            }
        }
    }
    report
}

/// What a directory of piece files occupies, counted the way the cache cleaner
/// counts: allocated blocks, never apparent length. A piece file written by
/// one chunk out of many is a hole plus 16 KiB, and reporting the piece's full
/// length as freed would be a number the disk never gives back.
fn occupancy(dir: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() {
            total += occupancy(&entry.path());
        } else if let Ok(metadata) = entry.metadata() {
            total += occupied_bytes(&metadata);
        }
    }
    total
}

#[cfg(unix)]
fn occupied_bytes(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    // `st_blocks` is in 512-byte units by POSIX, not the filesystem's block
    // size.
    metadata.blocks() * 512
}

#[cfg(not(unix))]
fn occupied_bytes(metadata: &std::fs::Metadata) -> u64 {
    // `std` exposes no cheap allocated-size call on Windows; the apparent
    // length is the same overestimate the cache cleaner accepts there.
    metadata.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ADOPTED: &str = "0123456789abcdef0123456789abcdef01234567";
    const DORMANT: &str = "fedcba9876543210fedcba9876543210fedcba98";
    const ORPHAN: &str = "1111111111111111111111111111111111111111";

    fn piece(root: &Path, hash: &str, bucket: &str, piece: &str, len: usize) {
        let dir = root.join(hash).join(bucket);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(piece), vec![0x5au8; len]).unwrap();
    }

    fn claims(hashes: &[&str]) -> HashSet<String> {
        hashes.iter().map(|h| h.to_string()).collect()
    }

    /// The point of the sweep, and the reason it has to be safe to run every
    /// launch: a process killed mid-write leaves piece data nothing knows
    /// about, and a second run must find nothing left to do.
    #[test]
    fn unclaimed_piece_data_goes_and_claimed_data_stays_however_often_it_runs() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(".pieces");
        piece(&root, ADOPTED, "0", "1", 4096);
        piece(&root, DORMANT, "3", "3001", 4096);
        piece(&root, ORPHAN, "0", "0", 8192);
        piece(&root, ORPHAN, "2", "2500", 8192);

        let claimed = claims(&[ADOPTED, DORMANT]);
        let first = sweep_unadopted(&root, &claimed);
        assert_eq!(first.removed, 1, "the orphan, and only the orphan");
        assert_eq!(first.errors, 0);
        assert!(first.freed_bytes >= 16384, "{first:?}");
        assert!(root.join(ADOPTED).join("0").join("1").is_file());
        assert!(root.join(DORMANT).join("3").join("3001").is_file());
        assert!(!root.join(ORPHAN).exists());

        let second = sweep_unadopted(&root, &claimed);
        assert_eq!(
            second,
            SweepReport::default(),
            "idempotent: the second pass finds the same claims and nothing to do"
        );
        assert!(root.join(ADOPTED).join("0").join("1").is_file());
    }

    /// The root belongs to the store alone, so anything in it that is not a
    /// claimed torrent's pieces is debris -- including a file where a
    /// directory should be, which is what an interrupted write leaves.
    #[test]
    fn debris_that_is_not_a_torrent_directory_goes_too() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(".pieces");
        piece(&root, ADOPTED, "0", "1", 512);
        std::fs::write(root.join("scratch.tmp"), b"half a write").unwrap();
        std::fs::create_dir_all(root.join("not-a-hash")).unwrap();

        let report = sweep_unadopted(&root, &claims(&[ADOPTED]));
        assert_eq!(report.removed, 2);
        assert_eq!(report.errors, 0);
        assert!(!root.join("scratch.tmp").exists());
        assert!(!root.join("not-a-hash").exists());
        assert!(root.join(ADOPTED).is_dir());
    }

    #[test]
    fn a_store_that_has_never_been_written_is_not_a_problem() {
        let tmp = tempfile::tempdir().unwrap();
        let report = sweep_unadopted(&tmp.path().join(".pieces"), &claims(&[ADOPTED]));
        assert_eq!(report, SweepReport::default());
    }

    /// A claim for a torrent with no data yet is not an error either: it is
    /// the ordinary state of a pin whose download has not started.
    #[test]
    fn a_claim_with_nothing_on_disk_sweeps_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(".pieces");
        piece(&root, ORPHAN, "0", "0", 512);
        let report = sweep_unadopted(&root, &claims(&[ADOPTED, DORMANT]));
        assert_eq!(report.removed, 1);
        assert!(!root.join(ADOPTED).exists());
    }
}
