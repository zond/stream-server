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
//! So the store is reconciled against the pin record once, at launch, before
//! the session opens: every directory under the piece root whose info hash no
//! pin claims is removed outright. Running before the session is what makes
//! that safe -- a torrent being restored or added concurrently would have a
//! directory, a registered store and no claim yet.
//!
//! It is idempotent by construction: it deletes what is not claimed, and a
//! second pass finds the same claims and nothing left to delete.
//!
//! What may claim a torrent is therefore the whole of the question, and the
//! answer is now one word: a pin. Everything else under this root is cache --
//! a window round a playhead, the committed half a peer is served from, a
//! slack file the next tick takes -- and none of it is worth a byte across a
//! restart, because nothing is playing in a process that has served nothing.
//! So the claim set is the pin record's keys ([`super::pin_record`]) and the
//! sweep runs *before the session opens*, while no store is registered and no
//! torrent can be mid-check: what it deletes, it deletes with nothing holding
//! a handle on it.
//!
//! The one state it refuses to run in is a pin record that would not read.
//! An unreadable record names no pins, and sweeping on that would delete
//! every offline download the user has -- so it is skipped for that boot and
//! the disk keeps what it held; see [`super::pin_record::PinsUnknown`].

use std::collections::HashSet;
use std::path::Path;

use super::pin_record::PinRecord;
use super::store::StoreRoot;

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
pub fn sweep_unadopted(root: &StoreRoot, adopted: &HashSet<String>) -> SweepReport {
    let mut report = SweepReport::default();
    let root = root.path();
    let store = StoreRoot::new(root.to_path_buf());
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
        let name = path.file_name().and_then(|name| name.to_str());
        let claimed = is_dir && name.is_some_and(|name| adopted.contains(name));
        if claimed {
            continue;
        }
        // What the store says is in there, never a walk of our own: the
        // bucketing is `StoreRoot`'s and this is the same reading the cache
        // cleaner gets.
        let freed = match (is_dir, name) {
            (true, Some(name)) => store.stat(name).occupancy(),
            _ => 0,
        };
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

/// The launch sweep, in the one order that is safe: whatever `record`
/// claims is kept and everything else under the piece root goes, before the
/// session opens.
///
/// Skipped outright for a record that would not read. An unreadable record
/// claims nothing, and a sweep on nothing is `remove_dir_all` over every
/// offline download the user has -- so the disk keeps what it held for that
/// boot, the condition is reported instead
/// ([`super::pin_record::PinsUnknown`]), and the next boot with a readable
/// record sweeps what this one left.
///
/// On the blocking pool: it is `read_dir` plus `remove_dir_all` over a tree
/// that can be the whole cache, and it runs on the thread that is opening the
/// session.
pub async fn sweep_before_session(download_dir: &Path, record: &PinRecord) -> SweepReport {
    if let Some(why) = record.unreadable() {
        tracing::warn!(
            path = %super::pin_record::path(download_dir).display(),
            error = %why,
            "the pin record could not be read; keeping every torrent's data and sweeping nothing this boot"
        );
        return SweepReport::default();
    }
    let root = StoreRoot::in_download_dir(download_dir);
    let claims = record.claims();
    match tokio::task::spawn_blocking(move || sweep_unadopted(&root, &claims)).await {
        Ok(report) => report,
        Err(error) => {
            tracing::warn!(%error, "the piece store sweep did not finish");
            SweepReport::default()
        }
    }
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
        let first = sweep_unadopted(&StoreRoot::new(root.clone()), &claimed);
        assert_eq!(first.removed, 1, "the orphan, and only the orphan");
        assert_eq!(first.errors, 0);
        assert!(first.freed_bytes >= 16384, "{first:?}");
        assert!(root.join(ADOPTED).join("0").join("1").is_file());
        assert!(root.join(DORMANT).join("3").join("3001").is_file());
        assert!(!root.join(ORPHAN).exists());

        let second = sweep_unadopted(&StoreRoot::new(root.clone()), &claimed);
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

        let report = sweep_unadopted(&StoreRoot::new(root.clone()), &claims(&[ADOPTED]));
        assert_eq!(report.removed, 2);
        assert_eq!(report.errors, 0);
        assert!(!root.join("scratch.tmp").exists());
        assert!(!root.join("not-a-hash").exists());
        assert!(root.join(ADOPTED).is_dir());
    }

    #[test]
    fn a_store_that_has_never_been_written_is_not_a_problem() {
        let tmp = tempfile::tempdir().unwrap();
        let report = sweep_unadopted(
            &StoreRoot::new(tmp.path().join(".pieces")),
            &claims(&[ADOPTED]),
        );
        assert_eq!(report, SweepReport::default());
    }

    /// A claim for a torrent with no data yet is not an error either: it is
    /// the ordinary state of a pin whose download has not started.
    #[test]
    fn a_claim_with_nothing_on_disk_sweeps_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(".pieces");
        piece(&root, ORPHAN, "0", "0", 512);
        let report = sweep_unadopted(&StoreRoot::new(root.clone()), &claims(&[ADOPTED, DORMANT]));
        assert_eq!(report.removed, 1);
        assert!(!root.join(ADOPTED).exists());
    }

    /// The claim set is the pin record's keys and nothing else: a torrent
    /// the session will restore in a moment, and whose pieces are right
    /// there, is cache unless it is pinned.
    #[tokio::test]
    async fn the_sweep_keeps_what_the_pin_record_names_and_takes_the_rest() {
        let tmp = tempfile::tempdir().unwrap();
        let download_dir = tmp.path().to_path_buf();
        let root = StoreRoot::in_download_dir(&download_dir)
            .path()
            .to_path_buf();
        piece(&root, ADOPTED, "0", "1", 4096);
        piece(&root, ORPHAN, "0", "0", 8192);

        let record = PinRecord::Pins(std::collections::BTreeMap::from([(
            ADOPTED.to_string(),
            vec![0usize],
        )]));
        let report = sweep_before_session(&download_dir, &record).await;
        assert_eq!(report.removed, 1, "{report:?}");
        assert!(root.join(ADOPTED).join("0").join("1").is_file(), "pinned");
        assert!(!root.join(ORPHAN).exists(), "and nothing else is claimed");
    }

    /// No record at all is an empty claim set -- first launch, or a user who
    /// has never pinned -- and the sweep runs on it.
    #[tokio::test]
    async fn an_absent_record_claims_nothing_and_the_sweep_runs() {
        let tmp = tempfile::tempdir().unwrap();
        let download_dir = tmp.path().to_path_buf();
        let root = StoreRoot::in_download_dir(&download_dir)
            .path()
            .to_path_buf();
        piece(&root, ADOPTED, "0", "1", 4096);

        let report = sweep_before_session(&download_dir, &PinRecord::Absent).await;
        assert_eq!(report.removed, 1, "{report:?}");
        assert!(!root.join(ADOPTED).exists());
    }

    /// And a record that would not read is not an empty claim set: nothing
    /// is swept at all, because the pins it named are exactly what would go.
    #[tokio::test]
    async fn an_unreadable_record_sweeps_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let download_dir = tmp.path().to_path_buf();
        let root = StoreRoot::in_download_dir(&download_dir)
            .path()
            .to_path_buf();
        piece(&root, ADOPTED, "0", "1", 4096);
        piece(&root, ORPHAN, "0", "0", 8192);

        let report =
            sweep_before_session(&download_dir, &PinRecord::Unreadable("broken".into())).await;
        assert_eq!(report, SweepReport::default());
        assert!(root.join(ADOPTED).join("0").join("1").is_file());
        assert!(root.join(ORPHAN).join("0").join("0").is_file());
    }
}
