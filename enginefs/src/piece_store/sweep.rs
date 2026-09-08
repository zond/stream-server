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
//!
//! What may claim a torrent is therefore the whole of the question. It is not
//! "what came up on this boot": a restore is allowed to fail -- the volume the
//! torrent writes to is not mounted, its `.torrent` will not parse, the add
//! errored -- and the failure says nothing at all about the data. The claim is
//! *what the session still has a record of*, which is what
//! [`session_recorded_hashes`] reads off disk.

use std::collections::HashSet;
use std::path::Path;

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

/// Info hashes librqbit's session persistence still has a record of, read
/// straight out of its folder (which is the engine's `download_dir` -- see
/// `LibrqbitBackend::new`, which hands `SessionPersistenceConfig::Json` that
/// same path).
///
/// Two records, because either can outlive the other and each on its own is
/// reason enough not to delete a download:
///
/// * `session.json`, the session database: a `torrents` map of
///   `SerializedTorrent`, of which only `info_hash` matters here.
/// * `<info hash>.bitv` and `<info hash>.torrent`, the per-torrent fastresume
///   bitfield and metadata file. A `session.json` that is truncated, half
///   written or unparseable takes every claim in it down with it, and these
///   are what is left; conversely a torrent added seconds before the process
///   died has a session entry and no bitfield yet.
///
/// Anything unreadable is a warning and no claim, never an error: a sweep must
/// still run. That direction is the safe one only because it is paired with
/// the second record -- losing *both* is the one case that can still take a
/// torrent's pieces, and by then the session has forgotten the torrent too.
pub fn session_recorded_hashes(persistence_folder: &Path) -> HashSet<String> {
    #[derive(serde::Deserialize)]
    struct SessionDatabase {
        #[serde(default)]
        torrents: std::collections::HashMap<String, SerializedTorrent>,
    }
    #[derive(serde::Deserialize)]
    struct SerializedTorrent {
        info_hash: String,
    }

    let mut hashes = HashSet::new();
    let db_path = persistence_folder.join("session.json");
    match std::fs::read(&db_path) {
        Ok(bytes) => match serde_json::from_slice::<SessionDatabase>(&bytes) {
            Ok(db) => hashes.extend(
                db.torrents
                    .into_values()
                    .filter_map(|torrent| info_hash_of(&torrent.info_hash)),
            ),
            Err(error) => tracing::warn!(
                path = %db_path.display(),
                %error,
                "could not read the session database; the per-torrent records are the only claims left"
            ),
        },
        // No session database is the ordinary first-launch state.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => tracing::warn!(
            path = %db_path.display(),
            %error,
            "could not open the session database; the per-torrent records are the only claims left"
        ),
    }

    match std::fs::read_dir(persistence_folder) {
        Ok(entries) => {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                if let Some((stem, "bitv" | "torrent")) = name.rsplit_once('.')
                    && let Some(hash) = info_hash_of(stem)
                {
                    hashes.insert(hash);
                }
            }
        }
        Err(error) => tracing::warn!(
            path = %persistence_folder.display(),
            %error,
            "could not read the session persistence folder"
        ),
    }
    hashes
}

/// `name` as a lowercase info hash, or `None` when it is not one. The same
/// shape `cache_cleaner::is_session_artifact` recognises: forty hex digits.
fn info_hash_of(name: &str) -> Option<String> {
    (name.len() == 40 && name.bytes().all(|b| b.is_ascii_hexdigit()))
        .then(|| name.to_ascii_lowercase())
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
            (true, Some(name)) => occupancy(&store.stat(name)),
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

/// What one torrent's directory occupies, counted the way the cache cleaner
/// counts: allocated blocks, never apparent length. A piece file written by
/// one chunk out of many is a hole plus 16 KiB, and reporting the piece's full
/// length as freed would be a number the disk never gives back.
///
/// Strays included: this is about to remove the directory whole, so what
/// leaves the disk with it is what it holds, piece file or not.
fn occupancy(stored: &super::store::StoredTorrent) -> u64 {
    stored
        .pieces
        .iter()
        .flat_map(|piece| piece.files())
        .chain(stored.strays.iter())
        .map(occupied_bytes)
        .sum()
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

    /// The claim that keeps a download alive on a boot where the restore
    /// failed: librqbit's own records, read off disk. Both of them, because
    /// either can be the only one left.
    #[test]
    fn the_sessions_own_records_are_claims() {
        let tmp = tempfile::tempdir().unwrap();
        let folder = tmp.path();
        std::fs::write(
            folder.join("session.json"),
            serde_json::json!({
                "torrents": { "0": { "info_hash": ADOPTED }, "7": { "info_hash": DORMANT } }
            })
            .to_string(),
        )
        .unwrap();
        // Recorded by a per-torrent file alone -- `session.json` has no entry
        // for it, which is what a truncated flush or a torrent added between
        // two flushes leaves.
        std::fs::write(folder.join(format!("{ORPHAN}.bitv")), [0u8; 8]).unwrap();
        // Debris that is not a record of anything.
        std::fs::write(folder.join("notes.bitv"), b"x").unwrap();
        std::fs::write(folder.join("dht.json"), b"{}").unwrap();

        assert_eq!(
            session_recorded_hashes(folder),
            claims(&[ADOPTED, DORMANT, ORPHAN])
        );
    }

    /// A session database that will not parse must not silently un-claim
    /// every torrent in it. It costs a warning and the per-torrent records
    /// carry the claims instead -- which is the whole reason both are read.
    #[test]
    fn an_unreadable_session_database_falls_back_to_the_per_torrent_records() {
        let tmp = tempfile::tempdir().unwrap();
        let folder = tmp.path();
        std::fs::write(folder.join("session.json"), b"{\"torrents\": {\"0\": ").unwrap();
        std::fs::write(folder.join(format!("{ADOPTED}.torrent")), b"d4:infod").unwrap();

        assert_eq!(session_recorded_hashes(folder), claims(&[ADOPTED]));
    }

    #[test]
    fn a_folder_with_no_session_in_it_records_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(session_recorded_hashes(tmp.path()).is_empty());
        assert!(session_recorded_hashes(&tmp.path().join("never")).is_empty());
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
}
