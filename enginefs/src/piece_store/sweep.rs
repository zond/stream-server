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
//! So the store is reconciled against the embedder's pin set once, at launch, before
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
//! So the claim set is the embedder's keys ([`super::pin_record`]) and the
//! sweep runs *before the session opens*, while no store is registered and no
//! torrent can be mid-check: what it deletes, it deletes with nothing holding
//! a handle on it.
//!
//! The one state it refuses to run in is one where nobody named the pins.
//! Silence names no pins, and sweeping on that would delete every offline
//! download the user has -- so it is skipped for that boot and the disk keeps
//! what it held; see [`super::pin_record::PinsUnknown`].

use std::collections::HashSet;
use std::path::Path;

use super::pin_record::PinSet;
use super::store::StoreRoot;

/// What one sweep did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SweepReport {
    /// Torrent directories removed because nothing claimed them.
    pub removed: usize,
    /// What they occupied, in bytes as the volume counts them -- the one
    /// `st_blocks` accounting this repository has, so a sparse or
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
        // bucketing is `StoreRoot`'s and this is the same reading a usage
        // figure gets.
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
/// Skipped outright when nobody named a set. Silence claims nothing, and a
/// sweep on nothing is `remove_dir_all` over every offline download the user
/// has -- so the disk keeps what it held for that boot, the condition is
/// reported instead ([`super::pin_record::PinsUnknown`]), and the next boot
/// that is told a set sweeps what this one left.
///
/// On the blocking pool: it is `read_dir` plus `remove_dir_all` over a tree
/// that can be the whole cache, and it runs on the thread that is opening the
/// session.
pub async fn sweep_before_session(download_dir: &Path, pins: Option<&PinSet>) -> SweepReport {
    let Some(pins) = pins else {
        tracing::warn!(
            "nobody named the pin set; keeping every torrent's data and sweeping nothing this boot"
        );
        return SweepReport::default();
    };
    let root = StoreRoot::in_download_dir(download_dir);
    let claims: HashSet<String> = pins.keys().map(|hash| hash.to_lowercase()).collect();
    let legacy_root = download_dir.to_path_buf();
    match tokio::task::spawn_blocking(move || {
        let mut report = sweep_unadopted(&root, &claims);
        // Beside the store, not under it, and nothing else will ever take
        // them: see `sweep_legacy_downloads`. On the same hop, because both
        // are `read_dir` plus `remove_dir_all` on the thread opening the
        // session.
        let legacy = sweep_legacy_downloads(&legacy_root);
        report.removed += legacy.removed;
        report.freed_bytes += legacy.freed_bytes;
        report.errors += legacy.errors;
        report
    })
    .await
    {
        Ok(report) => report,
        Err(error) => {
            tracing::warn!(%error, "the piece store sweep did not finish");
            SweepReport::default()
        }
    }
}

/// Directories under the download root that belong to something else, each
/// of which reconciles itself.
///
/// `.pieces` is [`sweep_unadopted`]'s, and it is the embedder's pin set that
/// decides what survives there. `.proxy` is the proxy cache's, emptied by its own
/// launch sweep. `.archives` is the archive scratch's, which has a lifetime
/// of its own. Handing any of them to [`sweep_legacy_downloads`] would be
/// one sweep deciding another's business, and for `.pieces` it would delete
/// every pin.
const NOT_OURS: [&str; 5] = [".pieces", ".proxy", ".archives", ".metadata", ".cache"];

/// Whether `name`, directly under the download root, is something the
/// session writes and reads.
///
/// Taken from the cache cleaner's `is_session_artifact`, which is what
/// exempted these from its walk for as long as it had one.
///
/// `pinned-downloads.json` is on the list although this server no longer
/// writes one: an install upgraded from a build that did still has the file,
/// and sweeping it as a previous release's *data* would be this sweep
/// deleting a record while a user might still get something out of it. librqbit keeps
/// its resume data beside the data itself -- `session.json`, a `.torrent`
/// and a `.bitv` per info hash -- and the DHT its bootstrap; the pin record
/// is this crate's own, and its atomic write leaves a `pinned-downloads
/// .json.tmp-<n>` behind if it is interrupted.
fn is_session_artifact(name: &str) -> bool {
    let name = name.strip_suffix(".tmp").unwrap_or(name);
    if matches!(
        name,
        "session.json" | "pinned-downloads.json" | "dht.json" | "dht-bootstrap.json"
    ) {
        return true;
    }
    if let Some(rest) = name.strip_prefix("pinned-downloads.json.tmp-") {
        return !rest.is_empty();
    }
    match name.rsplit_once('.') {
        Some((stem, "torrent" | "bitv")) => {
            stem.len() == 40 && stem.bytes().all(|b| b.is_ascii_hexdigit())
        }
        _ => false,
    }
}

/// Remove what an **older version of this server** left beside the store:
/// whole-file downloads at `<download dir>/<torrent name>/<file>`.
///
/// **This is the one category of byte with no owner.** Everything this
/// server writes now goes through the piece store or the proxy cache, and
/// both have an owner that bounds them and a launch sweep that reconciles
/// them. A whole-file download predates all of it: no store speaks for it,
/// no policy bounds it, no pass will ever look at it, and until the cache
/// cleaner was deleted its walk was the only thing that ever took one.
/// Left alone it is invisible disk usage that grows once and never shrinks
/// -- the failure this design exists to stop producing, wearing the clothes
/// of a previous release.
///
/// So it goes, once, at launch, and the rule is the cleaner's own: what is
/// not another component's directory ([`NOT_OURS`]) and not a session
/// artifact ([`is_session_artifact`]) is a previous release's data.
///
/// **It runs where the session's own data lives**, so the exemptions are
/// load-bearing rather than tidy: removing `session.json` would lose every
/// torrent the user has, and removing `.pieces` would delete every pin.
/// A name this server has never written is removed with them, which is the
/// same judgement [`sweep_unadopted`] makes about its own root: the
/// directory is this server's, and the alternative to deleting what we do
/// not recognise is keeping it for ever.
///
/// Skipped when nobody named a pin set, like [`sweep_before_session`] and
/// for the same reason: that boot keeps what the disk held.
pub fn sweep_legacy_downloads(download_dir: &Path) -> SweepReport {
    let mut report = SweepReport::default();
    let Ok(entries) = std::fs::read_dir(download_dir) else {
        return report;
    };
    for entry in entries {
        let Ok(entry) = entry else {
            report.errors += 1;
            continue;
        };
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            // A name this process cannot spell is one it cannot reason
            // about, and deleting it would be deleting something unread.
            report.errors += 1;
            continue;
        };
        if NOT_OURS.contains(&name) || is_session_artifact(name) {
            continue;
        }
        let is_dir = entry.file_type().is_ok_and(|kind| kind.is_dir());
        let freed = if is_dir {
            tree_bytes(&path)
        } else {
            std::fs::metadata(&path)
                .as_ref()
                .map(crate::chunk_store::occupied_bytes)
                .unwrap_or(0)
        };
        let removed = if is_dir {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        match removed {
            Ok(()) => {
                tracing::info!(
                    path = %path.display(),
                    freed,
                    "swept a previous release's whole-file download"
                );
                report.removed += 1;
                report.freed_bytes += freed;
            }
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "could not sweep legacy download data");
                report.errors += 1;
            }
        }
    }
    report
}

/// What a tree occupies, as the volume counts it.
///
/// A walk, unlike [`sweep_unadopted`]'s figure, because there is no store to
/// ask: these bytes are exactly the ones nothing keeps a reading of. It runs
/// once per launch over what a previous release left, and never again once
/// that is gone.
fn tree_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                tree_bytes(&path)
            } else {
                std::fs::metadata(&path)
                    .as_ref()
                    .map(crate::chunk_store::occupied_bytes)
                    .unwrap_or(0)
            }
        })
        .sum()
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

    /// **The one category of byte with no owner goes at launch.**
    ///
    /// A whole-file download from an older release lives beside the store,
    /// not under it: no store speaks for it, no policy bounds it, no pass
    /// will ever look at it, and the cache cleaner's walk -- deleted with
    /// the rest of the eviction machinery -- was the only thing that ever
    /// took one. Left alone it is disk usage that grows once and never
    /// shrinks.
    ///
    /// What it must not take is anything the session needs: `session.json`
    /// and the per-hash resume files are how every torrent the user has
    /// comes back, and `.pieces` is where every pin lives.
    #[test]
    fn a_previous_releases_whole_file_download_goes_and_the_session_stays() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        // A previous release's download: a directory named after the
        // torrent, with the file inside it.
        let legacy = root.join("Some Film (2011)");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("film.mkv"), vec![7u8; 4096]).unwrap();
        // And one it left directly under the root.
        std::fs::write(root.join("loose.mkv"), vec![7u8; 2048]).unwrap();

        // Everything the session reads at startup.
        std::fs::write(root.join("session.json"), b"{}").unwrap();
        std::fs::write(root.join("pinned-downloads.json"), b"{}").unwrap();
        std::fs::write(root.join("dht.json"), b"{}").unwrap();
        std::fs::write(root.join("dht-bootstrap.json"), b"{}").unwrap();
        std::fs::write(root.join(format!("{ADOPTED}.torrent")), b"d4:infod").unwrap();
        std::fs::write(root.join(format!("{ADOPTED}.bitv")), [0u8; 8]).unwrap();
        std::fs::write(root.join("pinned-downloads.json.tmp-7"), b"{}").unwrap();
        // And the three directories that reconcile themselves.
        for name in [".pieces", ".proxy", ".archives"] {
            std::fs::create_dir_all(root.join(name).join("inside")).unwrap();
        }

        let report = sweep_legacy_downloads(root);
        assert_eq!(report.removed, 2, "the film's directory and the loose file");
        assert_eq!(report.errors, 0);
        assert!(report.freed_bytes >= 4096 + 2048, "{report:?}");

        assert!(!legacy.exists(), "the previous release's download is gone");
        assert!(!root.join("loose.mkv").exists());
        for name in [
            "session.json",
            "pinned-downloads.json",
            "dht.json",
            "dht-bootstrap.json",
            "pinned-downloads.json.tmp-7",
        ] {
            assert!(root.join(name).is_file(), "{name} is the session's");
        }
        assert!(root.join(format!("{ADOPTED}.torrent")).is_file());
        assert!(root.join(format!("{ADOPTED}.bitv")).is_file());
        for name in [".pieces", ".proxy", ".archives"] {
            assert!(
                root.join(name).join("inside").exists(),
                "{name} reconciles itself and is not this sweep's"
            );
        }

        // And it is idempotent, like the sweep it runs beside.
        let again = sweep_legacy_downloads(root);
        assert_eq!(again, SweepReport::default(), "nothing left to take");
    }

    /// A name that only looks like a resume file is not one.
    ///
    /// The rule is the cleaner's: forty hex characters and one of two
    /// extensions. A directory a user named `notahash.torrent`, or a
    /// `.bitv` whose stem is the wrong length, is a previous release's as
    /// far as this sweep is concerned -- and that is the safe direction,
    /// because the session names its own files and this one would not open.
    #[test]
    fn only_a_real_resume_file_is_spared() {
        assert!(is_session_artifact(&format!("{ADOPTED}.torrent")));
        assert!(is_session_artifact(&format!("{ADOPTED}.bitv")));
        assert!(is_session_artifact("session.json"));
        assert!(is_session_artifact("session.json.tmp"));
        assert!(is_session_artifact("pinned-downloads.json.tmp-12"));
        assert!(!is_session_artifact("pinned-downloads.json.tmp-"));
        assert!(!is_session_artifact("notahash.torrent"));
        assert!(!is_session_artifact(&format!("{}.torrent", &ADOPTED[..39])));
        assert!(!is_session_artifact(&format!("{ADOPTED}.mkv")));
        assert!(!is_session_artifact("Some Film (2011)"));
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

    /// The claim set is the embedder's keys and nothing else: a torrent the
    /// session will restore in a moment, and whose pieces are right there,
    /// is cache unless the embedder named it.
    #[tokio::test]
    async fn the_sweep_keeps_what_the_embedder_names_and_takes_the_rest() {
        let tmp = tempfile::tempdir().unwrap();
        let download_dir = tmp.path().to_path_buf();
        let root = StoreRoot::in_download_dir(&download_dir)
            .path()
            .to_path_buf();
        piece(&root, ADOPTED, "0", "1", 4096);
        piece(&root, ORPHAN, "0", "0", 8192);

        let pins = PinSet::from([(ADOPTED.to_string(), vec![0usize])]);
        let report = sweep_before_session(&download_dir, Some(&pins)).await;
        assert_eq!(report.removed, 1, "{report:?}");
        assert!(root.join(ADOPTED).join("0").join("1").is_file(), "pinned");
        assert!(!root.join(ORPHAN).exists(), "and nothing else is claimed");
    }

    /// An embedder that names an empty set has said something -- the user
    /// has pinned nothing -- and the sweep acts on it.
    #[tokio::test]
    async fn an_empty_set_claims_nothing_and_the_sweep_runs() {
        let tmp = tempfile::tempdir().unwrap();
        let download_dir = tmp.path().to_path_buf();
        let root = StoreRoot::in_download_dir(&download_dir)
            .path()
            .to_path_buf();
        piece(&root, ADOPTED, "0", "1", 4096);

        let report = sweep_before_session(&download_dir, Some(&PinSet::new())).await;
        assert_eq!(report.removed, 1, "{report:?}");
        assert!(!root.join(ADOPTED).exists());
    }

    /// And an embedder that names no set at all is not an empty claim set:
    /// nothing is swept, because what it would have claimed is exactly what
    /// would go.
    #[tokio::test]
    async fn an_unnamed_pin_set_sweeps_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let download_dir = tmp.path().to_path_buf();
        let root = StoreRoot::in_download_dir(&download_dir)
            .path()
            .to_path_buf();
        piece(&root, ADOPTED, "0", "1", 4096);
        piece(&root, ORPHAN, "0", "0", 8192);

        let legacy = download_dir.join("Some Film (2011)");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("film.mkv"), vec![7u8; 4096]).unwrap();

        let report = sweep_before_session(&download_dir, None).await;
        assert_eq!(report, SweepReport::default());
        assert!(root.join(ADOPTED).join("0").join("1").is_file());
        assert!(root.join(ORPHAN).join("0").join("0").is_file());
        assert!(
            legacy.join("film.mkv").is_file(),
            "and a previous release's download is kept with everything else \
             that boot: nobody has said which torrents the user meant to keep"
        );
    }

    /// **The launch sweep takes the previous release's downloads too.**
    ///
    /// Two sweeps, one hop: what no pin claims under the store, and what an
    /// older version left beside it. The second has no other deleter at all
    /// -- see [`sweep_legacy_downloads`] -- so a boot that ran only the
    /// first would leave those bytes for the life of the install.
    #[tokio::test]
    async fn the_launch_sweep_takes_a_previous_releases_download_as_well() {
        let tmp = tempfile::tempdir().unwrap();
        let download_dir = tmp.path().to_path_buf();
        let root = StoreRoot::in_download_dir(&download_dir)
            .path()
            .to_path_buf();
        piece(&root, ADOPTED, "0", "1", 4096);
        let legacy = download_dir.join("Some Film (2011)");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("film.mkv"), vec![7u8; 8192]).unwrap();
        std::fs::write(download_dir.join("session.json"), b"{}").unwrap();

        let pins = PinSet::from([(ADOPTED.to_string(), vec![0usize])]);
        let report = sweep_before_session(&download_dir, Some(&pins)).await;

        assert_eq!(report.removed, 1, "the legacy download: {report:?}");
        assert!(report.freed_bytes >= 8192, "{report:?}");
        assert!(!legacy.exists(), "the previous release's download went");
        assert!(
            root.join(ADOPTED).join("0").join("1").is_file(),
            "and the pin it was asked to keep stayed"
        );
        assert!(
            download_dir.join("session.json").is_file(),
            "and so did the session"
        );
    }
}
