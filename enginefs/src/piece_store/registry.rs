//! Which store is the live one for each torrent under one root, so that
//! whoever decides retention reads the held set the store keeps and unlinks
//! through the store that keeps it.
//!
//! The live [`PieceStore`] of a running torrent is librqbit's: the factory
//! builds it and hands it to a torrent state this crate holds no reference
//! to. So the retention pass used to list the torrent's directory every
//! couple of seconds to learn what the store already knew, and the reclaim
//! unlinked by path behind the store's back -- which left the store's
//! open-handle cache holding an unlinked inode's blocks, and would have left
//! a held set it never told about the unlink standing over files that had
//! gone. The registry is the way back in: a store registers itself when
//! `init` has seeded it, the pass asks the registry for the set, and every
//! unlink of a registered torrent's piece goes through the registered
//! store's own delete.
//!
//! A registration is a `Weak`, and the live store is whichever `Inner` the
//! entry points at. A torrent in Error holds no storage -- librqbit pauses
//! (a take) and drops the successor -- so its `Inner` goes, takes its
//! registration with it, and the registry answers "no store" for the hash.
//! That is the answer the pass concludes nothing over; it is never an empty
//! set, for the reason [`PieceStore::held`] gives. It is an answer the
//! registry reaches *after* the state does: the `Inner` lives as long as
//! its last handle, and the errored live state's handle goes when the peer
//! tasks it cancelled have exited -- milliseconds after `run_state` says
//! Error. In that gap the hash still answers a set and a delete through it
//! still goes; nothing here reads the run state, and nothing that reads it
//! may trust one reading across an unlink.
//!
//! [`PieceStore`]: super::PieceStore
//! [`PieceStore::held`]: super::PieceStore::held

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

#[cfg(test)]
use super::store::PieceStore;
use super::store::{HeldSnapshot, Inner, StoreRoot};

/// The stores under one root, by the info hash each registered under.
///
/// One per [`super::PieceStoreFactory`], shared with everything that asks
/// after a torrent's held set or deletes a piece of it. Two sessions in one
/// process over two roots -- which the tests open -- have two of these, so
/// a hash is looked up only among the stores that could hold it.
pub struct StoreRegistry {
    /// The root the registered stores are under. What a delete of a hash no
    /// store is registered for still has to address, until every unlink in
    /// the process goes through a registered store.
    root: StoreRoot,
    by_hash: Mutex<HashMap<String, Weak<Inner>>>,
    /// The last epoch handed out, to any store under this root. One counter
    /// rather than one per hash because it is only ever compared with
    /// itself, and a hash that has had two stores is what it exists to
    /// tell apart -- see [`Self::insert`].
    epochs: AtomicU64,
}

/// What [`StoreRegistry::delete`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    /// A store is registered for the hash and the delete went through it:
    /// this many pieces had a file that really left the disk -- either
    /// copy, since either occupies blocks the volume gets back.
    Registered { unlinked: usize },
    /// A store is registered and its initial hash check may still be
    /// reading it, so nothing was unlinked: a piece taken from under the
    /// check is one the check has just claimed and the torrent then
    /// advertises without having.
    Refused,
    /// No store is registered for the hash -- the session does not hold the
    /// torrent, holds it in Error, or the store has not run `init`. Nothing
    /// was unlinked; the caller decides whether a path-addressed delete is
    /// its to make.
    Unregistered,
}

impl StoreRegistry {
    /// An empty registry over `root`.
    pub fn new(root: StoreRoot) -> Self {
        Self {
            root,
            by_hash: Mutex::new(HashMap::new()),
            epochs: AtomicU64::new(0),
        }
    }

    /// The root the registered stores are under.
    pub fn root(&self) -> &StoreRoot {
        &self.root
    }

    /// The held set of the live store registered for `info_hash`, or `None`
    /// when there is no such store: the session does not hold the torrent,
    /// holds it in Error, or its `init` has not run. Synchronous and
    /// lock-free past the map lookup -- a copy of the store's atomic words,
    /// no listing -- which is what lets a retention pass read it on every
    /// tick without a directory walk.
    pub fn held(&self, info_hash: &str) -> Option<HeldSnapshot> {
        self.live(info_hash)?.held()
    }

    /// Unlink `pieces` of `info_hash` through its registered store, so the
    /// store forgets its cached handles and clears its held bits with the
    /// files; or say why not.
    ///
    /// **Only for a caller that has already had the backend forget the
    /// pieces** and is holding that claim across this call -- the interlock
    /// [`crate::retention::take_claimed`] imposes, and the one way into the
    /// store's files there is. Refused outright while the store is under its
    /// initial check, asked at this instant and not earlier: the check
    /// reads every piece it means to claim, and a piece unlinked from under
    /// it becomes a have-bit over nothing.
    ///
    /// A piece the volume refuses to unlink is logged and stepped over, not
    /// returned: the count of what did go is what `ENOSPC` recovery reads.
    /// Its held bit stays, so the next pass offers it again, and the backend
    /// hands a piece it has already forgotten to that pass's claim once
    /// this one is released -- the retry is the next pass's, not the
    /// caller's.
    ///
    /// What this does not close: the check is asked of the store registered
    /// at entry and the run is unlinked through that store. A restart out
    /// of error that registers a fresh store over the same directory while
    /// the run is still being unlinked has that store's check reading files
    /// this loop is removing -- a window the length of one run's unlinks,
    /// open only for a torrent that errored between the claim and here.
    /// Closing it means an unlink under the registry's lock, which `init`
    /// takes on the reactor under librqbit's own; it is left open and named.
    pub fn delete(&self, info_hash: &str, pieces: &[u32]) -> DeleteOutcome {
        let Some(inner) = self.live(info_hash) else {
            return DeleteOutcome::Unregistered;
        };
        if inner.is_checking() {
            return DeleteOutcome::Refused;
        }
        let mut unlinked = 0;
        for piece in pieces {
            match inner.delete_piece(*piece) {
                Ok(taken) => unlinked += usize::from(taken),
                Err(error) => tracing::warn!(
                    info_hash = %info_hash,
                    piece,
                    error = %format!("{error:#}"),
                    "a piece the backend had agreed to forget could not be unlinked"
                ),
            }
        }
        DeleteOutcome::Registered { unlinked }
    }

    /// Whether a live store is registered for `info_hash` at all -- the
    /// question a delete **with no claim** asks before it touches anything.
    /// A registered store is one `init` seeded for a torrent librqbit
    /// holds, and that torrent keeps a have-set over the same pieces; an
    /// unlink nobody had the backend forget first leaves that have-set
    /// standing over nothing. So the claimless door goes by path only where
    /// this is false, and steps back where it is true -- whatever state the
    /// caller read the torrent in earlier, because the read and the unlink
    /// are two instants and a restart fits between them.
    pub fn is_registered(&self, info_hash: &str) -> bool {
        self.live(info_hash).is_some()
    }

    /// What every registered store holds, in bytes, by the held bits and
    /// the layout -- no I/O. What a budget or a usage figure is built from
    /// once nothing walks the disk to count it.
    pub fn occupancy(&self) -> u64 {
        self.every_live()
            .filter_map(|inner| inner.held())
            .map(|held| held.bytes())
            .sum()
    }

    /// What lies under the root that no registered store's held set
    /// accounts for, in bytes -- the other half of [`Self::occupancy`].
    ///
    /// One `read_dir` of the root and a `stat` of every directory no live
    /// store speaks for: a torrent the session holds in Error (which holds
    /// no storage, so it has no registration), one a previous process left,
    /// and whatever is not a piece file. And the staged copies of the
    /// stores that are registered, one `stat` each: a held set counts
    /// complete pieces, so without them every piece in flight was in
    /// neither half. Filesystem work, so it belongs off the reactor and is
    /// asked on demand, never on the tick.
    ///
    /// What a registered store's directory holds beyond its pieces and
    /// their staged copies is in neither half. Nothing writes such a file
    /// there, and finding one would take the walk of the tree this exists
    /// not to make.
    ///
    /// The registration is asked at this instant and the `stat` follows it,
    /// so a torrent that registers between the two is counted twice for the
    /// length of one usage figure and a torrent that errors between them is
    /// counted by neither. Both are a reading of a moving tree, which is
    /// what a usage figure is.
    pub fn unregistered_bytes(&self) -> u64 {
        self.root
            .unregistered_bytes(|info_hash| self.is_registered(info_hash))
            + self
                .every_live()
                .map(|inner| inner.staged_bytes())
                .sum::<u64>()
    }

    /// Whether the store registered for `info_hash` is under its initial
    /// hash check. False for a hash with no store: there is nothing to
    /// check.
    pub fn checking(&self, info_hash: &str) -> bool {
        self.live(info_hash)
            .is_some_and(|inner| inner.is_checking())
    }

    /// Which store of `info_hash`'s the live one is, or `None` for a hash
    /// with no store.
    ///
    /// It moves when a fresh store registers over an older one, which is
    /// what a restart out of error does -- and a restart out of error is
    /// when librqbit rebuilds the chunk tracker and forgets every hold-back
    /// it was told. That is the whole of what this is read for, so it must
    /// move across the *fresh* store rather than count the seeds of each:
    /// a per-store count starts over with the store, and a reader comparing
    /// it would be told nothing had happened. See [`Self::insert`].
    pub fn epoch(&self, info_hash: &str) -> Option<u64> {
        self.live(info_hash).map(|inner| inner.epoch())
    }

    /// How many hashes have an entry, live or not: the probe for the tests
    /// that pin what a store's drop does to the map.
    #[cfg(test)]
    pub fn registration_count(&self) -> usize {
        self.by_hash.lock().len()
    }

    /// The store registered for `info_hash` whose `Inner` is still alive.
    /// Lowercased on the way in, as [`StoreRoot::torrent_dir`] lowercases:
    /// a caller that spelled the hash any other way asks about the same
    /// store.
    fn live(&self, info_hash: &str) -> Option<Arc<Inner>> {
        self.by_hash
            .lock()
            .get(&info_hash.to_ascii_lowercase())?
            .upgrade()
    }

    fn every_live(&self) -> impl Iterator<Item = Arc<Inner>> {
        let live: Vec<Arc<Inner>> = self
            .by_hash
            .lock()
            .values()
            .filter_map(Weak::upgrade)
            .collect();
        live.into_iter()
    }

    /// Make `inner` the live store for `info_hash`, replacing whatever was
    /// there: a restart out of error builds a fresh store while the old
    /// one's registration may not have gone yet, and the newest is the one
    /// librqbit reads and writes.
    ///
    /// A store the hash has not had before takes an epoch here, and one
    /// that is already the registered store keeps the one it has: the epoch
    /// names the store, not the seed ([`Inner::epoch`]), and a store that
    /// seeds again is the same store with the same chunk tracker beside it
    /// -- what it holds may have changed, but nothing has forgotten what we
    /// held back. Assigned under the map lock, so the number a reader gets
    /// with a registration is the one that registration was given.
    pub(super) fn insert(&self, info_hash: &str, inner: &Arc<Inner>) {
        let mut by_hash = self.by_hash.lock();
        let key = info_hash.to_ascii_lowercase();
        let already = by_hash
            .get(&key)
            .is_some_and(|weak| Weak::as_ptr(weak) == Arc::as_ptr(inner));
        if !already {
            inner.set_epoch(self.epochs.fetch_add(1, Ordering::Relaxed) + 1);
        }
        by_hash.insert(key, Arc::downgrade(inner));
    }

    /// Drop the registration for `info_hash` if it still points at
    /// `inner` -- from `Inner`'s drop, so an earlier store going away never
    /// unregisters the one that replaced it.
    pub(super) fn forget(&self, info_hash: &str, inner: *const Inner) {
        let mut by_hash = self.by_hash.lock();
        let key = info_hash.to_ascii_lowercase();
        if by_hash
            .get(&key)
            .is_some_and(|weak| Weak::as_ptr(weak) == inner)
        {
            by_hash.remove(&key);
        }
    }
}

impl std::fmt::Debug for StoreRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreRegistry")
            .field("root", &self.root)
            .field("registered", &self.by_hash.lock().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::piece_store::layout::{FileSpec, PieceLayout};
    use librqbit::storage::TorrentStorage;
    use std::collections::BTreeSet;
    use std::path::Path;

    const HASH: &str = "0123456789abcdef0123456789abcdef01234567";
    const PIECE_LENGTH: u64 = 8;

    /// Four 8-byte pieces over one 30-byte file; the last piece is short.
    fn layout() -> Arc<PieceLayout> {
        Arc::new(PieceLayout::new(PIECE_LENGTH, 30, [FileSpec::payload(30)]).expect("layout"))
    }

    fn registry(root: &Path) -> Arc<StoreRegistry> {
        Arc::new(StoreRegistry::new(StoreRoot::new(root.join(".pieces"))))
    }

    /// A store the way the factory makes one, over the registry's root.
    fn store_under(registry: &Arc<StoreRegistry>) -> PieceStore {
        PieceStore::under(Arc::clone(registry), HASH, layout())
    }

    fn write_piece(store: &PieceStore, piece: u32) {
        let offset = u64::from(piece) * PIECE_LENGTH;
        let len = store.layout().piece_length_of(piece) as usize;
        store
            .pwrite_all(0, offset, &vec![piece as u8 + 1; len])
            .expect("write");
        store.complete_piece(piece).expect("complete");
    }

    fn held_of(registry: &StoreRegistry) -> Option<BTreeSet<u32>> {
        registry.held(HASH).map(|held| held.in_range(0..4))
    }

    /// The store the factory makes is the one the registry answers for, and
    /// only once `init` has seeded it: before that it is a store that holds
    /// unknown, and a registration of unknown would be read as a torrent
    /// holding nothing. `Session::delete` builds exactly such a store --
    /// create with no `init` -- to delete through, and it must neither
    /// register itself nor push the live store out.
    #[test]
    fn a_store_registers_when_init_seeds_it_and_a_fallback_store_never_does() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = registry(tmp.path());
        let store = store_under(&registry);
        assert!(
            registry.held(HASH).is_none(),
            "a store nothing has seeded is not registered"
        );
        assert_eq!(registry.delete(HASH, &[0]), DeleteOutcome::Unregistered);
        assert!(!registry.checking(HASH));
        assert!(!registry.is_registered(HASH));
        assert_eq!(registry.epoch(HASH), None);

        std::fs::create_dir_all(store.dir()).unwrap();
        write_piece(&store, 1);
        store.init_for_tests().unwrap();
        assert_eq!(
            held_of(&registry),
            Some(BTreeSet::from([1])),
            "seeded, and so registered, with what the seed found"
        );
        assert!(registry.is_registered(HASH));
        assert_eq!(registry.epoch(HASH), Some(1));

        // The delete fallback: a fresh store over the same hash, no init.
        let fallback = store_under(&registry);
        assert_eq!(
            held_of(&registry),
            Some(BTreeSet::from([1])),
            "a store that has not run init pushes nothing out of the registry"
        );
        drop(fallback);
        assert_eq!(
            held_of(&registry),
            Some(BTreeSet::from([1])),
            "and its drop unregisters nothing, because it registered nothing"
        );

        // A completion on the registered store is in the next reading
        // without anything listing the directory.
        write_piece(&store, 2);
        assert_eq!(held_of(&registry), Some(BTreeSet::from([1, 2])));
    }

    /// A restart out of error seeds a fresh store for the same hash; the
    /// registration is the newest store's, and the old one's drop -- which
    /// librqbit times as it likes -- must not take the new one's
    /// registration with it. Once the last store is gone the hash answers
    /// as a torrent in Error does: no store, not an empty set.
    #[test]
    fn the_newest_seeded_store_wins_and_an_older_ones_drop_leaves_it_registered() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = registry(tmp.path());
        let first = store_under(&registry);
        std::fs::create_dir_all(first.dir()).unwrap();
        write_piece(&first, 0);
        first.init_for_tests().unwrap();
        assert_eq!(held_of(&registry), Some(BTreeSet::from([0])));

        let second = store_under(&registry);
        second.init_for_tests().unwrap();
        write_piece(&second, 3);
        assert_eq!(
            held_of(&registry),
            Some(BTreeSet::from([0, 3])),
            "the second init replaced the registration: its completion is what is read"
        );
        assert_eq!(
            first.held().unwrap().in_range(0..4),
            BTreeSet::from([0]),
            "the first store is a different Inner and knows nothing of the second's completion"
        );
        assert_eq!(
            registry.epoch(HASH),
            Some(2),
            "the second seed, though it is the fresh store's first: what the \
             epoch is asked is whether the hold-back survived, and a number \
             each store counted for itself would say yes here"
        );

        drop(first);
        assert_eq!(
            held_of(&registry),
            Some(BTreeSet::from([0, 3])),
            "the earlier store's drop does not unregister the one that replaced it"
        );
        assert_eq!(registry.registration_count(), 1);
        let taken = second.take().unwrap();
        drop(second);
        assert_eq!(
            held_of(&registry),
            Some(BTreeSet::from([0, 3])),
            "a taken handle dropping leaves the Inner its successor still holds"
        );
        drop(taken);
        assert!(
            registry.held(HASH).is_none(),
            "the last handle gone, the hash has no store: unknown, not empty"
        );
        assert_eq!(
            registry.registration_count(),
            0,
            "and the store took its own registration with it"
        );
        assert_eq!(registry.delete(HASH, &[0]), DeleteOutcome::Unregistered);
    }

    /// The registered delete is the store's own: the cached handle goes
    /// with the file, the bit goes with the unlink, and a piece that was not
    /// there is not counted. `StoreRoot::delete_pieces` unlinked by path and
    /// could do neither of the first two.
    #[test]
    fn a_registry_delete_forgets_the_cached_handle_and_clears_the_bit() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = registry(tmp.path());
        let store = store_under(&registry);
        std::fs::create_dir_all(store.dir()).unwrap();
        for piece in 0..4 {
            write_piece(&store, piece);
        }
        store.init_for_tests().unwrap();

        let mut buf = [0u8; 8];
        store.pread_exact(0, 16, &mut buf).unwrap();
        assert_eq!(
            registry.delete(HASH, &[2, 3]),
            DeleteOutcome::Registered { unlinked: 2 }
        );
        let err = store.pread_exact(0, 16, &mut buf).unwrap_err();
        assert!(
            err.chain()
                .any(|e| e.downcast_ref::<super::super::MissingPiece>().is_some()),
            "the read sees the deletion, not the handle that was cached: {err:#}"
        );
        assert_eq!(held_of(&registry), Some(BTreeSet::from([0, 1])));
        assert!(!store.piece_path(2).exists() && !store.piece_path(3).exists());
        assert_eq!(
            registry.delete(HASH, &[3]),
            DeleteOutcome::Registered { unlinked: 0 },
            "a piece already gone was not on the disk to leave it"
        );
    }

    /// From `init` until the take that ends the initial check, the check may
    /// be reading any piece the seed found; a delete asked in that window is
    /// refused and the file and its bit stay. The instant the check has
    /// handed over, the same delete goes through.
    #[test]
    fn a_delete_asked_while_the_store_is_checking_is_refused_and_takes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = registry(tmp.path());
        let store = store_under(&registry);
        std::fs::create_dir_all(store.dir()).unwrap();
        write_piece(&store, 1);
        // `init`'s seed, not the test one: the check begins here.
        store.seed_from_disk().unwrap();
        assert!(registry.checking(HASH));
        assert_eq!(registry.delete(HASH, &[1]), DeleteOutcome::Refused);
        assert!(store.piece_path(1).is_file(), "the file stays");
        assert_eq!(
            held_of(&registry),
            Some(BTreeSet::from([1])),
            "and so does the bit"
        );

        let successor = store.take().unwrap();
        assert!(!registry.checking(HASH), "the take is the end of the check");
        assert_eq!(
            registry.delete(HASH, &[1]),
            DeleteOutcome::Registered { unlinked: 1 }
        );
        assert!(!store.piece_path(1).exists());
        assert_eq!(held_of(&registry), Some(BTreeSet::new()));
        drop(successor);
    }

    /// A hash is one store however it is spelled. librqbit and
    /// `StoreRoot::torrent_dir` spell it lowercase and the server lowercases
    /// before it asks, but the registry is the meeting point of every
    /// caller, and one that spelled the hash as the user typed it would
    /// otherwise be told the torrent it is looking at has no store.
    #[test]
    fn a_hash_answers_the_same_store_however_it_is_spelled() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = registry(tmp.path());
        let store = store_under(&registry);
        std::fs::create_dir_all(store.dir()).unwrap();
        write_piece(&store, 2);
        store.init_for_tests().unwrap();
        let upper = HASH.to_ascii_uppercase();
        assert_eq!(
            registry.held(&upper).map(|held| held.in_range(0..4)),
            Some(BTreeSet::from([2]))
        );
        assert!(registry.is_registered(&upper));
        assert_eq!(registry.epoch(&upper), Some(1));
        assert_eq!(
            registry.delete(&upper, &[2]),
            DeleteOutcome::Registered { unlinked: 1 }
        );
        assert_eq!(held_of(&registry), Some(BTreeSet::new()));
    }

    /// **What the registry counts and what it has to `stat`.**
    ///
    /// A registered store's bytes are its bits: no syscall, and the
    /// directory is stepped over entirely. What no store speaks for is
    /// everything else on the volume under this root -- a torrent held in
    /// Error, a directory a previous process left, a file the store would
    /// never have written -- and a usage figure that left those out would
    /// read smaller than the disk does. So they are `stat`ed, and the two
    /// halves are disjoint by construction: a hash is in one or the other,
    /// never both.
    #[test]
    fn what_no_store_speaks_for_is_stated_and_what_one_does_is_stepped_over() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = registry(tmp.path());
        assert_eq!(registry.unregistered_bytes(), 0, "no root yet");

        let store = store_under(&registry);
        std::fs::create_dir_all(store.dir()).unwrap();
        write_piece(&store, 0);
        store.init_for_tests().unwrap();
        assert_eq!(registry.occupancy(), 8);
        assert_eq!(
            registry.unregistered_bytes(),
            0,
            "the registered store's own directory is counted from its bits"
        );

        // A second torrent nothing registered: a previous process's, or one
        // this session holds in Error.
        let orphan = StoreRoot::new(tmp.path().join(".pieces"));
        let left = orphan.torrent_dir("89abcdef0123456789abcdef0123456789abcdef");
        std::fs::create_dir_all(left.join("0")).unwrap();
        std::fs::write(left.join("0").join("0"), [1u8; 8]).unwrap();
        // And debris directly under the root, which addresses no torrent at
        // all.
        std::fs::write(tmp.path().join(".pieces").join("stray"), [2u8; 8]).unwrap();
        // And a *directory* whose name this store would never have written:
        // a hash is lowercase here, so no registration can speak for this
        // one and `stat` of it would read the lowercase directory beside it
        // instead. It is on the volume, so what is in it is counted, and it
        // is counted the one way that cannot address the wrong files --
        // everything under the name, as a stray.
        //
        // Only where the filesystem tells the two names apart. On a
        // case-insensitive volume -- Windows, and macOS by default -- the
        // shouted name *is* the lowercase directory, there is no second
        // entry for a rule to get wrong, and writing through it would just
        // overwrite the piece written above. Asked there, this half of the
        // test would be asserting something about the filesystem.
        let shouted = tmp
            .path()
            .join(".pieces")
            .join("89ABCDEF0123456789ABCDEF0123456789ABCDEF");
        let case_matters = std::fs::metadata(&shouted).is_err();
        let mut expected = crate::chunk_store::occupied_bytes(
            &std::fs::metadata(left.join("0").join("0")).unwrap(),
        ) + crate::chunk_store::occupied_bytes(
            &std::fs::metadata(tmp.path().join(".pieces").join("stray")).unwrap(),
        );
        if case_matters {
            std::fs::create_dir_all(shouted.join("0")).unwrap();
            // Deliberately a different size from the lowercase directory's
            // piece: a rule that let this name through would `stat` the
            // *lowercase* directory beside it, and the two would only add
            // up if both held the same bytes.
            std::fs::write(shouted.join("0").join("0"), [3u8; 40960]).unwrap();
            expected += crate::chunk_store::occupied_bytes(
                &std::fs::metadata(shouted.join("0").join("0")).unwrap(),
            );
        }
        assert_eq!(registry.unregistered_bytes(), expected);
        assert_eq!(
            registry.occupancy(),
            8,
            "and none of it moved what the stores hold"
        );
    }

    /// **A piece in flight is on the volume, so it is in the figure.** A
    /// held set counts complete pieces, and the directory of a registered
    /// store is stepped over, so a staged copy was in neither half: up to a
    /// piece per piece in flight, and the copy a re-download writes over a
    /// complete one, all missing from `GET /cache.json`.
    #[test]
    fn a_registered_stores_staged_copies_are_in_the_unregistered_half() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = registry(tmp.path());
        let store = store_under(&registry);
        std::fs::create_dir_all(store.dir()).unwrap();
        write_piece(&store, 0);
        store.init_for_tests().unwrap();
        // Half of piece 1, and piece 0 being written again over its
        // complete copy.
        store.pwrite_all(0, PIECE_LENGTH, &[7u8; 4]).unwrap();
        store.pwrite_all(0, 0, &[7u8; 8]).unwrap();

        let staged = [0u32, 1]
            .iter()
            .map(|piece| {
                crate::chunk_store::occupied_bytes(
                    &std::fs::metadata(store.staging_path(*piece)).unwrap(),
                )
            })
            .sum::<u64>();
        assert!(staged > 0, "the staged copies are on the volume");
        assert_eq!(registry.unregistered_bytes(), staged);
        assert_eq!(registry.occupancy(), 8, "the complete piece, from its bit");
    }

    /// Occupancy is the registered stores' held bits priced by their
    /// layouts -- the short last piece at its own length -- and it follows
    /// completions and unlinks without a `stat`. A store nothing has seeded
    /// and a hash whose store is gone contribute nothing.
    #[test]
    fn occupancy_is_the_bytes_of_the_set_bits_and_moves_with_completion_and_unlink() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = registry(tmp.path());
        assert_eq!(registry.occupancy(), 0);
        let store = store_under(&registry);
        std::fs::create_dir_all(store.dir()).unwrap();
        write_piece(&store, 0);
        assert_eq!(
            registry.occupancy(),
            0,
            "unseeded, so unregistered, so uncounted"
        );
        store.init_for_tests().unwrap();
        assert_eq!(registry.occupancy(), 8);

        write_piece(&store, 3);
        assert_eq!(registry.occupancy(), 8 + 6, "the last piece is six bytes");

        // A second torrent under the same root, seeded whole.
        let other_layout =
            Arc::new(PieceLayout::new(PIECE_LENGTH, 16, [FileSpec::payload(16)]).expect("layout"));
        let other = PieceStore::under(
            Arc::clone(&registry),
            "89abcdef0123456789abcdef0123456789abcdef",
            other_layout,
        );
        std::fs::create_dir_all(other.dir()).unwrap();
        for piece in 0..2 {
            let offset = u64::from(piece) * PIECE_LENGTH;
            other.pwrite_all(0, offset, &[9u8; 8]).unwrap();
            other.complete_piece(piece).unwrap();
        }
        other.init_for_tests().unwrap();
        assert_eq!(registry.occupancy(), 14 + 16);

        assert_eq!(
            registry.delete(HASH, &[0]),
            DeleteOutcome::Registered { unlinked: 1 }
        );
        assert_eq!(
            registry.occupancy(),
            6 + 16,
            "an unlink takes its bytes off the count"
        );
        drop(other);
        assert_eq!(
            registry.occupancy(),
            6,
            "a store that is gone holds nothing"
        );
    }
}
