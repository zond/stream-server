//! Which staged pieces are waiting to be made durable, process-wide.
//!
//! [`super::store::PieceStore::complete_piece`] no longer flushes and renames
//! on librqbit's completion path: it queues the piece for its store's
//! committer thread and returns, and the piece is read from its staged copy
//! until the rename lands. This is the bookkeeping that makes that safe to
//! race against everything else that addresses the same file:
//!
//! * a delete of the piece (retention's reclaim, `remove_file`) must either
//!   cancel the commit before its rename or wait for the rename to finish and
//!   then delete the result -- never unlink the staged copy while the
//!   committer is renaming it, and never leave a rename to land after the
//!   delete has cleared the held bit;
//! * anything that reads the *durable* record -- librqbit's `has_piece`, and
//!   the walk `init` seeds the held set from -- must see a queued piece's
//!   rename first, or it would call a piece it is about to own absent.
//!
//! Keyed by the staged copy's **path** and kept for the whole process rather
//! than per store, because two stores can stand over one directory: a
//! restart out of error builds a fresh store while the errored one may still
//! have commits queued, and the fresh store's walk has to wait for them.
//!
//! The lock is never held across I/O. A rename on a busy eMMC waits behind
//! the journal like any other metadata operation, and a lock held across it
//! would put every completion of every torrent back behind the device --
//! which is what the queue exists to take them out from behind. So a rename
//! is announced ([`Pending::start_rename`]) and finished
//! ([`Pending::finish`]) under the lock, and done between the two without
//! it; a delete that meets a rename in progress waits on the condvar.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Condvar, Mutex};

/// Where one queued piece is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    /// In a committer's queue or being flushed. A delete may cancel it.
    Queued(u64),
    /// Being renamed into place. A delete waits for it.
    Renaming(u64),
}

impl State {
    fn id(self) -> u64 {
        match self {
            State::Queued(id) | State::Renaming(id) => id,
        }
    }
}

/// The process's queued commits. See the module docs.
pub(super) struct Pending {
    by_path: Mutex<HashMap<PathBuf, State>>,
    changed: Condvar,
    next_id: AtomicU64,
}

/// The one instance.
pub(super) static PENDING: LazyLock<Pending> = LazyLock::new(|| Pending {
    by_path: Mutex::new(HashMap::new()),
    changed: Condvar::new(),
    next_id: AtomicU64::new(1),
});

impl Pending {
    /// Queue the staged copy at `path`, and return the ticket the committer
    /// finishes it by. A commit of the same path still in progress is
    /// waited out first: nothing completes a piece twice without the piece
    /// going in between, but if it ever did, the second must not be taken
    /// for the first -- by its committer, or by a delete waiting on it.
    ///
    /// `and` runs under the same lock as the queueing, so that a delete's
    /// [`Self::cancel`] sees both or neither: the store marks the piece held
    /// there, and a delete that cancelled the commit and then cleared the
    /// bit must not have the bit set again behind it.
    pub(super) fn begin(&self, path: &Path, and: impl FnOnce()) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut map = self.by_path.lock();
        while map.contains_key(path) {
            self.changed.wait(&mut map);
        }
        map.insert(path.to_path_buf(), State::Queued(id));
        and();
        id
    }

    /// Take the commit of `path` off the queue, for a delete about to unlink
    /// the file: after this no rename of it will land. A rename already in
    /// progress is waited for -- its result is the file the delete then
    /// removes. Returns whether a queued commit was cancelled.
    pub(super) fn cancel(&self, path: &Path) -> bool {
        let mut map = self.by_path.lock();
        loop {
            match map.get(path) {
                Some(State::Renaming(_)) => self.changed.wait(&mut map),
                Some(State::Queued(_)) => {
                    map.remove(path);
                    self.changed.notify_all();
                    return true;
                }
                None => return false,
            }
        }
    }

    /// The committer's claim on the rename: false when the commit was
    /// cancelled while it was queued or being flushed, and then the
    /// committer does nothing more with it.
    pub(super) fn start_rename(&self, path: &Path, id: u64) -> bool {
        let mut map = self.by_path.lock();
        match map.get_mut(path) {
            Some(state @ State::Queued(_)) if state.id() == id => {
                *state = State::Renaming(id);
                true
            }
            _ => false,
        }
    }

    /// The commit of `path` is over, however it went.
    pub(super) fn finish(&self, path: &Path, id: u64) {
        let mut map = self.by_path.lock();
        if map.get(path).is_some_and(|state| state.id() == id) {
            map.remove(path);
        }
        self.changed.notify_all();
    }

    /// Whether a commit of `path` is queued or in progress.
    pub(super) fn is_pending(&self, path: &Path) -> bool {
        self.by_path.lock().contains_key(path)
    }

    /// Return once no commit of `path` is queued or in progress.
    pub(super) fn wait_for(&self, path: &Path) {
        let mut map = self.by_path.lock();
        while map.contains_key(path) {
            self.changed.wait(&mut map);
        }
    }

    /// Return once no commit of anything under `dir` is queued or in
    /// progress.
    pub(super) fn wait_under(&self, dir: &Path) {
        let mut map = self.by_path.lock();
        while map.keys().any(|path| path.starts_with(dir)) {
            self.changed.wait(&mut map);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending() -> Pending {
        Pending {
            by_path: Mutex::new(HashMap::new()),
            changed: Condvar::new(),
            next_id: AtomicU64::new(1),
        }
    }

    #[test]
    fn a_cancelled_commit_is_never_renamed() {
        let p = pending();
        let path = Path::new("/x/0/1.part");
        let id = p.begin(path, || ());
        assert!(p.cancel(path));
        assert!(
            !p.start_rename(path, id),
            "the rename of a cancelled commit"
        );
        assert!(!p.is_pending(path));
    }

    /// The half of the interlock the module doc puts first: a delete that
    /// meets a rename in progress does not return until the rename has, so
    /// what it unlinks next is the renamed file and not a name that is
    /// about to appear.
    #[test]
    fn a_delete_waits_out_a_rename_in_progress() {
        let p = std::sync::Arc::new(pending());
        let path = PathBuf::from("/x/0/1.part");
        let id = p.begin(&path, || ());
        assert!(p.start_rename(&path, id));
        let deleting = {
            let (p, path) = (p.clone(), path.clone());
            std::thread::spawn(move || p.cancel(&path))
        };
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            !deleting.is_finished(),
            "the delete went ahead of the rename"
        );
        p.finish(&path, id);
        assert!(
            !deleting.join().unwrap(),
            "a finished rename is not a queued commit to cancel"
        );
    }

    #[test]
    fn a_second_commit_of_a_path_waits_for_the_first() {
        let p = std::sync::Arc::new(pending());
        let path = PathBuf::from("/x/0/1.part");
        let first = p.begin(&path, || ());
        let second = {
            let (p, path) = (p.clone(), path.clone());
            std::thread::spawn(move || p.begin(&path, || ()))
        };
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!second.is_finished(), "queued over a commit in progress");
        assert!(p.start_rename(&path, first), "the first is still its own");
        p.finish(&path, first);
        let second = second.join().unwrap();
        assert!(p.start_rename(&path, second));
    }

    #[test]
    fn a_wait_under_a_directory_is_released_by_its_last_commit_only() {
        let p = std::sync::Arc::new(pending());
        let a = PathBuf::from("/t/abc/0/1.part");
        let b = PathBuf::from("/t/abc/0/2.part");
        let other = PathBuf::from("/t/def/0/1.part");
        let (ia, ib) = (p.begin(&a, || ()), p.begin(&b, || ()));
        p.begin(&other, || ());
        let waiting = {
            let p = p.clone();
            std::thread::spawn(move || p.wait_under(Path::new("/t/abc")))
        };
        p.finish(&a, ia);
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            !waiting.is_finished(),
            "released with a commit still queued"
        );
        p.finish(&b, ib);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !waiting.is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "still waiting on another directory's commit"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(p.is_pending(&other), "another torrent's commit is its own");
    }
}
