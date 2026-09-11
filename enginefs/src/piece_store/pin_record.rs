//! Who says what is pinned, and what it means when nobody does.
//!
//! A pin is the one claim on this store that outlives a process. Everything
//! else under the piece root is cache -- a window round a playhead, a slack
//! file on its way off the disk -- and the startup sweep
//! ([`super::sweep_before_session`]) deletes all of it before the session
//! opens. So something has to name the pins before that sweep runs, and this
//! server is not it: it keeps no record of its own.
//!
//! **The embedder is the authority.** It hands the set in at startup
//! ([`crate::BackendEngineFS::boot`]), because the one client that pins --
//! the app this is embedded in -- already keeps that list as the downloads
//! the user asked for, and two records of one fact are two records that can
//! disagree. Its list is the one the user sees and the one they act on; a
//! second copy here could only be a stale shadow of it, and the boot sweep
//! would act on the shadow.
//!
//! **`None` is "nobody told me", and it is not "nothing is pinned".** An
//! embedder that passes no set -- one whose own record would not read, one
//! that has not got that far, the standalone binary -- gets [`PinsUnknown`]:
//! nothing is swept, every restored torrent is kept and reported as pinned,
//! and nothing is ever deleted for want of a claim. Treating silence as an
//! empty set is what would delete every offline download a user has, which
//! is why the distinction is a type and not a default.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};

/// What the embedder says is pinned: info hash to the file indices of that
/// torrent it wants kept. Lowercase hashes are what every reader compares
/// against, and [`crate::BackendEngineFS::boot`] lowercases on the way in.
pub type PinSet = BTreeMap<String, Vec<usize>>;

/// The process-wide "the pin set is unknown" condition, set once at boot
/// when the embedder handed no set in, and never cleared: nothing in this
/// process can learn what the embedder did not say.
///
/// It is shared rather than copied because every reader of it has to agree:
/// the retention owner keeps everything while it holds
/// (`TorrentBacking::keeps_everything`) and the reconciler runs every
/// restored torrent as pinned (`Engine::is_pinned`). A copy taken at boot and
/// trusted later is how one half of that would keep deleting while the other
/// half reported everything safe.
#[derive(Debug, Default)]
pub struct PinsUnknown {
    unknown: AtomicBool,
    why: parking_lot::Mutex<String>,
}

impl PinsUnknown {
    /// Declare the pin set unknown, with the reason nobody named it.
    pub fn set(&self, why: impl Into<String>) {
        *self.why.lock() = why.into();
        self.unknown.store(true, Ordering::SeqCst);
    }

    /// Whether the pin set is unknown right now.
    pub fn is_set(&self) -> bool {
        self.unknown.load(Ordering::SeqCst)
    }

    /// Why, or `None` when the pin set is known.
    pub fn why(&self) -> Option<String> {
        self.is_set().then(|| self.why.lock().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The condition carries its reason, and holds for the life of the
    /// process: nothing here can learn a set the embedder did not hand in.
    #[test]
    fn the_unknown_condition_carries_its_reason_and_never_lifts() {
        let unknown = PinsUnknown::default();
        assert!(!unknown.is_set());
        assert_eq!(unknown.why(), None);
        unknown.set("the embedder passed no pin set");
        assert!(unknown.is_set());
        assert_eq!(
            unknown.why().as_deref(),
            Some("the embedder passed no pin set")
        );
    }
}
