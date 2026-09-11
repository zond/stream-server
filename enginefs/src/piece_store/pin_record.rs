//! The pin record: `pinned-downloads.json` as a value, read before anything
//! else at boot.
//!
//! A pin is the one claim on this store that outlives a process. Everything
//! else under the piece root is cache -- a window round a playhead, a slack
//! file on its way off the disk -- and the startup sweep
//! ([`super::sweep_before_session`]) deletes all of it before the session
//! opens. That makes this file the *only* thing standing between a user's
//! offline downloads and `remove_dir_all`, and it is why reading it is a
//! value rather than a side effect: the sweep cannot run until the record has
//! answered, and "could not read it" is an answer of its own.
//!
//! Three answers, and the third is the one that exists at all because the
//! sweep's claim set narrowed to this file:
//!
//! * [`PinRecord::Pins`] -- the record parsed. Its keys are the claim.
//! * [`PinRecord::Absent`] -- no file. The ordinary first-launch state, and
//!   an empty claim set: nothing is pinned, so nothing is kept.
//! * [`PinRecord::Unreadable`] -- the file is there and will not be read.
//!   Treating that as "no pins" is what would delete every offline download
//!   a truncated flush or a half-written rename left behind. Nothing is
//!   swept, every restored torrent is reported and kept as pinned
//!   ([`PinsUnknown`]), and the record is not overwritten until a pin or an
//!   unpin has made a true one.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

/// Where the pin set is persisted, relative to the download dir.
pub const PINNED_DOWNLOADS_FILE: &str = "pinned-downloads.json";

/// `pinned-downloads.json` in `download_dir`, which is where both the reader
/// here and `BackendEngineFS::persist_pinned_downloads` spell it.
pub fn path(download_dir: &Path) -> PathBuf {
    download_dir.join(PINNED_DOWNLOADS_FILE)
}

/// What the pin record said, or why it did not say anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinRecord {
    /// `{ "<info hash>": [file indices] }`, as the last change wrote it.
    Pins(BTreeMap<String, Vec<usize>>),
    /// No file at all: first launch, or a user who has never pinned.
    Absent,
    /// The file exists and could not be turned into a pin set, with the
    /// reason -- a parse error, or an I/O error that is not `NotFound`.
    Unreadable(String),
}

impl PinRecord {
    /// The pins to re-apply: the record's own map, and nothing for the two
    /// answers that carry no map. An unreadable record applies no pin
    /// *because it names none*, which is the opposite of naming none.
    pub fn pins(&self) -> BTreeMap<String, Vec<usize>> {
        match self {
            Self::Pins(pins) => pins.clone(),
            Self::Absent | Self::Unreadable(_) => BTreeMap::new(),
        }
    }

    /// The info hashes the sweep may not touch, lowercased -- the record's
    /// keys and nothing else, because a pin is now the only claim on piece
    /// data that survives a restart.
    ///
    /// Empty for [`Self::Unreadable`], which is safe only because the sweep
    /// does not run at all in that state ([`Self::unreadable`]); asking this
    /// of an unreadable record and sweeping on the answer is the data loss
    /// the variant exists to prevent.
    pub fn claims(&self) -> HashSet<String> {
        match self {
            Self::Pins(pins) => pins.keys().map(|hash| hash.to_lowercase()).collect(),
            Self::Absent | Self::Unreadable(_) => HashSet::new(),
        }
    }

    /// Why the record could not be read, or `None` when it could.
    pub fn unreadable(&self) -> Option<&str> {
        match self {
            Self::Unreadable(why) => Some(why),
            _ => None,
        }
    }
}

/// Read the pin record out of `download_dir`. A pure file read: it opens one
/// file, parses it and returns. Nothing here creates a directory, warns or
/// remembers anything, so it can run before the session -- before anything
/// at all -- which is what the sweep's ordering needs.
pub fn read(download_dir: &Path) -> PinRecord {
    let path = path(download_dir);
    match std::fs::read(&path) {
        Ok(bytes) => match serde_json::from_slice::<BTreeMap<String, Vec<usize>>>(&bytes) {
            Ok(pins) => PinRecord::Pins(pins),
            Err(error) => PinRecord::Unreadable(error.to_string()),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => PinRecord::Absent,
        Err(error) => PinRecord::Unreadable(error.to_string()),
    }
}

/// The process-wide "the pin set is unknown" condition, set once at boot by
/// a [`PinRecord::Unreadable`] and cleared by the first pin or unpin that
/// writes a true record.
///
/// It is shared rather than copied because every reader of it has to agree
/// within one tick: the retention owner keeps everything while it holds
/// (`TorrentBacking::keeps_everything`), the reconciler runs every restored
/// torrent as pinned (`Engine::is_pinned`), the persister refuses to write
/// the record, and `GET /downloads.json` says so on the wire. A copy taken at
/// boot and trusted later is how one half of that would keep deleting while
/// the other half reported everything safe.
#[derive(Debug, Default)]
pub struct PinsUnknown {
    unknown: AtomicBool,
    why: parking_lot::Mutex<String>,
}

impl PinsUnknown {
    /// Declare the pin set unknown, with the reason the record gave.
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

    /// The pin set is known again: a pin or an unpin has materialised the
    /// true set and written it. Ordered after that write, never before --
    /// clearing first would let a crash in between commit the loss the
    /// condition exists to refuse.
    pub fn clear(&self) {
        self.unknown.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH: &str = "0123456789abcdef0123456789abcdef01234567";

    /// The three answers, off the disk. The distinction that matters is
    /// between "no file" and "a file that would not read": one is an empty
    /// claim set the sweep acts on, the other is no claim set at all.
    #[test]
    fn a_missing_record_is_absent_and_a_broken_one_is_unreadable() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        assert_eq!(read(dir), PinRecord::Absent);
        assert!(read(dir).claims().is_empty());
        assert!(read(dir).unreadable().is_none());

        std::fs::write(
            path(dir),
            serde_json::json!({ HASH.to_uppercase(): [0, 2] }).to_string(),
        )
        .unwrap();
        let record = read(dir);
        assert_eq!(
            record.pins(),
            BTreeMap::from([(HASH.to_uppercase(), vec![0, 2])])
        );
        assert_eq!(record.claims(), HashSet::from([HASH.to_string()]));

        std::fs::write(path(dir), b"not json at all").unwrap();
        let record = read(dir);
        assert!(record.unreadable().is_some(), "{record:?}");
        assert!(
            record.claims().is_empty(),
            "and it claims nothing, which is why the sweep may not run on it"
        );

        // A directory where the file should be: not a parse error, and not
        // `NotFound` either -- the other half of "unreadable".
        std::fs::remove_file(path(dir)).unwrap();
        std::fs::create_dir(path(dir)).unwrap();
        assert!(read(dir).unreadable().is_some());
    }

    /// The condition carries its reason, and only a write of a true record
    /// clears it.
    #[test]
    fn the_unknown_condition_holds_until_it_is_cleared() {
        let unknown = PinsUnknown::default();
        assert!(!unknown.is_set());
        assert_eq!(unknown.why(), None);
        unknown.set("expected value at line 1");
        assert!(unknown.is_set());
        assert_eq!(unknown.why().as_deref(), Some("expected value at line 1"));
        unknown.clear();
        assert!(!unknown.is_set());
        assert_eq!(unknown.why(), None);
    }
}
