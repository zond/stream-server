//! **Who keeps a torrent running, said by the holder**: explicit holds,
//! each with an owner and a lifetime, which the reconciler combines with
//! the pins and the idle-sharing policy rather than guessing from what
//! happens to be reading.
//!
//! The question "should this torrent run?" used to be answered from two
//! inferences: the liveness cell ([`crate::retention::live`]) -- the last
//! entity a stream opened on, which a stream of anything else moves -- and
//! the reads open on the torrent at the instant of the tick. Neither is
//! what the app knows. A phone that handed its film to a television had
//! its own player paused and no read open between the receiver's
//! requests; any other open (a link through `/proxy`, a subtitle on
//! another torrent) moved the cell, and the next tick stopped the torrent
//! the television was waiting on -- measured on a phone,
//! `torrent_stopped_by_reconciler playing=false` just after a cast began.
//! The app *knows* the torrent is in use: a player screen is open on it,
//! or a cast of it is published. So it says so, and the hold lasts exactly
//! as long as that is true.
//!
//! Two kinds, and nothing else writes here:
//!
//! * **A player screen** ([`Holds::hold_for_player`]): the torrent the
//!   viewer's newest screen's own requests named -- the same told play
//!   that moves the play session ([`crate::retention::sessions`]) -- held
//!   from that request until the screen says it is gone
//!   ([`Holds::release_player`]), however long it is paused or stalled and
//!   whether or not a read is open. One per viewer: a newer screen of the
//!   viewer replaces an older one's hold, so a screen that never said
//!   goodbye (a crash in the app's teardown) holds nothing once the viewer
//!   plays anything else, and a request of an older screen than the newest
//!   moves nothing ([`crate::retention::sessions::Heard::Stale`]'s rule).
//! * **A cast** ([`Holds::hold`] for a published token): from publish to
//!   unpublish, whatever the receiver is doing. Taken while the screen's
//!   hold is still there, so a hand-over from the phone to the television
//!   has no instant in which nothing holds the torrent.
//!
//! **What releases a hold the holder forgot.** Holds live in this process's
//! memory only, and the server lives inside the app's process: an app that
//! is killed takes every hold with it, and a restart starts with none. A
//! screen's hold is bounded by the one-per-viewer rule above; a cast's is
//! an RAII value ([`TorrentHold`]) owned by its publication, which the
//! listener's stop unpublishes with every other.
//!
//! One lock, never held across an await or around another lock.

use std::collections::HashMap;
use std::sync::Arc;

use crate::retention::sessions::PlayerToken;

/// Who took a hold, for the log and for [`Holds::holders`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Holder {
    /// A viewer's player screen: `<viewer>.<screen>` as `p=` carries it.
    Player(String),
    /// A published cast.
    Cast,
}

/// What one hold is on: a torrent (lowercase), and the files of it the
/// holder uses -- `None` when it cannot say, which is all of them.
#[derive(Clone, Debug, PartialEq, Eq)]
struct On {
    info_hash: String,
    files: Option<Vec<usize>>,
}

impl On {
    fn file(&self, info_hash: &str, file_idx: usize) -> bool {
        self.info_hash == info_hash
            && self
                .files
                .as_ref()
                .is_none_or(|files| files.contains(&file_idx))
    }
}

#[derive(Debug, Default)]
struct Inner {
    next: u64,
    /// Holds taken with [`Holds::hold`], by id.
    held: HashMap<u64, On>,
    /// Each viewer's screen hold: the screen number it was taken by (or
    /// none, for a token without one) and what it is on.
    players: HashMap<String, (Option<u64>, On)>,
    /// Each viewer's newest screen that said it was gone: a request still
    /// carrying its token (or an older one's) holds nothing -- a cast the
    /// screen published reads with the screen's token, and must not bring
    /// the screen's hold back after the screen was left. One per viewer.
    closed: HashMap<String, u64>,
}

/// Every explicit hold on every torrent. Cheap to clone: one shared map.
#[derive(Clone, Debug, Default)]
pub struct Holds(Arc<parking_lot::Mutex<Inner>>);

/// A hold on one torrent, released when dropped.
#[derive(Debug)]
pub struct TorrentHold {
    holds: Holds,
    id: u64,
}

impl Drop for TorrentHold {
    fn drop(&mut self) {
        self.holds.0.lock().held.remove(&self.id);
    }
}

impl Holds {
    /// **Hold `info_hash` -- `files` of it, or all when `None` -- until the
    /// answer is dropped**: what a published cast owns for as long as it is
    /// published.
    pub fn hold(&self, info_hash: &str, files: Option<Vec<usize>>) -> TorrentHold {
        let info_hash = info_hash.to_lowercase();
        let mut inner = self.0.lock();
        inner.next += 1;
        let id = inner.next;
        inner.held.insert(
            id,
            On {
                info_hash: info_hash.clone(),
                files,
            },
        );
        drop(inner);
        tracing::info!(info_hash = %info_hash, holder = "cast", "torrent_held");
        TorrentHold {
            holds: self.clone(),
            id,
        }
    }

    /// **The player `token`'s screen is on `files` of `info_hash`**: its
    /// viewer's hold is on them from now on, replacing whatever that viewer
    /// held before -- unless `token` is an older screen than the one
    /// holding, or a screen that was left, which moves nothing. Answers
    /// whether the hold is now `token`'s.
    pub fn hold_for_player(&self, token: &str, info_hash: &str, files: Vec<usize>) -> bool {
        let parsed = PlayerToken::parse(token);
        let info_hash = info_hash.to_lowercase();
        let mut inner = self.0.lock();
        if let (Some((Some(holding), _)), Some(screen)) =
            (inner.players.get(&parsed.viewer), parsed.screen)
            && screen < *holding
        {
            return false;
        }
        if let (Some(closed), Some(screen)) = (inner.closed.get(&parsed.viewer), parsed.screen)
            && screen <= *closed
        {
            return false;
        }
        let previous = inner.players.insert(
            parsed.viewer,
            (
                parsed.screen,
                On {
                    info_hash: info_hash.clone(),
                    files: Some(files),
                },
            ),
        );
        drop(inner);
        if previous.as_ref().map(|(_, on)| &on.info_hash) != Some(&info_hash) {
            tracing::info!(info_hash = %info_hash, holder = "player", "torrent_held");
        }
        true
    }

    /// **The player screen `token` is gone**: its viewer's hold is
    /// released -- if this screen (or an older one) took it; a newer
    /// screen's hold stays -- and the screen is retired: a later request
    /// with its token (the cast it published, its player reconnecting)
    /// holds nothing. Answers whether a hold was released.
    pub fn release_player(&self, token: &str) -> bool {
        let parsed = PlayerToken::parse(token);
        let mut inner = self.0.lock();
        if let Some(screen) = parsed.screen {
            let closed = inner.closed.entry(parsed.viewer.clone()).or_insert(screen);
            *closed = (*closed).max(screen);
        }
        let ours = inner
            .players
            .get(&parsed.viewer)
            .is_some_and(|(holding, _)| match (holding, parsed.screen) {
                (Some(holding), Some(screen)) => screen >= *holding,
                _ => true,
            });
        if !ours {
            return false;
        }
        let released = inner.players.remove(&parsed.viewer);
        drop(inner);
        if let Some((_, on)) = &released {
            tracing::info!(info_hash = %on.info_hash, holder = "player", "torrent_released");
        }
        released.is_some()
    }

    /// **`file_idx` of `info_hash` was deleted**: a player screen's hold on
    /// it goes -- the screen plays a file that no longer exists, and the
    /// torrent must not run on its account. A cast's stays until its
    /// unpublish.
    pub fn forget_file(&self, info_hash: &str, file_idx: usize) {
        self.0.lock().players.retain(|_, (_, on)| {
            !(on.info_hash == info_hash
                && on
                    .files
                    .as_ref()
                    .is_some_and(|files| files.contains(&file_idx)))
        });
    }

    /// Whether anything holds `info_hash` (lowercase).
    pub fn holds(&self, info_hash: &str) -> bool {
        let inner = self.0.lock();
        inner.held.values().any(|on| on.info_hash == info_hash)
            || inner
                .players
                .values()
                .any(|(_, on)| on.info_hash == info_hash)
    }

    /// Whether anything holds `file_idx` of `info_hash`: what keeps that
    /// file's retention window whatever the liveness cell names.
    pub fn holds_file(&self, info_hash: &str, file_idx: usize) -> bool {
        let inner = self.0.lock();
        inner.held.values().any(|on| on.file(info_hash, file_idx))
            || inner
                .players
                .values()
                .any(|(_, on)| on.file(info_hash, file_idx))
    }

    /// Who holds `info_hash` now: what a test reads.
    pub fn holders(&self, info_hash: &str) -> Vec<Holder> {
        let inner = self.0.lock();
        let mut holders: Vec<Holder> = inner
            .players
            .iter()
            .filter(|(_, (_, on))| on.info_hash == info_hash)
            .map(|(viewer, (screen, _))| {
                Holder::Player(match screen {
                    Some(screen) => format!("{viewer}.{screen}"),
                    None => viewer.clone(),
                })
            })
            .collect();
        holders.extend(
            inner
                .held
                .values()
                .filter(|on| on.info_hash == info_hash)
                .map(|_| Holder::Cast),
        );
        holders
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **A cast's hold lasts as long as its value**, and two holds on one
    /// torrent are two: dropping one leaves the other.
    #[test]
    fn a_hold_is_held_until_dropped() {
        let holds = Holds::default();
        assert!(!holds.holds("aa"));
        let first = holds.hold("AA", None);
        let second = holds.hold("aa", Some(vec![1]));
        assert!(holds.holds("aa"), "held, whatever the case it was named in");
        assert!(holds.holds_file("aa", 0) && holds.holds_file("aa", 1));
        drop(first);
        assert!(holds.holds("aa"), "the second hold is still there");
        assert!(holds.holds_file("aa", 1), "the file it holds");
        assert!(
            !holds.holds_file("aa", 0),
            "not another file of the torrent"
        );
        drop(second);
        assert!(!holds.holds("aa"));
    }

    /// **A screen holds until it says it is gone**, one hold per viewer: the
    /// next screen replaces it, an older screen neither takes it back nor
    /// releases it, and only that screen (or a newer) releases it.
    #[test]
    fn a_screen_holds_one_torrent_per_viewer_until_it_is_released() {
        let holds = Holds::default();
        assert!(holds.hold_for_player("v.3", "aa", vec![0]));
        assert!(holds.holds("aa"));
        assert_eq!(holds.holders("aa"), vec![Holder::Player("v.3".into())]);

        assert!(
            !holds.hold_for_player("v.2", "bb", vec![0]),
            "an older screen"
        );
        assert!(holds.holds("aa") && !holds.holds("bb"));
        assert!(!holds.release_player("v.2"), "an older screen's goodbye");
        assert!(holds.holds("aa"));

        assert!(
            holds.hold_for_player("v.4", "bb", vec![0]),
            "the next screen"
        );
        assert!(!holds.holds("aa"), "the viewer's last screen let go");
        assert!(holds.holds("bb"));

        assert!(
            holds.hold_for_player("w.1", "bb", vec![0]),
            "another viewer"
        );
        assert!(holds.release_player("v.4"));
        assert!(holds.holds("bb"), "the other viewer still holds it");
        assert!(holds.release_player("w.1"));
        assert!(!holds.holds("bb"));
        assert!(!holds.release_player("w.1"), "nothing left to release");

        assert!(
            !holds.hold_for_player("w.1", "bb", vec![0]),
            "a screen that was left"
        );
        assert!(!holds.holds("bb"));
        assert!(
            holds.hold_for_player("w.2", "bb", vec![0]),
            "the viewer's next screen"
        );

        holds.forget_file("bb", 1);
        assert!(holds.holds("bb"), "another file was deleted");
        holds.forget_file("bb", 0);
        assert!(!holds.holds("bb"), "the file the screen plays was deleted");
    }
}
