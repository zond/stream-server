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
//!   has no instant in which nothing holds the torrent. A cast published
//!   with the viewer's play token is that viewer's: publishing it means
//!   the viewer is watching *it*, so it lets the viewer's idle share go.
//! * **An idle share** ([`Holds::idle_shares`]): what a viewer watched
//!   last, after they have left it -- the screen released
//!   ([`Holds::release_player`]) or the cast unpublished (the
//!   [`TorrentHold`] dropped). The play's hold is not dropped then but
//!   becomes the viewer's idle share on the same torrent, in the same step,
//!   so the torrent goes on giving back what the viewer took. It is
//!   released, explicitly, when the viewer starts watching something
//!   else: a request of a current screen of theirs that names another
//!   torrent's file ([`Holds::hold_for_player`]) or something that is not
//!   a torrent ([`Holds::moved_elsewhere`]), or a cast of theirs being
//!   published. Starting the same film again turns it back into a
//!   player's hold, under the same lock, with no instant between. One per
//!   viewer, the last thing they watched: two viewers leave two. A play
//!   with no viewer -- a cast published without a play token -- leaves
//!   none. Whether an idle share runs its torrent is not this module's
//!   question: the reconciler counts one only while idle sharing is
//!   allowed (`seedingEnabled` on and not held back by the app), read at
//!   the moment it asks, so the share outlives the setting going off and
//!   back on.
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
    /// A viewer's idle share: the last thing the viewer watched, by viewer.
    IdleShare(String),
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
    /// Holds taken with [`Holds::hold`], by id, with the viewer whose play
    /// the cast is, when it was published with one.
    held: HashMap<u64, (Option<String>, On)>,
    /// Each viewer's idle share: the last thing they watched, which they
    /// have left and not yet replaced with anything else.
    idle: HashMap<String, On>,
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
    /// The cast is unpublished. A viewer's cast becomes the viewer's idle
    /// share -- unless the viewer is watching something already: a screen
    /// of theirs holds a torrent, or another cast of theirs is published.
    fn drop(&mut self) {
        let mut inner = self.holds.0.lock();
        let Some((Some(viewer), on)) = inner.held.remove(&self.id) else {
            return;
        };
        let watching = inner.players.contains_key(&viewer)
            || inner
                .held
                .values()
                .any(|(other, _)| other.as_deref() == Some(viewer.as_str()));
        if !watching {
            let info_hash = on.info_hash.clone();
            inner.idle.insert(viewer, on);
            drop(inner);
            tracing::info!(info_hash = %info_hash, holder = "idle_share", "torrent_held");
        }
    }
}

impl Holds {
    /// **Hold `info_hash` -- `files` of it, or all when `None` -- until the
    /// answer is dropped**: what a published cast owns for as long as it is
    /// published. `player` is the play token the cast was published with,
    /// if any: the cast is then that viewer's, which lets the viewer's idle
    /// share go now (they are watching this) and leaves this torrent as
    /// their idle share when it is dropped.
    pub fn hold(
        &self,
        info_hash: &str,
        files: Option<Vec<usize>>,
        player: Option<&str>,
    ) -> TorrentHold {
        let info_hash = info_hash.to_lowercase();
        let viewer = player.map(|token| PlayerToken::parse(token).viewer);
        let mut inner = self.0.lock();
        inner.next += 1;
        let id = inner.next;
        let left = viewer.as_ref().and_then(|viewer| inner.idle.remove(viewer));
        inner.held.insert(
            id,
            (
                viewer,
                On {
                    info_hash: info_hash.clone(),
                    files,
                },
            ),
        );
        drop(inner);
        if let Some(left) = left {
            tracing::info!(info_hash = %left.info_hash, holder = "idle_share", "torrent_released");
        }
        tracing::info!(info_hash = %info_hash, holder = "cast", "torrent_held");
        TorrentHold {
            holds: self.clone(),
            id,
        }
    }

    /// **The player `token`'s screen is on `files` of `info_hash`**: its
    /// viewer's hold is on them from now on, replacing whatever that viewer
    /// held before, the viewer's idle share included -- the viewer is
    /// watching this now; when it is the same torrent the share simply
    /// becomes the screen's hold again, in one step -- unless `token` is an
    /// older screen than the one holding, or a screen that was left, which
    /// moves nothing. Answers whether the hold is now `token`'s.
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
        let left = inner.idle.remove(&parsed.viewer);
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
        if let Some(left) = left
            && left.info_hash != info_hash
        {
            tracing::info!(info_hash = %left.info_hash, holder = "idle_share", "torrent_released");
        }
        if previous.as_ref().map(|(_, on)| &on.info_hash) != Some(&info_hash) {
            tracing::info!(info_hash = %info_hash, holder = "player", "torrent_held");
        }
        true
    }

    /// **The player screen `token` is gone**: its viewer's hold is
    /// released -- if this screen (or an older one) took it; a newer
    /// screen's hold stays -- and the screen is retired: a later request
    /// with its token (the cast it published, its player reconnecting)
    /// holds nothing. The torrent it held becomes the viewer's idle share,
    /// unless a cast of the viewer's is published, which is what they are
    /// watching. Answers whether a hold was released.
    pub fn release_player(&self, token: &str) -> bool {
        self.leave_screen(token, true)
    }

    /// **The player `token`'s screen moved off every torrent** -- its
    /// request named something that is not a torrent (a `/proxy` link, a
    /// Drive file): the viewer is watching that now, so the screen's hold
    /// goes, and so does the viewer's idle share. Retires the screen as
    /// [`Self::release_player`] does. The caller has already found `token`
    /// to be a current screen of the viewer's.
    pub fn moved_elsewhere(&self, token: &str) {
        self.watching_elsewhere(token);
        self.leave_screen(token, false);
    }

    /// **The viewer of `player` (a play token) is watching something that
    /// is not a torrent** -- a cast of a link published with their token:
    /// their idle share goes. No screen is touched.
    pub fn watching_elsewhere(&self, player: &str) {
        let left = self
            .0
            .lock()
            .idle
            .remove(&PlayerToken::parse(player).viewer);
        if let Some(left) = left {
            tracing::info!(info_hash = %left.info_hash, holder = "idle_share", "torrent_released");
        }
    }

    fn leave_screen(&self, token: &str, idles: bool) -> bool {
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
        let casting = inner
            .held
            .values()
            .any(|(viewer, _)| viewer.as_deref() == Some(parsed.viewer.as_str()));
        let idle = match &released {
            Some((_, on)) if idles && !casting => {
                inner.idle.insert(parsed.viewer.clone(), on.clone());
                true
            }
            _ => false,
        };
        drop(inner);
        if let Some((_, on)) = &released {
            tracing::info!(info_hash = %on.info_hash, holder = "player", "torrent_released");
            if idle {
                tracing::info!(info_hash = %on.info_hash, holder = "idle_share", "torrent_held");
            }
        }
        released.is_some()
    }

    /// **`file_idx` of `info_hash` was deleted**: a player screen's hold on
    /// it goes -- the screen plays a file that no longer exists, and the
    /// torrent must not run on its account. A cast's stays until its
    /// unpublish.
    pub fn forget_file(&self, info_hash: &str, file_idx: usize) {
        let names = |on: &On| {
            on.info_hash == info_hash
                && on
                    .files
                    .as_ref()
                    .is_some_and(|files| files.contains(&file_idx))
        };
        let mut inner = self.0.lock();
        inner.players.retain(|_, (_, on)| !names(on));
        inner.idle.retain(|_, on| !names(on));
    }

    /// Whether something is using `info_hash` (lowercase) now: a player
    /// screen on it or a cast of it. Not an idle share -- see
    /// [`Self::idle_shares`].
    pub fn holds(&self, info_hash: &str) -> bool {
        let inner = self.0.lock();
        inner.held.values().any(|(_, on)| on.info_hash == info_hash)
            || inner
                .players
                .values()
                .any(|(_, on)| on.info_hash == info_hash)
    }

    /// Whether a viewer's idle share is on `info_hash` (lowercase): the
    /// last thing they watched, left and not replaced.
    pub fn idle_shares(&self, info_hash: &str) -> bool {
        self.0
            .lock()
            .idle
            .values()
            .any(|on| on.info_hash == info_hash)
    }

    /// Whether any hold of any kind is on `info_hash`: [`Self::holds`] or
    /// [`Self::idle_shares`]. What keeps a torrent's engine, and its files,
    /// in the session for its holder, whether or not it runs now.
    pub fn holds_any(&self, info_hash: &str) -> bool {
        self.holds(info_hash) || self.idle_shares(info_hash)
    }

    /// Whether any hold of any kind is on `file_idx` of `info_hash`: what
    /// keeps that file's retention window whatever the liveness cell names.
    pub fn holds_file(&self, info_hash: &str, file_idx: usize) -> bool {
        let inner = self.0.lock();
        inner
            .held
            .values()
            .any(|(_, on)| on.file(info_hash, file_idx))
            || inner
                .players
                .values()
                .any(|(_, on)| on.file(info_hash, file_idx))
            || inner.idle.values().any(|on| on.file(info_hash, file_idx))
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
                .filter(|(_, on)| on.info_hash == info_hash)
                .map(|_| Holder::Cast),
        );
        holders.extend(
            inner
                .idle
                .iter()
                .filter(|(_, on)| on.info_hash == info_hash)
                .map(|(viewer, _)| Holder::IdleShare(viewer.clone())),
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
        let first = holds.hold("AA", None, None);
        let second = holds.hold("aa", Some(vec![1]), None);
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

    /// **What a viewer watched last stays theirs to share until they watch
    /// something else.** A screen released, or a cast of theirs
    /// unpublished, leaves the viewer's idle share on its torrent; a
    /// request of a current screen naming another torrent, a move to
    /// something that is not a torrent, or a cast of theirs published lets
    /// it go; the same torrent again turns it back into the screen's hold.
    /// One per viewer: two viewers leave two. A cast with no play token
    /// leaves none.
    #[test]
    fn a_viewers_last_play_becomes_their_idle_share_until_they_watch_something_else() {
        let holds = Holds::default();
        assert!(holds.hold_for_player("v.1", "aa", vec![0]));
        assert!(holds.release_player("v.1"));
        assert!(!holds.holds("aa"), "nothing is using it");
        assert!(holds.idle_shares("aa") && holds.holds_any("aa"));
        assert!(
            holds.holds_file("aa", 0),
            "its window is still the viewer's"
        );
        assert_eq!(holds.holders("aa"), vec![Holder::IdleShare("v".into())]);

        // The released screen's own late request moves nothing.
        assert!(!holds.hold_for_player("v.1", "bb", vec![0]));
        assert!(holds.idle_shares("aa"));

        // A newer screen on another torrent: the viewer watches that now.
        assert!(holds.hold_for_player("v.2", "bb", vec![0]));
        assert!(!holds.idle_shares("aa") && !holds.holds_any("aa"));
        assert!(holds.release_player("v.2"));
        assert!(holds.idle_shares("bb"));

        // The same torrent again: the share is the screen's hold again.
        assert!(holds.hold_for_player("v.3", "bb", vec![0]));
        assert!(holds.holds("bb") && !holds.idle_shares("bb"));
        assert_eq!(holds.holders("bb"), vec![Holder::Player("v.3".into())]);

        // Another viewer's share is theirs: two viewers, two shares.
        assert!(holds.hold_for_player("w.1", "cc", vec![0]));
        assert!(holds.release_player("w.1"));
        assert!(holds.release_player("v.3"));
        assert!(holds.idle_shares("bb") && holds.idle_shares("cc"));

        // Something that is not a torrent, on a current screen.
        holds.moved_elsewhere("v.4");
        assert!(!holds.idle_shares("bb"));
        assert!(holds.idle_shares("cc"), "the other viewer's stays");
        holds.watching_elsewhere("w.2");
        assert!(!holds.idle_shares("cc"));

        // A deleted file takes the share on it with it.
        assert!(holds.hold_for_player("v.5", "ff", vec![0]));
        assert!(holds.release_player("v.5"));
        holds.forget_file("ff", 1);
        assert!(holds.idle_shares("ff"), "another file was deleted");
        holds.forget_file("ff", 0);
        assert!(!holds.idle_shares("ff"), "the file it shares was deleted");
    }

    /// **A cast of the viewer's is what they are watching**: published, it
    /// lets their idle share go and keeps a screen released under it from
    /// leaving one; unpublished, it leaves its torrent as their idle share,
    /// unless they are watching something else by then. A cast with no
    /// play token belongs to nobody and leaves nothing.
    #[test]
    fn a_viewers_cast_ends_their_idle_share_and_leaves_its_own() {
        let holds = Holds::default();
        assert!(holds.hold_for_player("v.1", "aa", vec![0]));
        assert!(holds.release_player("v.1"));
        assert!(holds.idle_shares("aa"));

        assert!(holds.hold_for_player("v.2", "bb", vec![0]));
        let cast = holds.hold("bb", Some(vec![0]), Some("v.2"));
        assert!(
            holds.release_player("v.2"),
            "the hand-over to the television"
        );
        assert!(
            !holds.idle_shares("bb"),
            "the cast is what the viewer watches"
        );
        assert_eq!(holds.holders("bb"), vec![Holder::Cast]);
        drop(cast);
        assert_eq!(holds.holders("bb"), vec![Holder::IdleShare("v".into())]);

        // A cast of another torrent lets the share go.
        let other = holds.hold("cc", None, Some("v.3"));
        assert!(!holds.holds_any("bb"));
        // And unpublished while a screen of the viewer's plays something
        // else, it leaves nothing: that is the last thing watched.
        assert!(holds.hold_for_player("v.4", "dd", vec![0]));
        drop(other);
        assert!(!holds.idle_shares("cc"));

        // Nobody's cast.
        drop(holds.hold("ee", None, None));
        assert!(!holds.holds_any("ee"));
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
