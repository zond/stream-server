//! **Which files the viewer's players are playing**: one play session per
//! viewer, and nothing but the viewer's own player starts or moves one.
//!
//! A play session is what shares a torrent's pieces (the draw,
//! `retention::owner`'s `State::draw`), so which file it is on must never
//! be inferred -- not from the reads open on a file, which a seek closes for
//! a moment, nor from the order in which a subtitle and a film happened to
//! open, nor from a length a player states. It is **told**: a stream
//! request carrying the player token (`p=`) is the viewer's player asking
//! for the file it plays, and it is the only thing that writes here
//! ([`PlaySessions::play`]). A request without one -- a subtitle, a side
//! file, another client on the HTTP routes, the server's own reads for a
//! translated source -- never starts a session, never moves one, and never
//! draws.
//!
//! **One session per viewer.** The app's token is `<viewer>.<screen>`
//! ([`PlayerToken`]): a viewer id per install and a number per player
//! screen. Every screen of one viewer -- the next episode opens a new one
//! -- carries on that viewer's session, and a request from an older screen
//! than the newest the viewer has used is ignored ([`Heard::Stale`]).
//!
//! **One viewer, in practice.** The server lives inside the app's process
//! and dies with it; the app pins stremio-core's streaming server to that
//! embedded server and reaches it over FFI, so no other device's player
//! ever uses it. The one outside client, a cast receiver, carries the
//! app's own token. Two viewers on one server cannot happen with xtremio,
//! and the rules below for two are what keeps a second token -- a client
//! on the HTTP routes -- from stopping the torrent under the first. A
//! session moving to another file ends what it was sharing there -- which ends only
//! with the torrent leaving the swarm -- and that is done at once **only
//! when the player that moved is the only one on the torrent**: the stop
//! interrupts exactly the viewer who moved (the next episode of the same
//! torrent, a brief restart). While another player's session is on the
//! torrent the end waits until no session is on it at all, so neither
//! player ever stops the torrent under the other
//! ([`PlaySessions::may_end_now`]).
//!
//! It is not the liveness cell ([`crate::retention::live`]): that one
//! still decides which file's window is kept and whether a torrent runs,
//! from every stream a request opens. This decides only what is shared,
//! and when a share ends.

use std::collections::{HashMap, HashSet};
use std::ops::Range;

/// What a player token's session is on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Played {
    /// One file of one torrent. `shares` is false for a file played in a
    /// way that shares nothing: a container file played by its URL, whose
    /// member the player never names, and each volume of a multi-volume
    /// set.
    Torrent {
        info_hash: String,
        file_idx: usize,
        shares: bool,
        /// **The member's byte extent within the file**, when what plays
        /// is a member of a single-file container opened through a media
        /// id: the member path knows what the file is and where the film
        /// lies in it. The draw is then sized from this extent's length and
        /// made inside it, and the file's content is not sniffed -- it is a
        /// container, and the member path said so. `None` for a file played
        /// as itself.
        member: Option<Range<u64>>,
    },
    /// Something that is not a torrent file: a proxied body.
    Elsewhere,
}

impl Played {
    fn torrent(&self) -> Option<(&str, usize)> {
        match self {
            Self::Torrent {
                info_hash,
                file_idx,
                ..
            } => Some((info_hash.as_str(), *file_idx)),
            Self::Elsewhere => None,
        }
    }
}

/// A player token as the app mints it: `<viewer>.<screen>` -- a viewer id
/// per install, and a number per player screen that only grows. The play
/// session is the viewer's, so each new screen of one viewer (the next
/// episode opens one) continues or moves the same session rather than
/// starting another; a screen's number says which of two of the viewer's
/// requests is newer. A token with no `.`, or whose screen part is not a
/// number, is a viewer with no screen number -- a client from before the
/// scheme -- and each of its requests is current.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlayerToken {
    pub viewer: String,
    pub screen: Option<u64>,
}

impl PlayerToken {
    pub fn parse(token: &str) -> Self {
        match token.rsplit_once('.') {
            Some((viewer, screen)) if !viewer.is_empty() => match screen.parse() {
                Ok(screen) => Self {
                    viewer: viewer.to_string(),
                    screen: Some(screen),
                },
                Err(_) => Self::whole(token),
            },
            _ => Self::whole(token),
        }
    }

    fn whole(token: &str) -> Self {
        Self {
            viewer: token.to_string(),
            screen: None,
        }
    }
}

#[derive(Debug)]
struct Session {
    /// The newest screen number a request of this viewer has carried.
    screen: Option<u64>,
    played: Played,
}

impl Session {
    fn on_torrent(&self, info_hash: &str) -> bool {
        self.played.torrent().is_some_and(|(h, _)| h == info_hash)
    }
}

#[derive(Default, Debug)]
struct Inner {
    by_viewer: HashMap<String, Session>,
    /// Files a session left while no other session was on their
    /// torrent: what they shared may end at once. Forgotten when a session
    /// is on the file again, when its torrent's shares have been ended, and
    /// when nothing is on its torrent to end anything under.
    left_alone: HashSet<(String, usize)>,
}

/// What [`PlaySessions::play`] made of a player's request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Heard {
    /// The viewer's newest screen: its session is on what it asked for,
    /// and `moved` says whether that is a change.
    Current { moved: bool },
    /// An older screen of the viewer than the newest it has heard from --
    /// the previous screen's player still reconnecting while the next one
    /// takes over. It moves nothing, starts nothing, and is no player's
    /// playback.
    Stale,
}

/// The play sessions, one per viewer. Behind one lock that is never held
/// across an await or around another lock. Bounded by the number of
/// viewers: one entry each, replaced, never accumulated.
#[derive(Default, Debug)]
pub struct PlaySessions(parking_lot::Mutex<Inner>);

impl PlaySessions {
    /// **The player `token` asked for `played`.** A request of the
    /// viewer's newest screen (or a newer one) puts its session on it; an
    /// older screen's is [`Heard::Stale`] and changes nothing.
    pub fn play(&self, token: &str, played: Played) -> Heard {
        let token = PlayerToken::parse(token);
        let mut inner = self.0.lock();
        if let (Some(session), Some(screen)) = (inner.by_viewer.get(&token.viewer), token.screen)
            && session.screen.is_some_and(|newest| screen < newest)
        {
            return Heard::Stale;
        }
        if let Some((hash, file)) = played.torrent() {
            inner.left_alone.remove(&(hash.to_string(), file));
        }
        let previous = inner.by_viewer.insert(
            token.viewer.clone(),
            Session {
                screen: token.screen,
                played: played.clone(),
            },
        );
        let Some(previous) = previous else {
            return Heard::Current { moved: true };
        };
        if previous.played == played {
            return Heard::Current { moved: false };
        }
        match (previous.played.torrent(), played.torrent()) {
            // The same file, only `shares` changed: nothing was left.
            (Some(left), Some(now)) if left == now => {}
            (Some((hash, file)), _) => {
                let others = inner
                    .by_viewer
                    .iter()
                    .any(|(viewer, session)| *viewer != token.viewer && session.on_torrent(hash));
                if !others {
                    inner.left_alone.insert((hash.to_string(), file));
                }
            }
            (None, _) => {}
        }
        Self::prune(&mut inner);
        Heard::Current { moved: true }
    }

    /// Nothing waits for a torrent no session is on: its shares end by the
    /// torrent being nobody's, not by a mark.
    fn prune(inner: &mut Inner) {
        let Inner {
            by_viewer,
            left_alone,
        } = inner;
        left_alone.retain(|(hash, _)| by_viewer.values().any(|session| session.on_torrent(hash)));
    }

    /// Whether a session is on `file_idx` of `info_hash` and shares it: the
    /// one condition under which that file's play
    /// session draws, and under which what it drew is still shared.
    pub fn covers(&self, info_hash: &str, file_idx: usize) -> bool {
        self.0.lock().by_viewer.values().any(|session| {
            matches!(&session.played, Played::Torrent { info_hash: h, file_idx: f, shares: true, .. }
                if h == info_hash && *f == file_idx)
        })
    }

    /// **Where in `file_idx` of `info_hash` the sessions sharing it play**:
    /// the member extent every session covering the file names
    /// ([`Played::Torrent`]'s `member`). `None` when no session covers the
    /// file, when one plays the file as itself, or when two name different
    /// members -- each of which is the file as a whole, sized and sniffed
    /// as one.
    pub fn member_of(&self, info_hash: &str, file_idx: usize) -> Option<Range<u64>> {
        let inner = self.0.lock();
        let mut members = inner
            .by_viewer
            .values()
            .filter_map(|session| match &session.played {
                Played::Torrent {
                    info_hash: h,
                    file_idx: f,
                    shares: true,
                    member,
                } if h == info_hash && *f == file_idx => Some(member.clone()),
                _ => None,
            });
        let first = members.next()??;
        members
            .all(|member| member.as_ref() == Some(&first))
            .then_some(first)
    }

    /// Whether a session is on some file of `info_hash`, sharing or not.
    pub fn on_torrent(&self, info_hash: &str) -> bool {
        self.0
            .lock()
            .by_viewer
            .values()
            .any(|session| session.on_torrent(info_hash))
    }

    /// Whether a session is on `file_idx` of `info_hash`, sharing or not.
    pub fn on_file(&self, info_hash: &str, file_idx: usize) -> bool {
        self.0
            .lock()
            .by_viewer
            .values()
            .any(|session| session.played.torrent() == Some((info_hash, file_idx)))
    }

    /// Whether what `file_idx` of `info_hash` shared may end now, with its
    /// torrent still played: the session that left it was the only one on
    /// the torrent, so stopping the torrent interrupts nobody but the
    /// viewer that moved. Otherwise it waits for no session to be on the
    /// torrent at all.
    pub fn may_end_now(&self, info_hash: &str, file_idx: usize) -> bool {
        self.0
            .lock()
            .left_alone
            .contains(&(info_hash.to_string(), file_idx))
    }

    /// `info_hash`'s shares have been ended: nothing it left is waiting.
    pub fn ended(&self, info_hash: &str) {
        self.0
            .lock()
            .left_alone
            .retain(|(hash, _)| hash != info_hash);
    }

    /// What `token`'s viewer's session is on, for the tests
    /// and the logs.
    pub fn of(&self, token: &str) -> Option<Played> {
        let viewer = PlayerToken::parse(token).viewer;
        self.0
            .lock()
            .by_viewer
            .get(&viewer)
            .map(|session| session.played.clone())
    }

    /// How many entries the sessions keep: one per viewer, and one per
    /// file waiting to end. For the tests of what is pruned.
    #[cfg(test)]
    pub(crate) fn entries(&self) -> (usize, usize) {
        let inner = self.0.lock();
        (inner.by_viewer.len(), inner.left_alone.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(info_hash: &str, file_idx: usize) -> Played {
        Played::Torrent {
            info_hash: info_hash.to_string(),
            file_idx,
            shares: true,
            member: None,
        }
    }

    const CURRENT: Heard = Heard::Current { moved: true };
    const SAME: Heard = Heard::Current { moved: false };

    /// **A token is a viewer and a screen**; one without a screen number is
    /// a viewer whose every request is current.
    #[test]
    fn a_token_is_a_viewer_and_a_screen() {
        assert_eq!(
            PlayerToken::parse("a1b2.7"),
            PlayerToken {
                viewer: "a1b2".into(),
                screen: Some(7)
            }
        );
        for whole in ["player-3", "a.b", ".7", "a1b2."] {
            assert_eq!(
                PlayerToken::parse(whole),
                PlayerToken {
                    viewer: whole.into(),
                    screen: None
                },
                "{whole}"
            );
        }
    }

    /// **The viewer's session follows its newest screen**, and an older
    /// screen's request -- the last screen's player still reconnecting --
    /// changes nothing.
    #[test]
    fn a_viewers_newest_screen_moves_its_session_and_an_older_one_is_stale() {
        let sessions = PlaySessions::default();
        assert_eq!(sessions.play("tv.1", file("t", 0)), CURRENT);
        // The next episode, on a new screen.
        assert_eq!(sessions.play("tv.2", file("t", 1)), CURRENT);
        assert!(sessions.may_end_now("t", 0));
        // The old screen reconnects.
        assert_eq!(sessions.play("tv.1", file("t", 0)), Heard::Stale);
        assert_eq!(sessions.of("tv.9"), Some(file("t", 1)));
        assert!(!sessions.covers("t", 0));
        // The same film on a new screen: no move.
        assert_eq!(sessions.play("tv.3", file("t", 1)), SAME);
    }

    /// **The legacy shape**: a client from before `<viewer>.<screen>` sends
    /// a bare token, and every one of its requests is current.
    #[test]
    fn a_token_with_no_screen_is_always_current() {
        let sessions = PlaySessions::default();
        assert_eq!(sessions.play("player-1", file("u", 0)), CURRENT);
        assert_eq!(sessions.play("player-1", file("u", 1)), CURRENT);
        assert_eq!(sessions.play("player-1", file("u", 0)), CURRENT);
    }

    /// **A lone viewer's move may end what it left at once; two viewers'
    /// never do, until no session is on the torrent.**
    #[test]
    fn a_move_ends_what_it_left_at_once_only_for_the_one_viewer_on_the_torrent() {
        let sessions = PlaySessions::default();
        sessions.play("tv.1", file("t", 0));
        sessions.play("phone.1", file("t", 5));
        assert_eq!(sessions.play("tv.2", file("t", 2)), CURRENT);
        assert!(
            !sessions.may_end_now("t", 0),
            "a stop would interrupt the other viewer"
        );
        sessions.play("phone.2", file("t", 6));
        assert!(!sessions.may_end_now("t", 5));
        assert!(sessions.on_torrent("t"));

        // Both leave: nobody is on it, and nothing waits for it.
        sessions.play("tv.3", Played::Elsewhere);
        sessions.play("phone.3", file("u", 0));
        assert!(!sessions.on_torrent("t"));
        assert_eq!(
            sessions.entries(),
            (2, 0),
            "a mark for a torrent nobody is on"
        );
    }

    /// **Only `shares` changing is no move**: nothing is left behind.
    #[test]
    fn a_change_of_shares_alone_leaves_nothing() {
        let sessions = PlaySessions::default();
        sessions.play("tv.1", file("t", 0));
        sessions.play(
            "tv.1",
            Played::Torrent {
                info_hash: "t".into(),
                file_idx: 0,
                shares: false,
                member: None,
            },
        );
        assert!(
            !sessions.may_end_now("t", 0),
            "the file being played was marked left"
        );
    }

    /// **A member's extent is the file's only while every session sharing
    /// the file names that one member**: a session playing the file as
    /// itself, or another member, makes it the whole file again, and a
    /// session that shares nothing names nothing.
    #[test]
    fn a_member_extent_is_the_one_every_sharing_session_names() {
        let member = |bytes: Range<u64>| Played::Torrent {
            info_hash: "t".into(),
            file_idx: 0,
            shares: true,
            member: Some(bytes),
        };
        let sessions = PlaySessions::default();
        assert_eq!(sessions.member_of("t", 0), None);
        sessions.play("tv.1", member(100..900));
        assert_eq!(sessions.member_of("t", 0), Some(100..900));
        assert_eq!(sessions.member_of("t", 1), None);
        sessions.play(
            "phone.1",
            Played::Torrent {
                info_hash: "t".into(),
                file_idx: 0,
                shares: false,
                member: None,
            },
        );
        assert_eq!(sessions.member_of("t", 0), Some(100..900));
        sessions.play("phone.2", member(100..900));
        assert_eq!(sessions.member_of("t", 0), Some(100..900));
        sessions.play("phone.3", member(0..50));
        assert_eq!(sessions.member_of("t", 0), None, "two members");
        sessions.play("phone.4", file("t", 0));
        assert_eq!(sessions.member_of("t", 0), None, "the file as itself");
    }

    /// An archive's session is on the torrent but shares nothing.
    #[test]
    fn an_archive_session_is_on_the_torrent_and_covers_nothing() {
        let sessions = PlaySessions::default();
        sessions.play(
            "tv.1",
            Played::Torrent {
                info_hash: "t".into(),
                file_idx: 0,
                shares: false,
                member: None,
            },
        );
        assert!(!sessions.covers("t", 0));
        assert!(sessions.on_torrent("t"));
    }
}
