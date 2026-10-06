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
//! follows every stream a request opens and decides only which file's
//! window is kept. Whether a torrent runs is its holds'
//! ([`crate::retention::holds`]). This decides only what is shared, and
//! when a share ends.

use std::collections::{HashMap, HashSet};
use std::ops::Range;

/// What a player token's session is on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Played {
    /// One file of one torrent. `shares` is false for a file played in a
    /// way that shares nothing: a container file played by its URL, whose
    /// member the player never names.
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
    /// **A member of a multi-volume set** (`film.part1.rar`,
    /// `film.part2.rar`, ...) played through a media id: every volume it
    /// lies in, in the member's order, each with the member's bytes in it.
    /// It shares, like a film: the set is **one thing played**, so a
    /// request for any of its volumes names this same value and moves
    /// nothing -- the reader crossing from one volume into the next is no
    /// player moving -- and leaving it leaves every volume at once. The
    /// draw is made once over the member's bytes in all of them.
    ///
    /// Per-volume extents and not one range over the volumes' concatenation:
    /// the member's bytes in each volume start after that volume's own
    /// headers, so a range over the concatenation would need each volume's
    /// boundaries and header lengths beside it to say which bytes of which
    /// file are the film -- the per-volume ranges again, in a costlier form.
    /// And per volume is what every reader of it asks: which files the
    /// session is on, and which pieces of each the member lies in.
    Set {
        info_hash: String,
        volumes: Vec<Volume>,
    },
    /// Something that is not a torrent file: a proxied body.
    Elsewhere,
}

/// One volume of a set a member is played across ([`Played::Set`]): the
/// file, and the member's bytes in it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Volume {
    pub file_idx: usize,
    pub member: Range<u64>,
}

impl Played {
    /// The torrent this is on, if it is on one.
    fn info_hash(&self) -> Option<&str> {
        match self {
            Self::Torrent { info_hash, .. } | Self::Set { info_hash, .. } => {
                Some(info_hash.as_str())
            }
            Self::Elsewhere => None,
        }
    }

    /// The files of its torrent this is on: one, or a set's volumes.
    fn files(&self) -> Vec<usize> {
        match self {
            Self::Torrent { file_idx, .. } => vec![*file_idx],
            Self::Set { volumes, .. } => volumes.iter().map(|volume| volume.file_idx).collect(),
            Self::Elsewhere => Vec::new(),
        }
    }

    /// Whether this is on `file_idx` of `info_hash`, sharing or not.
    fn on_file(&self, info_hash: &str, file_idx: usize) -> bool {
        self.info_hash() == Some(info_hash) && self.files().contains(&file_idx)
    }

    /// Whether this is on `file_idx` of `info_hash` and shares it.
    fn shares_file(&self, info_hash: &str, file_idx: usize) -> bool {
        match self {
            Self::Torrent {
                info_hash: h,
                file_idx: f,
                shares,
                ..
            } => *shares && h == info_hash && *f == file_idx,
            Self::Set { .. } => self.on_file(info_hash, file_idx),
            Self::Elsewhere => false,
        }
    }

    /// **What a draw over `file_idx` is made for under this**: the member
    /// of it played ([`Played::Torrent`]'s `member`, `None` for the file as
    /// itself), or the set it is a volume of. Two plays of one file that
    /// answer differently play different films in it, and a draw made for
    /// one is nothing the other shares.
    fn made_for(&self, file_idx: usize) -> (Option<Range<u64>>, Option<&[Volume]>) {
        match self {
            Self::Torrent {
                file_idx: f,
                member,
                ..
            } if *f == file_idx => (member.clone(), None),
            Self::Set { volumes, .. } => (None, Some(volumes.as_slice())),
            Self::Torrent { .. } | Self::Elsewhere => (None, None),
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
        self.played.info_hash() == Some(info_hash)
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
        if let Some(hash) = played.info_hash() {
            for file in played.files() {
                inner.left_alone.remove(&(hash.to_string(), file));
            }
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
        // What it left: every file it was on that it is not on now, and
        // every file it shared and still shares for another film -- another
        // member of the same container, the file as itself after a member
        // of it, a set after a member of one volume: a draw is made for a
        // member, and a new member on the same file is a move. The same
        // file with only `shares` changed leaves nothing, and a set left
        // leaves every volume of it.
        if let Some(hash) = previous.played.info_hash() {
            let left: Vec<usize> = previous
                .played
                .files()
                .into_iter()
                .filter(|file| {
                    !played.on_file(hash, *file)
                        || (previous.played.shares_file(hash, *file)
                            && played.shares_file(hash, *file)
                            && previous.played.made_for(*file) != played.made_for(*file))
                })
                .collect();
            let others = inner
                .by_viewer
                .iter()
                .any(|(viewer, session)| *viewer != token.viewer && session.on_torrent(hash));
            if !others {
                for file in left {
                    inner.left_alone.insert((hash.to_string(), file));
                }
            }
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
    /// session draws, and under which what it drew is still shared. Every
    /// volume of a set a session plays a member across is covered.
    pub fn covers(&self, info_hash: &str, file_idx: usize) -> bool {
        self.0
            .lock()
            .by_viewer
            .values()
            .any(|session| session.played.shares_file(info_hash, file_idx))
    }

    /// **Where in `file_idx` of `info_hash` the sessions sharing it play**:
    /// the member extent every session covering the file names
    /// ([`Played::Torrent`]'s `member`). `None` when no session covers the
    /// file, when one plays the file as itself, or when two name different
    /// members -- each of which is the file as a whole, sized and sniffed
    /// as one. A session playing a set the file is a volume of names no
    /// member of the file alone ([`Self::set_of`] is its question).
    pub fn member_of(&self, info_hash: &str, file_idx: usize) -> Option<Range<u64>> {
        let inner = self.0.lock();
        let mut members = inner
            .by_viewer
            .values()
            .filter(|session| session.played.shares_file(info_hash, file_idx))
            .map(|session| match &session.played {
                Played::Torrent { member, .. } => member.clone(),
                Played::Set { .. } | Played::Elsewhere => None,
            });
        let first = members.next()??;
        members
            .all(|member| member.as_ref() == Some(&first))
            .then_some(first)
    }

    /// **The set the sessions sharing `file_idx` of `info_hash` play a
    /// member across** ([`Played::Set`]): its volumes, in the member's
    /// order, each with the member's bytes in it. `None` when no session
    /// covers the file, and when a session covering it plays anything else
    /// -- the file as itself, a member of it alone, another set -- under
    /// which the file is sized and sniffed as itself, as two members are
    /// ([`Self::member_of`]).
    pub fn set_of(&self, info_hash: &str, file_idx: usize) -> Option<Vec<Volume>> {
        let inner = self.0.lock();
        let mut sets = inner
            .by_viewer
            .values()
            .filter(|session| session.played.shares_file(info_hash, file_idx))
            .map(|session| match &session.played {
                Played::Set { volumes, .. } => Some(volumes),
                Played::Torrent { .. } | Played::Elsewhere => None,
            });
        let first = sets.next()??;
        sets.all(|set| set == Some(first)).then(|| first.clone())
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
            .any(|session| session.played.on_file(info_hash, file_idx))
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

    /// **A new member on the same file is a move**: a draw is made for a
    /// member, so a session moving to another member of the file it is on
    /// -- or from the file as itself to a member of it, or from a member to
    /// a set the file is a volume of -- leaves the file as a move to another
    /// file would, while the same member again moves nothing and `shares`
    /// flipping alone leaves nothing.
    #[test]
    fn a_new_member_on_the_same_file_leaves_the_file() {
        let member = |bytes: Range<u64>| Played::Torrent {
            info_hash: "t".into(),
            file_idx: 0,
            shares: true,
            member: Some(bytes),
        };
        let sessions = PlaySessions::default();
        sessions.play("tv.1", member(0..500));
        assert_eq!(sessions.play("tv.1", member(0..500)), SAME);
        assert!(!sessions.may_end_now("t", 0));
        assert_eq!(sessions.play("tv.2", member(500..1000)), CURRENT);
        assert!(sessions.may_end_now("t", 0), "another member of the file");

        let sessions = PlaySessions::default();
        sessions.play("tv.1", file("t", 0));
        sessions.play("tv.2", member(0..500));
        assert!(
            sessions.may_end_now("t", 0),
            "the file, then a member of it"
        );

        let sessions = PlaySessions::default();
        sessions.play("tv.1", member(0..500));
        sessions.play("tv.2", set("t", &[(0, 0..500), (1, 0..500)]));
        assert!(sessions.may_end_now("t", 0), "a member, then a set");

        let sessions = PlaySessions::default();
        sessions.play("tv.1", file("t", 0));
        sessions.play(
            "tv.2",
            Played::Torrent {
                info_hash: "t".into(),
                file_idx: 0,
                shares: false,
                member: None,
            },
        );
        assert!(!sessions.may_end_now("t", 0), "only `shares` changed");
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

    fn set(info_hash: &str, volumes: &[(usize, Range<u64>)]) -> Played {
        Played::Set {
            info_hash: info_hash.into(),
            volumes: volumes
                .iter()
                .map(|(file_idx, member)| Volume {
                    file_idx: *file_idx,
                    member: member.clone(),
                })
                .collect(),
        }
    }

    /// **A set is one thing played**: it covers every volume, the same set
    /// asked for again -- the reader in its next volume -- moves nothing and
    /// leaves nothing, and leaving it leaves every volume at once, as
    /// leaving a film leaves the film.
    #[test]
    fn a_set_is_one_thing_played_and_leaving_it_leaves_every_volume() {
        let sessions = PlaySessions::default();
        let film = set("t", &[(0, 100..1000), (1, 0..600)]);
        assert_eq!(sessions.play("tv.1", film.clone()), CURRENT);
        assert!(sessions.covers("t", 0) && sessions.covers("t", 1));
        assert!(!sessions.covers("t", 2));
        assert!(sessions.on_file("t", 1) && !sessions.on_file("t", 2));
        // The reader crosses into the second volume.
        assert_eq!(sessions.play("tv.1", film.clone()), SAME);
        assert!(!sessions.may_end_now("t", 0) && !sessions.may_end_now("t", 1));
        assert_eq!(sessions.entries(), (1, 0));

        // The next episode, a file of the same torrent.
        assert_eq!(sessions.play("tv.2", file("t", 2)), CURRENT);
        assert!(
            sessions.may_end_now("t", 0),
            "the first volume was not left"
        );
        assert!(
            sessions.may_end_now("t", 1),
            "the second volume was not left"
        );
        assert!(!sessions.covers("t", 0) && !sessions.covers("t", 1));
    }

    /// **The set every session sharing a file names, or none**: a set's
    /// volume names no member of its own ([`PlaySessions::member_of`]), and
    /// a second session playing anything else of the file -- the file as
    /// itself, or another set -- makes it the file as a whole.
    #[test]
    fn a_set_is_the_one_every_sharing_session_names() {
        let sessions = PlaySessions::default();
        let film = set("t", &[(0, 100..1000), (1, 0..600)]);
        assert_eq!(sessions.set_of("t", 0), None);
        sessions.play("tv.1", film.clone());
        let Played::Set { volumes, .. } = &film else {
            unreachable!()
        };
        assert_eq!(sessions.set_of("t", 0).as_ref(), Some(volumes));
        assert_eq!(sessions.set_of("t", 1).as_ref(), Some(volumes));
        assert_eq!(sessions.set_of("t", 2), None);
        assert_eq!(sessions.member_of("t", 0), None);
        sessions.play("phone.1", film.clone());
        assert_eq!(sessions.set_of("t", 1).as_ref(), Some(volumes));
        sessions.play("phone.2", file("t", 1));
        assert_eq!(sessions.set_of("t", 1), None, "the file as itself");
        assert_eq!(sessions.set_of("t", 0).as_ref(), Some(volumes));
        sessions.play("phone.3", set("t", &[(0, 0..50), (1, 0..10)]));
        assert_eq!(sessions.set_of("t", 0), None, "two sets");

        // A set beside a member of one of its volumes: neither is the file's.
        let sessions = PlaySessions::default();
        sessions.play("tv.1", film.clone());
        sessions.play(
            "phone.1",
            Played::Torrent {
                info_hash: "t".into(),
                file_idx: 0,
                shares: true,
                member: Some(100..1000),
            },
        );
        assert_eq!(sessions.member_of("t", 0), None, "a set and a member");
        assert_eq!(sessions.set_of("t", 0), None, "a set and a member");
    }
}
