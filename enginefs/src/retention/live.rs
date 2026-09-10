//! Which entity is being played: one cell, one writer, read by everything
//! that has to know.
//!
//! Retention needs one fact that nothing else in this process could state:
//! **which stream is the live one right now**. The window a player is
//! inside is kept; everything else on the volume is slack and goes. That is
//! one entity at a time -- one file of one torrent, or one proxied URL --
//! because a television plays one thing, and because a rule that kept every
//! entity anybody had ever opened is a rule that keeps the disk.
//!
//! The cell is a [`tokio::sync::watch`] with exactly one writer:
//! `BackendEngineFS::on_stream_start` for a torrent file, and the proxy's
//! reader constructor for a proxied body. Both are "the server saw a stream
//! opened", which is the event, and neither is undone: a request that dies
//! after the server saw it opened still moved the live entity, because the
//! predecessor's bytes really are the ones nobody is playing any more.
//!
//! **Nothing here remembers a time.** The previous design measured
//! liveness with two clocks -- an idle grace on the torrent and a 90-second
//! grace on the proxy -- and both were the same mistake: a stream that has
//! stopped is not a stream that has been replaced. Pausing for an hour
//! changes nothing on the disk; opening something else changes it at once.
//! So the value is a *what*, not a *when*, and it is written only where a
//! new what begins.
//!
//! # Reading it
//!
//! A reader takes a [`Reading`] -- a copy -- and every consumer of one tick
//! is handed the same copy, so the reconciler's ladder and the retention
//! pass cannot disagree about what is playing inside one tick. Between
//! tasks the value can move, which is why every delete asks its door again
//! at the instant of the unlink ([`crate::retention::owner::Door`]) rather
//! than trusting the reading it started from.
//!
//! The cheap questions -- "is this file the live one?" -- are asked of
//! [`Live`] directly, off the watch's own lock and with nothing cloned:
//! they are asked once per reclaim run, on a blocking thread, and a
//! `String` per ask would be an allocation per unlink.

use std::path::PathBuf;

/// The one entity being played.
///
/// A torrent's liveness is per *file*, not per torrent: two files of one
/// torrent are two windows over two regions of one piece space, and a
/// subtitle fetched during playback must not make the video slack. The
/// proxy's is per entity directory, which is one origin URL.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LiveEntity {
    /// One file of one torrent, by lowercase info hash and file index.
    Torrent { info_hash: String, file_idx: usize },
    /// One proxied body, by the directory its chunks are bucketed under.
    Proxy { dir: PathBuf },
}

/// What one [`Live::open`] moved.
///
/// Handed back so a caller can act on the predecessor at once -- dropping
/// its slack without waiting for the next tick -- and `None` from an open
/// that moved nothing (the same entity again, or an aside the caller asked
/// to keep).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Switch {
    /// What was live before, and `None` when nothing was.
    pub from: Option<LiveEntity>,
    /// What is live now.
    pub to: LiveEntity,
}

/// The cell. One per server; handed to every owner that has to answer
/// "am I the one being played?".
///
/// It starts empty and a restart empties it: nothing is playing in a
/// process that has served nothing, which is the honest reading and also
/// the one that makes the first tick after a restart stop every unpinned
/// torrent.
#[derive(Debug)]
pub struct Live(tokio::sync::watch::Sender<Option<LiveEntity>>);

impl Default for Live {
    fn default() -> Self {
        Self(tokio::sync::watch::Sender::new(None))
    }
}

impl Live {
    /// Nothing is playing.
    pub fn new() -> Self {
        Self::default()
    }

    /// The server saw a stream open on `to`.
    ///
    /// **The one writer.** `keep_current` is the aside rule, and it is
    /// computed by the caller *before* this call: an open on another file
    /// of the live torrent while a reader of the live file is still open is
    /// a subtitle or a side file, not a move of the playing file. Deciding
    /// it here would mean reading a second lock under this one, and the
    /// caller has the reading anyway.
    ///
    /// Opening the entity that is already live moves nothing -- a seek is a
    /// second response on one entity, not a switch -- so no watcher is
    /// woken for it.
    pub fn open(&self, to: LiveEntity, keep_current: bool) -> Option<Switch> {
        let mut switch = None;
        self.0.send_if_modified(|current| {
            if keep_current || current.as_ref() == Some(&to) {
                return false;
            }
            switch = Some(Switch {
                from: current.take(),
                to: to.clone(),
            });
            *current = Some(to);
            true
        });
        switch
    }

    /// A copy of the value, for a caller that will ask several questions of
    /// one reading -- a reconciler tick hands the same copy to its ladder
    /// and to its retention pass, so the two cannot disagree.
    pub fn reading(&self) -> Reading {
        Reading(self.0.borrow().clone())
    }

    /// Whether `file_idx` of `info_hash` is the entity being played, off
    /// the watch's own lock with nothing cloned. Asked once per reclaim
    /// run, from a blocking thread.
    pub fn is_torrent_file(&self, info_hash: &str, file_idx: usize) -> bool {
        matches!(
            &*self.0.borrow(),
            Some(LiveEntity::Torrent { info_hash: hash, file_idx: idx })
                if hash == info_hash && *idx == file_idx
        )
    }

    /// Whether any file of `info_hash` is the entity being played.
    pub fn is_torrent(&self, info_hash: &str) -> bool {
        matches!(
            &*self.0.borrow(),
            Some(LiveEntity::Torrent { info_hash: hash, .. }) if hash == info_hash
        )
    }

    /// A receiver that is woken every time the value really changes: what a
    /// task drops the predecessor's slack from.
    pub fn changed(&self) -> tokio::sync::watch::Receiver<Option<LiveEntity>> {
        self.0.subscribe()
    }
}

/// One reading of [`Live`], as a value.
///
/// **It is a copy and it does not follow the cell.** Every consumer of one
/// tick gets the same copy so that a write landing mid-tick is seen by all
/// of them or by none; a consumer that has to be right at the instant of an
/// unlink asks [`Live`] again there.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reading(Option<LiveEntity>);

impl Reading {
    /// Nothing is playing, for a caller with no cell to read.
    pub fn nothing() -> Self {
        Self(None)
    }

    /// This reading names `entity`.
    pub fn of(entity: LiveEntity) -> Self {
        Self(Some(entity))
    }

    /// Whether the entity being played is this one.
    pub fn is(&self, entity: &LiveEntity) -> bool {
        self.0.as_ref() == Some(entity)
    }

    /// Whether some file of `info_hash` is the entity being played.
    pub fn is_torrent(&self, info_hash: &str) -> bool {
        self.file_of(info_hash).is_some()
    }

    /// The torrent file being played, by info hash and index, or `None`
    /// when a proxied body is playing or nothing is.
    pub fn torrent(&self) -> Option<(&str, usize)> {
        match &self.0 {
            Some(LiveEntity::Torrent {
                info_hash,
                file_idx,
            }) => Some((info_hash.as_str(), *file_idx)),
            _ => None,
        }
    }

    /// Which file of `info_hash` is being played, and `None` when the
    /// entity being played is another torrent, a proxied body, or nothing.
    pub fn file_of(&self, info_hash: &str) -> Option<usize> {
        match &self.0 {
            Some(LiveEntity::Torrent {
                info_hash: hash,
                file_idx,
            }) if hash == info_hash => Some(*file_idx),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn torrent(hash: &str, file_idx: usize) -> LiveEntity {
        LiveEntity::Torrent {
            info_hash: hash.to_string(),
            file_idx,
        }
    }

    /// The cell starts empty, and every open that really moves it says what
    /// it moved from.
    #[test]
    fn an_open_on_another_entity_is_a_switch_and_the_same_one_again_is_not() {
        let live = Live::new();
        assert_eq!(live.reading(), Reading::nothing());
        assert!(!live.is_torrent("aa"));

        assert_eq!(
            live.open(torrent("aa", 0), false),
            Some(Switch {
                from: None,
                to: torrent("aa", 0)
            }),
            "the first open has nothing to switch from"
        );
        assert_eq!(live.reading().file_of("aa"), Some(0));
        assert!(live.is_torrent_file("aa", 0));
        assert!(!live.is_torrent_file("aa", 1));

        assert_eq!(
            live.open(torrent("aa", 0), false),
            None,
            "a seek is a second response on one entity, not a switch"
        );
        assert_eq!(
            live.open(torrent("aa", 1), false),
            Some(Switch {
                from: Some(torrent("aa", 0)),
                to: torrent("aa", 1)
            }),
            "another file of the same torrent is another entity"
        );
        assert_eq!(
            live.open(torrent("bb", 0), false),
            Some(Switch {
                from: Some(torrent("aa", 1)),
                to: torrent("bb", 0)
            })
        );
        assert!(!live.is_torrent("aa"));
    }

    /// The aside rule, as the caller spells it: an open that says "keep
    /// what is current" moves nothing and wakes nobody.
    #[test]
    fn an_open_that_asks_to_keep_the_current_entity_moves_nothing() {
        let live = Live::new();
        live.open(torrent("aa", 0), false);
        let watcher = live.changed();
        assert!(!watcher.has_changed().expect("the sender is alive"));

        assert_eq!(live.open(torrent("aa", 3), true), None);
        assert_eq!(
            live.reading().file_of("aa"),
            Some(0),
            "the subtitle did not take the video's place"
        );
        assert!(
            !watcher.has_changed().expect("the sender is alive"),
            "and nothing was woken to drop the video's slack"
        );

        assert_eq!(
            live.open(torrent("aa", 3), false),
            Some(Switch {
                from: Some(torrent("aa", 0)),
                to: torrent("aa", 3)
            }),
            "the same open once no reader of file 0 is left does move it"
        );
        assert!(watcher.has_changed().expect("the sender is alive"));
    }
}
