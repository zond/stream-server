//! What we keep, and what we share: the arithmetic of it, with no filesystem
//! and no librqbit types in it.
//!
//! The store can hold a piece or give it back, and the want-set can want a
//! piece or stop wanting it. Neither of them decides *which* pieces. That
//! decision is a handful of numbers -- a budget, a piece length, the pieces of
//! one file, where the playhead is -- and it lives here on its own, beside
//! [`super::layout`] and for the same reason: it is the part that is easy to
//! get quietly wrong and easy to test exhaustively, and it should not have to
//! be reasoned about through a session, a disk and a swarm.
//!
//! # The policy
//!
//! **The budget** is bytes, per volume, and comes from the cache cleaner's
//! `CacheLimit::effective` -- `min(configured, occupied + available - floor)`.
//! It is an input here and is not recomputed: a second reading of "how much
//! room is there" that disagreed with the cleaner's would have the two layers
//! evicting against different numbers.
//!
//! **If the budget covers the whole file we keep the whole file** and share
//! all of it. No split and no window. This is the phone with 379 GB free and
//! it is every desktop.
//!
//! **If it does not, the budget is halved:**
//!
//! * Half is a rolling window around the playhead, roughly 90% ahead and 10%
//!   behind, so a short scan back is served from disk instead of from the
//!   swarm.
//! * Half is committed for sharing. It is filled *opportunistically*, from
//!   pieces we already hold -- nothing here ever asks for a byte outside the
//!   playhead -- and once a piece is in it, it stays.
//!
//! **Only what is committed is advertised** -- once an engine can be told
//! that. That is the whole of being a good citizen here: a piece we might
//! reclaim is never announced, so we never advertise-then-refuse, which is
//! what gets a client choked. A window piece would be held and readable and
//! *not* announced, because the window moves and it will go. On a small volume
//! that means we honestly seed little; on a roomy one we seed everything.
//!
//! **That last part is a decision, not yet a behaviour, and nothing in this
//! module can make it one.** At the rev this crate pins, `have` implies
//! announced on both paths out of librqbit -- the `have` broadcast
//! (`should_transmit_have` reaching `TorrentStateLive::should_advertise_have`)
//! and the handshake bitfield, which serialises `get_have_pieces()` whole --
//! and a window piece has to be `have` for the stream to read it.
//! `drop_pieces` gives *not* have, not wanted, not advertised; there is no
//! have-readable-unannounced third state to put a window piece in. So
//! [`RetentionPolicy::advertised`] is what a peer *should* be shown and not
//! what one is shown: wiring this policy to a torrent today would announce
//! every window piece and withdraw it again a few seconds later, which is
//! precisely the advertise-then-refuse the rule exists to avoid. The third
//! state is a fork change, it is one of the two things [`super`] is waiting
//! on, and until it lands nothing here is wired up -- so the policy decides
//! the honest thing rather than a weaker thing a wiring commit would have to
//! undo.
//!
//! # Two choices worth stating, because they look arbitrary
//!
//! **A piece is committed at the moment it leaves the window**, not while it
//! is still inside one, and not merely by being outside the one the window is
//! in now. That is the only moment where committing is provably free: the
//! piece is already on disk, and the alternative for it is deletion.
//! Committing a window piece early would be free too, but it would fill the
//! shared half with the read-ahead of the first minute of the file and settle
//! the whole session's sharing before playback had passed anything. So would
//! committing a piece *ahead* of the window -- read-ahead the playhead has not
//! reached, or a leftover cache from a previous session -- which is the same
//! mistake wearing the opposite sign: on the first pass of a stream that
//! resumes onto a warm cache it settles the permanent, never-reclaimed,
//! always-advertised set out of pieces playback has passed over none of.
//!
//! "Leaves the window" is therefore a transition and not a position, and
//! deciding it needs one pass of memory: the window the previous
//! [`RetentionPolicy::advance`] chose. A held piece is a commit candidate
//! exactly when that window covered it and this one does not. A held piece the
//! window has never covered is a reclaim candidate and nothing else, whichever
//! side of the playhead it is on. Waiting until the window releases a piece is
//! also what makes "a piece we might reclaim is never announced" decidable in
//! one place: the decision to advertise and the decision never to reclaim are
//! the same decision, taken once.
//!
//! **The first pieces offered win, and the set then never changes.** The
//! design says to choose by whatever is cheapest and explicitly not by rarity:
//! measured across four live swarms, rarity-based retention beat random by
//! ≤0.15%, and by 0.00% on three of them, because seeder fractions of 94-100%
//! cap the whole effect. So there is no availability map here and nothing to
//! rank -- the cheapest possible rule is "no comparison at all", and it has the
//! property the design asks for by construction: once the committed half is
//! full it does not churn, whatever the playhead does afterwards.
//!
//! # What this does not decide
//!
//! Nothing durable. The committed set is per-session state held in this
//! struct: sharing runs between sessions and stops when the next stream
//! starts, and there is no set to re-adopt at launch. A piece it never
//! reclaimed still has to be claimed by the startup sweep like everything else
//! (see [`super::sweep`]), because "clean on close" does not run when Android
//! kills the app.

use std::collections::BTreeSet;
use std::ops::Range;

/// How much of the rolling window sits behind the playhead, in percent. The
/// rest is ahead.
///
/// Behind is for a scan back -- a few seconds of rewind, a player re-reading
/// its container index -- not for a real seek, which reaches the swarm again
/// whatever we do. Ahead is what keeps playback fed, so it gets the rest.
const BEHIND_PERCENT: u64 = 10;

/// How a budget relates to the file it has to hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// The budget covers the file. Keep all of it, share all of it, drop
    /// nothing.
    Whole,
    /// It does not, so it is halved.
    Split {
        /// Pieces the rolling window may cover.
        window: u32,
        /// Pieces the committed set may hold.
        committed: u32,
    },
}

impl Shape {
    /// The most pieces this shape intends to hold at once, or `None` when it
    /// holds the whole file.
    ///
    /// The window and the committed set can overlap only in the sense that a
    /// committed piece may be read back into the window's range; they are
    /// disjoint as *sets*, because a piece is committed exactly when the window
    /// lets go of it. So the two halves sum, and the sum is the budget.
    pub fn piece_budget(self) -> Option<u32> {
        match self {
            Self::Whole => None,
            Self::Split { window, committed } => Some(window.saturating_add(committed)),
        }
    }
}

/// What one pass decided, at one playhead position.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Decision {
    /// The pieces the rolling window covers now: wanted and kept, and *not*
    /// advertised unless they are also committed.
    pub window: Range<u32>,
    /// Held pieces in neither the window nor the committed set. Drop them from
    /// the want-set and then delete them, in that order -- not have, not
    /// wanted, not advertised, then gone.
    pub reclaim: Vec<u32>,
    /// Pieces that joined the committed set on this pass. They may be
    /// advertised from now on, and nothing will ever reclaim them.
    pub committed: Vec<u32>,
    /// Pieces that left the committed set because we no longer hold them.
    ///
    /// Nothing here takes a committed piece away, so this is only ever the
    /// disk losing one behind our back -- but "only advertise what is
    /// committed" has to hold in both directions, and a committed piece that
    /// is gone is exactly the advertise-then-refuse this policy exists to
    /// avoid.
    pub withdrawn: Vec<u32>,
}

/// The policy for one file being streamed, carrying the committed set across
/// playhead positions.
///
/// It governs a range of *torrent* piece indices -- the pieces of the file
/// being played -- and ignores everything outside it, so the pieces of a
/// torrent's other files are neither committed nor reclaimed here. A piece on
/// the boundary between two files belongs to both, and reclaiming it is
/// [`super::store::PieceStore`]'s business rather than this module's: it
/// refuses to delete a piece another file still owns.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pieces: Range<u32>,
    shape: Shape,
    committed: BTreeSet<u32>,
    /// The window the previous [`Self::advance`] chose, and the whole of what
    /// this policy remembers about where the playhead has been.
    ///
    /// A piece is committable when the window *releases* it, which is a
    /// transition between two passes and cannot be read off one of them: a
    /// piece outside the current window is as likely to be read-ahead or a
    /// leftover cache the playhead has never reached as it is to be something
    /// playback has passed over. Empty until the first pass, so a stream that
    /// starts on a warm cache commits none of it -- those pieces are reclaim
    /// candidates like any other piece no window has covered.
    ///
    /// One pass is all the memory the rule needs, and the cost of it being
    /// only one is stated rather than hidden: a piece we did not hold at the
    /// moment the window let go of it is never committed later. That is the
    /// honest reading of "committed from pieces we already hold" -- we did not
    /// have it while it was covered -- and the piece is reclaimed like the
    /// rest of what the window has left behind.
    covered: Range<u32>,
}

impl RetentionPolicy {
    /// The policy for a file occupying `pieces` of the torrent and `bytes` of
    /// the disk, under a budget of `budget_bytes`.
    ///
    /// All four numbers have to describe the same file, and three of them
    /// overdetermine the fourth, so the disagreement is checked for rather
    /// than assumed away: `bytes` must be what `pieces` holds at this piece
    /// length, which is a full piece each except that the last may be short.
    /// A mismatch means the caller resolved the file to the wrong range, and
    /// every window and every reclaim after that would be about somebody
    /// else's bytes.
    pub fn new(
        budget_bytes: u64,
        piece_length: u64,
        pieces: Range<u32>,
        bytes: u64,
    ) -> anyhow::Result<Self> {
        if piece_length == 0 {
            anyhow::bail!("a piece length of zero");
        }
        if pieces.is_empty() {
            anyhow::bail!("a file with no pieces cannot be streamed");
        }
        let count = u64::from(pieces.end - pieces.start);
        let most = count.checked_mul(piece_length).ok_or_else(|| {
            anyhow::anyhow!("{count} pieces of {piece_length} bytes overflow a u64")
        })?;
        if bytes > most || bytes <= most - piece_length {
            anyhow::bail!(
                "{count} pieces of {piece_length} bytes cannot hold {bytes} bytes: \
                 that is between {} and {most}",
                most - piece_length + 1
            );
        }
        let covered = pieces.start..pieces.start;
        Ok(Self {
            pieces,
            shape: Self::shape_for(budget_bytes, piece_length, bytes),
            committed: BTreeSet::new(),
            covered,
        })
    }

    /// Halve the budget, or don't.
    ///
    /// The comparison that decides is in bytes and not in pieces, because the
    /// last piece of a file is usually short and "the budget covers the file"
    /// has to mean the file and not a rounded-up multiple of it. Everything
    /// after it is in pieces, and the conversion floors: a budget that is two
    /// and a half pieces buys two.
    fn shape_for(budget_bytes: u64, piece_length: u64, bytes: u64) -> Shape {
        if budget_bytes >= bytes {
            return Shape::Whole;
        }
        // `budget_bytes < bytes`, so this cannot exceed the file's own piece
        // count and cannot need more than the u32 piece indices already are.
        let budget = (budget_bytes / piece_length) as u32;
        // The odd piece goes to the window: it is what keeps playback fed,
        // and the committed half is generosity.
        let window = budget.div_ceil(2);
        Shape::Split {
            window,
            committed: budget - window,
        }
    }

    /// The pieces of the file this policy governs.
    pub fn pieces(&self) -> Range<u32> {
        self.pieces.clone()
    }

    pub fn shape(&self) -> Shape {
        self.shape
    }

    /// The committed set: what may be advertised, and nothing else may.
    ///
    /// *May be*, not *is*. Nothing reads this yet, and the engine has no way
    /// to be told it -- `have` implies announced at the rev we pin, so a
    /// window piece is announced by being readable. See the module docs.
    pub fn advertised(&self) -> &BTreeSet<u32> {
        &self.committed
    }

    pub fn is_advertised(&self, piece: u32) -> bool {
        self.committed.contains(&piece)
    }

    /// Where the rolling window sits for a playhead on `piece`.
    ///
    /// The window keeps its size and slides to stay inside the file, so a
    /// playhead at the start gets all of it ahead and one at the end gets all
    /// of it behind. Shrinking it at the ends instead would give the last
    /// minutes of a film a fraction of the read-ahead the middle got, for no
    /// reason -- the budget is the same there.
    ///
    /// It is never empty, even under a budget of nothing: the piece being read
    /// is not optional, and a policy that asked for the bytes under the
    /// player's head to be deleted would be asking for a stall. So a zero
    /// budget holds one piece, which is the one place this can exceed what it
    /// was given, and it is a piece rather than a stall.
    pub fn window_at(&self, playhead: u32) -> Range<u32> {
        let total = self.pieces.end - self.pieces.start;
        let want = match self.shape {
            Shape::Whole => total,
            Shape::Split { window, .. } => window.clamp(1, total),
        };
        let playhead = playhead.clamp(self.pieces.start, self.pieces.end - 1);
        let behind = (u64::from(want) * BEHIND_PERCENT / 100) as u32;
        let mut start = playhead.saturating_sub(behind).max(self.pieces.start);
        if start.saturating_add(want) > self.pieces.end {
            // `want <= total`, so sliding back to fit cannot cross the start.
            start = self.pieces.end - want;
        }
        start..start + want
    }

    /// Decide, for a playhead on `playhead` over the pieces we currently
    /// `held`, what the window covers, what joins the committed set, and what
    /// to give back.
    ///
    /// `held` is what is on disk -- the have-set, which under this design is
    /// the piece files themselves. Pieces outside this file's range are not
    /// this policy's to touch and are skipped, not reclaimed.
    ///
    /// Three things can happen to a held piece of this file, not two. A piece
    /// already in the committed set is **left alone** -- not reclaimed, and
    /// not committed a second time -- because nothing here ever takes a
    /// committed piece back (see [`Decision::committed`]); a caller that reads
    /// this as two outcomes and reclaims whatever is outside the window
    /// deletes the pieces it is advertising, which is the advertise-then-
    /// refuse the whole policy exists to avoid. Any *other* held piece outside
    /// the window is **committed** if the previous pass's window covered it --
    /// that is what makes it a piece the window released rather than one it
    /// has never reached (see [`Self::covered`]) -- and **reclaimed** if not.
    /// So the first pass of a stream commits nothing: no window has covered
    /// anything yet.
    ///
    /// Idempotent for a fixed playhead and a fixed `held`: the second call
    /// commits nothing new and reclaims the same pieces, because a reclaim is
    /// a request rather than a record. The caller is what makes it true, by
    /// deleting them and no longer holding them. The second call has a second
    /// reason to commit nothing new -- the window it is compared against is
    /// the one it is.
    pub fn advance(&mut self, playhead: u32, held: &BTreeSet<u32>) -> Decision {
        let window = self.window_at(playhead);
        let mut decision = Decision {
            window: window.clone(),
            ..Default::default()
        };

        // A committed piece we have stopped holding cannot stay advertised.
        let withdrawn = &mut decision.withdrawn;
        self.committed.retain(|piece| {
            if held.contains(piece) {
                true
            } else {
                withdrawn.push(*piece);
                false
            }
        });

        let capacity = match self.shape {
            // Whole means every piece of the file is shared, so there is
            // nothing to ration and nothing to leave out.
            Shape::Whole => usize::MAX,
            Shape::Split { committed, .. } => committed as usize,
        };
        // Ascending, so "the first pieces offered win" is a stable rule and
        // not a function of iteration order.
        for &piece in held {
            if !self.pieces.contains(&piece) || self.committed.contains(&piece) {
                continue;
            }
            // Outside the window is what makes a piece reclaimable. Having
            // been inside the *previous* one is what makes it committable:
            // together they are the window letting go of it.
            let outside = !window.contains(&piece);
            let released = outside && self.covered.contains(&piece);
            // Under `Whole` there is no window to be released from: the file
            // fits, so every piece of it is shared as soon as it arrives.
            if (released || self.shape == Shape::Whole) && self.committed.len() < capacity {
                self.committed.insert(piece);
                decision.committed.push(piece);
            } else if outside {
                decision.reclaim.push(piece);
            }
        }
        self.covered = window;
        decision
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PIECE: u64 = 1000;

    fn held(pieces: impl IntoIterator<Item = u32>) -> BTreeSet<u32> {
        pieces.into_iter().collect()
    }

    /// A file of `count` full pieces starting at piece 0.
    fn policy(budget_pieces: u64, count: u32) -> RetentionPolicy {
        RetentionPolicy::new(
            budget_pieces * PIECE,
            PIECE,
            0..count,
            u64::from(count) * PIECE,
        )
        .expect("a consistent file")
    }

    #[test]
    fn a_budget_that_covers_the_file_keeps_and_shares_all_of_it() {
        // The phone with 379 GB free, and every desktop.
        let mut p = policy(100, 20);
        assert_eq!(p.shape(), Shape::Whole);
        assert_eq!(p.shape().piece_budget(), None);
        assert_eq!(p.window_at(7), 0..20, "no split means no window either");

        let d = p.advance(7, &held(0..12));
        assert!(d.reclaim.is_empty(), "nothing is dropped when it all fits");
        assert_eq!(d.committed, (0..12).collect::<Vec<_>>());
        assert_eq!(p.advertised().len(), 12, "we seed everything we hold");

        // Including pieces that arrive later, and only once each.
        let d = p.advance(15, &held(0..20));
        assert_eq!(d.committed, (12..20).collect::<Vec<_>>());
        assert!(d.reclaim.is_empty() && d.withdrawn.is_empty());
        assert_eq!(p.advertised().len(), 20);
    }

    /// The boundary is in bytes, because the last piece of a file is usually
    /// short: a budget one byte under the file's real size is still a split.
    #[test]
    fn the_whole_file_test_is_the_files_own_length_not_a_rounded_one() {
        let bytes = 19 * PIECE + 1;
        let exact = RetentionPolicy::new(bytes, PIECE, 0..20, bytes).unwrap();
        assert_eq!(exact.shape(), Shape::Whole);
        let short = RetentionPolicy::new(bytes - 1, PIECE, 0..20, bytes).unwrap();
        assert_ne!(short.shape(), Shape::Whole, "one byte short is not covered");
        // 19 pieces and a byte of budget buys 19 pieces: 10 window, 9 shared.
        assert_eq!(
            short.shape(),
            Shape::Split {
                window: 10,
                committed: 9
            }
        );
    }

    /// A budget of nothing still has to let the player read. It buys exactly
    /// the piece under the playhead and shares none of it.
    #[test]
    fn a_budget_of_nothing_keeps_the_piece_being_read_and_shares_none() {
        let mut p = policy(0, 20);
        assert_eq!(
            p.shape(),
            Shape::Split {
                window: 0,
                committed: 0
            }
        );
        assert_eq!(p.shape().piece_budget(), Some(0));
        assert_eq!(
            p.window_at(7),
            7..8,
            "the bytes being read are not optional"
        );

        let d = p.advance(7, &held(0..20));
        assert!(d.committed.is_empty(), "nothing may be advertised");
        assert!(p.advertised().is_empty());
        assert_eq!(
            d.reclaim,
            (0..20).filter(|&x| x != 7).collect::<Vec<_>>(),
            "everything else goes back"
        );

        // And a budget of less than one whole piece is the same thing: the
        // conversion to pieces floors.
        let sub = RetentionPolicy::new(PIECE - 1, PIECE, 0..20, 20 * PIECE).unwrap();
        assert_eq!(
            sub.shape(),
            Shape::Split {
                window: 0,
                committed: 0
            }
        );
        assert_eq!(sub.window_at(0), 0..1);
    }

    #[test]
    fn the_window_is_ninety_percent_ahead_and_ten_behind() {
        let p = policy(20, 200);
        assert_eq!(
            p.shape(),
            Shape::Split {
                window: 10,
                committed: 10
            },
            "the budget is halved, and the odd piece would go to the window"
        );
        assert_eq!(p.window_at(100), 99..109, "one behind, nine ahead");

        // A hundred pieces of window: ten behind, ninety ahead.
        let p = policy(200, 2000);
        assert_eq!(
            p.shape(),
            Shape::Split {
                window: 100,
                committed: 100
            }
        );
        assert_eq!(p.window_at(1000), 990..1090);
    }

    /// At either end the window keeps its size and slides, rather than being
    /// cut in half by the edge of the file.
    #[test]
    fn a_playhead_at_either_end_gets_a_whole_window_anyway() {
        let p = policy(20, 200);
        let want = 10;

        assert_eq!(p.window_at(0), 0..10, "all of it ahead");
        assert_eq!(p.window_at(1), 0..10, "still pinned to the start");
        assert_eq!(p.window_at(2), 1..11, "and then it moves");

        assert_eq!(p.window_at(199), 190..200, "all of it behind");
        assert_eq!(p.window_at(198), 190..200);
        assert_eq!(p.window_at(197), 190..200, "197 - 1 behind = 196, slid up");

        for playhead in 0..200 {
            let w = p.window_at(playhead);
            assert_eq!(w.end - w.start, want, "the window never shrinks");
            assert!(w.contains(&playhead), "and always holds the playhead");
            assert!(w.end <= 200, "and stays inside the file");
        }

        // A playhead outside the file is clamped rather than refused: a stream
        // that has run off the end of its file is a bookkeeping mistake
        // upstairs, not a reason to stop deciding what to keep.
        assert_eq!(p.window_at(u32::MAX), p.window_at(199));
    }

    /// The window can be told to cover more pieces than the file has -- a file
    /// of one piece is the extreme -- and it is the file that wins.
    #[test]
    fn a_file_of_one_piece_is_the_whole_window_and_nothing_else() {
        // Not covered by the budget, so it splits, and the split is degenerate.
        let mut p = RetentionPolicy::new(PIECE / 2, PIECE, 7..8, PIECE).unwrap();
        assert_eq!(
            p.shape(),
            Shape::Split {
                window: 0,
                committed: 0
            }
        );
        assert_eq!(p.window_at(7), 7..8);
        assert_eq!(p.window_at(0), 7..8, "clamped from below");
        assert_eq!(p.window_at(9), 7..8, "and from above");
        let d = p.advance(7, &held([7]));
        assert!(d.reclaim.is_empty() && d.committed.is_empty());

        // Covered by it, and the one piece is shared.
        let mut p = RetentionPolicy::new(PIECE, PIECE, 7..8, PIECE).unwrap();
        assert_eq!(p.shape(), Shape::Whole);
        let d = p.advance(7, &held([7]));
        assert_eq!(d.committed, vec![7]);
        assert!(d.reclaim.is_empty());
    }

    /// A window wider than the pieces actually on disk has nothing to give
    /// back: the sparse cache is the normal state early in a stream.
    #[test]
    fn a_window_wider_than_what_is_held_reclaims_nothing() {
        let mut p = policy(40, 200);
        assert_eq!(
            p.shape(),
            Shape::Split {
                window: 20,
                committed: 20
            }
        );
        let d = p.advance(100, &held([98, 100, 103]));
        assert_eq!(d.window, 98..118);
        assert!(d.reclaim.is_empty(), "all three are inside it");
        assert!(
            d.committed.is_empty(),
            "and a piece inside the window has not been passed over yet"
        );
        // Nothing held at all is also fine.
        let d = p.advance(100, &BTreeSet::new());
        assert_eq!(
            d,
            Decision {
                window: 98..118,
                ..Default::default()
            }
        );
    }

    /// The window follows the playhead, a short scan back stays inside it, and
    /// what the window releases is committed while there is room and reclaimed
    /// once there is not.
    #[test]
    fn the_window_follows_the_playhead_and_releases_behind_it() {
        let mut p = policy(20, 200);
        // window 10 (one behind, nine ahead), committed 10.
        let mut disk: BTreeSet<u32> = (0..40).collect();

        // Arriving on a warm cache. No window has covered any of it, so the
        // shared half stays empty and everything outside the window goes back
        // -- 39 too: it is ahead of the window, and a piece we hold outside
        // the window is a piece we are not keeping.
        let d = p.advance(30, &disk);
        assert_eq!(d.window, 29..39);
        assert!(
            d.committed.is_empty(),
            "nothing has been passed over yet, whatever is on disk"
        );
        assert_eq!(d.reclaim, (0..29).chain([39]).collect::<Vec<_>>());
        for piece in &d.reclaim {
            disk.remove(piece);
        }
        assert_eq!(disk, held(29..39));

        // Playing on. The window slides off what it covered, and those ten --
        // which we hold, because covering them is what fetched them -- fill
        // the shared half.
        disk.extend(39..49);
        let d = p.advance(40, &disk);
        assert_eq!(d.window, 39..49);
        assert_eq!(
            d.committed,
            (29..39).collect::<Vec<_>>(),
            "the first ten the window let go of fill the shared half"
        );
        assert!(d.reclaim.is_empty(), "and nothing else is held");
        assert_eq!(*p.advertised(), held(29..39));

        // A scan back of a few seconds is served from the window itself, and
        // what the window leaves behind now has nowhere to go: the shared half
        // is full.
        let d = p.advance(39, &disk);
        assert_eq!(d.window, 38..48);
        assert!(d.committed.is_empty(), "the shared half is full");
        assert_eq!(d.reclaim, vec![48], "and 48 has just left the window");
        assert!(!d.reclaim.contains(&39));
    }

    /// A held piece the window has never covered is a reclaim candidate and
    /// never a commit candidate, whichever side of the playhead it is on.
    ///
    /// Reading "released" off the current window alone made this true of every
    /// piece outside it, so the first pass of a stream that resumed onto a warm
    /// cache settled the permanent, never-reclaimed, always-advertised set out
    /// of leftovers -- with the playhead on piece 0, having passed over
    /// nothing, which is verbatim the outcome the rule exists to avoid.
    #[test]
    fn a_piece_no_window_has_covered_is_never_committed() {
        let mut p = policy(20, 200);
        let d = p.advance(0, &held(0..40));
        assert_eq!(d.window, 0..10);
        assert!(
            d.committed.is_empty() && p.advertised().is_empty(),
            "committed {:?} with the playhead on piece 0",
            d.committed
        );
        assert_eq!(
            d.reclaim,
            (10..40).collect::<Vec<_>>(),
            "a cache the playhead has not reached is cache to give back"
        );
    }

    /// Once the committed half is full it does not move again, however the
    /// playhead does. That is the whole reason to choose by "first offered"
    /// rather than by any ranking: a set that re-ranks re-downloads.
    #[test]
    fn the_committed_set_does_not_churn_when_the_playhead_moves() {
        let mut p = policy(20, 400);
        let mut disk: BTreeSet<u32> = (0..40).collect();
        for playhead in [30u32, 60, 200, 5, 399, 100] {
            let d = p.advance(playhead, &disk);
            for piece in &d.reclaim {
                disk.remove(piece);
            }
            // Playing on: the window fills with what it now covers.
            disk.extend(d.window.clone());
        }
        let settled = p.advertised().clone();
        assert_eq!(settled.len(), 10, "the shared half, full");

        for playhead in [7u32, 350, 12, 399, 0, 180] {
            let d = p.advance(playhead, &disk);
            assert!(
                d.committed.is_empty() && d.withdrawn.is_empty(),
                "playhead {playhead} moved the committed set"
            );
            for piece in &d.reclaim {
                disk.remove(piece);
            }
            disk.extend(d.window.clone());
        }
        assert_eq!(*p.advertised(), settled);
    }

    /// A committed piece is never reclaimed, and a reclaimed piece is never
    /// advertised. Both directions, over a playthrough with seeks in it.
    #[test]
    fn nothing_outside_the_committed_set_is_ever_advertised() {
        let mut p = policy(30, 500);
        let Shape::Split { window, committed } = p.shape() else {
            panic!("a 500-piece file does not fit in 30 pieces");
        };
        assert_eq!((window, committed), (15, 15));
        let mut disk: BTreeSet<u32> = BTreeSet::new();
        let mut ever_advertised: BTreeSet<u32> = BTreeSet::new();

        for playhead in (0..500).step_by(3).chain([12, 480, 3, 260, 499, 0]) {
            let d = p.advance(playhead, &disk);
            for piece in &d.reclaim {
                assert!(
                    !ever_advertised.contains(piece),
                    "piece {piece} was advertised and is now being reclaimed"
                );
                assert!(disk.remove(piece), "reclaiming a piece we do not hold");
            }
            // Everything left is held or wanted; the window fills in.
            disk.extend(d.window.clone());
            ever_advertised.extend(p.advertised().iter().copied());
            assert!(
                p.advertised().iter().all(|piece| disk.contains(piece)),
                "advertising a piece that is not on disk"
            );
            assert!(
                disk.len() <= (window + committed) as usize,
                "held {} pieces on a budget of {}",
                disk.len(),
                window + committed
            );
        }
        assert_eq!(p.advertised().len(), committed as usize);
    }

    /// Losing a committed piece behind our back takes it out of the set. It is
    /// the one thing that can shrink it, and the alternative is advertising a
    /// piece that is not there.
    #[test]
    fn a_committed_piece_we_have_lost_stops_being_advertised() {
        let mut p = policy(20, 200);
        // Fill the shared half the only way it can be filled: hold a window's
        // worth, then move the playhead past it.
        let mut disk: BTreeSet<u32> = (0..10).collect();
        p.advance(0, &disk);
        disk.extend(10..20);
        p.advance(11, &disk);
        assert_eq!(*p.advertised(), held(0..10));

        disk.remove(&4);
        let d = p.advance(11, &disk);
        assert_eq!(d.withdrawn, vec![4]);
        assert!(!p.is_advertised(4));
        assert_eq!(*p.advertised(), held((0..10).filter(|&x| x != 4)));

        // It coming back does not put it back. Being on disk is not what
        // commits a piece -- the window releasing it is -- and this window has
        // released nothing since. So it is an ordinary held piece outside the
        // window, which is a piece to give back.
        disk.insert(4);
        let d = p.advance(11, &disk);
        assert!(d.committed.is_empty() && d.withdrawn.is_empty());
        assert_eq!(d.reclaim, vec![4]);
        assert_eq!(*p.advertised(), held((0..10).filter(|&x| x != 4)));

        // The slot it freed goes to the next piece the window lets go of.
        disk.remove(&4);
        disk.extend(20..30);
        let d = p.advance(21, &disk);
        assert_eq!(d.window, 20..30);
        assert_eq!(d.committed, vec![10], "one slot, one release");
        assert_eq!(p.advertised().len(), 10);
    }

    /// The policy governs one file. A torrent's other files are held by
    /// somebody else's decision and must not be reclaimed by this one.
    #[test]
    fn pieces_outside_this_files_range_are_left_alone() {
        let mut p =
            RetentionPolicy::new(10 * PIECE, PIECE, 100..200, 100 * PIECE).expect("consistent");
        assert_eq!(
            p.shape(),
            Shape::Split {
                window: 5,
                committed: 5
            }
        );
        let d = p.advance(150, &held([0, 5, 99, 100, 150, 199, 200, 4000]));
        assert_eq!(d.window, 150..155);
        assert!(
            d.committed.is_empty(),
            "no window has released anything yet"
        );
        assert_eq!(
            d.reclaim,
            vec![100, 199],
            "only pieces this file owns are considered at all"
        );

        // And the same once there is something to commit: the window has let
        // go of 150, and the other files' pieces are still not ours to touch.
        let d = p.advance(160, &held([0, 5, 99, 150, 160, 200, 4000]));
        assert_eq!(d.window, 160..165);
        assert_eq!(d.committed, vec![150]);
        assert!(d.reclaim.is_empty());
        assert!(!p.is_advertised(99) && !p.is_advertised(200));
    }

    /// Asking twice with nothing changed must not change anything the second
    /// time: a reclaim is a request, and it is the caller deleting the piece
    /// that makes it true.
    #[test]
    fn a_second_pass_over_the_same_disk_decides_the_same_thing() {
        let mut p = policy(20, 200);
        // With a release in the first of the two passes, so that "the same
        // thing" is not trivially "nothing".
        let mut disk: BTreeSet<u32> = (0..10).collect();
        p.advance(0, &disk);
        disk.extend(10..40);
        let first = p.advance(30, &disk);
        let second = p.advance(30, &disk);
        assert_eq!(first.window, second.window);
        assert_eq!(first.reclaim, second.reclaim);
        assert_eq!(first.committed, (0..10).collect::<Vec<_>>());
        assert!(
            second.committed.is_empty(),
            "already committed, and this window has let go of nothing since"
        );
        assert!(second.withdrawn.is_empty());
    }

    #[test]
    fn a_file_whose_numbers_do_not_agree_is_refused() {
        // 20 pieces of 1000 hold between 19001 and 20000 bytes.
        assert!(RetentionPolicy::new(0, PIECE, 0..20, 20_000).is_ok());
        assert!(RetentionPolicy::new(0, PIECE, 0..20, 19_001).is_ok());
        let err = RetentionPolicy::new(0, PIECE, 0..20, 19_000)
            .unwrap_err()
            .to_string();
        assert!(err.contains("between 19001 and 20000"), "{err}");
        assert!(RetentionPolicy::new(0, PIECE, 0..20, 20_001).is_err());
        assert!(
            RetentionPolicy::new(0, 0, 0..20, 20_000).is_err(),
            "zero piece length"
        );
        assert!(
            RetentionPolicy::new(0, PIECE, 5..5, 0).is_err(),
            "no pieces"
        );
    }

    /// The halves sum to the budget, the window never exceeds its half, and
    /// nothing held ever exceeds the two together -- over every budget from
    /// nothing to more than the file.
    #[test]
    fn the_split_holds_for_every_budget() {
        let count = 64u32;
        let bytes = u64::from(count) * PIECE;
        for budget_bytes in (0..=bytes + 2 * PIECE).step_by(PIECE as usize / 8) {
            let mut p = RetentionPolicy::new(budget_bytes, PIECE, 0..count, bytes).unwrap();
            match p.shape() {
                Shape::Whole => assert!(budget_bytes >= bytes),
                Shape::Split { window, committed } => {
                    assert!(budget_bytes < bytes);
                    assert_eq!(
                        u64::from(window + committed),
                        budget_bytes / PIECE,
                        "the halves must sum to the budget"
                    );
                    assert!(window >= committed, "the odd piece goes to the window");
                }
            }

            let mut disk: BTreeSet<u32> = BTreeSet::new();
            for playhead in (0..count).chain((0..count).rev()) {
                let d = p.advance(playhead, &disk);
                for piece in &d.reclaim {
                    disk.remove(piece);
                }
                disk.extend(d.window.clone());
                let ceiling = match p.shape() {
                    // One piece over, and only when the budget is nothing:
                    // the window is never empty.
                    Shape::Whole => count as usize,
                    Shape::Split { window, committed } => (window.max(1) + committed) as usize,
                };
                assert!(
                    disk.len() <= ceiling,
                    "budget {budget_bytes}: held {} of a ceiling of {ceiling}",
                    disk.len()
                );
                assert!(p.advertised().iter().all(|piece| disk.contains(piece)));
            }
        }
    }
}
