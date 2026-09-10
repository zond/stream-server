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
//! **That last part was a decision this module could state and not perform,
//! and it performs it now.** At the rev this crate used to pin, `have`
//! implied announced on both paths out of librqbit -- the `have` broadcast
//! and the handshake bitfield, which serialises `get_have_pieces()` whole --
//! and a window piece has to be `have` for the stream to read it, so wiring
//! this policy up would have announced every window piece and withdrawn it
//! again a few seconds later. The fork now has the third state,
//! `ManagedTorrent::set_pieces_advertised`: a suppression set on the chunk
//! tracker, independent of both the have-set and the reclaim want-set, and
//! settable *before* a piece is downloaded so no Have ever goes out for it.
//! [`crate::retention`] is what holds a window back with it and what puts a
//! committed piece into what we announce, and [`RetentionPolicy::advertised`]
//! is now what a peer really is shown.
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
//! [`RetentionPolicy::advance`] chose. But "that window covered it and this
//! one does not" is *not* the transition -- it is only the window stopping
//! covering the piece, which is a thing seeks do wholesale and in both
//! directions. **A release is a piece the playhead has moved past**, which is
//! three conditions and not one: the previous window covered it, it is behind
//! the playhead now, and the playhead got where it is by walking rather than
//! jumping -- every piece between where it was and where it is now was covered
//! by that window. The window is the whole of what we fetch, so a playhead
//! beyond the last piece it covered crossed pieces we never held and cannot
//! have played them. A held piece the window has never covered is a reclaim
//! candidate and nothing else, whichever side of the playhead it is on.
//! Waiting until the window releases a piece is also what makes "a piece we
//! might reclaim is never announced" decidable in one place: the decision to
//! advertise and the decision never to reclaim are the same decision, taken
//! once.
//!
//! **A large forward seek therefore commits nothing, exactly like a backward
//! one.** The pieces it jumps over were covered and are behind the playhead
//! now, and calling them released is the tempting reading -- but playback
//! never reached them either. They are the read-ahead of a position the player
//! walked away from, and settling the permanent, never-reclaimed,
//! always-advertised set out of them is the thing this section rules out, in
//! whichever direction it is done. So they are reclaimed like any other cache
//! the playhead has not reached. That is the rule the owner set -- we keep
//! pieces we have *seen*, and we never settle the shared set from read-ahead
//! -- rather than the shorter predicate: the cost of it is that a seeky
//! session shares less, on a volume too small to share much anyway, and the
//! cost of the other choice is that the rule stops being true. Being about
//! walking rather than about the sign of the movement is also why both seeks
//! land in the same place, and why neither needs a case of its own.
//!
//! One ambiguity is left, and it is bounded rather than hidden: inside the
//! reach of the previous window a seek and playing on are the same
//! observation -- a playhead that has moved forward over pieces we held -- so
//! a seek shorter than a window commits what it skipped. Those are pieces we
//! had already fetched for imminent playback, within a window of where the
//! playhead really was, and there are at most a window's worth of them; that
//! is a different thing from a warm cache or a jumped-over region, both of
//! which are unbounded and neither of which is next to the playhead at all.
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

/// How much of the budget is committed for sharing rather than spent on the
/// window.
///
/// **This is the only thing that differs between the two kinds of stream this
/// policy governs, and it is a number rather than a branch.** The budget is
/// split between what playback needs and what a peer may be offered; a
/// proxied URL response is not seeded, so nobody can be offered any of it and
/// the whole budget is window. Everything else -- the 90/10 window, the
/// release rule, the reclaim -- is the same arithmetic on both, which is what
/// "one retention policy over both stores" means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Share {
    /// Half of it: a torrent's pieces, which a peer may ask us for.
    Half,
    /// None of it. **Not "share nothing yet"** -- there is no swarm for a
    /// URL response and there never will be, so a committed set for one
    /// would be a permanently unreclaimable half of the cache held for a
    /// reader that does not exist.
    Nothing,
}

impl Share {
    /// How many of `budget` pieces this leaves for the committed set.
    ///
    /// The odd piece goes to the window: it is what keeps playback fed, and
    /// the committed half is generosity.
    fn committed_of(self, budget: u32) -> u32 {
        match self {
            Self::Half => budget / 2,
            Self::Nothing => 0,
        }
    }
}

/// How a budget relates to the file it has to hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// The budget covers the file. Keep all of it, share all of it, drop
    /// nothing.
    Whole,
    /// It does not, so it is split: see [`Share`] for what decides where.
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
/// the boundary between two files belongs to both, and this module cannot
/// tell: its reclaim set includes the file's first and last piece whoever
/// else owns bytes in them, and [`crate::retention`] is what takes those
/// back out before anything is deleted. The unpin path has its own answer to
/// the same question in [`super::store::PieceStore::remove_file`], which
/// deletes a boundary piece only once every file that owns bytes in it is
/// gone.
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
    /// It answers two questions and both are needed. *Was this piece covered?*
    /// -- and *did the playhead walk out of here, or jump?*, which is
    /// [`Range::end`] against the playhead: a playhead past the last piece
    /// this window covered crossed pieces no window ever held for it. Without
    /// the second question a seek commits the window it left, forward over the
    /// read-ahead it never reached and backward over the same, which is the
    /// mistake this field exists to prevent wearing one sign or the other.
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
    /// The policy for a stream occupying `pieces` of a chunk store and
    /// `bytes` of the disk, under a budget of `budget_bytes`, `share` of
    /// which may be committed for a peer.
    ///
    /// "Pieces" here are a torrent's when the caller is the piece store's
    /// adapter and 256 KiB chunks when it is `/proxy`'s: the arithmetic is
    /// the store's own -- an index space and a length -- and neither
    /// adapter's names for them reach in here.
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
        share: Share,
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
            shape: Self::shape_for(budget_bytes, piece_length, bytes, share),
            committed: BTreeSet::new(),
            covered,
        })
    }

    /// Split the budget, or don't.
    ///
    /// The comparison that decides is in bytes and not in pieces, because the
    /// last piece of a file is usually short and "the budget covers the file"
    /// has to mean the file and not a rounded-up multiple of it. Everything
    /// after it is in pieces, and the conversion floors: a budget that is two
    /// and a half pieces buys two.
    ///
    /// Where the split falls is [`Share`]'s and there is no branch on the
    /// kind of stream here: a torrent gives half of it to the committed set,
    /// a proxied response gives none, and what is left over is the window in
    /// both cases.
    fn shape_for(budget_bytes: u64, piece_length: u64, bytes: u64, share: Share) -> Shape {
        if budget_bytes >= bytes {
            return Shape::Whole;
        }
        // `budget_bytes < bytes`, so this cannot exceed the file's own piece
        // count and cannot need more than the u32 piece indices already are.
        let budget = (budget_bytes / piece_length) as u32;
        let committed = share.committed_of(budget);
        Shape::Split {
            window: budget - committed,
            committed,
        }
    }

    /// The pieces of the file this policy governs.
    pub fn pieces(&self) -> Range<u32> {
        self.pieces.clone()
    }

    pub fn shape(&self) -> Shape {
        self.shape
    }

    /// The committed set: what is advertised, and nothing else is.
    ///
    /// [`crate::retention`] is what makes that true -- it holds the whole
    /// file back from what the torrent announces before the reader opens,
    /// and puts a piece back only when [`Self::advance`] commits it. It is
    /// also what the cache cleaner is answered from: what we announce is
    /// exactly what nothing will reclaim.
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

    /// The pieces from `playhead` to the window's forward edge, as that edge
    /// stands once nothing clamps the window against the file's start:
    /// `playhead` and the nine tenths ahead of it, cut at the last piece.
    ///
    /// This is what a stream opened at `playhead` may fetch ahead of itself.
    /// Not [`Self::window_at`]`.end`: at the start of a file the window is
    /// clamped against piece zero, so its end is the whole window ahead of
    /// the playhead rather than nine tenths of it, and a lookahead sized from
    /// that end outruns the window for the first tenth of it as the playhead
    /// moves on -- the window's end stands still while the lookahead's
    /// advances -- fetching pieces the next pass reclaims. The reach is at
    /// most the window's end wherever the playhead is, and it moves with the
    /// playhead exactly as the unclamped window does.
    pub fn ahead_of(&self, playhead: u32) -> Range<u32> {
        let total = self.pieces.end - self.pieces.start;
        let want = match self.shape {
            Shape::Whole => total,
            Shape::Split { window, .. } => window.clamp(1, total),
        };
        let playhead = playhead.clamp(self.pieces.start, self.pieces.end - 1);
        let behind = (u64::from(want) * BEHIND_PERCENT / 100) as u32;
        let end = playhead.saturating_add(want - behind).min(self.pieces.end);
        playhead..end
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
    /// the window is **committed** if the window *released* it -- the previous
    /// pass covered it, it is behind `playhead` now, and `playhead` is no
    /// further on than the last piece that window covered, so playback walked
    /// past this piece rather than jumping over it (see `Self::covered`) --
    /// and **reclaimed** if not. So the first pass of a stream commits
    /// nothing, no window having covered anything yet, and so does either
    /// direction of a seek: what a seek leaves behind is read-ahead it never
    /// reached.
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
        // Did the playhead walk here or jump here? It walked if every piece
        // between where it was and where it is now was covered by the previous
        // window, and that window is the whole of what we fetch, so a playhead
        // past the last piece it covered crossed pieces we never held and
        // cannot have played them. `covered.end` is the boundary and it is
        // reachable, not excluded: it is the first piece that window did not
        // hold, and a playhead sitting on it has played up *to* that piece
        // rather than through it. Clamped the way `window_at` clamps, so a
        // playhead off the end of the file decides what the window decides.
        let playhead = playhead.clamp(self.pieces.start, self.pieces.end - 1);
        let walked = playhead <= self.covered.end;

        // Ascending, so "the first pieces offered win" is a stable rule and
        // not a function of iteration order.
        for &piece in held {
            if !self.pieces.contains(&piece) || self.committed.contains(&piece) {
                continue;
            }
            // Outside the window is what makes a piece reclaimable. Having
            // been inside the *previous* one, being behind the playhead now,
            // and the playhead having walked here rather than jumped are what
            // make it committable: together they are playback moving past it,
            // where "outside the previous window" alone is only the window
            // stopping covering it, which a seek does in either direction.
            let outside = !window.contains(&piece);
            let released = outside && piece < playhead && walked && self.covered.contains(&piece);
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
            Share::Half,
        )
        .expect("a consistent file")
    }

    /// The same file under a stream nothing can be shared from: `/proxy`'s.
    fn unshared(budget_pieces: u64, count: u32) -> RetentionPolicy {
        RetentionPolicy::new(
            budget_pieces * PIECE,
            PIECE,
            0..count,
            u64::from(count) * PIECE,
            Share::Nothing,
        )
        .expect("a consistent file")
    }

    /// Play the stream through `playheads`, as a caller would: honour every
    /// reclaim, and fill in whatever the window covers, because covering a
    /// piece is what fetches it.
    ///
    /// Stepping matters, and several tests below need it rather than a single
    /// leap: a playhead that arrives beyond the last piece the previous window
    /// covered has jumped over pieces we never held, and this policy commits
    /// nothing for a jump.
    fn play(
        p: &mut RetentionPolicy,
        disk: &mut BTreeSet<u32>,
        playheads: impl IntoIterator<Item = u32>,
    ) {
        for playhead in playheads {
            let d = p.advance(playhead, disk);
            for piece in &d.reclaim {
                disk.remove(piece);
            }
            disk.extend(d.window.clone());
        }
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

    /// **A stream with no swarm spends the whole budget on the window, and
    /// that is a number and not a case.**
    ///
    /// A proxied URL response is not seeded: nothing will ever ask us for a
    /// chunk of it. A committed half for one would be half the cache held
    /// permanently -- never reclaimed, by the committed set's own rule --
    /// for a reader that does not exist, and the window playback actually
    /// needs would be half what the disk could give it. So the split is
    /// [`Share::Nothing`] and everything else about the policy is the same
    /// arithmetic the torrent gets.
    #[test]
    fn a_stream_nothing_can_be_shared_from_spends_the_whole_budget_on_the_window() {
        let shared = policy(10, 40);
        let alone = unshared(10, 40);
        assert_eq!(
            shared.shape(),
            Shape::Split {
                window: 5,
                committed: 5
            }
        );
        assert_eq!(
            alone.shape(),
            Shape::Split {
                window: 10,
                committed: 0
            },
            "twice the window, because none of it is being kept for a peer"
        );
        assert_eq!(
            shared.shape().piece_budget(),
            alone.shape().piece_budget(),
            "and the same budget: the split moved, the total did not"
        );
    }

    /// And it commits nothing, ever -- so nothing of it is exempt from the
    /// reclaim, whatever the playhead does. Played straight through, the
    /// disk holds a window and no more.
    #[test]
    fn an_unshared_stream_commits_nothing_and_holds_only_its_window() {
        let mut p = unshared(10, 40);
        let mut disk = BTreeSet::new();
        play(&mut p, &mut disk, 0..40);
        assert!(
            p.advertised().is_empty(),
            "there is no peer to have been told about any of it"
        );
        assert!(
            disk.len() <= 10,
            "{} pieces on disk, and the budget is 10",
            disk.len()
        );

        // The same walk on the sharing policy settles a permanent set, which
        // is exactly what a proxied stream must not do.
        let mut shared = policy(10, 40);
        let mut shared_disk = BTreeSet::new();
        play(&mut shared, &mut shared_disk, 0..40);
        assert!(!shared.advertised().is_empty());
    }

    /// The boundary is in bytes, because the last piece of a file is usually
    /// short: a budget one byte under the file's real size is still a split.
    #[test]
    fn the_whole_file_test_is_the_files_own_length_not_a_rounded_one() {
        let bytes = 19 * PIECE + 1;
        let exact = RetentionPolicy::new(bytes, PIECE, 0..20, bytes, Share::Half).unwrap();
        assert_eq!(exact.shape(), Shape::Whole);
        let short = RetentionPolicy::new(bytes - 1, PIECE, 0..20, bytes, Share::Half).unwrap();
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
        let sub = RetentionPolicy::new(PIECE - 1, PIECE, 0..20, 20 * PIECE, Share::Half).unwrap();
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
        let mut p = RetentionPolicy::new(PIECE / 2, PIECE, 7..8, PIECE, Share::Half).unwrap();
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
        let mut p = RetentionPolicy::new(PIECE, PIECE, 7..8, PIECE, Share::Half).unwrap();
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

        // Playing on, a piece at a time. The window slides off the pieces
        // behind the playhead one by one, and those -- which we hold, because
        // covering them is what fetched them -- fill the shared half.
        let d = p.advance(31, &disk);
        assert_eq!(d.window, 30..40);
        assert_eq!(
            d.committed,
            vec![29],
            "the one piece the window has walked off"
        );
        assert!(d.reclaim.is_empty(), "and nothing else is held");
        disk.extend(d.window.clone());

        play(&mut p, &mut disk, 32..41);
        assert_eq!(
            *p.advertised(),
            held(29..39),
            "ten pieces of playback, and the shared half is full"
        );

        // A scan back of a few seconds is served from the window itself, and
        // what the window leaves behind now has nowhere to go: the shared half
        // is full.
        assert_eq!(p.window_at(40), 39..49);
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

    /// A seek back commits nothing. The previous window stops covering all ten
    /// of its pieces at once, but the playhead did not move *past* them: it
    /// moved back over the one it was on and away from nine it had never
    /// reached.
    ///
    /// Reading "released" as "the previous window covered it and this one does
    /// not" made a scrub back -- or a player reading a trailing `moov` atom and
    /// returning to 0 -- settle the permanent, never-reclaimed,
    /// always-advertised set out of read-ahead, which is the same mistake as
    /// committing a warm cache wearing the opposite sign.
    #[test]
    fn a_seek_back_commits_none_of_the_window_it_left() {
        let mut p = policy(20, 200);
        let disk = held(29..39);
        let d = p.advance(30, &disk);
        assert_eq!(d.window, 29..39);
        assert!(d.committed.is_empty() && d.reclaim.is_empty());

        // Back to the start, having played one piece.
        let d = p.advance(0, &disk);
        assert_eq!(d.window, 0..10);
        assert!(
            d.committed.is_empty() && p.advertised().is_empty(),
            "committed {:?} for a playhead that has only ever been on piece 30",
            d.committed
        );
        assert_eq!(
            d.reclaim,
            (29..39).collect::<Vec<_>>(),
            "read-ahead the playhead left behind is cache to give back"
        );
    }

    /// And a seek *forward* is the same, which is the point: the pieces it
    /// jumps over were covered and are behind the playhead now, and playback
    /// passed over none of them either. What decides is whether the window slid
    /// or jumped, not the sign of the movement.
    #[test]
    fn a_seek_on_commits_none_of_the_window_it_jumped_out_of() {
        let mut p = policy(20, 200);
        let mut disk = held(29..39);
        p.advance(30, &disk);

        let d = p.advance(120, &disk);
        assert_eq!(d.window, 119..129);
        assert!(
            d.committed.is_empty() && p.advertised().is_empty(),
            "committed {:?} the playhead skipped over",
            d.committed
        );
        assert_eq!(d.reclaim, (29..39).collect::<Vec<_>>());

        // Playing on from where it landed commits from there, once the window
        // has walked off something -- the seek cost us the shared half's
        // filling, not its filling ever again.
        disk = held(119..129);
        play(&mut p, &mut disk, 121..123);
        assert_eq!(*p.advertised(), held(119..121));
    }

    /// Walking and jumping part at the far edge of the previous window: the
    /// playhead may arrive at the first piece that window did not cover, having
    /// played up *to* it, and one piece further it must have played *through*
    /// a piece no window ever held for it, which is a seek.
    #[test]
    fn the_playhead_may_walk_to_the_edge_of_the_last_window_but_not_over_it() {
        let start = {
            let mut p = policy(20, 200);
            p.advance(30, &held(29..39)); // covers 29..39, holding all of it
            p
        };

        let mut walked = start.clone();
        let d = walked.advance(39, &held(29..39));
        assert_eq!(d.window, 38..48);
        assert_eq!(
            d.committed,
            (29..38).collect::<Vec<_>>(),
            "every piece it played through, and not the one it stopped on"
        );

        let mut jumped = start;
        let d = jumped.advance(40, &held(29..39));
        assert_eq!(d.window, 39..49);
        assert!(
            d.committed.is_empty(),
            "reaching 40 means playing piece 39, which no window ever held"
        );
        assert_eq!(d.reclaim, (29..39).collect::<Vec<_>>());
    }

    /// Once the committed half is full it does not move again, however the
    /// playhead does. That is the whole reason to choose by "first offered"
    /// rather than by any ranking: a set that re-ranks re-downloads.
    #[test]
    fn the_committed_set_does_not_churn_when_the_playhead_moves() {
        let mut p = policy(20, 400);
        let mut disk: BTreeSet<u32> = BTreeSet::new();
        // Filled the only way it can be: by playing, which is what walks the
        // window off a piece. The seeks come after.
        play(&mut p, &mut disk, 0..30);
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
        // Fill the shared half the only way it can be filled: play, and let
        // the window walk off what is behind the playhead.
        let mut disk: BTreeSet<u32> = BTreeSet::new();
        play(&mut p, &mut disk, 0..12);
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
        let d = p.advance(12, &disk);
        assert_eq!(d.window, 11..21);
        assert_eq!(d.committed, vec![10], "one slot, one release");
        assert_eq!(p.advertised().len(), 10);
    }

    /// The policy governs one file. A torrent's other files are held by
    /// somebody else's decision and must not be reclaimed by this one.
    #[test]
    fn pieces_outside_this_files_range_are_left_alone() {
        let mut p = RetentionPolicy::new(10 * PIECE, PIECE, 100..200, 100 * PIECE, Share::Half)
            .expect("consistent");
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

        // And the same once there is something to commit: playback has walked
        // to the far edge of that window and it has let go of 150, while the
        // other files' pieces are still not ours to touch.
        let d = p.advance(155, &held([0, 5, 99, 150, 155, 200, 4000]));
        assert_eq!(d.window, 155..160);
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
        let first = p.advance(10, &disk);
        let second = p.advance(10, &disk);
        assert_eq!(first.window, second.window);
        assert_eq!(first.reclaim, second.reclaim);
        assert_eq!(first.committed, (0..9).collect::<Vec<_>>());
        assert!(
            second.committed.is_empty(),
            "already committed, and this window has let go of nothing since"
        );
        assert!(second.withdrawn.is_empty());
    }

    #[test]
    fn a_file_whose_numbers_do_not_agree_is_refused() {
        // 20 pieces of 1000 hold between 19001 and 20000 bytes.
        assert!(RetentionPolicy::new(0, PIECE, 0..20, 20_000, Share::Half).is_ok());
        assert!(RetentionPolicy::new(0, PIECE, 0..20, 19_001, Share::Half).is_ok());
        let err = RetentionPolicy::new(0, PIECE, 0..20, 19_000, Share::Half)
            .unwrap_err()
            .to_string();
        assert!(err.contains("between 19001 and 20000"), "{err}");
        assert!(RetentionPolicy::new(0, PIECE, 0..20, 20_001, Share::Half).is_err());
        assert!(
            RetentionPolicy::new(0, 0, 0..20, 20_000, Share::Half).is_err(),
            "zero piece length"
        );
        assert!(
            RetentionPolicy::new(0, PIECE, 5..5, 0, Share::Half).is_err(),
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
            let mut p =
                RetentionPolicy::new(budget_bytes, PIECE, 0..count, bytes, Share::Half).unwrap();
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
