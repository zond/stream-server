//! **When each piece was last any use**, which is what decides the order
//! things leave the disk.
//!
//! Kept here because nothing else knows. `Backing::held` answers a bare set
//! of indices with no times on it -- the torrent's is a bitfield, the
//! proxy's a directory -- so a piece's arrival is recovered by diffing one
//! pass's listing against the last, and its reads are stamped exactly
//! because the read path carries its own clock.
//!
//! **This is the order things are given back in.** The coldest of what no
//! consumer is asking for is what a pass reclaims
//! ([`super::streams::Streams::coldest_of`]), and the same order is traced
//! so a field log can be read against what a pass took; see
//! `docs/read-pattern-retention.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

/// How many re-reads still halve a piece's age.
///
/// Doubling how long something survives each time it is read again is what
/// keeps a container index alive without anyone knowing where one is: an
/// `mp4`'s `moov` is read at every seek, so it earns its place by being
/// wanted rather than by being recognised. That is the whole of what
/// replaces `STRUCTURAL_PIECES`, which reserved eight slots first-come and
/// on the field's film gave one of them to ordinary `mdat`.
///
/// Bounded because the discount is exponential and a ceiling is the
/// difference between "survives a long time" and "never leaves": ten
/// halvings is about a thousandfold, which outlives any session, and the
/// eleventh would only make a piece harder to reason about.
const HALVINGS: u32 = 10;

/// What one piece has been to this entity.
#[derive(Debug, Clone, Copy)]
struct Use {
    /// When a pass first saw it on the disk. A pass tick, not the arrival:
    /// the listing is the only thing that reports one.
    fetched: Instant,
    /// When a read last took bytes out of it, exactly, or `None` for a
    /// piece nothing has read yet -- lookahead that arrived and has not
    /// been reached.
    read: Option<Instant>,
    /// How many visits have read it. One is no discount; the discount is
    /// for coming *back*.
    ///
    /// Visits and not reads: a response reads a 4 MiB piece in sixteen
    /// 256 KiB reads on one pass through it, and counted per read every
    /// piece a viewer played once was at the ten-halving ceiling, the
    /// same as an index read at every seek -- so the discount told
    /// nothing apart. A visit is a read by a different response from the
    /// last one that read the piece; one response reads forward, so it
    /// cannot come back to a piece it has left.
    reads: u32,
    /// Which response last read it, for telling a visit from the next
    /// chunk of the same one.
    reader: u64,
}

impl Use {
    /// The later of the two things that make a piece worth keeping.
    fn last_useful(&self) -> Instant {
        self.read.unwrap_or(self.fetched)
    }

    /// How old this piece is for the purpose of taking it, which is not how
    /// old it is.
    ///
    /// Halved once per re-read, to [`HALVINGS`]. A piece read five times is
    /// treated as a sixteenth its age, so it outlives sixteen pieces that
    /// were read once at the same moment -- which is what it means for
    /// something to be worth keeping because it keeps being wanted.
    fn age(&self, now: Instant) -> Duration {
        let plain = now.saturating_duration_since(self.last_useful());
        plain / (1u32 << self.reads.saturating_sub(1).min(HALVINGS))
    }
}

/// Every piece this entity holds, and when each was last any use.
#[derive(Debug, Default)]
pub(crate) struct Ledger {
    by_piece: BTreeMap<u32, Use>,
}

impl Ledger {
    /// Reconcile against what the disk actually holds.
    ///
    /// A piece the listing has and the ledger does not has just arrived --
    /// stamped `now`, which is this pass rather than the moment it landed.
    /// That is as fine a resolution as there is: nothing records arrivals,
    /// and an LRU whose granularity is tens of seconds does not need one.
    /// A piece the ledger has and the listing does not is gone, and is
    /// forgotten rather than aged, so a piece that comes back later comes
    /// back new.
    pub(crate) fn settle(&mut self, held: &BTreeSet<u32>, now: Instant) {
        self.by_piece.retain(|piece, _| held.contains(piece));
        for piece in held {
            self.by_piece.entry(*piece).or_insert(Use {
                fetched: now,
                read: None,
                reads: 0,
                reader: 0,
            });
        }
    }

    /// Note that a read took bytes out of every piece it covered.
    ///
    /// Exact, unlike an arrival: the read path stamps its own clock, and
    /// this is the half of `last_useful` that decides almost everything --
    /// a piece behind the playhead is one that was read, a piece ahead of
    /// it is one that arrived.
    ///
    /// `reader` is the response that served it: a read by the response
    /// that read the piece last is more of the same visit, and moves only
    /// the time ([`Use::reads`]).
    pub(crate) fn read(&mut self, pieces: std::ops::RangeInclusive<u32>, reader: u64, at: Instant) {
        for piece in pieces {
            if let Some(used) = self.by_piece.get_mut(&piece) {
                if used.read.is_none() || used.reader != reader {
                    used.reads = used.reads.saturating_add(1);
                }
                used.read = Some(at);
                used.reader = reader;
            }
        }
    }

    /// The pieces this entity would give up first, oldest by effective age,
    /// skipping anything `exempt` answers for.
    ///
    /// `exempt` is the tiers above the LRU: what is committed for sharing,
    /// and what is inside a live stream's window. Neither is offered here
    /// at all -- an LRU that ranked them and relied on never reaching them
    /// would be one bad budget away from taking the bytes a viewer is about
    /// to play.
    pub(crate) fn coldest(
        &self,
        now: Instant,
        exempt: impl Fn(u32) -> bool,
        want: usize,
    ) -> Vec<u32> {
        let mut candidates: Vec<(Duration, u32)> = self
            .by_piece
            .iter()
            .filter(|(piece, _)| !exempt(**piece))
            .map(|(piece, used)| (used.age(now), *piece))
            .collect();
        // Oldest first, and a stable tie-break so two pieces of one age go
        // in file order rather than in whatever order the map was walked.
        candidates.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        candidates
            .into_iter()
            .take(want)
            .map(|(_, piece)| piece)
            .collect()
    }

    /// How many pieces the ledger is tracking, for the trace line.
    pub(crate) fn len(&self) -> usize {
        self.by_piece.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(t0: Instant, secs: u64) -> Instant {
        t0 + Duration::from_secs(secs)
    }

    fn disk(pieces: std::ops::Range<u32>) -> BTreeSet<u32> {
        pieces.collect()
    }

    /// **A piece that was read is newer than one that merely arrived**, and
    /// the read is what the order is really made of.
    ///
    /// Lookahead that arrived and was never reached is exactly what a seek
    /// leaves behind, and it is the first thing that should go. A piece
    /// behind the playhead was read, and while a viewer might scrub back to
    /// it, it has at least been of use.
    #[test]
    fn a_piece_that_was_read_outranks_one_that_only_arrived() {
        let t0 = Instant::now();
        let mut ledger = Ledger::default();
        ledger.settle(&disk(0..4), t0);
        ledger.read(1..=1, 1, at(t0, 30));

        let coldest = ledger.coldest(at(t0, 60), |_| false, 4);
        assert_eq!(
            coldest.last(),
            Some(&1),
            "the one that was read is the last to go: {coldest:?}"
        );
    }

    /// **A piece read again survives longer, and nobody had to know what it
    /// was.**
    ///
    /// This is what keeps an `mp4`'s `moov` alive: it is read at every seek,
    /// so it earns its place by being wanted. What it replaces reserved
    /// eight slots first-come, and on the field's film gave one of them to
    /// ordinary media data in the middle of a second audio track, which
    /// then survived every pass for the life of the entity while the real
    /// index did not.
    #[test]
    fn a_piece_read_again_and_again_outlives_one_read_once() {
        let t0 = Instant::now();
        let mut ledger = Ledger::default();
        ledger.settle(&disk(0..2), t0);

        // Piece 1 was last read forty seconds before piece 0 -- older by
        // the plain reading -- but it was read four times and piece 0 once.
        ledger.read(0..=0, 1, at(t0, 50));
        for (reader, second) in [(2, 10), (3, 20), (4, 30), (5, 40)] {
            ledger.read(1..=1, reader, at(t0, second));
        }

        let coldest = ledger.coldest(at(t0, 100), |_| false, 2);
        assert_eq!(
            coldest,
            vec![0, 1],
            "the piece that keeps being wanted outlives the more recently \
             read one: ninety seconds halved four times is under six, \
             against fifty"
        );
    }

    /// **One pass through a piece is one visit, however many reads it
    /// took.** A response reads a 4 MiB piece in sixteen reads; counted per
    /// read, a piece played once had the ten-halving ceiling already and
    /// an index read at every seek could not outlive it.
    #[test]
    fn one_pass_through_a_piece_is_one_visit() {
        let t0 = Instant::now();
        let mut ledger = Ledger::default();
        ledger.settle(&disk(0..2), t0);

        // Piece 0 played through once by one response, sixteen reads.
        for _ in 0..16 {
            ledger.read(0..=0, 1, at(t0, 50));
        }
        // Piece 1, the index, read by three seeks' responses.
        for (reader, second) in [(2, 10), (3, 20), (4, 30)] {
            ledger.read(1..=1, reader, at(t0, second));
        }

        let coldest = ledger.coldest(at(t0, 100), |_| false, 2);
        assert_eq!(
            coldest,
            vec![0, 1],
            "the index read at every seek outlives the piece played once: \
             seventy seconds halved twice, against fifty"
        );
    }

    /// The tiers above the LRU are not ranked and relied upon not to be
    /// reached -- they are not offered at all. An LRU that merely put them
    /// last would be one bad budget away from taking the bytes a viewer is
    /// about to play.
    #[test]
    fn what_is_exempt_is_never_a_candidate() {
        let t0 = Instant::now();
        let mut ledger = Ledger::default();
        ledger.settle(&disk(0..4), t0);

        let coldest = ledger.coldest(at(t0, 60), |piece| piece < 2, 4);
        assert_eq!(coldest, vec![2, 3], "nothing exempt was offered");
    }

    /// A piece that left the disk is forgotten rather than aged, so one
    /// that comes back comes back new -- it is a fetch somebody paid for
    /// again, not an old piece that was away for a while.
    #[test]
    fn a_piece_that_left_the_disk_comes_back_new() {
        let t0 = Instant::now();
        let mut ledger = Ledger::default();
        ledger.settle(&disk(0..2), t0);
        ledger.read(0..=0, 1, t0);

        // Piece 0 goes, and comes back at a later pass.
        ledger.settle(&disk(1..2), at(t0, 10));
        ledger.settle(&disk(0..2), at(t0, 20));

        let coldest = ledger.coldest(at(t0, 30), |_| false, 2);
        assert_eq!(
            coldest,
            vec![1, 0],
            "piece 1 has been sitting there since t0; piece 0 arrived at 20"
        );
    }
}
