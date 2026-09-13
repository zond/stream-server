//! **What may not be unlinked, answerable without taking a lock.**
//!
//! The door is asked once per run-part on the torrent and once per
//! candidate chunk on the proxy, from a blocking thread, at the instant of
//! every unlink. Today each of those asks takes the entity's state lock and
//! walks its readers. That was affordable while the answer was one piece of
//! arithmetic over one window; it is not affordable as a walk of a stream
//! table, and a lock taken per chunk on a blocking thread is a contention
//! point on the path that is already the slowest thing a pass does.
//!
//! So the answer is published instead: one bit per piece, written only by
//! the owner and only under its own lock, read by the door with a load and
//! a mask. For the field's film that is 5,566 bits -- 87 words, 696 bytes.
//!
//! **The door never writes it.** "A stream dies when its region is
//! evicted" reads like a write from the reclaim, and the reclaim holds no
//! turn; two unordered writers to one piece of state is the defect this
//! module's own owner has spent four review rounds finding in other places.
//! The door refuses or does not refuse, and the owner alone decides what
//! the bits say.
//!
//! **Phase A: written and reported, never consulted.** The door still takes
//! its lock; this is populated beside it so a field log says how much of an
//! entity the new policy would be holding, before anything depends on it.

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

/// One bit per piece: set means "inside a live stream's window, committed
/// for sharing, or promised to a parked read -- may not be unlinked".
#[derive(Debug)]
pub struct Exempt {
    /// The entity's piece count, which bounds every index.
    pieces: u32,
    words: Box<[AtomicU64]>,
}

impl Exempt {
    pub fn for_pieces(pieces: u32) -> Self {
        Self {
            pieces,
            words: (0..(pieces as usize).div_ceil(64))
                .map(|_| AtomicU64::new(0))
                .collect(),
        }
    }

    fn slot(piece: u32) -> (usize, u64) {
        ((piece / 64) as usize, 1u64 << (piece % 64))
    }

    /// Whether `piece` may not be unlinked.
    ///
    ///
    /// One load and a mask, `Acquire` against the owner's `Release`. This
    /// is the whole of what the door will do on the unlink path.
    ///
    /// **Phase A**: no door calls it yet, which is the point -- the set is
    /// published and reported for a field log before any unlink depends on
    /// it.
    pub fn holds(&self, piece: u32) -> bool {
        if piece >= self.pieces {
            return false;
        }
        let (word, bit) = Self::slot(piece);
        self.words
            .get(word)
            .is_some_and(|word| word.load(Ordering::Acquire) & bit != 0)
    }

    /// Publish exactly `runs` as exempt, and nothing else.
    ///
    /// Whole-cloth rather than incrementally, because an incremental update
    /// is two readings of a set that changed in between -- which is the
    /// shape of defect this owner keeps finding. The cost is writing 87
    /// words a pass on the field's film, which is nothing beside the
    /// listing the pass has just done.
    ///
    /// The whole set is computed before any of it is stored, and stored a
    /// word at a time. Clearing first and setting after would flicker every
    /// piece that is exempt before and after the call through a moment of
    /// not-exempt, and a door asking in that moment would allow an unlink
    /// it should have refused; this way a piece only ever reads as clear
    /// when it is genuinely leaving.
    pub fn publish(&self, runs: &[Range<u32>]) {
        let mut next = vec![0u64; self.words.len()];
        for run in runs {
            for piece in run.start.min(self.pieces)..run.end.min(self.pieces) {
                let (word, bit) = Self::slot(piece);
                if let Some(slot) = next.get_mut(word) {
                    *slot |= bit;
                }
            }
        }
        for (slot, value) in self.words.iter().zip(next) {
            slot.store(value, Ordering::Release);
        }
    }

    /// Hold one more region, on top of whatever is published.
    ///
    /// **For a promise**, which is made between passes: a parked read is
    /// told which pieces are coming, and those may not be unlinked whatever
    /// the streams are doing. Never clears, because a caller that cleared
    /// would be a second writer deciding something is over; the next
    /// [`Self::publish`] is what takes it away, and the pass that publishes
    /// includes every promise live at that moment.
    ///
    /// Written by the owner under the entity's lock, like `publish`, so the
    /// two cannot interleave -- a promise set while a publication was
    /// computing its words would otherwise be lost by the store that
    /// followed it.
    pub fn hold(&self, run: Range<u32>) {
        for piece in run.start.min(self.pieces)..run.end.min(self.pieces) {
            let (word, bit) = Self::slot(piece);
            if let Some(slot) = self.words.get(word) {
                slot.fetch_or(bit, Ordering::Release);
            }
        }
    }

    /// How many pieces are exempt, for the trace line.
    pub fn count(&self) -> u32 {
        self.words
            .iter()
            .map(|word| word.load(Ordering::Relaxed).count_ones())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_published_run_is_held_and_nothing_else_is() {
        let exempt = Exempt::for_pieces(200);
        exempt.publish(&[10..20, 100..103]);

        assert!(!exempt.holds(9));
        assert!(exempt.holds(10));
        assert!(exempt.holds(19));
        assert!(!exempt.holds(20));
        assert!(exempt.holds(102));
        assert!(!exempt.holds(103));
        assert_eq!(exempt.count(), 13);
    }

    /// **Publishing replaces, it does not add.** A window that has moved on
    /// must stop holding what it left behind, or the exempt set only ever
    /// grows and the LRU beneath it is offered less and less until it is
    /// offered nothing.
    #[test]
    fn publishing_a_window_releases_the_one_before_it() {
        let exempt = Exempt::for_pieces(200);
        exempt.publish(std::slice::from_ref(&(10..20)));
        exempt.publish(std::slice::from_ref(&(30..40)));

        assert!(!exempt.holds(10), "the old window let go");
        assert!(exempt.holds(30), "and the new one holds");
        assert_eq!(exempt.count(), 10);
    }

    /// An index past the end of the entity is not exempt, and does not
    /// panic: the door is asked about whatever the backing offers it, and a
    /// run clipped to the entity is the owner's business rather than a
    /// precondition on this.
    #[test]
    fn a_piece_the_entity_does_not_have_is_not_held() {
        let exempt = Exempt::for_pieces(100);
        exempt.publish(std::slice::from_ref(&(90..120)));

        assert!(exempt.holds(99));
        assert!(!exempt.holds(100));
        assert!(!exempt.holds(10_000));
        assert_eq!(exempt.count(), 10, "only the pieces the entity has");
    }
}
