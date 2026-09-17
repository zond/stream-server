//! **How deep the backend splits the head of a stream's lookahead**, sized
//! here and handed down.
//!
//! The backend (`librqbit`, the fork's `CLAIMS.md`) splits the first few
//! pieces ahead of a reader into claims any faster peer may join, and asks
//! for them before anything deeper. How many pieces that is used to be a
//! constant, two, on the reasoning that every piece after those has a whole
//! piece of playback to arrive in. That holds only while a piece arrives in
//! less than a piece of playback. The field of 2026-09-17 -- a phone and a
//! television on one wifi, one 20 GB film -- had 4 MiB pieces completing in
//! two to twelve seconds against 1.2 s of playback each, and the reader
//! walked into piece after piece still in flight, waiting each time on its
//! slowest claim. Splitting had begun about a second before the reader
//! arrived, on pieces that needed six.
//!
//! The backend knows how long its pieces take (the median of the last
//! sixty-four completed, whole and split alike) but not how fast the reader
//! eats them or that the player has stopped; this crate knows both, so the
//! depth is decided here, on every pass, and applied when it changes:
//!
//! * **Start at the median.** A piece takes the median to arrive; the
//!   reader plays a piece in `piece_length / bitrate` seconds; so the split
//!   has to begin `ceil(median / seconds_per_piece)` pieces ahead for the
//!   piece to be whole when the reader gets there. Never fewer than
//!   [`FLOOR`], which is what the constant was. The median is of every
//!   piece on purpose: until the split reaches it a piece is one peer's
//!   whole reservation, so what whole pieces take is the time to cover,
//!   and a median of split pieces alone -- several peers filling each at
//!   once -- measured the mode it was sizing and shrank the depth back to
//!   a second before the reader.
//! * **One more per stall.** The player says when it has shown its
//!   buffering popup after having played -- the one signal that the
//!   arithmetic above was not enough, whatever the reason. Each such stall
//!   adds a piece for the rest of that video.
//! * **Never past the lookahead.** The split pieces are asked for before
//!   the rest of the window; a depth past the window is the whole window
//!   split, which is every piece cut into sixteen and nobody fetching whole.
//!   The ceiling is the stream's lookahead in pieces -- bitrate times the
//!   seconds the viewer's profile buys, over the piece length.
//! * **Reset per video.** The stalls are this video's: a new player on the
//!   torrent starts the count again, since the next episode's swarm is the
//!   same but its bitrate is not necessarily.
//! * **Up freely, down a piece a pass.** A rising median is the signal --
//!   pieces are taking longer and the split has to start earlier now. A
//!   falling one can wait: the depth the backend holds shrinks by one per
//!   pass towards what the arithmetic asks, so a median that dips for a
//!   few passes (the fast split pieces of a seek's burst filling the ring)
//!   does not pull the split zone out from under pieces already cut. The
//!   field of 2026-09-17 12:22 had the depth flap 7-3-4-5-6-8-5-4-3 in
//!   forty seconds on the arithmetic alone.
//!
//! Where no length has been stated there is no bitrate, so no median-derived
//! depth and no ceiling: the floor plus the stalls stands, as the constant
//! did before the player had said how long the film is.

use std::time::Duration;

/// The fewest pieces ever split: what the backend's constant was, and what
/// stands before anything is measured.
pub const FLOOR: usize = 2;

/// **How many pieces at the head of the lookahead to split**, from what a
/// pass knows.
///
/// `median` is the backend's median completion of a piece, `None` before
/// it has finished any; `bitrate` the film's own, `None` before a
/// player has stated a length; `lookahead_seconds` the seconds of film the
/// viewer's buffer profile buys; `stalls` how many times this video's
/// player has reported buffering after having played. Never zero.
pub fn depth(
    median: Option<Duration>,
    piece_length: u64,
    bitrate: Option<u64>,
    lookahead_seconds: u64,
    stalls: usize,
) -> usize {
    let piece_length = piece_length.max(1);
    let bitrate = bitrate.filter(|rate| *rate > 0);
    // Seconds of playback a piece holds: the unit the median is measured
    // against.
    let seconds_per_piece = bitrate.map(|rate| piece_length as f64 / rate as f64);
    let from_median = match (median, seconds_per_piece) {
        (Some(median), Some(seconds)) if seconds > 0.0 => {
            (median.as_secs_f64() / seconds).ceil() as usize
        }
        _ => 0,
    };
    let asked = from_median.max(FLOOR).saturating_add(stalls);
    // The lookahead in pieces, where there is a bitrate to state it in.
    let ceiling = bitrate.map(|rate| {
        let bytes = rate.saturating_mul(lookahead_seconds);
        usize::try_from(bytes.div_ceil(piece_length)).unwrap_or(usize::MAX)
    });
    ceiling.map_or(asked, |ceiling| asked.min(ceiling)).max(1)
}

/// **This video's stalls, and the depth last handed down** -- what the
/// pass keeps between two readings so it applies a depth only when it
/// changes, and lets it fall no faster than a piece a pass.
///
/// One per torrent, not per file: the split depth is the torrent's (it is
/// the piece tracker's), and a torrent plays one video at a time. Kept on
/// the read-pattern detector because that is the one thing the engine, which
/// hears from the player, and its backing, which runs the pass, already
/// share.
#[derive(Debug, Default)]
pub struct DeadlineDepth {
    stalls: usize,
    applied: Option<usize>,
}

impl DeadlineDepth {
    /// A new player opened on the torrent: the count starts again.
    pub fn opened(&mut self) {
        self.stalls = 0;
    }

    /// The player showed its buffering popup after having played.
    pub fn stalled(&mut self) {
        self.stalls = self.stalls.saturating_add(1);
    }

    /// How many stalls this video's player has reported.
    pub fn stalls(&self) -> usize {
        self.stalls
    }

    /// The depth to hand the backend given that the arithmetic asks for
    /// `asked`: `asked` itself when it is no lower than what the backend
    /// holds, one less than the backend holds when it is -- and `None` when
    /// that is what the backend holds already, so the caller applies only a
    /// change. The first reading always applies as asked: the backend's own
    /// default is not this crate's to assume.
    pub fn settle(&mut self, asked: usize) -> Option<usize> {
        let depth = match self.applied {
            Some(held) if asked < held => held - 1,
            _ => asked,
        };
        let changed = self.applied != Some(depth);
        self.applied = Some(depth);
        changed.then_some(depth)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PIECE: u64 = 4 << 20;
    /// 3.5 MB/s: the field's film.
    const RATE: u64 = 3_568_061;
    const SECONDS: u64 = 90;

    /// Nothing measured and nothing stated: the constant the backend had.
    #[test]
    fn nothing_known_is_the_floor() {
        assert_eq!(depth(None, PIECE, None, SECONDS, 0), FLOOR);
        assert_eq!(depth(None, PIECE, Some(RATE), SECONDS, 0), FLOOR);
        assert_eq!(
            depth(Some(Duration::from_secs(6)), PIECE, None, SECONDS, 0),
            FLOOR
        );
    }

    /// A piece plays for 1.18 s; a six-second median needs the split to
    /// start six pieces ahead -- 5.1, rounded up -- and a median under a
    /// piece of playback stays at the floor.
    #[test]
    fn the_median_is_measured_in_pieces_of_playback_rounded_up() {
        assert_eq!(
            depth(Some(Duration::from_secs(6)), PIECE, Some(RATE), SECONDS, 0),
            6
        );
        assert_eq!(
            depth(
                Some(Duration::from_millis(1_100)),
                PIECE,
                Some(RATE),
                SECONDS,
                0
            ),
            FLOOR
        );
        assert_eq!(
            depth(
                Some(Duration::from_millis(1_200)),
                PIECE,
                Some(RATE),
                SECONDS,
                0
            ),
            2
        );
        assert_eq!(
            depth(
                Some(Duration::from_millis(2_400)),
                PIECE,
                Some(RATE),
                SECONDS,
                0
            ),
            3
        );
    }

    /// Each stall is one more piece, on top of whatever the median says --
    /// with or without a bitrate to say it.
    #[test]
    fn every_stall_is_one_more_piece() {
        assert_eq!(depth(None, PIECE, None, SECONDS, 3), FLOOR + 3);
        assert_eq!(
            depth(Some(Duration::from_secs(6)), PIECE, Some(RATE), SECONDS, 2),
            8
        );
    }

    /// Ninety seconds of the film is 321 MB, 77 pieces: the depth cannot go
    /// past that however many stalls, and a lookahead shorter than the
    /// floor is the lookahead.
    #[test]
    fn the_lookahead_is_the_ceiling() {
        assert_eq!(depth(None, PIECE, Some(RATE), SECONDS, 100), 77);
        assert_eq!(
            depth(
                Some(Duration::from_secs(600)),
                PIECE,
                Some(RATE),
                SECONDS,
                0
            ),
            77
        );
        assert_eq!(depth(None, PIECE, Some(RATE), 1, 0), 1);
        // No bitrate, no ceiling: the stalls stand.
        assert_eq!(depth(None, PIECE, None, 1, 100), FLOOR + 100);
    }

    /// The counter resets on a new player and the depth applies on change
    /// and on the first reading.
    #[test]
    fn stalls_are_the_videos_and_a_depth_applies_when_it_changes() {
        let mut state = DeadlineDepth::default();
        assert_eq!(state.stalls(), 0);
        state.stalled();
        state.stalled();
        assert_eq!(state.stalls(), 2);
        state.opened();
        assert_eq!(state.stalls(), 0);

        assert_eq!(state.settle(2), Some(2), "the first reading is applied");
        assert_eq!(state.settle(2), None, "the same depth again is not");
        assert_eq!(state.settle(3), Some(3), "a change is");
    }

    /// The depth rises to whatever is asked at once and falls one piece a
    /// pass: eight asked after three is eight; three asked after eight is
    /// seven, then six, and a reading that asks for more on the way down
    /// takes it.
    #[test]
    fn the_depth_rises_at_once_and_falls_a_piece_a_pass() {
        let mut state = DeadlineDepth::default();
        assert_eq!(state.settle(3), Some(3));
        assert_eq!(state.settle(8), Some(8), "up, freely");
        assert_eq!(state.settle(3), Some(7), "down, one");
        assert_eq!(state.settle(3), Some(6));
        assert_eq!(state.settle(6), None, "what it holds is what is asked");
        assert_eq!(state.settle(9), Some(9), "and up again at once");
    }
}
