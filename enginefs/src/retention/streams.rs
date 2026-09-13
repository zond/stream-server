//! **What is reading this file, worked out from the reads themselves.**
//!
//! The retention layer decides what to keep and what to fetch from a
//! *classification*: [`Reading`] from a [`PlaybackIntent`], derived by
//! `playback_intent_for_request` from a priority header, two download flags
//! and the geometry of a `Range`. A player states none of that. It sends a
//! byte range, and every field failure this module exists to end has been
//! that derivation guessing wrong -- a live second track labelled container
//! metadata and starved for twenty seconds while seventeen seeders were
//! connected, a read fifteen megabytes inside `mdat` taken for the
//! container index.
//!
//! Nothing here asks what a read means. It watches where reads go.
//!
//! **Phase A: this module is observed and never obeyed.** Nothing reads a
//! [`Stream`] to decide anything; the one consumer is a trace line, and the
//! question it exists to answer is whether the join rule below sees two
//! streams on a film with two tracks, or twenty-two -- one per HTTP
//! reopen -- or one, having merged the tracks. See
//! `docs/read-pattern-retention.md`, which this implements and which says
//! what the later phases do with the answer.
//!
//! [`Reading`]: super::owner::Reading
//! [`PlaybackIntent`]: crate::backend::priorities::PlaybackIntent

use std::collections::{BTreeSet, HashMap};
use std::ops::Range;
use std::time::Instant;

/// One read that was served, as the detector sees it.
///
/// `arrived` and `returned` are both here because the gap that matters runs
/// from one read's return to the *next* read's arrival, and no other
/// pairing works: our own fetch latency sits between a read's arrival and
/// its return, so any measurement that spans it books a stall on a missing
/// piece as the consumer thinking. A stream blocked on a piece would then
/// measure as slow, be given a smaller window, and stay blocked.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Read {
    /// The offset this read ran from.
    pub begin: u64,
    /// The offset it reached.
    pub end: u64,
    /// When the consumer asked for it.
    pub arrived: Instant,
    /// When we finished serving it.
    pub returned: Instant,
}

impl Read {
    fn size(&self) -> u64 {
        self.end.saturating_sub(self.begin)
    }
}

/// A consumer moving through one file.
///
/// Not a connection. A player reopens its connection on every seek -- one
/// field session did it twenty-two times in two minutes, alternating
/// between the film and a second track twenty gigabytes away -- so a stream
/// keyed to a connection would be twenty-two streams where there are two.
/// What makes it one stream is that the reads continue each other.
#[derive(Debug)]
struct Stream {
    /// Which response is feeding it now. A change is a reopen.
    reader: u64,
    /// Where this consumer is: the end of its last read. A scrub back
    /// moves it back, because that is where the viewer now is and where a
    /// window would belong.
    end: u64,
    /// The read before this one, which is what the next sample is measured
    /// against.
    last: Read,
    /// How many reads have joined, first included.
    reads: u32,
    /// When the last one did.
    seen: Instant,
    /// How fast this consumer is eating the file, in bytes a second, or
    /// `None` until two reads have been served far enough apart to say.
    rate: Option<u64>,
}

impl Stream {
    /// Fold what this consumer ate since the last read into its rate.
    ///
    /// **What it ate, not what we sent.** A player resumes where it stopped
    /// consuming, which is behind where we stopped sending by whatever sat
    /// unread in the socket -- 1.15 MB to 9.0 MB over the measured session.
    /// Crediting the whole of the last read overstates the rate by that
    /// much: on one real reopen, 21,757,952 bytes served against
    /// 18,374,055 actually eaten, an 18% error.
    ///
    /// **And the gap runs from our return to its next arrival.** Any other
    /// pairing puts our own fetch latency inside it, so a consumer blocked
    /// on a missing piece measures as slow, is given a smaller window, and
    /// stays blocked. This pairing cannot: our stall happens after the read
    /// has arrived.
    ///
    /// Two samples are refused rather than folded. A gap of zero divides by
    /// nothing -- `consumed as f64 / 0.0` is `inf`, and `inf as u64`
    /// saturates to `u64::MAX`, which would read as a real measurement of
    /// an impossibly fast consumer. And a read that consumed nothing is a
    /// reopen landing behind, or a re-read of ground already served; it
    /// says where the consumer is, not how fast it is going, and folded in
    /// as a zero it would drag the rate to the floor on every seek.
    fn sample(&mut self, read: &Read) {
        let overlap = self
            .last
            .end
            .saturating_sub(read.begin)
            .min(self.last.size());
        let consumed = self.last.size().saturating_sub(overlap);
        let gap = read.arrived.saturating_duration_since(self.last.returned);
        if consumed == 0 || gap.is_zero() {
            return;
        }
        let sample = (consumed as f64 / gap.as_secs_f64()) as u64;
        self.rate = Some(match self.rate {
            None => sample,
            Some(rate) => {
                (rate * (8 - RATE_SMOOTHING_EIGHTHS) + sample * RATE_SMOOTHING_EIGHTHS) / 8
            }
        });
    }
}

/// Why a read had to start a stream of its own.
///
/// One variant, and it stays an enum because the field log needs to say
/// *that* a read started a stream and not only that the count went up: a
/// join rule too tight reports two streams having opened ten, and the two
/// readings are told apart by how often this appears.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rejected {
    /// The read began in no run this file's streams are in -- a different
    /// stretch of disk, or a piece the disk does not hold at all.
    Outside,
}

/// How much of a new sample the rate takes, in eighths.
///
/// A player's reads are bursty -- it drains as fast as the socket allows
/// until its own buffer is full, then asks only as often as it plays -- so
/// a rate that followed each sample would swing between the link's speed
/// and the film's. Seven eighths of the old number and one of the new
/// settles over roughly a dozen reads, which at fourteen reads a second is
/// under a second of film.
const RATE_SMOOTHING_EIGHTHS: u64 = 1;

/// The maximal unbroken stretch of `held` containing `piece`, inside
/// `bound`, or `None` for a piece the disk does not have.
///
/// A plain set rather than the piece store's own bitfield, because the
/// proxy's held set is a set of chunk indices and the rule is the same for
/// both: what a consumer may reach back to is however much of the disk was
/// kept behind it.
fn run_containing(held: &BTreeSet<u32>, piece: u32, bound: &Range<u32>) -> Option<Range<u32>> {
    if !bound.contains(&piece) || !held.contains(&piece) {
        return None;
    }
    let mut start = piece;
    while start > bound.start && held.contains(&(start - 1)) {
        start -= 1;
    }
    let mut end = piece.saturating_add(1);
    while end < bound.end && held.contains(&end) {
        end = end.saturating_add(1);
    }
    Some(start..end)
}

/// Every stream detected on one file.
#[derive(Debug, Default)]
pub(crate) struct FileStreams {
    streams: Vec<Stream>,
}

impl FileStreams {
    /// Take account of one served read, and say why it had to start a new
    /// stream when it did.
    ///
    /// **A read joins the stream that is in the same unbroken stretch of
    /// disk it is.** A consumer *is* the run of bytes it caused to be
    /// there, so two reads belong to one consumer exactly when the disk
    /// between them is whole, and a hole an eviction left is what ends one.
    ///
    /// Which makes the tolerance a measurement rather than a choice, and
    /// scales it the right way: a cache with room holds a long run, so a
    /// scrub back lands inside it and is the same viewer; a cache under
    /// pressure holds a short one, so the same scrub lands outside and is a
    /// new consumer that has to be fetched for. Bigger disks behave better,
    /// with no number anywhere.
    ///
    /// Three rules preceded it, each guessing that distance in bytes, each
    /// wrong in a way the last one could not have predicted. The span of
    /// everything a stream had served merged two tracks as soon as one
    /// connection ran long enough to span them. Sixteen megabytes from the
    /// last read is four and a half seconds of one film and thirty-two of
    /// another. What a single connection had served grows without bound on
    /// a connection nobody closes. See `docs/read-pattern-retention.md`.
    ///
    /// The read's **begin** is what is looked up, not its end. A consumer
    /// blocked on a missing piece serves up to the hole and stops, so its
    /// end is the first byte it does *not* have -- in no run by definition,
    /// and the reason the field showed twenty separate streams all ending
    /// at 23,320,330,240. Where it started is where it was.
    fn observe(
        &mut self,
        reader: u64,
        read: Read,
        held: &BTreeSet<u32>,
        bound: &Range<u32>,
        piece: u64,
    ) -> Option<Rejected> {
        // Before the join, so an expired stream cannot be resumed and a new
        // one starting where it left off is reported honestly as new.
        let now = read.returned;
        self.streams
            .retain(|stream| now.saturating_duration_since(stream.seen) < STREAM_IDLE);

        let at = |offset: u64| u32::try_from(offset / piece.max(1)).unwrap_or(u32::MAX);
        let run = run_containing(held, at(read.begin), bound);
        let nearest = run.and_then(|run| {
            self.streams
                .iter()
                .position(|stream| run.contains(&at(stream.end.saturating_sub(1))))
        });

        if let Some(index) = nearest {
            let stream = &mut self.streams[index];
            stream.sample(&read);
            if stream.reader != reader {
                stream.reader = reader;
            }
            // Wherever the last read ended, backwards included. A scrub
            // back is not ground already played -- it is ground about to be
            // played again, and a window belongs where the viewer is. The
            // cost is a transient demuxer re-read dragging the position a
            // few hundred kilobytes back until the next read jumps forward;
            // the two readings can never differ by more than
            // `SAME_CONSUMER`, since further than that is a stream of its
            // own.
            stream.end = read.end;
            stream.last = read;
            stream.reads = stream.reads.saturating_add(1);
            stream.seen = read.returned;
            return None;
        }

        self.streams.push(Stream {
            reader,
            end: read.end,
            last: read,
            reads: 1,
            seen: read.returned,
            rate: None,
        });
        Some(Rejected::Outside)
    }
}

/// How often the detector says what it has found.
///
/// The diagnostics report a tester sends back is a 400-line ring, and every
/// line this costs is one of somebody else's. Ten seconds is 0.1 lines a
/// second, against the five-second `stream_progress` line that is per open
/// response; the streams being looked for form over tens of seconds and
/// last for minutes, so nothing is missed by not saying it sooner.
const REPORT_EVERY: std::time::Duration = std::time::Duration::from_secs(10);

/// How long a stream nothing has read from stays a stream.
///
/// A placeholder the field log is meant to inform, like every constant in
/// `docs/read-pattern-retention.md`. Too short and a slow second track --
/// one measured at roughly 20 kB/s, reading forty kilobytes at a time --
/// expires between its own reads and is counted again and again, which
/// looks exactly like a join rule that is too tight. Too long and a
/// response that ended minutes ago is still reported as something being
/// read. Thirty seconds is longer than any gap that track left and shorter
/// than a viewer's pause.
const STREAM_IDLE: std::time::Duration = std::time::Duration::from_secs(30);

/// The detected streams of one entity, by file index.
#[derive(Debug, Default)]
pub(crate) struct Streams {
    by_file: HashMap<usize, FileStreams>,
    /// Reads served since the last pass, awaiting a listing to be answered
    /// against. A read carries its own timestamps, so waiting costs the
    /// answer nothing.
    pending: Vec<(usize, u64, Read)>,
    /// When the last report went out, or `None` having said nothing yet.
    reported: Option<Instant>,
}

impl Streams {
    /// Whether a report is due, marking it sent when it is.
    ///
    /// The throttle is here, with the clock passed in, rather than in a
    /// process-global map beside the trace call: `trace::pass`'s `SEEN` is
    /// keyed by the entity alone, so two event types sharing it silence
    /// each other on alternate intervals, and being global wall-clock state
    /// makes it order-dependent across the tests in one binary.
    pub(crate) fn report_due(&mut self, now: Instant) -> bool {
        let due = self
            .reported
            .is_none_or(|last| now.saturating_duration_since(last) >= REPORT_EVERY);
        if due {
            self.reported = Some(now);
        }
        due
    }
    /// Keep a served read until a pass can say which consumer it belonged
    /// to. The read path cannot answer that: membership is a question about
    /// the disk, and the disk's answer is a listing away.
    pub(crate) fn record(&mut self, file: usize, reader: u64, read: Read) {
        self.pending.push((file, reader, read));
    }

    /// Answer every read kept since the last pass against `held`, and say
    /// what the most recent one had to do.
    pub(crate) fn observe(
        &mut self,
        held: &BTreeSet<u32>,
        bound: &Range<u32>,
        piece: u64,
    ) -> Option<Rejected> {
        let mut last = None;
        for (file, reader, read) in std::mem::take(&mut self.pending) {
            last = self
                .by_file
                .entry(file)
                .or_default()
                .observe(reader, read, held, bound, piece);
        }
        last
    }

    /// How many streams are open on each file, for the trace line.
    pub(crate) fn counts(&self) -> Vec<(usize, usize)> {
        let mut counts: Vec<(usize, usize)> = self
            .by_file
            .iter()
            .map(|(file, streams)| (*file, streams.streams.len()))
            .collect();
        counts.sort_unstable();
        counts
    }

    /// Where each stream on `file` has reached, and how many reads took it
    /// there -- what a field log needs to say whether the two it found are
    /// the film and a second track, or two halves of the film.
    pub(crate) fn heads(&self, file: usize) -> Vec<(u64, u32)> {
        self.by_file
            .get(&file)
            .map(|streams| {
                streams
                    .streams
                    .iter()
                    .map(|stream| (stream.end, stream.reads))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// What each stream on `file` has measured its consumer to be eating,
    /// in bytes a second, or `None` for one that has not had two reads far
    /// enough apart to say.
    ///
    /// Reported beside the raw `consumed`/`gap_ms` of the last sample, not
    /// instead of them: at this seam a read is at most the 256 KiB the
    /// response asks for, so a rate could be measuring the socket draining
    /// while a player fills its buffer rather than the player consuming.
    /// Nothing is sized from this until a field log says which it is.
    pub(crate) fn rates(&self, file: usize) -> Vec<Option<u64>> {
        self.by_file
            .get(&file)
            .map(|streams| streams.streams.iter().map(|stream| stream.rate).collect())
            .unwrap_or_default()
    }

    /// What the last read of `file`'s newest stream consumed, and over how
    /// long -- the two halves of a rate sample, reported raw so a reading
    /// of the field log can check the arithmetic before anything is sized
    /// from it.
    ///
    /// Deliberately not an average. At this seam one read is at most the
    /// 256 KiB `ReaderStream` asks for, and within a connection consecutive
    /// reads are exactly contiguous, so `consumed` is the whole read and
    /// the gap is however long the socket took -- which is a delivery rate
    /// while the player is filling its buffer, and its true consumption
    /// only once that buffer is full and TCP backpressure sets the pace.
    /// Phase A's job is to find out which of those the numbers look like.
    pub(crate) fn last_sample(&self, file: usize) -> Option<(u64, std::time::Duration)> {
        let streams = &self.by_file.get(&file)?.streams;
        let stream = streams.last()?;
        Some((
            stream.last.size(),
            stream.last.returned - stream.last.arrived,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// The field film's geometry: 4 MiB pieces over 23,346,250,742 bytes.
    const PIECE: u64 = 4 * 1024 * 1024;
    const PIECES: u32 = 5_566;

    fn whole() -> Range<u32> {
        0..PIECES
    }

    /// A disk holding every piece of `runs` and nothing else.
    fn disk(runs: &[Range<u32>]) -> BTreeSet<u32> {
        runs.iter().flat_map(|run| run.clone()).collect()
    }

    /// A disk holding one unbroken run.
    fn run(pieces: Range<u32>) -> BTreeSet<u32> {
        pieces.collect()
    }

    fn at(t0: Instant, secs: u64) -> Instant {
        t0 + Duration::from_secs(secs)
    }

    fn read(begin: u64, end: u64, t0: Instant, secs: u64) -> Read {
        Read {
            begin,
            end,
            arrived: at(t0, secs),
            returned: at(t0, secs),
        }
    }

    /// **A reopened connection continues the stream it resumed**, however
    /// far behind it lands, so long as the disk between is whole.
    ///
    /// The numbers are a real reopen from the field: a stream served to
    /// 6,149,382,662 and a connection resuming 3.1 MB behind it, whose
    /// first read ends well short of where the old one got to. Three
    /// earlier rules each judged that distance and each was wrong about it;
    /// this one does not judge it at all, it asks whether the bytes in
    /// between are there.
    #[test]
    fn a_reopen_behind_our_send_position_continues_the_stream() {
        let t0 = Instant::now();
        let held = run(1_460..1_470);
        let mut streams = FileStreams::default();

        assert_eq!(
            streams.observe(
                1,
                read(6_149_120_518, 6_149_382_662, t0, 0),
                &held,
                &whole(),
                PIECE
            ),
            Some(Rejected::Outside),
            "the first read of a session joins nothing"
        );
        assert_eq!(
            streams.observe(
                2,
                read(6_146_281_365, 6_146_543_509, t0, 1),
                &held,
                &whole(),
                PIECE
            ),
            None,
            "and the reopen behind it is the same consumer: the disk is whole between them"
        );
        assert_eq!(streams.streams.len(), 1);
    }

    /// **A hole in the disk is what ends a stream**, and it is the only
    /// thing that does.
    ///
    /// The same two reads, on a disk an eviction has broken between them.
    /// Nothing about the consumer changed; what changed is what was kept,
    /// and that is the whole of the rule. It is also why a big cache
    /// behaves better than a small one with no number anywhere: more room
    /// means longer runs means a scrub back is still the same viewer.
    #[test]
    fn a_hole_between_two_reads_makes_them_two_consumers() {
        let t0 = Instant::now();
        // The reads are in pieces 1466 and 1465; the eviction took 1465
        // itself, which is both the hole and where the reopen lands.
        let held = disk(&[1_460..1_465, 1_466..1_470]);
        let mut streams = FileStreams::default();

        streams.observe(
            1,
            read(6_149_120_518, 6_149_382_662, t0, 0),
            &held,
            &whole(),
            PIECE,
        );
        assert_eq!(
            streams.observe(
                2,
                read(6_146_281_365, 6_146_543_509, t0, 1),
                &held,
                &whole(),
                PIECE
            ),
            Some(Rejected::Outside),
            "the run the reopen landed in is not the run the stream is in"
        );
        assert_eq!(streams.streams.len(), 2);
    }

    /// **A consumer stuck on a missing piece is one stream, not one per
    /// retry.**
    ///
    /// Every read ends at the same byte -- the boundary of the piece it is
    /// blocked on -- while its start creeps forward a few dozen bytes an
    /// attempt. The field showed twenty streams there, all ending at
    /// 23,320,330,240. What the rule looks up is where a read *began*,
    /// which is in the run; where it ended is the first byte it does not
    /// have, which is in no run by definition.
    #[test]
    fn a_consumer_retrying_a_blocked_piece_stays_one_stream() {
        let t0 = Instant::now();
        // Held up to the boundary of piece 5560, and not beyond.
        let held = run(5_556..5_560);
        let boundary = 23_320_330_240;
        let mut streams = FileStreams::default();

        for (attempt, begin) in [
            23_320_306_052u64,
            23_320_306_078,
            23_320_306_166,
            23_320_306_260,
        ]
        .into_iter()
        .enumerate()
        {
            let outcome = streams.observe(
                2,
                read(begin, boundary, t0, attempt as u64),
                &held,
                &whole(),
                PIECE,
            );
            if attempt == 0 {
                assert_eq!(outcome, Some(Rejected::Outside), "the first one is new");
            } else {
                assert_eq!(outcome, None, "every retry after it is the same consumer");
            }
        }
        assert_eq!(streams.streams.len(), 1);
        assert_eq!(streams.streams[0].reads, 4);
    }

    /// **Two tracks are two streams**, and need no clause of their own: the
    /// film's run and the second track's are different runs.
    #[test]
    fn a_second_track_in_another_run_is_its_own_stream() {
        let t0 = Instant::now();
        let held = disk(&[1_460..1_470, 5_556..5_560]);
        let mut streams = FileStreams::default();

        streams.observe(
            1,
            read(6_149_120_518, 6_149_382_662, t0, 0),
            &held,
            &whole(),
            PIECE,
        );
        assert_eq!(
            streams.observe(
                2,
                read(23_320_289_113, 23_320_330_240, t0, 1),
                &held,
                &whole(),
                PIECE
            ),
            Some(Rejected::Outside),
            "twenty gigabytes away, and a different stretch of disk"
        );
        assert_eq!(streams.streams.len(), 2, "two tracks, two streams");
    }

    /// A read into territory the disk does not hold is a new consumer: it
    /// is in no run, so it is in no stream, and something has to fetch for
    /// it.
    #[test]
    fn a_read_of_what_we_do_not_hold_is_a_new_consumer() {
        let t0 = Instant::now();
        let held = run(1_460..1_470);
        let mut streams = FileStreams::default();
        streams.observe(
            1,
            read(6_149_120_518, 6_149_382_662, t0, 0),
            &held,
            &whole(),
            PIECE,
        );

        assert_eq!(
            streams.observe(
                1,
                read(1_000_000_000, 1_000_262_144, t0, 1),
                &held,
                &whole(),
                PIECE
            ),
            Some(Rejected::Outside)
        );
        assert_eq!(streams.streams.len(), 2);
    }

    /// Reads within one connection are exactly contiguous -- `ReadCursor`
    /// advances by what each delivered -- and all inside one run.
    #[test]
    fn consecutive_reads_of_one_connection_are_one_stream() {
        let t0 = Instant::now();
        let held = run(0..4);
        let mut streams = FileStreams::default();
        streams.observe(7, read(0, 262_144, t0, 0), &held, &whole(), PIECE);
        for chunk in 1..4u64 {
            assert_eq!(
                streams.observe(
                    7,
                    read(chunk * 262_144, (chunk + 1) * 262_144, t0, chunk),
                    &held,
                    &whole(),
                    PIECE
                ),
                None
            );
        }
        assert_eq!(streams.streams.len(), 1);
        assert_eq!(streams.streams[0].reads, 4);
    }

    /// **The rate is what the consumer ate, over the time it took to come
    /// back for more.**
    ///
    /// The numbers are a real reopen from the field: 21,757,952 bytes
    /// served, a resume 3,383,897 behind our send position, so 18,374,055
    /// actually eaten. Crediting the whole read would overstate it by 18%.
    #[test]
    fn the_rate_credits_what_was_eaten_and_not_what_was_sent() {
        let t0 = Instant::now();
        // Both reads and the stream's own end are inside one run.
        let held = run(1_460..1_475);
        let mut streams = FileStreams::default();

        // One read of 21,757,952 bytes, returned at t0.
        streams.observe(
            1,
            Read {
                begin: 6_144_401_926,
                end: 6_166_159_878,
                arrived: t0,
                returned: t0,
            },
            &held,
            &whole(),
            PIECE,
        );
        // The consumer comes back one second later, 3,383,897 behind.
        streams.observe(
            1,
            Read {
                begin: 6_162_775_981,
                end: 6_163_038_125,
                arrived: at(t0, 1),
                returned: at(t0, 1),
            },
            &held,
            &whole(),
            PIECE,
        );

        assert_eq!(
            streams.streams[0].rate,
            Some(18_374_055),
            "what it ate in the second it took to ask again"
        );
    }

    /// A sample that measures nothing is refused rather than folded in.
    ///
    /// Two shapes of nothing. A gap of zero divides by it -- and `inf as
    /// u64` saturates to `u64::MAX` in Rust, which would read as a real
    /// measurement rather than an error. And a read that consumed nothing
    /// is a reopen landing behind or a re-read of ground already served: it
    /// says where the consumer is, not how fast it is going, and folded in
    /// as a zero it would drag the rate to the floor on every seek.
    #[test]
    fn a_sample_that_measures_nothing_is_refused() {
        let t0 = Instant::now();
        let held = run(0..8);
        let mut streams = FileStreams::default();

        streams.observe(1, read(0, 262_144, t0, 0), &held, &whole(), PIECE);
        // Same instant: no time passed.
        streams.observe(1, read(262_144, 524_288, t0, 0), &held, &whole(), PIECE);
        assert_eq!(streams.streams[0].rate, None, "a gap of zero says nothing");

        // A second later, but landing entirely behind what the last read
        // served: nothing was consumed.
        streams.observe(1, read(0, 262_144, t0, 1), &held, &whole(), PIECE);
        assert_eq!(
            streams.streams[0].rate, None,
            "and a read that ate nothing says nothing either"
        );
    }

    /// **A stream nothing has read from stops being one.**
    ///
    /// Without it a two-hour film accumulates a stream per seek for the
    /// whole session, and the count the field log is read for stops meaning
    /// anything. Pruned before the join, so an expired stream cannot be
    /// resumed by a read that happens to land in its old run.
    #[test]
    fn a_stream_nothing_has_read_from_stops_being_one() {
        let t0 = Instant::now();
        let held = disk(&[0..4, 2_000..2_004]);
        let mut streams = FileStreams::default();
        streams.observe(1, read(0, 262_144, t0, 0), &held, &whole(), PIECE);
        streams.observe(
            2,
            read(8_388_608_000, 8_388_870_144, t0, 1),
            &held,
            &whole(),
            PIECE,
        );
        assert_eq!(streams.streams.len(), 2);

        assert_eq!(
            streams.observe(1, read(262_144, 524_288, t0, 20), &held, &whole(), PIECE),
            None
        );
        assert_eq!(streams.streams.len(), 2, "twenty seconds is not idle yet");

        assert_eq!(
            streams.observe(1, read(524_288, 786_432, t0, 40), &held, &whole(), PIECE),
            None
        );
        assert_eq!(
            streams.streams.len(),
            1,
            "the one that stopped reading is gone; the one that did not is not"
        );
    }

    /// The report is throttled on the detector's own clock, not a global
    /// one: a test must drive it without sleeping, and two entities
    /// reporting must not silence each other.
    #[test]
    fn a_report_waits_for_its_interval() {
        let t0 = Instant::now();
        let mut streams = Streams::default();

        assert!(
            streams.report_due(t0),
            "the first one has nothing to wait for"
        );
        assert!(!streams.report_due(at(t0, 5)));
        assert!(streams.report_due(at(t0, 10)));
        assert!(!streams.report_due(at(t0, 11)));
    }

    /// Files do not share streams: the same offsets in two files are two
    /// consumers, and a table keyed by file is what says so.
    #[test]
    fn two_files_do_not_share_a_stream() {
        let t0 = Instant::now();
        let held = run(0..4);
        let mut streams = Streams::default();
        streams.record(0, 1, read(0, 262_144, t0, 0));
        streams.record(1, 2, read(0, 262_144, t0, 1));
        streams.observe(&held, &whole(), PIECE);

        assert_eq!(streams.counts(), vec![(0, 1), (1, 1)]);
    }
}
