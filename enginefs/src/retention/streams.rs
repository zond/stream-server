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

use std::collections::HashMap;
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
    /// The furthest this consumer has been served, never walked back by a
    /// re-read.
    end: u64,
    /// The read before this one, which is what the next sample is measured
    /// against.
    last: Read,
    /// How many reads have joined, first included.
    reads: u32,
    /// When the last one did.
    seen: Instant,
}

/// Why a read had to start a stream of its own.
///
/// One variant, and it stays an enum because the field log needs to say
/// *that* a read started a stream and not only that the count went up: a
/// join rule too tight reports two streams having opened ten, and the two
/// readings are told apart by how often this appears.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rejected {
    /// Every stream's last read ended further than [`SAME_CONSUMER`] away.
    Outside,
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
    /// **A read joins the stream whose last read ended nearest it, within
    /// [`SAME_CONSUMER`].** The question being asked is not whether this
    /// read continues the stream -- it is whether it is the same consumer,
    /// and a consumer that re-reads, retries or scrubs back two seconds is
    /// still one playhead.
    ///
    /// An earlier rule asked instead whether the read began inside
    /// everything the stream had served and reached past all of it. That is
    /// right at the granularity the design was drawn at, a whole HTTP
    /// response, and wrong at the granularity reads actually arrive: one
    /// read is at most the 256 KiB the response asks for, and a reopened
    /// connection resumes 1.15 MB to 9.0 MB behind where we stopped
    /// sending, so its first dozen reads are entirely inside what the old
    /// one served and none of them reaches past it. The field said so
    /// plainly -- a stream served to 6,149,382,662, a reopen at
    /// 6,146,281,365 whose first read ended at 6,146,543,509, and 43
    /// streams reported on a film with two tracks.
    ///
    /// The same rule caught the other half of that log: a consumer blocked
    /// on a missing piece retries from a few bytes further back and every
    /// read ends at the same byte, so each is a strict subset of the last
    /// and each opened a stream of its own. Twenty of them, all at
    /// 23,320,330,240.
    ///
    /// Two tracks stay apart without the extends-past clause, because they
    /// are twenty gigabytes apart and [`SAME_CONSUMER`] is sixteen
    /// megabytes.
    fn observe(&mut self, reader: u64, read: Read) -> Option<Rejected> {
        // Before the join, so an expired stream cannot be resumed and a new
        // one starting where it left off is reported honestly as new.
        let now = read.returned;
        self.streams
            .retain(|stream| now.saturating_duration_since(stream.seen) < STREAM_IDLE);

        let nearest = self
            .streams
            .iter()
            .enumerate()
            .map(|(index, stream)| (index, stream.last.end.abs_diff(read.begin)))
            .filter(|(_, apart)| *apart <= SAME_CONSUMER)
            .min_by_key(|(_, apart)| *apart)
            .map(|(index, _)| index);

        if let Some(index) = nearest {
            let stream = &mut self.streams[index];
            if stream.reader != reader {
                stream.reader = reader;
            }
            // A high-water mark, never a position. A re-read or a scrub back
            // is the same consumer and belongs to this stream, but letting
            // it move the stream backwards would walk the window back over
            // ground already played.
            stream.end = stream.end.max(read.end);
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

/// How near a stream's last read another has to land to be the same
/// consumer.
///
/// Wider than the socket overhang and far narrower than a seek, which is
/// the whole of it. A reopened connection resumes where the *player*
/// stopped consuming rather than where we stopped sending, and the gap is
/// whatever we had written into the socket that it never read: 1.15 MB to
/// 9.0 MB over the session this was measured on, never once ahead. A real
/// seek is gigabytes. Sixteen megabytes sits between them with room either
/// side, and is about four seconds of this film.
///
/// It admits a scrub back of a second or two, which joins rather than
/// starting a stream. That is the right answer: the same viewer at
/// essentially the same place is one playhead, and the window should stay
/// where it is.
const SAME_CONSUMER: u64 = 16 * 1024 * 1024;

/// The detected streams of one entity, by file index.
#[derive(Debug, Default)]
pub(crate) struct Streams {
    by_file: HashMap<usize, FileStreams>,
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
    pub(crate) fn observe(&mut self, file: usize, reader: u64, read: Read) -> Option<Rejected> {
        self.by_file.entry(file).or_default().observe(reader, read)
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

    /// **A reopened connection continues the stream it resumed, from its
    /// very first read.**
    ///
    /// The case the field disproved the old rule on. A player does not
    /// resume where we stopped sending; it resumes where it stopped
    /// consuming, and the gap is what we had written into the socket that
    /// it never read -- 1.15 MB to 9.0 MB over the measured session. But a
    /// single read is at most 256 KiB, so the resuming connection's first
    /// reads are all *behind* where the old one got to, and a rule asking
    /// "does this reach past what we served?" rejects every one of them.
    /// The numbers here are a real reopen from that log, where it opened a
    /// second stream and then eleven more.
    #[test]
    fn a_reopen_behind_our_send_position_continues_the_stream_at_once() {
        let t0 = Instant::now();
        let mut streams = FileStreams::default();

        // Served to 6,149,382,662 on one connection, 256 KiB at a time.
        assert_eq!(
            streams.observe(1, read(6_149_120_518, 6_149_382_662, t0, 0)),
            Some(Rejected::Outside)
        );
        // The player reopens 3.1 MB behind that. Its first read ends well
        // short of where we had got to.
        assert_eq!(
            streams.observe(2, read(6_146_281_365, 6_146_543_509, t0, 1)),
            None,
            "the same consumer, three megabytes back, on its first read"
        );

        assert_eq!(streams.streams.len(), 1);
        assert_eq!(
            streams.streams[0].end, 6_149_382_662,
            "and the stream's reach did not walk backwards with it"
        );
    }

    /// **A consumer stuck on a missing piece is one stream, not one per
    /// retry.**
    ///
    /// Every read ends at the same byte -- the boundary of the piece it is
    /// blocked on -- while its start creeps forward a few dozen bytes per
    /// attempt, so each read is a strict subset of the one before. Asking
    /// whether a read reaches past what the stream served makes every
    /// attempt a new stream: the field log has twenty of them, all ending
    /// at 23,320,330,240.
    #[test]
    fn a_consumer_retrying_a_blocked_piece_stays_one_stream() {
        let t0 = Instant::now();
        let mut streams = FileStreams::default();
        let boundary = 23_320_330_240;

        for (attempt, begin) in [
            23_320_306_052u64,
            23_320_306_078,
            23_320_306_166,
            23_320_306_260,
            23_320_306_321,
        ]
        .into_iter()
        .enumerate()
        {
            let outcome = streams.observe(2, read(begin, boundary, t0, attempt as u64));
            if attempt == 0 {
                assert_eq!(outcome, Some(Rejected::Outside), "the first one is new");
            } else {
                assert_eq!(outcome, None, "every retry after it is the same consumer");
            }
        }

        assert_eq!(streams.streams.len(), 1);
        assert_eq!(streams.streams[0].reads, 5);
    }

    /// **Two tracks twenty gigabytes apart are two streams.**
    ///
    /// What the whole design is for, and it needs no clause of its own:
    /// they are further apart than any consumer can be from itself.
    #[test]
    fn a_second_track_far_from_the_film_is_its_own_stream() {
        let t0 = Instant::now();
        let mut streams = FileStreams::default();

        streams.observe(1, read(6_149_120_518, 6_149_382_662, t0, 0));
        assert_eq!(
            streams.observe(2, read(23_320_289_113, 23_320_330_240, t0, 1)),
            Some(Rejected::Outside),
            "twenty gigabytes is not the same playhead"
        );

        assert_eq!(streams.streams.len(), 2, "two tracks, two streams");
    }

    /// A real seek is a new stream; a scrub of a second or two is not.
    ///
    /// The tolerance has one job, to sit between the socket overhang and a
    /// seek. Four seconds of this film is inside it and deliberately so --
    /// the same viewer at essentially the same place is one playhead, and
    /// the window should stay where it is.
    #[test]
    fn a_seek_starts_a_stream_and_a_scrub_does_not() {
        let t0 = Instant::now();
        let mut streams = FileStreams::default();
        streams.observe(1, read(6_000_000_000, 6_000_262_144, t0, 0));

        assert_eq!(
            streams.observe(1, read(5_998_000_000, 5_998_262_144, t0, 1)),
            None,
            "two megabytes back is the same viewer"
        );
        assert_eq!(
            streams.observe(2, read(1_000_000_000, 1_000_262_144, t0, 2)),
            Some(Rejected::Outside),
            "five gigabytes back is a seek"
        );
        assert_eq!(streams.streams.len(), 2);
    }

    /// **Nearness is to the stream's last read, not to how far it ever
    /// got.**
    ///
    /// The two are the same until a consumer works behind its own reach,
    /// which is what a reopen and a scrub both do. A viewer scrubbing back
    /// twice over is one viewer: measured from the last read each step is
    /// small, measured from the high-water mark they accumulate, and the
    /// second step lands outside a tolerance the first was well inside.
    #[test]
    fn nearness_is_to_the_last_read_and_not_to_the_high_water_mark() {
        let t0 = Instant::now();
        let mut streams = FileStreams::default();
        streams.observe(1, read(6_200_000_000, 6_200_262_144, t0, 0));

        // Back fifteen megabytes: inside the tolerance either way.
        assert_eq!(
            streams.observe(1, read(6_185_000_000, 6_185_262_144, t0, 1)),
            None
        );
        // And fifteen more. Thirty from where the stream reached, fifteen
        // from where it actually is.
        assert_eq!(
            streams.observe(1, read(6_170_000_000, 6_170_262_144, t0, 2)),
            None,
            "still the same viewer, however far the stream once got"
        );

        assert_eq!(streams.streams.len(), 1);
        assert_eq!(
            streams.streams[0].end, 6_200_262_144,
            "and its reach is still its reach"
        );
    }

    /// Reads within one connection are exactly contiguous -- `ReadCursor`
    /// advances by what each delivered -- so the ordinary case joins with
    /// nothing between them at all.
    #[test]
    fn consecutive_reads_of_one_connection_are_one_stream() {
        let t0 = Instant::now();
        let mut streams = FileStreams::default();
        streams.observe(7, read(0, 262_144, t0, 0));
        for chunk in 1..4u64 {
            assert_eq!(
                streams.observe(7, read(chunk * 262_144, (chunk + 1) * 262_144, t0, chunk)),
                None
            );
        }

        assert_eq!(streams.streams.len(), 1);
        assert_eq!(streams.streams[0].reads, 4);
        assert_eq!(streams.streams[0].end, 4 * 262_144);
    }

    /// The report is throttled on the detector's own clock, not on a global
    /// one: a test must be able to drive it without sleeping, and two
    /// entities reporting must not silence each other.
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
        let mut streams = Streams::default();
        streams.observe(0, 1, read(0, 262_144, t0, 0));
        streams.observe(1, 2, read(0, 262_144, t0, 1));

        assert_eq!(streams.counts(), vec![(0, 1), (1, 1)]);
    }
}
