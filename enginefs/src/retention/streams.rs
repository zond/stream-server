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
    /// Which response is feeding it now. A change is a reopen, and resets
    /// `begin`: without that, one connection served without a seek grows
    /// the span to the whole file and every read in it starts "inside".
    reader: u64,
    /// Where the current connection began serving.
    begin: u64,
    /// How far we have served it.
    end: u64,
    /// The read before this one, which is what the next sample is measured
    /// against.
    last: Read,
    /// How many reads have joined, first included.
    reads: u32,
    /// When the last one did.
    seen: Instant,
}

/// Why a read did not join the stream it came nearest to.
///
/// Recorded because the failure this module can have is invisible in a
/// count: a join rule that is too tight reports two streams and opens ten,
/// and a join rule that is too loose reports two streams that are not the
/// two tracks. Which clause rejected the nearest candidate says which.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rejected {
    /// No stream had served the offset it began at.
    Outside,
    /// One had, and this read did not reach past what it had served -- a
    /// backward seek into territory we have already sent, or a second track
    /// whose reads sit inside a long-running stream's span.
    DidNotExtend,
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
    /// A read joins a stream when that stream has served the offset it
    /// begins at *and* it reaches past what that stream has served. Both
    /// halves are load-bearing. The first is why no tolerance constant is
    /// needed: a reopened connection resumes where the *consumer* stopped
    /// consuming, which is behind where we stopped sending -- by 1.15 MB to
    /// 9.0 MB in the session this was measured on, and never once ahead --
    /// so "inside what we have already served you" is exactly the window a
    /// resume lands in, and it is the stream's own span rather than a
    /// number somebody picked.
    ///
    /// The second is what keeps two tracks apart. On a connection that runs
    /// without a seek the film's span reaches the end of the file, so a
    /// second track's reads *do* begin inside it -- but they do not extend
    /// it, and neither does a backward seek. Without that clause the two
    /// playheads merge into one, which is the failure the whole design is
    /// for.
    fn observe(&mut self, reader: u64, read: Read) -> Option<Rejected> {
        // Before the join, so an expired stream cannot be resumed and a new
        // one starting where it left off is reported honestly as new.
        let now = read.returned;
        self.streams
            .retain(|stream| now.saturating_duration_since(stream.seen) < STREAM_IDLE);

        let mut nearest: Option<(usize, u64)> = None;
        let mut inside_any = false;
        for (index, stream) in self.streams.iter().enumerate() {
            if !(stream.begin <= read.begin && read.begin <= stream.end) {
                continue;
            }
            inside_any = true;
            if read.end <= stream.end {
                continue;
            }
            // Nearest by how little of what we served it skipped back over.
            let distance = stream.end.saturating_sub(read.begin);
            if nearest.is_none_or(|(_, best)| distance < best) {
                nearest = Some((index, distance));
            }
        }

        if let Some((index, _)) = nearest {
            let stream = &mut self.streams[index];
            if stream.reader != reader {
                stream.reader = reader;
                stream.begin = read.begin;
            }
            stream.end = read.end;
            stream.last = read;
            stream.reads = stream.reads.saturating_add(1);
            stream.seen = read.returned;
            return None;
        }

        self.streams.push(Stream {
            reader,
            begin: read.begin,
            end: read.end,
            last: read,
            reads: 1,
            seen: read.returned,
        });
        Some(if inside_any {
            Rejected::DidNotExtend
        } else {
            Rejected::Outside
        })
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

    /// **A reopened connection continues the stream it resumed.**
    ///
    /// The player does not resume where we stopped sending; it resumes
    /// where it stopped consuming, and the difference is what we had
    /// written into the socket that it never read. Measured over one field
    /// session every reopen landed between 1.15 MB and 9.0 MB behind our
    /// send position and never once ahead, which is why the test is at the
    /// film's real offsets rather than at tidy small numbers: the numbers
    /// here are a real reopen from that log.
    #[test]
    fn a_reopen_behind_our_send_position_continues_the_stream() {
        let t0 = Instant::now();
        let mut streams = FileStreams::default();

        // Served to 3,478,455,795 on one connection.
        assert_eq!(
            streams.observe(1, read(3_456_697_843, 3_478_455_795, t0, 0)),
            Some(Rejected::Outside)
        );
        // The player reopens 3.4 MB behind that, and reads past it.
        assert_eq!(
            streams.observe(2, read(3_475_071_898, 3_482_411_930, t0, 1)),
            None,
            "a resume inside what we served is the same stream"
        );

        assert_eq!(streams.streams.len(), 1);
        let stream = &streams.streams[0];
        assert_eq!(stream.end, 3_482_411_930);
        assert_eq!(stream.reads, 2);
        assert_eq!(
            stream.begin, 3_475_071_898,
            "and the new connection's span starts where it reopened"
        );
    }

    /// **A second track inside the film's span is its own stream.**
    ///
    /// The case the whole design is for. On a connection that runs without
    /// a seek the film's span reaches the end of the file, so the second
    /// track's reads twenty gigabytes in *do* begin inside it. What tells
    /// them apart is that they do not reach past it.
    #[test]
    fn a_second_track_inside_the_served_span_is_its_own_stream() {
        let t0 = Instant::now();
        let mut streams = FileStreams::default();

        // The film, served from the start to the end of a 23 GB file.
        streams.observe(1, read(0, 23_346_250_742, t0, 0));
        // The second track, reading 41 kB at 23.32 GB -- inside that span.
        assert_eq!(
            streams.observe(2, read(23_320_289_113, 23_320_330_240, t0, 1)),
            Some(Rejected::DidNotExtend),
            "it began inside the film's span, so only the second clause rejects it"
        );

        assert_eq!(streams.streams.len(), 2, "two tracks, two streams");
    }

    /// A backward seek into territory we have already sent is a new
    /// stream, by the same clause and for the same reason: it begins inside
    /// the span and goes nowhere past it.
    #[test]
    fn a_backward_seek_inside_the_served_span_starts_a_new_stream() {
        let t0 = Instant::now();
        let mut streams = FileStreams::default();
        streams.observe(1, read(0, 900_000_000, t0, 0));

        assert_eq!(
            streams.observe(2, read(100_000_000, 100_262_144, t0, 1)),
            Some(Rejected::DidNotExtend)
        );
        assert_eq!(streams.streams.len(), 2);
    }

    /// A read nowhere near anything we have served is rejected by the first
    /// clause, not the second -- which is the difference between "the join
    /// rule is too tight" and "the join rule is working".
    #[test]
    fn a_read_past_every_span_says_it_was_outside_them() {
        let t0 = Instant::now();
        let mut streams = FileStreams::default();
        streams.observe(1, read(0, 262_144, t0, 0));

        assert_eq!(
            streams.observe(2, read(9_000_000_000, 9_000_262_144, t0, 1)),
            Some(Rejected::Outside)
        );
    }

    /// **A read that begins before anything we served is not a resume, even
    /// when it ends past one.**
    ///
    /// The lower half of the join's first clause, and it is reachable: a
    /// reopen resets the span to that one read, so a stream is briefly only
    /// one read wide, and a player that then seeks back a little and reads
    /// a full chunk would begin before the span and end after it. It has
    /// not continued anything -- it began somewhere we never sent -- and
    /// the clause that catches a backward seek does not catch this one,
    /// because this one does extend.
    #[test]
    fn a_read_beginning_before_the_span_is_not_a_resume_though_it_ends_past_it() {
        let t0 = Instant::now();
        let mut streams = FileStreams::default();

        // A reopen, so the span is exactly the one read it just served.
        streams.observe(1, read(1_000_000, 1_262_144, t0, 0));
        assert_eq!(streams.streams[0].begin, 1_000_000);

        assert_eq!(
            streams.observe(2, read(900_000, 1_300_000, t0, 1)),
            Some(Rejected::Outside),
            "it started where we had sent nothing, so it resumed nothing"
        );
        assert_eq!(streams.streams.len(), 2);
    }

    /// Reads within one connection are exactly contiguous -- `ReadCursor`
    /// advances by what each delivered -- so the ordinary case joins with
    /// nothing skipped back over, and does not reset the span.
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
        assert_eq!(
            streams.streams[0].begin, 0,
            "the same connection, so the span was never reset"
        );
    }

    /// **A stream nothing has read from for a while stops being one.**
    ///
    /// Without this a two-hour film accumulates a stream per seek, for
    /// ever, and the count the field log is being read for stops meaning
    /// anything. Pruned before the join so an expired stream cannot be
    /// resumed by a read that happens to land in its old span.
    #[test]
    fn a_stream_nothing_has_read_from_stops_being_one() {
        let t0 = Instant::now();
        let mut streams = FileStreams::default();
        streams.observe(1, read(0, 262_144, t0, 0));
        streams.observe(2, read(9_000_000_000, 9_000_262_144, t0, 1));
        assert_eq!(streams.streams.len(), 2);

        // One of them keeps reading; the other never does again.
        assert_eq!(streams.observe(1, read(262_144, 524_288, t0, 20)), None);
        assert_eq!(streams.streams.len(), 2, "twenty seconds is not idle yet");

        assert_eq!(streams.observe(1, read(524_288, 786_432, t0, 40)), None);
        assert_eq!(
            streams.streams.len(),
            1,
            "the one that stopped reading is gone; the one that did not is not"
        );
        assert_eq!(streams.streams[0].end, 786_432);
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
