//! **What is reading this file, worked out from the reads themselves.**
//!
//! The retention layer used to decide what to keep and what to fetch from a
//! *classification*: a `Reading` of `Playback` or `Probe`, derived from a
//! `PlaybackIntent`, derived in turn from a priority header, two download
//! flags and the geometry of a `Range`. A player states none of that. It
//! sends a byte range, and every field failure this module exists to end
//! was that derivation guessing wrong -- a live second track labelled
//! container metadata and starved for twenty seconds while seventeen
//! seeders were connected, a read fifteen megabytes inside `mdat` taken for
//! the container index.
//!
//! Nothing here asks what a read means. It watches where reads go.
//!
//! **This is what a pass obeys.** What an entity's consumers are fetched
//! for ([`Streams::want`]), what no unlink may touch ([`Streams::exempt`]),
//! what is given back first ([`Streams::coldest_of`]) and which of a file's
//! readers is the viewer ([`Streams::busiest`]) are all answered from here,
//! through `Backing::reading` on both backings. The only thing [`Fetching`]
//! is still asked is how far a stream reads ahead before a duration has
//! been stated. See `docs/read-pattern-retention.md`, which this
//! implements.
//!
//! [`Fetching`]: crate::backend::priorities::Fetching

use std::collections::{BTreeSet, HashMap};
use std::ops::Range;
use std::time::{Duration, Instant};

/// One read that was served, as the detector sees it.
///
/// `arrived` and `returned` are both here because the gap that matters runs
/// from one read's return to the *next* read's arrival, and no other
/// pairing works: our own fetch latency sits between a read's arrival and
/// its return, so any measurement that spans it books a stall on a missing
/// piece as the consumer thinking. A stream blocked on a piece would then
/// measure as slow, be given a smaller window, and stay blocked.
#[derive(Debug, Clone, Copy)]
pub struct Read {
    /// The offset this read ran from.
    pub begin: u64,
    /// The offset it reached.
    pub end: u64,
    /// When the consumer asked for it.
    pub arrived: Instant,
    /// When we finished serving it.
    pub returned: Instant,
}

/// One consumer of a file, read off its stream for the cache window.
///
/// Three facts and a position, and the window wants exactly these to say
/// whether this reader is the one about to stop the film: how much it
/// eats a second when it is eating ([`Self::rate`], the sustained figure,
/// `None` before a read has come back late enough to measure one), how
/// long since it last asked for anything ([`Self::idle`]), and what one of
/// its reads is worth ([`Self::last_read`]) -- the tolerance below which
/// "bytes in front of it" means "waiting for more" and not "has some".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reader {
    /// The last byte it read. [`Stream::end`] is exclusive, and at a
    /// boundary would name the unit this reader is waiting for rather than
    /// the one it is reading out of.
    pub at: u64,
    pub rate: Option<u64>,
    pub idle: Duration,
    pub last_read: u64,
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
    /// Where this consumer is, in bytes of the file.
    ///
    /// **A byte-weighted average of where its reads end**, not the last
    /// read's end ([`Self::place`]): a read moves it by a share that grows
    /// with the read's size, so a viewer's 256 KiB reads carry it and a
    /// crawler's 91-byte read at the tail of the same held run does not.
    /// A seek inside held bytes moves it over a handful of reads, which
    /// costs nothing -- the bytes under the new position are held, which
    /// is why the read joined this stream at all. A read about to run out
    /// of held bytes places it outright, so the window lands where the
    /// fetching is needed.
    end: u64,
    /// The read before this one, which is what the next sample is measured
    /// against.
    last: Read,
    /// How many reads have joined, first included.
    reads: u32,
    /// Every byte those reads asked for, summed over the stream's life.
    ///
    /// **What says which of a file's readers is the viewer.** A rate can
    /// only be measured once a read comes back late, and a stream that has
    /// not measured one is priced at the film's own arithmetic -- so by
    /// rate a crawler that has just started outranks a viewer who has been
    /// measured slower than the film's average. Bytes asked for need no
    /// such admission rule and separate the two by orders of magnitude:
    /// in the field log of 2026-09-14 the viewer's reads ran to tens of
    /// megabytes a body against the crawler's 39 kB.
    eaten: u64,
    /// When the last one did.
    seen: Instant,
    /// When its first read arrived. What says a stream was started after
    /// another's last read -- the viewer moving on from it; see
    /// [`FileStreams::current_viewer`].
    began: Instant,
    /// How fast this consumer is eating the file, in bytes a second, or
    /// `None` until a read has come back late enough to say anything. See
    /// [`Stream::sample`]: this is a *correction* to the file's own
    /// arithmetic and never the whole of the answer.
    rate: Option<u64>,
    /// How much is currently fetched ahead of it, in bytes. Grows towards
    /// what the rate asks for rather than jumping to it -- see
    /// [`Stream::grant`].
    window: u64,
    /// When the read that last earned a grant came in, or `None` having
    /// never been granted. A window grows for a consumer that is
    /// consuming; see [`Stream::grant`].
    granted_for: Option<Instant>,
    /// Whether the last grant was cut short by the doubling -- the window
    /// is still on its way to what the rate asks for. What says a stall
    /// reported now is the window filling and not the swarm falling short;
    /// see [`Streams::viewer_filling`]. True until the first grant: a
    /// stream nothing has granted has no window at all.
    filling: bool,
    /// The last sample [`Self::sample`] admitted: what the consumer ate and
    /// the gap it took, from the previous read's return to this one's
    /// arrival. Kept raw for the trace ([`Streams::last_sample`]), so a
    /// reading of the field log can check the rate's arithmetic.
    sampled: Option<(u64, std::time::Duration)>,
}

impl Stream {
    /// Whether nothing has read from this stream for [`STREAM_DORMANT`] as
    /// of `now`. A dormant stream is kept -- a resumed read rejoins it --
    /// but it holds no window, takes no share and is nobody's head.
    fn dormant(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.seen) >= STREAM_DORMANT
    }

    /// Move [`Self::end`] for `read`, which has just joined this stream.
    ///
    /// `missing_within` is how many bytes lie between the read's end and
    /// the first piece ahead of it the disk does not hold, or `u64::MAX`
    /// when the held run reaches the file's end. **A read within its
    /// window -- a piece at least -- of missing data places the position
    /// outright**: the fetch
    /// is needed there now and a position that lagged behind would draw
    /// the window short of the reads. Every other read moves the position
    /// by its share of the way, `size / (size + piece / 8)`, so
    /// the crawler's tail read inside a fully held film is a rounding
    /// error and a viewer's seek converges in a handful of reads.
    fn place(&mut self, read: &Read, missing_within: u64, piece: u64) {
        let size = read.size();
        if missing_within <= self.window.max(piece) {
            self.end = read.end;
            return;
        }
        let smoothing = (piece / PLAYHEAD_SMOOTHING_PIECE_SHARE).max(1);
        let from = i128::from(self.end);
        let delta = i128::from(read.end) - from;
        let moved = delta * i128::from(size) / (i128::from(size) + i128::from(smoothing));
        self.end = u64::try_from(from + moved).unwrap_or(0);
    }

    /// Fold what this consumer ate since the last read into its rate, if
    /// what it did says anything at all.
    ///
    /// **A starving player asks again the instant we answer**, because the
    /// socket has room and it has nothing to play. So the gap between our
    /// return and its next request measures *us* and not it: the field
    /// measured 262,144 bytes across a gap of under a millisecond, over and
    /// over, which read as five to eighteen gigabytes a second and asked
    /// for the whole film as a window. Consumption is only observable once
    /// we are already keeping up, which is exactly when we least need to
    /// discover it.
    ///
    /// So a sample counts only when the player came back *later than the
    /// picture it was carrying*. A read of `consumed` bytes is
    /// `consumed / ceiling` seconds of film; a player that returns sooner
    /// than that cannot have played what it was just given, so it is
    /// catching up, and the read measures our delivery. One that returns
    /// later has played it and waited, and that gap is the consumer's own.
    ///
    /// **There is no tolerance in that and no constant.** The comparison is
    /// against the film's own arithmetic, so it scales with the film: a
    /// dense film gives a read more seconds of picture, a sparse one fewer.
    ///
    /// It biases the measurement, and the bias is the useful direction. A
    /// sample saying "faster than the film's average" is exactly the one
    /// that cannot be told from starving, so it is discarded; what survives
    /// says "slower", which is what a subtitle track at twenty kilobytes a
    /// second is, and telling those apart is the whole of what the disk
    /// allocation needs. By construction nothing here can exceed `ceiling`:
    /// the gap that admits a sample is the gap that bounds it.
    ///
    /// Without a ceiling -- an entity whose length nobody has stated, which
    /// is a proxied URL -- there is no picture to compare against and the
    /// delivery rate is all there is. That is the same choice the policy
    /// this replaces makes: no duration, no time caps, byte arithmetic
    /// alone.
    ///
    /// **What it ate, not what we sent.** A player resumes where it stopped
    /// consuming, which is behind where we stopped sending by whatever sat
    /// unread in the socket -- 1.15 MB to 9.0 MB over the measured session.
    /// Crediting the whole of the last read overstates it: on one real
    /// reopen, 21,757,952 bytes served against 18,374,055 actually eaten.
    ///
    /// A read that consumed nothing is refused as well. It is a reopen
    /// landing behind, or a re-read of ground already served; it says where
    /// the consumer is, not how fast it is going, and folded in as a zero
    /// it would drag the rate to the floor on every seek.
    fn sample(&mut self, read: &Read, ceiling: Option<u64>) {
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
        // The picture that read was carrying, which is the least the player
        // could have spent on it.
        if let Some(ceiling) = ceiling.filter(|ceiling| *ceiling > 0)
            && gap.as_secs_f64() < consumed as f64 / ceiling as f64
        {
            return;
        }
        let sample = (consumed as f64 / gap.as_secs_f64()) as u64;
        self.sampled = Some((consumed, gap));
        // **Seeded from the film's own arithmetic, not from the first
        // sample.** A stream nothing has measured is already fetched at the
        // ceiling -- that is what `demand` does with a `None` rate -- so
        // taking the first admitted sample whole was a step out of it with
        // no averaging behind it at all, and the first admitted sample is
        // the one most likely to be wrong.
        //
        // It is wrong in a particular direction, which is why this matters.
        // A starving player asks again the instant it is answered, so its
        // reads are refused here; what survives admission is the moments
        // the player had some buffer and idled, and a long idle over a
        // small read is a *low* sample. On a link that alternates stalling
        // and bursting, the admitted samples are systematically the low
        // ones -- so one of them used to collapse the window to a tenth of
        // the film's rate, on exactly the link where a buffer is worth
        // most (field log 2026-09-14 18:21, a wifi-to-mobile switch:
        // `rates=[Some(367710)]` against a 3,568,061 B/s film, a want set
        // of eight pieces where the arithmetic allows seventy-seven).
        //
        // Seeded, the same sample moves the rate by an eighth -- one part
        // in `RATE_SMOOTHING` -- and the transition out of "no measurement"
        // is continuous rather than a step.
        self.rate = Some(match self.rate.or(ceiling) {
            None => sample,
            Some(rate) => (rate * (RATE_SMOOTHING - 1) + sample) / RATE_SMOOTHING,
        });
    }

    /// What this stream should be fetched at, in bytes a second.
    ///
    /// **Arithmetic first, measurement as a correction.** `ceiling` is the
    /// file's own size over its duration: exact at the first report,
    /// nothing to converge, and no read pattern can distort it. A stream
    /// nothing has measured is fetched at it from the start, which is what
    /// a playhead that has never once been kept up with needs -- and costs
    /// nothing, because the window still grows a doubling at a time
    /// ([`Stream::grant`]), so a consumer that turns out to be a one-shot
    /// probe has had the floor and stopped.
    ///
    /// A measurement can only lower it. See [`Stream::sample`] for why that
    /// is the only thing a measurement can honestly say.
    fn demand(&self, ceiling: Option<u64>) -> u64 {
        match (self.rate, ceiling) {
            (Some(rate), Some(ceiling)) => rate.min(ceiling),
            (Some(rate), None) => rate,
            (None, Some(ceiling)) => ceiling,
            // Neither arithmetic nor measurement: nothing is claimed, and
            // the demand floor is what the stream gets.
            (None, None) => 0,
        }
    }
}

impl Stream {
    /// Grow this stream's window towards `target`, and answer what it is
    /// now.
    ///
    /// **At most twice what it was.** A window that jumped straight to what
    /// one measurement asked for would hand a consumer that read once -- a
    /// container index, a probe, anything that opens and closes -- a whole
    /// film's worth of lookahead off a single sample. That is not
    /// hypothetical: it is the build that fetched 1.6 GB to play 100 MB,
    /// and the reason the policy this replaces keeps a probe's window and
    /// never wants it.
    ///
    /// Doubling costs the real consumer nothing. A grant is one step per
    /// pass of this file, not per read: a film reaches a 315 MB window
    /// from the 8 MB floor in six doublings, so six passes -- a dozen
    /// seconds at the reconciler's two-second tick, six stride moves for
    /// the proxy -- while a stream that turns out to be a one-shot probe
    /// has cost eight megabytes and stopped.
    fn grant(&mut self, target: u64, floor: u64) -> u64 {
        // **And only for a consumer that is consuming.** A doubling a pass
        // is what lets a real stream reach its window in a handful of
        // passes; applied to a stream that has not read since the last
        // grant it is a
        // lookahead that goes on growing after the reading has stopped. A
        // read of a container's index closes the moment it has what it
        // came for, and the field measured the tail of a paused film being
        // fetched for as long as the torrent was up.
        if self.granted_for == Some(self.seen) {
            return self.window.max(floor);
        }
        self.granted_for = Some(self.seen);
        let ceiling = if self.window == 0 {
            floor
        } else {
            self.window.saturating_mul(2)
        };
        // Cut short by the doubling: the window has not reached what was
        // asked, and the next pass will grow it again.
        self.filling = ceiling < target;
        self.window = target.min(ceiling).max(floor);
        self.window
    }
}

/// Why a read had to start a stream of its own.
///
/// One variant, and it stays an enum because the field log needs to say
/// *that* a read started a stream and not only that the count went up: a
/// join rule too tight reports two streams having opened ten, and the two
/// readings are told apart by how often this appears.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejected {
    /// The read began in no run this file's streams are in -- a different
    /// stretch of disk, or a piece the disk does not hold at all.
    Outside,
}

/// One over how much of a new sample the rate takes.
///
/// A player's reads are bursty -- it drains as fast as the socket allows
/// until its own buffer is full, then asks only as often as it plays -- so
/// a rate that followed each sample would swing between the link's speed
/// and the film's. A seventh of the old number and one of the new settles
/// over roughly a dozen reads.
///
/// **It was widened to a sixteenth and put back.** The argument for
/// widening is real -- only *admitted* samples move this
/// ([`Stream::sample`]), and admission is biased low on a bad link, so a
/// short memory tracks a stalling link down and leaves the stream too thin
/// to ride the next stall out. What the widening missed is that the seed
/// it was paired with only exists where a duration does. A proxied stream
/// states none, so its first admitted sample still sets the rate outright,
/// and a longer memory only makes that one sample stickier: on Windows,
/// where the first read of a test is slower and so the first sample is
/// lower, the cache kept three chunks of a played run where the assertion
/// wanted four.
///
/// So the width may only be widened for streams that are seeded, and that
/// is a second constant for one idea. The seed is what removes the cliff
/// -- one sample can no longer collapse the window -- and the width is
/// second order beside it: at either value the decay takes a dozen-odd
/// admitted samples, which is not the difference between riding a bad
/// patch out and not.
const RATE_SMOOTHING: u64 = 8;

/// The smallest window a stream is ever granted, in pieces.
///
/// Not a tunable: it is what a sequential reader needs to not block on the
/// very next piece it asks for. A consumer given only the piece it is
/// sitting in reads to the boundary and stops -- which is the field's
/// second track exactly, forty kilobytes at a time, every read ending at
/// 23,320,330,240. One piece past where it is, is the least that can be
/// called a lookahead.
const FLOOR_PIECES: u64 = 2;

/// How many of the LRU's next candidates the trace names.
///
/// Enough to see which end of the file they are at and whether they are the
/// pieces a viewer just played, and few enough not to crowd a 400-line ring
/// that has to hold the rest of a session.
pub const COLDEST_REPORTED: usize = 8;

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

/// Where one file lies in the torrent, which is what turns an offset
/// inside it into the piece index the disk is listed by.
///
/// A read carries an offset in its own file; `held` is a set of torrent
/// pieces. For a file starting at torrent offset zero those are the same
/// number -- every single-file torrent, and the first file of every other
/// -- so dividing the offset by the piece length is right about the field's
/// film and wrong about the second episode of a season pack, silently: the
/// run it looked up would be tens of thousands of pieces from the read.
#[derive(Debug, Clone)]
struct Geometry {
    /// The file's first byte within the torrent.
    offset: u64,
    /// The torrent pieces the file lies in, which bound every run this
    /// file's streams can be in and every window they can be granted.
    bound: Range<u32>,
}

impl Geometry {
    /// The torrent piece an offset inside this file falls in.
    fn at(&self, piece: u64, offset: u64) -> u32 {
        u32::try_from(self.offset.saturating_add(offset) / piece.max(1)).unwrap_or(u32::MAX)
    }
}

/// Every stream detected on one file.
#[derive(Debug)]
pub struct FileStreams {
    /// Where the file is, learned from the pass: the read path has a file
    /// offset and nothing else.
    geometry: Geometry,
    /// The file's own bitrate -- its size over its duration -- or `None`
    /// for a file whose length nobody has stated. The ceiling every stream
    /// on it is fetched at, and the measure of what a read's gap has to
    /// beat to say anything. See [`Stream::sample`].
    ceiling: Option<u64>,
    streams: Vec<Stream>,
    /// When each piece of this file was last any use. Kept because
    /// `Backing::held` answers a set with no times on it; see
    /// [`crate::retention::ledger`].
    ///
    /// **Per file, because a listing is.** A pass lists the pieces of the
    /// entity it is for, and a ledger settled against another file's
    /// listing would find none of its own pieces in it and forget the lot
    /// -- so every pass of every other file would reset this one's arrival
    /// times, and an LRU built on them would rank a file nobody has touched
    /// for a minute as freshly fetched.
    ledger: super::ledger::Ledger,
    /// What these streams hold, published for the door to read without
    /// taking anything. See [`super::exempt`].
    exempt: std::sync::Arc<super::exempt::Exempt>,
}

impl FileStreams {
    fn on(geometry: Geometry) -> Self {
        let exempt = std::sync::Arc::new(super::exempt::Exempt::for_pieces(geometry.bound.end));
        Self {
            geometry,
            ceiling: None,
            streams: Vec::new(),
            ledger: super::ledger::Ledger::default(),
            exempt,
        }
    }
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
        piece: u64,
    ) -> Option<Rejected> {
        let at = |offset: u64| self.geometry.at(piece, offset);
        let run = run_containing(held, at(read.begin), &self.geometry.bound);
        let nearest = run.as_ref().and_then(|run| {
            self.streams
                .iter()
                .position(|stream| run.contains(&at(stream.end.saturating_sub(1))))
        });

        if let (Some(index), Some(run)) = (nearest, run) {
            // How far the read is from running out of held bytes: the
            // first missing piece of its run, or nothing when the run
            // reaches the file's end -- the film's own end is not a hole.
            let missing_within = if run.end >= self.geometry.bound.end {
                u64::MAX
            } else {
                let into_piece = self.geometry.offset.saturating_add(read.end) % piece.max(1);
                u64::from(run.end.saturating_sub(at(read.end.saturating_sub(1))))
                    .saturating_mul(piece)
                    .saturating_sub(into_piece)
            };
            let stream = &mut self.streams[index];
            stream.sample(&read, self.ceiling);
            if stream.reader != reader {
                stream.reader = reader;
            }
            // Backwards included: a scrub back is not ground already
            // played, it is ground about to be played again, and a window
            // belongs where the viewer is. What bounds how far a read and
            // the position can differ is the held run: a read joins this
            // stream only while the stream's position and the read's first
            // byte are in one run of held pieces, and a read past a hole
            // is a stream of its own. See [`Stream::place`].
            stream.place(&read, missing_within, piece);
            stream.eaten = stream
                .eaten
                .saturating_add(read.end.saturating_sub(read.begin));
            stream.last = read;
            stream.reads = stream.reads.saturating_add(1);
            stream.seen = read.returned;
            return None;
        }

        self.streams.push(Stream {
            reader,
            end: read.end,
            eaten: read.end.saturating_sub(read.begin),
            last: read,
            reads: 1,
            seen: read.returned,
            began: read.arrived,
            rate: None,
            window: 0,
            granted_for: None,
            filling: true,
            sampled: None,
        });
        Some(Rejected::Outside)
    }
}

impl FileStreams {
    /// The piece this file is being consumed at; see
    /// [`Streams::busiest`].
    fn busiest(&self, piece: u64, now: Instant) -> Option<u32> {
        self.viewer(now)
            .map(|stream| self.geometry.at(piece, stream.end))
    }

    /// The live stream that has asked for the most bytes -- the viewer
    /// among a file's readers, by the argument at [`Stream::eaten`]; `None`
    /// for a file nothing live is reading.
    fn viewer(&self, now: Instant) -> Option<&Stream> {
        self.streams
            .iter()
            .filter(|stream| !stream.dormant(now))
            .max_by_key(|stream| stream.eaten)
    }

    /// [`Self::viewer`], less every stream the file's reading has moved on
    /// from: one that another live stream began after it was last read.
    ///
    /// A seek within one file is exactly that. The stream the viewer left
    /// stays live for [`STREAM_DORMANT`] and has asked for more bytes than
    /// the one it is on now for most of that, so by bytes alone it would be
    /// the viewer for half a minute after every seek -- and it is the new
    /// stream's window that is ramping. The crawler beside a viewer is not
    /// moved on from: it began before the viewer's latest read, and the
    /// viewer is read after it began, so both stay and bytes decide.
    ///
    /// Never empty while anything is live: the live stream that began last
    /// began after nothing else was last read.
    fn current_viewer(&self, now: Instant) -> Option<&Stream> {
        let live = || self.streams.iter().filter(|stream| !stream.dormant(now));
        live()
            .filter(|stream| !live().any(|other| other.began > stream.seen))
            .max_by_key(|stream| stream.eaten)
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

/// How many reads of files no pass has described yet are kept.
///
/// A read waits for its own file's geometry, which arrives with that file's
/// first pass; this is the backstop for a file that somehow never gets one,
/// so the detector's memory is bounded by something other than the length
/// of the session. Two hundred and fifty-six is a few seconds of one
/// consumer's reads at the rate the field measured.
const WAITING_READS: usize = 256;

/// How long a stream nothing has read from stays live.
///
/// A stream is never expired on a timer: it lives with its entity, like
/// the torrent does, and a resumed read rejoins it. What idleness changes
/// is what it holds. A dormant stream's window is no longer exempt from
/// reclaim, so its pieces are the LRU's like everything else -- a paused
/// viewer's pieces stay for as long as nothing colder needs the room, and
/// a seek's abandoned stream pins nothing -- and it takes no share of the
/// allowance and is nobody's head. Thirty seconds is longer than any gap
/// the field's slow second track left between its own reads.
const STREAM_DORMANT: std::time::Duration = std::time::Duration::from_secs(30);

/// How many bytes of reading move a stream's position halfway to a read's
/// end ([`Stream::place`]), as a share of the piece: an eighth. On the
/// field's 4 MiB pieces that is half a mebibyte -- a viewer's 256 KiB
/// read moves the position a third of the way, so a seek inside held
/// bytes converges in a couple of dozen reads, under two seconds of
/// playback, while the crawler's 91-byte read at the tail moves it by a
/// fiftieth of a percent. In pieces rather than bytes because a piece is
/// the unit everything else here is measured in, the fakes' included.
const PLAYHEAD_SMOOTHING_PIECE_SHARE: u64 = 8;

impl FileStreams {
    /// The pieces these streams want fetched ahead of them.
    ///
    /// **Shared as equal seconds, never as equal bytes.** Playback is gated
    /// by the worst track: ninety seconds of subtitles buys nothing while
    /// video has two, so the allocation that makes sense is the one that
    /// maximises the minimum. Equal shares of the *bytes* does the opposite
    /// -- ten megabytes split between a 3.5 MB/s film and a 20 kB/s second
    /// track gives the film 1.4 seconds and the track four minutes.
    ///
    /// So a single `t` is solved for, such that every stream's rate times
    /// `t` fits the budget, and each is granted `t` seconds. Scaling every
    /// stream's byte target by one factor is the same arithmetic, since
    /// bytes are rate times time; what must not happen is dividing the
    /// budget into equal parts.
    ///
    /// Then each is floored at [`FLOOR_PIECES`] and grown towards its share
    /// rather than jumped to it ([`Stream::grant`]). The floor is applied
    /// after the share and not subtracted before it: two pieces on two
    /// streams is sixteen megabytes, and a device with less free space than
    /// that is not playing video, so it is not a case the allocation has to
    /// be shaped around.
    /// What these streams would ask for over `seconds`, before anything is
    /// shared out -- the demand this file puts on the entity's allowance.
    fn asked(&self, seconds: u64, now: Instant) -> u64 {
        // Saturating against a `seconds` nobody should hand in: the
        // `Maximum` profile is a finite day, precisely so this does not
        // saturate and hand every stream the whole allowance.
        self.streams
            .iter()
            .filter(|stream| !stream.dormant(now))
            .map(|stream| stream.demand(self.ceiling).saturating_mul(seconds))
            .fold(0u64, u64::saturating_add)
    }

    /// Grow every stream towards its share of the allowance and answer
    /// where the windows now reach.
    ///
    /// `share` is the entity's, not this file's: it is one factor over
    /// every stream of every file, which is what makes the shares equal in
    /// seconds across the whole of what is being read. A file that scaled
    /// its own demand against the whole allowance would be promised it
    /// alone, and two files being read would together promise twice the
    /// disk there is.
    fn grant(
        &mut self,
        seconds: u64,
        piece: u64,
        share: impl Fn(u64) -> u64,
        now: Instant,
    ) -> Vec<Range<u32>> {
        let floor = FLOOR_PIECES.saturating_mul(piece);
        let ceiling = self.ceiling;
        for stream in self
            .streams
            .iter_mut()
            .filter(|stream| !stream.dormant(now))
        {
            let target = share(stream.demand(ceiling).saturating_mul(seconds));
            stream.grant(target, floor);
        }
        let windows = self.windows(piece, now);
        // What a window covers is what may not be unlinked, so the answer
        // is published here, where it is known, rather than recomputed at a
        // door that would have to take this lock to do it. **Here and not
        // in [`Self::windows`]**: that is also what a pass of *another*
        // file reads for this one, and a publication from there rewrote
        // this file's set with its windows alone -- without the promises
        // its own pass had published beside them. The set is this file's
        // pass's to write, and the backing overwrites it a call later with
        // the windows and the promises together.
        self.exempt.publish(&windows);
        windows
    }

    /// Where the windows already granted reach, granting nothing.
    ///
    /// What a pass of *another* file reports for this one. The grant is a
    /// doubling, one step per pass ([`Stream::grant`]), and a file whose
    /// window doubled on every pass of every other file would reach a full
    /// lookahead at a rate set by how many files are being read rather than
    /// by its own.
    fn windows(&self, piece: u64, now: Instant) -> Vec<Range<u32>> {
        self.streams
            .iter()
            .filter(|stream| !stream.dormant(now))
            .filter_map(|stream| {
                let from = self.geometry.at(piece, stream.end);
                let to = self
                    .geometry
                    .at(piece, stream.end.saturating_add(stream.window))
                    .saturating_add(1);
                let window = from.max(self.geometry.bound.start)..to.min(self.geometry.bound.end);
                (!window.is_empty()).then_some(window)
            })
            .collect()
    }
}

/// The detected streams of one entity, by file index.
#[derive(Debug, Default)]
pub struct Streams {
    by_file: HashMap<usize, FileStreams>,
    /// **How deep the backend is splitting the head of the lookahead**, and
    /// this video's stalls that size it; see [`super::deadline`]. The
    /// engine counts what the player reports here, the pass reads it.
    pub deadline: super::deadline::DeadlineDepth,
    /// Reads served since the last pass, awaiting the listing of their own
    /// file to be answered against. A read carries its own timestamps, so
    /// waiting costs the answer nothing.
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
    pub fn report_due(&mut self, now: Instant) -> bool {
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
    pub fn record(&mut self, file: usize, reader: u64, read: Read) {
        self.pending.push((file, reader, read));
    }

    /// Say where one file lies in the torrent.
    ///
    /// From the pass, because the pass's domain is the one place that has
    /// it: the read path knows an offset inside a file and no more, and
    /// every question asked here -- which run a read is in, which pieces a
    /// window covers -- is about torrent pieces.
    /// Only the geometry is written for a file already here, never the
    /// whole entry: the exempt set is a handle a door may already be
    /// holding, and replacing it would leave that door reading a set
    /// nothing publishes into.
    pub fn domain(&mut self, file: usize, offset: u64, bound: Range<u32>, ceiling: Option<u64>) {
        let geometry = Geometry { offset, bound };
        match self.by_file.entry(file) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().geometry = geometry;
                entry.get_mut().ceiling = ceiling;
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let mut streams = FileStreams::on(geometry);
                streams.ceiling = ceiling;
                entry.insert(streams);
            }
        }
    }

    /// Answer every read kept since the last pass against `held`, and say
    /// what the most recent one had to do.
    /// **A pass answers its own file's reads, against its own listing.**
    /// `held` is what the disk holds of `file`, so it is the only set the
    /// reads of that file can be answered against: a read of another file
    /// looked up here is looked up in a listing that does not cover it, so
    /// it is in no run, so it is a consumer of its own -- every read of it,
    /// for as long as both files are being read. Those wait for their own
    /// pass, which every file being read gets.
    pub fn observe(
        &mut self,
        file: usize,
        held: &BTreeSet<u32>,
        piece: u64,
        now: Instant,
    ) -> Option<Rejected> {
        let pending = std::mem::take(&mut self.pending);
        let mut last = None;
        let mut waiting = Vec::new();
        match self.by_file.get_mut(&file) {
            Some(streams) => {
                // The listing first, so a read of a piece that arrived this
                // pass finds it in the ledger to stamp.
                streams.ledger.settle(held, now);
                for (at_file, reader, read) in pending {
                    if at_file != file {
                        waiting.push((at_file, reader, read));
                        continue;
                    }
                    let pieces = streams.geometry.at(piece, read.begin)
                        ..=streams.geometry.at(piece, read.end.saturating_sub(1));
                    last = streams.observe(reader, read, held, piece);
                    streams.ledger.read(pieces, read.returned);
                }
            }
            // A pass for a file nothing has described can answer nothing:
            // every conversion here goes through a geometry, and the pass
            // states that before it asks. Everything waits, and the cap
            // below is what keeps waiting bounded.
            None => waiting = pending,
        }
        // Bounded, because a file whose pass never comes would otherwise
        // keep every read of the session. The newest are the ones a
        // detector could still make something of.
        let overflow = waiting.len().saturating_sub(WAITING_READS);
        waiting.drain(..overflow);
        self.pending = waiting;
        last
    }

    /// What the LRU would give up first, and how many pieces it is watching
    /// -- exempting everything in `kept`, which is what no unlink may
    /// touch: the tiers above it.
    pub fn coldest_of(
        &self,
        file: usize,
        now: Instant,
        kept: &[Range<u32>],
        how_many: usize,
    ) -> (usize, Vec<u32>) {
        let exempt = |piece: u32| kept.iter().any(|window| window.contains(&piece));
        let Some(ledger) = self.by_file.get(&file).map(|streams| &streams.ledger) else {
            return (0, Vec::new());
        };
        (ledger.len(), ledger.coldest(now, exempt, how_many))
    }

    /// What one file's streams hold, for a door to read without taking
    /// this lock.
    ///
    /// **The same handle for the life of the entity**, whether or not a
    /// pass has described the file yet: a door is meant to keep this and
    /// ask it per unlink without coming back here, and one handed a fresh
    /// empty set before the first pass would refuse nothing for as long as
    /// it held it. So a file asked about before its first pass is made
    /// here, with the extent the caller knows and the trivial geometry;
    /// the pass overwrites the geometry and keeps the set.
    ///
    /// Empty until something is published into it, which is the right
    /// answer for a file no stream is on.
    ///
    /// Published under the owner's lock and read at the door with a load
    /// and a bit test. See [`super::exempt::Exempt::holds`].
    pub fn exempt(&mut self, file: usize, pieces: u32) -> std::sync::Arc<super::exempt::Exempt> {
        self.by_file
            .entry(file)
            .or_insert_with(|| {
                FileStreams::on(Geometry {
                    offset: 0,
                    bound: 0..pieces,
                })
            })
            .exempt
            .clone()
    }

    /// How many pieces of `file` its streams are holding, for the trace
    /// line: the size of the answer the door would be getting.
    pub fn held_by_streams(&self, file: usize) -> u32 {
        self.by_file
            .get(&file)
            .map(|streams| streams.exempt.count())
            .unwrap_or(0)
    }

    /// How many streams are open on each file, for the trace line.
    pub fn counts(&self) -> Vec<(usize, usize)> {
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
    pub fn heads(&self, file: usize) -> Vec<(u64, u32)> {
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

    /// What every stream on every file of this entity wants fetched ahead
    /// of it. See [`FileStreams::want`].
    pub fn want(
        &mut self,
        file: usize,
        seconds: u64,
        budget: u64,
        piece: u64,
        now: Instant,
    ) -> Vec<Range<u32>> {
        // The demand of everything being read, so the sharing is over the
        // entity and not over one file of it. Live streams only: a dormant
        // one holds no window and takes no share ([`STREAM_DORMANT`]).
        let asked: u64 = self
            .by_file
            .values()
            .map(|streams| streams.asked(seconds, now))
            .fold(0u64, u64::saturating_add);
        // One factor for every stream of every file, which is what makes
        // the shares equal in seconds. Nothing to scale when it all fits.
        let share = |want: u64| {
            if asked <= budget || asked == 0 {
                want
            } else {
                ((want as u128 * budget as u128) / asked as u128) as u64
            }
        };
        let mut windows = Vec::new();
        for (idx, streams) in self.by_file.iter_mut() {
            // Granted for the file whose pass this is, and only reported
            // for the others: a grant is a doubling, one step per pass, and
            // a window that doubled on every pass of every file would grow
            // at a rate set by how many files are being read.
            windows.extend(if *idx == file {
                streams.grant(seconds, piece, share, now)
            } else {
                streams.windows(piece, now)
            });
        }
        windows
    }

    /// **The piece the file is being consumed at**: where its busiest
    /// stream has reached, or `None` for a file nothing is reading.
    ///
    /// Busiest by the bytes its reads have asked for ([`Stream::eaten`]),
    /// not by its measured rate: a rate is admitted only from a read that
    /// came back late, so a stream that has measured none is priced at the
    /// film's own arithmetic and would outrank a viewer measured slower
    /// than the film's average. `piece` is the torrent's piece length,
    /// because the answer is a torrent piece and a stream's head is an
    /// offset inside its file.
    ///
    /// **What this is for is the pass's cadence and its trace line**, and
    /// not what is kept: a file being read by a viewer and by mpv's index
    /// crawler has two streams, both of them a player's, and which one is
    /// the viewer is exactly what a rate says and a range header does not.
    /// See [`crate::retention::owner::Consumers::at`].
    pub fn busiest(&self, file: usize, piece: u64, now: Instant) -> Option<u32> {
        self.by_file.get(&file)?.busiest(piece, now)
    }

    /// **Whether the viewer's window is still filling** -- the live stream
    /// that has asked for the most bytes, over every file of the entity,
    /// had its last grant cut short by the doubling ([`Stream::grant`]).
    /// `None` for an entity nothing live is reading. Of each file's
    /// streams only the ones its reading has not moved on from count
    /// ([`FileStreams::current_viewer`]): after a seek, the stream being
    /// ramped is the new one, not the one with the longer history.
    ///
    /// What a stall reported by the player is read against: a window
    /// reaches what its rate asks for in a handful of passes after an open
    /// or a seek, and a stall inside that ramp is the window filling, not
    /// the swarm falling short of a full one. The player cannot tell the
    /// two apart -- both are its buffering popup after a frame -- and this
    /// side can, so the count that deepens the split
    /// ([`super::deadline`]) skips the ramp here rather than by a guessed
    /// grace period there.
    pub fn viewer_filling(&self, now: Instant) -> Option<bool> {
        self.by_file
            .values()
            .filter_map(|streams| streams.current_viewer(now))
            .max_by_key(|stream| stream.eaten)
            .map(|stream| stream.filling)
    }

    /// What each stream on `file` has measured its consumer to be eating,
    /// in bytes a second, or `None` for one that has not had two reads far
    /// enough apart to say.
    ///
    /// Reported beside the raw `consumed`/`gap_ms` of the last sample, not
    /// instead of them: at this seam a read is at most the 256 KiB the
    /// response asks for, so a rate could be measuring the socket draining
    /// while a player fills its buffer rather than the player consuming.
    /// So nothing is ever sized from this alone: a window is sized from the
    /// film's own arithmetic, which a measured rate may only lower
    /// ([`Stream::demand`]).
    /// Every consumer of `file`, as the cache window needs to know it:
    /// where it is, what it has been eating, and whether it is still at
    /// it. See [`Reader`].
    pub fn readers(&self, file: usize, now: Instant) -> Vec<Reader> {
        self.by_file
            .get(&file)
            .map(|streams| {
                streams
                    .streams
                    .iter()
                    .map(|stream| Reader {
                        at: stream.end.saturating_sub(1),
                        rate: stream.rate,
                        idle: now.saturating_duration_since(stream.seen),
                        last_read: stream.last.size(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn rates(&self, file: usize) -> Vec<Option<u64>> {
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
    /// Which of the two a number is, is what [`Stream::sample`] settles:
    /// most of them are delivery, so a sample is refused unless the player
    /// came back later than the picture it was carrying, and what survives
    /// may only ever lower the film's own arithmetic. These two halves stay
    /// raw so a field log can be read against the window a pass sized.
    pub fn last_sample(&self, file: usize) -> Option<(u64, std::time::Duration)> {
        self.by_file.get(&file)?.streams.last()?.sampled
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

    /// The detector for a film whose size over its duration is `ceiling`
    /// bytes a second, at the start of the torrent.
    fn film_at(ceiling: u64) -> FileStreams {
        let mut streams = file_at(0);
        streams.ceiling = Some(ceiling);
        streams
    }

    /// The detector for a file that starts at torrent offset `offset` and
    /// runs to the end of the torrent.
    fn file_at(offset: u64) -> FileStreams {
        FileStreams::on(Geometry {
            offset,
            bound: u32::try_from(offset / PIECE).unwrap()..PIECES,
        })
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
        let mut streams = file_at(0);

        assert_eq!(
            streams.observe(1, read(6_149_120_518, 6_149_382_662, t0, 0), &held, PIECE),
            Some(Rejected::Outside),
            "the first read of a session joins nothing"
        );
        assert_eq!(
            streams.observe(2, read(6_146_281_365, 6_146_543_509, t0, 1), &held, PIECE),
            None,
            "and the reopen behind it is the same consumer: the disk is whole between them"
        );
        assert_eq!(streams.streams.len(), 1);
    }

    /// **Which of a file's readers is the viewer, when both are a
    /// player's.**
    ///
    /// mpv keeps a second reader crawling the container's index for as long
    /// as a film is open, reopening about once a second; in the field log
    /// of 2026-09-14 it took 39 kB an open against the viewer's tens of
    /// megabytes, and both arrive as `Range: bytes=X-` over the same file.
    /// The crawler is the *newest* reader most of the time and the
    /// *longest-lived* stream some of the time, so neither recency nor age
    /// can be asked. Bytes eaten can.
    #[test]
    fn the_busiest_stream_is_the_one_that_has_eaten_the_most() {
        let t0 = Instant::now();
        let tail = PIECES as u64 * PIECE - 4 * PIECE;
        // Two runs with a hole between them, so the two readers cannot be
        // taken for one consumer.
        let held = disk(&[0..8, (PIECES - 4)..PIECES]);
        let mut streams = file_at(0);

        // The viewer, from the head, reading megabytes.
        streams.observe(1, read(0, PIECE, t0, 0), &held, PIECE);
        streams.observe(1, read(PIECE, 3 * PIECE, t0, 1), &held, PIECE);
        // The crawler at the tail, opened later and nibbling.
        streams.observe(2, read(tail, tail + 39_316, t0, 2), &held, PIECE);
        assert_eq!(streams.streams.len(), 2, "a hole apart, so two consumers");

        assert!(
            streams
                .busiest(PIECE, t0 + Duration::from_secs(2))
                .is_some_and(|piece| piece < 8),
            "the newest reader is at the tail; the one eating the file is not"
        );

        // And it follows the bytes rather than the order: let the crawler
        // outgrow the viewer and it becomes the answer.
        for step in 0..200u64 {
            let from = tail + 39_316 + step * 262_144;
            streams.observe(2, read(from, from + 262_144, t0, 3 + step), &held, PIECE);
        }
        assert_eq!(
            streams
                .busiest(PIECE, t0 + Duration::from_secs(203))
                .map(|piece| piece >= PIECES - 4),
            Some(true),
            "whoever is eating the file is where the file is being consumed"
        );
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
        let mut streams = file_at(0);

        streams.observe(1, read(6_149_120_518, 6_149_382_662, t0, 0), &held, PIECE);
        assert_eq!(
            streams.observe(2, read(6_146_281_365, 6_146_543_509, t0, 1), &held, PIECE),
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
        let mut streams = file_at(0);

        for (attempt, begin) in [
            23_320_306_052u64,
            23_320_306_078,
            23_320_306_166,
            23_320_306_260,
        ]
        .into_iter()
        .enumerate()
        {
            let outcome =
                streams.observe(2, read(begin, boundary, t0, attempt as u64), &held, PIECE);
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
        let mut streams = file_at(0);

        streams.observe(1, read(6_149_120_518, 6_149_382_662, t0, 0), &held, PIECE);
        assert_eq!(
            streams.observe(2, read(23_320_289_113, 23_320_330_240, t0, 1), &held, PIECE),
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
        let mut streams = file_at(0);
        streams.observe(1, read(6_149_120_518, 6_149_382_662, t0, 0), &held, PIECE);

        assert_eq!(
            streams.observe(1, read(1_000_000_000, 1_000_262_144, t0, 1), &held, PIECE),
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
        let mut streams = file_at(0);
        streams.observe(7, read(0, 262_144, t0, 0), &held, PIECE);
        for chunk in 1..4u64 {
            assert_eq!(
                streams.observe(
                    7,
                    read(chunk * 262_144, (chunk + 1) * 262_144, t0, chunk),
                    &held,
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
        let mut streams = file_at(0);

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
            PIECE,
        );

        assert_eq!(
            streams.streams[0].rate,
            Some(18_374_055),
            "what it ate in the second it took to ask again"
        );
    }

    /// **A player that comes back sooner than the picture it was given is
    /// catching up, and says nothing about how fast it plays.**
    ///
    /// The numbers are the field's: 262,144 bytes -- one read -- of a film
    /// at 3,568,061 bytes a second, which is 73 milliseconds of picture,
    /// returned after a gap the log printed as `gap_ms=0`. Taken as a
    /// measurement that is 5.5 gigabytes a second, and the want set it
    /// produced was the whole film: `want=[0..5567] exempt=5567`. A starving
    /// player asks again the instant we answer, so the gap measures our
    /// delivery; consumption is only observable once we are keeping up.
    #[test]
    fn a_read_returned_sooner_than_its_own_picture_is_not_a_measurement() {
        let t0 = Instant::now();
        let held = run(0..8);
        let mut streams = film_at(3_568_061);

        streams.observe(1, read(0, 262_144, t0, 0), &held, PIECE);
        // Four hundred microseconds later, which is what the socket
        // draining looks like.
        streams.observe(
            1,
            Read {
                begin: 262_144,
                end: 524_288,
                arrived: t0 + Duration::from_micros(400),
                returned: t0 + Duration::from_micros(400),
            },
            &held,
            PIECE,
        );

        assert_eq!(
            streams.streams[0].rate, None,
            "it cannot have played 73 ms of film in 400 us, so it was catching up"
        );
    }

    /// And one that comes back later than its own picture has played it and
    /// waited, so the gap is the consumer's own.
    ///
    /// **Which is the only thing a measurement can honestly say: slower.**
    /// By construction it cannot say faster -- the gap that admits a sample
    /// is the gap that bounds it -- and that is the useful direction: a
    /// subtitle track at twenty kilobytes a second is what the disk
    /// allocation has to tell apart from the film.
    #[test]
    fn a_read_returned_later_than_its_picture_measures_the_consumer() {
        let t0 = Instant::now();
        let held = run(0..8);
        let ceiling = 3_568_061;
        let mut streams = film_at(ceiling);

        streams.observe(1, read(0, 262_144, t0, 0), &held, PIECE);
        // A second later: this consumer ate 262,144 bytes in that second,
        // which is a fourteenth of the film's rate.
        streams.observe(
            1,
            Read {
                begin: 262_144,
                end: 524_288,
                arrived: t0 + Duration::from_secs(1),
                returned: t0 + Duration::from_secs(1),
            },
            &held,
            PIECE,
        );

        // **One sample moves the rate; it does not set it.** The rate is
        // seeded from the film's arithmetic, which is where `demand` has
        // this stream anyway while `rate` is `None`, so an admitted sample
        // is a correction to that and not a replacement of it -- an eighth
        // of the way, one part in `RATE_SMOOTHING`, here.
        let once = streams.streams[0]
            .rate
            .expect("a real gap is a measurement");
        assert_eq!(
            once,
            (ceiling * (RATE_SMOOTHING - 1) + 262_144) / RATE_SMOOTHING
        );
        assert!(once < ceiling, "and a measurement can only say slower");

        // And it keeps saying it: the same consumer, measured again and
        // again, converges on what it is really eating.
        for second in 2..60u64 {
            streams.observe(
                1,
                Read {
                    begin: 262_144 * second,
                    end: 262_144 * (second + 1),
                    arrived: t0 + Duration::from_secs(second),
                    returned: t0 + Duration::from_secs(second),
                },
                &held,
                PIECE,
            );
        }
        let settled = streams.streams[0].rate.expect("still measured");
        assert!(
            settled < once && settled < 2 * 262_144,
            "sixty seconds of a consumer eating 256 KiB a second did not \
             settle near 256 KiB a second: {settled}"
        );
    }

    /// **A stream nothing has measured is fetched at the film's own
    /// bitrate**, which is arithmetic and not a guess: size over duration
    /// is exact at the first report and no read pattern can distort it.
    ///
    /// A playhead that has never once been kept up with is exactly the
    /// stream no measurement can describe, and it is the one that most
    /// needs fetching for. It costs nothing to start it there, because the
    /// window still grows a doubling at a time.
    #[test]
    fn a_stream_nothing_has_measured_is_fetched_at_the_films_own_rate() {
        let t0 = Instant::now();
        let mut streams = film_at(3_568_061);
        stream_at(&mut streams, 100, 0, t0);
        streams.streams[0].rate = None;
        streams.streams[0].window = 0;

        // Five passes of doubling from the floor, with a read between each:
        // a window grows for a consumer that is consuming.
        for step in 0..5u64 {
            streams.streams[0].seen = at(t0, step + 1);
            want(&mut streams, 90, u64::MAX, PIECE, t0);
        }
        assert!(
            streams.streams[0].window > FLOOR_PIECES * PIECE,
            "an unmeasured stream grew past the floor: {}",
            streams.streams[0].window
        );
    }

    /// And a measurement only ever lowers it. A second track measured at a
    /// fraction of the film's rate is fetched at the fraction, which is
    /// what leaves the film the disk.
    #[test]
    fn a_measured_stream_is_fetched_at_what_it_measured() {
        let t0 = Instant::now();
        let ceiling = 3_568_061;
        let mut streams = film_at(ceiling);
        // A track at a fifth of a megabyte a second: above the demand
        // floor, so what it is granted is its own rate and not the floor.
        stream_at(&mut streams, 100, 200_000, t0);

        want(&mut streams, 90, u64::MAX, PIECE, t0);
        let window = streams.streams[0].window;
        assert_eq!(window, 200_000 * 90, "ninety seconds of what it measured");
        assert!(
            window < ceiling * 90,
            "and not ninety seconds of the film, which is what leaves the film the disk"
        );
    }

    /// **A rate measured before the film stated its length is still bound
    /// by it once it does.**
    ///
    /// Not hypothetical: the duration arrives from the app after playback
    /// starts, so the first reads of every session are measured with no
    /// ceiling to refuse them -- and those are exactly the reads of a
    /// player that has nothing buffered. The field measured 5.5 gigabytes a
    /// second that way.
    #[test]
    fn a_rate_measured_before_the_ceiling_was_known_is_still_bound_by_it() {
        let t0 = Instant::now();
        let held = run(0..8);
        let mut streams = file_at(0);

        streams.observe(1, read(0, 262_144, t0, 0), &held, PIECE);
        streams.observe(
            1,
            Read {
                begin: 262_144,
                end: 524_288,
                arrived: t0 + Duration::from_micros(400),
                returned: t0 + Duration::from_micros(400),
            },
            &held,
            PIECE,
        );
        let measured = streams.streams[0].rate.expect("no ceiling refused nothing");
        assert!(measured > 100_000_000, "the socket, measured: {measured}");

        // And then the duration arrives.
        let ceiling = 3_568_061;
        streams.ceiling = Some(ceiling);
        streams.streams[0].window = u64::MAX;

        want(&mut streams, 90, u64::MAX, PIECE, t0);
        assert_eq!(
            streams.streams[0].window,
            ceiling * 90,
            "ninety seconds of film, not ninety seconds of the socket"
        );
    }

    /// **The last sample reported is the one the rate was made from.**
    ///
    /// The trace prints `consumed` and `gap_ms` as the two halves of the
    /// last rate sample, so a reading of the field log can check the
    /// arithmetic. What it printed was the last read's size beside how long
    /// *we* took to serve it -- `returned - arrived`, the one pairing the
    /// `Read` doc rules out, since it books a stall on a missing piece as
    /// the consumer thinking. The sample is the previous read's bytes over
    /// the gap from its return to the next read's arrival.
    #[test]
    fn the_last_sample_is_what_the_rate_was_made_from() {
        let t0 = Instant::now();
        let held = run(0..8);
        let mut streams = file_at(0);
        let ms = |n: u64| t0 + Duration::from_millis(n);
        // Served in 300 ms; the consumer came back 700 ms after that.
        streams.observe(
            1,
            Read {
                begin: 0,
                end: 262_144,
                arrived: ms(0),
                returned: ms(300),
            },
            &held,
            PIECE,
        );
        streams.observe(
            1,
            Read {
                begin: 262_144,
                end: 524_288,
                arrived: ms(1_000),
                returned: ms(1_100),
            },
            &held,
            PIECE,
        );
        assert_eq!(
            streams.streams[0].sampled,
            Some((262_144, Duration::from_millis(700))),
            "the sample is the bytes the consumer ate over the gap it took, not \
             our own service latency"
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
        let mut streams = file_at(0);

        streams.observe(1, read(0, 262_144, t0, 0), &held, PIECE);
        // Same instant: no time passed.
        streams.observe(1, read(262_144, 524_288, t0, 0), &held, PIECE);
        assert_eq!(streams.streams[0].rate, None, "a gap of zero says nothing");

        // A second later, but landing entirely behind what the last read
        // served: nothing was consumed.
        streams.observe(1, read(0, 262_144, t0, 1), &held, PIECE);
        assert_eq!(
            streams.streams[0].rate, None,
            "and a read that ate nothing says nothing either"
        );
    }

    /// One file's share of its own demand: what [`Streams::want`] does when
    /// the entity is one file, which is every test below that builds a
    /// [`FileStreams`] directly.
    fn want(
        streams: &mut FileStreams,
        seconds: u64,
        budget: u64,
        piece: u64,
        now: Instant,
    ) -> Vec<Range<u32>> {
        let asked = streams.asked(seconds, now);
        streams.grant(
            seconds,
            piece,
            |want| {
                if asked <= budget || asked == 0 {
                    want
                } else {
                    ((want as u128 * budget as u128) / asked as u128) as u64
                }
            },
            now,
        )
    }

    /// Put a stream on `streams` at `piece` with a measured rate, without
    /// driving reads through it -- the want set is being tested here, not
    /// the detector.
    fn stream_at(streams: &mut FileStreams, piece: u32, rate: u64, t0: Instant) {
        streams.streams.push(Stream {
            reader: 1,
            end: u64::from(piece) * PIECE,
            eaten: PIECE,
            last: read(0, 0, t0, 0),
            reads: 1,
            seen: t0,
            began: t0,
            rate: Some(rate),
            window: u64::MAX,
            granted_for: None,
            filling: false,
            sampled: None,
        });
    }

    /// A window is granted where the stream is *in the torrent*, which for
    /// a file that does not start at the beginning is not where it is in
    /// the file.
    ///
    /// The old arithmetic would place this window a hundred pieces in,
    /// outside the file's own bound, where the clamp then drags it to the
    /// file's first piece -- fetching the head of the episode over and over
    /// while the viewer is an hour into it.
    #[test]
    fn a_window_covers_the_torrent_pieces_the_stream_is_actually_in() {
        let t0 = Instant::now();
        let start = 2_000u64;
        let mut streams = file_at(start * PIECE);
        stream_at(&mut streams, 100, 3_500_000, t0);

        let windows = want(&mut streams, 90, u64::MAX, PIECE, t0);
        assert_eq!(
            windows[0].start,
            u32::try_from(start).unwrap() + 100,
            "the window begins where the consumer is: {windows:?}"
        );
    }

    /// **A disk too small for every stream is shared as equal seconds, not
    /// equal bytes.**
    ///
    /// Playback is gated by the worst track, so the allocation that makes
    /// sense maximises the minimum. Equal shares of the bytes does the
    /// opposite: the film and a second track at a hundred and seventy-five
    /// times its rate, given half the disk each, leaves the film with a
    /// fraction of a second and the track with minutes it will never use.
    #[test]
    fn a_short_disk_is_shared_as_seconds_and_not_as_bytes() {
        let t0 = Instant::now();
        let mut streams = file_at(0);
        stream_at(&mut streams, 100, 3_500_000, t0);
        stream_at(&mut streams, 5_000, 1_000_000, t0);

        // Sixty seconds asked for, and half of that on the disk. Rates far
        // enough above the floor that the share is what decides, not it.
        let asked = (3_500_000 + 1_000_000) * 60;
        let windows = want(&mut streams, 60, asked / 2, PIECE, t0);

        let film = u64::from(windows[0].end - windows[0].start);
        let track = u64::from(windows[1].end - windows[1].start);
        // Equal seconds means the windows are in the ratio of the rates,
        // 3.5 to 1. Equal bytes would have made them the same size, which
        // is 1 to 1 -- the shape that gives a film a second and a half of
        // lookahead while a subtitle track sits on four minutes of it.
        assert!(
            film > track * 3 && film < track * 4,
            "the windows are not in the ratio of the rates: {film} against {track}"
        );
    }

    /// Put a stream on one file of `streams`, with a measured rate and a
    /// window already at its ceiling, so what a grant answers is the share
    /// and not the doubling.
    fn stream_on(streams: &mut Streams, file: usize, start: u32, at: u32, rate: u64, t0: Instant) {
        streams.domain(file, u64::from(start) * PIECE, start..PIECES, None);
        let file = streams
            .by_file
            .get_mut(&file)
            .expect("the file was just described");
        stream_at(file, at, rate, t0);
    }

    /// **The allowance is shared across everything being read, not within
    /// each file.**
    ///
    /// A season pack plays one episode while a second track of it is read
    /// from another file; a film plays while its subtitles are fetched from
    /// a sibling. If each file scaled its own demand against the whole
    /// allowance, two files being read would together be promised twice the
    /// disk there is -- and the shares would not be equal seconds either,
    /// which is the one thing the sharing exists to be.
    #[test]
    fn the_allowance_is_shared_across_the_files_being_read() {
        let t0 = Instant::now();
        let mut streams = Streams::default();
        // The film in the first half of the torrent, a second track in the
        // second, at a fraction of its rate.
        stream_on(&mut streams, 0, 0, 100, 3_500_000, t0);
        stream_on(&mut streams, 1, 2_783, 100, 1_000_000, t0);

        // Sixty seconds asked for between them, and half of it on the disk.
        let asked = (3_500_000 + 1_000_000) * 60;
        streams.want(0, 60, asked / 2, PIECE, t0);
        let windows = streams.want(1, 60, asked / 2, PIECE, t0);

        let width = |of: &Range<u32>| u64::from(of.end - of.start);
        let film: u64 = windows.iter().filter(|w| w.start < 2_783).map(width).sum();
        let track: u64 = windows.iter().filter(|w| w.start >= 2_783).map(width).sum();
        assert!(
            film > track * 3 && film < track * 4,
            "the windows are not in the ratio of the rates: {film} against {track}"
        );
    }

    /// **Under the `Maximum` profile the shares are still equal seconds.**
    ///
    /// `Maximum` asks for the whole file, and for a while it was `u64::MAX`
    /// seconds: every stream's demand saturated to `u64::MAX`, the scaling
    /// factor came out as one, and each stream of the entity was granted
    /// the whole allowance on its own -- two files being read together were
    /// promised twice the disk there is. A day is longer than any film and
    /// leaves the arithmetic intact.
    #[test]
    fn under_the_maximum_profile_the_shares_are_still_equal_seconds() {
        let t0 = Instant::now();
        let mut streams = Streams::default();
        stream_on(&mut streams, 0, 0, 100, 3_500_000, t0);
        stream_on(&mut streams, 1, 2_783, 100, 1_000_000, t0);

        let seconds = crate::backend::priorities::MAXIMUM_WINDOW_SECONDS;
        let budget = 60 * PIECE;
        let mut windows = Vec::new();
        for _ in 0..10 {
            streams.want(0, seconds, budget, PIECE, t0);
            windows = streams.want(1, seconds, budget, PIECE, t0);
        }

        let width = |of: &Range<u32>| u64::from(of.end - of.start);
        let film: u64 = windows.iter().filter(|w| w.start < 2_783).map(width).sum();
        let track: u64 = windows.iter().filter(|w| w.start >= 2_783).map(width).sum();
        assert!(
            film + track <= 60 + 2 * FLOOR_PIECES,
            "together promised more than the sixty pieces allowed: {film} + {track}"
        );
        assert!(
            film > track * 3 && film < track * 4,
            "the windows are not in the ratio of the rates: {film} against {track}"
        );
    }

    /// **A dormant stream of another file holds no window on this file's
    /// pass.**
    ///
    /// Dormancy is judged as of the pass's clock, whichever file the pass
    /// is for, so a season pack's finished episode does not hold its
    /// window exempt against the reclaim through the whole of the next.
    /// The stream itself stays, for the viewer who comes back to it.
    #[test]
    fn a_dormant_stream_of_another_file_holds_no_window() {
        let t0 = Instant::now();
        let mut streams = Streams::default();
        stream_on(&mut streams, 0, 0, 100, 3_500_000, t0);
        stream_on(&mut streams, 1, 2_783, 100, 3_500_000, t0);
        let held = run(0..PIECES);

        // A minute on, a pass for file 1: nothing has read either file.
        let later = t0 + Duration::from_secs(60);
        streams.observe(1, &held, PIECE, later);
        let windows = streams.want(1, 90, u64::MAX, PIECE, later);
        assert!(
            windows.is_empty(),
            "a stream unread for a minute still held a window: {windows:?}"
        );
        assert_eq!(
            streams.by_file[&0].streams.len(),
            1,
            "and it is kept, for the read that comes back to it"
        );

        // File 1 is read again: its stream is live, file 0's is dormant,
        // and the dormant one takes no share of an allowance that fits
        // exactly one stream's sixty seconds.
        streams.by_file.get_mut(&1).unwrap().streams[0].seen = later;
        let windows = streams.want(1, 60, 3_500_000 * 60, PIECE, later);
        let width: u64 = windows.iter().map(|w| u64::from(w.end - w.start)).sum();
        assert!(
            width > 40,
            "the live stream was granted {width} pieces: the dormant one took a share"
        );
    }

    /// **A pass of one file does not write into another file's exempt
    /// set.**
    ///
    /// The set is what that file's door reads, and it holds the promises of
    /// that file's parked reads beside the windows its own pass published.
    /// Reporting file 1's windows for file 0's pass used to republish them
    /// into file 1's set alone, and the promise bit went with it.
    #[test]
    fn a_pass_of_one_file_does_not_publish_into_anothers_exempt_set() {
        let t0 = Instant::now();
        let mut streams = Streams::default();
        stream_on(&mut streams, 0, 0, 100, 3_500_000, t0);
        stream_on(&mut streams, 1, 2_783, 100, 3_500_000, t0);
        // Windows at the floor, so the parked piece below is outside them.
        for file in [0, 1] {
            streams.by_file.get_mut(&file).unwrap().streams[0].window = 0;
        }
        // A read of file 1 parks on a piece far from its stream.
        streams.by_file[&1].exempt.hold(4_000..4_001);

        streams.want(0, 90, u64::MAX, PIECE, t0);
        assert!(
            streams.by_file[&1].exempt.holds(4_000),
            "file 0's pass overwrote file 1's promise"
        );
    }

    /// **A window doubles on its own file's pass, and on nobody else's.**
    ///
    /// The grant is one doubling per pass, and the passes of one entity are
    /// per file: a window that grew on every pass of every file would reach
    /// its full lookahead at a rate set by how many files are being read
    /// rather than by how fast its own consumer is going.
    #[test]
    fn a_pass_of_one_file_does_not_grow_another_files_window() {
        let t0 = Instant::now();
        let mut streams = Streams::default();
        stream_on(&mut streams, 0, 0, 100, 3_500_000, t0);
        stream_on(&mut streams, 1, 2_783, 100, 3_500_000, t0);
        // Both at the floor, where a doubling is visible.
        for file in [0, 1] {
            streams.by_file.get_mut(&file).unwrap().streams[0].window = 0;
        }

        for _ in 0..4 {
            streams.want(0, 90, u64::MAX, PIECE, t0);
        }
        assert_eq!(
            streams.by_file[&1].streams[0].window, 0,
            "four passes of the other file granted this one nothing"
        );
        streams.want(1, 90, u64::MAX, PIECE, t0);
        assert!(
            streams.by_file[&1].streams[0].window > 0,
            "and its own pass is what grants it"
        );
    }

    /// **A stream is filling until a grant reaches what it asked for, and
    /// the viewer is the one whose filling the entity reports.** Nothing
    /// granted is filling; a grant the doubling cut short is filling; a
    /// grant that reached the target is not. Between two files the one
    /// whose live stream has asked for the most bytes answers, and an
    /// entity with no live stream answers nothing.
    #[test]
    fn the_viewers_window_is_filling_until_a_grant_reaches_its_target() {
        let t0 = Instant::now();
        let mut streams = Streams::default();
        stream_on(&mut streams, 0, 0, 100, 3_500_000, t0);
        stream_on(&mut streams, 1, 2_783, 100, 3_500_000, t0);
        for file in [0, 1] {
            let stream = &mut streams.by_file.get_mut(&file).unwrap().streams[0];
            stream.window = 0;
            stream.filling = true;
        }
        streams.by_file.get_mut(&0).unwrap().streams[0].eaten = 10 * PIECE;
        assert_eq!(
            streams.viewer_filling(t0),
            Some(true),
            "nothing granted yet: no window at all"
        );

        // A grant a pass, each for a fresh read: 8 MB, 16, 32 ... towards
        // 315 MB, which the seventh reaches.
        for pass in 1..=6u64 {
            streams.by_file.get_mut(&0).unwrap().streams[0].seen = t0 + Duration::from_secs(pass);
            streams.want(0, 90, u64::MAX, PIECE, t0 + Duration::from_secs(pass));
            assert_eq!(
                streams.viewer_filling(t0 + Duration::from_secs(pass)),
                Some(true),
                "pass {pass}: the doubling cut the grant short"
            );
        }
        streams.by_file.get_mut(&0).unwrap().streams[0].seen = t0 + Duration::from_secs(7);
        streams.want(0, 90, u64::MAX, PIECE, t0 + Duration::from_secs(7));
        assert_eq!(
            streams.viewer_filling(t0 + Duration::from_secs(7)),
            Some(false),
            "the grant reached what the rate asked for"
        );

        // The other file's stream, ungranted, becomes the viewer by bytes.
        streams.by_file.get_mut(&1).unwrap().streams[0].eaten = 100 * PIECE;
        assert_eq!(
            streams.viewer_filling(t0 + Duration::from_secs(7)),
            Some(true)
        );

        // Everything dormant: nothing to say.
        assert_eq!(streams.viewer_filling(t0 + Duration::from_secs(600)), None);
    }

    /// **After a seek within one file, the window filling is the new
    /// stream's.** The stream the viewer left stays live for
    /// [`STREAM_DORMANT`] and has eaten far more, so by bytes alone it was
    /// the viewer for half a minute after every seek, its full window said
    /// "not filling", and a stall during the new stream's ramp was counted
    /// against the swarm. A crawler that began before the viewer's latest
    /// read does not take the viewer's place.
    #[test]
    fn after_a_seek_the_stall_rule_reads_the_stream_the_viewer_is_on_now() {
        let t0 = Instant::now();
        let mut streams = Streams::default();
        stream_on(&mut streams, 0, 0, 100, 3_500_000, t0);
        stream_on(&mut streams, 0, 0, 4_000, 3_500_000, t0);
        stream_on(&mut streams, 0, 0, 5_000, 3_500_000, t0);
        let file = streams.by_file.get_mut(&0).unwrap();
        // The film before the seek: a full window, a history of bytes.
        file.streams[0].eaten = 500 * PIECE;
        file.streams[0].seen = t0 + Duration::from_secs(10);
        // The crawler at the tail, opened after the film and read now and
        // then; nothing granted it a window worth having yet.
        file.streams[2].began = t0 + Duration::from_secs(1);
        file.streams[2].eaten = PIECE / 100;
        file.streams[2].seen = t0 + Duration::from_secs(11);
        file.streams[2].filling = true;
        assert_eq!(
            streams.viewer_filling(t0 + Duration::from_secs(11)),
            Some(false),
            "before the seek: the film's full window, not the crawler's"
        );

        // The seek: a new stream, begun after the film's last read and
        // ramping.
        let file = streams.by_file.get_mut(&0).unwrap();
        file.streams[1].began = t0 + Duration::from_secs(12);
        file.streams[1].seen = t0 + Duration::from_secs(13);
        file.streams[1].eaten = 4 * PIECE;
        file.streams[1].filling = true;
        assert_eq!(
            streams.viewer_filling(t0 + Duration::from_secs(14)),
            Some(true),
            "the stream the viewer left answered for the one it is on"
        );
    }

    /// **A window grows towards what the rate asks for; it never jumps to
    /// it.**
    ///
    /// A consumer that reads once and stops -- a container index, a probe,
    /// anything that opens and closes -- would otherwise be handed a film's
    /// worth of lookahead off a single sample. That is the build that
    /// fetched 1.6 GB to play 100 MB. Doubling costs a real consumer
    /// nothing: the grant is one doubling per pass of its file, and it
    /// reaches a full window in six of them.
    #[test]
    fn a_window_doubles_towards_its_target_rather_than_jumping_to_it() {
        let t0 = Instant::now();
        let mut streams = file_at(0);
        stream_at(&mut streams, 100, 3_500_000, t0);
        streams.streams[0].window = 0;

        let floor = FLOOR_PIECES * PIECE;
        let first = want(&mut streams, 90, u64::MAX, PIECE, t0);
        assert_eq!(
            u64::from(first[0].end - first[0].start) * PIECE,
            floor + PIECE,
            "the first grant is the floor, whatever the rate asks for"
        );

        let mut granted = streams.streams[0].window;
        for _ in 0..4 {
            want(&mut streams, 90, u64::MAX, PIECE, t0);
            let now = streams.streams[0].window;
            assert!(
                now <= granted * 2,
                "a window grew more than double in one pass: {granted} to {now}"
            );
            granted = now;
        }
        assert!(
            granted < 3_500_000 * 90,
            "five passes from the floor is not yet a full window"
        );
    }

    /// **A stream nothing has read from goes dormant, and stays.**
    ///
    /// It is not expired: a stream lives with its entity, like the torrent
    /// does, and a resumed read rejoins it. What idleness takes is its
    /// standing -- no window, no share, not the head -- so its pieces are
    /// the LRU's like everything else. A paused viewer's pieces stay for
    /// as long as nothing colder needs the room, and a seek's abandoned
    /// stream pins nothing.
    #[test]
    fn a_stream_nothing_has_read_from_goes_dormant_and_stays() {
        let t0 = Instant::now();
        let held = disk(&[0..4, 2_000..2_004]);
        let mut streams = file_at(0);
        streams.observe(1, read(0, 262_144, t0, 0), &held, PIECE);
        streams.observe(2, read(8_388_608_000, 8_388_870_144, t0, 1), &held, PIECE);
        assert_eq!(streams.streams.len(), 2);

        assert_eq!(
            streams.observe(1, read(262_144, 524_288, t0, 20), &held, PIECE),
            None
        );
        let at_20 = t0 + Duration::from_secs(20);
        assert!(
            streams.streams.iter().all(|stream| !stream.dormant(at_20)),
            "twenty seconds is not idle yet"
        );

        assert_eq!(
            streams.observe(1, read(524_288, 786_432, t0, 40), &held, PIECE),
            None
        );
        let at_40 = t0 + Duration::from_secs(40);
        assert_eq!(
            streams.streams.len(),
            2,
            "the one that stopped reading is kept"
        );
        assert!(
            streams.streams[1].dormant(at_40) && !streams.streams[0].dormant(at_40),
            "the one that stopped reading is dormant; the one that did not is not"
        );
        assert_eq!(
            want(&mut streams, 90, u64::MAX, PIECE, at_40).len(),
            1,
            "a dormant stream holds no window"
        );
        assert_eq!(
            streams.busiest(PIECE, at_40),
            Some(0),
            "and is not where the file is being consumed"
        );

        // Reading from it again wakes it: the same stream, not a new one.
        assert_eq!(
            streams.observe(2, read(8_388_870_144, 8_389_132_288, t0, 60), &held, PIECE),
            None,
            "a resumed read rejoined its stream"
        );
        assert_eq!(streams.streams.len(), 2);
    }

    /// **A crawler's read at the tail does not move the viewer's
    /// position.**
    ///
    /// On a fully held film every piece is one run, so mpv's index crawl
    /// at the tail joins the viewer's stream -- rightly, membership is the
    /// held run. The position is a byte-weighted average of where reads
    /// end ([`Stream::place`]): the viewer's 256 KiB reads carry it and
    /// the crawler's 91 bytes do not, so the window and the head stay
    /// with the viewer.
    #[test]
    fn a_crawlers_read_at_the_tail_does_not_move_the_viewers_position() {
        let t0 = Instant::now();
        let held = run(0..PIECES);
        let mut streams = file_at(0);
        for i in 0..3u64 {
            streams.observe(1, read(i * 262_144, (i + 1) * 262_144, t0, i), &held, PIECE);
        }
        let before = streams.streams[0].end;
        let tail = u64::from(PIECES - 1) * PIECE + 100;
        streams.observe(2, read(tail, tail + 91, t0, 3), &held, PIECE);
        assert_eq!(streams.streams.len(), 1, "one run, one stream");
        let moved = streams.streams[0].end - before;
        assert!(
            moved * 1_000 < tail - before,
            "the crawler's 91 bytes at the tail moved the position {moved} bytes of the \
             {} to the tail",
            tail - before
        );
        // And the viewer's next read pulls it straight back.
        streams.observe(1, read(3 * 262_144, 4 * 262_144, t0, 4), &held, PIECE);
        assert!(
            streams.streams[0].end < 2 * PIECE,
            "the position did not come back to the viewer: {}",
            streams.streams[0].end
        );
        assert!(matches!(
            streams.busiest(PIECE, t0 + Duration::from_secs(4)),
            Some(0 | 1)
        ));
    }

    /// **A seek inside held bytes converges over a handful of reads.**
    ///
    /// Gradual, because the bytes under the new position are held -- that
    /// is why the read joined this stream -- so nothing is waiting on the
    /// window getting there. Two dozen 256 KiB reads, under two seconds of
    /// playback, bring a four-hundred-megabyte seek within a couple of
    /// mebibytes.
    #[test]
    fn a_seek_inside_held_bytes_converges_over_a_few_reads() {
        let t0 = Instant::now();
        let held = run(0..PIECES);
        let mut streams = file_at(0);
        streams.observe(1, read(0, 262_144, t0, 0), &held, PIECE);
        let target = 100 * PIECE;
        streams.observe(1, read(target, target + 262_144, t0, 1), &held, PIECE);
        let after_one = streams.streams[0].end;
        assert!(
            after_one > 262_144 && after_one < target,
            "one read moved the position part of the way, not all: {after_one}"
        );
        for i in 1..24u64 {
            let from = target + i * 262_144;
            streams.observe(1, read(from, from + 262_144, t0, 1 + i), &held, PIECE);
        }
        let end = streams.streams[0].end;
        let reads_end = target + 24 * 262_144;
        assert!(
            reads_end - end < 2 * 1024 * 1024,
            "two dozen reads in, the position is still {} bytes behind the reads",
            reads_end - end
        );
    }

    /// **A read about to run out of held bytes places the position at
    /// once.**
    ///
    /// The window is drawn from the position, and where the next pieces
    /// are missing the window is what fetches them: a position lagging a
    /// seek would draw it short of the reads and the viewer would wait on
    /// pieces nothing was asking for.
    #[test]
    fn a_read_about_to_run_out_of_held_bytes_places_the_position_at_once() {
        let t0 = Instant::now();
        let held = run(0..8);
        let mut streams = file_at(0);
        streams.observe(1, read(0, 262_144, t0, 0), &held, PIECE);
        // Into the last held piece, with piece 8 missing beyond it.
        let from = 7 * PIECE + 1_000;
        streams.observe(1, read(from, from + 262_144, t0, 1), &held, PIECE);
        assert_eq!(
            streams.streams[0].end,
            from + 262_144,
            "a read a piece short of missing data was smoothed instead of placed"
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

    /// **A read's offset is inside its file; the disk is listed by torrent
    /// piece**, and the second episode of a season pack is where those stop
    /// being the same number.
    ///
    /// The file here begins two thousand pieces into the torrent, and the
    /// disk holds its first run and nothing else. A detector that divided
    /// the offset by the piece length would look up piece 0, find nothing
    /// held there, and report every read of the episode as a consumer of
    /// its own -- which is the whole detector failing, silently, on every
    /// torrent whose file does not start at zero.
    #[test]
    fn a_read_is_looked_up_at_the_piece_the_torrent_holds_it_in() {
        let t0 = Instant::now();
        let start = 2_000u64;
        let held = run(u32::try_from(start).unwrap()..u32::try_from(start).unwrap() + 10);
        let mut streams = file_at(start * PIECE);

        assert_eq!(
            streams.observe(1, read(0, 262_144, t0, 0), &held, PIECE),
            Some(Rejected::Outside),
            "the first read of a session joins nothing"
        );
        assert_eq!(
            streams.observe(1, read(262_144, 524_288, t0, 1), &held, PIECE),
            None,
            "and the next one continues it: both are in the run the torrent \
             holds at piece 2000, which is where this file begins"
        );
        assert_eq!(streams.streams.len(), 1);
    }

    /// A read served before its file's first pass waits for it rather than
    /// being answered with whatever geometry is to hand.
    ///
    /// Nothing but the pass knows where a file lies, and the read path runs
    /// first: a read taken on another file's geometry asks about a stretch
    /// of disk tens of thousands of pieces from the one it was in. Kept,
    /// because the answer is a pass away and the read is still true.
    #[test]
    fn a_read_of_a_file_no_pass_has_described_waits_for_one() {
        let t0 = Instant::now();
        let start = 2_000u32;
        let held = run(start..start + 10);
        let mut streams = Streams::default();

        streams.record(1, 7, read(0, 262_144, t0, 0));
        streams.observe(1, &held, PIECE, t0);
        assert!(
            streams.counts().is_empty(),
            "nothing was attributed to a file nothing has described"
        );

        streams.domain(1, u64::from(start) * PIECE, start..PIECES, None);
        streams.observe(1, &held, PIECE, at(t0, 1));
        assert_eq!(
            streams.counts(),
            vec![(1, 1)],
            "and the read that waited is answered against its own file"
        );
    }

    /// What waits for a pass is bounded. A file whose pass never comes
    /// would otherwise hold every read of the session, and the reads worth
    /// keeping are the newest.
    #[test]
    fn reads_waiting_for_a_pass_do_not_accumulate_without_end() {
        let t0 = Instant::now();
        let mut streams = Streams::default();
        let reads = WAITING_READS + 64;
        for chunk in 0..reads as u64 {
            streams.record(
                3,
                7,
                read(chunk * 262_144, (chunk + 1) * 262_144, t0, chunk),
            );
        }
        streams.observe(3, &run(0..PIECES), PIECE, t0);

        streams.domain(3, 0, whole(), None);
        streams.observe(3, &run(0..PIECES), PIECE, at(t0, 1));
        let heads = streams.heads(3);
        assert_eq!(heads.len(), 1);
        assert_eq!(heads[0].1, WAITING_READS as u32, "the newest are kept");
        assert!(
            reads as u64 * 262_144 - heads[0].0 < 1024 * 1024,
            "and they reach where the consumer really is, a read or two of smoothing \
             behind: {}",
            heads[0].0
        );
    }

    /// **What a pass grants is what the door is told**, and the telling
    /// costs the door nothing: the set the pass published is read with a
    /// load and a bit test, against a lock this file would otherwise have
    /// to be asked for once per candidate piece, per unlink, from a
    /// blocking thread.
    #[test]
    fn the_door_reads_the_window_the_pass_granted() {
        let t0 = Instant::now();
        let mut streams = Streams::default();
        streams.domain(0, 0, whole(), None);
        // A stream a third of the way in, moving at the film's rate.
        let held = run(1_800..1_860);
        streams.record(0, 1, read(1_850 * PIECE, 1_850 * PIECE + 262_144, t0, 0));
        streams.observe(0, &held, PIECE, t0);
        let windows = streams.want(0, 90, u64::MAX, PIECE, t0);

        let exempt = streams.exempt(0, PIECES);
        let window = windows[0].clone();
        assert!(
            window.clone().all(|piece| exempt.holds(piece)),
            "every piece of the granted window is held: {window:?}"
        );
        assert!(
            !exempt.holds(window.start - 1) && !exempt.holds(window.end),
            "and nothing on either side of it is"
        );
    }

    /// A file no pass has described holds nothing, rather than holding
    /// everything: a door asking about a file nothing is reading gets an
    /// answer, and the answer is that it may take what it likes.
    ///
    /// **And it is the same answer it will keep getting.** A door is meant
    /// to hold this handle and ask it per unlink; handed a fresh empty set
    /// each time it asked early, it would refuse nothing for as long as it
    /// held one -- which is the whole life of a stream that opened before
    /// its first pass.
    #[test]
    fn a_file_no_pass_has_described_refuses_nothing_yet() {
        let t0 = Instant::now();
        let mut streams = Streams::default();
        let exempt = streams.exempt(7, PIECES);
        assert!(!exempt.holds(0));
        assert_eq!(exempt.count(), 0);

        // The file's first pass, on the handle already handed out.
        streams.domain(7, 0, whole(), None);
        let held = run(1_800..1_860);
        streams.record(7, 1, read(1_850 * PIECE, 1_850 * PIECE + 262_144, t0, 0));
        streams.observe(7, &held, PIECE, t0);
        let windows = streams.want(7, 90, u64::MAX, PIECE, t0);

        assert!(
            windows[0].clone().all(|piece| exempt.holds(piece)),
            "the handle from before the pass is the one the pass published into"
        );
    }

    /// **A pass of one file leaves another file's ledger alone.**
    ///
    /// A listing covers the pieces of the file it was taken for, so a
    /// ledger settled against another file's listing finds none of its own
    /// pieces in it and forgets the lot. Every pass of every other file
    /// would then reset this one's arrival times, and the LRU built on them
    /// would rank a file nobody has touched for a minute as freshly
    /// fetched -- and, being the newest thing there is, the last to be
    /// given up.
    #[test]
    fn a_pass_of_one_file_does_not_forget_another_files_pieces() {
        let t0 = Instant::now();
        let mut streams = Streams::default();
        streams.domain(0, 0, 0..2_783, None);
        streams.domain(1, 2_783 * PIECE, 2_783..PIECES, None);

        // The first file's pass, which is what puts its pieces in a ledger.
        streams.observe(0, &run(0..8), PIECE, t0);
        let (tracked, _) = streams.coldest_of(0, at(t0, 1), &[], 8);
        assert_eq!(tracked, 8, "the first file's pieces are being watched");

        // And the second file's pass, over a disk that holds none of them.
        streams.observe(1, &run(2_783..2_791), PIECE, at(t0, 1));

        let (tracked, coldest) = streams.coldest_of(0, at(t0, 2), &[], 8);
        assert_eq!(tracked, 8, "the first file's pieces are still watched");
        assert_eq!(
            coldest,
            (0..8).collect::<Vec<_>>(),
            "and still at the age the pass that found them gave them"
        );
    }

    /// Files do not share streams: the same offsets in two files are two
    /// consumers, and a table keyed by file is what says so.
    #[test]
    fn two_files_do_not_share_a_stream() {
        let t0 = Instant::now();
        let held = run(0..4);
        let mut streams = Streams::default();
        streams.domain(0, 0, whole(), None);
        streams.domain(1, 0, whole(), None);
        streams.record(0, 1, read(0, 262_144, t0, 0));
        streams.record(1, 2, read(0, 262_144, t0, 1));
        streams.observe(0, &held, PIECE, t0);
        streams.observe(1, &held, PIECE, t0);

        assert_eq!(streams.counts(), vec![(0, 1), (1, 1)]);
    }
}
