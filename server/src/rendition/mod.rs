//! **Renditions**: a cast the receiver can decode, produced on demand,
//! nothing on disk (`docs/design/renditions.md`, step F1 of
//! `docs/design/media-pipeline.md` §5).
//!
//! A rendition is a published cast token ([`crate::ServerHandle::publish_rendition`])
//! with **one progressive fragmented MP4** behind it
//! (`/cast/{token}/stream.mp4`, `crate::cast`), its media segments each
//! produced on demand by a [`Producer`](crate::rendition::Producer) the
//! embedder installed -- the thing that demuxes, decodes and encodes, which
//! the server never does itself. The server's half is everything else: the
//! route, the file's byte layout, the cut rule, the fMP4 muxer (`mux.rs`),
//! the ring of fragments in memory, and the speed a run is judged by.
//!
//! **Not HLS, and seekable by bytes.** zond's Chromecast with Google TV
//! plays no HLS above 720p through its Media Source path, and plays a
//! progressive fragmented MP4 through its plain `<video src>` path -- and
//! seeks in one with a single `Range` when it has a length, exact bytes and
//! a `sidx` (`docs/design/renditions.md` §2.8). So the file's layout is
//! fixed before it is made (`layout.rs`): a header (`ftyp` + `moov` +
//! `sidx`), then one slot per segment, mirrored from the source's index or
//! estimated, each its segment's fragment padded to the slot's end
//! (`slots.rs` says what goes in one and what happens when it does not
//! fit).
//!
//! # The rules this keeps
//!
//! * **Nothing on disk.** Fragments live in a ring in memory -- two behind
//!   as many as [`RING_CAP`](crate::rendition::RING_CAP) holds, the farthest from where any run is asked
//!   dropped first, never the [`LOOKAHEAD`](crate::rendition::LOOKAHEAD) ahead of one -- and are
//!   dropped.
//! * **Every byte is the same however often it is made**: the layout is
//!   frozen with the first run's formats and the source's index, the
//!   overflow rule's decision per slot is recorded, and a run asks its
//!   producer for [`SEEK_BACK`](crate::rendition::SEEK_BACK) before its first cut, so a slot's samples do
//!   not depend on where its run started.
//! * **The cut rule**: segment N begins at the first video sync sample at or
//!   after its cut -- an indexed sync sample's time, or N x T -- and ends
//!   where N+1 begins; an audio sample belongs to the segment whose cuts its
//!   presentation time falls between. What a run is handed before its first
//!   cut is discarded. A slot is produced whole before it is sent.
//! * **A request for slot N** is answered from the ring; or waits, when N is
//!   the slot in production or at most [`LOOKAHEAD`](crate::rendition::LOOKAHEAD) past it (and joins that
//!   production, never restarts it); or is a seek: the run is dropped and a
//!   new one starts at N -- except the receiver's opening read, which does
//!   not move the first run, and a read never takes the run back from a later
//!   one ([`Ask`](crate::rendition::Ask)).
//! * **The lookahead blocks the producer**: a run completes at most
//!   [`LOOKAHEAD`](crate::rendition::LOOKAHEAD) slots past the last request, then stops reading its
//!   sink, and the producer's next write blocks.
//! * **An idle run is let go** after [`IDLE_RELEASE`](crate::rendition::IDLE_RELEASE) without a request,
//!   and never while a request waits for it to make a slot (a source that
//!   stalls is waited for, however long); the ring is kept and the next
//!   request starts a new run where it asks.
//! * **The first run's formats are frozen** into the init segment; a later run
//!   whose formats differ fails the rendition.
//! * **A run slower than real time fails the rendition with a sentence**:
//!   under 1.0x over [`SPEED_WINDOW`](crate::rendition::SPEED_WINDOW) of busy time once its first slot
//!   is out, where busy time leaves out the sink's and the reader's waits
//!   (`speed.rs`).
//! * **Cut by unpublish**, as a plain cast is: the run is dropped (its sink
//!   answers [`Stopped`](crate::rendition::Stopped), its reader is cancelled), the ring goes, and a
//!   request waiting on a slot is answered with an error.

pub(crate) mod layout;
pub(crate) mod mux;
mod run;
mod slots;
pub(crate) mod speed;

use crate::media::{MediaId, MediaReader, PlayToken};
use crate::state::AppState;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

pub(crate) use speed::WaitClock;

/// Segments produced past the last request before the producer is made to
/// wait (`L`, §2.1).
pub const LOOKAHEAD: u64 = 2;
/// How many runs may be live at once: two reads far apart (a receiver's
/// demuxer reading one place while its data source fills another) each
/// have one, rather than take one from each other in turn.
pub const MAX_RUNS: usize = 2;
/// The most the ring holds, whatever the segment count says. Slots are
/// kept until it is full, the farthest from where any run is asked first.
pub const RING_CAP: usize = 96 * 1024 * 1024;
/// How long a run is kept with no request before it is let go.
pub const IDLE_RELEASE: Duration = Duration::from_secs(60);
/// The busy time a run's speed is judged over.
pub const SPEED_WINDOW: Duration = Duration::from_secs(10);
/// How far before a segment's cut a run asks its producer to start: a
/// container may store a sample shown at or after the cut before the sync
/// sample it seeks to (audio interleaved ahead of video), and a segment
/// made by a run started at it must hold exactly what one made by a run
/// passing through it holds. What comes before the cut is discarded.
pub const SEEK_BACK: Duration = Duration::from_secs(2);
/// Samples in flight between a producer and its run.
const SINK_CAPACITY: usize = 32;

/// What the app asks for. Crosses FFI from Dart, so plain data.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenditionSpec {
    /// The film's duration, from mpv: how many segments there are.
    pub duration_ms: u64,
    /// Target segment length (6000; `docs/design/renditions.md` §6).
    pub segment_ms: u32,
    /// Where the receiver will start: the first run begins at this
    /// segment, so the init segment and the first segment come from one run.
    pub start_ms: u64,
    pub video: VideoPlan,
    pub audio: AudioPlan,
    /// Which of the source's audio tracks, by its ordinal among audio
    /// tracks (mpv's selection, mapped by the app).
    pub audio_track: u32,
}

/// What happens to the video.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum VideoPlan {
    /// The source's samples, repackaged.
    Copy,
    /// Transcoded to H.264 at this size and bitrate.
    #[serde(rename_all = "camelCase")]
    H264 {
        width: u32,
        height: u32,
        bitrate: u32,
    },
}

/// What happens to the sound.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AudioPlan {
    /// The source's AAC samples, repackaged.
    Copy,
    /// Converted to stereo AAC at this bitrate.
    #[serde(rename_all = "camelCase")]
    AacStereo { bitrate: u32 },
}

/// Where a rendition is, as the app's cast watchdog reads it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum RenditionState {
    /// A run is live: producing, or waiting for the receiver to move the
    /// lookahead.
    Producing,
    /// No run: none asked for yet, one let go after [`IDLE_RELEASE`], or the
    /// last one reached the end of the film. The next request starts one.
    Idle,
    /// The rendition cannot go on; `sentence` is what the viewer is shown.
    Failed { sentence: String },
    /// The token is not published (any more): unpublished, the listener
    /// stopped, or never a rendition this server published.
    Ended,
}

/// Which track a format or a sample is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TrackKind {
    Video,
    Audio,
}

/// A track's format, as a producer reports it before its first sample.
/// Codec configuration bytes are what Android's `MediaFormat` carries as
/// `csd-0`/`csd-1`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrackFormat {
    /// H.264: `csd0` the SPS, `csd1` the PPS, both Annex-B (start codes).
    H264 {
        width: u32,
        height: u32,
        csd0: Bytes,
        csd1: Bytes,
    },
    /// HEVC: `csd0` the VPS, SPS and PPS, Annex-B.
    Hevc {
        width: u32,
        height: u32,
        csd0: Bytes,
    },
    /// AAC: `csd0` the AudioSpecificConfig.
    Aac {
        sample_rate: u32,
        channels: u32,
        csd0: Bytes,
    },
}

impl TrackFormat {
    fn kind(&self) -> TrackKind {
        match self {
            Self::H264 { .. } | Self::Hevc { .. } => TrackKind::Video,
            Self::Aac { .. } => TrackKind::Audio,
        }
    }
}

/// **One sync sample of the source's video, where the source's index puts
/// it**: what a rendition's byte layout mirrors (`layout.rs`). The producer
/// reports the index once, when [`Job::wants_index`] asks, before its first
/// sample ([`SampleSink::index`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    /// Presentation time, microseconds on the film's clock -- the clock the
    /// samples are on.
    pub pts_us: i64,
    /// The byte in the source where the sync sample begins (or the
    /// container unit that begins with it).
    pub pos: u64,
}

/// One access unit: presentation time in microseconds from the film's
/// start, whether it is a sync sample, and its bytes (video in Annex-B).
#[derive(Clone, Debug)]
pub struct Sample {
    pub track: TrackKind,
    pub pts_us: i64,
    pub key: bool,
    pub data: Bytes,
}

/// The run is gone -- unpublished, superseded by a seek, let go while
/// idle, or failed -- and the producer should stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stopped;

impl std::fmt::Display for Stopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the rendition's run has stopped")
    }
}

impl std::error::Error for Stopped {}

/// A producer that cannot begin a run, with the sentence a viewer is shown.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProducerRefusal(pub String);

impl std::fmt::Display for ProducerRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ProducerRefusal {}

/// **What the server asks the embedder**: turn a reader over a media id
/// into encoded samples, from a time. Installed once
/// ([`crate::ServerHandle::install_producer`]).
pub trait Producer: Send + Sync + 'static {
    /// Begin a run and return at once. The run reads `job.reader` and
    /// writes to `job.sink` **on the producer's own thread**, never the
    /// caller's (which is the server's runtime); it ends when the sink
    /// answers [`Stopped`], the reader fails, or the source ends
    /// ([`SampleSink::end`]).
    fn start(&self, job: Job) -> Result<(), ProducerRefusal>;
}

/// One run's work.
pub struct Job {
    /// Opened by the server with the publication's play, as a cast body is:
    /// the reads are the viewer's playback.
    pub reader: MediaReader,
    pub spec: RenditionSpec,
    /// Start here: [`SEEK_BACK`] before the cut of the first segment the
    /// run makes. The producer starts at the sync sample at or before it;
    /// the server discards what precedes the cut.
    pub from: Duration,
    /// The rendition's layout is not fixed yet: report the source's index
    /// ([`SampleSink::index`]) before the first sample, if it has one.
    pub wants_index: bool,
    pub sink: SampleSink,
}

/// What a sink carries into its run.
pub(crate) enum SinkMessage {
    Format(TrackKind, TrackFormat),
    Index(Vec<IndexEntry>),
    Sample(Sample),
    End,
    Fail(String),
}

/// What a sink shows besides its answers: the time it spent blocked, and
/// whether it is blocked now.
#[derive(Default)]
pub(crate) struct SinkShown {
    pub(crate) waits: WaitClock,
    pub(crate) media: speed::MediaClock,
    blocked: AtomicBool,
}

/// **A run's way back into the server.** Blocking, for a foreign thread, in
/// [`MediaReader`]'s mould: called from inside a tokio runtime it answers
/// [`Stopped`] rather than block a worker.
///
/// A producer reports every track's format ([`Self::format`]) before its
/// first sample: the formats are taken as they stand at the first sample.
pub struct SampleSink {
    tx: mpsc::Sender<SinkMessage>,
    stop: CancellationToken,
    shown: Arc<SinkShown>,
}

/// A probe on a sink, for the tests of the lookahead: whether the producer
/// is blocked in it right now.
#[doc(hidden)]
#[derive(Clone)]
pub struct SinkProbe {
    shown: Arc<SinkShown>,
    stop: CancellationToken,
}

impl SinkProbe {
    /// The producer's write is waiting for the run to take it.
    pub fn is_blocked(&self) -> bool {
        self.shown.blocked.load(Ordering::SeqCst)
    }

    /// The run is gone: the next write answers [`Stopped`].
    pub fn is_stopped(&self) -> bool {
        self.stop.is_cancelled()
    }
}

impl SampleSink {
    /// A track's format: codec, codec configuration bytes, size or rate
    /// and channels.
    pub fn format(&self, track: TrackKind, format: TrackFormat) -> Result<(), Stopped> {
        self.send(SinkMessage::Format(track, format))
    }

    /// The source's index of the video's sync samples, when
    /// [`Job::wants_index`] asks -- before the first sample. Without one the
    /// layout is estimated.
    pub fn index(&self, entries: Vec<IndexEntry>) -> Result<(), Stopped> {
        self.send(SinkMessage::Index(entries))
    }

    /// One access unit. Blocks while the run is [`LOOKAHEAD`] segments
    /// ahead of the last request (the pause), and answers [`Stopped`] once
    /// the run is dropped.
    pub fn sample(&self, sample: Sample) -> Result<(), Stopped> {
        self.send(SinkMessage::Sample(sample))
    }

    /// The source ended: the last segment is whatever is buffered.
    pub fn end(self) {
        let _ = self.send(SinkMessage::End);
    }

    /// The run cannot go on, with the sentence a viewer is shown.
    pub fn fail(self, sentence: String) {
        let _ = self.send(SinkMessage::Fail(sentence));
    }

    /// A probe for the tests: see [`SinkProbe`].
    #[doc(hidden)]
    pub fn probe(&self) -> SinkProbe {
        SinkProbe {
            shown: self.shown.clone(),
            stop: self.stop.clone(),
        }
    }

    fn send(&self, message: SinkMessage) -> Result<(), Stopped> {
        if self.stop.is_cancelled() || tokio::runtime::Handle::try_current().is_ok() {
            return Err(Stopped);
        }
        if let SinkMessage::Sample(sample) = &message {
            self.shown.media.note(sample.pts_us);
        }
        match self.tx.try_send(message) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(Stopped),
            Err(mpsc::error::TrySendError::Full(message)) => {
                self.shown.waits.enter(Instant::now());
                self.shown.blocked.store(true, Ordering::SeqCst);
                let sent = self.tx.blocking_send(message);
                self.shown.blocked.store(false, Ordering::SeqCst);
                self.shown.waits.leave(Instant::now());
                sent.map_err(|_| Stopped)
            }
        }
    }
}

/// The durations a rendition runs by, settable for the tests so a release
/// or a speed window is not a minute of a test's life.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RenditionTuning {
    pub idle_release: Duration,
    pub speed_window: Duration,
    /// The ring's cap in bytes ([`RING_CAP`]).
    pub ring_cap: usize,
}

impl Default for RenditionTuning {
    fn default() -> Self {
        Self {
            idle_release: IDLE_RELEASE,
            speed_window: SPEED_WINDOW,
            ring_cap: RING_CAP,
        }
    }
}

/// What a rendition holds right now, for the tests.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenditionProbe {
    /// Runs begun since the publish.
    pub runs_started: u64,
    /// The slot the latest live run began at, if one is live.
    pub run_from: Option<u64>,
    /// The slot the latest live run is producing.
    pub in_production: Option<u64>,
    /// How many runs are live.
    pub live_runs: usize,
    /// The slots in the ring.
    pub ring: Vec<u64>,
    /// Whether the layout -- and with it the init segment -- is frozen.
    pub init: bool,
    /// The file's length, once the layout is frozen.
    pub total: Option<u64>,
    /// Whether the layout mirrors the source's index (or is estimated).
    pub exact: Option<bool>,
    /// The slots whose content spilled into the next, and the ones cut
    /// short.
    pub spilled: Vec<u64>,
    pub truncated: Vec<u64>,
}

/// Why a request under a rendition is not answered with bytes.
#[derive(Debug, PartialEq, Eq)]
pub enum NotServed {
    /// Past the last slot.
    NotFound,
    /// The rendition failed; the sentence.
    Failed(String),
    /// The publication was cut while the request waited.
    Cut,
}

/// What a request for a slot is (`docs/design/renditions.md` §2.8). A
/// slot in the ring is answered; one a live run will make within its
/// lookahead is waited for (the request **joins** that run); otherwise the
/// request starts a run there when fewer than [`MAX_RUNS`] are live -- two
/// readers far apart each get their own -- and, with every run taken,
/// replaces one **nobody is waiting on** (the least recently asked), or --
/// a [`Ask::Seek`] on its first look -- any, the least recently asked. A
/// request waited on is never taken from, so two readers never take the
/// runs from each other in turn; a request that may do none of this waits.
/// The first run, before any read has asked it for a slot, counts as
/// waited on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ask {
    /// The first slot of a range the receiver chose.
    Seek,
    /// A range read on into its next slot.
    Continue,
    /// The receiver opening the file: a read from the header on, into the
    /// first slot it reaches without having chosen it. It starts a run only
    /// when none is live: the first run is at the spec's start, where the
    /// receiver was told to play and is about to seek, and the second is
    /// for a reader that chose where it reads.
    Open,
}

/// A request counted in its run's waiters while it waits for that run to
/// make its slot; uncounted when it stops waiting, however it stops.
struct Joined {
    rendition: Arc<Rendition>,
    run: Option<u64>,
}

impl Joined {
    fn set(&mut self, inner: &mut Inner, run: Option<u64>) {
        if self.run == run {
            return;
        }
        if let Some(old) = self.run.and_then(|generation| inner.run_mut(generation)) {
            old.joined -= 1;
        }
        if let Some(new) = run.and_then(|generation| inner.run_mut(generation)) {
            new.joined += 1;
        }
        self.run = run;
    }
}

impl Drop for Joined {
    fn drop(&mut self) {
        if let Some(generation) = self.run {
            if let Some(run) = self.rendition.inner().run_mut(generation) {
                run.joined -= 1;
            }
            // A run waiting for its waiters to go looks again.
            self.rendition.bump();
        }
    }
}

/// What a request that is not answered from the ring nor joins a run may
/// do about its slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Start {
    /// Start a run there, beside the live ones.
    Beside,
    /// Start a run there in place of the least recently asked one nobody
    /// waits on.
    ReplacingUnwaited,
    /// Start a run there in place of the least recently asked one.
    Replacing,
    /// Wait.
    Wait,
}

impl Ask {
    /// What a range asks for the slot after this one: it reads on into it.
    pub(crate) fn then(self) -> Self {
        Ask::Continue
    }

    /// What a request may do (see [`Ask`]): on its `first` look or later,
    /// with `live` runs live, of which `unwaited` have nobody waiting on
    /// them.
    fn start(self, first: bool, live: usize, unwaited: usize) -> Start {
        if live < MAX_RUNS {
            Start::Beside
        } else if self == Ask::Seek && first {
            Start::Replacing
        } else if unwaited > 0 {
            Start::ReplacingUnwaited
        } else {
            Start::Wait
        }
    }
}

/// A live run, as a request sees it.
pub(crate) struct RunSlot {
    generation: u64,
    /// The first run, which no read has asked for a slot of yet: it is
    /// counted as waited on.
    reserved: bool,
    /// The slot it began at; for the run that freezes the layout, known
    /// once it has.
    from: u64,
    /// The slot in production: the lowest not yet in the ring.
    next_out: u64,
    /// The last slot a request asked of it -- the lookahead counts from
    /// here -- and when.
    last_request: u64,
    last_request_at: Instant,
    /// Requests waiting for it to make their slot: while any is, it is not
    /// idle, however long the making takes (a source that stalls is waited
    /// for).
    joined: usize,
    stop: CancellationToken,
}

impl RunSlot {
    /// Whether nobody waits on this run.
    fn unwaited(&self) -> bool {
        self.joined == 0 && !self.reserved
    }

    /// Whether a request for `slot` waits for this run: the slot in
    /// production or within the lookahead past it.
    fn joins(&self, slot: u64) -> bool {
        slot >= self.next_out && slot <= self.next_out + LOOKAHEAD
    }

    /// Whether this run made `slot` or is making it: what a request
    /// answered from the ring moves the lookahead of.
    fn covers(&self, slot: u64) -> bool {
        slot >= self.from && slot <= self.next_out + LOOKAHEAD
    }
}

/// The decisions that make a slot the same every time it is made
/// (`slots.rs`).
#[derive(Default)]
struct SlotPlan {
    /// How each slot made so far ended.
    ends: BTreeMap<u64, slots::End>,
    /// Slots a run was started at before the slot ahead of them was made:
    /// their start is their own segment's beginning, decided.
    fixed: std::collections::BTreeSet<u64>,
}

impl SlotPlan {
    /// Where slot `slot`'s content starts.
    fn start(&self, slot: u64) -> slots::Cursor {
        match slot
            .checked_sub(1)
            .and_then(|before| self.ends.get(&before))
        {
            Some(slots::End::Spill(cursor)) => *cursor,
            _ => slots::Cursor::at(slot),
        }
    }

    /// Whether slot `slot`'s start is decided: the slot before it was made,
    /// or a run started at it.
    fn decided(&self, slot: u64) -> bool {
        self.fixed.contains(&slot)
            || slot
                .checked_sub(1)
                .is_some_and(|before| self.ends.contains_key(&before))
    }
}

pub(crate) struct Inner {
    formats: Option<mux::Formats>,
    layout: Option<Arc<layout::Layout>>,
    /// Each slot's fragment, by slot; the padding is not kept.
    ring: BTreeMap<u64, Bytes>,
    ring_bytes: usize,
    /// The live runs, at most [`MAX_RUNS`].
    runs: Vec<RunSlot>,
    ring_cap: usize,
    failed: Option<String>,
    plan: SlotPlan,
    runs_started: u64,
}

impl Inner {
    pub(crate) fn run(&self, generation: u64) -> Option<&RunSlot> {
        self.runs.iter().find(|run| run.generation == generation)
    }

    fn run_mut(&mut self, generation: u64) -> Option<&mut RunSlot> {
        self.runs
            .iter_mut()
            .find(|run| run.generation == generation)
    }

    /// A request for `slot` reached run `generation`: its lookahead counts
    /// from there, and it is not idle.
    fn note_request(&mut self, generation: u64, slot: u64, now: Instant) {
        if let Some(run) = self.run_mut(generation) {
            run.last_request = slot;
            run.last_request_at = now;
        }
    }

    /// Whether run `generation` must wait for a request before it makes
    /// slot `next_out`: it is [`LOOKAHEAD`] past the last one asked of it,
    /// or the ring is full and it is past that.
    pub(crate) fn gated(&self, generation: u64, next_out: u64) -> bool {
        self.run(generation).is_none_or(|run| {
            next_out > run.last_request + LOOKAHEAD
                || (self.ring_bytes >= self.ring_cap && next_out > run.last_request)
        })
    }

    /// The run a new one takes the place of: the least recently asked --
    /// of those nobody waits on, when `unwaited_only`.
    fn replaced(&self, unwaited_only: bool) -> Option<usize> {
        self.runs
            .iter()
            .enumerate()
            .filter(|(_, run)| !unwaited_only || run.unwaited())
            .min_by_key(|(_, run)| run.last_request_at)
            .map(|(at, _)| at)
    }

    /// When run `generation` is idle: `release` after the last request asked
    /// of it -- or never, while a request waits for it to make a slot.
    pub(crate) fn idle_at(&self, generation: u64, release: Duration) -> Option<Instant> {
        self.run(generation)
            .and_then(|run| (run.joined == 0).then(|| run.last_request_at + release))
    }

    /// Over the cap, the slots farthest from where any run is asked go
    /// first; none at or ahead of a run's last request within its
    /// lookahead.
    fn trim(&mut self) {
        let anchors: Vec<u64> = self.runs.iter().map(|run| run.last_request).collect();
        while self.ring_bytes > self.ring_cap {
            let far = self
                .ring
                .keys()
                .copied()
                .filter(|key| {
                    !anchors
                        .iter()
                        .any(|anchor| key >= anchor && *key <= anchor + LOOKAHEAD)
                })
                .max_by_key(|key| {
                    anchors
                        .iter()
                        .map(|anchor| key.abs_diff(*anchor))
                        .min()
                        .unwrap_or(u64::MAX)
                });
            let Some(far) = far else { break };
            if let Some(bytes) = self.ring.remove(&far) {
                self.ring_bytes -= bytes.len();
            }
        }
    }

    fn insert(&mut self, slot: u64, bytes: Bytes) {
        self.ring_bytes += bytes.len();
        if let Some(old) = self.ring.insert(slot, bytes) {
            self.ring_bytes -= old.len();
        }
        self.trim();
    }
}

/// One published rendition: the spec, the producer, the layout, and the
/// ring with the run that fills it.
pub(crate) struct Rendition {
    id: MediaId,
    play: Option<PlayToken>,
    spec: RenditionSpec,
    producer: Arc<dyn Producer>,
    tuning: RenditionTuning,
    /// The publication's cut: every run's stop is a child of it.
    cut: CancellationToken,
    inner: Mutex<Inner>,
    /// Bumped on every change a request or a run may be waiting for.
    version: watch::Sender<u64>,
}

impl Rendition {
    pub(crate) fn new(
        id: MediaId,
        play: Option<PlayToken>,
        spec: RenditionSpec,
        producer: Arc<dyn Producer>,
        tuning: RenditionTuning,
        cut: CancellationToken,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            spec.segment_ms > 0,
            "a rendition's segment length must be above zero"
        );
        anyhow::ensure!(
            spec.duration_ms > 0,
            "a rendition's duration must be above zero"
        );
        Ok(Self {
            id,
            play,
            spec,
            producer,
            tuning,
            cut,
            inner: Mutex::new(Inner {
                formats: None,
                layout: None,
                ring: BTreeMap::new(),
                ring_bytes: 0,
                runs: Vec::new(),
                ring_cap: tuning.ring_cap,
                failed: None,
                plan: SlotPlan::default(),
                runs_started: 0,
            }),
            version: watch::channel(0).0,
        })
    }

    fn inner(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn bump(&self) {
        self.version
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    fn segment_us(&self) -> i64 {
        i64::from(self.spec.segment_ms) * 1000
    }

    fn duration_us(&self) -> i64 {
        i64::try_from(self.spec.duration_ms.saturating_mul(1000)).unwrap_or(i64::MAX)
    }

    pub(crate) fn has_layout(&self) -> bool {
        self.inner().layout.is_some()
    }

    pub(crate) fn state(&self) -> RenditionState {
        if self.cut.is_cancelled() {
            return RenditionState::Ended;
        }
        let inner = self.inner();
        if let Some(sentence) = &inner.failed {
            return RenditionState::Failed {
                sentence: sentence.clone(),
            };
        }
        if !inner.runs.is_empty() {
            RenditionState::Producing
        } else {
            RenditionState::Idle
        }
    }

    pub(crate) fn probe(&self) -> RenditionProbe {
        let inner = self.inner();
        let ended = |truncated: bool| {
            inner
                .plan
                .ends
                .iter()
                .filter(|(_, end)| {
                    if truncated {
                        matches!(end, slots::End::Truncated { .. })
                    } else {
                        matches!(end, slots::End::Spill(_))
                    }
                })
                .map(|(slot, _)| *slot)
                .collect()
        };
        RenditionProbe {
            runs_started: inner.runs_started,
            run_from: inner.runs.last().map(|run| run.from),
            in_production: inner.runs.last().map(|run| run.next_out),
            live_runs: inner.runs.len(),
            ring: inner.ring.keys().copied().collect(),
            init: inner.layout.is_some(),
            total: inner.layout.as_ref().map(|layout| layout.total),
            exact: inner.layout.as_ref().map(|layout| layout.exact),
            spilled: ended(false),
            truncated: ended(true),
        }
    }

    /// Fail the rendition: nothing more is produced, and every request
    /// waiting is answered with the sentence.
    fn fail(&self, inner: &mut Inner, sentence: String) {
        if inner.failed.is_none() {
            tracing::warn!(sentence = %sentence, "rendition failed");
            inner.failed = Some(sentence);
        }
        for run in inner.runs.drain(..) {
            run.stop.cancel();
        }
    }

    /// Start a run -- at `slot` (the seek path), or, with no layout yet, the
    /// run that will freeze it, from a segment before the spec's start --
    /// dropping the least recently asked run if [`MAX_RUNS`] are live (of
    /// those nobody waits on, when `unwaited_only`). Answers its generation.
    fn start_run(
        self: &Arc<Self>,
        inner: &mut Inner,
        state: &AppState,
        slot: Option<u64>,
        unwaited_only: bool,
    ) -> u64 {
        while inner.runs.len() >= MAX_RUNS {
            let at = inner.replaced(unwaited_only).unwrap_or(0);
            inner.runs.remove(at).stop.cancel();
        }
        inner.runs_started += 1;
        let generation = inner.runs_started;
        let back = i64::try_from(SEEK_BACK.as_micros()).unwrap_or(i64::MAX);
        let (from_us, from_slot) = match (slot, inner.layout.clone()) {
            (Some(slot), Some(layout)) => {
                // A run is started here: the slot's start is decided now, if
                // the slot before has not decided it, and the lookahead is
                // counted from it -- whoever asked last.
                inner.plan.fixed.insert(slot);
                let anchor = inner.plan.start(slot).anchor();
                let from = if anchor == 0 {
                    0
                } else {
                    layout.cuts[anchor as usize].saturating_sub(back).max(0)
                };
                (from, slot)
            }
            _ => {
                let start_us = i64::try_from(self.spec.start_ms.saturating_mul(1000))
                    .unwrap_or(i64::MAX)
                    .min(self.duration_us());
                let from = start_us
                    .saturating_sub(self.segment_us())
                    .saturating_sub(back)
                    .max(0);
                (from, 0)
            }
        };
        let stop = self.cut.child_token();
        // Its lookahead counts from where it starts: whoever asked last.
        inner.runs.push(RunSlot {
            generation,
            reserved: slot.is_none(),
            from: from_slot,
            next_out: from_slot,
            last_request: from_slot,
            last_request_at: Instant::now(),
            joined: 0,
            stop: stop.clone(),
        });
        tracing::info!(
            from = slot,
            from_ms = from_us / 1000,
            runs = generation,
            stage = "rendition_run_start",
            "rendition run started"
        );
        tokio::spawn(run::run(
            self.clone(),
            state.clone(),
            generation,
            slot,
            Duration::from_micros(from_us as u64),
            stop,
        ));
        generation
    }

    /// **The first sample of a run**: the layout frozen from the first run's
    /// formats and index -- or, for a later run, its formats checked against
    /// it -- and where the run's output starts: `start`, or the first slot
    /// cut at or after `from` for the run that froze the layout. That
    /// slot's start is decided, and with it the run's first cursor.
    pub(crate) fn freeze(
        &self,
        generation: u64,
        formats: &mux::Formats,
        index: Option<&[IndexEntry]>,
        source_len: u64,
        from: Duration,
        start: Option<u64>,
    ) -> Result<(Arc<layout::Layout>, u64, slots::Cursor), run::Frozen> {
        let mut inner = self.inner();
        if inner.run(generation).is_none() {
            return Err(run::Frozen::Stopped);
        }
        let layout = match (&inner.formats, inner.layout.clone()) {
            (Some(frozen), Some(layout)) => {
                if frozen != formats {
                    return Err(run::Frozen::Failed(
                        "The conversion came back from a seek in a different format, which the \
                         television cannot follow; cast the film again."
                            .to_string(),
                    ));
                }
                layout
            }
            _ => {
                let init = mux::init_segment(formats, self.spec.duration_ms)
                    .map_err(run::Frozen::Failed)?;
                let plan =
                    layout::Plan::new(index, source_len, self.duration_us(), self.segment_us());
                let (track, timescale) = formats.indexed_track();
                let layout = Arc::new(
                    layout::Layout::new(init, plan, track, timescale, self.duration_us())
                        .map_err(run::Frozen::Failed)?,
                );
                tracing::info!(
                    exact = layout.exact,
                    slots = layout.slots.len(),
                    total = layout.total,
                    source = source_len,
                    stage = "rendition_layout",
                    "rendition layout frozen"
                );
                inner.formats = Some(formats.clone());
                inner.layout = Some(layout.clone());
                layout
            }
        };
        let from_us = i64::try_from(from.as_micros()).unwrap_or(i64::MAX);
        let slot = start.unwrap_or_else(|| layout.first_slot_from(from_us));
        let slot = slot.min(layout.slots.len() as u64 - 1);
        inner.plan.fixed.insert(slot);
        let cursor = inner.plan.start(slot);
        if start.is_none() {
            // The run that froze the layout: it is the receiver's first
            // request's, which asked for the spec's start.
            if let Some(run) = inner.run_mut(generation) {
                run.from = slot;
                run.next_out = slot;
            }
            inner.note_request(generation, slot, Instant::now());
        }
        drop(inner);
        self.bump();
        Ok((layout, slot, cursor))
    }

    /// Slot `slot` made by run `generation`: its fragment in the ring, and
    /// its end recorded the first time -- a spill that a run started at the
    /// next slot has made impossible since recorded as the truncation it
    /// keeps the same bytes of. Answers the end recorded -- this one, or the
    /// one it was first made with -- or `None` when the run is not live.
    pub(crate) fn publish_slot(
        &self,
        generation: u64,
        slot: u64,
        filled: slots::Filled,
    ) -> Option<slots::End> {
        let recorded = {
            let mut inner = self.inner();
            inner.run_mut(generation)?.next_out = slot + 1;
            let count = inner.layout.as_ref().map_or(0, |layout| layout.slots.len()) as u64;
            let end = filled.end_now(slot + 1 >= count || inner.plan.decided(slot + 1));
            let recorded = *inner.plan.ends.entry(slot).or_insert(end);
            inner.insert(slot, filled.fragment);
            recorded
        };
        self.bump();
        Some(recorded)
    }

    /// **The file's layout**: frozen by the first run's first sample --
    /// starting that run, from before the spec's start, if none is live.
    pub(crate) async fn layout(
        self: &Arc<Self>,
        state: &AppState,
    ) -> Result<Arc<layout::Layout>, NotServed> {
        let mut seen = self.version.subscribe();
        loop {
            {
                let mut inner = self.inner();
                if self.cut.is_cancelled() {
                    return Err(NotServed::Cut);
                }
                if let Some(sentence) = &inner.failed {
                    return Err(NotServed::Failed(sentence.clone()));
                }
                if let Some(layout) = &inner.layout {
                    return Ok(layout.clone());
                }
                if inner.runs.is_empty() {
                    self.start_run(&mut inner, state, None, false);
                }
            }
            if !self.wait(&mut seen).await {
                return Err(NotServed::Cut);
            }
        }
    }

    /// The init segment (`ftyp` + `moov`), as the file begins with it.
    pub(crate) async fn init(self: &Arc<Self>, state: &AppState) -> Result<Bytes, NotServed> {
        let layout = self.layout(state).await?;
        Ok(layout.header.slice(..layout.init_len))
    }

    /// **Slot `slot`'s fragment**: from the ring, after the production of
    /// the run it joins, or from a run started at it, as [`Ask`] says.
    pub(crate) async fn slot(
        self: &Arc<Self>,
        state: &AppState,
        slot: u64,
        ask: Ask,
    ) -> Result<Bytes, NotServed> {
        let mut seen = self.version.subscribe();
        let mut noted = None;
        let mut first = true;
        let mut joined = Joined {
            rendition: self.clone(),
            run: None,
        };
        loop {
            {
                let mut inner = self.inner();
                if self.cut.is_cancelled() {
                    return Err(NotServed::Cut);
                }
                if let Some(sentence) = &inner.failed {
                    return Err(NotServed::Failed(sentence.clone()));
                }
                let count = inner.layout.as_ref().map_or(0, |layout| layout.slots.len()) as u64;
                if slot >= count {
                    return Err(NotServed::NotFound);
                }
                let now = Instant::now();
                if inner.ring.contains_key(&slot) {
                    joined.set(&mut inner, None);
                    // A read from the ring moves the lookahead of the run
                    // that made it, if one is making on from it.
                    let covering = inner
                        .runs
                        .iter()
                        .filter(|run| run.covers(slot))
                        .max_by_key(|run| run.from)
                        .map(|run| run.generation);
                    if let Some(run) = covering.and_then(|generation| inner.run_mut(generation)) {
                        run.reserved = false;
                    }
                    if let Some(generation) = covering
                        && noted != Some(generation)
                    {
                        inner.note_request(generation, slot, now);
                        inner.trim();
                        self.bump();
                    }
                    return Ok(inner.ring[&slot].clone());
                }
                let joins = inner
                    .runs
                    .iter()
                    .find(|run| run.joins(slot))
                    .map(|run| run.generation);
                let unwaited = inner.runs.iter().filter(|run| run.unwaited()).count();
                let start = if ask == Ask::Open && !inner.runs.is_empty() {
                    // The receiver opening the file reads on into a slot it
                    // did not choose; it is about to seek where a run is.
                    Start::Wait
                } else {
                    ask.start(first, inner.runs.len(), unwaited)
                };
                first = false;
                let target = match (joins, start) {
                    (Some(generation), _) => Some(generation),
                    (None, Start::Beside | Start::Replacing) => {
                        Some(self.start_run(&mut inner, state, Some(slot), false))
                    }
                    (None, Start::ReplacingUnwaited) => {
                        Some(self.start_run(&mut inner, state, Some(slot), true))
                    }
                    (None, Start::Wait) => None,
                };
                joined.set(&mut inner, target);
                if let Some(run) = target.and_then(|generation| inner.run_mut(generation)) {
                    run.reserved = false;
                }
                if let Some(generation) = target
                    && noted != Some(generation)
                {
                    noted = Some(generation);
                    inner.note_request(generation, slot, now);
                    // The gate moved: a run waiting on it looks again.
                    self.bump();
                }
            }
            if !self.wait(&mut seen).await {
                return Err(NotServed::Cut);
            }
        }
    }

    /// Wait for the next change. `false` when nothing can change any more.
    ///
    /// The cut reaches a waiting request through the run: a request waits
    /// only with a run live (it starts one if none is), that run's stop is a
    /// child of the cut, and a run's end always bumps the version -- after
    /// which the request finds the cut at the top of its loop.
    async fn wait(&self, seen: &mut watch::Receiver<u64>) -> bool {
        seen.changed().await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(duration_ms: u64, segment_ms: u32) -> RenditionSpec {
        RenditionSpec {
            duration_ms,
            segment_ms,
            start_ms: 0,
            video: VideoPlan::Copy,
            audio: AudioPlan::Copy,
            audio_track: 0,
        }
    }

    /// **What a request may start**: with room, a run beside the live ones;
    /// with every run taken, a seek on its first look in place of any, and
    /// anything else in place of one nobody waits on -- or nothing.
    #[test]
    fn a_request_starts_a_run_with_room_and_takes_none_waited_on() {
        for ask in [Ask::Seek, Ask::Continue, Ask::Open] {
            assert_eq!(ask.start(false, MAX_RUNS - 1, 0), Start::Beside, "{ask:?}");
            assert_eq!(ask.start(false, MAX_RUNS, 0), Start::Wait, "{ask:?}");
            assert_eq!(
                ask.start(false, MAX_RUNS, 1),
                Start::ReplacingUnwaited,
                "{ask:?}"
            );
            assert_eq!(ask.then(), Ask::Continue, "a range reads on from {ask:?}");
        }
        assert_eq!(Ask::Seek.start(true, MAX_RUNS, 0), Start::Replacing);
        assert_eq!(Ask::Continue.start(true, MAX_RUNS, 0), Start::Wait);
        assert_eq!(Ask::Open.start(true, MAX_RUNS, 0), Start::Wait);
    }

    fn inner_with(runs: &[u64], ring: &[u64], cap: usize) -> Inner {
        let mut inner = Inner {
            formats: None,
            layout: None,
            ring: BTreeMap::new(),
            ring_bytes: 0,
            runs: Vec::new(),
            ring_cap: usize::MAX,
            failed: None,
            plan: SlotPlan::default(),
            runs_started: 0,
        };
        for (generation, at) in runs.iter().enumerate() {
            inner.runs.push(RunSlot {
                generation: generation as u64,
                reserved: false,
                from: *at,
                next_out: *at,
                last_request: *at,
                last_request_at: Instant::now(),
                joined: 0,
                stop: CancellationToken::new(),
            });
        }
        for slot in ring {
            inner.insert(*slot, Bytes::from(vec![0u8; 10]));
        }
        inner.ring_cap = cap;
        inner.trim();
        inner
    }

    /// **A run is waited on** while a request waits for its making, and
    /// while it is the first run nobody has asked yet; otherwise another
    /// request may take its place.
    #[test]
    fn a_run_is_unwaited_only_with_nobody_waiting() {
        let mut inner = inner_with(&[4], &[], usize::MAX);
        let run = &mut inner.runs[0];
        assert!(run.unwaited());
        run.joined = 1;
        assert!(!run.unwaited());
        run.joined = 0;
        run.reserved = true;
        assert!(!run.unwaited());
    }

    /// **A run taken in place of is the least recently asked**, and -- for
    /// a request that may take only an unwaited one -- never one a request
    /// waits on.
    #[test]
    fn the_run_replaced_is_the_least_recently_asked_unwaited() {
        let mut inner = inner_with(&[4, 30], &[], usize::MAX);
        let now = Instant::now();
        inner.runs[0].last_request_at = now - Duration::from_secs(5);
        inner.runs[1].last_request_at = now;
        assert_eq!(inner.replaced(false), Some(0));
        assert_eq!(inner.replaced(true), Some(0));
        inner.runs[0].joined = 1;
        assert_eq!(inner.replaced(false), Some(0), "a seek takes any");
        assert_eq!(inner.replaced(true), Some(1), "not the one waited on");
        inner.runs[1].joined = 1;
        assert_eq!(inner.replaced(true), None);
    }

    /// **The ring keeps what it can hold**, and over its cap drops the
    /// slots farthest from where any run is asked -- never one a run is
    /// asked at or within its lookahead.
    #[test]
    fn the_ring_drops_the_slots_farthest_from_any_run() {
        let all = [0, 1, 2, 3, 10, 11, 12, 30];
        let kept = |cap| {
            inner_with(&[2, 11], &all, cap)
                .ring
                .keys()
                .copied()
                .collect::<Vec<_>>()
        };
        assert_eq!(kept(usize::MAX), all, "under the cap, everything");
        assert_eq!(kept(60), vec![1, 2, 3, 10, 11, 12], "30 and 0 go first");
        assert_eq!(kept(1), vec![2, 3, 11, 12], "never what a run is making");
    }

    #[test]
    fn the_spec_crosses_ffi_in_camel_case() {
        let spec = RenditionSpec {
            video: VideoPlan::H264 {
                width: 1920,
                height: 1080,
                bitrate: 8_000_000,
            },
            audio: AudioPlan::AacStereo { bitrate: 192_000 },
            ..spec(1000, 6000)
        };
        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "durationMs": 1000, "segmentMs": 6000, "startMs": 0,
                "video": {"h264": {"width": 1920, "height": 1080, "bitrate": 8_000_000}},
                "audio": {"aacStereo": {"bitrate": 192_000}},
                "audioTrack": 0
            })
        );
        assert_eq!(
            serde_json::to_value(RenditionState::Failed {
                sentence: "no".into()
            })
            .unwrap(),
            serde_json::json!({"state": "failed", "sentence": "no"})
        );
    }
}
