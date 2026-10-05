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
//!   dropped first, never the lookahead ahead of one -- and are
//!   dropped.
//! * **Every byte is the same however often it is made**: the layout is
//!   frozen with the first run's formats and the source's index, the
//!   overflow rule's decision per slot is recorded, and a run asks its
//!   producer for [`SEEK_BACK`](crate::rendition::SEEK_BACK) before its first cut, so a slot's samples do
//!   not depend on where its run started.
//! * **The cut rule**: segment N begins at the first video sync sample at or
//!   after its cut -- an indexed sync sample's time (every one a second or
//!   more apart: `layout::MIRROR_GRID_US`), or N x T -- and ends
//!   where N+1 begins; an audio sample belongs to the segment whose cuts its
//!   presentation time falls between. What a run is handed before its first
//!   cut is discarded. A slot is produced whole before it is sent.
//! * **A request for slot N** is answered from the ring; or waits, when N is
//!   the slot in production or within the lookahead past it (and joins that
//!   production, never restarts it); or is a seek: the run is dropped and a
//!   new one starts at N -- except that a read never takes the run back from
//!   a later one ([`Ask`](crate::rendition::Ask)). **Requests, and nothing
//!   else, make slots**: never where a player was told to start.
//! * **The lookahead blocks the producer**: a run completes the slots
//!   that begin within [`LOOKAHEAD_TIME`](crate::rendition::LOOKAHEAD_TIME)
//!   of the last request's, and at least [`LOOKAHEAD`](crate::rendition::LOOKAHEAD)
//!   past it, then stops reading its sink, and the producer's next write
//!   blocks.
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

/// The fewest slots produced past the last request before the producer is
/// made to wait (`L`, §2.1).
pub const LOOKAHEAD: u64 = 2;
/// How much of the film is produced past the last request before the
/// producer is made to wait, when that is more than [`LOOKAHEAD`] slots: a
/// mirrored layout has a slot per sync sample, a few seconds each, and the
/// lookahead was two six-second segments when it was counted in slots.
pub const LOOKAHEAD_TIME: Duration = Duration::from_secs(12);
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
/// How much of the film after the receiver's start a preparation makes
/// ([`crate::ServerHandle::prepare_rendition`]): the slots from the one
/// the receiver will ask for to the one holding the start plus this, so
/// it has this much film in hand when it begins to play. Nothing before
/// that slot: see [`Rendition::prepare`].
pub const PREPARED_AFTER: Duration = Duration::from_secs(6);

/// What the app asks for. Crosses FFI from Dart, so plain data.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenditionSpec {
    /// The film's duration, from mpv: how many segments there are.
    pub duration_ms: u64,
    /// Target segment length (6000; `docs/design/renditions.md` §6), for a
    /// source without an index, whose slots are estimated on this grid. A
    /// source with one is cut at its sync samples
    /// (`docs/design/renditions.md`, *A slot per sync sample*), whatever
    /// this says.
    pub segment_ms: u32,
    /// Where the receiver will be told to start. Production never reads it
    /// -- the receiver's requests decide what is made -- only a preparation
    /// does, to ask for the slot the receiver will ask for first
    /// ([`crate::ServerHandle::prepare_rendition`]).
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

/// **How far a published rendition has got towards the slot its receiver
/// starts in**, as the app asks before it tells the receiver to load
/// ([`crate::ServerHandle::rendition_readiness`]): the phase a preparation
/// ([`crate::ServerHandle::prepare_rendition`]) is in. A receiver's first
/// answer waits for exactly this, and a receiver gives up on a load whose
/// first answer stays silent too long -- so the app waits here instead,
/// where the viewer can see why and cancel.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "phase", rename_all = "camelCase")]
pub enum RenditionReadiness {
    /// The first run is reading the source's formats and index -- for a
    /// Matroska file, its Cues, usually at the end -- and the file's layout
    /// is not fixed yet: nothing of it can be answered.
    Index,
    /// The layout is fixed, so the header (`ftyp`, `moov`, `sidx`) is
    /// answerable; the slot the receiver starts in is being made.
    Start,
    /// The slot the receiver starts in is made: told to load now, the
    /// receiver's first requests are answered at once.
    Ready,
    /// The rendition cannot go on; `sentence` is what the viewer is shown.
    Failed { sentence: String },
    /// Not published (any more), or not a rendition.
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

    /// One access unit. Blocks while the run is its lookahead
    /// ([`LOOKAHEAD_TIME`]) ahead of the last request (the pause), and answers [`Stopped`] once
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
    /// How much of the film a run makes past its last request
    /// ([`LOOKAHEAD_TIME`]); never fewer than [`LOOKAHEAD`] slots.
    pub lookahead: Duration,
}

impl Default for RenditionTuning {
    fn default() -> Self {
        Self {
            idle_release: IDLE_RELEASE,
            speed_window: SPEED_WINDOW,
            ring_cap: RING_CAP,
            lookahead: LOOKAHEAD_TIME,
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

/// **What a rendition has made, and where**, for the app's cast panel
/// ([`crate::cast::CastNumbers`]): what is sent, the layout, the live runs,
/// and counts that only grow. No rate and no clock: the app takes two and
/// divides by the time between them.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenditionNumbers {
    /// What the app asked for: the picture copied or converted, the sound
    /// copied or converted.
    pub video: VideoPlan,
    pub audio: AudioPlan,
    /// What the first run reported it makes, once it has: the tracks the
    /// init segment describes.
    pub video_out: Option<TrackOut>,
    pub audio_out: Option<TrackOut>,
    /// The file's layout, once the first run has fixed it.
    pub layout: Option<LayoutNumbers>,
    /// The runs live now.
    pub runs: Vec<RunNumbers>,
    /// Runs begun since the publish: one more for every seek that started
    /// one, every restart after an idle release.
    pub runs_started: u64,
    /// Slots made since the publish, every making counted (a slot dropped
    /// from the ring and asked for again is made again), and the film
    /// they hold, in milliseconds.
    pub slots_made: u64,
    pub film_made_ms: u64,
    /// The slot the receiver last asked for -- the first slot of a range,
    /// or one a range read on into -- and where it begins on the film's
    /// clock.
    pub asked_slot: Option<u64>,
    pub asked_ms: Option<u64>,
    /// The last slot of the unbroken run of slots in the ring from
    /// [`Self::asked_slot`] on, and where it ends on the film's clock: how
    /// far ahead of the receiver the film is made. `None` when the slot
    /// asked for is not in the ring.
    pub made_to: Option<u64>,
    pub made_to_ms: Option<u64>,
}

/// A track a rendition makes, as its run reported it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackOut {
    /// `h264`, `hevc` or `aac`.
    pub codec: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub channels: Option<u32>,
    pub sample_rate: Option<u32>,
}

impl TrackOut {
    fn of(format: &TrackFormat) -> Self {
        let (codec, width, height, channels, sample_rate) = match format {
            TrackFormat::H264 { width, height, .. } => {
                ("h264", Some(*width), Some(*height), None, None)
            }
            TrackFormat::Hevc { width, height, .. } => {
                ("hevc", Some(*width), Some(*height), None, None)
            }
            TrackFormat::Aac {
                sample_rate,
                channels,
                ..
            } => ("aac", None, None, Some(*channels), Some(*sample_rate)),
        };
        Self {
            codec: codec.to_string(),
            width,
            height,
            channels,
            sample_rate,
        }
    }
}

/// A rendition's layout, in numbers.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LayoutNumbers {
    pub slots: u64,
    /// How much film every slot holds, when they all hold the same (an
    /// estimated layout's grid); `None` for slots of their own lengths.
    pub slot_ms: Option<u64>,
    /// Mirrored from the source's index, or estimated.
    pub exact: bool,
    /// The file's length in bytes.
    pub total: u64,
}

/// A live run, in numbers.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunNumbers {
    /// The slot it began at, and where that is on the film's clock (`None`
    /// before the layout is fixed).
    pub from: u64,
    pub from_ms: Option<u64>,
    /// How many slots it has made.
    pub produced: u64,
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
/// **Nothing else makes a slot**: production is the receiver's requests --
/// or a preparation making the ones the receiver will ask first, through
/// this same path -- and never where a player was told to start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ask {
    /// The first slot of a range the receiver chose.
    Seek,
    /// A range read on into its next slot.
    Continue,
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
        self.joined == 0
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
    /// [`RenditionTuning::lookahead`], in microseconds.
    lookahead_us: i64,
    failed: Option<String>,
    plan: SlotPlan,
    runs_started: u64,
    /// Slots made and the film they hold, since the publish
    /// ([`RenditionNumbers`]).
    slots_made: u64,
    film_made_us: i64,
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

    /// **The last slot of the lookahead from slot `slot`**: the last
    /// whose start is at most [`RenditionTuning::lookahead`] after
    /// `slot`'s, and never fewer than [`LOOKAHEAD`] slots on -- in slots
    /// alone before the layout is fixed.
    fn ahead(&self, slot: u64) -> u64 {
        let least = slot + LOOKAHEAD;
        match &self.layout {
            Some(layout) => layout.reach(slot, self.lookahead_us).max(least),
            None => least,
        }
    }

    /// Whether a request for `slot` waits for `run`: the slot in production
    /// or within the lookahead past it.
    fn joins(&self, run: &RunSlot, slot: u64) -> bool {
        slot >= run.next_out && slot <= self.ahead(run.next_out)
    }

    /// Whether `run` made `slot` or is making it: what a request answered
    /// from the ring moves the lookahead of.
    fn covers(&self, run: &RunSlot, slot: u64) -> bool {
        slot >= run.from && slot <= self.ahead(run.next_out)
    }

    /// Whether run `generation` must wait for a request before it makes
    /// slot `next_out`: it is past the lookahead from the last one asked of
    /// it, or the ring is full and it is past that.
    pub(crate) fn gated(&self, generation: u64, next_out: u64) -> bool {
        self.run(generation).is_none_or(|run| {
            next_out > self.ahead(run.last_request)
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
        let anchors: Vec<(u64, u64)> = self
            .runs
            .iter()
            .map(|run| (run.last_request, self.ahead(run.last_request)))
            .collect();
        while self.ring_bytes > self.ring_cap {
            let far = self
                .ring
                .keys()
                .copied()
                .filter(|key| {
                    !anchors
                        .iter()
                        .any(|(anchor, ahead)| key >= anchor && key <= ahead)
                })
                .max_by_key(|key| {
                    anchors
                        .iter()
                        .map(|(anchor, _)| key.abs_diff(*anchor))
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
    /// What every run's reader reads, counted for the publication.
    pub(crate) source: Arc<crate::media::reader::SourceTally>,
    inner: Mutex<Inner>,
    /// Bumped on every change a request or a run may be waiting for.
    version: watch::Sender<u64>,
    /// A preparation was asked for ([`Self::begin_prepare`]): once.
    prepare_begun: AtomicBool,
    /// The preparation made the slot the receiver starts in.
    prepared: AtomicBool,
}

impl Rendition {
    pub(crate) fn new(
        id: MediaId,
        play: Option<PlayToken>,
        spec: RenditionSpec,
        producer: Arc<dyn Producer>,
        tuning: RenditionTuning,
        cut: CancellationToken,
        source: Arc<crate::media::reader::SourceTally>,
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
            source,
            inner: Mutex::new(Inner {
                formats: None,
                layout: None,
                ring: BTreeMap::new(),
                ring_bytes: 0,
                runs: Vec::new(),
                ring_cap: tuning.ring_cap,
                lookahead_us: i64::try_from(tuning.lookahead.as_micros()).unwrap_or(i64::MAX),
                failed: None,
                plan: SlotPlan::default(),
                runs_started: 0,
                slots_made: 0,
                film_made_us: 0,
            }),
            version: watch::channel(0).0,
            prepare_begun: AtomicBool::new(false),
            prepared: AtomicBool::new(false),
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

    /// How far the preparation has got ([`RenditionReadiness`]).
    pub(crate) fn readiness(&self) -> RenditionReadiness {
        if self.cut.is_cancelled() {
            return RenditionReadiness::Ended;
        }
        let inner = self.inner();
        if let Some(sentence) = &inner.failed {
            return RenditionReadiness::Failed {
                sentence: sentence.clone(),
            };
        }
        if self.prepared.load(Ordering::SeqCst) {
            RenditionReadiness::Ready
        } else if inner.layout.is_none() {
            RenditionReadiness::Index
        } else {
            RenditionReadiness::Start
        }
    }

    /// Whether this is the first ask for a preparation: the caller starts
    /// one ([`Self::prepare`]) only then.
    pub(crate) fn begin_prepare(&self) -> bool {
        !self.prepare_begun.swap(true, Ordering::SeqCst)
    }

    /// **Ask for what the receiver will ask for first, before it does** --
    /// a simulated receiver, through the same request path as the real
    /// one, so nothing here produces anything a request would not: the
    /// header (which waits for the layout: the source's formats and index),
    /// slot 0 (Chrome's FFmpeg demuxer reads on from the header into the
    /// first fragment before it seeks), then every slot from the one a
    /// receiver told to start at `spec.start_ms` jumps to
    /// ([`layout::Layout::slot_for_time`]) to the one for
    /// [`PREPARED_AFTER`] after it, in order, so one run makes them all.
    ///
    /// **Nothing before the start's slot.** zond's TV used to ask first for
    /// a slot before it, and for a while the 12 s before the start were
    /// prepared against that. It was Chrome asking from the start of the
    /// 32 KiB block the slot's first byte was in -- the tail of the slot
    /// before -- and slots begin on those blocks now
    /// ([`layout::SLOT_ALIGN`]): three loads on the TV at 123.0 s, 97.3 s
    /// (the last half second before a key) and 45.0 s each asked for the
    /// file's start and then this slot, nothing else. Film before the
    /// start is film a torrent would have to fetch before the cast begins.
    /// The film after it is a slot in hand once it plays.
    ///
    /// Its first requests then find these in the ring -- held by its cap
    /// like any slot ([`RING_CAP`]: slot 0 with its run's lookahead and the
    /// prepared slots with theirs are some 30 s of film, which fit it up to
    /// some 25 Mbit/s; past that the farthest from the last one asked go
    /// first). Waits as long as the source takes, as a request does: a run
    /// with a request waiting on it is never let go, and nothing gives up
    /// (the viewer cancels by unpublishing, which ends this with
    /// [`NotServed::Cut`]).
    pub(crate) async fn prepare(self: &Arc<Self>, state: &AppState) -> Result<u64, NotServed> {
        let layout = self.layout(state).await?;
        let us = |duration: Duration| i64::try_from(duration.as_micros()).unwrap_or(i64::MAX);
        let start_us = us(Duration::from_millis(self.spec.start_ms)).min(self.duration_us());
        let slot = layout.slot_for_time(start_us);
        let last = layout.slot_for_time(start_us.saturating_add(us(PREPARED_AFTER)));
        self.slot(state, 0, Ask::Seek).await?;
        for prepared in slot..=last.max(slot) {
            self.slot(state, prepared, Ask::Seek).await?;
        }
        self.prepared.store(true, Ordering::SeqCst);
        self.bump();
        tracing::info!(slot, stage = "rendition_prepared", "rendition prepared");
        Ok(slot)
    }

    /// Where slot `slot` ends on the film's clock: where the next begins,
    /// or the film's end.
    fn slot_end_us(&self, layout: &layout::Layout, slot: u64) -> i64 {
        if slot + 1 >= layout.slots.len() as u64 {
            self.duration_us()
        } else {
            slot_start_us(layout, slot + 1)
        }
    }

    /// **What it has made, and where** ([`RenditionNumbers`]), with
    /// `asked` the slot the receiver last asked for. One look under the
    /// lock; nothing waits.
    pub(crate) fn numbers(&self, asked: Option<u64>) -> RenditionNumbers {
        let inner = self.inner();
        let layout = inner.layout.as_deref();
        let ms = |us: i64| (us.max(0) / 1000) as u64;
        let at_ms = |slot: u64| layout.map(|layout| ms(slot_start_us(layout, slot)));
        let made_to = asked
            .filter(|slot| inner.ring.contains_key(slot))
            .map(|asked| {
                let mut to = asked;
                for slot in inner.ring.range(asked + 1..).map(|(slot, _)| *slot) {
                    if slot != to + 1 {
                        break;
                    }
                    to = slot;
                }
                to
            });
        RenditionNumbers {
            video: self.spec.video.clone(),
            audio: self.spec.audio.clone(),
            video_out: inner
                .formats
                .as_ref()
                .and_then(|formats| formats.video.as_ref())
                .map(TrackOut::of),
            audio_out: inner
                .formats
                .as_ref()
                .and_then(|formats| formats.audio.as_ref())
                .map(TrackOut::of),
            layout: layout.map(|layout| LayoutNumbers {
                slots: layout.slots.len() as u64,
                slot_ms: uniform_ms(layout),
                exact: layout.exact,
                total: layout.total,
            }),
            runs: inner
                .runs
                .iter()
                .map(|run| RunNumbers {
                    from: run.from,
                    from_ms: at_ms(run.from),
                    produced: run.next_out.saturating_sub(run.from),
                })
                .collect(),
            runs_started: inner.runs_started,
            slots_made: inner.slots_made,
            film_made_ms: ms(inner.film_made_us),
            asked_slot: asked,
            asked_ms: asked.and_then(at_ms),
            made_to,
            made_to_ms: made_to
                .zip(layout)
                .map(|(slot, layout)| ms(self.slot_end_us(layout, slot))),
        }
    }

    /// Runs begun since the publish.
    pub(crate) fn runs_started(&self) -> u64 {
        self.inner().runs_started
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
    /// run that will freeze it, from the film's start --
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
            // The run that fixes the layout, for the file's header: from
            // the film's start, which is what a reader of the header reads
            // on into. Never from where a player was told to start.
            _ => (0, 0),
        };
        let stop = self.cut.child_token();
        // Its lookahead counts from where it starts: whoever asked last.
        inner.runs.push(RunSlot {
            generation,
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
                    layout::Layout::new(
                        init,
                        plan,
                        (track, timescale, formats.decode_ahead_us()),
                        formats.sound_beside_picture(),
                        self.duration_us(),
                    )
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
            // The run that froze the layout, for the header's reader: it
            // reads on into slot 0.
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
            inner.slots_made += 1;
            if let Some(layout) = &inner.layout {
                let span = self.slot_end_us(layout, slot) - slot_start_us(layout, slot);
                inner.film_made_us = inner.film_made_us.saturating_add(span.max(0));
            }
            let end = filled.end_now(slot + 1 >= count || inner.plan.decided(slot + 1));
            let recorded = *inner.plan.ends.entry(slot).or_insert(end);
            inner.insert(slot, filled.fragment);
            recorded
        };
        self.bump();
        Some(recorded)
    }

    /// **The file's layout**: frozen by the first run's first sample --
    /// starting that run, from the film's start, if none is live.
    pub(crate) async fn layout(
        self: &Arc<Self>,
        state: &AppState,
    ) -> Result<Arc<layout::Layout>, NotServed> {
        let mut seen = self.version.subscribe();
        // Counted among the first run's waiters while this waits: a run is
        // never let go while something waits for what it makes, and the
        // source's index can take minutes behind a thin swarm.
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
                if let Some(layout) = &inner.layout {
                    return Ok(layout.clone());
                }
                if inner.runs.is_empty() {
                    self.start_run(&mut inner, state, None, false);
                }
                let first = inner.runs.first().map(|run| run.generation);
                joined.set(&mut inner, first);
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
                        .filter(|run| inner.covers(run, slot))
                        .max_by_key(|run| run.from)
                        .map(|run| run.generation);
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
                    .find(|run| inner.joins(run, slot))
                    .map(|run| run.generation);
                let unwaited = inner.runs.iter().filter(|run| run.unwaited()).count();
                let start = ask.start(first, inner.runs.len(), unwaited);
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

/// Where slot `slot` begins on the film's clock: its cut, the first slot
/// from the film's first sync sample.
fn slot_start_us(layout: &layout::Layout, slot: u64) -> i64 {
    match slot {
        0 => layout.first_us,
        _ => layout.cuts.get(slot as usize).copied().unwrap_or(i64::MAX),
    }
}

/// How much film each slot after the first holds, in milliseconds, when
/// every one holds the same: an estimated layout's grid.
fn uniform_ms(layout: &layout::Layout) -> Option<u64> {
    let cuts = layout.cuts.get(1..layout.slots.len())?;
    let step = cuts.get(1)?.checked_sub(*cuts.first()?)?;
    (step > 0 && cuts.windows(2).all(|pair| pair[1] - pair[0] == step))
        .then_some((step / 1000) as u64)
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
        for ask in [Ask::Seek, Ask::Continue] {
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
    }

    fn inner_with(runs: &[u64], ring: &[u64], cap: usize) -> Inner {
        let mut inner = Inner {
            formats: None,
            layout: None,
            ring: BTreeMap::new(),
            ring_bytes: 0,
            runs: Vec::new(),
            ring_cap: usize::MAX,
            lookahead_us: 0,
            failed: None,
            plan: SlotPlan::default(),
            runs_started: 0,
            slots_made: 0,
            film_made_us: 0,
        };
        for (generation, at) in runs.iter().enumerate() {
            inner.runs.push(RunSlot {
                generation: generation as u64,
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
        assert!(run.unwaited());
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
        for (readiness, json) in [
            (
                RenditionReadiness::Index,
                serde_json::json!({"phase": "index"}),
            ),
            (
                RenditionReadiness::Start,
                serde_json::json!({"phase": "start"}),
            ),
            (
                RenditionReadiness::Ready,
                serde_json::json!({"phase": "ready"}),
            ),
            (
                RenditionReadiness::Ended,
                serde_json::json!({"phase": "ended"}),
            ),
            (
                RenditionReadiness::Failed {
                    sentence: "no".into(),
                },
                serde_json::json!({"phase": "failed", "sentence": "no"}),
            ),
        ] {
            assert_eq!(serde_json::to_value(readiness).unwrap(), json);
        }
    }
}
