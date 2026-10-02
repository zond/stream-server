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
//!   the last request, [`LOOKAHEAD`](crate::rendition::LOOKAHEAD) ahead, [`RING_CAP`](crate::rendition::RING_CAP) at most -- and are
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
//!   not move the first run ([`Ask::Open`](crate::rendition::Ask::Open)).
//! * **The lookahead blocks the producer**: a run completes at most
//!   [`LOOKAHEAD`](crate::rendition::LOOKAHEAD) slots past the last request, then stops reading its
//!   sink, and the producer's next write blocks.
//! * **An idle run is let go** after [`IDLE_RELEASE`](crate::rendition::IDLE_RELEASE) without a request; the
//!   ring is kept and the next request starts a new run where it asks.
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
/// Segments kept behind the last request, for a receiver's retry.
pub const BEHIND: u64 = 2;
/// The most the ring holds, whatever the segment count says.
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
}

impl Default for RenditionTuning {
    fn default() -> Self {
        Self {
            idle_release: IDLE_RELEASE,
            speed_window: SPEED_WINDOW,
        }
    }
}

/// What a rendition holds right now, for the tests.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenditionProbe {
    /// Runs begun since the publish.
    pub runs_started: u64,
    /// The slot the live run began at, if one is live.
    pub run_from: Option<u64>,
    /// The slot the live run is producing.
    pub in_production: Option<u64>,
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

/// What a request for a slot is (`docs/design/renditions.md`, "Seeking by
/// bytes").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ask {
    /// A read the receiver chose: a range that begins in the slot, or one
    /// read on into it. It may move the run there.
    Seek,
    /// The receiver opening the file: a read from the header on, into the
    /// first slot it reaches without having chosen it. It does not move the
    /// first run -- started at the spec's start, where the receiver was told
    /// to play and is about to seek -- before any read has asked for a
    /// slot; it waits for that run instead, or for none to be live.
    Open,
}

/// The live run, as a request sees it.
struct RunSlot {
    generation: u64,
    /// The first run, which no read has asked for a slot of yet: an
    /// [`Ask::Open`] leaves it where it is.
    reserved: bool,
    /// The slot it began at; for the run that freezes the layout, known
    /// once it has.
    from: u64,
    /// The slot in production: the lowest not yet in the ring.
    next_out: u64,
    stop: CancellationToken,
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

struct Inner {
    formats: Option<mux::Formats>,
    layout: Option<Arc<layout::Layout>>,
    /// Each slot's fragment, by slot; the padding is not kept.
    ring: BTreeMap<u64, Bytes>,
    ring_bytes: usize,
    run: Option<RunSlot>,
    failed: Option<String>,
    plan: SlotPlan,
    last_request: u64,
    last_request_at: Instant,
    runs_started: u64,
}

impl Inner {
    fn note_request(&mut self, slot: u64, now: Instant) {
        self.last_request = slot;
        self.last_request_at = now;
        let (low, high) = (slot.saturating_sub(BEHIND), slot + LOOKAHEAD);
        let gone: Vec<u64> = self
            .ring
            .keys()
            .copied()
            .filter(|key| *key < low || *key > high)
            .collect();
        for key in gone {
            if let Some(bytes) = self.ring.remove(&key) {
                self.ring_bytes -= bytes.len();
            }
        }
        // The cap: the oldest behind the request go first; nothing at or
        // ahead of it is taken (the gate stops production instead).
        while self.ring_bytes > RING_CAP {
            let Some(oldest) = self.ring.keys().next().copied().filter(|key| *key < slot) else {
                break;
            };
            if let Some(bytes) = self.ring.remove(&oldest) {
                self.ring_bytes -= bytes.len();
            }
        }
    }

    fn insert(&mut self, slot: u64, bytes: Bytes) {
        self.ring_bytes += bytes.len();
        if let Some(old) = self.ring.insert(slot, bytes) {
            self.ring_bytes -= old.len();
        }
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
                run: None,
                failed: None,
                plan: SlotPlan::default(),
                last_request: 0,
                last_request_at: Instant::now(),
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
        if inner.run.is_some() {
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
            run_from: inner.run.as_ref().map(|run| run.from),
            in_production: inner.run.as_ref().map(|run| run.next_out),
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
        if let Some(run) = inner.run.take() {
            run.stop.cancel();
        }
    }

    /// Start a run, dropping the live one: at `slot` (the seek path), or --
    /// with no layout yet -- the run that will freeze it, from a segment
    /// before the spec's start.
    fn start_run(self: &Arc<Self>, inner: &mut Inner, state: &AppState, slot: Option<u64>) {
        if let Some(old) = inner.run.take() {
            old.stop.cancel();
        }
        inner.runs_started += 1;
        let generation = inner.runs_started;
        let back = i64::try_from(SEEK_BACK.as_micros()).unwrap_or(i64::MAX);
        let (from_us, from_slot) = match (slot, inner.layout.clone()) {
            (Some(slot), Some(layout)) => {
                // A run is started here: the slot's start is decided now, if
                // the slot before has not decided it.
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
        inner.run = Some(RunSlot {
            generation,
            reserved: slot.is_none(),
            from: from_slot,
            next_out: from_slot,
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
        if !inner
            .run
            .as_ref()
            .is_some_and(|run| run.generation == generation)
        {
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
            if let Some(run) = inner.run.as_mut() {
                run.from = slot;
                run.next_out = slot;
            }
            inner.note_request(slot, Instant::now());
        }
        drop(inner);
        self.bump();
        Ok((layout, slot, cursor))
    }

    /// Whether slot `slot`, made for the first time, may spill into the
    /// next: there is one, and its start is not decided.
    pub(crate) fn may_spill(&self, slot: u64) -> bool {
        let inner = self.inner();
        let count = inner.layout.as_ref().map_or(0, |layout| layout.slots.len()) as u64;
        slot + 1 < count && !inner.plan.decided(slot + 1)
    }

    /// Slot `slot` made, under the run's generation: its fragment in the
    /// ring, and its end recorded the first time. Answers the end recorded
    /// -- this one, or the one it was first made with -- or `None` when the
    /// run is no longer the live one.
    pub(crate) fn publish_slot(
        &self,
        generation: u64,
        slot: u64,
        end: slots::End,
        fragment: Bytes,
    ) -> Option<slots::End> {
        let recorded = {
            let mut inner = self.inner();
            let run = inner
                .run
                .as_mut()
                .filter(|run| run.generation == generation)?;
            run.next_out = slot + 1;
            let recorded = *inner.plan.ends.entry(slot).or_insert(end);
            inner.insert(slot, fragment);
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
                if inner.run.is_none() {
                    inner.last_request_at = Instant::now();
                    self.start_run(&mut inner, state, None);
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

    /// **Slot `slot`'s fragment**: from the ring, after the production it
    /// joins, or from a new run started at it -- except that an
    /// [`Ask::Open`] waits for the first run rather than move it ([`Ask`]).
    pub(crate) async fn slot(
        self: &Arc<Self>,
        state: &AppState,
        slot: u64,
        ask: Ask,
    ) -> Result<Bytes, NotServed> {
        let mut seen = self.version.subscribe();
        let mut noted = false;
        let mut deferring = false;
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
                let in_ring = inner.ring.contains_key(&slot);
                let joins = inner
                    .run
                    .as_ref()
                    .is_some_and(|run| slot >= run.next_out && slot <= run.next_out + LOOKAHEAD);
                // Once an opening read has waited for the first run, it
                // goes on waiting while any run is live: the receiver's
                // jump to its start has asked for that run by then.
                deferring = ask == Ask::Open
                    && !in_ring
                    && !joins
                    && inner
                        .run
                        .as_ref()
                        .is_some_and(|run| deferring || run.reserved);
                if !deferring {
                    if !noted {
                        noted = true;
                        inner.note_request(slot, Instant::now());
                        // The gate moved: a run waiting on it looks again.
                        self.bump();
                    }
                    if let Some(run) = inner.run.as_mut() {
                        run.reserved = false;
                    }
                    if let Some(bytes) = inner.ring.get(&slot) {
                        return Ok(bytes.clone());
                    }
                    if !joins {
                        self.start_run(&mut inner, state, Some(slot));
                    }
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
