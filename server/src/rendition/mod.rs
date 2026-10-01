//! **Renditions**: a cast the receiver can decode, produced on demand,
//! nothing on disk (`docs/design/renditions.md`, step F1 of
//! `docs/design/media-pipeline.md` §5).
//!
//! A rendition is a published cast token ([`crate::ServerHandle::publish_rendition`])
//! with **one progressive fragmented MP4** behind it
//! (`/cast/{token}/stream.mp4`, `crate::cast`): the init segment, then
//! media segments in order from a start time, each produced on demand by a
//! [`Producer`](crate::rendition::Producer) the embedder installed -- the
//! thing that demuxes, decodes and encodes, which the server never does
//! itself. The server's half is everything else: the route, the cut rule,
//! the fMP4 muxer (`mux.rs`), the ring of segments in memory, and the
//! speed a run is judged by.
//!
//! **Not HLS.** zond's Chromecast with Google TV plays no HLS above 720p
//! through its Media Source path -- ours, ffmpeg's or Mux's own, TS or
//! fMP4, at any bitrate -- and plays the same film as a progressive
//! fragmented MP4 through its plain `<video src>` path
//! (`docs/design/renditions.md`, F2). A segment is an internal unit: the
//! stream is the segments one after another, and a seek is a new stream
//! from another start.
//!
//! # The rules this keeps
//!
//! * **Nothing on disk.** Segments live in a ring in memory -- two behind
//!   the last request, [`LOOKAHEAD`](crate::rendition::LOOKAHEAD) ahead, [`RING_CAP`](crate::rendition::RING_CAP) at most -- and are
//!   dropped.
//! * **The cut rule**: segment N begins at the first video sync sample whose
//!   presentation time is at or after N x T and ends where N+1 begins; an
//!   audio sample belongs to the segment its presentation time falls in on
//!   the N x T grid. What a run is handed before its first cut is
//!   discarded. A segment is produced whole before it is sent.
//! * **A request for segment N** is answered from the ring; or waits, when
//!   N is the segment in production or at most [`LOOKAHEAD`](crate::rendition::LOOKAHEAD) past it (and
//!   joins that production, never restarts it); or is a seek: the run is
//!   dropped and a new one starts at N x T.
//! * **The lookahead blocks the producer**: a run completes at most
//!   [`LOOKAHEAD`](crate::rendition::LOOKAHEAD) segments past the last request, then stops reading its
//!   sink, and the producer's next write blocks.
//! * **An idle run is let go** after [`IDLE_RELEASE`](crate::rendition::IDLE_RELEASE) without a request; the
//!   ring is kept and the next request starts a new run where it asks.
//! * **The first run's formats are frozen** into the init segment; a later run
//!   whose formats differ fails the rendition.
//! * **A run slower than real time fails the rendition with a sentence**:
//!   under 1.0x over [`SPEED_WINDOW`](crate::rendition::SPEED_WINDOW) of busy time once its first segment
//!   is out, where busy time leaves out the sink's and the reader's waits
//!   (`speed.rs`).
//! * **Cut by unpublish**, as a plain cast is: the run is dropped (its sink
//!   answers [`Stopped`](crate::rendition::Stopped), its reader is cancelled), the ring goes, and a
//!   request waiting on a segment is answered with an error.

pub(crate) mod mux;
mod run;
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
    /// Start here: N x T for a run that begins at segment N. The producer
    /// starts at the sync sample at or before it; the server discards what
    /// precedes the cut.
    pub from: Duration,
    pub sink: SampleSink,
}

/// What a sink carries into its run.
pub(crate) enum SinkMessage {
    Format(TrackKind, TrackFormat),
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
    /// The segment the live run began at, if one is live.
    pub run_from: Option<u64>,
    /// The segment the live run is producing.
    pub in_production: Option<u64>,
    /// The segments in the ring.
    pub ring: Vec<u64>,
    /// Whether the init segment is frozen.
    pub init: bool,
}

/// Why a request under a rendition is not answered with bytes.
#[derive(Debug, PartialEq, Eq)]
pub enum NotServed {
    /// Past the last segment, or past the film's end.
    NotFound,
    /// The rendition failed; the sentence.
    Failed(String),
    /// The publication was cut while the request waited.
    Cut,
}

/// The live run, as a request sees it.
struct RunSlot {
    generation: u64,
    from: u64,
    /// The segment in production: the lowest not yet in the ring.
    next_out: u64,
    stop: CancellationToken,
}

struct Inner {
    formats: Option<mux::Formats>,
    init: Option<Bytes>,
    ring: BTreeMap<u64, Bytes>,
    ring_bytes: usize,
    run: Option<RunSlot>,
    failed: Option<String>,
    /// The first segment past the film's end, once a run reached it.
    end: Option<u64>,
    last_request: u64,
    last_request_at: Instant,
    runs_started: u64,
}

impl Inner {
    fn note_request(&mut self, segment: u64, now: Instant) {
        self.last_request = segment;
        self.last_request_at = now;
        let (low, high) = (segment.saturating_sub(BEHIND), segment + LOOKAHEAD);
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
            let Some(oldest) = self
                .ring
                .keys()
                .next()
                .copied()
                .filter(|key| *key < segment)
            else {
                break;
            };
            if let Some(bytes) = self.ring.remove(&oldest) {
                self.ring_bytes -= bytes.len();
            }
        }
    }

    fn insert(&mut self, segment: u64, bytes: Bytes) {
        self.ring_bytes += bytes.len();
        if let Some(old) = self.ring.insert(segment, bytes) {
            self.ring_bytes -= old.len();
        }
    }
}

/// One published rendition: the spec, the producer, and the ring with the
/// run that fills it.
pub(crate) struct Rendition {
    id: MediaId,
    play: Option<PlayToken>,
    spec: RenditionSpec,
    count: u64,
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
        let count = spec.duration_ms.div_ceil(u64::from(spec.segment_ms));
        Ok(Self {
            count,
            id,
            play,
            spec,
            producer,
            tuning,
            cut,
            inner: Mutex::new(Inner {
                formats: None,
                init: None,
                ring: BTreeMap::new(),
                ring_bytes: 0,
                run: None,
                failed: None,
                end: None,
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

    /// How many segments the film is cut into: `ceil(duration / T)`.
    pub(crate) fn count(&self) -> u64 {
        self.count
    }

    /// The segment a stream asked to start at `from_ms` begins with -- the
    /// spec's start when it names none -- held inside the film.
    pub(crate) fn first_segment(&self, from_ms: Option<u64>) -> u64 {
        let from = from_ms.unwrap_or(self.spec.start_ms);
        (from / u64::from(self.spec.segment_ms)).min(self.count - 1)
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
        RenditionProbe {
            runs_started: inner.runs_started,
            run_from: inner.run.as_ref().map(|run| run.from),
            in_production: inner.run.as_ref().map(|run| run.next_out),
            ring: inner.ring.keys().copied().collect(),
            init: inner.init.is_some(),
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

    /// Start a run at `segment`, dropping the live one: the seek path.
    fn start_run(self: &Arc<Self>, inner: &mut Inner, state: &AppState, segment: u64) {
        if let Some(old) = inner.run.take() {
            old.stop.cancel();
        }
        inner.runs_started += 1;
        let generation = inner.runs_started;
        let stop = self.cut.child_token();
        inner.run = Some(RunSlot {
            generation,
            from: segment,
            next_out: segment,
            stop: stop.clone(),
        });
        tracing::info!(
            from = segment,
            runs = generation,
            stage = "rendition_run_start",
            "rendition run started"
        );
        tokio::spawn(run::run(
            self.clone(),
            state.clone(),
            generation,
            segment,
            stop,
        ));
    }

    /// The init segment: frozen from the first run's formats, waiting for
    /// them -- and starting that run at the spec's start, if none is live.
    pub(crate) async fn init(self: &Arc<Self>, state: &AppState) -> Result<Bytes, NotServed> {
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
                if let Some(init) = &inner.init {
                    return Ok(init.clone());
                }
                if inner.run.is_none() {
                    let start =
                        (self.spec.start_ms / u64::from(self.spec.segment_ms)).min(self.count - 1);
                    inner.note_request(start, Instant::now());
                    self.start_run(&mut inner, state, start);
                }
            }
            if !self.wait(&mut seen).await {
                return Err(NotServed::Cut);
            }
        }
    }

    /// Segment `segment`: from the ring, after the production it joins, or
    /// from a new run started at it.
    pub(crate) async fn segment(
        self: &Arc<Self>,
        state: &AppState,
        segment: u64,
    ) -> Result<Bytes, NotServed> {
        if segment >= self.count {
            return Err(NotServed::NotFound);
        }
        let mut seen = self.version.subscribe();
        let mut noted = false;
        loop {
            {
                let mut inner = self.inner();
                if self.cut.is_cancelled() {
                    return Err(NotServed::Cut);
                }
                if let Some(sentence) = &inner.failed {
                    return Err(NotServed::Failed(sentence.clone()));
                }
                if !noted {
                    noted = true;
                    inner.note_request(segment, Instant::now());
                    // The gate moved: a run waiting on it looks again.
                    self.bump();
                }
                if let Some(bytes) = inner.ring.get(&segment) {
                    return Ok(bytes.clone());
                }
                if inner.end.is_some_and(|end| segment >= end) {
                    return Err(NotServed::NotFound);
                }
                let joins = inner.run.as_ref().is_some_and(|run| {
                    segment >= run.next_out && segment <= run.next_out + LOOKAHEAD
                });
                if !joins {
                    self.start_run(&mut inner, state, segment);
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
