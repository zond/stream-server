//! A rendition producer for the tests (`docs/design/renditions.md` §5 F1).
//! It is Rust, on a plain thread per run, emitting synthetic samples
//! through the same `Producer` trait the embedder's Kotlin producer
//! implements.
//!
//! It emits H.264-shaped video: a fixed GOP, a sync sample every
//! [`Knobs::gop`] frames at [`Knobs::fps`], x264's real SPS and PPS as
//! `csd-0`/`csd-1`, and NAL units in Annex-B whose payload names the
//! frame. It emits AAC frames every 1024 samples at 48 kHz (21.33 ms) with
//! a stereo AAC-LC AudioSpecificConfig. A run from `from` starts as a
//! demuxer does: video at the sync sample at or before it, audio one frame
//! before it.
//!
//! The knobs: a speed factor (a producer slower than real time paces
//! itself, which is the work being simulated, not a wait for anything); a
//! run that reports another format; a run that fails; reads from the
//! job's reader at every sync sample, so a slow source is a slow producer
//! in wall time and not in busy time; the size of a video frame; and the
//! index it reports when the job wants one ([`IndexKnob`]): every sync
//! sample at a position in proportion to its time over the reader's
//! length, one squeezed against the next, or none.
//!
//! **Converting** ([`Knobs::convert`]): the run feeds its video through a
//! `ConvertedPicture` over a [`fake_converter::FakeConverter`] with the
//! job's segments, as the embedder's producer does. It sends the sound's
//! format, then holds the sound until the picture's format has gone. With
//! [`Knobs::open_gop`], the film's GOPs are open: each sync picture is
//! followed in decode order by the last pictures of the GOP before.

#![allow(dead_code)]

#[path = "fake_converter.rs"]
pub mod fake_converter;

use bytes::Bytes;
use fake_converter::{ConverterRecord, FakeConverter, FakeOptions};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use stream_server::rendition::SinkProbe;
use stream_server::{
    Converted, ConvertedPicture, IndexEntry, Job, PictureTarget, Producer, ProducerRefusal, Sample,
    SourcePicture, TrackFormat, TrackKind,
};

/// x264's SPS for 320x240 High, Annex-B, as `csd-0` carries it.
pub const SPS: &[u8] = &[
    0, 0, 0, 1, 0x67, 0x64, 0x00, 0x0d, 0xac, 0xd9, 0x41, 0x41, 0xfb, 0x01, 0x10, 0x00, 0x00, 0x03,
    0x00, 0x10, 0x00, 0x00, 0x03, 0x03, 0x20, 0xf1, 0x42, 0x99, 0x60,
];
/// Its PPS, as `csd-1` carries it.
pub const PPS: &[u8] = &[0, 0, 0, 1, 0x68, 0xeb, 0xe3, 0xcb, 0x22, 0xc0];
/// An SPS a run that changes format reports instead: the same with
/// another level.
pub const OTHER_SPS: &[u8] = &[
    0, 0, 0, 1, 0x67, 0x64, 0x00, 0x1e, 0xac, 0xd9, 0x41, 0x41, 0xfb, 0x01, 0x10, 0x00, 0x00, 0x03,
    0x00, 0x10, 0x00, 0x00, 0x03, 0x03, 0x20, 0xf1, 0x42, 0x99, 0x60,
];
/// AAC-LC, 48 kHz, stereo.
pub const ASC: &[u8] = &[0x11, 0x90];
/// Samples per AAC frame.
const AAC_FRAME: i64 = 1024;
const AAC_RATE: i64 = 48_000;

/// The index a run reports when its job wants one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexKnob {
    /// Every sync sample, at the reader's length times its time over the
    /// film's.
    Proportional,
    /// The same, but the first sync sample at or after `at_us` is put one
    /// byte before the next (or the source's end, for the last). The
    /// segment it starts gets almost no bytes of the source, and overflows
    /// its slot.
    Squeezed { at_us: i64 },
}

#[derive(Clone, Debug)]
pub struct Knobs {
    pub fps: u32,
    /// Frames per GOP: a sync sample every `gop` frames.
    pub gop: u32,
    /// How long the film is: the producer ends there.
    pub length: Duration,
    /// Media time per wall time. `None` is as fast as it can.
    pub speed: Option<f64>,
    /// The run (counted from 0) that reports [`OTHER_SPS`].
    pub other_format_on_run: Option<usize>,
    /// The run that fails after a few samples, with this sentence.
    pub fail_on_run: Option<(usize, String)>,
    /// Read one byte of the job's reader at `stride` x the GOP's index at
    /// every sync sample. That is a seek and a read the source answers.
    pub read_stride: Option<u64>,
    /// Filler bytes in each video frame after its NAL header and number.
    pub frame_bytes: usize,
    /// The index reported. `None` reports none.
    pub index: Option<IndexKnob>,
    /// A gate the first run waits at before it reports its index (a source
    /// whose index is behind a stalled swarm). It waits until the test opens
    /// it, or the run is stopped.
    pub index_gate: Option<Arc<Gate>>,
    /// A gate every run waits at after its first sample, until the test
    /// opens it. The layout is fixed by then, and the slots are not made.
    pub start_gate: Option<Arc<Gate>>,
    /// A gate run `n` (counted from 0) alone waits at after its first
    /// sample, until the test opens it.
    pub run_gate: Option<(usize, Arc<Gate>)>,
    /// A gate every run waits at before it hands over the first sample at
    /// or after this time, until the test opens it. It is a source that
    /// stalls there.
    pub time_gate: Option<(i64, Arc<Gate>)>,
    /// Convert the picture into this, through a fake converter.
    pub convert: Option<PictureTarget>,
    /// How the fake converter behaves.
    pub converter: FakeOptions,
    /// Leading pictures per GOP: the last this many pictures of each GOP
    /// come after the next sync picture in decode order.
    pub open_gop: u32,
    /// A converting run sends its sound as it comes, not held until the
    /// picture's format has gone (a producer with the bug).
    pub unheld_sound: bool,
}

/// A source that stalls until opened ([`Knobs::index_gate`]), with a probe
/// for whether a run is parked at it.
#[derive(Debug, Default)]
pub struct Gate {
    open: Mutex<bool>,
    opened: std::sync::Condvar,
    parked: AtomicBool,
}

impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// A run is waiting at the gate right now.
    pub fn is_parked(&self) -> bool {
        self.parked.load(Ordering::SeqCst)
    }

    /// Let every run through.
    pub fn open(&self) {
        *self.open.lock().unwrap() = true;
        self.opened.notify_all();
    }

    /// Wait until the gate opens (`true`) or `stopped` says the run is gone
    /// (`false`). The stop is looked at between short waits, because the
    /// sink has no way to wake this.
    fn pass(&self, stopped: impl Fn() -> bool) -> bool {
        let mut open = self.open.lock().unwrap();
        self.parked.store(true, Ordering::SeqCst);
        while !*open {
            if stopped() {
                self.parked.store(false, Ordering::SeqCst);
                return false;
            }
            open = self
                .opened
                .wait_timeout(open, Duration::from_millis(5))
                .unwrap()
                .0;
        }
        self.parked.store(false, Ordering::SeqCst);
        true
    }
}

impl Default for Knobs {
    fn default() -> Self {
        Self {
            fps: 25,
            gop: 12,
            length: Duration::from_secs(60),
            speed: None,
            other_format_on_run: None,
            fail_on_run: None,
            read_stride: None,
            frame_bytes: 120,
            index: Some(IndexKnob::Proportional),
            index_gate: None,
            start_gate: None,
            run_gate: None,
            time_gate: None,
            convert: None,
            converter: FakeOptions::default(),
            open_gop: 0,
            unheld_sound: false,
        }
    }
}

impl Knobs {
    pub fn video_pts(&self, frame: i64) -> i64 {
        frame * 1_000_000 / i64::from(self.fps)
    }

    pub fn is_key(&self, frame: i64) -> bool {
        frame % i64::from(self.gop) == 0
    }

    pub fn audio_pts(frame: i64) -> i64 {
        (frame * AAC_FRAME * 1_000_000 + AAC_RATE / 2) / AAC_RATE
    }

    /// Every video frame's presentation time, in decode order (which is
    /// presentation order here).
    pub fn video_frames(&self) -> Vec<(i64, bool)> {
        let length = self.length.as_micros() as i64;
        (0..)
            .map(|frame| (self.video_pts(frame), self.is_key(frame)))
            .take_while(|(pts, _)| *pts < length)
            .collect()
    }

    /// Every audio frame's presentation time.
    pub fn audio_frames(&self) -> Vec<i64> {
        let length = self.length.as_micros() as i64;
        (0..)
            .map(Self::audio_pts)
            .take_while(|pts| *pts < length)
            .collect()
    }

    /// Every sync sample's index entry for a source `len` bytes long, as
    /// `knob` places them.
    pub fn index(&self, knob: IndexKnob, len: u64) -> Vec<IndexEntry> {
        let length = self.length.as_micros() as i64;
        let place = |pts: i64| (pts as i128 * len as i128 / length as i128) as u64;
        let keys: Vec<i64> = self
            .video_frames()
            .into_iter()
            .filter(|(_, key)| *key)
            .map(|(pts, _)| pts)
            .collect();
        let squeezed = match knob {
            IndexKnob::Squeezed { at_us } => keys.iter().position(|pts| *pts >= at_us),
            IndexKnob::Proportional => None,
        };
        keys.iter()
            .enumerate()
            .map(|(at, pts)| IndexEntry {
                pts_us: *pts,
                pos: if Some(at) == squeezed {
                    keys.get(at + 1).map_or(len - 1, |next| place(*next) - 1)
                } else {
                    place(*pts)
                },
            })
            .collect()
    }

    /// Every video frame's number in decode order. With [`Self::open_gop`]
    /// at `n`, the last `n` frames of each GOP come right after the next
    /// GOP's sync frame (the film's last GOP's at the end).
    pub fn decode_order(&self) -> Vec<i64> {
        let count = self.video_frames().len() as i64;
        let gop = i64::from(self.gop);
        let lead = i64::from(self.open_gop).min(gop - 1);
        let leading = |frame: i64| frame % gop >= gop - lead;
        let mut order = Vec::with_capacity(count as usize);
        for frame in 0..count {
            if leading(frame) {
                continue;
            }
            order.push(frame);
            if self.is_key(frame) && frame > 0 {
                order.extend((frame - lead).max(0)..frame);
            }
        }
        let last_key = (count - 1) / gop * gop;
        order.extend((last_key + 1..count).filter(|frame| leading(*frame)));
        order
    }

    /// The first sync sample at or after `at_us`.
    pub fn key_at_or_after(&self, at_us: i64) -> Option<i64> {
        self.video_frames()
            .into_iter()
            .find(|(pts, key)| *key && *pts >= at_us)
            .map(|(pts, _)| pts)
    }
}

/// What one run did, as the tests read it.
pub struct RunRecord {
    pub from: Duration,
    /// The job asked for the source's index.
    pub wanted_index: bool,
    pub probe: SinkProbe,
    /// The sink answered `Stopped`.
    pub stopped: AtomicBool,
    /// The run reached the end of the film.
    pub ended: AtomicBool,
    /// `(track, pts)` of every sample the sink took.
    pub emitted: Mutex<Vec<(TrackKind, i64)>>,
    /// The thread is gone.
    pub done: AtomicBool,
    /// What its converter did, when it converts.
    pub converter: Arc<ConverterRecord>,
}

impl RunRecord {
    pub fn stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    pub fn done(&self) -> bool {
        self.done.load(Ordering::SeqCst)
    }

    pub fn emitted(&self) -> Vec<(TrackKind, i64)> {
        self.emitted.lock().unwrap().clone()
    }
}

pub struct TestProducer {
    pub knobs: Knobs,
    runs: Mutex<Vec<Arc<RunRecord>>>,
}

impl TestProducer {
    pub fn new(knobs: Knobs) -> Arc<Self> {
        Arc::new(Self {
            knobs,
            runs: Mutex::new(Vec::new()),
        })
    }

    pub fn runs(&self) -> Vec<Arc<RunRecord>> {
        self.runs.lock().unwrap().clone()
    }
}

impl Producer for TestProducer {
    fn start(&self, job: Job) -> Result<(), ProducerRefusal> {
        let record = Arc::new(RunRecord {
            from: job.from,
            wanted_index: job.wants_index,
            probe: job.sink.probe(),
            stopped: AtomicBool::new(false),
            ended: AtomicBool::new(false),
            emitted: Mutex::new(Vec::new()),
            done: AtomicBool::new(false),
            converter: Arc::new(ConverterRecord::default()),
        });
        let index = {
            let mut runs = self.runs.lock().unwrap();
            runs.push(record.clone());
            runs.len() - 1
        };
        let knobs = self.knobs.clone();
        std::thread::spawn(move || {
            match knobs.convert {
                Some(target) => produce_converted(&knobs, index, job, &record, target),
                None => produce(&knobs, index, job, &record),
            }
            record.done.store(true, Ordering::SeqCst);
        });
        Ok(())
    }
}

/// A frame number as four bytes with the top bit set in each, so a NAL
/// payload never holds a start code. That gives 28 bits, seven to a byte.
pub fn frame_bytes(frame: i64) -> [u8; 4] {
    let frame = frame as u32;
    [
        0x80 | ((frame >> 21) & 0x7f) as u8,
        0x80 | ((frame >> 14) & 0x7f) as u8,
        0x80 | ((frame >> 7) & 0x7f) as u8,
        0x80 | (frame & 0x7f) as u8,
    ]
}

/// [`frame_bytes`] read back.
pub fn frame_of(bytes: &[u8]) -> i64 {
    bytes[..4]
        .iter()
        .fold(0i64, |frame, byte| (frame << 7) | i64::from(byte & 0x7f))
}

fn video_payload(frame: i64, key: bool, filler: usize) -> Bytes {
    let mut data = vec![0, 0, 0, 1, if key { 0x65 } else { 0x41 }];
    data.extend_from_slice(&frame_bytes(frame));
    data.extend(std::iter::repeat_n(0xab, filler));
    Bytes::from(data)
}

fn audio_payload(frame: i64) -> Bytes {
    let mut data = (frame as u32).to_be_bytes().to_vec();
    data.extend(std::iter::repeat_n(0x21, 28));
    Bytes::from(data)
}

fn produce(knobs: &Knobs, index: usize, job: Job, record: &RunRecord) {
    let Job {
        mut reader,
        from,
        sink,
        wants_index,
        ..
    } = job;
    let from_us = from.as_micros() as i64;
    let gop = i64::from(knobs.gop);
    let mut video = from_us * i64::from(knobs.fps) / 1_000_000 / gop * gop;
    let mut audio = (from_us * AAC_RATE / AAC_FRAME / 1_000_000 - 1).max(0);
    let length = knobs.length.as_micros() as i64;
    let sps = if knobs.other_format_on_run == Some(index) {
        OTHER_SPS
    } else {
        SPS
    };
    let formats = [
        (
            TrackKind::Video,
            TrackFormat::H264 {
                width: 320,
                height: 240,
                csd0: Bytes::from_static(sps),
                csd1: Bytes::from_static(PPS),
            },
        ),
        (
            TrackKind::Audio,
            TrackFormat::Aac {
                sample_rate: 48_000,
                channels: 2,
                csd0: Bytes::from_static(ASC),
            },
        ),
    ];
    for (track, format) in formats {
        if sink.format(track, format).is_err() {
            record.stopped.store(true, Ordering::SeqCst);
            return;
        }
    }
    if wants_index && let Some(gate) = &knobs.index_gate {
        let probe = sink.probe();
        if !gate.pass(|| probe.is_stopped()) {
            record.stopped.store(true, Ordering::SeqCst);
            return;
        }
    }
    if wants_index
        && let Some(knob) = knobs.index
        && sink.index(knobs.index(knob, reader.len())).is_err()
    {
        record.stopped.store(true, Ordering::SeqCst);
        return;
    }
    let wall = Instant::now();
    let first = knobs.video_pts(video).min(Knobs::audio_pts(audio));
    let mut written = 0usize;
    loop {
        let video_pts = knobs.video_pts(video);
        let audio_pts = Knobs::audio_pts(audio);
        let take_video = video_pts <= audio_pts;
        let pts = if take_video { video_pts } else { audio_pts };
        if pts >= length {
            break;
        }
        if let Some(speed) = knobs.speed {
            // The simulated work: a producer this slow takes this long.
            let due = wall + Duration::from_micros(((pts - first) as f64 / speed) as u64);
            let now = Instant::now();
            if due > now {
                std::thread::sleep(due - now);
            }
        }
        let sample = if take_video {
            let key = knobs.is_key(video);
            if key && let Some(stride) = knobs.read_stride {
                let at = (video / gop) as u64 * stride % reader.len().max(1);
                let mut byte = [0u8; 1];
                let read = reader.seek(at).and_then(|_| reader.read(&mut byte));
                if let Err(error) = read {
                    sink.fail(format!("the test source failed: {error}"));
                    return;
                }
            }
            let sample = Sample {
                track: TrackKind::Video,
                pts_us: video_pts,
                key,
                data: video_payload(video, key, knobs.frame_bytes),
            };
            video += 1;
            sample
        } else {
            let sample = Sample {
                track: TrackKind::Audio,
                pts_us: audio_pts,
                key: true,
                data: audio_payload(audio),
            };
            audio += 1;
            sample
        };
        let (track, pts) = (sample.track, sample.pts_us);
        if let Some((at, gate)) = &knobs.time_gate
            && pts >= *at
        {
            let probe = sink.probe();
            if !gate.pass(|| probe.is_stopped()) {
                record.stopped.store(true, Ordering::SeqCst);
                return;
            }
        }
        if sink.sample(sample).is_err() {
            record.stopped.store(true, Ordering::SeqCst);
            return;
        }
        record.emitted.lock().unwrap().push((track, pts));
        written += 1;
        let gate = knobs.start_gate.as_ref().or(knobs
            .run_gate
            .as_ref()
            .filter(|(run, _)| *run == index)
            .map(|(_, gate)| gate));
        if written == 1
            && let Some(gate) = gate
        {
            let probe = sink.probe();
            if !gate.pass(|| probe.is_stopped()) {
                record.stopped.store(true, Ordering::SeqCst);
                return;
            }
        }
        if let Some((run, sentence)) = &knobs.fail_on_run
            && *run == index
            && written >= 10
        {
            sink.fail(sentence.clone());
            return;
        }
    }
    // The reader is dropped here, on this thread, as a foreign producer's
    // would be.
    drop(reader);
    sink.end();
    record.ended.store(true, Ordering::SeqCst);
}

/// What a converting producer says when its converter fails.
pub const PICTURE_FAILED: &str = "Converting this film's picture for the television failed.";

/// A converting run: the sound's format, the index, then the picture
/// through a `ConvertedPicture` over the fake converter, the sound held
/// until the picture's format has gone.
fn produce_converted(
    knobs: &Knobs,
    index: usize,
    job: Job,
    record: &RunRecord,
    target: PictureTarget,
) {
    let Job {
        reader,
        from,
        sink,
        wants_index,
        segments,
        ..
    } = job;
    let stopped = || record.stopped.store(true, Ordering::SeqCst);
    let audio_format = TrackFormat::Aac {
        sample_rate: 48_000,
        channels: 2,
        csd0: Bytes::from_static(ASC),
    };
    if sink.format(TrackKind::Audio, audio_format).is_err() {
        return stopped();
    }
    let reported = match knobs.index {
        Some(knob) if wants_index => Some(knobs.index(knob, reader.len())),
        _ => None,
    };
    if let Some(entries) = &reported
        && sink.index(entries.clone()).is_err()
    {
        return stopped();
    }
    let mut options = knobs.converter.clone();
    if knobs.other_format_on_run == Some(index) {
        let mut csd = OTHER_SPS.to_vec();
        csd.extend_from_slice(PPS);
        options.csd = Bytes::from(csd);
    }
    let segments = segments.segments(reported.as_deref());
    let converter = FakeConverter::new(options, record.converter.clone());
    let mut picture = ConvertedPicture::new(converter, segments, target);

    // Video from the sync frame at or before `from`, in decode order.
    let from_us = from.as_micros() as i64;
    let gop = i64::from(knobs.gop);
    let key = from_us * i64::from(knobs.fps) / 1_000_000 / gop * gop;
    let order = knobs.decode_order();
    let mut video = order
        .iter()
        .position(|frame| *frame == key)
        .unwrap_or(order.len());
    let mut audio = (from_us * AAC_RATE / AAC_FRAME / 1_000_000 - 1).max(0);
    let length = knobs.length.as_micros() as i64;
    let mut held: Option<VecDeque<Sample>> = (!knobs.unheld_sound).then(VecDeque::new);
    let wall = Instant::now();
    let first = order
        .get(video)
        .map_or(i64::MAX, |frame| knobs.video_pts(*frame))
        .min(Knobs::audio_pts(audio));

    // Hand the sink what the driver gave back. `false` once the run is
    // gone or failed.
    let forward =
        |out: Result<Vec<Converted>, String>, held: &mut Option<VecDeque<Sample>>| -> bool {
            let out = match out {
                Ok(out) => out,
                Err(_) => return false,
            };
            for converted in out {
                let sent = match converted {
                    Converted::Format(format) => {
                        let mut sent = sink.format(TrackKind::Video, format);
                        for sample in held.take().into_iter().flatten() {
                            let pts = sample.pts_us;
                            sent = sent.and_then(|()| sink.sample(sample));
                            record.emitted.lock().unwrap().push((TrackKind::Audio, pts));
                        }
                        sent
                    }
                    Converted::Sample(sample) => {
                        let pts = sample.pts_us;
                        let sent = sink.sample(sample);
                        record.emitted.lock().unwrap().push((TrackKind::Video, pts));
                        sent
                    }
                };
                if sent.is_err() {
                    stopped();
                    return false;
                }
            }
            true
        };
    loop {
        let video_pts = order.get(video).map(|frame| knobs.video_pts(*frame));
        let audio_pts = Knobs::audio_pts(audio);
        let take_video = video_pts.is_some_and(|pts| pts <= audio_pts);
        let pts = if take_video {
            video_pts.unwrap_or(i64::MAX)
        } else {
            audio_pts
        };
        if pts >= length {
            break;
        }
        if let Some(speed) = knobs.speed {
            let due = wall + Duration::from_micros(((pts - first).max(0) as f64 / speed) as u64);
            let now = Instant::now();
            if due > now {
                std::thread::sleep(due - now);
            }
        }
        if take_video {
            let frame = order[video];
            video += 1;
            let key = knobs.is_key(frame);
            let out = picture.push(SourcePicture {
                pts_us: pts,
                key,
                data: video_payload(frame, key, knobs.frame_bytes),
            });
            let failed = out.is_err();
            if !forward(out, &mut held) {
                if failed {
                    sink.fail(PICTURE_FAILED.to_string());
                }
                return;
            }
        } else {
            let sample = Sample {
                track: TrackKind::Audio,
                pts_us: audio_pts,
                key: true,
                data: audio_payload(audio),
            };
            audio += 1;
            match held.as_mut() {
                Some(held) => held.push_back(sample),
                None => {
                    if sink.sample(sample).is_err() {
                        return stopped();
                    }
                    record
                        .emitted
                        .lock()
                        .unwrap()
                        .push((TrackKind::Audio, audio_pts));
                }
            }
        }
    }
    let out = picture.finish();
    let failed = out.is_err();
    if !forward(out, &mut held) {
        if failed {
            sink.fail(PICTURE_FAILED.to_string());
        }
        return;
    }
    drop(reader);
    sink.end();
    record.ended.store(true, Ordering::SeqCst);
}
