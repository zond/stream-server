//! A rendition producer for the tests (`docs/design/renditions.md` §5 F1):
//! Rust, on a plain thread per run, emitting synthetic samples through the
//! same `Producer` trait the embedder's Kotlin producer implements.
//!
//! What it emits is H.264-shaped video -- a fixed GOP, a sync sample every
//! [`Knobs::gop`] frames at [`Knobs::fps`], x264's real SPS and PPS as
//! `csd-0`/`csd-1`, and NAL units in Annex-B whose payload names the frame
//! -- and AAC frames every 1024 samples at 48 kHz (21.33 ms) with a stereo
//! AAC-LC AudioSpecificConfig. A run from `from` starts as a demuxer does:
//! video at the sync sample at or before it, audio one frame before it.
//!
//! The knobs: a speed factor (a producer slower than real time paces
//! itself, which is the work being simulated, not a wait for anything); a
//! run that reports another format; a run that fails; reads from the
//! job's reader at every sync sample, so a slow source is a slow producer
//! in wall time and not in busy time; the size of a video frame; and the
//! index it reports when the job wants one ([`IndexKnob`]): every sync
//! sample at a position in proportion to its time over the reader's
//! length, one squeezed against the next, or none.

#![allow(dead_code)]

use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use stream_server::rendition::SinkProbe;
use stream_server::{IndexEntry, Job, Producer, ProducerRefusal, Sample, TrackFormat, TrackKind};

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
    /// byte before the next: the segment it starts gets almost no bytes of
    /// the source, and overflows its slot.
    Squeezed { at_us: i64 },
}

#[derive(Clone, Debug)]
pub struct Knobs {
    pub fps: u32,
    /// Frames per GOP: a sync sample every `gop` frames.
    pub gop: u32,
    /// How long the film is: the producer ends there.
    pub length: Duration,
    /// Media time per wall time; `None` is as fast as it can.
    pub speed: Option<f64>,
    /// The run (counted from 0) that reports [`OTHER_SPS`].
    pub other_format_on_run: Option<usize>,
    /// The run that fails after a few samples, with this sentence.
    pub fail_on_run: Option<(usize, String)>,
    /// Read one byte of the job's reader at `stride` x the GOP's index at
    /// every sync sample: a seek and a read the source answers.
    pub read_stride: Option<u64>,
    /// Filler bytes in each video frame after its NAL header and number.
    pub frame_bytes: usize,
    /// The index reported; `None` reports none.
    pub index: Option<IndexKnob>,
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
                    keys.get(at + 1)
                        .map_or(place(*pts), |next| place(*next) - 1)
                } else {
                    place(*pts)
                },
            })
            .collect()
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
        });
        let index = {
            let mut runs = self.runs.lock().unwrap();
            runs.push(record.clone());
            runs.len() - 1
        };
        let knobs = self.knobs.clone();
        std::thread::spawn(move || {
            produce(&knobs, index, job, &record);
            record.done.store(true, Ordering::SeqCst);
        });
        Ok(())
    }
}

/// A frame number as four bytes with the top bit set in each, so a NAL
/// payload never holds a start code: 28 bits, seven to a byte.
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
        if sink.sample(sample).is_err() {
            record.stopped.store(true, Ordering::SeqCst);
            return;
        }
        record.emitted.lock().unwrap().push((track, pts));
        written += 1;
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
