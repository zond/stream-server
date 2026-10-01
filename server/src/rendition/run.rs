//! **The run task**: one producer run, on the server's runtime. It opens
//! the reader, hands the producer its [`Job`], and takes the samples off
//! the sink's channel -- applying the cut rule ([`Cutter`]), muxing each
//! segment as it completes and putting it in the ring -- while it judges
//! the run's speed, stops taking samples once [`LOOKAHEAD`] segments are
//! ahead of the last request, and lets the run go when it has been idle for
//! the release period. Dropping the task's channel is what makes the sink
//! answer [`super::Stopped`]; cancelling the reader is what wakes a
//! producer parked in a read.

use super::mux::{self, Formats, MuxSample};
use super::speed::Speed;
use super::{
    Job, LOOKAHEAD, RING_CAP, Rendition, SINK_CAPACITY, SampleSink, SinkMessage, SinkShown,
    TrackKind,
};
use crate::state::AppState;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// One segment's samples, as the cut rule assigned them.
#[derive(Default)]
struct Pending {
    video: Vec<MuxSample>,
    audio: Vec<MuxSample>,
}

/// A completed segment, ready to mux.
pub(crate) struct Cut {
    pub index: u64,
    pub video: Vec<MuxSample>,
    pub video_next: Option<i64>,
    pub audio: Vec<MuxSample>,
    pub audio_next: Option<i64>,
}

/// **The cut rule**, applied to one run's samples in the order the
/// producer wrote them. Segment N begins at the first video sync sample at
/// or after N x T; each audio sample goes to the segment its presentation
/// time falls in on the N x T grid; what precedes the run's first cut is
/// discarded. A segment is complete once video has moved past it (a later
/// cut) and audio has reached its end -- or, for a track the run does not
/// have, at once.
pub(crate) struct Cutter {
    segment_us: i64,
    has_video: bool,
    has_audio: bool,
    /// The segment video samples go to now; `None` before the first cut.
    video_segment: Option<u64>,
    audio_high: Option<i64>,
    open: BTreeMap<u64, Pending>,
    /// The lowest segment not yet completed.
    pub(crate) next_out: u64,
}

impl Cutter {
    pub(crate) fn new(segment_us: i64, start: u64, has_video: bool, has_audio: bool) -> Self {
        Self {
            segment_us,
            has_video,
            has_audio,
            video_segment: None,
            audio_high: None,
            open: BTreeMap::new(),
            next_out: start,
        }
    }

    fn segment_of(&self, pts_us: i64) -> u64 {
        (pts_us.max(0) / self.segment_us) as u64
    }

    /// Take one sample; answer the segments it completed, in order.
    pub(crate) fn push(&mut self, track: TrackKind, sample: MuxSample) -> Vec<Cut> {
        let segment = match track {
            TrackKind::Video if self.has_video => match self.video_segment {
                // Nothing before the first sync sample: no segment begins
                // anywhere else.
                None if !sample.key => return Vec::new(),
                None => self.segment_of(sample.pts_us),
                // A sync sample cuts when it is at or after the next
                // segment's time; the segments it passes over are empty.
                Some(current) if sample.key => current.max(self.segment_of(sample.pts_us)),
                Some(current) => current,
            },
            TrackKind::Audio if self.has_audio => self.segment_of(sample.pts_us),
            // A track with no format: nothing in the init segment describes
            // it, so nothing can carry it.
            _ => return Vec::new(),
        };
        // Before the run's first segment -- the producer starts at the sync
        // sample before it, and audio a frame early -- or behind a segment
        // already out: discarded, and not kept.
        if segment < self.next_out {
            return Vec::new();
        }
        match track {
            TrackKind::Video => self.video_segment = Some(segment),
            TrackKind::Audio => {
                self.audio_high = Some(
                    self.audio_high
                        .map_or(sample.pts_us, |high| high.max(sample.pts_us)),
                );
            }
        }
        let pending = self.open.entry(segment).or_default();
        match track {
            TrackKind::Video => pending.video.push(sample),
            TrackKind::Audio => pending.audio.push(sample),
        }
        self.complete(false)
    }

    /// The source ended: every segment begun is complete.
    pub(crate) fn finish(&mut self) -> Vec<Cut> {
        self.complete(true)
    }

    fn done(&self, segment: u64) -> bool {
        let video = !self.has_video || self.video_segment.is_some_and(|at| at > segment);
        let audio = !self.has_audio
            || self
                .audio_high
                .is_some_and(|high| high >= (segment as i64 + 1) * self.segment_us);
        video && audio
    }

    fn complete(&mut self, ending: bool) -> Vec<Cut> {
        let last = self.open.keys().next_back().copied();
        let mut cuts = Vec::new();
        loop {
            let segment = self.next_out;
            let ready = if ending {
                last.is_some_and(|last| segment <= last)
            } else {
                self.done(segment)
            };
            if !ready {
                break;
            }
            let pending = self.open.remove(&segment).unwrap_or_default();
            let after = self.open.range(segment + 1..);
            let video_next = after
                .clone()
                .find_map(|(_, later)| later.video.first().map(|sample| sample.pts_us));
            let audio_next = after
                .clone()
                .find_map(|(_, later)| later.audio.first().map(|sample| sample.pts_us));
            cuts.push(Cut {
                index: segment,
                video: pending.video,
                video_next,
                audio: pending.audio,
                audio_next,
            });
            self.next_out += 1;
        }
        cuts
    }
}

/// How a run ended.
enum Outcome {
    /// Cut, superseded by a seek, or failed elsewhere: nothing to record.
    Stopped,
    /// Let go after the release period with no request.
    Released,
    /// The source ended.
    Ended,
    /// The rendition cannot go on.
    Failed(String),
}

/// Cancels the reader when the run ends, however it ends: a producer
/// parked in a read is woken, and its next read answers at once.
struct CancelOnDrop(crate::media::Canceller);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Run `generation` of `rendition`, from `start`, until it ends.
pub(crate) async fn run(
    rendition: Arc<Rendition>,
    state: AppState,
    generation: u64,
    start: u64,
    stop: CancellationToken,
) {
    let mut produced = 0u64;
    let outcome = drive(&rendition, &state, generation, start, &stop, &mut produced).await;
    stop.cancel();
    let kind = match &outcome {
        Outcome::Stopped => "stopped",
        Outcome::Released => "released",
        Outcome::Ended => "ended",
        Outcome::Failed(_) => "failed",
    };
    {
        let mut inner = rendition.inner();
        let ours = inner
            .run
            .as_ref()
            .is_some_and(|run| run.generation == generation);
        if ours {
            inner.run = None;
            if let Outcome::Failed(sentence) = outcome {
                rendition.fail(&mut inner, sentence);
            }
        }
    }
    rendition.bump();
    tracing::info!(
        from = start,
        produced,
        outcome = kind,
        stage = "rendition_run_end",
        "rendition run ended"
    );
}

fn speed_sentence(spec: &super::RenditionSpec, media: Duration, busy: Duration) -> String {
    let what = match (&spec.video, &spec.audio) {
        (super::VideoPlan::H264 { .. }, _) => "convert this film to H.264",
        (super::VideoPlan::Copy, super::AudioPlan::AacStereo { .. }) => "convert this film's sound",
        (super::VideoPlan::Copy, super::AudioPlan::Copy) => "repackage this film",
    };
    let seconds = |duration: Duration| {
        let tenths = (duration.as_millis() + 50) / 100;
        if tenths.is_multiple_of(10) {
            format!("{}", tenths / 10)
        } else {
            format!("{}.{}", tenths / 10, tenths % 10)
        }
    };
    format!(
        "This phone cannot {what} fast enough for the television: it made {} seconds of film in {}.",
        seconds(media),
        seconds(busy)
    )
}

async fn drive(
    rendition: &Arc<Rendition>,
    state: &AppState,
    generation: u64,
    start: u64,
    stop: &CancellationToken,
    produced: &mut u64,
) -> Outcome {
    let started = Instant::now();
    let reader = tokio::select! {
        biased;
        () = stop.cancelled() => return Outcome::Stopped,
        opened = state.media.open_reader(state, &rendition.id, rendition.play.clone()) => opened,
    };
    let reader = match reader {
        Ok(reader) => reader,
        Err(refusal) => return Outcome::Failed(refusal.to_string()),
    };
    rendition.note_source_len(reader.len());
    let reader_waits = reader.waits();
    let _cancel_reader = CancelOnDrop(reader.canceller());
    let (tx, mut rx) = mpsc::channel(SINK_CAPACITY);
    let shown = Arc::new(SinkShown::default());
    let sink = SampleSink {
        tx,
        stop: stop.clone(),
        shown: shown.clone(),
    };
    let segment_us = rendition.segment_us();
    let job = Job {
        reader,
        spec: rendition.spec.clone(),
        from: Duration::from_micros(start * segment_us as u64),
        sink,
    };
    if let Err(refusal) = rendition.producer.start(job) {
        return Outcome::Failed(refusal.0);
    }

    let busy = |now: Instant| {
        now.saturating_duration_since(started)
            .saturating_sub(shown.waits.total(now))
            .saturating_sub(reader_waits.total(now))
    };
    let window = rendition.tuning.speed_window;
    let mut speed = Speed::new(window);
    let mut tick = tokio::time::interval((window / 10).max(Duration::from_millis(5)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut seen = rendition.version.subscribe();
    let mut formats = Formats::default();
    let mut cutter: Option<Cutter> = None;

    loop {
        let (gated, idle_at) = {
            let inner = rendition.inner();
            if !inner
                .run
                .as_ref()
                .is_some_and(|run| run.generation == generation)
            {
                return Outcome::Stopped;
            }
            let next_out = cutter.as_ref().map_or(start, |cutter| cutter.next_out);
            let gated = next_out > inner.last_request + LOOKAHEAD
                || (inner.ring_bytes >= RING_CAP && next_out > inner.last_request);
            (gated, inner.last_request_at + rendition.tuning.idle_release)
        };
        tokio::select! {
            biased;
            () = stop.cancelled() => return Outcome::Stopped,
            () = tokio::time::sleep_until(idle_at.into()) => {
                // Looked at again under the lock: a request may have come
                // in since the deadline was read.
                let inner = rendition.inner();
                if Instant::now() >= inner.last_request_at + rendition.tuning.idle_release {
                    return Outcome::Released;
                }
            }
            _ = tick.tick() => {
                if let Some(slow) = speed.check(busy(Instant::now()), shown.media.media()) {
                    return Outcome::Failed(speed_sentence(&rendition.spec, slow.media, slow.busy));
                }
            }
            changed = seen.changed() => {
                if changed.is_err() {
                    return Outcome::Stopped;
                }
            }
            message = rx.recv(), if !gated => {
                let Some(message) = message else {
                    return Outcome::Failed(
                        "The conversion stopped without saying why.".to_string(),
                    );
                };
                match message {
                    SinkMessage::Format(track, format) => {
                        if format.kind() != track {
                            return Outcome::Failed(
                                "The conversion described a track as something it is not."
                                    .to_string(),
                            );
                        }
                        if cutter.is_none() {
                            match track {
                                TrackKind::Video => formats.video = Some(format),
                                TrackKind::Audio => formats.audio = Some(format),
                            }
                        }
                    }
                    SinkMessage::Sample(sample) => {
                        if cutter.is_none() {
                            if let Err(sentence) = freeze(rendition, &formats) {
                                return Outcome::Failed(sentence);
                            }
                            cutter = Some(Cutter::new(
                                segment_us,
                                start,
                                formats.video.is_some(),
                                formats.audio.is_some(),
                            ));
                        }
                        let Some(cutting) = cutter.as_mut() else {
                            continue;
                        };
                        let cuts = cutting.push(
                            sample.track,
                            MuxSample {
                                pts_us: sample.pts_us,
                                key: sample.key,
                                data: sample.data,
                            },
                        );
                        if !cuts.is_empty() {
                            *produced += cuts.len() as u64;
                            let next_out = cutting.next_out;
                            if !publish(rendition, generation, &formats, cuts, next_out, None) {
                                return Outcome::Stopped;
                            }
                            speed.start(busy(Instant::now()), shown.media.media());
                        }
                    }
                    SinkMessage::End => {
                        let Some(cutting) = cutter.as_mut() else {
                            return Outcome::Failed(
                                "The conversion ended before it produced anything.".to_string(),
                            );
                        };
                        let cuts = cutting.finish();
                        *produced += cuts.len() as u64;
                        let next_out = cutting.next_out;
                        publish(rendition, generation, &formats, cuts, next_out, Some(next_out));
                        return Outcome::Ended;
                    }
                    SinkMessage::Fail(sentence) => return Outcome::Failed(sentence),
                }
            }
        }
    }
}

/// The first sample of a run: freeze the formats into the init segment,
/// or -- for a later run -- check they are the ones frozen.
fn freeze(rendition: &Rendition, formats: &Formats) -> Result<(), String> {
    let mut inner = rendition.inner();
    match &inner.formats {
        None => {
            let init = mux::init_segment(formats, rendition.spec.duration_ms)?;
            inner.formats = Some(formats.clone());
            inner.init = Some(init);
            drop(inner);
            rendition.bump();
            Ok(())
        }
        Some(frozen) if frozen == formats => Ok(()),
        Some(_) => Err(
            "The conversion came back from a seek in a different format, which the television \
             cannot follow; cast the film again."
                .to_string(),
        ),
    }
}

/// Mux `cuts` into the ring, under the run's generation; `false` when the
/// run is no longer the live one.
fn publish(
    rendition: &Rendition,
    generation: u64,
    formats: &Formats,
    cuts: Vec<Cut>,
    next_out: u64,
    end: Option<u64>,
) -> bool {
    let segments: Vec<(u64, bytes::Bytes)> = cuts
        .into_iter()
        .map(|cut| {
            let bytes = mux::media_segment(
                formats,
                (cut.index + 1) as u32,
                &cut.video,
                cut.video_next,
                &cut.audio,
                cut.audio_next,
            );
            (cut.index, bytes)
        })
        .collect();
    {
        let mut inner = rendition.inner();
        let Some(run) = inner
            .run
            .as_mut()
            .filter(|run| run.generation == generation)
        else {
            return false;
        };
        run.next_out = next_out;
        for (index, bytes) in segments {
            inner.insert(index, bytes);
        }
        if let Some(end) = end {
            inner.end = Some(inner.end.map_or(end, |known| known.min(end)));
        }
    }
    rendition.bump();
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    const T: i64 = 1_000_000;

    fn sample(pts_us: i64, key: bool) -> MuxSample {
        MuxSample {
            pts_us,
            key,
            data: Bytes::new(),
        }
    }

    fn pts(samples: &[MuxSample]) -> Vec<i64> {
        samples.iter().map(|sample| sample.pts_us).collect()
    }

    /// Keys every 400 ms, frames every 200 ms, from 0: segment 1 begins at
    /// the key at 1.2 s, the first at or after 1 s.
    #[test]
    fn a_segment_begins_at_the_first_key_at_or_after_its_time() {
        let mut cutter = Cutter::new(T, 0, true, false);
        let mut cuts = Vec::new();
        for frame in 0..10i64 {
            cuts.extend(cutter.push(TrackKind::Video, sample(frame * 200_000, frame % 2 == 0)));
        }
        assert_eq!(cuts.len(), 1);
        assert_eq!(cuts[0].index, 0);
        assert_eq!(
            pts(&cuts[0].video),
            vec![0, 200_000, 400_000, 600_000, 800_000, 1_000_000]
        );
        assert_eq!(cuts[0].video_next, Some(1_200_000));
        let rest = cutter.finish();
        assert_eq!(
            rest.iter().map(|cut| cut.index).collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(rest[0].video.first().map(|s| s.pts_us), Some(1_200_000));
    }

    /// A run from segment 2, handed the key before it: nothing before the
    /// first key at or after 2 s, and no audio before 2 s.
    #[test]
    fn what_precedes_the_runs_cut_is_discarded() {
        let mut cutter = Cutter::new(T, 2, true, true);
        let mut cuts = Vec::new();
        for frame in 6..8i64 {
            cuts.extend(cutter.push(TrackKind::Audio, sample(frame * 250_000 - 100_000, true)));
            cuts.extend(cutter.push(TrackKind::Video, sample(frame * 250_000, frame % 3 == 0)));
        }
        assert!(cutter.open.is_empty(), "nothing before the cut is kept");
        for frame in 8..20i64 {
            cuts.extend(cutter.push(TrackKind::Audio, sample(frame * 250_000 - 100_000, true)));
            cuts.extend(cutter.push(TrackKind::Video, sample(frame * 250_000, frame % 3 == 0)));
        }
        cuts.extend(cutter.finish());
        assert_eq!(cuts[0].index, 2);
        assert_eq!(cuts[0].video.first().map(|s| s.pts_us), Some(2_250_000));
        assert!(cuts[0].audio.iter().all(|s| s.pts_us >= 2_000_000));
        assert!(cuts[0].audio.iter().all(|s| s.pts_us < 3_000_000));
    }

    /// A GOP longer than T leaves the segment between empty.
    #[test]
    fn a_long_gop_leaves_a_segment_empty() {
        let mut cutter = Cutter::new(T, 0, true, false);
        let mut cuts = Vec::new();
        for (pts_us, key) in [
            (0, true),
            (500_000, false),
            (2_500_000, true),
            (2_700_000, false),
        ] {
            cuts.extend(cutter.push(TrackKind::Video, sample(pts_us, key)));
        }
        assert_eq!(
            cuts.iter().map(|cut| cut.index).collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert!(cuts[1].video.is_empty());
        assert_eq!(cuts[0].video_next, Some(2_500_000));
    }

    #[test]
    fn the_sentence_names_the_work_and_the_measurement() {
        let spec = super::super::RenditionSpec {
            duration_ms: 1,
            segment_ms: 1,
            start_ms: 0,
            video: super::super::VideoPlan::H264 {
                width: 1,
                height: 1,
                bitrate: 1,
            },
            audio: super::super::AudioPlan::Copy,
            audio_track: 0,
        };
        assert_eq!(
            speed_sentence(&spec, Duration::from_millis(7000), Duration::from_secs(10)),
            "This phone cannot convert this film to H.264 fast enough for the television: it \
             made 7 seconds of film in 10."
        );
    }
}
