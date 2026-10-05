//! **The run task**: one producer run, on the server's runtime. It opens
//! the reader, hands the producer its [`Job`], and takes the samples off
//! the sink's channel -- applying the cut rule ([`Cutter`]), muxing each
//! segment as it completes and putting it in the ring -- while it judges
//! the run's speed, stops taking samples once its lookahead
//! ([`LOOKAHEAD_TIME`](super::LOOKAHEAD_TIME)) is made past the last request, and lets the run go when it has been idle for
//! the release period. Dropping the task's channel is what makes the sink
//! answer [`super::Stopped`]; cancelling the reader is what wakes a
//! producer parked in a read.

use super::layout::Layout;
use super::mux::{self, Formats, MuxSample};
use super::slots::{self, Cursor, End};
use super::speed::Speed;
use super::{
    IndexEntry, Job, Rendition, SINK_CAPACITY, SampleSink, SinkMessage, SinkShown, TrackKind,
};
use crate::state::AppState;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// How far before a segment's cut its sound begins: an audio frame that
/// starts this little before the cut is the segment's. More than one AAC
/// frame at any rate (HE-AAC at 44.1 kHz is 46 ms), so the frame playing
/// at the sync sample is in the sync sample's fragment.
pub(crate) const AUDIO_LEAD_US: i64 = 64_000;

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
/// or after its cut (`cuts[N]`, from the layout: an indexed sync sample's
/// time, or N x T); each audio sample goes to the segment whose cuts its
/// presentation time falls between, [`AUDIO_LEAD_US`] early; what precedes the run's first segment
/// is discarded. A segment is complete once video has moved past it (a
/// later cut) and audio has reached the next cut -- or, for a track the run
/// does not have, at once.
pub(crate) struct Cutter {
    /// `cuts[0]` is [`i64::MIN`].
    cuts: Arc<[i64]>,
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
    pub(crate) fn new(cuts: Arc<[i64]>, start: u64, has_video: bool, has_audio: bool) -> Self {
        Self {
            cuts,
            has_video,
            has_audio,
            video_segment: None,
            audio_high: None,
            open: BTreeMap::new(),
            next_out: start,
        }
    }

    fn segment_of(&self, pts_us: i64) -> u64 {
        (self.cuts.partition_point(|cut| *cut <= pts_us) as u64).saturating_sub(1)
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
                // segment's cut; the segments it passes over are empty.
                Some(current) if sample.key => current.max(self.segment_of(sample.pts_us)),
                Some(current) => current,
            },
            TrackKind::Audio if self.has_audio => {
                self.segment_of(sample.pts_us.saturating_add(AUDIO_LEAD_US))
            }
            // A track with no format: nothing in the init segment describes
            // it, so nothing can carry it.
            _ => return Vec::new(),
        };
        // Before the run's first segment -- the producer starts well before
        // it, at a sync sample -- or behind a segment already out:
        // discarded, and not kept.
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
                .cuts
                .get(segment as usize + 1)
                .is_some_and(|next| self.audio_high.is_some_and(|high| high >= *next));
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

/// Run `generation` of `rendition` until it ends: the producer handed
/// `from`, the output starting at slot `start` (or, for the run that
/// freezes the layout, at the first slot cut at or after `from`).
pub(crate) async fn run(
    rendition: Arc<Rendition>,
    state: AppState,
    generation: u64,
    start: Option<u64>,
    from: Duration,
    stop: CancellationToken,
) {
    let mut produced = 0u64;
    let outcome = drive(
        &rendition,
        &state,
        generation,
        start,
        from,
        &stop,
        &mut produced,
    )
    .await;
    stop.cancel();
    let kind = match &outcome {
        Outcome::Stopped => "stopped",
        Outcome::Released => "released",
        Outcome::Ended => "ended",
        Outcome::Failed(_) => "failed",
    };
    {
        let mut inner = rendition.inner();
        let ours = inner.run(generation).is_some();
        if ours {
            inner.runs.retain(|run| run.generation != generation);
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

/// A run's output side, once the layout is frozen: the cutter, and the
/// slots it fills from the segments the cutter completes.
struct Making {
    layout: Arc<Layout>,
    cutter: Cutter,
    /// The next slot to make.
    slot: u64,
    /// Where its content starts.
    cursor: Cursor,
    /// Segments completed and not yet wholly in a slot.
    segments: BTreeMap<u64, Cut>,
}

impl Making {
    /// Take the segments the cutter completed and make every slot they
    /// complete (all that are left, at the film's `ending`); `false` once
    /// the run is no longer the live one.
    fn take(
        &mut self,
        rendition: &Rendition,
        generation: u64,
        formats: &Formats,
        cuts: Vec<Cut>,
        ending: bool,
    ) -> bool {
        for cut in cuts {
            self.segments.insert(cut.index, cut);
        }
        let count = self.layout.slots.len() as u64;
        while self.slot < count && (ending || self.cutter.next_out > self.slot) {
            let slot = self.slot;
            let content = slots::gather(&self.segments, self.cursor, slot);
            // At its label: mirrored (the label is the cut) and opening
            // with its own segment, not with what the slot before spilled.
            let at_label = self.layout.exact && self.cursor == Cursor::at(slot);
            let mux = |video: &[MuxSample],
                       video_next: Option<i64>,
                       audio: &[MuxSample],
                       audio_next: Option<i64>| {
                mux::media_segment(
                    formats, slot, at_label, video, video_next, audio, audio_next,
                )
            };
            let size = self.layout.slots[slot as usize].size;
            let filled = content.fill(size, &mux);
            // The end the slot was first made with, which says where the
            // next starts: the same prefix whichever it is now.
            let Some(end) = rendition.publish_slot(generation, slot, filled) else {
                return false;
            };
            self.cursor = match end {
                End::Spill(cursor) => cursor,
                End::Natural | End::Truncated { .. } => Cursor::at(slot + 1),
            };
            let keep = self.cursor.anchor();
            self.segments.retain(|segment, _| *segment >= keep);
            self.slot += 1;
        }
        true
    }
}

async fn drive(
    rendition: &Arc<Rendition>,
    state: &AppState,
    generation: u64,
    start: Option<u64>,
    from: Duration,
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
    let source_len = reader.len();
    let reader_waits = reader.waits();
    let _cancel_reader = CancelOnDrop(reader.canceller());
    let (tx, mut rx) = mpsc::channel(SINK_CAPACITY);
    let shown = Arc::new(SinkShown::default());
    let sink = SampleSink {
        tx,
        stop: stop.clone(),
        shown: shown.clone(),
    };
    let job = Job {
        reader,
        spec: rendition.spec.clone(),
        from,
        wants_index: !rendition.has_layout(),
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
    let mut index: Option<Vec<IndexEntry>> = None;
    let mut making: Option<Making> = None;

    loop {
        let (gated, idle_at) = {
            let inner = rendition.inner();
            let Some(run) = inner.run(generation) else {
                return Outcome::Stopped;
            };
            let next_out = making.as_ref().map_or(run.next_out, |making| making.slot);
            (
                inner.gated(generation, next_out),
                inner.idle_at(generation, rendition.tuning.idle_release),
            )
        };
        tokio::select! {
            biased;
            () = stop.cancelled() => return Outcome::Stopped,
            () = async {
                match idle_at {
                    Some(at) => tokio::time::sleep_until(at.into()).await,
                    None => std::future::pending().await,
                }
            } => {
                // Looked at again under the lock: a request may have come
                // in, or begun waiting, since the deadline was read.
                let idle_at = rendition
                    .inner()
                    .idle_at(generation, rendition.tuning.idle_release);
                if idle_at.is_some_and(|at| Instant::now() >= at) {
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
                        if making.is_none() {
                            match track {
                                TrackKind::Video => formats.video = Some(format),
                                TrackKind::Audio => formats.audio = Some(format),
                            }
                        }
                    }
                    SinkMessage::Index(entries) => {
                        if making.is_none() {
                            index = Some(entries);
                        }
                    }
                    SinkMessage::Sample(sample) => {
                        if making.is_none() {
                            let frozen = rendition.freeze(
                                generation,
                                &formats,
                                index.as_deref(),
                                source_len,
                                from,
                                start,
                            );
                            match frozen {
                                Ok((layout, slot, cursor)) => {
                                    let cutter = Cutter::new(
                                        layout.cuts.clone(),
                                        cursor.anchor(),
                                        formats.video.is_some(),
                                        formats.audio.is_some(),
                                    );
                                    making = Some(Making {
                                        layout,
                                        cutter,
                                        slot,
                                        cursor,
                                        segments: BTreeMap::new(),
                                    });
                                }
                                Err(Frozen::Failed(sentence)) => return Outcome::Failed(sentence),
                                Err(Frozen::Stopped) => return Outcome::Stopped,
                            }
                        }
                        let Some(making) = making.as_mut() else {
                            continue;
                        };
                        let before = making.slot;
                        let cuts = making.cutter.push(
                            sample.track,
                            MuxSample {
                                pts_us: sample.pts_us,
                                key: sample.key,
                                data: sample.data,
                            },
                        );
                        if !cuts.is_empty() {
                            if !making.take(rendition, generation, &formats, cuts, false) {
                                return Outcome::Stopped;
                            }
                            if making.slot > before {
                                *produced += making.slot - before;
                                speed.start(busy(Instant::now()), shown.media.media());
                            }
                        }
                    }
                    SinkMessage::End => {
                        let Some(making) = making.as_mut() else {
                            return Outcome::Failed(
                                "The conversion ended before it produced anything.".to_string(),
                            );
                        };
                        let before = making.slot;
                        let cuts = making.cutter.finish();
                        making.take(rendition, generation, &formats, cuts, true);
                        *produced += making.slot - before;
                        return Outcome::Ended;
                    }
                    SinkMessage::Fail(sentence) => return Outcome::Failed(sentence),
                }
            }
        }
    }
}

/// Why a run could not freeze the layout.
pub(crate) enum Frozen {
    /// The rendition cannot go on: the sentence.
    Failed(String),
    /// The run is no longer the live one.
    Stopped,
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

    /// Cuts every T, as an estimated layout makes them.
    fn grid() -> Arc<[i64]> {
        (0..100)
            .map(|k| if k == 0 { i64::MIN } else { k * T })
            .collect()
    }

    fn pts(samples: &[MuxSample]) -> Vec<i64> {
        samples.iter().map(|sample| sample.pts_us).collect()
    }

    /// Keys every 400 ms, frames every 200 ms, from 0: segment 1 begins at
    /// the key at 1.2 s, the first at or after 1 s.
    #[test]
    fn a_segment_begins_at_the_first_key_at_or_after_its_time() {
        let mut cutter = Cutter::new(grid(), 0, true, false);
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
        let mut cutter = Cutter::new(grid(), 2, true, true);
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

    /// **Cuts from an index**: segment 1 begins at the indexed key at
    /// 1.2 s, and audio before it (by more than the lead) is segment 0's,
    /// whatever the grid says.
    #[test]
    fn indexed_cuts_are_where_segments_begin() {
        let cuts: Arc<[i64]> = vec![i64::MIN, 1_200_000, 2_400_000].into();
        let mut cutter = Cutter::new(cuts, 0, true, true);
        let mut out = Vec::new();
        for frame in 0..15i64 {
            out.extend(cutter.push(TrackKind::Audio, sample(frame * 200_000 - 1, true)));
            out.extend(cutter.push(TrackKind::Video, sample(frame * 200_000, frame % 2 == 0)));
        }
        assert_eq!(
            out.iter().map(|cut| cut.index).collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            pts(&out[0].video),
            vec![0, 200_000, 400_000, 600_000, 800_000, 1_000_000]
        );
        assert_eq!(pts(&out[1].video).first(), Some(&1_200_000));
        // The frame 1 us before the cut is within the lead: segment 1's.
        assert_eq!(pts(&out[0].audio).last(), Some(&999_999));
        assert_eq!(pts(&out[1].audio).first(), Some(&1_199_999));
    }

    /// **A segment waits for its audio**: video moving past the next cut
    /// does not complete it while audio before that cut can still come --
    /// a container that stores audio after the video it plays beside.
    #[test]
    fn a_segment_waits_for_audio_up_to_the_next_cut() {
        let cuts: Arc<[i64]> = vec![i64::MIN, 1_000_000].into();
        let mut cutter = Cutter::new(cuts, 0, true, true);
        let mut out = Vec::new();
        out.extend(cutter.push(TrackKind::Video, sample(0, true)));
        out.extend(cutter.push(TrackKind::Audio, sample(0, true)));
        out.extend(cutter.push(TrackKind::Video, sample(1_000_000, true)));
        assert!(out.is_empty(), "audio has not reached the cut");
        out.extend(cutter.push(TrackKind::Audio, sample(800_000, true)));
        out.extend(cutter.push(TrackKind::Audio, sample(1_000_000, true)));
        assert_eq!(out.len(), 1);
        assert_eq!(pts(&out[0].audio), vec![0, 800_000]);
    }

    /// **A segment's sound begins at or before its picture**: the audio
    /// frame playing at the cut is the segment's, not the one before's --
    /// a demuxer seeking every stream to the sync sample's time looks for
    /// sound at or before it in the same fragment, and finding none goes
    /// back to the last it read (FFmpeg n6's `mov_read_seek`, measured).
    #[test]
    fn a_segments_sound_begins_at_or_before_its_sync_sample() {
        let cuts: Arc<[i64]> = vec![i64::MIN, 1_000_000, 2_000_000].into();
        let mut cutter = Cutter::new(cuts, 0, true, true);
        let mut out = Vec::new();
        // Audio frames every 21.333 ms from -5 ms; keys every 500 ms.
        let mut audio = (0..).map(|n| n * 21_333 - 5_000).peekable();
        for frame in 0..60i64 {
            let pts = frame * 40_000;
            while audio.peek().is_some_and(|at| *at <= pts) {
                out.extend(cutter.push(TrackKind::Audio, sample(audio.next().unwrap(), true)));
            }
            out.extend(cutter.push(TrackKind::Video, sample(pts, pts % 500_000 == 0)));
        }
        assert!(out.len() >= 2);
        for cut in &out[1..] {
            let key = cut.video[0].pts_us;
            let sound = cut.audio[0].pts_us;
            assert!(
                sound <= key,
                "segment {}: sound from {sound}, picture from {key}",
                cut.index
            );
            assert!(
                key - sound < AUDIO_LEAD_US,
                "segment {}: no more than the lead",
                cut.index
            );
        }
    }

    /// A GOP longer than T leaves the segment between empty.
    #[test]
    fn a_long_gop_leaves_a_segment_empty() {
        let mut cutter = Cutter::new(grid(), 0, true, false);
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
