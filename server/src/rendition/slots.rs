//! **What goes in a slot**, and what happens when it does not fit.
//!
//! A slot's size is fixed before anything is produced (`layout.rs`); its
//! content is its segment's samples, muxed into one fragment, which must
//! leave [`MIN_PAD`] of the slot over. Mirrored from an index, a segment
//! fits its slot by construction, up to the headroom; estimated, or with
//! an index that put a sync sample's cluster well before the sample, it may
//! not. **The overflow rule**, deterministic so that every byte of the file
//! is the same however often it is made:
//!
//! 1. **Spill**: the slot keeps the longest prefix of its samples that
//!    fits -- cut, in decode order, only where no sample kept is shown
//!    after a sample left over, so decode times stay in order across the
//!    two fragments -- and the rest begins the next slot's content, before
//!    that segment's own samples. Allowed only while the next slot's start
//!    is not decided yet: nothing of it has been made, and no run has been
//!    started at it.
//! 2. **Truncate**, when it is: the same prefix, and what does not fit is
//!    dropped (logged). The picture freezes for those frames; the file
//!    stays the file every earlier answer described.
//!
//! Spilled or truncated, the slot keeps the same prefix -- a function of its
//! content and its size alone -- so a slot made again, after the ring let it
//! go or by a run started at it, is the same bytes whichever it is now. What
//! the first decision fixes is where the next slot starts: the spill's end
//! ([`Cursor`]), or its own segment's beginning; it is recorded once, by
//! `Rendition`'s plan, and kept.

use super::layout::MIN_PAD;
use super::mux::MuxSample;
use super::run::Cut;
use bytes::Bytes;
use std::collections::BTreeMap;

/// A place in the sequence of segments' samples: the next video sample is
/// the `video.1`-th of segment `video.0`, the next audio sample the
/// `audio.1`-th of segment `audio.0`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Cursor {
    pub video: (u64, u32),
    pub audio: (u64, u32),
}

impl Cursor {
    /// The beginning of segment `segment`.
    pub(crate) fn at(segment: u64) -> Self {
        Self {
            video: (segment, 0),
            audio: (segment, 0),
        }
    }

    /// The earliest segment the cursor is in: where a run that makes the
    /// slot starting here has to start.
    pub(crate) fn anchor(&self) -> u64 {
        self.video.0.min(self.audio.0)
    }
}

/// How a slot's content ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum End {
    /// At its segment's end: everything fitted.
    Natural,
    /// Earlier, where the next slot's content begins.
    Spill(Cursor),
    /// Earlier, the rest dropped: the first `video` and `audio` samples
    /// kept.
    Truncated { video: usize, audio: usize },
}

/// One sample of a slot's content, with where it came from.
#[derive(Clone, Debug)]
struct Placed {
    segment: u64,
    index: u32,
    sample: MuxSample,
}

impl Placed {
    fn place(&self) -> (u64, u32) {
        (self.segment, self.index)
    }
}

/// A slot's samples: from its start to the end of its segment.
pub(crate) struct Content {
    slot: u64,
    video: Vec<Placed>,
    audio: Vec<Placed>,
    video_next: Option<i64>,
    audio_next: Option<i64>,
}

/// The content of slot `slot` starting at `start`, from the segments a run
/// made (`segments` holds every one from `start.anchor()` to `slot`; one
/// missing -- past the film's end -- is empty).
pub(crate) fn gather(segments: &BTreeMap<u64, Cut>, start: Cursor, slot: u64) -> Content {
    let mut video = Vec::new();
    let mut audio = Vec::new();
    for (segment, cut) in segments.range(start.anchor()..=slot) {
        let segment = *segment;
        for (track, samples, out) in [
            (start.video, &cut.video, &mut video),
            (start.audio, &cut.audio, &mut audio),
        ] {
            if segment < track.0 {
                continue;
            }
            let skip = if segment == track.0 { track.1 } else { 0 };
            for (index, sample) in samples.iter().enumerate().skip(skip as usize) {
                out.push(Placed {
                    segment,
                    index: index as u32,
                    sample: sample.clone(),
                });
            }
        }
    }
    let own = segments.get(&slot);
    Content {
        slot,
        video,
        audio,
        video_next: own.and_then(|cut| cut.video_next),
        audio_next: own.and_then(|cut| cut.audio_next),
    }
}

/// Muxes a fragment: video and its next sample's time, audio and its.
pub(crate) type Muxer<'a> =
    &'a dyn Fn(&[MuxSample], Option<i64>, &[MuxSample], Option<i64>) -> Bytes;

/// A slot's fragment and how its content ended.
pub(crate) struct Filled {
    pub end: End,
    pub fragment: Bytes,
}

fn samples(placed: &[Placed]) -> Vec<MuxSample> {
    placed.iter().map(|placed| placed.sample.clone()).collect()
}

impl Content {
    fn mux_prefix(&self, video: usize, audio: usize, mux: Muxer<'_>) -> Bytes {
        let video_next = if video < self.video.len() {
            self.video[video..]
                .iter()
                .map(|placed| placed.sample.pts_us)
                .min()
        } else {
            self.video_next
        };
        let audio_next = self
            .audio
            .get(audio)
            .map_or(self.audio_next, |placed| Some(placed.sample.pts_us));
        mux(
            &samples(&self.video[..video]),
            video_next,
            &samples(&self.audio[..audio]),
            audio_next,
        )
    }

    /// The prefixes a slot may keep, smallest first, each a `(video, audio)`
    /// count: video cut only where every sample kept is shown before every
    /// sample left over, audio up to the first left-over video's time. The
    /// whole content is last; nothing at all is first.
    fn prefixes(&self) -> Vec<(usize, usize)> {
        let mut out = vec![(0, 0)];
        if self.video.is_empty() {
            out.extend((1..=self.audio.len()).map(|audio| (0, audio)));
            return out;
        }
        let pts: Vec<i64> = self.video.iter().map(|p| p.sample.pts_us).collect();
        let mut suffix_min = vec![i64::MAX; pts.len() + 1];
        for at in (0..pts.len()).rev() {
            suffix_min[at] = suffix_min[at + 1].min(pts[at]);
        }
        let mut prefix_max = i64::MIN;
        for video in 0..pts.len() {
            if video > 0 {
                prefix_max = prefix_max.max(pts[video - 1]);
            }
            if prefix_max >= suffix_min[video] {
                continue;
            }
            let before = suffix_min[video];
            let audio = self
                .audio
                .iter()
                .take_while(|placed| placed.sample.pts_us < before)
                .count();
            if (video, audio) != (0, 0) {
                out.push((video, audio));
            }
        }
        out.push((self.video.len(), self.audio.len()));
        out
    }

    /// Where the next slot starts when this one keeps `(video, audio)`.
    fn cursor_after(&self, video: usize, audio: usize) -> Cursor {
        let next = (self.slot + 1, 0);
        Cursor {
            video: self.video.get(video).map_or(next, Placed::place),
            audio: self.audio.get(audio).map_or(next, Placed::place),
        }
    }

    /// **Fill a slot of `size` bytes for the first time**: everything when
    /// it fits; otherwise the longest prefix that does, the rest spilled
    /// into the next slot when `may_spill`, dropped when not.
    pub(crate) fn fill(&self, size: u64, may_spill: bool, mux: Muxer<'_>) -> Filled {
        let fits = |fragment: &Bytes| fragment.len() as u64 + MIN_PAD <= size;
        let whole = self.mux_prefix(self.video.len(), self.audio.len(), mux);
        if fits(&whole) {
            return Filled {
                end: End::Natural,
                fragment: whole,
            };
        }
        let prefixes = self.prefixes();
        // The largest that fits: fragments only grow along the list, the
        // empty one always fits a slot (SLOT_BASE), the whole one did not.
        let (mut low, mut high) = (0, prefixes.len() - 1);
        while high - low > 1 {
            let mid = (low + high) / 2;
            let (video, audio) = prefixes[mid];
            if fits(&self.mux_prefix(video, audio, mux)) {
                low = mid;
            } else {
                high = mid;
            }
        }
        let (video, audio) = prefixes[low];
        let end = if may_spill {
            End::Spill(self.cursor_after(video, audio))
        } else {
            tracing::warn!(
                slot = self.slot,
                dropped_video = self.video.len() - video,
                dropped_audio = self.audio.len() - audio,
                stage = "rendition_slot_truncated",
                "a rendition's segment did not fit its slot and the next was already made; \
                 its end is dropped"
            );
            End::Truncated { video, audio }
        };
        Filled {
            end,
            fragment: self.mux_prefix(video, audio, mux),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(pts_us: i64, key: bool, len: usize) -> MuxSample {
        MuxSample {
            pts_us,
            key,
            data: Bytes::from(vec![0u8; len]),
        }
    }

    /// Segment `index`: video frames at `pts` (decode order), 100 bytes
    /// each, the first a key; audio every 250 ms of its second, 10 bytes.
    fn cut(index: u64, pts: &[i64]) -> Cut {
        let start = index as i64 * 1_000_000;
        Cut {
            index,
            video: pts
                .iter()
                .enumerate()
                .map(|(at, pts)| sample(start + pts, at == 0, 100))
                .collect(),
            video_next: Some(start + 1_000_000),
            audio: (0..4)
                .map(|n| sample(start + n * 250_000, true, 10))
                .collect(),
            audio_next: Some(start + 1_000_000),
        }
    }

    /// A muxer whose fragment is one byte per byte of sample, and whose
    /// bytes say what was kept: the presentation times, in order.
    fn fake(video: &[MuxSample], _: Option<i64>, audio: &[MuxSample], _: Option<i64>) -> Bytes {
        let mut out = Vec::new();
        for sample in video.iter().chain(audio) {
            let mut bytes = vec![0u8; sample.data.len()];
            bytes[..8].copy_from_slice(&sample.pts_us.to_be_bytes());
            out.extend_from_slice(&bytes);
        }
        Bytes::from(out)
    }

    /// The times [`fake`] wrote, `videos` video samples first.
    fn kept(fragment: &Bytes, videos: usize) -> Vec<i64> {
        let mut out = Vec::new();
        let mut at = 0;
        while at < fragment.len() {
            out.push(i64::from_be_bytes(fragment[at..at + 8].try_into().unwrap()));
            at += if out.len() <= videos { 100 } else { 10 };
        }
        out
    }

    fn segments(cuts: Vec<Cut>) -> BTreeMap<u64, Cut> {
        cuts.into_iter().map(|cut| (cut.index, cut)).collect()
    }

    /// **What fits is kept whole.**
    #[test]
    fn a_segment_that_fits_ends_naturally() {
        let segments = segments(vec![cut(3, &[0, 250_000, 500_000, 750_000])]);
        let content = gather(&segments, Cursor::at(3), 3);
        let filled = content.fill(440 + MIN_PAD, true, &fake);
        assert_eq!(filled.end, End::Natural);
        assert_eq!(filled.fragment.len(), 440);
    }

    /// **A segment that does not fit spills its end into the next slot**:
    /// the longest prefix that fits, audio up to the first video left over,
    /// and the next slot starts there and holds the rest before its own.
    #[test]
    fn an_overflow_spills_into_the_next_slot() {
        let segments = segments(vec![
            cut(3, &[0, 250_000, 500_000, 750_000]),
            cut(4, &[0, 500_000]),
        ]);
        let content = gather(&segments, Cursor::at(3), 3);
        // Room for two frames and their two audio frames, not three.
        let filled = content.fill(220 + MIN_PAD + 50, true, &fake);
        let cursor = Cursor {
            video: (3, 2),
            audio: (3, 2),
        };
        assert_eq!(filled.end, End::Spill(cursor));
        assert_eq!(filled.fragment.len(), 220);
        let next = gather(&segments, cursor, 4);
        let all = next.fill(10_000, true, &fake);
        assert_eq!(all.end, End::Natural);
        // Two of segment 3's frames, two of segment 4's, then the audio:
        // two of 3's and four of 4's.
        assert_eq!(all.fragment.len(), 4 * 100 + 6 * 10);
    }

    /// **The cut never puts a frame shown earlier after one shown later**:
    /// with B-frames (decode order I P B B), a prefix may end after the I,
    /// or after a P's B-frames, never between a P and the B-frames shown
    /// before it.
    #[test]
    fn a_spill_cuts_only_between_whole_groups() {
        // I0 P3 B1 B2 | P6 B4 B5, frames at 100 ms.
        let segments = segments(vec![cut(
            0,
            &[0, 300_000, 100_000, 200_000, 600_000, 400_000, 500_000],
        )]);
        let content = gather(&segments, Cursor::at(0), 0);
        let valid: Vec<usize> = content.prefixes().iter().map(|(video, _)| *video).collect();
        assert_eq!(
            valid,
            vec![0, 1, 4, 7],
            "nothing, the I, the first group, all"
        );
        // Room for five frames: only four may be kept.
        let filled = content.fill(500 + 10 + MIN_PAD, true, &fake);
        assert_eq!(
            filled.end,
            End::Spill(Cursor {
                video: (0, 4),
                audio: (0, 2),
            })
        );
    }

    /// **When the next slot's start is decided, the overflow is dropped**:
    /// the same prefix, the end recorded as truncated.
    #[test]
    fn an_overflow_with_the_next_slot_decided_is_truncated() {
        let segments = segments(vec![cut(3, &[0, 250_000, 500_000, 750_000])]);
        let content = gather(&segments, Cursor::at(3), 3);
        let filled = content.fill(220 + MIN_PAD + 50, false, &fake);
        assert_eq!(filled.end, End::Truncated { video: 2, audio: 2 });
        assert_eq!(
            kept(&filled.fragment, 2),
            vec![3_000_000, 3_250_000, 3_000_000, 3_250_000]
        );
        let spilled = content.fill(220 + MIN_PAD + 50, true, &fake);
        assert_eq!(
            spilled.fragment, filled.fragment,
            "the same prefix either way"
        );
    }

    /// Nothing fits but the empty fragment: everything spills.
    #[test]
    fn a_slot_too_small_for_one_frame_spills_it_all() {
        let segments = segments(vec![cut(3, &[0, 250_000])]);
        let content = gather(&segments, Cursor::at(3), 3);
        let filled = content.fill(50 + MIN_PAD, true, &fake);
        assert_eq!(filled.end, End::Spill(Cursor::at(3)));
        assert!(filled.fragment.is_empty());
    }
}
