//! **The byte layout of a rendition's file**, fixed before a byte of it is
//! made (`docs/design/renditions.md`, "Seeking by bytes").
//!
//! A receiver seeks in a progressive file by bytes: it reads the header,
//! finds the time it wants in the `sidx`, and asks for a `Range` there. So
//! the file has to have a length, and every byte of it a value that is the
//! same however often and from wherever it is asked for, before most of it
//! has been produced. That is this layout: the header (`ftyp` + `moov` +
//! `sidx`), then one **slot** per segment, each holding that segment's
//! fragment (`moof` + `mdat`, after a `styp` unless the slot opens at its
//! `sidx` label: `mux::media_segment`) padded with a `free` box to the
//! slot's end. Slot sizes are decided here, from the source, and never
//! change; what goes in a slot is the run's business (`run.rs`), which
//! makes it fit.
//!
//! **Mirrored, when the source has an index.** The segments are cut at the
//! source's own sync samples (the first indexed one at or after each
//! `k x T`), and slot `k` is as long as the source's bytes from segment
//! `k`'s sync sample to segment `k+1`'s -- the same samples, so about the
//! same bytes -- plus [`headroom`]: a fragment carries a `moof` per segment
//! where a Matroska cluster carries a few bytes per block, and an MP4
//! source's sample tables are in its `moov`, not beside the samples. The
//! last slot runs to the source's end. The `sidx` says each slot's length
//! and the indexed times, so it is exact by construction.
//!
//! **Estimated, when it has none** (a transport stream, a Matroska file
//! with no cues): segments on the `k x T` grid, slots in proportion to time
//! over the source's average bytes per second, [`ESTIMATE_SLACK_PERCENT`]
//! larger, plus [`SLOT_BASE`].
//!
//! **The last [`TAIL_ZEROS`] bytes of every slot are zeros**, whatever the
//! fragment: a fit leaves at least [`MIN_PAD`] (`run.rs`), and a `free`
//! box's header is eight bytes. A read of a slot's tail -- a demuxer
//! peeking at the file's last bytes for an `mfra` size -- is answered
//! without producing anything.

use super::IndexEntry;
use super::mux;
use super::run;
use bytes::Bytes;

/// Bytes every slot is given beyond its share of the source.
pub(crate) const SLOT_BASE: u64 = 8 * 1024;
/// A mirrored slot is also given its span over this.
pub(crate) const HEADROOM_DIVISOR: u64 = 64;
/// How much larger than the source's average an estimated slot is.
pub(crate) const ESTIMATE_SLACK_PERCENT: u64 = 15;
/// How much later than its cut an estimated slot's time in the `sidx` is:
/// the longest GOP assumed. An estimated segment begins at the first sync
/// sample at or after its cut -- up to a GOP after it -- and a demuxer that
/// picks the slot whose time is at or before a target must find a sync
/// sample at or before the target in it, or it falls back to what it read
/// before and decodes forward from there (Chrome did, from 24 s to 5:00, on
/// zond's 10-minute film as a transport stream).
pub(crate) const ESTIMATE_LABEL_LATE_US: i64 = 10_000_000;
/// The bytes at the end of every slot that are always zero.
pub(crate) const TAIL_ZEROS: u64 = 16;
/// The least room a fragment leaves in its slot: a `free` box's header and
/// the zero tail.
pub(crate) const MIN_PAD: u64 = 8 + TAIL_ZEROS;
/// How far from the film's end an index may stop and still be one: a GOP
/// is seconds, so an index that stops minutes short is a few entries a
/// demuxer added as it read, not the source's index.
const INDEX_REACH_US: i64 = 60_000_000;

/// **How far apart a mirrored layout's cuts are at least**: a slot is
/// cut at the first indexed sync sample at or after each `k` x this, so a
/// film whose sync samples are a second or more apart has **a slot per
/// sync sample** -- what a seek lands on (FFmpeg's demuxer knows the sync
/// samples of the fragments it has read, and a seek reads one slot's first
/// chunk: the slot's start is where it lands) -- and one whose sync
/// samples are closer has slots of about a second. The spec's segment
/// length is not used: it was the receiver's landing a whole segment early
/// (`docs/design/renditions.md`, *A slot per sync sample*).
pub(crate) const MIRROR_GRID_US: i64 = 1_000_000;

/// The most slots a `sidx` can count.
const MAX_SLOTS: i64 = u16::MAX as i64;

/// The grid a mirrored layout of a film `duration_us` long is cut on:
/// [`MIRROR_GRID_US`], or wider when that would make more slots than a
/// `sidx` counts (a film over 18 hours).
pub(crate) fn mirror_grid(duration_us: i64) -> i64 {
    MIRROR_GRID_US.max(duration_us / (MAX_SLOTS - 1) + 1)
}

/// The room a mirrored slot gets beyond its source span: [`SLOT_BASE`] and
/// a [`HEADROOM_DIVISOR`]th of the span.
pub(crate) fn headroom(span: u64) -> u64 {
    SLOT_BASE + span / HEADROOM_DIVISOR
}

/// **The room a slot's chunks take**, for a segment `duration_us` long:
/// the muxer lays a slot down as a `moof` and an `mdat` per
/// [`mux::CHUNK_US`] of decode time (`mux::media_segment`), and a segment
/// that long touches at most `duration_us / CHUNK_US + 2` chunks, so at
/// most that many less one beyond the first, each [`mux::CHUNK_OVERHEAD`]
/// bytes. Exact in the count of headers, not a share of the bytes: what
/// the samples weigh is the same whichever chunk they are in.
pub(crate) fn interleave_room(duration_us: i64) -> u64 {
    let chunks = duration_us.max(0) as u64 / mux::CHUNK_US as u64 + 1;
    chunks * mux::CHUNK_OVERHEAD
}

/// Where the segments are cut and how long each slot is, from the source;
/// no header yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Plan {
    /// Segment `k` begins at `cuts[k]` (microseconds, the film's clock):
    /// its video at the first sync sample at or after it, its audio there.
    /// `cuts[0]` is [`i64::MIN`]: everything before the second cut is the
    /// first segment's.
    pub cuts: Vec<i64>,
    /// Where the first segment's video begins, for the `sidx`.
    pub first_us: i64,
    /// How much later than its cut each later slot's time in the `sidx` is
    /// ([`ESTIMATE_LABEL_LATE_US`] for an estimate: its segment's real
    /// start is not known; none mirrored, where the cut is the sync
    /// sample).
    pub label_late_us: i64,
    /// Each slot's length in bytes.
    pub sizes: Vec<u64>,
    /// Mirrored from an index (or estimated).
    pub exact: bool,
}

impl Plan {
    /// The plan for a source `source_len` bytes long and `duration_us`
    /// long, cut every `segment_us`: mirrored from `index` when it is one
    /// ([`usable`]), estimated otherwise.
    pub(crate) fn new(
        index: Option<&[IndexEntry]>,
        source_len: u64,
        duration_us: i64,
        segment_us: i64,
    ) -> Self {
        let segment_us = segment_us.max(1);
        let duration_us = duration_us.max(1);
        match index.and_then(|index| usable(index, source_len, duration_us)) {
            Some(candidates) => Self::mirrored(
                &candidates,
                source_len,
                duration_us,
                mirror_grid(duration_us),
            ),
            None => Self::estimated(source_len, duration_us, segment_us),
        }
    }

    fn mirrored(
        candidates: &[IndexEntry],
        source_len: u64,
        duration_us: i64,
        segment_us: i64,
    ) -> Self {
        // Segment k's sync sample: the first candidate at or after k x T,
        // never one already taken -- a GOP longer than T makes no empty
        // segment, it makes fewer.
        let mut chosen: Vec<IndexEntry> = vec![candidates[0]];
        let mut next = 1;
        let mut k = 1i64;
        while next < candidates.len() {
            let target = k.saturating_mul(segment_us);
            match candidates[next..].iter().position(|c| c.pts_us >= target) {
                None => break,
                Some(at) => {
                    let found = next + at;
                    chosen.push(candidates[found]);
                    next = found + 1;
                    let after = candidates[found].pts_us / segment_us + 1;
                    k = after.max(k + 1);
                }
            }
        }
        let mut cuts = Vec::with_capacity(chosen.len());
        let mut sizes = Vec::with_capacity(chosen.len());
        for (at, entry) in chosen.iter().enumerate() {
            cuts.push(if at == 0 { i64::MIN } else { entry.pts_us });
            let end = chosen.get(at + 1).map_or(source_len, |next| next.pos);
            let span = end - entry.pos;
            let lasts = chosen
                .get(at + 1)
                .map_or(duration_us, |next| next.pts_us)
                .saturating_sub(entry.pts_us);
            sizes.push(span + headroom(span) + interleave_room(lasts));
        }
        Self {
            cuts,
            first_us: chosen[0].pts_us.max(0),
            label_late_us: 0,
            sizes,
            exact: true,
        }
    }

    fn estimated(source_len: u64, duration_us: i64, segment_us: i64) -> Self {
        let count = (duration_us as u64).div_ceil(segment_us as u64).max(1);
        let scaled = |k: u64| -> u64 {
            let at = (k as i128 * segment_us as i128).min(duration_us as i128);
            let bytes = at * source_len as i128 * (100 + ESTIMATE_SLACK_PERCENT) as i128
                / (100 * duration_us as i128);
            bytes as u64
        };
        let cuts = (0..count)
            .map(|k| {
                if k == 0 {
                    i64::MIN
                } else {
                    k as i64 * segment_us
                }
            })
            .collect();
        let sizes = (0..count)
            .map(|k| scaled(k + 1) - scaled(k) + SLOT_BASE + interleave_room(segment_us))
            .collect();
        Self {
            cuts,
            first_us: 0,
            label_late_us: ESTIMATE_LABEL_LATE_US,
            sizes,
            exact: false,
        }
    }
}

/// The entries of `index` a layout can mirror -- sorted by time, each
/// after the last in the file, inside the source -- or `None` when it is no
/// index of the whole film: fewer than two, starting or stopping more than
/// [`INDEX_REACH_US`] from the film's ends (the end: or a twentieth of the
/// film), or with a gap longer than that between two entries -- the few
/// entries a demuxer adds while reading or seeking in a file that has none
/// (a transport stream's seek leaves its start and its end).
pub(crate) fn usable(
    index: &[IndexEntry],
    source_len: u64,
    duration_us: i64,
) -> Option<Vec<IndexEntry>> {
    let mut sorted: Vec<IndexEntry> = index
        .iter()
        .copied()
        .filter(|entry| entry.pos < source_len)
        .collect();
    sorted.sort_by_key(|entry| (entry.pts_us, entry.pos));
    let mut candidates: Vec<IndexEntry> = Vec::with_capacity(sorted.len());
    for entry in sorted {
        // Several sync samples in one cluster share its position: the
        // first is the one a slot can start at.
        if candidates.last().is_none_or(|last| entry.pos > last.pos) {
            candidates.push(entry);
        }
    }
    let (first, last) = (candidates.first()?, candidates.last()?);
    let reach = INDEX_REACH_US.max(duration_us / 20);
    let dense = candidates
        .windows(2)
        .all(|pair| pair[1].pts_us - pair[0].pts_us <= INDEX_REACH_US);
    (candidates.len() >= 2
        && dense
        && first.pts_us <= INDEX_REACH_US
        && last.pts_us.saturating_add(reach) >= duration_us)
        .then_some(candidates)
}

/// One slot: where it is in the file and how long.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Slot {
    pub offset: u64,
    pub size: u64,
}

impl Slot {
    /// Whether a read from `from` (an offset inside the slot) to the slot's
    /// end, or less, lies in its zero tail.
    pub(crate) fn in_tail(&self, from: u64) -> bool {
        from >= self.size.saturating_sub(TAIL_ZEROS)
    }
}

/// The whole file's shape: the header bytes, the slots, the length.
#[derive(Debug)]
pub(crate) struct Layout {
    /// `ftyp` + `moov` + `sidx` (two for an estimated layout with sound).
    pub header: Bytes,
    /// Where the init segment ends inside [`Self::header`].
    pub init_len: usize,
    pub slots: Vec<Slot>,
    pub total: u64,
    /// The plan's cuts, which the run's cutter cuts at.
    pub cuts: std::sync::Arc<[i64]>,
    pub exact: bool,
    /// How far after its cut each slot's `sidx` label is (0 mirrored, a GOP
    /// for an estimated layout): what [`Self::slot_for_time`] reads.
    pub label_late_us: i64,
    /// Where the first slot's video begins.
    pub first_us: i64,
}

impl Layout {
    /// The layout for `plan` behind `init`: the `sidx` for `indexed`
    /// (a track and its clock, which every track shares), the film
    /// `duration_us` long. An error is the sentence a viewer is shown, for
    /// a film no `sidx` can describe.
    ///
    /// An estimated layout indexes `sound` (the sound's track beside a
    /// picture) again, in a second `sidx` labelled at each cut less the
    /// sound's lead, not a GOP late. A seek places the picture on a sync
    /// sample inside the slot its late label picked, which can be before
    /// that label; FFmpeg then seeks the sound to that sample's time, and
    /// by the picture's labels that is the slot before -- 6.0 to 8.0
    /// asked for it (master does not). Labelled early, the sound's slot is
    /// the picture's or a later one, never an earlier.
    pub(crate) fn new(
        init: Bytes,
        plan: Plan,
        indexed: (u32, u32),
        sound: Option<u32>,
        duration_us: i64,
    ) -> Result<Self, String> {
        let (track, timescale) = indexed;
        let count = plan.sizes.len();
        if count == 0 || count > usize::from(u16::MAX) {
            return Err("This film is too long to send to the television in one piece.".into());
        }
        if plan.sizes.iter().any(|size| *size >= 1 << 31) {
            return Err(
                "This film has a stretch too large to send to the television in one piece.".into(),
            );
        }
        let earliest = mux::ticks(plan.first_us, timescale);
        let end = mux::ticks(duration_us, timescale).max(earliest);
        let refs = |late_us: i64| -> Vec<(u32, u32)> {
            let starts: Vec<u64> = (0..count)
                .map(|k| {
                    if k == 0 {
                        earliest
                    } else {
                        mux::ticks(plan.cuts[k].saturating_add(late_us), timescale)
                            .clamp(earliest, end)
                    }
                })
                .collect();
            (0..count)
                .map(|k| {
                    let next = starts.get(k + 1).copied().unwrap_or(end);
                    let duration = next.saturating_sub(starts[k]);
                    (
                        plan.sizes[k] as u32,
                        u32::try_from(duration).unwrap_or(u32::MAX),
                    )
                })
                .collect()
        };
        let sound = sound
            .filter(|_| plan.label_late_us > 0)
            .map(|sound| mux::sidx(sound, timescale, earliest, 0, &refs(-run::AUDIO_LEAD_US)));
        let sound_len = sound.as_ref().map_or(0, Vec::len) as u64;
        let sidx = mux::sidx(
            track,
            timescale,
            earliest,
            sound_len,
            &refs(plan.label_late_us),
        );
        let init_len = init.len();
        let mut header = Vec::with_capacity(init_len + sidx.len() + sound_len as usize);
        header.extend_from_slice(&init);
        header.extend_from_slice(&sidx);
        header.extend_from_slice(sound.as_deref().unwrap_or_default());
        let mut offset = header.len() as u64;
        let slots = plan
            .sizes
            .iter()
            .map(|size| {
                let slot = Slot {
                    offset,
                    size: *size,
                };
                offset += size;
                slot
            })
            .collect();
        Ok(Self {
            header: Bytes::from(header),
            init_len,
            slots,
            total: offset,
            cuts: plan.cuts.into(),
            exact: plan.exact,
            label_late_us: plan.label_late_us,
            first_us: plan.first_us,
        })
    }

    /// The slot holding byte `offset`, if it is past the header.
    pub(crate) fn slot_at(&self, offset: u64) -> Option<u64> {
        if offset < self.header.len() as u64 || offset >= self.total {
            return None;
        }
        let at = self.slots.partition_point(|slot| slot.offset <= offset);
        Some(at as u64 - 1)
    }

    /// **The slot whose `sidx` label is at or before `at_us`**: the slot
    /// holding it for a mirrored layout, possibly an earlier one for an
    /// estimated layout, whose labels are late. What a receiver told to
    /// start there asks for -- or the slot before, for a time on a cut
    /// (FFmpeg seeks by the time less a frame or two); a preparation makes
    /// it and its neighbours before the receiver is told to load.
    pub(crate) fn slot_for_time(&self, at_us: i64) -> u64 {
        let late = self.label_late_us;
        let picked = self
            .cuts
            .partition_point(|cut| cut.saturating_add(late) <= at_us)
            .saturating_sub(1) as u64;
        picked.min(self.slots.len() as u64 - 1)
    }

    /// Where slot `slot` begins on the film's clock: its cut, the first
    /// slot from the film's first sync sample.
    fn start_us(&self, slot: u64) -> i64 {
        match slot {
            0 => self.first_us,
            _ => self.cuts.get(slot as usize).copied().unwrap_or(i64::MAX),
        }
    }

    /// **The last slot that begins at most `ahead_us` after slot `slot`
    /// does** (`slot` itself at least): how far a run's lookahead reaches
    /// from it, and a preparation's from a start.
    pub(crate) fn reach(&self, slot: u64, ahead_us: i64) -> u64 {
        let until = self.start_us(slot).saturating_add(ahead_us);
        let last = (self.cuts.partition_point(|cut| *cut <= until) as u64).saturating_sub(1);
        last.max(slot)
    }

    /// The first slot whose cut is at or after `from_us`: the first a run
    /// handed samples from `from_us` on can make whole.
    pub(crate) fn first_slot_from(&self, from_us: i64) -> u64 {
        if from_us <= 0 {
            return 0;
        }
        self.cuts.partition_point(|cut| *cut < from_us) as u64
    }
}

/// The `free` box header for `pad` bytes of padding (`pad >= MIN_PAD`).
pub(crate) fn free_header(pad: u64) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[..4].copy_from_slice(&(pad as u32).to_be_bytes());
    out[4..].copy_from_slice(b"free");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: i64 = 1_000_000;

    fn entry(pts_us: i64, pos: u64) -> IndexEntry {
        IndexEntry { pts_us, pos }
    }

    /// **Mirrored**: segment k at the first indexed sync sample at or after
    /// k x T, each slot the source's bytes to the next one plus headroom,
    /// the last to the source's end; a GOP longer than T makes one segment,
    /// not an empty one.
    #[test]
    fn an_index_is_mirrored_slot_by_slot() {
        // Keys at 0, 0.48, 0.96, 1.44, 1.92, 4.0, 4.48 s (a long GOP from
        // 1.92 to 4.0), 1000 bytes a second, the film 5 s and 5000 bytes.
        let index: Vec<IndexEntry> = [0, 480, 960, 1440, 1920, 4000, 4480]
            .iter()
            .map(|ms| entry(ms * 1000, 100 + *ms as u64))
            .collect();
        let plan = Plan::new(Some(&index), 5_100, 5_000_000, T);
        assert!(plan.exact);
        assert_eq!(
            plan.cuts,
            vec![i64::MIN, 1_440_000, 4_000_000],
            "1 s takes 1.44, 2 s and 3 s take 4.0 once, 4 s and 5 s nothing new"
        );
        assert_eq!(plan.first_us, 0);
        let spans = [1_440, 4_000 - 1_440, 5_100 - 4_100];
        let lasts = [1_440_000, 4_000_000 - 1_440_000, 5_000_000 - 4_000_000];
        assert_eq!(
            plan.sizes,
            spans
                .iter()
                .zip(lasts)
                .map(|(span, lasts)| span + headroom(*span) + interleave_room(lasts))
                .collect::<Vec<_>>()
        );
    }

    /// **Mirrored, a slot per sync sample, whatever the segment length**:
    /// keys every 2.8 s in 6 s segments are a slot each -- a seek lands on
    /// a slot's start, so the slot must start at the key before the target
    /// -- and keys every 0.4 s make slots of about a second, the first key
    /// at or after each second. A film too long for a `sidx`'s count of
    /// slots at one a second is cut on a wider grid.
    #[test]
    fn a_mirrored_layout_has_a_slot_per_sync_sample() {
        let keys: Vec<i64> = (0..11).map(|k| k * 2_800_000).collect();
        let index: Vec<IndexEntry> = keys
            .iter()
            .enumerate()
            .map(|(k, pts)| entry(*pts, 1_000 * k as u64))
            .collect();
        let plan = Plan::new(Some(&index), 11_000, 30_000_000, 6 * T);
        assert!(plan.exact);
        assert_eq!(plan.cuts[1..], keys[1..]);

        let dense: Vec<IndexEntry> = (0..75)
            .map(|k| entry(k * 400_000, 100 * k as u64))
            .collect();
        let plan = Plan::new(Some(&dense), 7_500, 30_000_000, 6 * T);
        assert_eq!(
            plan.cuts[1..6],
            [1_200_000, 2_000_000, 3_200_000, 4_000_000, 5_200_000]
        );

        assert_eq!(mirror_grid(3_600_000_000), MIRROR_GRID_US);
        let days = 2 * 24 * 3_600_000_000;
        assert!(days / mirror_grid(days) < 65_535);
    }

    /// **How far a lookahead reaches**: the last slot beginning within the
    /// time of the slot it is counted from, that slot at least.
    #[test]
    fn reach_is_the_last_slot_beginning_within_the_time() {
        let keys: Vec<i64> = (0..11).map(|k| 40_000 + k * 2_800_000).collect();
        let index: Vec<IndexEntry> = keys
            .iter()
            .enumerate()
            .map(|(k, pts)| entry(*pts, 1_000 * k as u64))
            .collect();
        let plan = Plan::new(Some(&index), 11_000, 30_000_000, 6 * T);
        let layout = Layout::new(Bytes::new(), plan, (1, 90_000), None, 30_000_000).unwrap();
        // From slot 0 (at 0.04 s): slots beginning by 12.04 s, the last at
        // 11.24 s (slot 4).
        assert_eq!(layout.reach(0, 12_000_000), 4);
        assert_eq!(layout.reach(2, 12_000_000), 6);
        assert_eq!(layout.reach(2, 0), 2);
        assert_eq!(layout.reach(10, 12_000_000), 10);
    }

    /// Entries that share a position (several keys in one cluster) or go
    /// back in the file are not slot starts; entries out of order in time
    /// are sorted first.
    #[test]
    fn only_entries_further_into_the_file_are_candidates() {
        let index = [
            entry(2_000_000, 300),
            entry(0, 10),
            entry(1_000_000, 200),
            entry(1_500_000, 200),
            entry(2_500_000, 250),
            entry(3_000_000, 400),
        ];
        let candidates = usable(&index, 1000, 3_000_000).expect("an index");
        assert_eq!(
            candidates,
            vec![
                entry(0, 10),
                entry(1_000_000, 200),
                entry(2_000_000, 300),
                entry(3_000_000, 400)
            ]
        );
    }

    /// **An index that is not the film's is estimated instead**: none, one
    /// entry, or a few at the start (what a demuxer adds as it reads a
    /// file with none).
    #[test]
    fn a_partial_index_is_no_index() {
        let start = [entry(0, 0), entry(2_000_000, 1000)];
        let hour = 3_600_000_000;
        assert!(usable(&start, 1 << 30, hour).is_none());
        assert!(!Plan::new(Some(&start), 1 << 30, hour, 6 * T).exact);
        assert!(!Plan::new(Some(&start[..1]), 1 << 30, 3 * T, T).exact);
        assert!(!Plan::new(None, 1 << 30, 3 * T, T).exact);
        // Within a minute of the end is the whole film's, every gap a
        // minute at most.
        let whole: Vec<IndexEntry> = (0..=60)
            .map(|minute| entry(minute * 59_000_000, minute as u64 * 1000))
            .collect();
        assert!(usable(&whole, 1 << 30, hour).is_some());
        // What a transport stream's seek leaves: its start and its end, an
        // hour apart -- no index.
        let ends = [entry(0, 0), entry(hour - 50_000_000, 1000)];
        assert!(usable(&ends, 1 << 30, hour).is_none());
        // Nor one that begins a minute in.
        assert!(usable(&whole[2..], 1 << 30, hour).is_none());
    }

    /// **Estimated**: the grid, slots in proportion to time over the
    /// source's bytes, the slack and the base on top; the last slot is the
    /// film's remainder.
    #[test]
    fn no_index_is_estimated_on_the_grid() {
        let plan = Plan::new(None, 10_000, 2_500_000, T);
        assert!(!plan.exact);
        assert_eq!(plan.cuts, vec![i64::MIN, 1_000_000, 2_000_000]);
        let slack = |bytes: u64| bytes * (100 + ESTIMATE_SLACK_PERCENT) / 100;
        let room = interleave_room(T);
        assert_eq!(
            plan.sizes,
            vec![
                slack(4_000) + SLOT_BASE + room,
                slack(8_000) - slack(4_000) + SLOT_BASE + room,
                slack(10_000) - slack(8_000) + SLOT_BASE + room,
            ]
        );
        assert_eq!(
            plan.sizes.iter().sum::<u64>(),
            slack(10_000) + 3 * (SLOT_BASE + room)
        );
    }

    fn u32_at(data: &[u8], at: usize) -> u32 {
        u32::from_be_bytes(data[at..at + 4].try_into().unwrap())
    }

    /// **The header is the init segment and a `sidx`** -- version 1, the
    /// track's clock, the first sync sample's time, offsets from right after
    /// it -- with one reference per slot: its size, its duration from the
    /// cuts (the last to the film's end), starting with a SAP. The slots
    /// follow it back to back, to the total.
    #[test]
    fn the_sidx_references_every_slot() {
        let index: Vec<IndexEntry> = (0..6)
            .map(|k| entry(k * 1_000_000 + 40_000, 1000 + k as u64 * 10_000))
            .collect();
        let plan = Plan::new(Some(&index), 70_000, 6_000_000, T);
        let sizes = plan.sizes.clone();
        let init = Bytes::from_static(b"\0\0\0\x08ftyp");
        let layout = Layout::new(init, plan, (1, 90_000), None, 6_000_000).expect("a layout");
        let sidx = &layout.header[8..];
        assert_eq!(u32_at(sidx, 0) as usize, sidx.len());
        assert_eq!(&sidx[4..8], b"sidx");
        assert_eq!(sidx[8], 1, "version 1");
        assert_eq!(u32_at(sidx, 12), 1, "the video track");
        assert_eq!(u32_at(sidx, 16), 90_000);
        assert_eq!(&sidx[20..28], &(40_000u64 * 9 / 100).to_be_bytes());
        assert_eq!(&sidx[28..36], &[0; 8], "first_offset");
        assert_eq!(u16::from_be_bytes([sidx[38], sidx[39]]), 6);
        let mut offset = layout.header.len() as u64;
        for (k, size) in sizes.iter().enumerate() {
            let at = 40 + k * 12;
            assert_eq!(u64::from(u32_at(sidx, at)), *size, "slot {k}'s size");
            let duration = if k == 5 {
                6_000_000 - 5_040_000
            } else {
                1_000_000
            };
            assert_eq!(
                u32_at(sidx, at + 4),
                if k == 0 {
                    // From the first key to the second cut.
                    (1_040_000 - 40_000) * 9 / 100
                } else {
                    duration * 9 / 100
                },
                "slot {k}'s duration"
            );
            assert_eq!(u32_at(sidx, at + 8), 1 << 31, "starts with a SAP");
            assert_eq!(layout.slots[k].offset, offset);
            offset += size;
        }
        assert_eq!(layout.total, offset);
        assert_eq!(layout.slot_at(layout.header.len() as u64 - 1), None);
        assert_eq!(layout.slot_at(layout.header.len() as u64), Some(0));
        assert_eq!(layout.slot_at(layout.slots[3].offset - 1), Some(2));
        assert_eq!(layout.slot_at(layout.slots[3].offset), Some(3));
        assert_eq!(layout.slot_at(layout.total - 1), Some(5));
        assert_eq!(layout.slot_at(layout.total), None);
    }

    /// **A film no `sidx` can describe is refused with a sentence**: more
    /// slots than its 16-bit count, or a slot past its 31-bit size.
    #[test]
    fn a_layout_past_the_sidx_is_refused() {
        let plan = |count: usize, size: u64| Plan {
            cuts: vec![i64::MIN; count],
            first_us: 0,
            label_late_us: 0,
            sizes: vec![size; count],
            exact: false,
        };
        let layout = |plan| Layout::new(Bytes::new(), plan, (1, 90_000), None, 1_000_000);
        assert!(layout(plan(65_535, 100)).is_ok());
        assert!(layout(plan(65_536, 100)).is_err());
        assert!(layout(plan(2, (1 << 31) - 1)).is_ok());
        assert!(layout(plan(2, 1 << 31)).is_err());
    }

    /// **An estimated slot's time in the `sidx` is a GOP late**: its segment
    /// begins at the first sync sample at or after the cut, so the slot a
    /// demuxer picks for a time must be one that began before it. The times
    /// stop at the film's end.
    #[test]
    fn an_estimated_slot_is_labelled_a_gop_after_its_cut() {
        let plan = Plan::new(None, 1_000_000, 30_000_000, 6 * T);
        let layout = Layout::new(Bytes::new(), plan, (1, 1000), None, 30_000_000).unwrap();
        let sidx = &layout.header[..];
        let durations: Vec<u32> = (0..5).map(|k| u32_at(sidx, 40 + k * 12 + 4)).collect();
        // Slot 0 from 0 to 16 s, slot 1 from 16 to 22, ..., slot 3 from 28
        // to the end at 30, slot 4 from the end.
        assert_eq!(durations, vec![16_000, 6_000, 6_000, 2_000, 0]);
        let mirrored = Plan::new(
            Some(&[entry(0, 0), entry(6 * T + 40_000, 500), entry(12 * T, 900)]),
            1_000,
            12 * T,
            6 * T,
        );
        assert_eq!(
            mirrored.label_late_us, 0,
            "a mirrored cut is the sync sample"
        );
    }

    /// **A slot's room for its chunks' headers**: 160 bytes for each half
    /// second its segment lasts, and one more -- the chunks a segment that
    /// long can touch, less the first.
    #[test]
    fn a_slot_has_room_for_its_chunks_headers() {
        assert_eq!(mux::CHUNK_OVERHEAD, 160);
        assert_eq!(interleave_room(0), 160);
        assert_eq!(interleave_room(499_999), 160);
        assert_eq!(interleave_room(7_500_000), 16 * 160);
        assert_eq!(interleave_room(-5), 160);
    }

    /// A run handed samples from a time makes whole the first slot cut at or
    /// after it.
    #[test]
    fn a_run_from_a_time_makes_the_first_slot_cut_after_it() {
        let plan = Plan::new(None, 10_000, 10_000_000, T);
        let layout = Layout::new(Bytes::new(), plan, (1, 90_000), None, 10_000_000).unwrap();
        assert_eq!(layout.first_slot_from(-5), 0);
        assert_eq!(layout.first_slot_from(0), 0);
        assert_eq!(layout.first_slot_from(1), 1);
        assert_eq!(layout.first_slot_from(3_000_000), 3);
        assert_eq!(layout.first_slot_from(3_000_001), 4);
    }
}
