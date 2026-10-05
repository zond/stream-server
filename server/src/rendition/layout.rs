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
//! `sidx` label: `mux::media_segment`) padded with `free` boxes to the
//! slot's end ([`padding`]). Slot sizes are decided here, from the source, and never
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
//! **Every slot after the first begins on a 32 KiB boundary**
//! ([`SLOT_ALIGN`]), each lengthened by less than a block to end where the
//! next begins: the block Chrome asks a seek's byte in is then the slot's
//! own, not the tail of the one before.
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
/// **Every slot after the first begins on a multiple of this**: the block
/// Chrome fetches a file in (`kBlockSizeShift`, 32 KiB, in the Chromecast's
/// Chrome 92 and today's). A demuxer seeking to a slot asks for the byte
/// its `sidx` says, and Chrome asks the server from the start of the block
/// that byte is in -- up to 32 KiB before it. Unaligned, that was the end
/// of the slot before, whose bytes there nothing can say without making it:
/// every seek on zond's TV asked from a round 32 KiB (`bytes=139886592-`
/// for a slot some kilobytes on) and the run started a slot early, a whole
/// GOP made and, from a torrent, fetched before the one wanted. Aligned,
/// the block the demuxer's byte is in begins at the slot.
pub(crate) const SLOT_ALIGN: u64 = 32 * 1024;
/// The least room a fragment leaves in its slot: a `free` box's header and
/// the zero tail.
pub(crate) const MIN_PAD: u64 = 8 + TAIL_ZEROS;
/// The length of a `free` box in a run of padding, the last of a run
/// under twice it ([`padding`]). A demuxer steps over a `free` box, and
/// libavformat makes of a step past what it holds read -- by more than its
/// `short_seek_threshold`, 4096 bytes in the 4.4 a Chromecast with Google
/// TV runs -- a seek; Chrome's reader makes of a seek past what has
/// arrived a new request. Boxes this short are stepped over by reading,
/// whatever the padding's length (measured: `tools/lavf-harness`).
pub(crate) const PAD_BOX: u64 = 1024;
// The longest step over one, under libavformat 4.4's threshold.
const _: () = assert!(2 * PAD_BOX - 8 <= 4096);
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
const MAX_SLOTS: i64 = u16::MAX as i64 / 2;

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
/// that long touches at most `duration_us / CHUNK_US + 2` chunks, and the
/// first may be split in two to fit the slot's first part: at most that
/// many beyond the first, each [`mux::CHUNK_OVERHEAD`] bytes, and the first
/// part's padding ([`mux::FIRST_PART`]). Exact in the count of headers,
/// not a share of the bytes: what the samples weigh is the same whichever
/// chunk they are in.
pub(crate) fn interleave_room(duration_us: i64) -> u64 {
    let chunks = duration_us.max(0) as u64 / mux::CHUNK_US as u64 + 2;
    chunks * mux::CHUNK_OVERHEAD + mux::FIRST_PART as u64
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
    /// for an estimated layout), and how far before it (the indexed track's
    /// decode times' run ahead, `D` for a picture): what
    /// [`Self::slot_for_time`] reads.
    pub label_late_us: i64,
    pub label_ahead_us: i64,
    /// Where the first slot's video begins.
    pub first_us: i64,
}

impl Layout {
    /// The layout for `plan` behind `init`: the `sidx` for `indexed`
    /// (a track and its clock, which every track shares, and how far its
    /// decode times run ahead of its presentation times: `D` for a picture,
    /// [`mux::DECODE_AHEAD_US`]), the film `duration_us` long. A slot after
    /// the first is labelled at its first decode time -- its cut less `D`:
    /// FFmpeg 4.4 takes the label *as* that time (the at-label rule), so no
    /// other label shows the slot's first chunk at its own time there. Every
    /// version picks a slot by its label against the time sought, so a slot
    /// begins `D` before its sync sample is shown ([`Self::slot_for_time`]).
    /// An error is the sentence a viewer is shown, for a film no `sidx` can
    /// describe.
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
        indexed: (u32, u32, i64),
        sound: Option<u32>,
        duration_us: i64,
    ) -> Result<Self, String> {
        let (track, timescale, ahead_us) = indexed;
        let count = plan.sizes.len();
        if count == 0 || 2 * count > usize::from(u16::MAX) {
            return Err("This film is too long to send to the television in one piece.".into());
        }
        // The header's length does not depend on the slots' sizes (a `sidx`
        // is 40 bytes and 12 a reference), so each slot can be lengthened
        // to end where the next must begin ([`SLOT_ALIGN`]) before the
        // `sidx` is written. The last slot ends the file wherever it ends.
        let sidx_len = |refs: usize| 40 + 12 * refs as u64;
        let with_sound = sound.is_some() && plan.label_late_us > 0;
        let header_len = init.len() as u64 + sidx_len(2 * count) * if with_sound { 2 } else { 1 };
        let mut sizes = plan.sizes.clone();
        let mut end = header_len;
        for size in &mut sizes[..count - 1] {
            end += *size;
            let pad = (SLOT_ALIGN - end % SLOT_ALIGN) % SLOT_ALIGN;
            *size += pad;
            end += pad;
        }
        let plan = Plan { sizes, ..plan };
        if plan.sizes.iter().any(|size| *size >= 1 << 31) {
            return Err(
                "This film has a stretch too large to send to the television in one piece.".into(),
            );
        }
        let earliest = mux::ticks(plan.first_us, timescale);
        let end = mux::ticks(duration_us, timescale).max(earliest);
        // Two references a slot: its first part, labelled at its time and
        // lasting to the next slot's, and the rest, lasting nothing --
        // labelled at the next slot's time, so a seek never picks it, and
        // there in FFmpeg's index after the first part (`mux::FIRST_PART`).
        let refs = |late_us: i64, ahead_us: i64| -> Vec<(u32, u32, bool)> {
            let starts: Vec<u64> = (0..count)
                .map(|k| {
                    if k == 0 {
                        earliest
                    } else {
                        mux::ticks(
                            plan.cuts[k]
                                .saturating_add(late_us)
                                .saturating_sub(ahead_us),
                            timescale,
                        )
                        // Short of the film's end by a tick a slot still to
                        // come, so every slot's first part lasts something:
                        // only a slot's rest lasts nothing.
                        .clamp(
                            earliest,
                            end.saturating_sub((count - k) as u64).max(earliest),
                        )
                    }
                })
                .collect();
            (0..count)
                .flat_map(|k| {
                    let next = starts.get(k + 1).copied().unwrap_or(end);
                    let duration = next.saturating_sub(starts[k]);
                    let first = mux::FIRST_PART as u32;
                    [
                        (first, u32::try_from(duration).unwrap_or(u32::MAX), true),
                        ((plan.sizes[k] as u32).saturating_sub(first), 0, false),
                    ]
                })
                .collect()
        };
        let sound = sound
            .filter(|_| plan.label_late_us > 0)
            .map(|sound| mux::sidx(sound, timescale, earliest, 0, &refs(-run::AUDIO_LEAD_US, 0)));
        let sound_len = sound.as_ref().map_or(0, Vec::len) as u64;
        let sidx = mux::sidx(
            track,
            timescale,
            earliest,
            sound_len,
            &refs(plan.label_late_us, ahead_us),
        );
        let init_len = init.len();
        let mut header = Vec::with_capacity(init_len + sidx.len() + sound_len as usize);
        header.extend_from_slice(&init);
        header.extend_from_slice(&sidx);
        header.extend_from_slice(sound.as_deref().unwrap_or_default());
        debug_assert_eq!(header.len() as u64, header_len);
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
            label_ahead_us: ahead_us,
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

    /// **The slot whose `sidx` label is the last at or before `at_us`**:
    /// what a receiver told to start there asks for -- FFmpeg picks a
    /// fragment by its label against the time sought
    /// (`search_frag_timestamp`), nothing taken off it, no composition
    /// offset being negative. A mirrored slot is labelled `D` before its
    /// sync sample is shown, so this is the slot holding the time, or the
    /// next one when the time is in the last `D` before that slot's sync
    /// sample; an estimated layout's labels are a GOP late, so there it can
    /// be an earlier one. A preparation makes it and its neighbours before
    /// the receiver is told to load.
    pub(crate) fn slot_for_time(&self, at_us: i64) -> u64 {
        let label = |cut: &i64| {
            cut.saturating_add(self.label_late_us)
                .saturating_sub(self.label_ahead_us)
        };
        // The first slot's label is the film's start: always at or before.
        let later = self.cuts.get(1..).unwrap_or_default();
        let picked = later.partition_point(|cut| label(cut) <= at_us) as u64;
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

/// A `free` box's header, the box `size` long.
fn free_header(size: u64) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[..4].copy_from_slice(&(size as u32).to_be_bytes());
    out[4..].copy_from_slice(b"free");
    out
}

/// How many [`PAD_BOX`]-long boxes `pad` bytes of padding begin with; one
/// more, the rest, ends them.
fn pad_boxes(pad: u64) -> u64 {
    (pad / PAD_BOX).saturating_sub(1)
}

/// Bytes `from..to` of `pad` bytes of padding (`pad >= 8`, `to <= pad`),
/// appended to `out`: `free` boxes [`PAD_BOX`] long, the last one the rest
/// -- under two of them -- and every body zeros.
pub(crate) fn padding(pad: u64, from: u64, to: u64, out: &mut Vec<u8>) {
    let whole = pad_boxes(pad);
    let mut at = from;
    while at < to {
        let index = (at / PAD_BOX).min(whole);
        let start = index * PAD_BOX;
        let size = if index < whole { PAD_BOX } else { pad - start };
        if at < start + 8 {
            let upto = to.min(start + 8);
            out.extend_from_slice(
                &free_header(size)[(at - start) as usize..(upto - start) as usize],
            );
            at = upto;
        } else {
            let upto = to.min(start + size);
            out.resize(out.len() + (upto - at) as usize, 0);
            at = upto;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: i64 = 1_000_000;

    /// **Padding is boxes a demuxer reads its way over**: `free` boxes
    /// that fill it exactly, none two [`PAD_BOX`] long -- libavformat 4.4
    /// seeks over a step past its buffer of more than 4096 bytes, and a
    /// receiver with nothing buffered then asks again, once a slot -- the
    /// last holding the zero tail, and any range of it the same bytes.
    #[test]
    fn padding_is_short_boxes_whatever_its_length() {
        for pad in [
            8,
            MIN_PAD,
            PAD_BOX - 1,
            PAD_BOX,
            2 * PAD_BOX - 1,
            2 * PAD_BOX,
            2 * PAD_BOX + 1,
            3 * PAD_BOX + 7,
            700_000,
        ] {
            let mut whole = Vec::new();
            padding(pad, 0, pad, &mut whole);
            assert_eq!(whole.len() as u64, pad);
            let mut at = 0;
            let mut last = 0;
            while at < whole.len() {
                let size = u32::from_be_bytes(whole[at..at + 4].try_into().unwrap()) as usize;
                assert_eq!(&whole[at + 4..at + 8], b"free", "{pad} at {at}");
                assert!(
                    size >= 8 && (size as u64) < 2 * PAD_BOX,
                    "{pad}: a box of {size}"
                );
                assert!(whole[at + 8..at + size].iter().all(|byte| *byte == 0));
                last = size;
                at += size;
            }
            assert_eq!(at as u64, pad, "{pad}: the boxes fill it");
            if pad >= MIN_PAD {
                assert!(last as u64 >= MIN_PAD, "{pad}: the tail is zeros");
            }
            for (from, to) in [(0, 1), (3, 9), (PAD_BOX - 2, PAD_BOX + 5), (pad / 2, pad)] {
                let (from, to) = (from.min(pad), to.min(pad));
                let mut part = Vec::new();
                padding(pad, from, to, &mut part);
                assert_eq!(
                    part,
                    whole[from as usize..to as usize],
                    "{pad}: {from}..{to}"
                );
            }
        }
    }

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
        let layout = Layout::new(
            Bytes::new(),
            plan,
            (1, 90_000, mux::DECODE_AHEAD_US),
            None,
            30_000_000,
        )
        .unwrap();
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
        let init = Bytes::from_static(b"\0\0\0\x08ftyp");
        let layout = Layout::new(
            init,
            plan,
            (1, 90_000, mux::DECODE_AHEAD_US),
            None,
            6_000_000,
        )
        .expect("a layout");
        // As lengthened to end on a block
        // (`every_slot_after_the_first_begins_on_a_block`).
        let sizes: Vec<u64> = layout.slots.iter().map(|slot| slot.size).collect();
        let sidx = &layout.header[8..];
        assert_eq!(u32_at(sidx, 0) as usize, sidx.len());
        assert_eq!(&sidx[4..8], b"sidx");
        assert_eq!(sidx[8], 1, "version 1");
        assert_eq!(u32_at(sidx, 12), 1, "the video track");
        assert_eq!(u32_at(sidx, 16), 90_000);
        assert_eq!(&sidx[20..28], &(40_000u64 * 9 / 100).to_be_bytes());
        assert_eq!(&sidx[28..36], &[0; 8], "first_offset");
        assert_eq!(u16::from_be_bytes([sidx[38], sidx[39]]), 12, "two a slot");
        let mut offset = layout.header.len() as u64;
        for (k, size) in sizes.iter().enumerate() {
            // The slot's first part, then the rest: lasting nothing, no
            // SAP, there to be the fragment after the first in FFmpeg's
            // index (`mux::FIRST_PART`).
            let at = 40 + 2 * k * 12;
            let rest = at + 12;
            assert_eq!(
                u32_at(sidx, at) as usize,
                mux::FIRST_PART,
                "slot {k}'s first part"
            );
            assert_eq!(
                u64::from(u32_at(sidx, at) + u32_at(sidx, rest)),
                *size,
                "slot {k}'s size"
            );
            assert_eq!(u32_at(sidx, rest + 4), 0, "slot {k}'s rest lasts nothing");
            assert_eq!(
                u32_at(sidx, rest + 8),
                0,
                "slot {k}'s rest starts with no SAP"
            );
            // Labelled half a second (`D`) before each cut, the first at
            // the first key: the first slot to `D` before the second cut,
            // the last from `D` before its cut to the film's end.
            let duration = if k == 5 {
                6_000_000 - (5_040_000 - 500_000)
            } else {
                1_000_000
            };
            assert_eq!(
                u32_at(sidx, at + 4),
                if k == 0 {
                    (1_040_000 - 500_000 - 40_000) * 9 / 100
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

    /// **Every slot after the first begins on a 32 KiB boundary**, so the
    /// block Chrome fetches for the byte a seek asks is the slot's own
    /// first, not the tail of the slot before: each slot is its plan's
    /// length and less than a block more, the `sidx` says the lengths as
    /// lengthened, and the last slot ends the file where the plan ends it.
    #[test]
    fn every_slot_after_the_first_begins_on_a_block() {
        let index: Vec<IndexEntry> = (0..6)
            .map(|k| entry(k * 1_000_000 + 40_000, 1000 + k as u64 * 123_457))
            .collect();
        let plan = Plan::new(Some(&index), 800_000, 6_000_000, T);
        let planned = plan.sizes.clone();
        let layout = Layout::new(
            Bytes::from_static(b"\0\0\0\x08ftyp"),
            plan,
            (1, 90_000, mux::DECODE_AHEAD_US),
            None,
            6_000_000,
        )
        .expect("a layout");
        let sidx = &layout.header[8..];
        assert_ne!(
            layout.slots[0].offset % SLOT_ALIGN,
            0,
            "the first slot follows the header, wherever that ends"
        );
        for (k, (slot, planned)) in layout.slots.iter().zip(&planned).enumerate() {
            if k > 0 {
                assert_eq!(slot.offset % SLOT_ALIGN, 0, "slot {k} begins on a block");
            }
            let last = k + 1 == planned_len(&layout);
            assert!(
                slot.size >= *planned && slot.size < planned + SLOT_ALIGN,
                "slot {k}: {} for {planned}",
                slot.size
            );
            if last {
                assert_eq!(slot.size, *planned, "the last slot is not lengthened");
            }
            let at = 40 + 2 * k * 12;
            assert_eq!(
                u64::from(u32_at(sidx, at) + u32_at(sidx, at + 12)),
                slot.size,
                "the sidx says slot {k}'s length as lengthened"
            );
        }
        assert!(
            layout.slots[..5]
                .iter()
                .zip(&planned)
                .any(|(slot, planned)| slot.size > *planned),
            "nothing was lengthened: the test's sizes fell on blocks by themselves"
        );
        let last = layout.slots[5];
        assert_eq!(layout.total, last.offset + last.size);
    }

    fn planned_len(layout: &Layout) -> usize {
        layout.slots.len()
    }

    /// **The slot a receiver asks first is the one its time's label picks**:
    /// the last slot labelled at or before the time, read back from the
    /// `sidx` the receiver is handed. Keys at 0.04, 1.04, ... 5.04 s, each
    /// slot labelled half a second (`D`) before its key: 1.0 s is the first
    /// slot's (its key at 0.04 s), 1.03 s -- a frame before the second
    /// key -- and 0.6 s -- in the last `D` before it -- are the second's
    /// already, and 0.5 s is still the first's.
    #[test]
    fn the_slot_for_a_time_is_the_last_labelled_at_or_before_it() {
        let index: Vec<IndexEntry> = (0..6)
            .map(|k| entry(k * 1_000_000 + 40_000, 1000 + k as u64 * 10_000))
            .collect();
        let plan = Plan::new(Some(&index), 70_000, 6_000_000, T);
        let layout = Layout::new(
            Bytes::from_static(b"\0\0\0\x08ftyp"),
            plan,
            (1, 90_000, mux::DECODE_AHEAD_US),
            None,
            6_000_000,
        )
        .expect("a layout");
        let sidx = &layout.header[8..];
        let mut label = u64::from_be_bytes(sidx[20..28].try_into().unwrap());
        let labels: Vec<u64> = (0..6)
            .map(|k| {
                let at = label;
                label += u64::from(u32_at(sidx, 40 + 2 * k * 12 + 4));
                at
            })
            .collect();
        for at_us in [
            0i64, 40_000, 500_000, 539_000, 540_000, 600_000, 1_000_000, 1_030_000, 1_040_000,
            1_540_000, 3_300_000, 4_539_000, 4_540_000, 5_900_000, 9_000_000,
        ] {
            let ticks = mux::ticks(at_us, 90_000);
            let by_label = labels
                .iter()
                .rposition(|label| *label <= ticks)
                .unwrap_or(0) as u64;
            assert_eq!(layout.slot_for_time(at_us), by_label, "{at_us} us");
        }
        assert_eq!(layout.slot_for_time(500_000), 0);
        assert_eq!(
            layout.slot_for_time(600_000),
            1,
            "in the last D before 1.04 s"
        );
        assert_eq!(layout.slot_for_time(1_000_000), 1);
        assert_eq!(layout.slot_for_time(5_900_000), 5);
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
        let layout = |plan| {
            Layout::new(
                Bytes::new(),
                plan,
                (1, 90_000, mux::DECODE_AHEAD_US),
                None,
                1_000_000,
            )
        };
        // Two references a slot.
        assert!(layout(plan(32_767, 10_000)).is_ok());
        assert!(layout(plan(32_768, 10_000)).is_err());
        // The last slot's length is the plan's; one before it may be up
        // to a block longer, to end where the next begins.
        assert!(layout(plan(1, (1 << 31) - 1)).is_ok());
        assert!(layout(plan(1, 1 << 31)).is_err());
        assert!(layout(plan(2, (1 << 31) - SLOT_ALIGN)).is_ok());
        assert!(layout(plan(2, (1 << 31) - 1)).is_err());
    }

    /// **An estimated slot's time in the `sidx` is a GOP late**: its segment
    /// begins at the first sync sample at or after the cut, so the slot a
    /// demuxer picks for a time must be one that began before it. The times
    /// stop at the film's end.
    #[test]
    fn an_estimated_slot_is_labelled_a_gop_after_its_cut() {
        let plan = Plan::new(None, 1_000_000, 30_000_000, 6 * T);
        let layout = Layout::new(
            Bytes::new(),
            plan,
            (1, 1000, mux::DECODE_AHEAD_US),
            None,
            30_000_000,
        )
        .unwrap();
        let sidx = &layout.header[..];
        let durations: Vec<u32> = (0..5).map(|k| u32_at(sidx, 40 + 2 * k * 12 + 4)).collect();
        // A GOP late less the half second its decode times run ahead (`D`):
        // slot 0 from 0 to 15.5 s, slot 1 from 15.5 to 21.5, ..., slot 3
        // from 27.5 to the end at 30 less a tick, slot 4 that last tick --
        // a slot's first part never lasts nothing, which marks its rest.
        assert_eq!(durations, vec![15_500, 6_000, 6_000, 2_499, 1]);
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
    /// second its segment lasts and two more -- the chunks a segment that
    /// long can touch less the first, and one the first may be split into
    /// -- and the slot's first part (`mux::FIRST_PART`).
    #[test]
    fn a_slot_has_room_for_its_chunks_headers() {
        assert_eq!(mux::CHUNK_OVERHEAD, 160);
        assert_eq!(mux::FIRST_PART, 8192);
        assert_eq!(interleave_room(0), 2 * 160 + 8192);
        assert_eq!(interleave_room(499_999), 2 * 160 + 8192);
        assert_eq!(interleave_room(7_500_000), 17 * 160 + 8192);
        assert_eq!(interleave_room(-5), 2 * 160 + 8192);
    }

    /// A run handed samples from a time makes whole the first slot cut at or
    /// after it.
    #[test]
    fn a_run_from_a_time_makes_the_first_slot_cut_after_it() {
        let plan = Plan::new(None, 10_000, 10_000_000, T);
        let layout = Layout::new(
            Bytes::new(),
            plan,
            (1, 90_000, mux::DECODE_AHEAD_US),
            None,
            10_000_000,
        )
        .unwrap();
        assert_eq!(layout.first_slot_from(-5), 0);
        assert_eq!(layout.first_slot_from(0), 0);
        assert_eq!(layout.first_slot_from(1), 1);
        assert_eq!(layout.first_slot_from(3_000_000), 3);
        assert_eq!(layout.first_slot_from(3_000_001), 4);
    }
}
