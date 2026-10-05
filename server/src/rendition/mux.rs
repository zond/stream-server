//! **The fMP4 muxer** (`docs/design/renditions.md` §2.2): an init segment
//! from the tracks' formats, and a media segment from one segment's
//! samples. Hand-written, because the boxes are a short list and a crate
//! for them would be a dependency for forty lines of byte layout.
//!
//! The init segment is `ftyp` + `moov` (`mvhd`, `mvex` with `mehd` and one
//! `trex` per track, and one `trak` per track: `tkhd`, `mdia` with `mdhd`,
//! `hdlr` and `minf` -- `vmhd`/`smhd`, `dinf`/`dref`, and an `stbl` whose
//! only entry is the sample description: `avc1`+`avcC`, `hvc1`+`hvcC` --
//! the `hvcC` with the SEI messages beside the parameter sets, and a `colr`
//! after it when the SPS describes its colours -- or `mp4a`+`esds`, every
//! sample table empty). A media segment is a `styp`
//! unless it opens at its `sidx` label (see [`media_segment`]), then per
//! half second of it ([`CHUNK_US`]) a `moof` (`mfhd`, and per track with
//! samples in the chunk one `traf`: `tfhd` with default-base-is-moof,
//! `tfdt` version 1, `trun` version 1) + `mdat`, the chunk's picture and
//! then the sound beside it, so a reader going straight through never
//! goes back and forth between the two.
//!
//! **Times.** Video is on a 90 kHz clock, audio on its sample rate, both
//! from the producer's microseconds. A video sample's decode time is not
//! reported (`MediaExtractor` has only a presentation time), so the decode
//! times of a segment are its presentation times sorted -- the reorder
//! window is at most a segment, which starts at a sync sample -- and each
//! sample's composition offset is the difference, signed (`trun` version
//! 1). With no reordering, every offset is zero.
//!
//! **Samples** come in Annex-B (start codes) and leave length-prefixed
//! with four-byte lengths, access unit delimiters dropped; a sample that
//! does not begin with a start code is taken as length-prefixed already.

use super::TrackFormat;
use bytes::Bytes;

/// The video clock: 90 kHz, as every MPEG system clock is.
pub(crate) const VIDEO_TIMESCALE: u32 = 90_000;

/// The track ids: video is 1, audio is 2 (or 1 when there is no video).
pub(crate) const VIDEO_TRACK: u32 = 1;

/// What a run reported for its tracks, frozen into the init segment by the
/// first run and compared against every later one.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Formats {
    pub video: Option<TrackFormat>,
    pub audio: Option<TrackFormat>,
}

impl Formats {
    fn audio_track_id(&self) -> u32 {
        if self.video.is_some() { 2 } else { 1 }
    }

    /// The sound's track when there is a picture too: the one an estimated
    /// layout indexes again, early (see [`super::layout::Layout::new`]).
    pub(crate) fn sound_beside_picture(&self) -> Option<u32> {
        (self.video.is_some() && self.audio.is_some()).then(|| self.audio_track_id())
    }

    /// The track the `sidx` indexes, and its clock: the video's, or the
    /// sound's when there is no picture.
    pub(crate) fn indexed_track(&self) -> (u32, u32) {
        if self.video.is_some() {
            (VIDEO_TRACK, VIDEO_TIMESCALE)
        } else {
            (self.audio_track_id(), self.audio_timescale())
        }
    }

    /// The sound's clock: the picture's when there is one, so that every
    /// track counts time alike, or else its sample rate. FFmpeg before 6.0
    /// (commit e1e981c, 2022) places a track the `sidx` does not index by
    /// comparing its seek time against the indexed track's times unscaled:
    /// with sound on 48 kHz and picture on 90 kHz, a seek to 85 s placed
    /// the sound at 45 s, and the Chromecast read on from there.
    fn audio_timescale(&self) -> u32 {
        if self.video.is_some() {
            VIDEO_TIMESCALE
        } else {
            self.audio_sample_rate()
        }
    }

    fn audio_sample_rate(&self) -> u32 {
        match &self.audio {
            Some(TrackFormat::Aac { sample_rate, .. }) => (*sample_rate).max(1),
            _ => 48_000,
        }
    }

    /// One AAC frame (1024 samples) on the sound's clock.
    fn audio_frame_ticks(&self) -> u64 {
        1024 * u64::from(self.audio_timescale()) / u64::from(self.audio_sample_rate())
    }
}

/// One access unit for the muxer: when it is shown, whether it is a sync
/// sample, and its bytes as the producer gave them.
#[derive(Clone, Debug)]
pub(crate) struct MuxSample {
    pub pts_us: i64,
    pub key: bool,
    pub data: Bytes,
}

// --- Box writing --------------------------------------------------------------

fn bx(kind: &[u8; 4], parts: &[&[u8]]) -> Vec<u8> {
    let len: usize = 8 + parts.iter().map(|part| part.len()).sum::<usize>();
    let mut out = Vec::with_capacity(len);
    out.extend_from_slice(&(len as u32).to_be_bytes());
    out.extend_from_slice(kind);
    for part in parts {
        out.extend_from_slice(part);
    }
    out
}

fn full(kind: &[u8; 4], version: u8, flags: u32, parts: &[&[u8]]) -> Vec<u8> {
    let mut head = flags.to_be_bytes();
    head[0] = version;
    let mut all: Vec<&[u8]> = Vec::with_capacity(parts.len() + 1);
    all.push(&head);
    all.extend_from_slice(parts);
    bx(kind, &all)
}

/// The identity matrix `mvhd` and `tkhd` carry.
const MATRIX: [u32; 9] = [0x0001_0000, 0, 0, 0, 0x0001_0000, 0, 0, 0, 0x4000_0000];

fn matrix() -> Vec<u8> {
    MATRIX
        .iter()
        .flat_map(|value| value.to_be_bytes())
        .collect()
}

// --- Annex-B ---------------------------------------------------------------------

/// The NAL units of an Annex-B buffer, start codes and trailing zeros off.
/// A buffer with no start code at all is one NAL unit.
pub(crate) fn annex_b_units(data: &[u8]) -> Vec<&[u8]> {
    let mut starts = Vec::new();
    let mut i = 0;
    while i + 3 <= data.len() {
        if data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1 {
            starts.push(i + 3);
            i += 3;
        } else {
            i += 1;
        }
    }
    if starts.is_empty() {
        return if data.is_empty() { vec![] } else { vec![data] };
    }
    let mut units = Vec::with_capacity(starts.len());
    for (index, &start) in starts.iter().enumerate() {
        let end = starts.get(index + 1).map_or(data.len(), |next| next - 3);
        let mut unit = &data[start..end];
        while let [rest @ .., 0] = unit {
            unit = rest;
        }
        if !unit.is_empty() {
            units.push(unit);
        }
    }
    units
}

fn starts_with_start_code(data: &[u8]) -> bool {
    data.starts_with(&[0, 0, 1]) || data.starts_with(&[0, 0, 0, 1])
}

/// A video sample, length-prefixed: Annex-B's units each behind a four-byte
/// length, access unit delimiters dropped (H.264 type 9, HEVC type 35).
fn length_prefixed(data: &Bytes, hevc: bool) -> Bytes {
    if !starts_with_start_code(data) {
        return data.clone();
    }
    let mut out = Vec::with_capacity(data.len() + 16);
    for unit in annex_b_units(data) {
        let delimiter = if hevc {
            (unit[0] >> 1) & 0x3f == 35
        } else {
            unit[0] & 0x1f == 9
        };
        if delimiter {
            continue;
        }
        out.extend_from_slice(&(unit.len() as u32).to_be_bytes());
        out.extend_from_slice(unit);
    }
    Bytes::from(out)
}

// --- Parameter sets --------------------------------------------------------------

/// An RBSP bit reader over a NAL unit's payload, emulation prevention
/// bytes removed.
struct Bits {
    data: Vec<u8>,
    bit: usize,
}

impl Bits {
    fn of_nal(payload: &[u8]) -> Self {
        let mut data = Vec::with_capacity(payload.len());
        let mut zeros = 0;
        for &byte in payload {
            if zeros >= 2 && byte == 3 {
                zeros = 0;
                continue;
            }
            zeros = if byte == 0 { zeros + 1 } else { 0 };
            data.push(byte);
        }
        Self { data, bit: 0 }
    }

    fn u(&mut self, n: u32) -> Option<u64> {
        let mut value = 0u64;
        for _ in 0..n {
            let byte = *self.data.get(self.bit / 8)?;
            let set = (byte >> (7 - (self.bit % 8))) & 1;
            value = (value << 1) | u64::from(set);
            self.bit += 1;
        }
        Some(value)
    }

    fn ue(&mut self) -> Option<u64> {
        let mut zeros = 0;
        while self.u(1)? == 0 {
            zeros += 1;
            if zeros > 32 {
                return None;
            }
        }
        Some((1u64 << zeros) - 1 + self.u(zeros)?)
    }
}

/// What an `avcC` needs from an H.264 SPS beyond its first three bytes.
struct AvcSps {
    chroma_format_idc: u8,
    bit_depth_luma_minus8: u8,
    bit_depth_chroma_minus8: u8,
}

fn high_profile(profile: u8) -> bool {
    matches!(
        profile,
        100 | 110 | 122 | 244 | 44 | 83 | 86 | 118 | 128 | 138 | 139 | 134 | 135
    )
}

fn parse_avc_sps(sps: &[u8]) -> Option<AvcSps> {
    let mut bits = Bits::of_nal(sps.get(1..)?);
    let profile = bits.u(8)? as u8;
    bits.u(16)?; // constraint flags, level
    bits.ue()?; // seq_parameter_set_id
    let mut parsed = AvcSps {
        chroma_format_idc: 1,
        bit_depth_luma_minus8: 0,
        bit_depth_chroma_minus8: 0,
    };
    if high_profile(profile) {
        parsed.chroma_format_idc = bits.ue()? as u8;
        if parsed.chroma_format_idc == 3 {
            bits.u(1)?;
        }
        parsed.bit_depth_luma_minus8 = bits.ue()? as u8;
        parsed.bit_depth_chroma_minus8 = bits.ue()? as u8;
    }
    Some(parsed)
}

/// `avcC` from the parameter sets in `csd-0` and `csd-1`, in Annex-B.
pub(crate) fn avcc(csd0: &[u8], csd1: &[u8]) -> Result<Vec<u8>, String> {
    let mut sps = Vec::new();
    let mut pps = Vec::new();
    let mut sps_ext = Vec::new();
    for unit in annex_b_units(csd0).into_iter().chain(annex_b_units(csd1)) {
        match unit[0] & 0x1f {
            7 => sps.push(unit),
            8 => pps.push(unit),
            13 => sps_ext.push(unit),
            _ => {}
        }
    }
    let first = *sps
        .first()
        .ok_or("the video's codec configuration carries no H.264 sequence parameter set")?;
    if first.len() < 4 || pps.is_empty() {
        return Err("the video's H.264 codec configuration is incomplete".to_string());
    }
    let parsed =
        parse_avc_sps(first).ok_or("the video's H.264 sequence parameter set cannot be read")?;
    let mut out = vec![1, first[1], first[2], first[3], 0xfc | 3];
    out.push(0xe0 | sps.len() as u8);
    for unit in &sps {
        out.extend_from_slice(&(unit.len() as u16).to_be_bytes());
        out.extend_from_slice(unit);
    }
    out.push(pps.len() as u8);
    for unit in &pps {
        out.extend_from_slice(&(unit.len() as u16).to_be_bytes());
        out.extend_from_slice(unit);
    }
    if matches!(first[1], 100 | 110 | 122 | 144) {
        out.push(0xfc | parsed.chroma_format_idc);
        out.push(0xf8 | parsed.bit_depth_luma_minus8);
        out.push(0xf8 | parsed.bit_depth_chroma_minus8);
        out.push(sps_ext.len() as u8);
        for unit in &sps_ext {
            out.extend_from_slice(&(unit.len() as u16).to_be_bytes());
            out.extend_from_slice(unit);
        }
    }
    Ok(out)
}

/// What an `hvcC` needs from an HEVC SPS.
struct HevcSps {
    /// The twelve bytes of the general profile, tier and level.
    general: [u8; 12],
    max_sub_layers_minus1: u8,
    temporal_id_nesting: bool,
    chroma_format_idc: u8,
    bit_depth_luma_minus8: u8,
    bit_depth_chroma_minus8: u8,
    /// The VUI's colour description, when it gives one ([`hevc_vui_colour`]).
    colour: Option<Colour>,
}

/// A colour description as an SPS's VUI gives it and a `colr` box of type
/// `nclx` carries it (ISO/IEC 23091-2 code points, as both use).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Colour {
    primaries: u8,
    transfer: u8,
    matrix: u8,
    full_range: bool,
}

fn parse_hevc_sps(sps: &[u8]) -> Option<HevcSps> {
    let mut bits = Bits::of_nal(sps.get(2..)?);
    bits.u(4)?; // sps_video_parameter_set_id
    let max_sub_layers_minus1 = bits.u(3)? as u8;
    let temporal_id_nesting = bits.u(1)? == 1;
    let mut general = [0u8; 12];
    for byte in &mut general {
        *byte = bits.u(8)? as u8;
    }
    let mut profile_present = [false; 8];
    let mut level_present = [false; 8];
    for i in 0..usize::from(max_sub_layers_minus1) {
        profile_present[i] = bits.u(1)? == 1;
        level_present[i] = bits.u(1)? == 1;
    }
    if max_sub_layers_minus1 > 0 {
        for _ in max_sub_layers_minus1..8 {
            bits.u(2)?;
        }
    }
    for i in 0..usize::from(max_sub_layers_minus1) {
        if profile_present[i] {
            bits.u(88)?;
        }
        if level_present[i] {
            bits.u(8)?;
        }
    }
    bits.ue()?; // sps_seq_parameter_set_id
    let chroma_format_idc = bits.ue()? as u8;
    if chroma_format_idc == 3 {
        bits.u(1)?;
    }
    bits.ue()?; // pic_width_in_luma_samples
    bits.ue()?; // pic_height_in_luma_samples
    if bits.u(1)? == 1 {
        for _ in 0..4 {
            bits.ue()?;
        }
    }
    let bit_depth_luma_minus8 = bits.ue()? as u8;
    let bit_depth_chroma_minus8 = bits.ue()? as u8;
    let colour = hevc_vui_colour(&mut bits, max_sub_layers_minus1);
    Some(HevcSps {
        general,
        max_sub_layers_minus1,
        temporal_id_nesting,
        chroma_format_idc,
        bit_depth_luma_minus8,
        bit_depth_chroma_minus8,
        colour,
    })
}

/// **The colour description in an HEVC SPS's VUI** (H.265 7.3.2.2 and
/// E.2.1), read on from just after the bit depths: everything between is
/// walked only to reach it. `None` when the SPS has no VUI, the VUI no
/// colour description, or the walk runs off the end -- the `hvcC` is still
/// written, without a `colr`.
///
/// It is what tells a receiver an HDR10 film is PQ on BT.2020 (and an HLG
/// one HLG) from the sample entry, the way a Matroska file says it in its
/// `Colour` element; the SPS says it too, but a demuxer reading the
/// container (FFmpeg's, inside a Cast receiver's Chrome) takes the
/// container's word.
fn hevc_vui_colour(bits: &mut Bits, max_sub_layers_minus1: u8) -> Option<Colour> {
    let log2_max_poc_lsb = bits.ue()? + 4;
    let ordering_for_all = bits.u(1)? == 1;
    let first = if ordering_for_all {
        0
    } else {
        max_sub_layers_minus1
    };
    for _ in first..=max_sub_layers_minus1 {
        for _ in 0..3 {
            bits.ue()?; // max_dec_pic_buffering, num_reorder_pics, max_latency_increase
        }
    }
    for _ in 0..6 {
        bits.ue()?; // coding and transform block sizes, hierarchy depths
    }
    if bits.u(1)? == 1 && bits.u(1)? == 1 {
        // scaling_list_enabled_flag, sps_scaling_list_data_present_flag:
        // scaling_list_data() (7.3.4); a signed value is as long as an
        // unsigned one, so `ue` steps over `se` too.
        for size_id in 0..4u32 {
            let step = if size_id == 3 { 3 } else { 1 };
            for _ in (0..6).step_by(step) {
                if bits.u(1)? == 0 {
                    bits.ue()?; // scaling_list_pred_matrix_id_delta
                } else {
                    let coefficients = 64.min(1u32 << (4 + (size_id << 1)));
                    if size_id > 1 {
                        bits.ue()?; // scaling_list_dc_coef_minus8
                    }
                    for _ in 0..coefficients {
                        bits.ue()?; // scaling_list_delta_coef
                    }
                }
            }
        }
    }
    bits.u(2)?; // amp_enabled_flag, sample_adaptive_offset_enabled_flag
    if bits.u(1)? == 1 {
        // pcm_enabled_flag: two bit depths, two sizes, a flag.
        bits.u(8)?;
        bits.ue()?;
        bits.ue()?;
        bits.u(1)?;
    }
    // st_ref_pic_set() (7.3.7), each set's NumDeltaPocs kept for the next,
    // which may be predicted from it.
    let sets = bits.ue()?;
    if sets > 64 {
        return None;
    }
    let mut deltas: Vec<u64> = Vec::with_capacity(sets as usize);
    for index in 0..sets as usize {
        let predicted = index != 0 && bits.u(1)? == 1;
        if predicted {
            bits.u(1)?; // delta_rps_sign
            bits.ue()?; // abs_delta_rps_minus1
            let mut count = 0;
            for _ in 0..=deltas[index - 1] {
                let used = bits.u(1)? == 1;
                if used || bits.u(1)? == 1 {
                    count += 1;
                }
            }
            deltas.push(count);
        } else {
            let negative = bits.ue()?;
            let positive = bits.ue()?;
            if negative + positive > 32 {
                return None;
            }
            for _ in 0..negative + positive {
                bits.ue()?; // delta_poc_minus1
                bits.u(1)?; // used_by_curr_pic_flag
            }
            deltas.push(negative + positive);
        }
    }
    if bits.u(1)? == 1 {
        // long_term_ref_pics_present_flag
        let long_terms = bits.ue()?;
        if long_terms > 32 {
            return None;
        }
        for _ in 0..long_terms {
            bits.u(log2_max_poc_lsb as u32)?;
            bits.u(1)?;
        }
    }
    bits.u(2)?; // sps_temporal_mvp_enabled_flag, strong_intra_smoothing_enabled_flag
    if bits.u(1)? == 0 {
        return None; // no VUI
    }
    if bits.u(1)? == 1 && bits.u(8)? == 255 {
        bits.u(32)?; // aspect_ratio_idc EXTENDED_SAR: sar_width, sar_height
    }
    if bits.u(1)? == 1 {
        bits.u(1)?; // overscan_appropriate_flag
    }
    if bits.u(1)? == 0 {
        return None; // no video_signal_type
    }
    bits.u(3)?; // video_format
    let full_range = bits.u(1)? == 1;
    if bits.u(1)? == 0 {
        return None; // no colour_description
    }
    Some(Colour {
        primaries: bits.u(8)? as u8,
        transfer: bits.u(8)? as u8,
        matrix: bits.u(8)? as u8,
        full_range,
    })
}

/// A `colr` box of type `nclx` (ISO/IEC 14496-12 12.1.5) for `colour`.
fn colr(colour: Colour) -> Vec<u8> {
    let mut body = Vec::with_capacity(11);
    body.extend_from_slice(b"nclx");
    body.extend_from_slice(&u16::from(colour.primaries).to_be_bytes());
    body.extend_from_slice(&u16::from(colour.transfer).to_be_bytes());
    body.extend_from_slice(&u16::from(colour.matrix).to_be_bytes());
    body.push(u8::from(colour.full_range) << 7);
    bx(b"colr", &[&body])
}

/// The `colr` box for an HEVC track whose SPS (in `csd0`, Annex-B)
/// describes its colours, or nothing.
pub(crate) fn hevc_colr(csd0: &[u8]) -> Option<Vec<u8>> {
    let sps = annex_b_units(csd0)
        .into_iter()
        .find(|unit| unit.len() >= 2 && (unit[0] >> 1) & 0x3f == 33)?;
    parse_hevc_sps(sps)?.colour.map(colr)
}

/// `hvcC` from the VPS, SPS and PPS in `csd-0`, in Annex-B, and the SEI
/// messages beside them: a film's mastering display and light levels (an
/// HDR10 encode's) are often there and nowhere in its samples -- x265
/// writes them with its headers, which a Matroska file keeps in its
/// `hvcC` -- so dropping them would leave the receiver's decoder without
/// them.
pub(crate) fn hvcc(csd0: &[u8]) -> Result<Vec<u8>, String> {
    let units = annex_b_units(csd0);
    let of_type = |kind: u8| -> Vec<&[u8]> {
        units
            .iter()
            .copied()
            .filter(|unit| unit.len() >= 2 && (unit[0] >> 1) & 0x3f == kind)
            .collect()
    };
    let (vps, sps, pps) = (of_type(32), of_type(33), of_type(34));
    let first = *sps
        .first()
        .ok_or("the video's codec configuration carries no HEVC sequence parameter set")?;
    if vps.is_empty() || pps.is_empty() {
        return Err("the video's HEVC codec configuration is incomplete".to_string());
    }
    let parsed =
        parse_hevc_sps(first).ok_or("the video's HEVC sequence parameter set cannot be read")?;
    let mut out = vec![1];
    out.extend_from_slice(&parsed.general);
    out.extend_from_slice(&0xf000u16.to_be_bytes()); // min_spatial_segmentation_idc 0
    out.push(0xfc); // parallelismType 0
    out.push(0xfc | parsed.chroma_format_idc);
    out.push(0xf8 | parsed.bit_depth_luma_minus8);
    out.push(0xf8 | parsed.bit_depth_chroma_minus8);
    out.extend_from_slice(&0u16.to_be_bytes()); // avgFrameRate unknown
    out.push(
        ((parsed.max_sub_layers_minus1 + 1) << 3) | (u8::from(parsed.temporal_id_nesting) << 2) | 3,
    );
    // Parameter sets complete (`hvc1`); SEI arrays not claimed complete.
    let (prefix_sei, suffix_sei) = (of_type(39), of_type(40));
    let arrays: Vec<(u8, &Vec<&[u8]>)> = [
        (0x80 | 32u8, &vps),
        (0x80 | 33, &sps),
        (0x80 | 34, &pps),
        (39, &prefix_sei),
        (40, &suffix_sei),
    ]
    .into_iter()
    .filter(|(_, set)| !set.is_empty())
    .collect();
    out.push(arrays.len() as u8);
    for (kind, set) in arrays {
        out.push(kind);
        out.extend_from_slice(&(set.len() as u16).to_be_bytes());
        for unit in set.iter() {
            out.extend_from_slice(&(unit.len() as u16).to_be_bytes());
            out.extend_from_slice(unit);
        }
    }
    Ok(out)
}

/// An MPEG-4 descriptor: tag, a length in the one-byte form when it fits
/// and the four-byte form otherwise, body.
fn descriptor(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    if body.len() < 0x80 {
        out.push(body.len() as u8);
    } else {
        let len = body.len() as u32;
        out.extend_from_slice(&[
            0x80 | ((len >> 21) & 0x7f) as u8,
            0x80 | ((len >> 14) & 0x7f) as u8,
            0x80 | ((len >> 7) & 0x7f) as u8,
            (len & 0x7f) as u8,
        ]);
    }
    out.extend_from_slice(body);
    out
}

/// `esds` for AAC: an ES descriptor whose decoder configuration is MPEG-4
/// audio (object type `0x40`) carrying the AudioSpecificConfig.
fn esds(config: &[u8]) -> Vec<u8> {
    let specific = descriptor(5, config);
    let mut decoder = vec![0x40, 0x15, 0, 0, 0];
    decoder.extend_from_slice(&0u32.to_be_bytes()); // max bitrate
    decoder.extend_from_slice(&0u32.to_be_bytes()); // average bitrate
    decoder.extend_from_slice(&specific);
    let decoder = descriptor(4, &decoder);
    let sl = descriptor(6, &[2]);
    let mut es = vec![0, 0, 0]; // ES_ID 0, no flags
    es.extend_from_slice(&decoder);
    es.extend_from_slice(&sl);
    full(b"esds", 0, 0, &[&descriptor(3, &es)])
}

// --- The init segment ------------------------------------------------------------

fn visual_entry(kind: &[u8; 4], width: u32, height: u32, config: Vec<u8>) -> Vec<u8> {
    let mut head = Vec::with_capacity(78);
    head.extend_from_slice(&[0; 6]); // reserved
    head.extend_from_slice(&1u16.to_be_bytes()); // data_reference_index
    head.extend_from_slice(&[0; 16]); // pre_defined, reserved, pre_defined
    head.extend_from_slice(&(width as u16).to_be_bytes());
    head.extend_from_slice(&(height as u16).to_be_bytes());
    head.extend_from_slice(&0x0048_0000u32.to_be_bytes()); // 72 dpi
    head.extend_from_slice(&0x0048_0000u32.to_be_bytes());
    head.extend_from_slice(&0u32.to_be_bytes()); // reserved
    head.extend_from_slice(&1u16.to_be_bytes()); // frame_count
    head.extend_from_slice(&[0; 32]); // compressorname
    head.extend_from_slice(&0x0018u16.to_be_bytes()); // depth
    head.extend_from_slice(&(-1i16).to_be_bytes()); // pre_defined
    bx(kind, &[&head, &config])
}

fn audio_entry(sample_rate: u32, channels: u32, config: &[u8]) -> Vec<u8> {
    let mut head = Vec::with_capacity(28);
    head.extend_from_slice(&[0; 6]);
    head.extend_from_slice(&1u16.to_be_bytes());
    head.extend_from_slice(&[0; 8]); // reserved
    head.extend_from_slice(&(channels as u16).to_be_bytes());
    head.extend_from_slice(&16u16.to_be_bytes()); // samplesize
    head.extend_from_slice(&[0; 4]); // pre_defined, reserved
    head.extend_from_slice(&((sample_rate.min(0xffff)) << 16).to_be_bytes());
    bx(b"mp4a", &[&head, &esds(config)])
}

/// The film's length on the movie clock (milliseconds) for the 32-bit
/// fields of `mvhd` and `tkhd`: some 49 days fit.
fn movie_duration(duration_ms: u64) -> u32 {
    u32::try_from(duration_ms).unwrap_or(u32::MAX - 1)
}

/// One track's `trak`.
fn trak(
    track_id: u32,
    timescale: u32,
    format: &TrackFormat,
    duration_ms: u64,
) -> Result<Vec<u8>, String> {
    let (entry, handler, width, height, audio) = match format {
        TrackFormat::H264 {
            width,
            height,
            csd0,
            csd1,
        } => (
            visual_entry(b"avc1", *width, *height, bx(b"avcC", &[&avcc(csd0, csd1)?])),
            *b"vide",
            *width,
            *height,
            false,
        ),
        TrackFormat::Hevc {
            width,
            height,
            csd0,
        } => (
            visual_entry(
                b"hvc1",
                *width,
                *height,
                [
                    bx(b"hvcC", &[&hvcc(csd0)?]),
                    hevc_colr(csd0).unwrap_or_default(),
                ]
                .concat(),
            ),
            *b"vide",
            *width,
            *height,
            false,
        ),
        TrackFormat::Aac {
            sample_rate,
            channels,
            csd0,
        } => {
            if csd0.is_empty() {
                return Err("the sound's AAC codec configuration is empty".to_string());
            }
            (
                audio_entry(*sample_rate, *channels, csd0),
                *b"soun",
                0,
                0,
                true,
            )
        }
    };

    let mut tkhd = Vec::with_capacity(80);
    tkhd.extend_from_slice(&0u32.to_be_bytes()); // creation
    tkhd.extend_from_slice(&0u32.to_be_bytes()); // modification
    tkhd.extend_from_slice(&track_id.to_be_bytes());
    tkhd.extend_from_slice(&0u32.to_be_bytes()); // reserved
    tkhd.extend_from_slice(&movie_duration(duration_ms).to_be_bytes()); // duration, ms
    tkhd.extend_from_slice(&[0; 8]); // reserved
    tkhd.extend_from_slice(&0u16.to_be_bytes()); // layer
    tkhd.extend_from_slice(&0u16.to_be_bytes()); // alternate_group
    tkhd.extend_from_slice(&(if audio { 0x0100u16 } else { 0 }).to_be_bytes());
    tkhd.extend_from_slice(&0u16.to_be_bytes()); // reserved
    tkhd.extend_from_slice(&matrix());
    tkhd.extend_from_slice(&(width << 16).to_be_bytes());
    tkhd.extend_from_slice(&(height << 16).to_be_bytes());
    let tkhd = full(b"tkhd", 0, 3, &[&tkhd]);

    let mut mdhd = Vec::with_capacity(20);
    // Version 1: a 64-bit duration, since a film at 90 kHz passes 32 bits
    // after thirteen hours.
    mdhd.extend_from_slice(&0u64.to_be_bytes()); // creation
    mdhd.extend_from_slice(&0u64.to_be_bytes()); // modification
    mdhd.extend_from_slice(&timescale.to_be_bytes());
    mdhd.extend_from_slice(&ticks(duration_ms as i64 * 1000, timescale).to_be_bytes());
    mdhd.extend_from_slice(&0x55c4u16.to_be_bytes()); // 'und'
    mdhd.extend_from_slice(&0u16.to_be_bytes());
    let mdhd = full(b"mdhd", 1, 0, &[&mdhd]);

    let name: &[u8] = if audio {
        b"SoundHandler\0"
    } else {
        b"VideoHandler\0"
    };
    let hdlr = full(b"hdlr", 0, 0, &[&[0; 4], &handler, &[0; 12], name]);

    let media_header = if audio {
        full(b"smhd", 0, 0, &[&[0; 4]])
    } else {
        full(b"vmhd", 0, 1, &[&[0; 8]])
    };
    let dinf = bx(
        b"dinf",
        &[&full(
            b"dref",
            0,
            0,
            &[&1u32.to_be_bytes(), &full(b"url ", 0, 1, &[])],
        )],
    );
    let stsd = full(b"stsd", 0, 0, &[&1u32.to_be_bytes(), &entry]);
    let empty = 0u32.to_be_bytes();
    let stbl = bx(
        b"stbl",
        &[
            &stsd,
            &full(b"stts", 0, 0, &[&empty]),
            &full(b"stsc", 0, 0, &[&empty]),
            &full(b"stsz", 0, 0, &[&empty, &empty]),
            &full(b"stco", 0, 0, &[&empty]),
        ],
    );
    let minf = bx(b"minf", &[&media_header, &dinf, &stbl]);
    let mdia = bx(b"mdia", &[&mdhd, &hdlr, &minf]);
    Ok(bx(b"trak", &[&tkhd, &mdia]))
}

fn trex(track_id: u32) -> Vec<u8> {
    let mut body = Vec::with_capacity(20);
    body.extend_from_slice(&track_id.to_be_bytes());
    body.extend_from_slice(&1u32.to_be_bytes()); // default_sample_description_index
    body.extend_from_slice(&[0; 12]); // duration, size, flags
    full(b"trex", 0, 0, &[&body])
}

/// The init segment: `ftyp` and `moov` for `formats`, the film
/// `duration_ms` long. An error is the sentence a viewer is shown: a
/// codec configuration the muxer cannot describe.
pub(crate) fn init_segment(formats: &Formats, duration_ms: u64) -> Result<Bytes, String> {
    if formats.video.is_none() && formats.audio.is_none() {
        return Err("the conversion produced no track at all".to_string());
    }
    let ftyp = bx(b"ftyp", &[b"iso6", &0u32.to_be_bytes(), b"iso6mp41cmfc"]);

    let tracks = u32::from(formats.video.is_some()) + u32::from(formats.audio.is_some());
    let mut mvhd = Vec::with_capacity(96);
    mvhd.extend_from_slice(&0u32.to_be_bytes());
    mvhd.extend_from_slice(&0u32.to_be_bytes());
    mvhd.extend_from_slice(&1000u32.to_be_bytes()); // timescale: ms
    mvhd.extend_from_slice(&movie_duration(duration_ms).to_be_bytes()); // duration, ms
    mvhd.extend_from_slice(&0x0001_0000u32.to_be_bytes()); // rate
    mvhd.extend_from_slice(&0x0100u16.to_be_bytes()); // volume
    mvhd.extend_from_slice(&[0; 10]);
    mvhd.extend_from_slice(&matrix());
    mvhd.extend_from_slice(&[0; 24]); // pre_defined
    mvhd.extend_from_slice(&(tracks + 1).to_be_bytes()); // next_track_ID
    let mvhd = full(b"mvhd", 0, 0, &[&mvhd]);

    let mehd = full(b"mehd", 1, 0, &[&duration_ms.to_be_bytes()]);
    let mut mvex: Vec<Vec<u8>> = vec![mehd];
    let mut traks = Vec::new();
    if let Some(video) = &formats.video {
        traks.push(trak(VIDEO_TRACK, VIDEO_TIMESCALE, video, duration_ms)?);
        mvex.push(trex(VIDEO_TRACK));
    }
    if let Some(audio) = &formats.audio {
        traks.push(trak(
            formats.audio_track_id(),
            formats.audio_timescale(),
            audio,
            duration_ms,
        )?);
        mvex.push(trex(formats.audio_track_id()));
    }
    let mvex_parts: Vec<&[u8]> = mvex.iter().map(Vec::as_slice).collect();
    let mvex = bx(b"mvex", &mvex_parts);
    let mut moov_parts: Vec<&[u8]> = vec![&mvhd];
    moov_parts.extend(traks.iter().map(Vec::as_slice));
    moov_parts.push(&mvex);
    let moov = bx(b"moov", &moov_parts);

    let mut out = ftyp;
    out.extend_from_slice(&moov);
    Ok(Bytes::from(out))
}

/// The segment index (`sidx`, version 1): one reference per slot of the
/// file, on `track`'s clock (`timescale`), the first starting at
/// `earliest` and `first_offset` bytes after this box. Each reference
/// is `(size, duration)`: a media reference of `size` bytes and
/// `duration` ticks that starts with a stream access point of unknown type
/// -- what FFmpeg's MP4 demuxer (the one in a Cast receiver's Chrome) reads
/// to jump to the slot that holds a time with one `Range`.
pub(crate) fn sidx(
    track: u32,
    timescale: u32,
    earliest: u64,
    first_offset: u64,
    refs: &[(u32, u32)],
) -> Vec<u8> {
    let mut body = Vec::with_capacity(28 + refs.len() * 12);
    body.extend_from_slice(&track.to_be_bytes());
    body.extend_from_slice(&timescale.to_be_bytes());
    body.extend_from_slice(&earliest.to_be_bytes());
    body.extend_from_slice(&first_offset.to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes()); // reserved
    body.extend_from_slice(&(refs.len() as u16).to_be_bytes());
    for &(size, duration) in refs {
        body.extend_from_slice(&(size & 0x7fff_ffff).to_be_bytes());
        body.extend_from_slice(&duration.to_be_bytes());
        body.extend_from_slice(&(1u32 << 31).to_be_bytes()); // starts with a SAP
    }
    full(b"sidx", 1, 0, &[&body])
}

// --- Media segments --------------------------------------------------------------

/// Microseconds on a `timescale` clock, rounded, never below zero.
pub(crate) fn ticks(us: i64, timescale: u32) -> u64 {
    let scaled = (i128::from(us.max(0)) * i128::from(timescale) + 500_000) / 1_000_000;
    scaled as u64
}

/// **How long a stretch of a slot's picture is laid down before the
/// sound that plays beside it**: a slot's samples go into the file in
/// chunks this long, each a `moof` and an `mdat` of its own -- the
/// picture's samples whose decode times fall in the chunk, then the
/// sound's.
///
/// FFmpeg's MP4 demuxer -- the one in a Cast receiver's Chrome -- takes
/// the next sample by its byte position while the tracks' next samples are
/// within a second of each other, and by time when they are further apart
/// (`mov_find_next_sample`, `AV_TIME_BASE`), and seeks its reader to the
/// sample it takes. With a slot's picture whole and then its sound, a
/// reader went to the sound at the fragment's end every time the picture
/// got a second ahead, and back: on zond's TV a 7.5 s, 8 MB slot was a new
/// request every half second, and the cast "worked for a second before
/// failing". Chunked, a reader going straight through finds the next
/// sample of either track in the next bytes.
pub(crate) const CHUNK_US: i64 = 500_000;

/// The most chunks a slot is cut into: a slot whose samples span more than
/// this many [`CHUNK_US`] is cut into chunks a whole multiple of it long,
/// so the slot's `mfhd` numbers stay its own ([`sequence`]).
pub(crate) const MAX_CHUNKS: u64 = 4096;

/// What one chunk beyond a slot's first costs at most: a `moof` header and
/// its `mfhd`, per track a `traf` with its `tfhd`, `tfdt` and `trun`
/// header, and an `mdat` header. A sample's own `trun` entry costs the same
/// whichever chunk it is in. What the layout reserves per chunk
/// ([`super::layout::interleave_room`]).
pub(crate) const CHUNK_OVERHEAD: u64 = 8 + 16 + 2 * (8 + 16 + 20 + 20) + 8;

/// The `mfhd` sequence number of chunk `chunk` of slot `slot`: increasing
/// through the file, whichever run makes which slot.
fn sequence(slot: u64, chunk: u64) -> u32 {
    u32::try_from(slot * MAX_CHUNKS + chunk + 1).unwrap_or(u32::MAX)
}

/// One track's samples in a segment, laid out: per sample its decode time
/// on the track's clock, its `trun` entry (duration, size, flags,
/// composition offset) and its bytes.
struct Laid {
    track_id: u32,
    timescale: u32,
    /// Each sample's decode time: the first sample's, then each the one
    /// before it plus that one's duration -- so a chunk's `tfdt` is exactly
    /// where the chunk before it ended.
    times: Vec<u64>,
    entries: Vec<(u32, u32, u32, i32)>,
    data: Vec<Bytes>,
    video: bool,
}

impl Laid {
    fn new(
        track_id: u32,
        timescale: u32,
        base: u64,
        entries: Vec<(u32, u32, u32, i32)>,
        data: Vec<Bytes>,
        video: bool,
    ) -> Self {
        let times = entries
            .iter()
            .scan(base, |at, entry| {
                let time = *at;
                *at += u64::from(entry.0);
                Some(time)
            })
            .collect();
        Self {
            track_id,
            timescale,
            times,
            entries,
            data,
            video,
        }
    }

    /// The chunk a sample `at` this track's decode time falls in, chunks
    /// `scale` x [`CHUNK_US`] long on the film's clock.
    fn chunk_of(&self, at: u64, scale: u64) -> u64 {
        let chunk = i128::from(self.timescale) * i128::from(CHUNK_US) * i128::from(scale);
        (i128::from(at) * 1_000_000 / chunk) as u64
    }
}

/// Sync or not, as `trun`'s sample flags say it: a sync sample depends on
/// nothing; any other depends on others and is not a sync sample.
fn sample_flags(key: bool) -> u32 {
    if key { 0x0200_0000 } else { 0x0101_0000 }
}

fn lay_video(samples: &[MuxSample], next_pts: Option<i64>, hevc: bool) -> Laid {
    let pts: Vec<u64> = samples
        .iter()
        .map(|sample| ticks(sample.pts_us, VIDEO_TIMESCALE))
        .collect();
    // Decode times: the presentation times sorted, moved on so that the
    // first is the first sample's presentation time -- the slot's sync
    // sample, its `sidx` label. An open GOP's leading pictures (shown
    // before the sync sample they follow) would otherwise put the slot's
    // first decode time before its label, and FFmpeg 5.0 and later, which
    // time a fragment by its `tfdt`, sought the sound to a time in the
    // slot before (`docs/design/renditions.md`, *A slot per sync sample*).
    let mut dts = pts.clone();
    dts.sort_unstable();
    let lead = match (pts.first(), dts.first()) {
        (Some(first), Some(earliest)) => first - earliest,
        _ => 0,
    };
    for at in &mut dts {
        *at += lead;
    }
    let next = next_pts.map(|pts| ticks(pts, VIDEO_TIMESCALE));
    let mut entries = Vec::with_capacity(samples.len());
    let mut data = Vec::with_capacity(samples.len());
    let mut last_duration = u64::from(VIDEO_TIMESCALE / 25);
    for (index, sample) in samples.iter().enumerate() {
        let duration = match dts.get(index + 1) {
            Some(following) => following - dts[index],
            None => match next {
                Some(next) if next > dts[index] => next - dts[index],
                _ => last_duration,
            },
        };
        last_duration = duration;
        let bytes = length_prefixed(&sample.data, hevc);
        let offset = pts[index] as i64 - dts[index] as i64;
        entries.push((
            duration as u32,
            bytes.len() as u32,
            sample_flags(sample.key),
            offset as i32,
        ));
        data.push(bytes);
    }
    Laid::new(
        VIDEO_TRACK,
        VIDEO_TIMESCALE,
        dts.first().copied().unwrap_or(0),
        entries,
        data,
        true,
    )
}

fn lay_audio(
    track_id: u32,
    timescale: u32,
    frame_ticks: u64,
    samples: &[MuxSample],
    next_pts: Option<i64>,
) -> Laid {
    let pts: Vec<u64> = samples
        .iter()
        .map(|sample| ticks(sample.pts_us, timescale))
        .collect();
    let next = next_pts.map(|pts| ticks(pts, timescale));
    let mut entries = Vec::with_capacity(samples.len());
    let mut last_duration = frame_ticks;
    for (index, sample) in samples.iter().enumerate() {
        let duration = match pts.get(index + 1) {
            Some(following) if *following > pts[index] => following - pts[index],
            Some(_) => last_duration,
            None => match next {
                Some(next) if next > pts[index] => next - pts[index],
                _ => last_duration,
            },
        };
        last_duration = duration;
        entries.push((duration as u32, sample.data.len() as u32, 0, 0));
    }
    Laid::new(
        track_id,
        timescale,
        pts.first().copied().unwrap_or(0),
        entries,
        samples.iter().map(|sample| sample.data.clone()).collect(),
        false,
    )
}

/// The `traf` for samples `range` of a laid-out track, with its data offset
/// (from the start of its `moof`) written in.
fn traf(laid: &Laid, range: std::ops::Range<usize>, data_offset: u32) -> Vec<u8> {
    let tfhd = full(b"tfhd", 0, 0x02_0000, &[&laid.track_id.to_be_bytes()]);
    let tfdt = full(b"tfdt", 1, 0, &[&laid.times[range.start].to_be_bytes()]);
    let flags: u32 = if laid.video {
        0x001 | 0x100 | 0x200 | 0x400 | 0x800
    } else {
        0x001 | 0x100 | 0x200
    };
    let entries = &laid.entries[range];
    let mut body = Vec::with_capacity(8 + entries.len() * 16);
    body.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    body.extend_from_slice(&data_offset.to_be_bytes());
    for &(duration, size, flags, offset) in entries {
        body.extend_from_slice(&duration.to_be_bytes());
        body.extend_from_slice(&size.to_be_bytes());
        if laid.video {
            body.extend_from_slice(&flags.to_be_bytes());
            body.extend_from_slice(&offset.to_be_bytes());
        }
    }
    let trun = full(b"trun", 1, flags, &[&body]);
    bx(b"traf", &[&tfhd, &tfdt, &trun])
}

/// **A slot's samples cut into chunks** ([`CHUNK_US`]): per chunk, the range
/// of each laid-out track's samples in it, in the order they go into the
/// file. A pure function of the samples' times, so the same samples make
/// the same chunks whichever run made them and wherever it started.
///
/// The picture's samples go by their decode time (they stay in decode
/// order); the sound's by theirs, held within the picture's chunks -- the
/// sound that begins a segment just before its sync sample (the audio
/// lead) goes with the sync sample, and sound that runs past the last
/// picture goes with it -- so no chunk is sound alone while there is a
/// picture. Chunks are [`CHUNK_US`] long on the film's clock, or a whole
/// multiple of it when the samples would make more than [`MAX_CHUNKS`].
fn chunks(laid: &[Laid]) -> Vec<Vec<std::ops::Range<usize>>> {
    let span = |scale: u64| -> (u64, u64) {
        let mut first = u64::MAX;
        let mut last = 0;
        for track in laid {
            if let (Some(start), Some(end)) = (track.times.first(), track.times.last()) {
                first = first.min(track.chunk_of(*start, scale));
                last = last.max(track.chunk_of(*end, scale));
            }
        }
        (first, last)
    };
    let mut scale = 1;
    loop {
        let (first, last) = span(scale);
        if first == u64::MAX || last - first < MAX_CHUNKS {
            break;
        }
        scale += 1;
    }
    // Each track's chunk per sample; the sound's held within the picture's.
    let picture = laid.iter().find(|track| track.video);
    let bounds = picture.and_then(|picture| {
        Some((
            picture.chunk_of(*picture.times.first()?, scale),
            picture.chunk_of(*picture.times.last()?, scale),
        ))
    });
    let per_sample: Vec<Vec<u64>> = laid
        .iter()
        .map(|track| {
            track
                .times
                .iter()
                .map(|at| {
                    let chunk = track.chunk_of(*at, scale);
                    match bounds {
                        Some((low, high)) if !track.video => chunk.clamp(low, high),
                        _ => chunk,
                    }
                })
                .collect()
        })
        .collect();
    let mut all: Vec<u64> = per_sample.iter().flatten().copied().collect();
    all.sort_unstable();
    all.dedup();
    all.into_iter()
        .map(|chunk| {
            per_sample
                .iter()
                .map(|track| {
                    let start = track.partition_point(|at| *at < chunk);
                    let end = track.partition_point(|at| *at <= chunk);
                    start..end
                })
                .collect()
        })
        .collect()
}

/// Slot `slot`'s fragment: its samples -- the video in decode order -- as
/// a `moof` and an `mdat` per chunk ([`chunks`]), each `mdat` the chunk's
/// picture and then its sound, after a `styp` unless the slot opens
/// `at_label` -- its first samples at the time its slot's `sidx` reference
/// says. `next_*` is the presentation time of the track's first sample
/// after this slot, when it is known, which is the last sample's duration.
///
/// **A `moof` per chunk, not a `trun` per chunk in one `moof`.** FFmpeg
/// records, per fragment and track, where its samples begin in the index
/// as each `trun` is read, so with several `trun`s it records the last
/// one's; a fragment read later from earlier in the file -- a seek back to
/// a slot not read before -- is put in front of that last `trun`, in the
/// middle of the slot read before it, and the index is out of order
/// (`mov_read_trun`, 4.4 to master). A `moof` per chunk is a fragment per
/// chunk, each recorded where it begins. The slot's first `moof` is at the
/// slot's start, where the `sidx` points, as before.
///
/// **The `styp` is there to keep the `sidx` and the first `moof` apart**,
/// and only then. FFmpeg keeps one fragment-index entry per offset, the
/// `sidx`'s reference and each `moof` it reads:
///
/// * apart (a `styp` first), they are two entries for one fragment, and
///   after a seek to it every version (4.4 to master) parsed its `moof`
///   twice -- seeking the second stream picks the `moof`'s own entry,
///   unread -- doubling its samples in the index. A later seek back to a
///   fragment read neither at the start nor since then lost the index's
///   order and landed at the end of what was read at the start: zond's
///   TV, back to 0:43 after seeks to 1:55, 7:05 and 8:41, read fragment
///   after fragment, buffering.
/// * together, FFmpeg up to 4.4 (the TV's Chrome 92) takes the fragment's
///   decode time from the `sidx` label, not the `tfdt` (5.0 added
///   `use_tfdt`, on by default). Right when the label is the segment's
///   start -- a mirrored layout's cut -- and wrong otherwise: an estimated
///   layout labels its slots a GOP late (and its sound early), and a slot
///   that opens with what the slot before spilled opens before its cut.
///   The later chunks' `moof`s have no label and are read by their `tfdt`.
pub(crate) fn media_segment(
    formats: &Formats,
    slot: u64,
    at_label: bool,
    video: &[MuxSample],
    video_next: Option<i64>,
    audio: &[MuxSample],
    audio_next: Option<i64>,
) -> Bytes {
    let hevc = matches!(formats.video, Some(TrackFormat::Hevc { .. }));
    let mut laid = Vec::with_capacity(2);
    if formats.video.is_some() && !video.is_empty() {
        laid.push(lay_video(video, video_next, hevc));
    }
    if formats.audio.is_some() && !audio.is_empty() {
        laid.push(lay_audio(
            formats.audio_track_id(),
            formats.audio_timescale(),
            formats.audio_frame_ticks(),
            audio,
            audio_next,
        ));
    }
    let mut out = if at_label {
        Vec::new()
    } else {
        bx(b"styp", &[b"msdh", &0u32.to_be_bytes(), b"msdhmsix"])
    };
    let mut chunks = chunks(&laid);
    if chunks.is_empty() {
        // Nothing at all: a `moof` with only its `mfhd`, and an empty `mdat`.
        chunks.push(Vec::new());
    }
    for (index, ranges) in chunks.iter().enumerate() {
        let mfhd = full(
            b"mfhd",
            0,
            0,
            &[&sequence(slot, index as u64).to_be_bytes()],
        );
        let tracks: Vec<(&Laid, std::ops::Range<usize>)> = laid
            .iter()
            .zip(ranges.iter().cloned())
            .filter(|(_, range)| !range.is_empty())
            .collect();
        // The moof's size does not depend on the offsets written into it,
        // so lay it out once with zeros to learn it, then with the offsets.
        let sized: usize = 8
            + mfhd.len()
            + tracks
                .iter()
                .map(|(track, range)| traf(track, range.clone(), 0).len())
                .sum::<usize>();
        let mut offset = (sized + 8) as u32;
        let mut trafs = Vec::with_capacity(tracks.len());
        for (track, range) in &tracks {
            trafs.push(traf(track, range.clone(), offset));
            offset += track.data[range.clone()]
                .iter()
                .map(|data| data.len() as u32)
                .sum::<u32>();
        }
        let mut moof_parts: Vec<&[u8]> = vec![&mfhd];
        moof_parts.extend(trafs.iter().map(Vec::as_slice));
        let moof = bx(b"moof", &moof_parts);
        debug_assert_eq!(moof.len(), sized);
        let payload: Vec<&[u8]> = tracks
            .iter()
            .flat_map(|(track, range)| track.data[range.clone()].iter().map(|data| data.as_ref()))
            .collect();
        out.extend_from_slice(&moof);
        out.extend_from_slice(&bx(b"mdat", &payload));
    }
    Bytes::from(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// x264's SPS and PPS for 320x240 High, as `csd-0`/`csd-1` carry them.
    const X264_SPS: &[u8] = &[
        0, 0, 0, 1, 0x67, 0x64, 0x00, 0x0d, 0xac, 0xd9, 0x41, 0x41, 0xfb, 0x01, 0x10, 0x00, 0x00,
        0x03, 0x00, 0x10, 0x00, 0x00, 0x03, 0x03, 0x20, 0xf1, 0x42, 0x99, 0x60,
    ];
    const X264_PPS: &[u8] = &[0, 0, 0, 1, 0x68, 0xeb, 0xe3, 0xcb, 0x22, 0xc0];

    /// x265's VPS, SPS and PPS for 320x240 Main, as `csd-0` carries them.
    const X265_CSD: &[u8] = &[
        0, 0, 0, 1, 0x40, 0x01, 0x0c, 0x01, 0xff, 0xff, 0x01, 0x60, 0x00, 0x00, 0x03, 0x00, 0x90,
        0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x3c, 0x95, 0x98, 0x09, 0, 0, 0, 1, 0x42, 0x01,
        0x01, 0x01, 0x60, 0x00, 0x00, 0x03, 0x00, 0x90, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00,
        0x3c, 0xa0, 0x0a, 0x08, 0x0f, 0x16, 0x59, 0x59, 0xa4, 0x93, 0x2b, 0xc0, 0x5a, 0x02, 0x00,
        0x00, 0x03, 0x00, 0x02, 0x00, 0x00, 0x03, 0x00, 0x32, 0x10, 0, 0, 0, 1, 0x44, 0x01, 0xc1,
        0x72, 0xb4, 0x62, 0x40,
    ];

    #[test]
    fn annex_b_splits_on_both_start_code_lengths() {
        let data = [
            0, 0, 0, 1, 0x67, 1, 2, 0, 0, 1, 0x68, 3, 0, 0, 0, 1, 0x65, 4,
        ];
        let units = annex_b_units(&data);
        assert_eq!(
            units,
            vec![&[0x67u8, 1, 2][..], &[0x68, 3][..], &[0x65, 4][..]]
        );
        let sample = length_prefixed(
            &Bytes::from_static(&[0, 0, 0, 1, 0x09, 0xf0, 0, 0, 1, 0x65, 9, 9]),
            false,
        );
        assert_eq!(
            sample.as_ref(),
            &[0, 0, 0, 3, 0x65, 9, 9],
            "the delimiter is dropped and the slice length-prefixed"
        );
        let already = Bytes::from_static(&[0, 0, 0, 2, 0x65, 1]);
        assert_eq!(length_prefixed(&already, false), already);
    }

    #[test]
    fn avcc_carries_the_parameter_sets_and_the_high_profile_tail() {
        let avcc = avcc(X264_SPS, X264_PPS).expect("an avcC");
        assert_eq!(&avcc[..5], &[1, 0x64, 0x00, 0x0d, 0xff]);
        assert_eq!(avcc[5], 0xe1);
        let sps_len = u16::from_be_bytes([avcc[6], avcc[7]]) as usize;
        assert_eq!(&avcc[8..8 + sps_len], &X264_SPS[4..]);
        let at = 8 + sps_len;
        assert_eq!(avcc[at], 1);
        let pps_len = u16::from_be_bytes([avcc[at + 1], avcc[at + 2]]) as usize;
        assert_eq!(&avcc[at + 3..at + 3 + pps_len], &X264_PPS[4..]);
        // 4:2:0, 8-bit, no extensions.
        assert_eq!(&avcc[at + 3 + pps_len..], &[0xfd, 0xf8, 0xf8, 0]);
    }

    #[test]
    fn hvcc_reads_the_profile_and_the_bit_depths_from_the_sps() {
        let hvcc = hvcc(X265_CSD).expect("an hvcC");
        assert_eq!(hvcc[0], 1);
        assert_eq!(hvcc[1], 0x01, "Main, main tier, profile space 0");
        assert_eq!(hvcc[12], 0x3c, "level 2");
        assert_eq!(hvcc[16], 0xfd, "4:2:0");
        assert_eq!(hvcc[17], 0xf8, "8-bit luma");
        assert_eq!(hvcc[18], 0xf8, "8-bit chroma");
        assert_eq!(hvcc[22], 3, "three arrays");
        assert_eq!(hvcc[23], 0x80 | 32, "the VPS first");
    }

    /// x265's headers for a 320x240 Main 10 HDR10 encode (BT.2020, PQ,
    /// a mastering display and light levels), as `csd-0` carries them: the
    /// VPS, the SPS -- whose VUI says BT.2020 primaries (9), PQ (16), the
    /// BT.2020 non-constant matrix (9), limited range -- the PPS, and the
    /// two SEI messages x265 writes with its headers (light levels, then
    /// the mastering display).
    const X265_HDR10_CSD: &[u8] = &[
        0, 0, 0, 1, 0x40, 0x01, 0x0c, 0x01, 0xff, 0xff, 0x02, 0x20, 0x00, 0x00, 0x03, 0x00, 0x90,
        0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00, 0x3c, 0x95, 0x98, 0x09, 0, 0, 0, 1, 0x42, 0x01,
        0x01, 0x02, 0x20, 0x00, 0x00, 0x03, 0x00, 0x90, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00,
        0x3c, 0xa0, 0x0a, 0x08, 0x0f, 0x13, 0x65, 0x95, 0x9a, 0x49, 0x32, 0xbc, 0x05, 0xa8, 0x48,
        0x80, 0x48, 0x20, 0x00, 0x00, 0x03, 0x00, 0x20, 0x00, 0x00, 0x03, 0x03, 0x21, 0, 0, 0, 1,
        0x44, 0x01, 0xc1, 0x72, 0xbc, 0x62, 0x40, 0, 0, 0, 1, 0x4e, 0x01, 0x90, 0x04, 0x03, 0xe8,
        0x01, 0x90, 0x80, 0, 0, 0, 1, 0x4e, 0x01, 0x89, 0x18, 0x33, 0xc2, 0x86, 0xc4, 0x1d, 0x4c,
        0x0b, 0xb8, 0x84, 0xd0, 0x3e, 0x80, 0x3d, 0x13, 0x40, 0x42, 0x00, 0x98, 0x96, 0x80, 0x00,
        0x00, 0x03, 0x00, 0x01, 0x80,
    ];

    /// x265's SPS for 320x240 Main, BT.709 in full range, eight B-frames
    /// in a pyramid and six references: more reference picture sets to walk.
    const X265_709_FULL_SPS: &[u8] = &[
        0, 0, 0, 1, 0x42, 0x01, 0x01, 0x01, 0x60, 0x00, 0x00, 0x03, 0x00, 0x90, 0x00, 0x00, 0x03,
        0x00, 0x00, 0x03, 0x00, 0x3c, 0xa0, 0x0a, 0x08, 0x0f, 0x16, 0x59, 0xd8, 0xa9, 0x24, 0xca,
        0xf0, 0x16, 0xe0, 0x20, 0x20, 0x20, 0x80, 0x00, 0x00, 0x03, 0x00, 0x80, 0x00, 0x00, 0x0c,
        0x84,
    ];

    /// **An HDR10 film's `hvc1` says what it is**: Main 10 and ten bits in
    /// the `hvcC`, the two SEI messages kept beside the parameter sets, and
    /// a `colr` with the VUI's colour description -- what a receiver's
    /// demuxer reads to show PQ as PQ.
    #[test]
    fn an_hdr10_hvcc_keeps_its_sei_and_its_colours_go_in_a_colr() {
        let hvcc = hvcc(X265_HDR10_CSD).expect("an hvcC");
        assert_eq!(hvcc[1], 0x02, "Main 10");
        assert_eq!(hvcc[17], 0xfa, "10-bit luma");
        assert_eq!(hvcc[18], 0xfa, "10-bit chroma");
        assert_eq!(hvcc[22], 4, "VPS, SPS, PPS and the SEI");
        let arrays: Vec<(u8, u16)> = {
            let mut at = 23;
            let mut out = Vec::new();
            while at < hvcc.len() {
                let kind = hvcc[at];
                let count = u16::from_be_bytes([hvcc[at + 1], hvcc[at + 2]]);
                at += 3;
                for _ in 0..count {
                    at += 2 + usize::from(u16::from_be_bytes([hvcc[at], hvcc[at + 1]]));
                }
                out.push((kind, count));
            }
            out
        };
        assert_eq!(arrays, vec![(0xa0, 1), (0xa1, 1), (0xa2, 1), (39, 2)]);

        assert_eq!(
            hevc_colr(X265_HDR10_CSD).expect("a colour description"),
            [
                &[0, 0, 0, 19][..],
                b"colr",
                b"nclx",
                &[0, 9, 0, 16, 0, 9, 0]
            ]
            .concat()
        );
        assert_eq!(
            hevc_colr(X265_709_FULL_SPS).expect("a colour description"),
            [
                &[0, 0, 0, 19][..],
                b"colr",
                b"nclx",
                &[0, 1, 0, 1, 0, 1, 0x80]
            ]
            .concat()
        );
        // x265 by default says nothing about colour: no `colr` at all.
        assert_eq!(hevc_colr(X265_CSD), None);

        // And the sample entry carries both, the `colr` after the `hvcC`.
        let format = TrackFormat::Hevc {
            width: 320,
            height: 240,
            csd0: Bytes::from_static(X265_HDR10_CSD),
        };
        let trak = trak(1, VIDEO_TIMESCALE, &format, 1000).expect("a trak");
        let hvc1 = trak
            .windows(4)
            .position(|window| window == b"hvc1")
            .expect("an hvc1 entry");
        let hvcc_at = trak
            .windows(4)
            .position(|window| window == b"hvcC")
            .unwrap();
        let colr_at = trak
            .windows(4)
            .position(|window| window == b"colr")
            .unwrap();
        assert!(hvc1 < hvcc_at && hvcc_at < colr_at);
    }

    fn sample(pts_us: i64, key: bool) -> MuxSample {
        MuxSample {
            pts_us,
            key,
            data: Bytes::from_static(&[0, 0, 0, 1, 0x41, 0xaa]),
        }
    }

    /// Decode order I P B B: the decode times are the presentation times
    /// sorted, and each offset is the difference, negative where a frame is
    /// shown before the one decoded in its slot.
    #[test]
    fn reordered_video_gets_signed_composition_offsets() {
        let laid = lay_video(
            &[
                sample(0, true),
                sample(120_000, false),
                sample(40_000, false),
                sample(80_000, false),
            ],
            Some(160_000),
            false,
        );
        assert_eq!(laid.times[0], 0);
        let offsets: Vec<i32> = laid.entries.iter().map(|entry| entry.3).collect();
        assert_eq!(offsets, vec![0, 7200, -3600, -3600]);
        let durations: Vec<u32> = laid.entries.iter().map(|entry| entry.0).collect();
        assert_eq!(durations, vec![3600, 3600, 3600, 3600]);
        assert_eq!(laid.entries[0].2, 0x0200_0000);
        assert_eq!(laid.entries[1].2, 0x0101_0000);
    }

    /// Beside a picture the sound counts on the picture's clock: one AAC
    /// frame with nothing after it lasts 1024 samples there (1920 ticks of
    /// 90 kHz at 48 kHz); alone it keeps its sample rate.
    #[test]
    fn the_sound_counts_on_the_pictures_clock() {
        let aac = |sample_rate| TrackFormat::Aac {
            sample_rate,
            channels: 2,
            csd0: Bytes::from_static(&[0x11, 0x90]),
        };
        let picture = Some(TrackFormat::H264 {
            width: 320,
            height: 240,
            csd0: Bytes::from_static(X264_SPS),
            csd1: Bytes::from_static(X264_PPS),
        });
        let both = Formats {
            video: picture,
            audio: Some(aac(48_000)),
        };
        assert_eq!(both.audio_timescale(), VIDEO_TIMESCALE);
        assert_eq!(both.audio_frame_ticks(), 1920);
        let laid = lay_audio(
            2,
            both.audio_timescale(),
            both.audio_frame_ticks(),
            &[MuxSample {
                pts_us: 1_000_000,
                key: true,
                data: Bytes::from_static(&[1]),
            }],
            None,
        );
        assert_eq!(laid.times[0], 90_000);
        assert_eq!(laid.entries[0].0, 1920);
        let alone = Formats {
            video: None,
            audio: Some(aac(44_100)),
        };
        assert_eq!(alone.audio_timescale(), 44_100);
        assert_eq!(alone.audio_frame_ticks(), 1024);
    }

    /// The `trun`'s data offsets point at each track's bytes in the `mdat`.
    #[test]
    fn data_offsets_point_into_the_mdat() {
        let formats = Formats {
            video: Some(TrackFormat::H264 {
                width: 320,
                height: 240,
                csd0: Bytes::from_static(X264_SPS),
                csd1: Bytes::from_static(X264_PPS),
            }),
            audio: Some(TrackFormat::Aac {
                sample_rate: 48_000,
                channels: 2,
                csd0: Bytes::from_static(&[0x11, 0x90]),
            }),
        };
        let audio = [MuxSample {
            pts_us: 0,
            key: true,
            data: Bytes::from_static(&[0xde, 0xad]),
        }];
        let segment = media_segment(&formats, 0, true, &[sample(0, true)], None, &audio, None);
        assert_eq!(&segment[4..8], b"moof", "the segment begins with its moof");
        let moof = &segment[..];
        // The audio traf's trun is the last box in the moof: its data
        // offset is the 4 bytes after its sample count.
        let find = |needle: &[u8]| {
            moof.windows(4)
                .enumerate()
                .filter(|(_, window)| *window == needle)
                .map(|(at, _)| at)
                .collect::<Vec<_>>()
        };
        let truns = find(b"trun");
        assert_eq!(truns.len(), 2);
        let offset_at = |trun: usize| {
            u32::from_be_bytes(moof[trun + 12..trun + 16].try_into().unwrap()) as usize
        };
        assert_eq!(
            &moof[offset_at(truns[0])..offset_at(truns[0]) + 6],
            &[0, 0, 0, 2, 0x41, 0xaa]
        );
        assert_eq!(
            &moof[offset_at(truns[1])..offset_at(truns[1]) + 2],
            &[0xde, 0xad]
        );
    }
    /// Every sample of a fragment as a demuxer indexes it: `(track, decode
    /// time in microseconds, byte position, size)`, read from each `moof`'s
    /// `traf`s -- the `tfdt`, the `trun`'s durations and sizes, its data
    /// offset from its own `moof`.
    fn indexed(fragment: &[u8]) -> Vec<(u32, i64, usize, usize)> {
        let u32_at =
            |data: &[u8], at: usize| u32::from_be_bytes(data[at..at + 4].try_into().unwrap());
        let boxes = |data: &[u8], from: usize, to: usize| {
            let mut out = Vec::new();
            let mut at = from;
            while at < to {
                let size = u32_at(data, at) as usize;
                out.push((data[at + 4..at + 8].to_vec(), at, size));
                at += size;
            }
            out
        };
        let mut out = Vec::new();
        for (kind, moof, size) in boxes(fragment, 0, fragment.len()) {
            if kind != b"moof" {
                continue;
            }
            for (kind, traf, traf_size) in boxes(fragment, moof + 8, moof + size) {
                if kind != b"traf" {
                    continue;
                }
                let (mut track, mut time) = (0, 0u64);
                for (kind, at, _) in boxes(fragment, traf + 8, traf + traf_size) {
                    match kind.as_slice() {
                        b"tfhd" => track = u32_at(fragment, at + 12),
                        b"tfdt" => {
                            time =
                                u64::from_be_bytes(fragment[at + 12..at + 20].try_into().unwrap())
                        }
                        b"trun" => {
                            let flags = u32_at(fragment, at + 8) & 0xff_ffff;
                            let count = u32_at(fragment, at + 12) as usize;
                            let mut pos = moof + u32_at(fragment, at + 16) as usize;
                            let step = if flags & 0x800 != 0 { 16 } else { 8 };
                            for sample in 0..count {
                                let entry = at + 20 + sample * step;
                                let duration = u64::from(u32_at(fragment, entry));
                                let bytes = u32_at(fragment, entry + 4) as usize;
                                out.push((track, (time * 1_000_000 / 90_000) as i64, pos, bytes));
                                time += duration;
                                pos += bytes;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        out
    }

    /// **A reader taking a slot's samples as FFmpeg's MP4 demuxer does
    /// reads it front to back** (`mov_find_next_sample`: the next sample by
    /// byte position while the tracks are within a second of each other,
    /// by time when they are not, and a seek of the reader to it). An 8 s
    /// slot of an 8 Mbit/s film, picture and sound: laid picture-first,
    /// such a reader went to the sound at the slot's end every time the
    /// picture got a second ahead, and back -- a request each on zond's TV,
    /// every half second. Chunked, every sample it takes begins where the
    /// one before ended, or a `moof` and `mdat` header later.
    #[test]
    fn ffmpegs_reader_takes_a_slot_front_to_back() {
        let formats = Formats {
            video: Some(TrackFormat::H264 {
                width: 320,
                height: 240,
                csd0: Bytes::from_static(X264_SPS),
                csd1: Bytes::from_static(X264_PPS),
            }),
            audio: Some(TrackFormat::Aac {
                sample_rate: 48_000,
                channels: 2,
                csd0: Bytes::from_static(&[0x11, 0x90]),
            }),
        };
        // 24 frames a second, a sync sample first, 40 kB a frame; an AAC
        // frame every 1024 samples, from the audio lead before the cut.
        let video: Vec<MuxSample> = (0..192i64)
            .map(|frame| MuxSample {
                pts_us: 10_000_000 + frame * 1_000_000 / 24,
                key: frame == 0,
                data: Bytes::from(vec![0x41; 40_000]),
            })
            .collect();
        let audio: Vec<MuxSample> = (0..378i64)
            .map(|frame| MuxSample {
                pts_us: 10_000_000 - 60_000 + frame * 64_000 / 3,
                key: true,
                data: Bytes::from(vec![0x21; 400]),
            })
            .collect();
        let fragment = media_segment(
            &formats,
            7,
            true,
            &video,
            Some(18_000_000),
            &audio,
            Some(10_000_000 - 60_000 + 378 * 64_000 / 3),
        );
        let samples = indexed(&fragment);
        assert_eq!(samples.len(), video.len() + audio.len());
        let tracks: Vec<Vec<(u32, i64, usize, usize)>> = [1, 2]
            .iter()
            .map(|track| samples.iter().filter(|s| s.0 == *track).copied().collect())
            .collect();
        let mut next = [0usize, 0usize];
        let mut end = None;
        let mut jumps = Vec::new();
        loop {
            // mov_find_next_sample, over the two tracks' next samples.
            let mut best: Option<usize> = None;
            for track in 0..2 {
                let Some(candidate) = tracks[track].get(next[track]) else {
                    continue;
                };
                best = Some(match best {
                    None => track,
                    Some(other) => {
                        let current = tracks[other][next[other]];
                        let better = if (candidate.1 - current.1).abs() <= 1_000_000 {
                            candidate.2 < current.2
                        } else {
                            candidate.1 < current.1
                        };
                        if better { track } else { other }
                    }
                });
            }
            let Some(track) = best else { break };
            let (_, _, pos, size) = tracks[track][next[track]];
            if let Some(end) = end
                && !(end..=end + 1024).contains(&pos)
            {
                jumps.push((end, pos));
            }
            end = Some(pos + size);
            next[track] += 1;
        }
        assert!(
            jumps.is_empty(),
            "the reader jumped {} times: {:?}",
            jumps.len(),
            &jumps[..jumps.len().min(6)]
        );
    }
    /// **A slot longer than 4096 half seconds is cut into longer chunks**,
    /// so its `mfhd` numbers stay its own: a picture every 0.4 s for 40
    /// minutes would be 4800 chunks, and slot 7's numbers would run into
    /// slot 8's.
    #[test]
    fn a_long_slot_keeps_its_mfhd_numbers() {
        let formats = Formats {
            video: Some(TrackFormat::H264 {
                width: 320,
                height: 240,
                csd0: Bytes::from_static(X264_SPS),
                csd1: Bytes::from_static(X264_PPS),
            }),
            audio: None,
        };
        let video: Vec<MuxSample> = (0..6000i64)
            .map(|frame| sample(frame * 400_000, frame == 0))
            .collect();
        let fragment = media_segment(&formats, 7, true, &video, None, &[], None);
        let mut numbers = Vec::new();
        let mut at = 0;
        while at < fragment.len() {
            let size = u32::from_be_bytes(fragment[at..at + 4].try_into().unwrap()) as usize;
            if &fragment[at + 4..at + 8] == b"moof" {
                numbers.push(u32::from_be_bytes(
                    fragment[at + 20..at + 24].try_into().unwrap(),
                ));
            }
            at += size;
        }
        assert_eq!(numbers.first(), Some(&(7 * 4096 + 1)));
        assert!(
            numbers.len() <= 4096 && numbers.len() >= 2048,
            "{} chunks",
            numbers.len()
        );
        assert!(numbers.windows(2).all(|pair| pair[1] == pair[0] + 1));
        assert!(*numbers.last().unwrap() <= 8 * 4096);
    }

    /// **An open GOP's slot begins, in decode time, at its sync sample**:
    /// decode order CRA (shown at 10 s), two leading pictures shown before
    /// it, then the rest. The decode times are the presentation times
    /// sorted and moved on to start at the CRA's: the first `moof`'s `tfdt`
    /// is the slot's `sidx` label. Before, it was the first leading
    /// picture's time, 80 ms before the label, and FFmpeg 5.0 and later
    /// (which time a fragment by its `tfdt`) sought the sound to that time
    /// -- in the slot before -- and 6.1 landed the picture there too.
    #[test]
    fn an_open_gop_slot_begins_at_its_sync_sample() {
        let formats = Formats {
            video: Some(TrackFormat::H264 {
                width: 320,
                height: 240,
                csd0: Bytes::from_static(X264_SPS),
                csd1: Bytes::from_static(X264_PPS),
            }),
            audio: None,
        };
        let frame = 40_000;
        let order = [0, -2, -1, 3, 1, 2, 6, 4, 5];
        let video: Vec<MuxSample> = order
            .iter()
            .map(|at| sample(10_000_000 + at * frame, *at == 0))
            .collect();
        let fragment = media_segment(&formats, 3, true, &video, Some(10_280_000), &[], None);
        let samples = indexed(&fragment);
        assert_eq!(samples[0].1, 10_000_000, "the CRA's decode time is its own");
        let dts: Vec<i64> = samples.iter().map(|sample| sample.1).collect();
        assert!(dts.windows(2).all(|pair| pair[0] < pair[1]), "{dts:?}");
        assert_eq!(dts.last(), Some(&(10_000_000 + 8 * frame)));
    }
}
