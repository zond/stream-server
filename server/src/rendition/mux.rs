//! **The fMP4 muxer** (`docs/design/renditions.md` §2.2): an init segment
//! from the tracks' formats, and a media segment from one segment's
//! samples. Hand-written, because the boxes are a short list and a crate
//! for them would be a dependency for forty lines of byte layout.
//!
//! The init segment is `ftyp` + `moov` (`mvhd`, `mvex` with `mehd` and one
//! `trex` per track, and one `trak` per track: `tkhd`, `mdia` with `mdhd`,
//! `hdlr` and `minf` -- `vmhd`/`smhd`, `dinf`/`dref`, and an `stbl` whose
//! only entry is the sample description: `avc1`+`avcC`, `hvc1`+`hvcC` or
//! `mp4a`+`esds`, every sample table empty). A media segment is `styp` +
//! `moof` (`mfhd`, and per track with samples one `traf`: `tfhd` with
//! default-base-is-moof, `tfdt` version 1, `trun` version 1) + `mdat`,
//! video's bytes first.
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
const VIDEO_TRACK: u32 = 1;

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

    fn audio_timescale(&self) -> u32 {
        match &self.audio {
            Some(TrackFormat::Aac { sample_rate, .. }) => (*sample_rate).max(1),
            _ => 48_000,
        }
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
    Some(HevcSps {
        general,
        max_sub_layers_minus1,
        temporal_id_nesting,
        chroma_format_idc,
        bit_depth_luma_minus8,
        bit_depth_chroma_minus8,
    })
}

/// `hvcC` from the VPS, SPS and PPS in `csd-0`, in Annex-B.
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
    out.push(3);
    for (kind, set) in [(32u8, &vps), (33, &sps), (34, &pps)] {
        out.push(0x80 | kind);
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

/// One track's `trak`.
fn trak(track_id: u32, timescale: u32, format: &TrackFormat) -> Result<Vec<u8>, String> {
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
            visual_entry(b"hvc1", *width, *height, bx(b"hvcC", &[&hvcc(csd0)?])),
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
    tkhd.extend_from_slice(&0u32.to_be_bytes()); // duration
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
    mdhd.extend_from_slice(&0u32.to_be_bytes());
    mdhd.extend_from_slice(&0u32.to_be_bytes());
    mdhd.extend_from_slice(&timescale.to_be_bytes());
    mdhd.extend_from_slice(&0u32.to_be_bytes()); // duration
    mdhd.extend_from_slice(&0x55c4u16.to_be_bytes()); // 'und'
    mdhd.extend_from_slice(&0u16.to_be_bytes());
    let mdhd = full(b"mdhd", 0, 0, &[&mdhd]);

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
    mvhd.extend_from_slice(&0u32.to_be_bytes()); // duration: in mehd
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
        traks.push(trak(VIDEO_TRACK, VIDEO_TIMESCALE, video)?);
        mvex.push(trex(VIDEO_TRACK));
    }
    if let Some(audio) = &formats.audio {
        traks.push(trak(
            formats.audio_track_id(),
            formats.audio_timescale(),
            audio,
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

// --- Media segments --------------------------------------------------------------

/// Microseconds on a `timescale` clock, rounded, never below zero.
pub(crate) fn ticks(us: i64, timescale: u32) -> u64 {
    let scaled = (i128::from(us.max(0)) * i128::from(timescale) + 500_000) / 1_000_000;
    scaled as u64
}

/// One track's run in a segment, laid out: base decode time, and per
/// sample duration, size, flags and composition offset.
struct Laid {
    track_id: u32,
    base: u64,
    entries: Vec<(u32, u32, u32, i32)>,
    data: Vec<Bytes>,
    video: bool,
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
    let mut dts = pts.clone();
    dts.sort_unstable();
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
    Laid {
        track_id: VIDEO_TRACK,
        base: dts.first().copied().unwrap_or(0),
        entries,
        data,
        video: true,
    }
}

fn lay_audio(track_id: u32, timescale: u32, samples: &[MuxSample], next_pts: Option<i64>) -> Laid {
    let pts: Vec<u64> = samples
        .iter()
        .map(|sample| ticks(sample.pts_us, timescale))
        .collect();
    let next = next_pts.map(|pts| ticks(pts, timescale));
    let mut entries = Vec::with_capacity(samples.len());
    let mut last_duration = 1024u64;
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
    Laid {
        track_id,
        base: pts.first().copied().unwrap_or(0),
        entries,
        data: samples.iter().map(|sample| sample.data.clone()).collect(),
        video: false,
    }
}

/// The `traf` for one laid-out track, with its data offset (from the start
/// of the `moof`) written in.
fn traf(laid: &Laid, data_offset: u32) -> Vec<u8> {
    let tfhd = full(b"tfhd", 0, 0x02_0000, &[&laid.track_id.to_be_bytes()]);
    let tfdt = full(b"tfdt", 1, 0, &[&laid.base.to_be_bytes()]);
    let flags: u32 = if laid.video {
        0x001 | 0x100 | 0x200 | 0x400 | 0x800
    } else {
        0x001 | 0x100 | 0x200
    };
    let mut body = Vec::with_capacity(8 + laid.entries.len() * 16);
    body.extend_from_slice(&(laid.entries.len() as u32).to_be_bytes());
    body.extend_from_slice(&data_offset.to_be_bytes());
    for &(duration, size, flags, offset) in &laid.entries {
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

/// Segment `sequence`'s bytes: `styp`, `moof` and `mdat` for the video
/// samples (in decode order) and the audio samples given. `next_*` is the
/// presentation time of the track's first sample after this segment, when
/// it is known, which is the last sample's duration.
pub(crate) fn media_segment(
    formats: &Formats,
    sequence: u32,
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
            audio,
            audio_next,
        ));
    }
    let styp = bx(b"styp", &[b"msdh", &0u32.to_be_bytes(), b"msdhmsix"]);
    let mfhd = full(b"mfhd", 0, 0, &[&sequence.to_be_bytes()]);

    // The moof's size does not depend on the offsets written into it, so
    // lay it out once with zeros to learn it, then again with the offsets.
    let sized: usize = 8 + mfhd.len() + laid.iter().map(|l| traf(l, 0).len()).sum::<usize>();
    let mut offset = (sized + 8) as u32;
    let mut trafs = Vec::with_capacity(laid.len());
    for track in &laid {
        trafs.push(traf(track, offset));
        offset += track.data.iter().map(|data| data.len() as u32).sum::<u32>();
    }
    let mut moof_parts: Vec<&[u8]> = vec![&mfhd];
    moof_parts.extend(trafs.iter().map(Vec::as_slice));
    let moof = bx(b"moof", &moof_parts);
    debug_assert_eq!(moof.len(), sized);

    let payload: Vec<&[u8]> = laid
        .iter()
        .flat_map(|track| track.data.iter().map(|data| data.as_ref()))
        .collect();
    let mdat = bx(b"mdat", &payload);

    let mut out = Vec::with_capacity(styp.len() + moof.len() + mdat.len());
    out.extend_from_slice(&styp);
    out.extend_from_slice(&moof);
    out.extend_from_slice(&mdat);
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
        assert_eq!(laid.base, 0);
        let offsets: Vec<i32> = laid.entries.iter().map(|entry| entry.3).collect();
        assert_eq!(offsets, vec![0, 7200, -3600, -3600]);
        let durations: Vec<u32> = laid.entries.iter().map(|entry| entry.0).collect();
        assert_eq!(durations, vec![3600, 3600, 3600, 3600]);
        assert_eq!(laid.entries[0].2, 0x0200_0000);
        assert_eq!(laid.entries[1].2, 0x0101_0000);
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
        let segment = media_segment(&formats, 1, &[sample(0, true)], None, &audio, None);
        let styp_len = u32::from_be_bytes(segment[..4].try_into().unwrap()) as usize;
        let moof = &segment[styp_len..];
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
}
