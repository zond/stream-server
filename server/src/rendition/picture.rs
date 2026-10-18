//! **A converted picture**: the contract between a run and the embedder's
//! converter (`docs/design/renditions.md` §5 F4). The server never decodes.
//! The producer feeds the source's pictures, in decode order, to a
//! [`ConvertedPicture`] over the embedder's [`PictureConverter`]. That
//! decides which segment's encoder each decoded picture goes to and checks
//! what comes back. Segment k holds the pictures *shown* in
//! [cut k, cut k+1), whatever the source's decode order. So a segment's
//! bytes are the same whether a run started at it or passed through it
//! (open-GOP leading pictures included).
use super::{IndexEntry, Sample, TrackFormat, TrackKind};
use bytes::Bytes;
use std::sync::Arc;

/// What a converted picture is made as. Crosses FFI in [`super::VideoPlan::Convert`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PictureTarget {
    /// H.264 High, 8-bit 4:2:0.
    H264High,
    /// HEVC Main, 8-bit 4:2:0.
    HevcMain,
    /// HEVC Main 10, 10-bit 4:2:0, with the source's colours and transfer (HDR10, HLG) kept.
    HevcMain10,
}

impl PictureTarget {
    pub fn hevc(self) -> bool {
        !matches!(self, Self::H264High)
    }

    pub fn ten_bit(self) -> bool {
        matches!(self, Self::HevcMain10)
    }
}

/// **Where a run's segments begin, and the first one it makes.**
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segments {
    /// Segment k begins at `cuts[k]` µs on the film's clock; `cuts[0]` is `i64::MIN`.
    cuts: Arc<[i64]>,
    /// The run's first segment: its start slot's anchor. Pictures shown before its cut are not encoded.
    first: u64,
}

impl Segments {
    /// For tests and benches; `cuts[0]` must be `i64::MIN`, the rest increasing.
    #[doc(hidden)]
    pub fn new(cuts: Arc<[i64]>, first: u64) -> Self {
        debug_assert!(cuts.first() == Some(&i64::MIN));
        debug_assert!(cuts.windows(2).all(|pair| pair[0] < pair[1]));
        Self { cuts, first }
    }

    pub fn first(&self) -> u64 {
        self.first
    }

    pub fn count(&self) -> u64 {
        self.cuts.len() as u64
    }

    /// The segment a picture shown at `pts_us` belongs to: the last whose cut is at or before it
    /// (the same rule as the run's cutter).
    pub fn of(&self, pts_us: i64) -> u64 {
        (self.cuts.partition_point(|cut| *cut <= pts_us) as u64).saturating_sub(1)
    }

    /// Where `segment` begins (`i64::MIN` for segment 0).
    pub fn start_us(&self, segment: u64) -> i64 {
        usize::try_from(segment)
            .ok()
            .and_then(|at| self.cuts.get(at))
            .copied()
            .unwrap_or(i64::MAX)
    }

    /// What a converter does with a decoded picture shown at `pts_us`.
    pub fn route(&self, pts_us: i64) -> Route {
        let segment = self.of(pts_us);
        if segment < self.first {
            Route::Drop
        } else {
            Route::Encode { segment }
        }
    }
}

/// **Where a run's segments will begin**, before the producer has read the source's index
/// ([`super::Job::segments`]).
#[derive(Clone, Debug)]
pub struct SegmentPlan {
    frozen: Option<Segments>,
    source_len: u64,
    duration_us: i64,
    segment_us: i64,
    /// How the layout this run fixes sizes its slots. A converted
    /// rendition's segments are at least
    /// [`CONVERTED_MIN_SEGMENT_US`](super::layout::CONVERTED_MIN_SEGMENT_US)
    /// long, so the cuts depend on it.
    sizing: super::layout::Sizing,
}

impl SegmentPlan {
    /// The plan a run is handed. `frozen` is the frozen layout's segments,
    /// from the run's first segment; `None` for the run that fixes the
    /// layout, whose segments are cut from the index it reports.
    pub(crate) fn of_run(
        frozen: Option<Segments>,
        source_len: u64,
        duration_us: i64,
        segment_us: i64,
        sizing: super::layout::Sizing,
    ) -> Self {
        Self {
            frozen,
            source_len,
            duration_us,
            segment_us,
            sizing,
        }
    }

    /// This run's segments. A later run's are the frozen layout's, and `index` is not read. The
    /// run that fixes the layout (`Job::wants_index`) cuts them from `index`, which must be exactly
    /// what it reports through `SampleSink::index` (`None` if it reports nothing). The layout is
    /// cut from the same thing by the same function (`layout::Plan::new`).
    pub fn segments(&self, index: Option<&[IndexEntry]>) -> Segments {
        match &self.frozen {
            Some(frozen) => frozen.clone(),
            None => {
                let plan = super::layout::Plan::new(
                    index,
                    self.source_len,
                    self.duration_us,
                    self.segment_us,
                    self.sizing,
                );
                // The run that fixes the layout starts at the film's start,
                // so its first segment is the first.
                Segments::new(plan.cuts.into(), 0)
            }
        }
    }

    /// A converted rendition's plan for a film, before its layout is
    /// fixed: the cuts a converted rendition of it would have.
    #[doc(hidden)]
    pub fn for_film(source_len: u64, duration_ms: u64, segment_ms: u32) -> Self {
        Self::of_run(
            None,
            source_len,
            i64::try_from(duration_ms.saturating_mul(1000)).unwrap_or(i64::MAX),
            i64::from(segment_ms) * 1000,
            // The cuts depend on the picture being converted, not on the
            // rates, and only the cuts are read.
            super::layout::Sizing::Rate {
                video_bps: 0,
                sound_bps: 0,
            },
        )
    }
}

/// What happens to one decoded picture.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    /// Before the run's first segment. It was decoded only because later pictures need it. Release it unencoded.
    Drop,
    /// Encoded by `segment`'s encoder. A converter whose encoder is for an earlier segment ends that
    /// encoder first (every picture out, then [`PictureEvent::SegmentDone`]) and starts a fresh one,
    /// whose first picture is a sync picture.
    Encode { segment: u64 },
}

/// One access unit of the source's picture, in decode order, as the container stores it: Annex-B
/// for H.264 and HEVC (Dolby Vision NAL units dropped), AV1's and VP9's own framing otherwise.
#[derive(Clone, Debug)]
pub struct SourcePicture {
    pub pts_us: i64,
    pub key: bool,
    pub data: Bytes,
}

/// What a converter hands back, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PictureEvent {
    /// `segment`'s encoder started. Its codec configuration, Annex-B: the SPS then the PPS for
    /// H.264; the VPS, SPS, PPS and any SEI for HEVC (an HDR10 film's mastering display and light
    /// levels among them). `width`×`height` is the picture it encodes.
    Config {
        segment: u64,
        width: u32,
        height: u32,
        csd: Bytes,
    },
    /// One encoded picture of `segment`, Annex-B, at the presentation time it went in with.
    Picture {
        segment: u64,
        pts_us: i64,
        key: bool,
        data: Bytes,
    },
    /// `segment`'s encoder reached its end. `pictures_in` went in and `pictures_out` came out.
    SegmentDone {
        segment: u64,
        pictures_in: u32,
        pictures_out: u32,
    },
}

/// **What converts a run's picture**: a decoder and one encoder per segment, which the embedder
/// implements (xtremio: Android's MediaCodec through the NDK). It is built for one run on the
/// producer's thread, and its decoder starts at the run's first sync sample. It is dropped when the
/// run ends, however it ends, and dropping releases every codec. Its contract:
/// 1. `push` queues exactly that picture into the decoder. It blocks until the decoder takes it,
///    and drains both codecs meanwhile.
/// 2. Every decoded picture, in the decoder's (presentation) order, is routed by
///    `segments.route(pts)`. A picture routed below the open encoder's segment is an error.
/// 3. A fresh encoder's events start with `Config`. Its first `Picture` is a sync picture. Every
///    picture keeps the presentation time it went in with.
/// 4. All of segment j's events come before any of j+1's.
/// 5. `finish` ends the decoder's stream, routes what is left, and ends the open encoder.
///
/// Errors are a developer's detail. The producer logs them and fails the run with its sentence.
pub trait PictureConverter: Send {
    fn push(
        &mut self,
        picture: SourcePicture,
        segments: &Segments,
    ) -> Result<Vec<PictureEvent>, String>;
    fn finish(&mut self, segments: &Segments) -> Result<Vec<PictureEvent>, String>;
}

impl<C: PictureConverter + ?Sized> PictureConverter for Box<C> {
    fn push(
        &mut self,
        picture: SourcePicture,
        segments: &Segments,
    ) -> Result<Vec<PictureEvent>, String> {
        (**self).push(picture, segments)
    }

    fn finish(&mut self, segments: &Segments) -> Result<Vec<PictureEvent>, String> {
        (**self).finish(segments)
    }
}

/// What the producer hands its sink, in order.
#[derive(Clone, Debug)]
pub enum Converted {
    /// The video's format, once a run, before its first picture. Hold every sample (sound too)
    /// until it has gone: the run takes the formats as they stand at its first sample.
    Format(TrackFormat),
    Sample(Sample),
}

/// The encoder a driver has seen start and not yet end.
#[derive(Clone, Copy, Debug)]
struct Open {
    segment: u64,
    /// Pictures it has handed back.
    out: u32,
}

/// **The driver**: the run's segments over a converter, with every check the bytes depend on.
pub struct ConvertedPicture<C: PictureConverter> {
    converter: C,
    segments: Segments,
    target: PictureTarget,
    /// The video's format, from the first encoder's configuration.
    format: Option<TrackFormat>,
    /// The first encoder's configuration and size, which every later one must repeat.
    csd: Option<(u32, u32, Bytes)>,
    open: Option<Open>,
    /// The last segment whose encoder ended.
    done: Option<u64>,
    last_pts: Option<i64>,
    /// A picture was pushed.
    pushed: bool,
}

impl<C: PictureConverter> ConvertedPicture<C> {
    pub fn new(converter: C, segments: Segments, target: PictureTarget) -> Self {
        Self {
            converter,
            segments,
            target,
            format: None,
            csd: None,
            open: None,
            done: None,
            last_pts: None,
            pushed: false,
        }
    }

    pub fn segments(&self) -> &Segments {
        &self.segments
    }

    pub fn push(&mut self, picture: SourcePicture) -> Result<Vec<Converted>, String> {
        if !self.pushed {
            self.pushed = true;
            let first = self.segments.first;
            if first > 0 && self.segments.route(picture.pts_us) != Route::Drop {
                // The decoder starts at this picture, so nothing before
                // segment `first`'s cut is decoded. Its segment may then
                // begin at a later sync picture than a run passing through
                // it would begin it at.
                tracing::warn!(
                    first,
                    pts_us = picture.pts_us,
                    cut_us = self.segments.start_us(first),
                    "a converted run's first picture is at or after its first segment's cut"
                );
            }
        }
        let events = self.converter.push(picture, &self.segments)?;
        self.take(events)
    }

    pub fn finish(&mut self) -> Result<Vec<Converted>, String> {
        let events = self.converter.finish(&self.segments)?;
        let out = self.take(events)?;
        if let Some(open) = self.open {
            return Err(format!(
                "the converter finished with segment {}'s encoder still open",
                open.segment
            ));
        }
        Ok(out)
    }

    fn take(&mut self, events: Vec<PictureEvent>) -> Result<Vec<Converted>, String> {
        let mut out = Vec::with_capacity(events.len());
        for event in events {
            match event {
                PictureEvent::Config {
                    segment,
                    width,
                    height,
                    csd,
                } => {
                    if let Some(open) = self.open {
                        return Err(format!(
                            "segment {segment}'s encoder started before segment {}'s ended",
                            open.segment
                        ));
                    }
                    self.check_next(segment)?;
                    match &self.csd {
                        Some((w, h, first)) => {
                            if (*w, *h) != (width, height) || *first != csd {
                                return Err(format!(
                                    "segment {segment}'s encoder is configured unlike the \
                                     run's first: {width}x{height} csd {} against {w}x{h} csd {}",
                                    hex(&csd),
                                    hex(first)
                                ));
                            }
                        }
                        None => {
                            let format = self.format_of(width, height, &csd)?;
                            self.csd = Some((width, height, csd));
                            self.format = Some(format.clone());
                            out.push(Converted::Format(format));
                        }
                    }
                    self.open = Some(Open { segment, out: 0 });
                }
                PictureEvent::Picture {
                    segment,
                    pts_us,
                    key,
                    data,
                } => {
                    let Some(open) = self.open.as_mut().filter(|open| open.segment == segment)
                    else {
                        return Err(format!(
                            "a picture of segment {segment} at {pts_us} us came before its \
                             encoder's configuration"
                        ));
                    };
                    if open.out == 0 && !key {
                        return Err(format!(
                            "segment {segment}'s first picture at {pts_us} us is not a sync picture"
                        ));
                    }
                    if self.last_pts.is_some_and(|last| pts_us <= last) {
                        return Err(format!(
                            "a picture at {pts_us} us came after one at {} us",
                            self.last_pts.unwrap_or_default()
                        ));
                    }
                    let belongs = self.segments.of(pts_us);
                    if belongs != segment {
                        return Err(format!(
                            "a picture at {pts_us} us is segment {belongs}'s, not segment \
                             {segment}'s"
                        ));
                    }
                    open.out += 1;
                    self.last_pts = Some(pts_us);
                    out.push(Converted::Sample(Sample {
                        track: TrackKind::Video,
                        pts_us,
                        key,
                        data,
                    }));
                }
                PictureEvent::SegmentDone {
                    segment,
                    pictures_in,
                    pictures_out,
                } => {
                    let seen = match self.open {
                        Some(open) if open.segment == segment => open.out,
                        Some(open) => {
                            return Err(format!(
                                "segment {segment}'s encoder ended while segment {}'s was open",
                                open.segment
                            ));
                        }
                        // An encoder that was given nothing ends without
                        // ever having said its configuration.
                        None => {
                            self.check_next(segment)?;
                            0
                        }
                    };
                    if pictures_in != pictures_out || pictures_out != seen {
                        return Err(format!(
                            "segment {segment}'s encoder took {pictures_in} pictures and gave \
                             back {pictures_out}, of which {seen} arrived"
                        ));
                    }
                    self.open = None;
                    self.done = Some(segment);
                }
            }
        }
        Ok(out)
    }

    /// Whether `segment` may be the next encoder's: at or after the run's
    /// first, and after the last one that ended.
    fn check_next(&self, segment: u64) -> Result<(), String> {
        if segment < self.segments.first {
            return Err(format!(
                "an encoder for segment {segment} came before the run's first, {}",
                self.segments.first
            ));
        }
        if self.done.is_some_and(|done| segment <= done) {
            return Err(format!(
                "an encoder for segment {segment} came after segment {}'s ended",
                self.done.unwrap_or_default()
            ));
        }
        Ok(())
    }

    /// The video's format from the first encoder's configuration.
    fn format_of(&self, width: u32, height: u32, csd: &Bytes) -> Result<TrackFormat, String> {
        if csd.is_empty() {
            return Err("the encoder's configuration is empty".to_string());
        }
        if self.target.hevc() {
            return Ok(TrackFormat::Hevc {
                width,
                height,
                csd0: csd.clone(),
            });
        }
        let mut sps = Vec::new();
        let mut pps = Vec::new();
        for unit in super::mux::annex_b_units(csd) {
            let to = match unit[0] & 0x1f {
                7 => &mut sps,
                8 => &mut pps,
                _ => continue,
            };
            to.extend_from_slice(&[0, 0, 0, 1]);
            to.extend_from_slice(unit);
        }
        if sps.is_empty() || pps.is_empty() {
            return Err(format!(
                "the encoder's H.264 configuration lacks a sequence or picture parameter set: {}",
                hex(csd)
            ));
        }
        Ok(TrackFormat::H264 {
            width,
            height,
            csd0: Bytes::from(sps),
            csd1: Bytes::from(pps),
        })
    }
}

/// Bytes as hex, for a detail in a log.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
