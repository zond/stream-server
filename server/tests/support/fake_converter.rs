//! A picture converter for the tests (`stream_server::PictureConverter`),
//! modelling what the embedder's MediaCodec one does without decoding
//! anything (`docs/design/renditions.md` §5 F4).
//!
//! - **The decoder** takes pictures in decode order and gives them back in
//!   presentation order, holding [`FakeOptions::reorder`] of them. Started
//!   at a sync picture, it drops the pictures that follow it in decode
//!   order but are shown before it (an open GOP's leading pictures, which
//!   reference a picture it never saw).
//! - **Each decoded picture is routed** by `Segments::route`, as the
//!   contract says.
//! - **An encoder per segment** starts with its configuration
//!   ([`FakeOptions::csd`], or [`FakeOptions::csd_from`]'s from a segment
//!   on), holds [`FakeOptions::latency`] pictures, and makes its first
//!   picture a sync picture.
//! - **Slow** ([`FakeOptions::slow`]): every picture pushed takes that long,
//!   which is the work being simulated, not a wait.
//!
//! An encoded picture is one H.264 NAL unit (an IDR when it is a sync
//! picture) naming its presentation time in milliseconds and its segment,
//! in [`super::frame_bytes`]'s scheme ([`encoded_of`] reads it back), then
//! filler.

#![allow(dead_code)]

use bytes::Bytes;
use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use stream_server::{PictureConverter, PictureEvent, Route, Segments, SourcePicture};

#[derive(Clone, Debug)]
pub struct FakeOptions {
    /// Pictures the decoder holds before it gives back the earliest.
    pub reorder: usize,
    /// Pictures an encoder holds before it gives back the earliest.
    pub latency: usize,
    /// How long each picture pushed takes.
    pub slow: Option<Duration>,
    /// Every encoder's configuration, Annex-B.
    pub csd: Bytes,
    /// Another configuration, for every segment from the one named on.
    pub csd_from: Option<(u64, Bytes)>,
    pub width: u32,
    pub height: u32,
    /// Filler bytes after each encoded picture's names.
    pub filler: usize,
}

impl Default for FakeOptions {
    fn default() -> Self {
        let mut csd = super::SPS.to_vec();
        csd.extend_from_slice(super::PPS);
        Self {
            reorder: 2,
            latency: 3,
            slow: None,
            csd: Bytes::from(csd),
            csd_from: None,
            width: 320,
            height: 240,
            filler: 100,
        }
    }
}

/// What a converter did, as the tests read it.
#[derive(Debug, Default)]
pub struct ConverterRecord {
    /// The presentation times of the pictures routed `Drop`.
    pub dropped: Mutex<Vec<i64>>,
    /// The segments an encoder was started for, in order.
    pub encoders: Mutex<Vec<u64>>,
    /// The leading pictures the decoder could not decode.
    pub undecodable: Mutex<Vec<i64>>,
}

struct Encoder {
    segment: u64,
    held: VecDeque<i64>,
    pictures_in: u32,
    pictures_out: u32,
}

pub struct FakeConverter {
    options: FakeOptions,
    record: Arc<ConverterRecord>,
    /// The first picture pushed: the decoder starts there.
    start: Option<i64>,
    decoded: BTreeSet<i64>,
    open: Option<Encoder>,
}

impl FakeConverter {
    pub fn new(options: FakeOptions, record: Arc<ConverterRecord>) -> Self {
        Self {
            options,
            record,
            start: None,
            decoded: BTreeSet::new(),
            open: None,
        }
    }

    fn route(
        &mut self,
        pts_us: i64,
        segments: &Segments,
        out: &mut Vec<PictureEvent>,
    ) -> Result<(), String> {
        let segment = match segments.route(pts_us) {
            Route::Drop => {
                self.record.dropped.lock().unwrap().push(pts_us);
                return Ok(());
            }
            Route::Encode { segment } => segment,
        };
        match &self.open {
            Some(open) if open.segment > segment => {
                return Err(format!(
                    "a picture at {pts_us} us routed to segment {segment} below the open {}",
                    open.segment
                ));
            }
            Some(open) if open.segment == segment => {}
            _ => {
                self.end_encoder(out);
                let csd = match &self.options.csd_from {
                    Some((from, csd)) if segment >= *from => csd.clone(),
                    _ => self.options.csd.clone(),
                };
                self.record.encoders.lock().unwrap().push(segment);
                out.push(PictureEvent::Config {
                    segment,
                    width: self.options.width,
                    height: self.options.height,
                    csd,
                });
                self.open = Some(Encoder {
                    segment,
                    held: VecDeque::new(),
                    pictures_in: 0,
                    pictures_out: 0,
                });
            }
        }
        let latency = self.options.latency;
        let open = self.open.as_mut().expect("an encoder is open");
        open.held.push_back(pts_us);
        open.pictures_in += 1;
        while let Some(pts) = self
            .open
            .as_mut()
            .filter(|open| open.held.len() > latency)
            .and_then(|open| open.held.pop_front())
        {
            out.push(self.encoded(pts));
        }
        Ok(())
    }

    /// The open encoder's next picture out.
    fn encoded(&mut self, pts_us: i64) -> PictureEvent {
        let open = self.open.as_mut().expect("an encoder is open");
        let key = open.pictures_out == 0;
        open.pictures_out += 1;
        let segment = open.segment;
        PictureEvent::Picture {
            segment,
            pts_us,
            key,
            data: encoded(pts_us, segment, key, self.options.filler),
        }
    }

    fn end_encoder(&mut self, out: &mut Vec<PictureEvent>) {
        while let Some(pts) = self.open.as_mut().and_then(|open| open.held.pop_front()) {
            out.push(self.encoded(pts));
        }
        if let Some(open) = self.open.take() {
            out.push(PictureEvent::SegmentDone {
                segment: open.segment,
                pictures_in: open.pictures_in,
                pictures_out: open.pictures_out,
            });
        }
    }
}

impl PictureConverter for FakeConverter {
    fn push(
        &mut self,
        picture: SourcePicture,
        segments: &Segments,
    ) -> Result<Vec<PictureEvent>, String> {
        if let Some(slow) = self.options.slow {
            // The simulated work.
            std::thread::sleep(slow);
        }
        let start = match self.start {
            Some(start) => start,
            None if picture.key => *self.start.insert(picture.pts_us),
            None => {
                return Err("the decoder was started at a picture that is not a sync one".into());
            }
        };
        if picture.pts_us < start {
            self.record.undecodable.lock().unwrap().push(picture.pts_us);
            return Ok(Vec::new());
        }
        self.decoded.insert(picture.pts_us);
        let mut out = Vec::new();
        while self.decoded.len() > self.options.reorder {
            let pts = self.decoded.pop_first().expect("decoded");
            self.route(pts, segments, &mut out)?;
        }
        Ok(out)
    }

    fn finish(&mut self, segments: &Segments) -> Result<Vec<PictureEvent>, String> {
        let mut out = Vec::new();
        while let Some(pts) = self.decoded.pop_first() {
            self.route(pts, segments, &mut out)?;
        }
        self.end_encoder(&mut out);
        Ok(out)
    }
}

/// An encoded picture's bytes: one Annex-B H.264 NAL unit naming
/// `pts_us` (in milliseconds) and `segment`.
pub fn encoded(pts_us: i64, segment: u64, key: bool, filler: usize) -> Bytes {
    let mut data = vec![0, 0, 0, 1, if key { 0x65 } else { 0x41 }];
    data.extend_from_slice(&super::frame_bytes(pts_us / 1000));
    data.extend_from_slice(&super::frame_bytes(segment as i64));
    data.extend(std::iter::repeat_n(0xab, filler));
    Bytes::from(data)
}

/// What an encoded picture's NAL unit (its header byte first) names: its
/// presentation time in milliseconds, its segment, and whether it is an
/// IDR.
pub fn encoded_of(unit: &[u8]) -> (i64, u64, bool) {
    (
        super::frame_of(&unit[1..]),
        super::frame_of(&unit[5..]) as u64,
        unit[0] == 0x65,
    )
}
