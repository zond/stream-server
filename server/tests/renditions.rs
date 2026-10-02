//! Renditions (`stream_server::rendition`, `docs/design/renditions.md`,
//! steps F1-F2): `ServerHandle::publish_rendition`, the file at
//! `/cast/{token}/stream.mp4` on the LAN listener -- a length, ranges, a
//! `sidx`, one padded slot per segment, every byte fixed before it is made
//! -- the run task's cut rule, lookahead, seeks, idle release, speed and
//! cut, the overflow rule, and the fMP4 muxer: the run and the ring
//! through the fragment a range asks for (`ServerHandle::rendition_segment`),
//! the file over HTTP -- all driven by the test producer
//! (`support/test_producer.rs`), a Rust producer on a plain thread behind
//! the same trait the embedder's is.
//!
//! The boxes are read back by a parser of this file's own ([`Boxes`]), so
//! a muxer bug is not mirrored by the code that checks it. That no token
//! reaches a log line is `log_redaction.rs`'s.
//!
//! Every server here is offline, on ephemeral ports, and reads a file of
//! its own; nothing waits by sleeping -- every wait is a bounded poll on a
//! state the server or the producer shows.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use stream_server::rendition::{NotServed, RenditionTuning};
use stream_server::{
    AudioPlan, CastToken, LocalFile, MediaId, MediaSpec, RenditionSpec, RenditionState,
    ServerConfig, TrackKind, VideoPlan,
};

#[path = "support/torrent_fixtures.rs"]
mod torrent_fixtures;
use torrent_fixtures::offline_config;

#[path = "support/test_producer.rs"]
mod test_producer;
use test_producer::{ASC, IndexKnob, Knobs, PPS, SPS, TestProducer, frame_of};

/// A bound on a mistake, never a wait a correct run spends.
const BOUND: Duration = Duration::from_secs(60);

/// The segment length most tests use: short, so a run covers several.
const T_MS: u32 = 1000;
const T_US: i64 = T_MS as i64 * 1000;

/// How long the source file is unless a test says.
const SOURCE_LEN: usize = 1 << 20;

/// Poll `ready` until it holds, or fail naming `what`.
fn until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + BOUND;
    while !ready() {
        assert!(Instant::now() < deadline, "never: {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

// --- The fixture -----------------------------------------------------------------

struct Fixture {
    handle: stream_server::ServerHandle,
    lan: String,
    id: MediaId,
    producer: Arc<TestProducer>,
    _dirs: [tempfile::TempDir; 3],
}

impl Fixture {
    /// A server with the LAN listener up, a local file of `len` bytes
    /// registered, and a test producer with `knobs` installed.
    fn start_with(knobs: Knobs, tuning: RenditionTuning, len: usize) -> anyhow::Result<Self> {
        let config_dir = tempfile::tempdir()?;
        let cache_dir = tempfile::tempdir()?;
        let files = tempfile::tempdir()?;
        let handle = stream_server::start(ServerConfig {
            http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            config_dir: Some(config_dir.path().join("config")),
            cache_dir: Some(cache_dir.path().join("cache")),
            lan_media_addr: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            ..offline_config()
        })?;
        handle.update_settings(serde_json::json!({ "lanMediaEnabled": true }))?;
        let lan = handle
            .set_lan_media(true)?
            .ok_or_else(|| anyhow::anyhow!("no LAN address"))?;
        let path = files.path().join("film.mkv");
        std::fs::write(&path, vec![7u8; len])?;
        let id = handle.register(MediaSpec::Local {
            file: LocalFile::Path(path),
            name: None,
        })?;
        let producer = TestProducer::new(knobs);
        handle.set_rendition_tuning(tuning);
        handle.install_producer(producer.clone());
        Ok(Self {
            handle,
            lan: format!("http://{lan}"),
            id,
            producer,
            _dirs: [config_dir, cache_dir, files],
        })
    }

    fn start(knobs: Knobs, tuning: RenditionTuning) -> anyhow::Result<Self> {
        Self::start_with(knobs, tuning, SOURCE_LEN)
    }

    fn quick(knobs: Knobs) -> anyhow::Result<Self> {
        Self::start(knobs, RenditionTuning::default())
    }

    fn publish(&self, duration_ms: u64, start_ms: u64) -> anyhow::Result<CastToken> {
        self.handle
            .publish_rendition(&self.id, spec(duration_ms, T_MS, start_ms), None)
    }

    fn url(&self, token: &CastToken) -> String {
        format!("{}/cast/{}/stream.mp4", self.lan, token.as_str())
    }

    /// A `GET` of the file, with `range` as the `Range` header if any.
    fn get(&self, token: &CastToken, range: Option<String>) -> reqwest::blocking::Response {
        let request = reqwest::blocking::Client::builder()
            .timeout(BOUND)
            .build()
            .expect("a client")
            .get(self.url(token));
        let request = match range {
            Some(range) => request.header(reqwest::header::RANGE, range),
            None => request,
        };
        request.send().expect("the LAN listener answers")
    }

    /// The whole file.
    fn file(&self, token: &CastToken) -> Vec<u8> {
        let response = self.get(token, None);
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        response.bytes().expect("the whole file").to_vec()
    }

    /// Bytes `from..=to` of the file, answered `206`.
    fn range(&self, token: &CastToken, from: u64, to: u64) -> Vec<u8> {
        let response = self.get(token, Some(format!("bytes={from}-{to}")));
        assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
        let bytes = response.bytes().expect("the range").to_vec();
        assert_eq!(bytes.len() as u64, to + 1 - from);
        bytes
    }

    /// The file's header, read by ranges that end inside it -- so nothing
    /// but the header is asked for: the `ftyp`'s size, the `moov`'s, the
    /// `sidx`'s count, then the whole of it.
    fn header(&self, token: &CastToken) -> Header {
        let head = reqwest::blocking::Client::new()
            .head(self.url(token))
            .send()
            .expect("a HEAD");
        let total: u64 = head.headers()["content-length"]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let ftyp = u64::from(u32_at(&self.range(token, 0, 7), 0));
        let moov = u64::from(u32_at(&self.range(token, ftyp, ftyp + 7), 0));
        let sidx = self.range(token, ftyp + moov, ftyp + moov + 39);
        let count = u64::from(u16::from_be_bytes([sidx[38], sidx[39]]));
        let len = ftyp + moov + 40 + 12 * count;
        Header::of(&self.range(token, 0, len - 1), total)
    }

    /// The init segment, as the file begins with it.
    fn init(&self, token: &CastToken) -> Vec<u8> {
        self.handle
            .rendition_init(token)
            .expect("the init segment")
            .to_vec()
    }

    /// The fragment in slot `n`, as a range that begins in it asks for it.
    fn segment(&self, token: &CastToken, n: u64) -> Vec<u8> {
        self.handle
            .rendition_segment(token, n)
            .unwrap_or_else(|miss| panic!("segment {n}: {miss:?}"))
            .to_vec()
    }

    fn probe(&self, token: &CastToken) -> stream_server::rendition::RenditionProbe {
        self.handle
            .rendition_probe(token)
            .expect("the rendition is published")
    }
}

fn spec(duration_ms: u64, segment_ms: u32, start_ms: u64) -> RenditionSpec {
    RenditionSpec {
        duration_ms,
        segment_ms,
        start_ms,
        video: VideoPlan::Copy,
        audio: AudioPlan::Copy,
        audio_track: 0,
    }
}

// --- A box parser ----------------------------------------------------------------

/// The boxes at one level of an ISO BMFF buffer: type and body.
struct Boxes;

impl Boxes {
    fn of(data: &[u8]) -> Vec<(String, &[u8])> {
        let mut out = Vec::new();
        let mut at = 0;
        while at + 8 <= data.len() {
            let size = u32::from_be_bytes(data[at..at + 4].try_into().unwrap()) as usize;
            assert!(
                size >= 8 && at + size <= data.len(),
                "a box overruns its parent"
            );
            let kind = String::from_utf8_lossy(&data[at + 4..at + 8]).to_string();
            out.push((kind, &data[at + 8..at + size]));
            at += size;
        }
        assert_eq!(at, data.len(), "trailing bytes after the last box");
        out
    }

    /// The first box of `kind` in `data`.
    fn find<'a>(data: &'a [u8], kind: &str) -> &'a [u8] {
        Self::of(data)
            .into_iter()
            .find(|(found, _)| found == kind)
            .unwrap_or_else(|| panic!("no {kind} box"))
            .1
    }

    /// Down a path of container boxes.
    fn path<'a>(data: &'a [u8], path: &[&str]) -> &'a [u8] {
        path.iter().fold(data, |data, kind| Self::find(data, kind))
    }
}

fn u32_at(data: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(data[at..at + 4].try_into().unwrap())
}

/// The file's header as its boxes say it: where the init segment and the
/// `sidx` end, and each slot's place and duration by the `sidx`.
#[derive(Debug)]
struct Header {
    init_len: usize,
    earliest: u64,
    /// `(offset, size, duration)` per slot.
    slots: Vec<(u64, u64, u32)>,
    total: u64,
}

impl Header {
    /// From the file's first bytes (`head`), the file `total` long.
    fn of(head: &[u8], total: u64) -> Self {
        let size = |at: usize| u32_at(head, at) as usize;
        assert_eq!(&head[4..8], b"ftyp");
        let moov = size(0);
        assert_eq!(&head[moov + 4..moov + 8], b"moov");
        let init_len = moov + size(moov);
        let sidx = &head[init_len..init_len + size(init_len)];
        assert_eq!(&sidx[4..8], b"sidx");
        assert_eq!(sidx[8], 1, "sidx version 1");
        assert_eq!(u32_at(sidx, 12), 1, "the video track");
        assert_eq!(u32_at(sidx, 16), 90_000, "on the video's clock");
        assert_eq!(&sidx[28..36], &[0; 8], "slots begin right after it");
        let count = u16::from_be_bytes([sidx[38], sidx[39]]) as usize;
        let mut offset = (init_len + sidx.len()) as u64;
        let slots = (0..count)
            .map(|k| {
                let at = 40 + k * 12;
                let slot_size = u64::from(u32_at(sidx, at));
                assert_eq!(u32_at(sidx, at + 8), 1 << 31, "slot {k} starts with a SAP");
                let slot = (offset, slot_size, u32_at(sidx, at + 4));
                offset += slot_size;
                slot
            })
            .collect();
        assert_eq!(offset, total, "the sidx's slots end where the file does");
        Self {
            init_len,
            earliest: u64::from_be_bytes(sidx[20..28].try_into().unwrap()),
            slots,
            total,
        }
    }

    /// Slot `n`'s bytes, out of the whole file.
    fn slot<'a>(&self, file: &'a [u8], n: usize) -> &'a [u8] {
        let (offset, size, _) = self.slots[n];
        &file[offset as usize..(offset + size) as usize]
    }
}

/// A slot's fragment: `styp`, `moof`, `mdat`, then a `free` box to the
/// slot's end whose last bytes are zeros.
fn fragment_of(slot: &[u8]) -> &[u8] {
    let boxes = Boxes::of(slot);
    let kinds: Vec<&str> = boxes.iter().map(|(kind, _)| kind.as_str()).collect();
    assert_eq!(kinds, ["styp", "moof", "mdat", "free"]);
    let free = boxes[3].1.len() + 8;
    assert!(
        free >= 24,
        "the padding is at least a header and the zero tail"
    );
    assert!(slot[slot.len() - 16..].iter().all(|byte| *byte == 0));
    &slot[..slot.len() - free]
}

/// One track's run in a segment, as the boxes say it.
#[derive(Debug)]
struct Traf {
    track_id: u32,
    tfdt: u64,
    /// `(duration, size, composition offset)` per sample.
    samples: Vec<(u32, u32, i32)>,
    /// From the start of the `moof`.
    data_offset: usize,
}

struct Segment {
    sequence: u32,
    trafs: Vec<Traf>,
    /// The `moof` and everything after it: what data offsets count from.
    from_moof: Vec<u8>,
}

fn parse_segment(bytes: &[u8]) -> Segment {
    let top = Boxes::of(bytes);
    let kinds: Vec<&str> = top.iter().map(|(kind, _)| kind.as_str()).collect();
    assert_eq!(kinds, ["styp", "moof", "mdat"]);
    let moof_at = 8 + top[0].1.len();
    let moof = top[1].1;
    let mut sequence = 0;
    let mut trafs = Vec::new();
    for (kind, body) in Boxes::of(moof) {
        match kind.as_str() {
            "mfhd" => sequence = u32_at(body, 4),
            "traf" => {
                let tfhd = Boxes::find(body, "tfhd");
                assert_eq!(
                    u32_at(tfhd, 0) & 0xff_ffff,
                    0x02_0000,
                    "default-base-is-moof"
                );
                let tfdt = Boxes::find(body, "tfdt");
                assert_eq!(tfdt[0], 1, "tfdt version 1");
                let trun = Boxes::find(body, "trun");
                assert_eq!(trun[0], 1, "trun version 1");
                let flags = u32_at(trun, 0) & 0xff_ffff;
                let count = u32_at(trun, 4) as usize;
                let mut at = 8;
                let data_offset = if flags & 1 != 0 {
                    at += 4;
                    u32_at(trun, 8) as usize
                } else {
                    0
                };
                let mut samples = Vec::with_capacity(count);
                for _ in 0..count {
                    let mut field = |bit: u32| {
                        (flags & bit != 0).then(|| {
                            let value = u32_at(trun, at);
                            at += 4;
                            value
                        })
                    };
                    let duration = field(0x100).expect("durations");
                    let size = field(0x200).expect("sizes");
                    let _flags = field(0x400);
                    let offset = field(0x800).map_or(0, |value| value as i32);
                    samples.push((duration, size, offset));
                }
                trafs.push(Traf {
                    track_id: u32_at(tfhd, 4),
                    tfdt: u64::from_be_bytes(tfdt[4..12].try_into().unwrap()),
                    samples,
                    data_offset,
                });
            }
            other => panic!("an unexpected {other} in the moof"),
        }
    }
    Segment {
        sequence,
        trafs,
        from_moof: bytes[moof_at..].to_vec(),
    }
}

impl Segment {
    fn track(&self, id: u32) -> Option<&Traf> {
        self.trafs.iter().find(|traf| traf.track_id == id)
    }

    /// The video samples' presentation times in microseconds, each checked
    /// against the frame number its bytes carry.
    fn video_pts(&self, knobs: &Knobs) -> Vec<i64> {
        let Some(traf) = self.track(1) else {
            return Vec::new();
        };
        let mut dts = traf.tfdt as i64;
        let mut at = traf.data_offset;
        let mut out = Vec::new();
        for &(duration, size, offset) in &traf.samples {
            let pts_us = (dts + i64::from(offset)) * 1_000_000 / 90_000;
            let data = &self.from_moof[at..at + size as usize];
            assert_eq!(
                u32_at(data, 0) as usize,
                size as usize - 4,
                "one length-prefixed NAL unit"
            );
            let frame = frame_of(&data[5..]);
            assert_eq!(knobs.video_pts(frame), pts_us, "the bytes are the frame's");
            assert_eq!(data[4] == 0x65, knobs.is_key(frame), "an IDR is a key");
            out.push(pts_us);
            dts += i64::from(duration);
            at += size as usize;
        }
        out
    }

    fn audio_pts(&self) -> Vec<i64> {
        let Some(traf) = self.track(2) else {
            return Vec::new();
        };
        let mut ticks = traf.tfdt as i64;
        let mut at = traf.data_offset;
        let mut out = Vec::new();
        for &(duration, size, _) in &traf.samples {
            let pts_us = (ticks * 1_000_000 + 24_000) / 48_000;
            let frame = i64::from(u32_at(&self.from_moof, at));
            assert_eq!(Knobs::audio_pts(frame), pts_us, "the bytes are the frame's");
            out.push(pts_us);
            ticks += i64::from(duration);
            at += size as usize;
        }
        out
    }
}

/// Where segment `n` begins with an index of every sync sample: the first
/// at or after n x T; the first segment from the film's start.
fn cut(knobs: &Knobs, n: i64) -> i64 {
    if n == 0 {
        i64::MIN
    } else {
        knobs.key_at_or_after(n * T_US).unwrap_or(i64::MAX)
    }
}

/// What segment `n` must hold, by the cut rule, for `knobs`' film: video
/// from its cut's key to the next cut's, audio between the cuts.
fn expected(knobs: &Knobs, n: i64) -> (Vec<i64>, Vec<i64>) {
    let (from, to) = (cut(knobs, n), cut(knobs, n + 1));
    let video = knobs
        .video_frames()
        .into_iter()
        .map(|(pts, _)| pts)
        .filter(|pts| *pts >= from && *pts < to)
        .collect();
    let audio = knobs
        .audio_frames()
        .into_iter()
        .filter(|pts| *pts >= from && *pts < to)
        .collect();
    (video, audio)
}

/// `cut - SEEK_BACK`, at the film's start at the earliest: what a run made
/// for a segment asks its producer for.
fn asked_from(cut_us: i64) -> Duration {
    let back = stream_server::rendition::SEEK_BACK.as_micros() as i64;
    Duration::from_micros((cut_us - back).max(0) as u64)
}

// --- The header ------------------------------------------------------------------

/// **The init segment is `ftyp` + `moov`** with the producer's parameter
/// sets in the `avcC` and its AudioSpecificConfig in the `esds`; the first
/// run -- asked for the source's index -- starts a segment and
/// [`SEEK_BACK`](stream_server::rendition::SEEK_BACK) before the spec's
/// start, so it makes the segment the receiver starts at.
#[test]
fn the_init_segment_carries_the_producers_codec_configuration() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs::default())?;
    let token = fixture.publish(60_000, 7_500)?;
    let init = fixture.init(&token);
    let kinds: Vec<String> = Boxes::of(&init).into_iter().map(|(kind, _)| kind).collect();
    assert_eq!(kinds, ["ftyp", "moov"]);

    let moov = Boxes::find(&init, "moov");
    let traks: Vec<&[u8]> = Boxes::of(moov)
        .into_iter()
        .filter(|(kind, _)| kind == "trak")
        .map(|(_, body)| body)
        .collect();
    assert_eq!(traks.len(), 2);
    Boxes::path(moov, &["mvex", "trex"]);

    let stsd = Boxes::path(traks[0], &["mdia", "minf", "stbl", "stsd"]);
    let avc1 = Boxes::find(&stsd[8..], "avc1");
    let avcc = Boxes::find(&avc1[78..], "avcC");
    let sps_len = u16::from_be_bytes([avcc[6], avcc[7]]) as usize;
    assert_eq!(&avcc[8..8 + sps_len], &SPS[4..], "the producer's SPS");
    let pps_at = 8 + sps_len + 1;
    let pps_len = u16::from_be_bytes([avcc[pps_at], avcc[pps_at + 1]]) as usize;
    assert_eq!(
        &avcc[pps_at + 2..pps_at + 2 + pps_len],
        &PPS[4..],
        "and its PPS"
    );
    assert_eq!(&avcc[1..4], &SPS[5..8], "profile, compatibility, level");

    let stsd = Boxes::path(traks[1], &["mdia", "minf", "stbl", "stsd"]);
    let mp4a = Boxes::find(&stsd[8..], "mp4a");
    let esds = Boxes::find(&mp4a[28..], "esds");
    assert!(
        esds.windows(ASC.len() + 2)
            .any(|window| window[0] == 5 && window[1] as usize == ASC.len() && &window[2..] == ASC),
        "the AudioSpecificConfig is the decoder-specific info"
    );

    let runs = fixture.producer.runs();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].from, Duration::from_millis(7_500 - 1_000 - 2_000));
    assert!(runs[0].wanted_index);
    let probe = fixture.probe(&token);
    assert_eq!(probe.run_from, Some(5), "the first slot cut after 4.5 s");
    assert_eq!(probe.exact, Some(true));
    Ok(())
}

/// **The init segment says how long the film is**, in the box a player
/// reads it from: each track's `mdhd` (version 1, on the track's clock) --
/// Chrome's MP4 demuxer (ffmpeg's) takes a fragmented file's duration from
/// there and nowhere else -- and `mvhd` and `tkhd` on the movie clock.
#[test]
fn the_init_segment_says_how_long_the_film_is() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs::default())?;
    let token = fixture.publish(60_000, 0)?;
    let init = fixture.init(&token);
    let moov = Boxes::find(&init, "moov");
    let mvhd = Boxes::find(moov, "mvhd");
    assert_eq!(u32_at(mvhd, 12), 1000, "the movie clock is in ms");
    assert_eq!(u32_at(mvhd, 16), 60_000);
    let traks: Vec<&[u8]> = Boxes::of(moov)
        .into_iter()
        .filter(|(kind, _)| kind == "trak")
        .map(|(_, body)| body)
        .collect();
    for (trak, timescale) in traks.iter().zip([90_000u64, 48_000]) {
        let tkhd = Boxes::find(trak, "tkhd");
        assert_eq!(u32_at(tkhd, 20), 60_000, "tkhd, on the movie clock");
        let mdhd = Boxes::path(trak, &["mdia", "mdhd"]);
        assert_eq!(mdhd[0], 1, "a 64-bit mdhd");
        assert_eq!(u64::from(u32_at(mdhd, 20)), timescale);
        let duration = u64::from_be_bytes(mdhd[24..32].try_into().unwrap());
        assert_eq!(duration, 60 * timescale, "mdhd, on the track's own clock");
    }
    Ok(())
}

/// **The `sidx` mirrors the source's index**: one slot per segment, each
/// the source's bytes from its cut's sync sample to the next one's (the
/// last to the source's end) plus the headroom; durations from the cuts.
#[test]
fn the_sidx_mirrors_the_sources_index() -> anyhow::Result<()> {
    let knobs = Knobs::default();
    let fixture = Fixture::quick(knobs.clone())?;
    let token = fixture.publish(60_000, 0)?;
    let header = fixture.header(&token);
    assert_eq!(header.slots.len(), 60);
    assert_eq!(fixture.probe(&token).exact, Some(true));
    let place = |pts: i64| (pts as i128 * SOURCE_LEN as i128 / 60_000_000) as u64;
    let headroom = |span: u64| span + 8 * 1024 + span / 64;
    for (k, (_, size, duration)) in header.slots.iter().enumerate() {
        let k = k as i64;
        let start = if k == 0 { 0 } else { cut(&knobs, k) };
        let end = if k == 59 {
            SOURCE_LEN as u64
        } else {
            place(cut(&knobs, k + 1))
        };
        assert_eq!(*size, headroom(end - place(start)), "slot {k}'s size");
        let next = if k == 59 {
            60_000_000
        } else {
            cut(&knobs, k + 1)
        };
        assert_eq!(
            i64::from(*duration),
            (next - start) * 9 / 100,
            "slot {k}'s duration"
        );
    }
    assert_eq!(header.earliest, 0);
    Ok(())
}

/// **Without an index the layout is estimated**: slots on the grid, in
/// proportion to time over the source, 15% larger and a base on top.
#[test]
fn without_an_index_the_slots_are_estimated() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs {
        index: None,
        length: Duration::from_secs(10),
        ..Knobs::default()
    })?;
    let token = fixture.publish(10_000, 0)?;
    let header = fixture.header(&token);
    assert_eq!(fixture.probe(&token).exact, Some(false));
    assert_eq!(header.slots.len(), 10);
    let scaled = |k: u64| k * SOURCE_LEN as u64 * 115 / 1000;
    for (k, (_, size, duration)) in header.slots.iter().enumerate() {
        let k = k as u64;
        assert_eq!(*size, scaled(k + 1) - scaled(k) + 8 * 1024, "slot {k}");
        assert_eq!(*duration, 90_000);
    }
    let file = fixture.file(&token);
    for n in 0..10 {
        let segment = parse_segment(fragment_of(header.slot(&file, n)));
        assert_eq!(segment.sequence, n as u32 + 1);
    }
    Ok(())
}

// --- The file ----------------------------------------------------------------------

/// **The file is the header and every slot in order**: `video/mp4`, its
/// length and ranges offered, one body counted; each slot the fragment a
/// range asks for, then a `free` box to the slot's end.
#[test]
fn the_file_is_the_header_and_every_slot_padded() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs {
        length: Duration::from_millis(5_500),
        ..Knobs::default()
    })?;
    let token = fixture.publish(5_500, 0)?;
    let bodies = fixture.handle.lan_media_bodies_served();
    let response = fixture.get(&token, None);
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "video/mp4");
    assert_eq!(response.headers()["accept-ranges"], "bytes");
    let length: u64 = response.headers()["content-length"].to_str()?.parse()?;
    let file = response.bytes()?.to_vec();
    assert_eq!(file.len() as u64, length);
    assert_eq!(fixture.handle.lan_media_bodies_served(), bodies + 1);

    let header = Header::of(&file, length);
    assert_eq!(&file[..header.init_len], fixture.init(&token).as_slice());
    assert_eq!(header.slots.len(), 6);
    for n in 0..6 {
        let fragment = fragment_of(header.slot(&file, n));
        assert!(
            fragment == fixture.segment(&token, n as u64).as_slice(),
            "slot {n} is not its fragment"
        );
    }
    Ok(())
}

/// **Each segment is cut at the first indexed key at or after N x T** --
/// its `tfdt` is that key -- and holds exactly the video up to the next
/// cut, and the audio between the two cuts.
#[test]
fn each_segment_is_cut_at_the_first_key_at_or_after_its_time() -> anyhow::Result<()> {
    let knobs = Knobs::default();
    let fixture = Fixture::quick(knobs.clone())?;
    let token = fixture.publish(30_000, 0)?;
    fixture.init(&token);
    for n in 0..6i64 {
        let segment = parse_segment(&fixture.segment(&token, n as u64));
        assert_eq!(segment.sequence, n as u32 + 1);
        let video = segment.track(1).expect("video in every segment here");
        let key = knobs.key_at_or_after(n * T_US).unwrap();
        assert_eq!(
            video.tfdt,
            (key * 90_000 / 1_000_000) as u64,
            "segment {n}'s tfdt"
        );
        let (want_video, want_audio) = expected(&knobs, n);
        assert_eq!(segment.video_pts(&knobs), want_video, "segment {n}'s video");
        assert_eq!(segment.audio_pts(), want_audio, "segment {n}'s audio");
    }
    Ok(())
}

/// **What precedes the cut is discarded**: a run for segment 5, which the
/// producer starts at the key before 5.28 s less two seconds, puts nothing
/// before that key in segment 5, and no audio before it.
#[test]
fn a_run_discards_what_precedes_its_cut() -> anyhow::Result<()> {
    let knobs = Knobs::default();
    let fixture = Fixture::quick(knobs.clone())?;
    let token = fixture.publish(30_000, 0)?;
    fixture.init(&token);
    let segment = parse_segment(&fixture.segment(&token, 5));
    let runs = fixture.producer.runs();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[1].from, asked_from(5_280_000));
    assert!(!runs[1].wanted_index, "the layout is frozen already");
    let emitted = runs[1].emitted();
    assert!(
        emitted
            .iter()
            .any(|(track, pts)| *track == TrackKind::Video && *pts < 5 * T_US),
        "the producer did hand over video before the cut: {emitted:?}"
    );
    assert!(
        emitted
            .iter()
            .any(|(track, pts)| *track == TrackKind::Audio && *pts < 5 * T_US),
        "and audio"
    );
    assert!(
        fixture.probe(&token).ring.iter().all(|n| *n >= 5),
        "nothing before the run's segment is made: {:?}",
        fixture.probe(&token).ring
    );
    let (want_video, want_audio) = expected(&knobs, 5);
    assert_eq!(want_video.first(), Some(&5_280_000));
    assert_eq!(segment.video_pts(&knobs), want_video);
    assert_eq!(segment.audio_pts(), want_audio);
    assert_eq!(segment.track(1).unwrap().tfdt, 5_280_000 * 90 / 1000);
    Ok(())
}

/// **Every range is the same bytes** as the whole file's, however it is
/// asked for: a far range first (a seek, made by a run started there),
/// then overlapping ranges across slot boundaries, then the whole file
/// read in order by another run.
#[test]
fn every_range_is_the_same_bytes_as_the_whole() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs {
        length: Duration::from_millis(12_000),
        ..Knobs::default()
    })?;
    let token = fixture.publish(12_000, 0)?;
    let header = fixture.header(&token);
    let far = header.slots[9].0 + 100;
    let far_bytes = fixture.range(&token, far, far + 5_000);
    let middle = header.slots[3].0 - 7;
    let first = fixture.range(&token, middle - 2_000, middle + 2_000);
    let second = fixture.range(&token, middle, middle + 30_000);
    let file = fixture.file(&token);
    assert!(file[far as usize..far as usize + 5_001] == far_bytes[..]);
    assert!(file[(middle - 2_000) as usize..=(middle + 2_000) as usize] == first[..]);
    assert!(file[middle as usize..=(middle + 30_000) as usize] == second[..]);
    assert!(
        fixture.probe(&token).runs_started >= 3,
        "the far slot, the middle and the whole file were made by different runs"
    );
    Ok(())
}

/// **A range is answered `206`** with the bytes it names and where they
/// are in the file; past the end is `416` naming the length.
#[test]
fn a_range_is_answered_with_its_bytes_and_where_they_are() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs {
        length: Duration::from_millis(6_000),
        ..Knobs::default()
    })?;
    let token = fixture.publish(6_000, 0)?;
    let header = fixture.header(&token);
    let total = header.total;
    let response = fixture.get(&token, Some(format!("bytes={}-", total - 100)));
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response.headers()["content-range"],
        format!("bytes {}-{}/{total}", total - 100, total - 1).as_str()
    );
    assert_eq!(response.headers()["content-length"], "100");
    assert_eq!(response.bytes()?.len(), 100);
    let past = fixture.get(&token, Some(format!("bytes={total}-")));
    assert_eq!(past.status(), reqwest::StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        past.headers()["content-range"],
        format!("bytes */{total}").as_str()
    );
    Ok(())
}

/// **A slot's last bytes are zeros, answered without making anything**:
/// a demuxer's peek at the file's end (an `mfra` size) moves no run.
#[test]
fn the_end_of_the_file_is_answered_without_a_run() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs::default())?;
    let token = fixture.publish(30_000, 0)?;
    let header = fixture.header(&token);
    let before = fixture.probe(&token);
    let tail = fixture.range(&token, header.total - 16, header.total - 1);
    assert_eq!(tail, vec![0; 16]);
    let after = fixture.probe(&token);
    assert_eq!(after.runs_started, before.runs_started);
    assert_eq!(after.run_from, before.run_from);
    Ok(())
}

/// **A receiver opening the file reads on from the header without moving
/// the run**: the first run makes the slot at the spec's start, a read of
/// the header that goes on into slot 0 waits rather than move it, and the
/// receiver's jump to its start joins it.
#[test]
fn opening_the_file_does_not_move_the_run_from_the_start() -> anyhow::Result<()> {
    let fixture = Fixture::start(
        Knobs::default(),
        RenditionTuning {
            idle_release: Duration::from_secs(3600),
            ..RenditionTuning::default()
        },
    )?;
    let token = fixture.publish(60_000, 30_000)?;
    let mut opening = fixture.get(&token, Some("bytes=0-".to_string()));
    assert_eq!(opening.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let mut first = [0u8; 8];
    std::io::Read::read_exact(&mut opening, &mut first)?;
    assert_eq!(&first[4..], b"ftyp");
    let header = fixture.header(&token);
    // A segment and two seconds before the start: the first cut after
    // 27 s.
    assert_eq!(fixture.probe(&token).run_from, Some(27));
    until("the first run makes the slots from its start", || {
        fixture.probe(&token).ring == [27, 28, 29]
    });
    let at = header.slots[30].0;
    let target = fixture.range(&token, at, at + 1000);
    assert_eq!(&target[4..8], b"styp");
    let probe = fixture.probe(&token);
    assert_eq!(probe.runs_started, 1, "one run, from the start");
    assert_eq!(probe.run_from, Some(27));
    drop(opening);
    Ok(())
}

/// **Two reads far apart do not take the run from each other**: a body
/// reading on from slot 2 that its client stopped taking (`ffprobe` keeps
/// its first connection open while it seeks on a second), and a range at
/// slot 40. The range moves the run at most once; the body reading on
/// waits for it rather than move it back.
#[test]
fn a_read_left_open_does_not_take_the_run_back() -> anyhow::Result<()> {
    let fixture = Fixture::start(
        Knobs::default(),
        RenditionTuning {
            idle_release: Duration::from_secs(3600),
            ..RenditionTuning::default()
        },
    )?;
    let token = fixture.publish(60_000, 0)?;
    let header = fixture.header(&token);
    let mut left_open = fixture.get(&token, Some(format!("bytes={}-", header.slots[2].0)));
    let mut first = [0u8; 8];
    std::io::Read::read_exact(&mut left_open, &mut first)?;
    assert_eq!(&first[4..], b"styp");
    let (at, size, _) = header.slots[40];
    let far = fixture.range(&token, at, at + size - 1);
    assert_eq!(&far[4..8], b"styp");
    let probe = fixture.probe(&token);
    assert!(
        probe.runs_started <= 3,
        "{} runs: the reads took the run from each other",
        probe.runs_started
    );
    drop(left_open);
    Ok(())
}

/// **Two seeks waiting at once take turns, not the run from each other**:
/// a range at slot 40 waits for the run it moved there; a range at slot 10
/// moves it again and keeps it; the first waits for that run to be let go
/// -- not while the second still waits on it -- and then has a run of its
/// own, counted from its own slot.
#[test]
fn two_seeks_waiting_at_once_take_turns() -> anyhow::Result<()> {
    let fixture = Fixture::start(
        Knobs {
            speed: Some(4.0),
            ..Knobs::default()
        },
        RenditionTuning {
            idle_release: Duration::from_millis(300),
            ..RenditionTuning::default()
        },
    )?;
    let token = fixture.publish(60_000, 0)?;
    let header = fixture.header(&token);
    let slot = |n: usize| {
        let (at, size, _) = header.slots[n];
        (at, at + size - 1)
    };
    let (fixture, token) = (&fixture, &token);
    let (far, near) = std::thread::scope(|scope| {
        let (from, to) = slot(40);
        let far = scope.spawn(move || fixture.range(token, from, to));
        until("the far range's run is at slot 40", || {
            fixture.probe(token).run_from == Some(40)
        });
        let (from, to) = slot(10);
        let near = fixture.range(token, from, to);
        (far.join().expect("the far range"), near)
    });
    assert_eq!(&far[4..8], b"styp");
    assert_eq!(&near[4..8], b"styp");
    let runs = fixture.probe(token).runs_started;
    assert!(
        runs <= 4,
        "{runs} runs: the seeks took the run from each other"
    );
    Ok(())
}

// --- The overflow rule -------------------------------------------------------------

/// Knobs whose segments overflow the slot an index squeezed: frames big
/// enough that a segment is mostly its share of the source.
fn squeezed() -> Knobs {
    Knobs {
        length: Duration::from_secs(20),
        frame_bytes: 2_000,
        index: Some(IndexKnob::Squeezed { at_us: 10 * T_US }),
        ..Knobs::default()
    }
}

/// The source for [`squeezed`]: about as many bytes as the film's samples.
const SQUEEZED_LEN: usize = 20 * 60_000;

/// Every video and audio frame in the file's slots, in order.
fn frames_in(file: &[u8], header: &Header, knobs: &Knobs) -> (Vec<i64>, Vec<i64>) {
    let (mut video, mut audio) = (Vec::new(), Vec::new());
    for n in 0..header.slots.len() {
        let segment = parse_segment(fragment_of(header.slot(file, n)));
        video.extend(segment.video_pts(knobs));
        audio.extend(segment.audio_pts());
    }
    (video, audio)
}

/// **A segment that overflows its slot spills into the next**: the slot
/// keeps what fits, the next begins with the rest, and the file holds every
/// frame once, in order. The next slot made again by a run started at it
/// starts where the spill left off: the same bytes.
#[test]
fn an_overflowing_segment_spills_into_the_next_slot() -> anyhow::Result<()> {
    let knobs = squeezed();
    let fixture = Fixture::start_with(knobs.clone(), RenditionTuning::default(), SQUEEZED_LEN)?;
    let token = fixture.publish(20_000, 0)?;
    let file = fixture.file(&token);
    let header = Header::of(&file, file.len() as u64);
    let probe = fixture.probe(&token);
    assert_eq!(probe.exact, Some(true));
    assert_eq!(probe.spilled, vec![10]);
    assert!(probe.truncated.is_empty());
    let (video, audio) = frames_in(&file, &header, &knobs);
    let all_video: Vec<i64> = knobs
        .video_frames()
        .into_iter()
        .map(|(pts, _)| pts)
        .collect();
    assert_eq!(video, all_video, "every frame, once, in order");
    assert_eq!(audio, knobs.audio_frames());
    let spilled = parse_segment(fragment_of(header.slot(&file, 11)));
    assert!(
        spilled.video_pts(&knobs)[0] < cut(&knobs, 11),
        "slot 11 begins with segment 10's end"
    );

    // Slot 11 again, from a run started there.
    let at = header.slots[11].0;
    let runs = fixture.probe(&token).runs_started;
    let ring_edge = header.slots[19].0;
    fixture.range(&token, ring_edge, ring_edge + 100);
    let again = fixture.range(&token, at, at + header.slots[11].1 - 1);
    assert!(
        fixture.probe(&token).runs_started > runs,
        "slot 11 was made again"
    );
    assert!(again == header.slot(&file, 11), "and is the same bytes");

    // Slot 10 again, from a run started there: it spills as it did the
    // first time, though the next slot's start is decided now.
    fixture.range(&token, ring_edge, ring_edge + 100);
    let (at, size, _) = header.slots[10];
    let runs = fixture.probe(&token).runs_started;
    let ten = fixture.range(&token, at, at + size - 1);
    assert!(
        fixture.probe(&token).runs_started > runs,
        "slot 10 was made again"
    );
    assert!(ten == header.slot(&file, 10), "and is the same bytes");
    assert!(fixture.probe(&token).truncated.is_empty());
    Ok(())
}

/// **A segment that overflows a slot whose next is already decided is
/// truncated**: the next slot was made first (a seek), so this one keeps
/// what fits and drops the rest -- and the next slot's bytes do not change.
#[test]
fn an_overflow_into_a_decided_slot_is_truncated() -> anyhow::Result<()> {
    let knobs = squeezed();
    let fixture = Fixture::start_with(knobs.clone(), RenditionTuning::default(), SQUEEZED_LEN)?;
    let token = fixture.publish(20_000, 0)?;
    let header = fixture.header(&token);
    let (at, size, _) = header.slots[11];
    let next_first = fixture.range(&token, at, at + size - 1);
    let (at10, size10, _) = header.slots[10];
    let ten = fixture.range(&token, at10, at10 + size10 - 1);
    let probe = fixture.probe(&token);
    assert_eq!(probe.truncated, vec![10]);
    assert!(probe.spilled.is_empty());
    let segment = parse_segment(fragment_of(&ten));
    let (want_video, _) = expected(&knobs, 10);
    let kept = segment.video_pts(&knobs);
    assert!(
        !kept.is_empty() && kept.len() < want_video.len(),
        "a prefix of segment 10's video: {kept:?}"
    );
    assert_eq!(kept, want_video[..kept.len()]);
    let file = fixture.file(&token);
    assert!(header.slot(&file, 11) == next_first.as_slice());
    assert!(header.slot(&file, 10) == ten.as_slice());
    Ok(())
}

// --- Seeks, joins, the lookahead, the idle release -------------------------------

/// **A request far ahead is a seek**: slot 40 after 0-3 starts a new run
/// there, and the old run's sink answers `Stopped`.
#[test]
fn a_request_far_ahead_starts_a_new_run_and_stops_the_old() -> anyhow::Result<()> {
    let knobs = Knobs::default();
    let fixture = Fixture::quick(knobs.clone())?;
    let token = fixture.publish(60_000, 0)?;
    fixture.init(&token);
    for n in 0..4 {
        fixture.segment(&token, n);
    }
    assert_eq!(fixture.probe(&token).runs_started, 1);
    let segment = parse_segment(&fixture.segment(&token, 40));
    let runs = fixture.producer.runs();
    assert_eq!(runs.len(), 2);
    let key = knobs.key_at_or_after(40 * T_US).unwrap();
    assert_eq!(runs[1].from, asked_from(key));
    let probe = fixture.probe(&token);
    assert_eq!(probe.runs_started, 2);
    assert_eq!(probe.run_from, Some(40));
    until("the old run's sink answers Stopped", || runs[0].stopped());
    assert_eq!(segment.track(1).unwrap().tfdt, (key * 90 / 1000) as u64);
    Ok(())
}

/// **A request for the slot in production joins it**: two requests for
/// it while a slow producer makes it get the same bytes from one run.
#[test]
fn a_request_for_the_segment_in_production_joins_it() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs {
        speed: Some(2.0),
        ..Knobs::default()
    })?;
    let token = fixture.publish(60_000, 0)?;
    fixture.init(&token);
    assert_eq!(fixture.probe(&token).in_production, Some(0));
    let bodies: Vec<Vec<u8>> = std::thread::scope(|scope| {
        let requests: Vec<_> = (0..2)
            .map(|_| scope.spawn(|| fixture.segment(&token, 0)))
            .collect();
        requests
            .into_iter()
            .map(|request| request.join().expect("the request thread"))
            .collect()
    });
    assert_eq!(bodies[0], bodies[1]);
    assert_eq!(fixture.probe(&token).runs_started, 1, "no second run");
    assert_eq!(fixture.producer.runs().len(), 1);
    Ok(())
}

/// **With nothing requested the producer blocks after L slots**: the
/// ring holds the requested slot and the two after it, and the sink's
/// next write waits; a request for the next slot lets one more through.
#[test]
fn the_lookahead_blocks_the_producer() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs::default())?;
    let token = fixture.publish(60_000, 0)?;
    fixture.init(&token);
    let run = fixture.producer.runs()[0].clone();
    // The sink blocks whenever the channel is full, which a producer this
    // fast makes it often; the lookahead is the state it stays in: the
    // ring at L ahead, the next slot in production, the sink blocked.
    let held = |ring: &[u64], next: u64| {
        let probe = fixture.probe(&token);
        probe.ring == ring && probe.in_production == Some(next) && run.probe.is_blocked()
    };
    until(
        "the producer blocks with L = 2 slots ahead of slot 0",
        || held(&[0, 1, 2], 3),
    );
    let furthest = |run: &test_producer::RunRecord| {
        run.emitted().iter().map(|(_, pts)| *pts).max().unwrap_or(0)
    };
    assert!(
        furthest(&run) < 5 * T_US,
        "the producer was held a channel's worth past slot 3's cut, not let run on: {}",
        furthest(&run)
    );

    fixture.segment(&token, 1);
    until(
        "one more slot is made, and the producer blocks again",
        || held(&[0, 1, 2, 3], 4),
    );
    assert!(furthest(&run) < 6 * T_US);
    assert!(!run.stopped());
    Ok(())
}

/// **An idle run is let go**: after the release period with no request
/// the run is dropped (its sink answers `Stopped`) and the ring kept; the
/// next request, at the ring's edge, starts a new run there.
#[test]
fn an_idle_run_is_released_and_restarted_at_the_rings_edge() -> anyhow::Result<()> {
    let knobs = Knobs::default();
    let fixture = Fixture::start(
        knobs.clone(),
        RenditionTuning {
            idle_release: Duration::from_millis(300),
            ..RenditionTuning::default()
        },
    )?;
    let token = fixture.publish(60_000, 0)?;
    fixture.init(&token);
    fixture.segment(&token, 0);
    let first = fixture.producer.runs()[0].clone();
    until("the idle run is let go", || {
        fixture.probe(&token).run_from.is_none()
    });
    until("its sink answers Stopped", || first.stopped());
    assert_eq!(fixture.handle.rendition_state(&token), RenditionState::Idle);
    assert_eq!(
        fixture.probe(&token).ring,
        vec![0, 1, 2],
        "the ring is kept"
    );

    fixture.segment(&token, 1);
    assert_eq!(
        fixture.probe(&token).runs_started,
        1,
        "served from the ring"
    );
    let segment = parse_segment(&fixture.segment(&token, 3));
    let runs = fixture.producer.runs();
    assert_eq!(runs.len(), 2);
    assert_eq!(
        runs[1].from,
        asked_from(knobs.key_at_or_after(3 * T_US).unwrap()),
        "at the ring's edge"
    );
    assert_eq!(segment.sequence, 4);
    Ok(())
}

// --- The cut ---------------------------------------------------------------------

/// **Unpublish answers a range still waiting for its first slot with an
/// error** -- a `503`, never a clean end -- and stops the producer.
#[test]
fn unpublish_wakes_a_waiting_range_and_stops_the_producer() -> anyhow::Result<()> {
    let fixture = Fixture::start(
        Knobs {
            speed: Some(0.05),
            ..Knobs::default()
        },
        // Neither the speed rule nor the idle release may end the run
        // here: only the cut.
        RenditionTuning {
            speed_window: Duration::from_secs(3600),
            idle_release: Duration::from_secs(3600),
        },
    )?;
    let token = fixture.publish(60_000, 0)?;
    let header = fixture.header(&token);
    let before = fixture.handle.lan_media_requests_served();
    let url = fixture.url(&token);
    let range = format!("bytes={}-", header.slots[0].0);
    let waiting = std::thread::spawn(move || {
        reqwest::blocking::Client::builder()
            .timeout(BOUND)
            .build()
            .and_then(|client| {
                client
                    .get(&url)
                    .header(reqwest::header::RANGE, range)
                    .send()
            })
            .map(|response| (response.status(), response.text().unwrap_or_default()))
    });
    until("the range request reaches the listener", || {
        fixture.handle.lan_media_requests_served() > before
    });
    assert_eq!(
        fixture.handle.rendition_state(&token),
        RenditionState::Producing
    );
    assert!(fixture.handle.unpublish(&token));
    let (status, body) = waiting.join().expect("the request thread")?;
    assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("unpublished"), "{body}");
    let run = fixture.producer.runs()[0].clone();
    until("the producer is stopped", || run.stopped() && run.done());
    assert_eq!(
        fixture.handle.rendition_state(&token),
        RenditionState::Ended
    );
    assert_eq!(
        fixture.get(&token, None).status(),
        reqwest::StatusCode::NOT_FOUND
    );
    Ok(())
}

/// **Unpublish breaks a body partway with an error**, never a clean end
/// a receiver would read as the film being over.
#[test]
fn unpublish_breaks_a_body_partway() -> anyhow::Result<()> {
    let fixture = Fixture::start(
        Knobs {
            speed: Some(1.0),
            ..Knobs::default()
        },
        RenditionTuning {
            speed_window: Duration::from_secs(3600),
            idle_release: Duration::from_secs(3600),
        },
    )?;
    let token = fixture.publish(60_000, 0)?;
    let mut response = fixture.get(&token, None);
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut first = [0u8; 8];
    std::io::Read::read_exact(&mut response, &mut first)?;
    assert_eq!(&first[4..], b"ftyp");
    assert!(fixture.handle.unpublish(&token));
    let mut rest = Vec::new();
    let read = std::io::Read::read_to_end(&mut response, &mut rest);
    assert!(read.is_err(), "a clean end after {} bytes", rest.len());
    Ok(())
}

// --- Speed -----------------------------------------------------------------------

/// **A producer slower than real time fails the rendition with its
/// sentence**, and `rendition_state` says so; a range asked for then is
/// answered `503` with it.
#[test]
fn a_producer_slower_than_real_time_fails_the_rendition() -> anyhow::Result<()> {
    let fixture = Fixture::start(
        Knobs {
            speed: Some(0.5),
            ..Knobs::default()
        },
        RenditionTuning {
            speed_window: Duration::from_secs(1),
            ..RenditionTuning::default()
        },
    )?;
    let token = fixture.publish(60_000, 0)?;
    let header = fixture.header(&token);
    let mut sentence = String::new();
    until("the rendition fails", || {
        match fixture.handle.rendition_state(&token) {
            RenditionState::Failed { sentence: said } => {
                sentence = said;
                true
            }
            _ => false,
        }
    });
    assert!(
        sentence.starts_with(
            "This phone cannot repackage this film fast enough for the television: it made "
        ),
        "{sentence}"
    );
    assert!(sentence.contains(" seconds of film in 1"), "{sentence}");
    let response = fixture.get(&token, Some(format!("bytes={}-", header.slots[5].0)));
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = response.json()?;
    assert_eq!(body["refused"], "renditionFailed");
    assert_eq!(body["message"], sentence.as_str());
    let run = fixture.producer.runs()[0].clone();
    until("the producer is stopped", || run.stopped());
    Ok(())
}

/// **A rendition that fails partway breaks its body with an error**: the
/// file had begun (`200`), and a clean end would tell the receiver the
/// film is over.
#[test]
fn a_rendition_that_fails_partway_breaks_its_body() -> anyhow::Result<()> {
    let fixture = Fixture::start(
        Knobs {
            speed: Some(0.5),
            ..Knobs::default()
        },
        RenditionTuning {
            speed_window: Duration::from_secs(1),
            ..RenditionTuning::default()
        },
    )?;
    let token = fixture.publish(60_000, 0)?;
    let mut response = fixture.get(&token, None);
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut rest = Vec::new();
    let read = std::io::Read::read_to_end(&mut response, &mut rest);
    assert!(read.is_err(), "a clean end after {} bytes", rest.len());
    assert!(matches!(
        fixture.handle.rendition_state(&token),
        RenditionState::Failed { .. }
    ));
    Ok(())
}

/// **A producer that keeps up is not failed**, judged over the same
/// window: one a little faster than real time makes slot after slot with
/// the rendition producing throughout.
#[test]
fn a_producer_faster_than_real_time_is_not_failed() -> anyhow::Result<()> {
    let fixture = Fixture::start(
        Knobs {
            speed: Some(1.5),
            ..Knobs::default()
        },
        RenditionTuning {
            speed_window: Duration::from_secs(1),
            ..RenditionTuning::default()
        },
    )?;
    let token = fixture.publish(60_000, 0)?;
    fixture.init(&token);
    for n in 0..5 {
        fixture.segment(&token, n);
    }
    assert_eq!(
        fixture.handle.rendition_state(&token),
        RenditionState::Producing
    );
    Ok(())
}

/// The bytes of [`slow_origin`]'s entity at `offset`.
fn origin_byte(offset: usize) -> u8 {
    (offset % 251) as u8
}

const ORIGIN_LEN: usize = 16 << 20;

/// The slowness under test, not a wait for anything: every request to
/// [`slow_origin`] is answered this late.
const SOURCE_DELAY: Duration = Duration::from_millis(800);

/// An origin that answers ranges, each [`SOURCE_DELAY`] late: a slow
/// source.
fn slow_origin() -> anyhow::Result<SocketAddr> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let addr = listener.local_addr()?;
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            std::thread::spawn(move || serve_slowly(stream));
        }
    });
    Ok(addr)
}

fn serve_slowly(mut stream: TcpStream) {
    let Ok(second) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(second);
    let mut range = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("range: bytes=") {
            range = value.trim().split_once('-').map(|(first, last)| {
                (
                    first.parse::<usize>().unwrap_or(0),
                    last.parse::<usize>()
                        .unwrap_or(ORIGIN_LEN - 1)
                        .min(ORIGIN_LEN - 1),
                )
            });
        }
    }
    std::thread::sleep(SOURCE_DELAY);
    let (first, last) = range.unwrap_or((0, ORIGIN_LEN - 1));
    let head = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Type: video/x-matroska\r\nETag: \"slow\"\r\n\
         Accept-Ranges: bytes\r\nContent-Range: bytes {first}-{last}/{ORIGIN_LEN}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        last + 1 - first
    );
    if stream.write_all(head.as_bytes()).is_err() {
        return;
    }
    let mut at = first;
    while at <= last {
        let end = (at + 64 * 1024).min(last + 1);
        let chunk: Vec<u8> = (at..end).map(origin_byte).collect();
        if stream.write_all(&chunk).is_err() {
            return;
        }
        at = end;
    }
}

/// **A slow source is not a slow producer**: a producer that waits on its
/// reader for most of its wall time -- slower than real time on the clock
/// -- is not failed, because the reader's waits are not its busy time.
#[test]
fn a_slow_source_is_not_a_slow_producer() -> anyhow::Result<()> {
    let fixture = Fixture::start(
        Knobs {
            read_stride: Some(512 * 1024),
            ..Knobs::default()
        },
        RenditionTuning {
            speed_window: Duration::from_secs(1),
            ..RenditionTuning::default()
        },
    )?;
    let origin = slow_origin()?;
    let link = url::Url::parse(&format!(
        "http://{}/proxy/?d={}",
        fixture.handle.http_addr(),
        urlencoding::encode(&format!("http://{origin}/film.mkv"))
    ))?;
    let id = fixture.handle.register(MediaSpec::StreamingUrl(link))?;
    let token = fixture
        .handle
        .publish_rendition(&id, spec(60_000, T_MS, 0), None)?;
    let wall = Instant::now();
    fixture.init(&token);
    let started = Instant::now();
    for n in 0..4 {
        fixture.segment(&token, n);
    }
    let spent = started.elapsed();
    assert!(
        spent > Duration::from_secs(4),
        "the source made the producer slower than real time on the clock ({spent:?} for 4 s; \
         {:?} since the publish)",
        wall.elapsed()
    );
    assert_eq!(
        fixture.handle.rendition_state(&token),
        RenditionState::Producing
    );
    Ok(())
}

// --- Formats, failures, routes, refusals -------------------------------------------

/// **A later run in another format fails the rendition**: the init segment
/// describes the first run's, and a seek that comes back different cannot
/// be served under it.
#[test]
fn a_format_change_on_a_later_run_fails_the_rendition() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs {
        other_format_on_run: Some(1),
        ..Knobs::default()
    })?;
    let token = fixture.publish(60_000, 0)?;
    let header = fixture.header(&token);
    fixture.segment(&token, 0);
    // A receiver's seek: a range at slot 30, whose run comes back
    // different, answered before a byte of it is sent.
    let response = fixture.get(&token, Some(format!("bytes={}-", header.slots[30].0)));
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let refused = fixture.handle.rendition_segment(&token, 30);
    assert!(matches!(refused, Err(NotServed::Failed(_))), "{refused:?}");
    let RenditionState::Failed { sentence } = fixture.handle.rendition_state(&token) else {
        panic!("the rendition did not fail");
    };
    assert!(sentence.contains("different format"), "{sentence}");
    Ok(())
}

/// **A producer's `fail` is the rendition's**, with its sentence; the file
/// asked for then is answered `503` with it.
#[test]
fn a_producers_failure_fails_the_rendition_with_its_sentence() -> anyhow::Result<()> {
    let sentence = "This phone has no decoder for this film's sound.";
    let fixture = Fixture::quick(Knobs {
        fail_on_run: Some((0, sentence.to_string())),
        ..Knobs::default()
    })?;
    let token = fixture.publish(60_000, 0)?;
    fixture.handle.rendition_init(&token).ok();
    until("the rendition fails", || {
        fixture.handle.rendition_state(&token)
            == RenditionState::Failed {
                sentence: sentence.to_string(),
            }
    });
    let response = fixture.get(&token, None);
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = response.json()?;
    assert_eq!(body["message"], sentence);
    Ok(())
}

/// **A `HEAD` answers the file's length**, which the first run's formats
/// and index fix: it starts that run and waits for them, and ranges are
/// offered.
#[test]
fn a_head_answers_the_length() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs::default())?;
    let token = fixture.publish(60_000, 0)?;
    let response = reqwest::blocking::Client::new()
        .head(fixture.url(&token))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "video/mp4");
    assert_eq!(response.headers()["accept-ranges"], "bytes");
    let length: u64 = response.headers()["content-length"].to_str()?.parse()?;
    assert_eq!(Some(length), fixture.probe(&token).total);
    assert_eq!(fixture.probe(&token).runs_started, 1);
    Ok(())
}

/// **A token names what it was published as**: a plain publication has no
/// stream; a rendition's serves its file and the source as it is; there
/// is no HLS any more.
#[test]
fn a_plain_publication_has_no_stream() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs::default())?;
    let plain = fixture.handle.publish(&fixture.id, None)?;
    assert_eq!(
        fixture.get(&plain, None).status(),
        reqwest::StatusCode::NOT_FOUND
    );
    let token = fixture.publish(2_000, 0)?;
    assert_eq!(fixture.get(&token, None).status(), reqwest::StatusCode::OK);
    let as_is = reqwest::blocking::get(format!("{}/cast/{}", fixture.lan, token.as_str()))?;
    assert_eq!(as_is.status(), reqwest::StatusCode::OK);
    assert_eq!(as_is.bytes()?.len(), SOURCE_LEN);
    for file in ["hls/index.m3u8", "hls/init.mp4", "hls/0.m4s", "x.mp4"] {
        let response =
            reqwest::blocking::get(format!("{}/cast/{}/{file}", fixture.lan, token.as_str()))?;
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND, "{file}");
    }
    Ok(())
}

/// **No producer, no rendition**: `publish_rendition` is refused, saying
/// so, and nothing is published.
#[test]
fn publish_rendition_is_refused_without_a_producer() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let files = tempfile::tempdir()?;
    let handle = stream_server::start(ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        lan_media_addr: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
        ..offline_config()
    })?;
    handle.update_settings(serde_json::json!({ "lanMediaEnabled": true }))?;
    handle.set_lan_media(true)?;
    let path = files.path().join("film.mkv");
    std::fs::write(&path, b"film")?;
    let id = handle.register(MediaSpec::Local {
        file: LocalFile::Path(path),
        name: None,
    })?;
    let refused = handle
        .publish_rendition(&id, spec(60_000, T_MS, 0), None)
        .expect_err("no producer is installed");
    assert!(refused.to_string().contains("noProducer"), "{refused}");
    // The same call with a producer is published: it was the producer.
    handle.install_producer(TestProducer::new(Knobs::default()));
    let token = handle.publish_rendition(&id, spec(60_000, T_MS, 0), None)?;
    assert_eq!(handle.rendition_state(&token), RenditionState::Idle);
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// A by-hand check, not CI: with `RENDITION_DUMP=<dir>`, write the file of
/// a 30 s rendition as `stream.mp4`, for `ffprobe`.
#[test]
#[ignore = "writes a file for a by-hand ffprobe; run with RENDITION_DUMP=<dir>"]
fn dump_for_ffprobe() -> anyhow::Result<()> {
    let dir = std::path::PathBuf::from(std::env::var("RENDITION_DUMP")?);
    std::fs::create_dir_all(&dir)?;
    let fixture = Fixture::quick(Knobs {
        length: Duration::from_millis(30_000),
        ..Knobs::default()
    })?;
    let token = fixture.publish(30_000, 0)?;
    std::fs::write(dir.join("stream.mp4"), fixture.file(&token))?;
    Ok(())
}
