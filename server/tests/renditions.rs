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
    AudioPlan, CastToken, LocalFile, MediaId, MediaSpec, RenditionReadiness, RenditionSpec,
    RenditionState, ServerConfig, TrackKind, VideoPlan,
};

#[path = "support/torrent_fixtures.rs"]
mod torrent_fixtures;
use torrent_fixtures::offline_config;

#[path = "support/test_producer.rs"]
mod test_producer;
use test_producer::{ASC, Gate, IndexKnob, Knobs, PPS, SPS, TestProducer, frame_of};

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
        Self::start(knobs, tuning())
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
        // A second `sidx` (the sound's) follows when the first says so.
        let after = u64::from_be_bytes(sidx[28..36].try_into().unwrap());
        let len = ftyp + moov + 40 + 12 * count + after;
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

/// The tuning these tests run by: the lookahead in slots alone
/// ([`stream_server::rendition::LOOKAHEAD`]), as the cut rule's one-second
/// slots here make the reasoning about which run makes which slot plain;
/// the lookahead by time has tests of its own.
fn tuning() -> RenditionTuning {
    RenditionTuning {
        lookahead: Duration::ZERO,
        ..RenditionTuning::default()
    }
}

/// A ring that keeps only the slots a run is making: anything read again
/// is made again.
fn small_ring() -> RenditionTuning {
    RenditionTuning {
        ring_cap: 1,
        ..tuning()
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
    /// The sound's `sidx`, when there is one: its earliest time and each
    /// slot's duration.
    sound: Option<(u64, Vec<u32>)>,
    total: u64,
}

impl Header {
    /// From the file's first bytes (`head`), the file `total` long: the
    /// init segment, then the video's `sidx`, on 90 kHz -- a demuxer
    /// seeking the sound finds its slot by the video's times, the same slot
    /// -- naming the slots, which begin right after it, or after the
    /// sound's `sidx` when one follows (an estimated layout's: the same
    /// slots, labelled early), and end where the file does. Two references
    /// a slot: its first part (`mux::FIRST_PART`, 8 KiB), with the slot's
    /// duration and a SAP, and the rest, lasting nothing, with none.
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
        let after = u64::from_be_bytes(sidx[28..36].try_into().unwrap()) as usize;
        let refs = u16::from_be_bytes([sidx[38], sidx[39]]) as usize;
        assert_eq!(refs % 2, 0, "two references a slot");
        let count = refs / 2;
        let sound = (after > 0).then(|| {
            let at = init_len + sidx.len();
            let sound = &head[at..at + size(at)];
            assert_eq!(
                sound.len(),
                after,
                "the slots begin right after the sound's sidx"
            );
            assert_eq!(&sound[4..8], b"sidx");
            assert_eq!(u32_at(sound, 12), 2, "the sound's track");
            assert_eq!(u32_at(sound, 16), 90_000, "on the video's clock");
            assert_eq!(&sound[28..36], &[0; 8], "slots begin right after it");
            assert_eq!(u16::from_be_bytes([sound[38], sound[39]]) as usize, refs);
            let durations = (0..count)
                .map(|k| {
                    let at = 40 + 2 * k * 12;
                    for part in [at, at + 12] {
                        assert_eq!(u32_at(sound, part), u32_at(sidx, part), "slot {k}'s size");
                    }
                    assert_eq!(u32_at(sound, at + 12 + 4), 0, "slot {k}'s rest");
                    u32_at(sound, at + 4)
                })
                .collect();
            (
                u64::from_be_bytes(sound[20..28].try_into().unwrap()),
                durations,
            )
        });
        let mut offset = (init_len + sidx.len() + after) as u64;
        let slots = (0..count)
            .map(|k| {
                let at = 40 + 2 * k * 12;
                let rest = at + 12;
                assert_eq!(u32_at(sidx, at), FIRST_PART, "slot {k}'s first part");
                assert_eq!(u32_at(sidx, at + 8), 1 << 31, "slot {k} starts with a SAP");
                assert_eq!(u32_at(sidx, rest + 4), 0, "slot {k}'s rest lasts nothing");
                assert_eq!(u32_at(sidx, rest + 8), 0, "slot {k}'s rest has no SAP");
                let slot_size = u64::from(u32_at(sidx, at) + u32_at(sidx, rest));
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
            sound,
            total,
        }
    }

    /// Slot `n`'s bytes, out of the whole file.
    fn slot<'a>(&self, file: &'a [u8], n: usize) -> &'a [u8] {
        let (offset, size, _) = self.slots[n];
        &file[offset as usize..(offset + size) as usize]
    }
}

/// A slot's fragment: a `styp` unless the slot opens at its `sidx` label
/// ([`opens_at_label`]), the first chunk's `moof` and a `free` box to the
/// slot's first part's end (`FIRST_PART`), that chunk's `mdat`, a `moof`
/// and an `mdat` per later chunk, then a `free` box to the slot's end whose
/// last bytes are zeros.
fn fragment_of(slot: &[u8]) -> &[u8] {
    let boxes = Boxes::of(slot);
    let kinds: Vec<&str> = boxes.iter().map(|(kind, _)| kind.as_str()).collect();
    let styp = usize::from(kinds[0] == "styp");
    let last = kinds.len() - 1;
    assert_eq!(kinds[last], "free", "{kinds:?}");
    assert!(last > styp + 2, "{kinds:?}");
    assert_eq!(kinds[styp..styp + 3], ["moof", "free", "mdat"], "{kinds:?}");
    let first_part: usize = boxes[..styp + 2]
        .iter()
        .map(|(_, body)| body.len() + 8)
        .sum();
    assert_eq!(first_part, FIRST_PART as usize, "the first part");
    for pair in kinds[styp + 3..last].chunks(2) {
        assert_eq!(pair, ["moof", "mdat"], "{kinds:?}");
    }
    let free = boxes[last].1.len() + 8;
    assert!(
        free >= 24,
        "the padding is at least a header and the zero tail"
    );
    assert!(slot[slot.len() - 16..].iter().all(|byte| *byte == 0));
    &slot[..slot.len() - free]
}

/// Whether a slot opens with its `moof`, where the `sidx` points -- no
/// `styp` between. Only a slot whose first samples are at its label may:
/// FFmpeg up to 4.4 (the Chromecast with Google TV's Chrome 92) takes a
/// fragment's decode time from the label when the two are one offset, and
/// apart, every version parsed the `moof` twice after a seek to it, and a
/// later seek back landed at the end of what was read at the start.
fn opens_at_label(slot: &[u8]) -> bool {
    match &slot[4..8] {
        b"moof" => true,
        b"styp" => false,
        other => panic!("a slot opening with {other:?}"),
    }
}

/// One track's samples in a segment, as the boxes say them, every chunk's
/// run joined.
#[derive(Debug)]
struct Traf {
    track_id: u32,
    /// The first chunk's.
    tfdt: u64,
    /// `(duration, size, composition offset)` per sample.
    samples: Vec<(u32, u32, i32)>,
    /// Each sample's bytes, where its run's data offset says.
    data: Vec<Vec<u8>>,
}

struct Segment {
    /// The first `moof`'s.
    sequence: u32,
    trafs: Vec<Traf>,
    /// Per chunk -- a `moof` and its `mdat` -- the `(track, first decode
    /// time, last decode time)` of each run in it, in the order its bytes
    /// are in the `mdat`.
    chunks: Vec<Vec<(u32, u64, u64)>>,
}

/// A slot's fragment read back: every `moof` and `mdat` pair, each run's
/// samples taken from where its data offset points (from its own `moof`),
/// and checked to be the `mdat`'s bytes, front to back, with nothing
/// between; each chunk's `tfdt` where the track's chunk before it ended,
/// and the `mfhd` numbers rising by one.
fn parse_segment(bytes: &[u8]) -> Segment {
    let top = Boxes::of(bytes);
    let mut at = 0;
    let mut trafs: Vec<Traf> = Vec::new();
    let mut chunks = Vec::new();
    let mut sequences = Vec::new();
    let mut boxes = top.iter().peekable();
    if boxes.peek().is_some_and(|(kind, _)| kind == "styp") {
        at += 8 + boxes.next().unwrap().1.len();
    }
    while let Some((kind, moof)) = boxes.next() {
        assert_eq!(kind, "moof");
        let moof_at = at;
        let mut next = boxes.next().expect("an mdat after each moof");
        let mut padding = 0;
        if next.0 == "free" {
            padding = 8 + next.1.len();
            next = boxes.next().expect("an mdat after the first part");
        }
        let (mdat_kind, mdat) = next;
        assert_eq!(mdat_kind, "mdat");
        let mdat_at = moof_at + 8 + moof.len() + padding + 8;
        let mut expected_at = mdat_at;
        let mut chunk = Vec::new();
        for (kind, body) in Boxes::of(moof) {
            match kind.as_str() {
                "mfhd" => sequences.push(u32_at(body, 4)),
                "traf" => {
                    let tfhd = Boxes::find(body, "tfhd");
                    assert_eq!(
                        u32_at(tfhd, 0) & 0xff_ffff,
                        0x02_0000,
                        "default-base-is-moof"
                    );
                    let track_id = u32_at(tfhd, 4);
                    let tfdt = Boxes::find(body, "tfdt");
                    assert_eq!(tfdt[0], 1, "tfdt version 1");
                    let tfdt = u64::from_be_bytes(tfdt[4..12].try_into().unwrap());
                    let runs: Vec<&[u8]> = Boxes::of(body)
                        .into_iter()
                        .filter(|(kind, _)| kind == "trun")
                        .map(|(_, trun)| trun)
                        .collect();
                    assert_eq!(runs.len(), 1, "one trun per traf");
                    let trun = runs[0];
                    assert_eq!(trun[0], 1, "trun version 1");
                    let flags = u32_at(trun, 0) & 0xff_ffff;
                    let count = u32_at(trun, 4) as usize;
                    assert!(flags & 1 != 0, "a data offset");
                    let mut data_at = moof_at + u32_at(trun, 8) as usize;
                    assert_eq!(data_at, expected_at, "runs back to back in the mdat");
                    let mut field_at = 12;
                    let traf = match trafs.iter_mut().find(|traf| traf.track_id == track_id) {
                        Some(traf) => {
                            let ended = traf.tfdt
                                + traf
                                    .samples
                                    .iter()
                                    .map(|sample| u64::from(sample.0))
                                    .sum::<u64>();
                            assert_eq!(tfdt, ended, "track {track_id}'s chunks join up");
                            traf
                        }
                        None => {
                            trafs.push(Traf {
                                track_id,
                                tfdt,
                                samples: Vec::new(),
                                data: Vec::new(),
                            });
                            trafs.last_mut().unwrap()
                        }
                    };
                    let mut last = tfdt;
                    let mut time = tfdt;
                    for _ in 0..count {
                        let mut field = |bit: u32| {
                            (flags & bit != 0).then(|| {
                                let value = u32_at(trun, field_at);
                                field_at += 4;
                                value
                            })
                        };
                        let duration = field(0x100).expect("durations");
                        let size = field(0x200).expect("sizes");
                        let _flags = field(0x400);
                        let offset = field(0x800).map_or(0, |value| value as i32);
                        traf.samples.push((duration, size, offset));
                        last = time;
                        time += u64::from(duration);
                        traf.data
                            .push(bytes[data_at..data_at + size as usize].to_vec());
                        data_at += size as usize;
                    }
                    expected_at = data_at;
                    chunk.push((track_id, tfdt, last));
                }
                other => panic!("an unexpected {other} in the moof"),
            }
        }
        assert_eq!(
            expected_at,
            mdat_at + mdat.len(),
            "the mdat is its runs' bytes and nothing else"
        );
        chunks.push(chunk);
        at = mdat_at + mdat.len();
    }
    assert_eq!(at, bytes.len());
    for pair in sequences.windows(2) {
        assert_eq!(pair[1], pair[0] + 1, "the mfhd numbers rise by one");
    }
    Segment {
        sequence: sequences[0],
        trafs,
        chunks,
    }
}

/// The first `moof`'s `mfhd` number in slot `n`: 4096 numbers a slot.
fn first_sequence(n: u64) -> u32 {
    (n * 4096 + 1) as u32
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
        let mut out = Vec::new();
        for (&(duration, size, offset), data) in traf.samples.iter().zip(&traf.data) {
            // As FFmpeg shows it: its `dts_shift` is `D`, which the film's
            // first sample's offset sets (`mux.rs`).
            let pts_us = (dts + i64::from(offset) + AHEAD_TICKS) * 1_000_000 / 90_000;
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
        }
        out
    }

    fn audio_pts(&self) -> Vec<i64> {
        let Some(traf) = self.track(2) else {
            return Vec::new();
        };
        let mut ticks = traf.tfdt as i64;
        let mut out = Vec::new();
        for (&(duration, _, _), data) in traf.samples.iter().zip(&traf.data) {
            // The sound is on the video's 90 kHz clock (FFmpeg before 6.0
            // compares the two tracks' times unscaled).
            let frame = i64::from(u32_at(data, 0));
            let pts_us = Knobs::audio_pts(frame);
            assert_eq!(
                (pts_us * 90_000 + 500_000) / 1_000_000,
                ticks,
                "the bytes are the frame's"
            );
            out.push(pts_us);
            ticks += i64::from(duration);
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
/// from its cut's key to the next cut's, audio between the cuts, each the
/// audio lead (64 ms) early.
fn expected(knobs: &Knobs, n: i64) -> (Vec<i64>, Vec<i64>) {
    let (from, to) = (cut(knobs, n), cut(knobs, n + 1));
    let lead = |cut: i64| cut.saturating_sub(AUDIO_LEAD_US);
    let video = knobs
        .video_frames()
        .into_iter()
        .map(|(pts, _)| pts)
        .filter(|pts| *pts >= from && *pts < to)
        .collect();
    let audio = knobs
        .audio_frames()
        .into_iter()
        .filter(|pts| *pts >= lead(from) && *pts < lead(to))
        .collect();
    (video, audio)
}

/// How long a slot's first part is (`mux::FIRST_PART`).
const FIRST_PART: u32 = 8 * 1024;

/// How far a slot's decode times run ahead of its presentation times
/// (`D`, `mux::DECODE_AHEAD_US`): half a second, on the 90 kHz clock.
const AHEAD_TICKS: i64 = 45_000;

/// How far before a segment's cut its sound begins: `D` and 64 ms
/// (`run::AUDIO_LEAD_US`).
const AUDIO_LEAD_US: i64 = 564_000;

/// Where a slot whose sync sample is shown at `key_us` begins decoding:
/// `D` before it -- the film's first slot at it.
fn decodes_from(key_us: i64) -> u64 {
    let key = key_us * 90_000 / 1_000_000;
    if key_us == 0 {
        0
    } else {
        (key - AHEAD_TICKS) as u64
    }
}

/// The room a slot is given for its chunks' headers, for a segment
/// `duration_us` long: a segment touches at most one chunk per 500 ms and
/// one more, the first may be split in two, and each chunk past the first
/// is a `moof` (with its `mfhd`), two `traf`s (`tfhd`, `tfdt`, a `trun`
/// header) and an `mdat` header -- 160 bytes; and the first part's padding
/// (`FIRST_PART`).
fn chunk_room(duration_us: i64) -> u64 {
    (duration_us as u64 / 500_000 + 2) * 160 + u64::from(FIRST_PART)
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
/// run -- asked for the source's index -- starts at the film's start, what
/// a reader of the header reads on into, **whatever start the spec names**:
/// production follows requests, never where a player was told to start.
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
    assert_eq!(runs[0].from, Duration::ZERO, "not the spec's 7.5 s");
    assert!(runs[0].wanted_index);
    let probe = fixture.probe(&token);
    assert_eq!(probe.run_from, Some(0));
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
    // Every track on one clock, the video's: FFmpeg before 6.0 places the
    // sound by the video's `sidx` times without rescaling them.
    for (trak, timescale) in traks.iter().zip([90_000u64, 90_000]) {
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
/// last to the source's end) plus the headroom and the room for its
/// chunks' headers; durations from the cuts.
#[test]
fn the_sidx_mirrors_the_sources_index() -> anyhow::Result<()> {
    let knobs = Knobs::default();
    let fixture = Fixture::quick(knobs.clone())?;
    let token = fixture.publish(60_000, 0)?;
    let header = fixture.header(&token);
    assert_eq!(header.slots.len(), 60);
    // Mirrored, the video's labels are its sync samples: the sound needs
    // no index of its own (FFmpeg seeks it to the picture's sample, which
    // its own slot holds, with the sound's lead).
    assert!(header.sound.is_none(), "one sidx");
    let file = fixture.file(&token);
    for n in 0..header.slots.len() {
        assert!(
            opens_at_label(header.slot(&file, n)),
            "slot {n} opens with its moof"
        );
    }
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
        let next = if k == 59 {
            60_000_000
        } else {
            cut(&knobs, k + 1)
        };
        assert_eq!(
            *size,
            headroom(end - place(start)) + chunk_room(next - start),
            "slot {k}'s size"
        );
        // Labelled at its first decode time: a slot after the first `D`
        // before its cut.
        let label = |k: i64, at: i64| {
            if k == 0 {
                at
            } else {
                at - AHEAD_TICKS * 100 / 9
            }
        };
        let end = if k == 59 { next } else { label(k + 1, next) };
        assert_eq!(
            i64::from(*duration),
            (end - label(k, start)) * 9 / 100,
            "slot {k}'s duration"
        );
    }
    assert_eq!(header.earliest, 0);
    Ok(())
}

/// **Without an index the layout is estimated**: slots on the grid, in
/// proportion to time over the source, 15% larger and a base on top, each
/// labelled in the `sidx` a GOP after its cut.
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
        assert_eq!(
            *size,
            scaled(k + 1) - scaled(k) + 8 * 1024 + chunk_room(T_US),
            "slot {k}"
        );
        // A GOP late (10 s), at the film's end at the latest: the first
        // slot to the end, the rest none.
        assert_eq!(*duration, if k == 0 { 900_000 } else { 0 }, "slot {k}");
    }
    // The sound's labels are each cut less its lead (564 ms: `D` and 64 ms),
    // not a GOP late: the slot FFmpeg picks for the sound by the picture's
    // sync sample's decode time is then never one before the picture's.
    let (earliest, durations) = header.sound.as_ref().expect("the sound's sidx");
    assert_eq!(*earliest, 0);
    let lead = (AUDIO_LEAD_US * 9 / 100) as u32;
    for (k, duration) in durations.iter().enumerate() {
        let expected = match k {
            0 => 90_000 - lead,
            9 => 90_000 + lead,
            _ => 90_000,
        };
        assert_eq!(*duration, expected, "the sound's slot {k}");
    }
    let file = fixture.file(&token);
    for n in 0..10 {
        // Labelled a GOP late, a slot keeps its sidx reference and its
        // moof apart, or FFmpeg 4.4 would time it by the label.
        assert!(!opens_at_label(header.slot(&file, n)), "slot {n}");
        let segment = parse_segment(fragment_of(header.slot(&file, n)));
        assert_eq!(segment.sequence, first_sequence(n as u64));
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
        assert_eq!(segment.sequence, first_sequence(n as u64));
        let video = segment.track(1).expect("video in every segment here");
        let key = knobs.key_at_or_after(n * T_US).unwrap();
        assert_eq!(video.tfdt, decodes_from(key), "segment {n}'s tfdt");
        let (want_video, want_audio) = expected(&knobs, n);
        assert_eq!(segment.video_pts(&knobs), want_video, "segment {n}'s video");
        assert_eq!(segment.audio_pts(), want_audio, "segment {n}'s audio");
    }
    Ok(())
}

/// **A seek lands on the sync sample at or before its target**, as FFmpeg's
/// demuxer seeks -- modelled, so it holds on every CI job: the slot whose
/// `sidx` label is the last at or before the target, its first `moof` read,
/// and the last sync sample at or before the target among the samples that
/// `moof` holds (`mov_seek_fragment`, `av_index_search_timestamp`). A film
/// with a key every 2.4 s, published with six-second segments: cut on the
/// segment grid, a slot held two or three GOPs, its first `moof` only the
/// first half second of them, and a seek landed at the slot's start -- up
/// to a whole slot early. Mirrored, a slot is cut at every sync sample a
/// second or more apart, whatever the segment length, so it lands within
/// a GOP.
#[test]
fn a_modelled_seek_lands_within_a_gop_of_its_target() -> anyhow::Result<()> {
    let knobs = Knobs {
        gop: 60,
        ..Knobs::default()
    };
    let fixture = Fixture::quick(knobs.clone())?;
    let token = fixture
        .handle
        .publish_rendition(&fixture.id, spec(60_000, 6000, 0), None)?;
    let header = fixture.header(&token);
    let file = fixture.file(&token);
    let gop_us = 2_400_000;
    assert_eq!(header.slots.len(), 25, "a slot per key");
    let mut label = header.earliest;
    let labels: Vec<u64> = header
        .slots
        .iter()
        .map(|(_, _, duration)| {
            let at = label;
            label += u64::from(*duration);
            at
        })
        .collect();
    for target_ms in [1_000i64, 7_100, 11_000, 30_000, 33_500, 47_900, 59_000] {
        // FFmpeg seeks by the time less its `dts_shift`, `D`.
        let target = (target_ms * 90 - AHEAD_TICKS) as u64;
        let slot = labels.iter().rposition(|label| *label <= target).unwrap();
        let segment = parse_segment(fragment_of(header.slot(&file, slot)));
        let (track, first, _) = segment.chunks[0][0];
        assert_eq!(track, 1, "the first moof begins with the picture");
        // The sync sample's decode time is `D` before it is shown -- the
        // film's first slot's at it.
        let ahead = if slot == 0 { 0 } else { AHEAD_TICKS };
        let landed_us = (first as i64 + ahead) * 1000 / 90;
        let frame = landed_us * i64::from(knobs.fps) / 1_000_000;
        assert!(
            knobs.is_key(frame),
            "{target_ms} ms: landed on a frame that is no key"
        );
        let target_us = target_ms * 1000;
        assert!(
            landed_us <= target_us && target_us - landed_us < gop_us,
            "{target_ms} ms landed at {landed_us} us, in slot {slot}"
        );
    }
    Ok(())
}

/// **After a seek FFmpeg reads on through the slot it landed in** --
/// modelled, so it holds on every CI job. Seeking, it reads the slot's
/// first `moof` and then, seeking each other track, goes on from the
/// fragment after that one in its index (`mov_seek_fragment`:
/// `next_root_atom = frag_index.item[index + 1].moof_offset`): every slot
/// has a second `sidx` reference, at the first chunk's `mdat`, so that
/// fragment is the slot's own rest -- its first chunk's samples and every
/// later chunk -- and not the next slot. With a slot one reference, the
/// fragment after its first `moof` was the next slot, and every seek
/// skipped the rest of the slot it landed in (2.4 s of a film with a key
/// every 2.8 s, in FFmpeg 4.4, 6.1 and master alike).
#[test]
fn after_a_seek_ffmpeg_reads_on_through_the_slot() -> anyhow::Result<()> {
    let knobs = Knobs {
        gop: 60,
        ..Knobs::default()
    };
    let fixture = Fixture::quick(knobs.clone())?;
    let token = fixture
        .handle
        .publish_rendition(&fixture.id, spec(60_000, 6000, 0), None)?;
    let file = fixture.file(&token);
    let header = Header::of(&file, file.len() as u64);
    let sidx = &file[header.init_len..];
    for (k, (offset, size, _)) in header.slots.iter().enumerate() {
        // The reference after the slot's first: where FFmpeg reads on.
        let at = 40 + (2 * k + 1) * 12;
        let first = u64::from(u32_at(sidx, at - 12));
        let rest = offset + first;
        assert!(rest < offset + size, "slot {k}'s rest is inside it");
        assert_eq!(
            &file[rest as usize + 4..rest as usize + 8],
            b"mdat",
            "slot {k}"
        );
        // From there on, in the file's order, every sample of the slot.
        let slot = header.slot(&file, k);
        let segment = parse_segment(fragment_of(slot));
        let first_moof = Boxes::of(slot)
            .iter()
            .position(|(kind, _)| kind == "moof")
            .unwrap();
        assert_eq!(first_moof, 0, "slot {k} opens at its label");
        assert!(segment.chunks.len() >= 4, "slot {k}: 2.4 s, chunks");
        assert_eq!(
            segment.track(1).unwrap().samples.len(),
            knobs.gop as usize,
            "slot {k}: the whole GOP"
        );
    }
    Ok(())
}

/// **A slot lays its picture and its sound down together**, half a second
/// at a time: a `moof` and an `mdat` per chunk, the chunk's picture --
/// decode times in one half second of the film's clock -- and then the
/// sound of the same half second; the sound's lead (64 ms before the cut)
/// goes with the first picture. A reader going straight through finds the
/// next sample of either track in the next bytes: FFmpeg's demuxer takes
/// samples by byte position while the tracks are within a second of each
/// other, and seeks for each one when they are not -- a request each, on
/// zond's TV, every half second of an 8 Mbit/s film laid picture-first.
#[test]
fn a_slot_lays_its_picture_and_sound_down_together() -> anyhow::Result<()> {
    let knobs = Knobs::default();
    let fixture = Fixture::quick(knobs.clone())?;
    let token = fixture.publish(30_000, 0)?;
    fixture.init(&token);
    let half = |ticks: u64| ticks / 45_000;
    // Slot 12 is cut at 12 s exactly (a key every 0.48 s): its sound's lead
    // is in the half second before, and goes with the picture all the same.
    for n in (0..6u64).chain([12]) {
        let segment = parse_segment(&fixture.segment(&token, n));
        let (video, audio) = expected(&knobs, n as i64);
        assert_eq!(segment.video_pts(&knobs), video, "slot {n}'s video");
        assert_eq!(segment.audio_pts(), audio, "slot {n}'s audio");
        assert!(
            segment.chunks.len() >= 2,
            "slot {n}: one second, two chunks"
        );
        let count = segment.chunks.len();
        let mut previous = None;
        for (at, chunk) in segment.chunks.iter().enumerate() {
            let [(1, first, last), (2, sound_first, sound_last)] = chunk[..] else {
                panic!("slot {n}, chunk {at}: picture then sound, {chunk:?}");
            };
            let cell = half(first);
            assert_eq!(half(last), cell, "slot {n}, chunk {at}: one half second");
            assert!(previous < Some(cell), "slot {n}: chunks in time order");
            previous = Some(cell);
            // The lead goes with the first picture, the sound after the
            // last picture with the last.
            if at > 0 {
                assert_eq!(half(sound_first), cell, "slot {n}, chunk {at}'s sound");
            } else {
                assert!(
                    sound_first <= first && first - sound_first <= 64 * 90,
                    "slot {n}: the sound's lead goes with the first picture"
                );
            }
            if at + 1 < count {
                assert_eq!(half(sound_last), cell, "slot {n}, chunk {at}'s sound");
            }
        }
    }
    Ok(())
}

/// **What precedes the cut is discarded**: a run for segment 6, which the
/// producer starts at the key before its cut less two seconds, puts
/// nothing before that key in segment 6, and no audio before it.
///
/// Segment 6, not 5: the first run, started for the header, pauses with
/// slot 3 next (two past slot 0), and a request within two slots of a
/// run's next one joins it -- slot 5 sometimes did, and no second run
/// started.
#[test]
fn a_run_discards_what_precedes_its_cut() -> anyhow::Result<()> {
    const N: u64 = 6;
    let knobs = Knobs::default();
    let fixture = Fixture::quick(knobs.clone())?;
    let token = fixture.publish(30_000, 0)?;
    fixture.init(&token);
    let segment = parse_segment(&fixture.segment(&token, N));
    let key = knobs.key_at_or_after(N as i64 * T_US).unwrap();
    let runs = fixture.producer.runs();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[1].from, asked_from(key));
    assert!(!runs[1].wanted_index, "the layout is frozen already");
    let emitted = runs[1].emitted();
    assert!(
        emitted
            .iter()
            .any(|(track, pts)| *track == TrackKind::Video && *pts < N as i64 * T_US),
        "the producer did hand over video before the cut: {emitted:?}"
    );
    assert!(
        emitted
            .iter()
            .any(|(track, pts)| *track == TrackKind::Audio && *pts < N as i64 * T_US),
        "and audio"
    );
    let (want_video, want_audio) = expected(&knobs, N as i64);
    assert!(key > N as i64 * T_US, "the cut is a key after N x T");
    assert_eq!(want_video.first(), Some(&key));
    assert_eq!(segment.video_pts(&knobs), want_video);
    assert_eq!(segment.audio_pts(), want_audio);
    assert_eq!(segment.track(1).unwrap().tfdt, decodes_from(key));
    Ok(())
}

/// **Every range is the same bytes** as the whole file's, however it is
/// asked for: a far range first (a seek, made by a run started there),
/// then overlapping ranges across slot boundaries, then the whole file
/// read in order by another run.
#[test]
fn every_range_is_the_same_bytes_as_the_whole() -> anyhow::Result<()> {
    // A ring that keeps only what a run is making, so the ranges below are
    // made by different runs.
    let fixture = Fixture::start(
        Knobs {
            length: Duration::from_millis(12_000),
            ..Knobs::default()
        },
        small_ring(),
    )?;
    let token = fixture.publish(12_000, 0)?;
    let header = fixture.header(&token);
    let far = header.slots[9].0 + 100;
    let far_bytes = fixture.range(&token, far, far + 5_000);
    let middle = header.slots[3].0 - 7;
    let first = fixture.range(&token, middle - 2_000, middle + 2_000);
    let second = fixture.range(&token, middle, middle + 30_000);
    // The whole file, as a receiver that chose where to read reads it.
    let header_len = header.slots[0].0;
    let mut file = fixture.range(&token, 0, header_len - 1);
    file.extend(fixture.range(&token, header_len, header.total - 1));
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

/// **A receiver gets what it asks for, from where it was told to start**:
/// its opening read from the header on runs into slot 0 (Chrome's FFmpeg
/// demuxer probes the first fragment before it seeks to `currentTime`),
/// and slot 0 is made for it; its jump to the slot for its start is made
/// for it too -- neither waiting on an idle release. On zond's TV the
/// opening read waited for a run nobody would start, at 10704 bytes, until
/// the receiver gave up.
#[test]
fn a_receiver_told_to_start_late_gets_slot_0_and_its_start() -> anyhow::Result<()> {
    let knobs = Knobs::default();
    let fixture = Fixture::start(
        knobs.clone(),
        RenditionTuning {
            // No release can stand in for a run the reads need.
            idle_release: Duration::from_secs(3600),
            ..tuning()
        },
    )?;
    let token = fixture.publish(60_000, 30_500)?;
    let header = fixture.header(&token);
    let mut opening = fixture.get(&token, Some("bytes=0-".to_string()));
    assert_eq!(opening.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let mut head = vec![0u8; header.slots[1].0 as usize];
    std::io::Read::read_exact(&mut opening, &mut head)?;
    assert_eq!(&head[4..8], b"ftyp");
    let fragment = fixture.segment(&token, 0);
    let slot0 = &head[header.slots[0].0 as usize..];
    assert_eq!(&slot0[..fragment.len()], &fragment[..], "slot 0");

    let start = slot_holding(&knobs, 30_500_000);
    let at = header.slots[start as usize].0;
    let target = fixture.range(&token, at, at + 1000);
    assert_eq!(&target[4..8], b"moof");
    // The run from the start, which the open body may have read on far
    // into, and one for the jump at most: no third, and no release.
    assert!(fixture.probe(&token).runs_started <= 2);
    drop(opening);
    Ok(())
}

/// The release period of [`a_body_waiting_on_a_stalled_slot_keeps_its_run_and_gets_it`],
/// and the window its "never let go" is measured over: ten of them.
const WAITED_RELEASE: Duration = Duration::from_millis(100);
const WAITED_WINDOW: Duration = Duration::from_secs(1);

/// **A body waiting on a slot its source has not given yet keeps its run**,
/// however short the release period, and gets the slot when the source
/// does: no release ends it, cleanly or otherwise.
#[test]
fn a_body_waiting_on_a_stalled_slot_keeps_its_run_and_gets_it() -> anyhow::Result<()> {
    let gate = Gate::new();
    let fixture = Fixture::start(
        Knobs {
            start_gate: Some(gate.clone()),
            ..Knobs::default()
        },
        RenditionTuning {
            idle_release: WAITED_RELEASE,
            ..tuning()
        },
    )?;
    let token = fixture.publish(60_000, 0)?;
    // The body is the run's first request: it waits for the layout, then
    // for slot 0, and is counted among the run's waiters throughout.
    let mut body = fixture.get(&token, Some("bytes=0-".to_string()));
    until("the source stalls under slot 0", || gate.is_parked());
    std::thread::sleep(WAITED_WINDOW);
    assert_eq!(fixture.probe(&token).runs_started, 1, "never let go");
    assert!(!fixture.producer.runs()[0].probe.is_stopped());
    let header = fixture.header(&token);
    gate.open();
    let mut head = vec![0u8; header.slots[1].0 as usize];
    std::io::Read::read_exact(&mut body, &mut head)?;
    let fragment = fixture.segment(&token, 0);
    let slot0 = &head[header.slots[0].0 as usize..];
    assert_eq!(&slot0[..fragment.len()], &fragment[..]);
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
            ..tuning()
        },
    )?;
    let token = fixture.publish(60_000, 0)?;
    let header = fixture.header(&token);
    let mut left_open = fixture.get(&token, Some(format!("bytes={}-", header.slots[2].0)));
    let mut first = [0u8; 8];
    std::io::Read::read_exact(&mut left_open, &mut first)?;
    assert_eq!(&first[4..], b"moof");
    let (at, size, _) = header.slots[40];
    let far = fixture.range(&token, at, at + size - 1);
    assert_eq!(&far[4..8], b"moof");
    let probe = fixture.probe(&token);
    assert!(
        probe.runs_started <= 3,
        "{} runs: the reads took the run from each other",
        probe.runs_started
    );
    drop(left_open);
    Ok(())
}

/// **Three seeks waiting at once take turns**: with both runs taken, a
/// seek takes one only on its first look, so the three are made one after
/// another -- not by runs taken from each other while they wait.
#[test]
fn three_seeks_waiting_at_once_take_turns() -> anyhow::Result<()> {
    let fixture = Fixture::start(
        Knobs {
            speed: Some(4.0),
            ..Knobs::default()
        },
        RenditionTuning {
            idle_release: Duration::from_millis(300),
            speed_window: Duration::from_secs(3600),
            ..tuning()
        },
    )?;
    let token = fixture.publish(60_000, 0)?;
    let header = fixture.header(&token);
    let (fixture, token, header) = (&fixture, &token, &header);
    std::thread::scope(|scope| {
        let reads: Vec<_> = [10usize, 30, 50]
            .map(|n| {
                scope.spawn(move || {
                    let (at, size, _) = header.slots[n];
                    fixture.range(token, at, at + size - 1)
                })
            })
            .into_iter()
            .collect();
        for read in reads {
            assert_eq!(&read.join().expect("a read")[4..8], b"moof");
        }
    });
    let runs = fixture.probe(token).runs_started;
    assert!(
        runs <= 6,
        "{runs} runs: the seeks took the runs from each other"
    );
    Ok(())
}

/// **Two readers reading on far apart each keep a run**: the receiver's
/// read at its seek's target and another read walking forward from
/// earlier (zond's TV, 2026-10-02: a seek to 1:00 and a read on from 0:30
/// at once). Both read six slots to the end, in turns, and the runs are
/// the first and one each -- neither takes the other's.
#[test]
fn two_readers_reading_on_far_apart_each_keep_a_run() -> anyhow::Result<()> {
    let fixture = Fixture::start(
        Knobs {
            speed: Some(8.0),
            ..Knobs::default()
        },
        RenditionTuning {
            idle_release: Duration::from_secs(3600),
            ..tuning()
        },
    )?;
    let token = fixture.publish(60_000, 0)?;
    let header = fixture.header(&token);
    let span = |from: usize| {
        let to = header.slots[from + 5];
        (header.slots[from].0, to.0 + to.1 - 1)
    };
    let (fixture, token) = (&fixture, &token);
    let bodies: Vec<Vec<u8>> = std::thread::scope(|scope| {
        [10usize, 40]
            .map(|from| {
                let (start, end) = span(from);
                scope.spawn(move || fixture.range(token, start, end))
            })
            .into_iter()
            .map(|read| read.join().expect("a reader"))
            .collect()
    });
    for (body, from) in bodies.iter().zip([10usize, 40]) {
        let (start, end) = span(from);
        assert_eq!(body.len() as u64, end + 1 - start);
    }
    let probe = fixture.probe(token);
    assert!(
        probe.runs_started <= 3,
        "{} runs for two readers: they took the runs from each other",
        probe.runs_started
    );
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
            ..tuning()
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
    assert_eq!(&far[4..8], b"moof");
    assert_eq!(&near[4..8], b"moof");
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
    let fixture = Fixture::start_with(knobs.clone(), small_ring(), SQUEEZED_LEN)?;
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
    // So it opens before its label, and keeps a styp; its neighbours open
    // at theirs.
    assert!(!opens_at_label(header.slot(&file, 11)));
    assert!(opens_at_label(header.slot(&file, 10)));
    assert!(opens_at_label(header.slot(&file, 12)));

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
    let fixture = Fixture::start_with(knobs.clone(), tuning(), SQUEEZED_LEN)?;
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

/// **The last slot cannot spill**: there is no slot after it, so a last
/// segment that overflows keeps what fits and drops the rest, and the
/// file still ends where its length says.
#[test]
fn an_overflowing_last_slot_is_truncated() -> anyhow::Result<()> {
    let knobs = Knobs {
        index: Some(IndexKnob::Squeezed { at_us: 19 * T_US }),
        ..squeezed()
    };
    let fixture = Fixture::start_with(knobs.clone(), tuning(), SQUEEZED_LEN)?;
    let token = fixture.publish(20_000, 0)?;
    let file = fixture.file(&token);
    let header = Header::of(&file, file.len() as u64);
    let last = header.slots.len() - 1;
    let probe = fixture.probe(&token);
    assert_eq!(probe.truncated, vec![last as u64]);
    assert!(probe.spilled.is_empty());
    parse_segment(fragment_of(header.slot(&file, last)));
    Ok(())
}

// --- Seeks, joins, the lookahead, the idle release -------------------------------

/// **A request far ahead is a seek, beside the run that is there**: slot
/// 40 after 0-3 starts a second run at 40 and leaves the first; a third
/// place, with both runs live, takes the least recently asked -- the
/// first, whose sink answers `Stopped`.
#[test]
fn a_far_request_starts_a_run_and_a_third_takes_the_least_recently_asked() -> anyhow::Result<()> {
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
    assert_eq!((probe.runs_started, probe.live_runs), (2, 2));
    assert_eq!(probe.run_from, Some(40));
    assert_eq!(segment.track(1).unwrap().tfdt, decodes_from(key));
    assert!(!runs[0].stopped(), "the first run is left where it is");

    fixture.segment(&token, 20);
    until("the first run's sink answers Stopped", || runs[0].stopped());
    assert!(!runs[1].stopped(), "the run asked last is kept");
    assert_eq!(fixture.probe(&token).live_runs, 2);
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

/// **The lookahead is 12 s of film, never fewer than two slots**: with the
/// cut rule's one-second slots, a run asked for slot 0 makes every slot
/// that begins within 12 s of it -- slot 12, cut at 12.0 s, the last -- and
/// blocks before slot 13; asked for slot 1 (cut at 1.44 s), it makes on to
/// the last slot beginning by 13.44 s. Counted in slots (two), a slot per
/// sync sample a second or two long would run two or four seconds ahead
/// of the receiver, where six-second segments ran twelve.
#[test]
fn the_lookahead_is_twelve_seconds_of_film() -> anyhow::Result<()> {
    let knobs = Knobs::default();
    let fixture = Fixture::start(knobs.clone(), RenditionTuning::default())?;
    let token = fixture.publish(60_000, 0)?;
    fixture.init(&token);
    let run = fixture.producer.runs()[0].clone();
    let last_within = |from_us: i64| {
        (1..60)
            .take_while(|n| cut(&knobs, *n) <= from_us + 12_000_000)
            .last()
            .unwrap() as u64
    };
    let held = |last: u64| {
        let probe = fixture.probe(&token);
        probe.ring == (0..=last).collect::<Vec<_>>()
            && probe.in_production == Some(last + 1)
            && run.probe.is_blocked()
    };
    let from_zero = last_within(0);
    assert_eq!(from_zero, 12);
    until("the producer blocks 12 s ahead of slot 0", || {
        held(from_zero)
    });
    fixture.segment(&token, 1);
    let from_one = last_within(cut(&knobs, 1));
    assert!(from_one > from_zero, "{from_one}");
    until("the producer blocks 12 s ahead of slot 1", || {
        held(from_one)
    });
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
            ..tuning()
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
    assert_eq!(segment.sequence, first_sequence(3));
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
            ..tuning()
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
            ..tuning()
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
            ..tuning()
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
            ..tuning()
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
            ..tuning()
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
            ..tuning()
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

// --- Preparing before the receiver is told to load -------------------------------

/// The slot holding `at_us`, by `knobs`' cuts: what a receiver told to start
/// there asks for first from a mirrored layout.
fn slot_holding(knobs: &Knobs, at_us: i64) -> u64 {
    (1..).find(|n| cut(knobs, *n) > at_us).unwrap() as u64 - 1
}

/// The slots a preparation for a start at `start_us` makes in `knobs`'
/// 60 s film, besides slot 0: those of the 12 s before it to the 6 s after
/// it -- the film's ends permitting.
fn prepared(knobs: &Knobs, start_us: i64) -> Vec<u64> {
    let first = slot_holding(knobs, (start_us - 12_000_000).max(0));
    let last = slot_holding(knobs, (start_us + 6_000_000).min(59_999_999));
    (first..=last).collect()
}

/// **A preparation makes the header, slot 0 and the receiver's start with
/// the slots of the 12 s before it and the 6 s after, with no request at
/// all**: the receiver's first range after the load has been for the
/// start's slot, the one before it (FFmpeg seeks a time on a cut to the
/// slot before: `docs/design/renditions.md`, *Prepared before the load*)
/// and one some 12 s earlier (zond's TV), so whichever it asks is ready.
#[test]
fn preparing_makes_the_receivers_start_and_its_neighbours() -> anyhow::Result<()> {
    let knobs = Knobs::default();
    let fixture = Fixture::quick(knobs.clone())?;
    let token = fixture.publish(60_000, 20_500)?;
    assert_eq!(
        fixture.handle.rendition_readiness(&token),
        RenditionReadiness::Index
    );
    assert_eq!(fixture.probe(&token).runs_started, 0, "nothing until asked");

    assert!(fixture.handle.prepare_rendition(&token));
    until("the rendition is ready", || {
        fixture.handle.rendition_readiness(&token) == RenditionReadiness::Ready
    });
    let start = slot_holding(&knobs, 20_500_000);
    let probe = fixture.probe(&token);
    assert!(probe.init, "the layout, and with it the header, is made");
    let slots = prepared(&knobs, 20_500_000);
    assert!(slots.contains(&start) && slots.len() > 12, "{slots:?}");
    for slot in std::iter::once(0).chain(slots.iter().copied()) {
        assert!(
            probe.ring.contains(&slot),
            "slot {slot} in {:?}",
            probe.ring
        );
    }
    assert_eq!(
        probe.runs_started, 2,
        "the header's from the start, and one from 12 s before the start"
    );
    assert_eq!(
        fixture.producer.runs()[1].from,
        asked_from(cut(&knobs, slots[0] as i64))
    );
    assert_eq!(fixture.handle.lan_media_requests_served(), 0);
    assert!(fixture.handle.prepare_rendition(&token), "a second ask");
    Ok(())
}

/// **Ready waits for the 6 s after the start too**: with the source
/// stalled where the last of their slots ends, every slot to the one
/// before it is made and the preparation says `start`, not `ready`, for as
/// long as the stall lasts; once the source goes on, `ready`, with that
/// last slot in the ring.
#[test]
fn ready_waits_for_the_film_after_the_start() -> anyhow::Result<()> {
    let knobs = Knobs::default();
    let start = slot_holding(&knobs, 20_500_000);
    let last = *prepared(&knobs, 20_500_000).last().unwrap();
    assert!(
        last >= start + 5,
        "6 s of one-second slots after {start}: {last}"
    );
    let gate = Gate::new();
    let fixture = Fixture::quick(Knobs {
        time_gate: Some((cut(&knobs, last as i64 + 1), gate.clone())),
        ..knobs.clone()
    })?;
    let token = fixture.publish(60_000, 20_500)?;
    assert!(fixture.handle.prepare_rendition(&token));
    until("the slot before the last is made", || {
        fixture.probe(&token).ring.contains(&(last - 1))
    });
    until("the source stalls", || gate.is_parked());
    std::thread::sleep(STALL_WINDOW);
    assert_eq!(
        fixture.handle.rendition_readiness(&token),
        RenditionReadiness::Start,
        "ready without the film after the start"
    );
    gate.open();
    until("the rendition is ready", || {
        fixture.handle.rendition_readiness(&token) == RenditionReadiness::Ready
    });
    assert!(fixture.probe(&token).ring.contains(&last));
    Ok(())
}

/// **The receiver's first range for any prepared slot is answered from the
/// ring, and the run that made them goes on**: no run is started for it --
/// the run that made the slots, ahead of it, is moved back to it, not
/// restarted -- and the receiver reading on into the slots after is
/// answered by the same run.
#[test]
fn a_first_request_for_any_prepared_slot_starts_no_run() -> anyhow::Result<()> {
    let knobs = Knobs::default();
    for first in prepared(&knobs, 20_500_000) {
        let fixture = Fixture::quick(knobs.clone())?;
        let token = fixture.publish(60_000, 20_500)?;
        assert!(fixture.handle.prepare_rendition(&token));
        until("the rendition is ready", || {
            fixture.handle.rendition_readiness(&token) == RenditionReadiness::Ready
        });
        let prepared_runs = fixture.probe(&token).runs_started;

        // The receiver: its header, its opening read through slot 0, then
        // a range from slot `first` on through three slots.
        let header = fixture.header(&token);
        fixture.range(&token, 0, header.slots[1].0 - 1);
        let from = header.slots[first as usize];
        let to = header.slots[first as usize + 2];
        let bytes = fixture.range(&token, from.0, to.0 + to.1 - 1);
        let fragment = fixture.segment(&token, first);
        assert_eq!(&bytes[..fragment.len()], &fragment[..], "slot {first}");
        let probe = fixture.probe(&token);
        assert_eq!(
            probe.runs_started, prepared_runs,
            "slot {first}: answered from what the preparation made"
        );
        let runs = fixture.producer.runs();
        assert!(
            !runs[1].stopped(),
            "slot {first}: the preparation's run goes on"
        );
    }
    Ok(())
}

/// **Preparing at the film's ends stays inside it**: a start in slot 0
/// makes the slots of the first 6 s, all from the header's run; one in the
/// last slot makes those of the last 12 s.
#[test]
fn preparing_at_the_films_ends_stays_inside_it() -> anyhow::Result<()> {
    let knobs = Knobs::default();
    for (start_ms, slots) in [
        (0u32, (0..=5).collect::<Vec<u64>>()),
        (59_500, (47..=59).collect()),
    ] {
        let fixture = Fixture::quick(knobs.clone())?;
        let token = fixture.publish(60_000, u64::from(start_ms))?;
        assert!(fixture.handle.prepare_rendition(&token));
        until("the rendition is ready", || {
            fixture.handle.rendition_readiness(&token) == RenditionReadiness::Ready
        });
        let probe = fixture.probe(&token);
        assert_eq!(prepared(&knobs, i64::from(start_ms) * 1000), slots);
        for slot in &slots {
            assert!(
                probe.ring.contains(slot),
                "{start_ms}: slot {slot} in {:?}",
                probe.ring
            );
        }
        assert_eq!(
            probe.runs_started,
            if start_ms == 0 { 1 } else { 2 },
            "{start_ms}: runs"
        );
    }
    Ok(())
}

/// **A preparation for an estimated layout makes the slot the receiver's
/// demuxer picks by its late label**, which is before the slot holding the
/// time: that is the one its first range asks for.
#[test]
fn preparing_an_estimated_layout_makes_the_slot_its_label_picks() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs {
        index: None,
        ..Knobs::default()
    })?;
    let token = fixture.publish(60_000, 7_500)?;
    assert!(fixture.handle.prepare_rendition(&token));
    until("the rendition is ready", || {
        fixture.handle.rendition_readiness(&token) == RenditionReadiness::Ready
    });
    let probe = fixture.probe(&token);
    assert_eq!(probe.exact, Some(false));
    // Labelled 10 s after its cut, no slot but the first is labelled at or
    // before 7.5 s.
    assert!(probe.ring.contains(&0), "slot 0 in {:?}", probe.ring);
    Ok(())
}

/// The window over which "a stalled preparation is not given up on" is
/// measured: ten release periods of the tuning below, in which a run
/// nobody counted as waiting would have been let go ten times.
const STALL_WINDOW: Duration = Duration::from_millis(300);

/// **A preparation waits on a stalled source without giving up, and says
/// how far it has got**: `index` while the source's index does not come --
/// its run is not let go however short the release period -- `start` once
/// the layout is fixed and the slots are not made, `ready` once they are.
#[test]
fn preparing_waits_on_a_stalled_source_and_reports_its_phase() -> anyhow::Result<()> {
    let index_gate = Gate::new();
    let start_gate = Gate::new();
    let fixture = Fixture::start(
        Knobs {
            index_gate: Some(index_gate.clone()),
            start_gate: Some(start_gate.clone()),
            ..Knobs::default()
        },
        RenditionTuning {
            idle_release: Duration::from_millis(30),
            ..tuning()
        },
    )?;
    // Far enough in that the preparation's run is not the header's.
    let token = fixture.publish(60_000, 30_000)?;
    assert!(fixture.handle.prepare_rendition(&token));
    until("the run waits on the source's index", || {
        index_gate.is_parked()
    });
    assert_eq!(
        fixture.handle.rendition_readiness(&token),
        RenditionReadiness::Index
    );
    std::thread::sleep(STALL_WINDOW);
    assert!(
        index_gate.is_parked(),
        "the run waiting on the index was let go"
    );
    assert_eq!(fixture.probe(&token).runs_started, 1);
    assert!(!fixture.producer.runs()[0].probe.is_stopped());
    assert_eq!(
        fixture.handle.rendition_readiness(&token),
        RenditionReadiness::Index
    );

    index_gate.open();
    until("the run waits after its first sample", || {
        start_gate.is_parked()
    });
    until("the layout is fixed", || {
        fixture.handle.rendition_readiness(&token) == RenditionReadiness::Start
    });
    start_gate.open();
    until("the rendition is ready", || {
        fixture.handle.rendition_readiness(&token) == RenditionReadiness::Ready
    });
    assert_eq!(
        fixture.probe(&token).runs_started,
        2,
        "the header's run and the jump's"
    );
    Ok(())
}

/// **Unpublish ends a preparation waiting on a stalled source**: the wait
/// goes, the producer is told to stop, and the readiness is `ended`.
#[test]
fn unpublish_ends_a_waiting_preparation() -> anyhow::Result<()> {
    let gate = Gate::new();
    let fixture = Fixture::quick(Knobs {
        index_gate: Some(gate.clone()),
        ..Knobs::default()
    })?;
    let token = fixture.publish(60_000, 0)?;
    assert!(fixture.handle.prepare_rendition(&token));
    until("the run waits on the source", || gate.is_parked());
    assert_eq!(fixture.handle.renditions_preparing(), 1);

    assert!(fixture.handle.unpublish(&token));
    until("the preparation ends", || {
        fixture.handle.renditions_preparing() == 0
    });
    let run = fixture.producer.runs()[0].clone();
    until("the producer is stopped", || run.done());
    assert!(run.stopped());
    assert_eq!(
        fixture.handle.rendition_readiness(&token),
        RenditionReadiness::Ended
    );
    assert!(!fixture.handle.prepare_rendition(&token), "not published");
    Ok(())
}

/// **Unpublish during a preparation frees everything**: with slot 0 made
/// and the run for the start's neighbours stalled in its source, the
/// preparation ends, every run's producer is told to stop and stops, and
/// the rendition -- its ring with it -- is gone.
#[test]
fn unpublish_during_a_preparation_frees_everything() -> anyhow::Result<()> {
    let gate = Gate::new();
    let fixture = Fixture::quick(Knobs {
        run_gate: Some((1, gate.clone())),
        ..Knobs::default()
    })?;
    let token = fixture.publish(60_000, 20_500)?;
    assert!(fixture.handle.prepare_rendition(&token));
    until("the neighbours' run waits on its source", || {
        gate.is_parked()
    });
    let probe = fixture.probe(&token);
    assert!(probe.ring.contains(&0), "slot 0 made: {:?}", probe.ring);
    assert_eq!(probe.live_runs, 2);
    assert_eq!(
        fixture.handle.rendition_readiness(&token),
        RenditionReadiness::Start
    );
    assert_eq!(fixture.handle.renditions_preparing(), 1);

    assert!(fixture.handle.unpublish(&token));
    until("the preparation ends", || {
        fixture.handle.renditions_preparing() == 0
    });
    for run in fixture.producer.runs() {
        until("every producer stops", || run.done());
        assert!(run.stopped());
    }
    assert!(fixture.handle.rendition_probe(&token).is_none());
    assert_eq!(
        fixture.handle.rendition_readiness(&token),
        RenditionReadiness::Ended
    );
    Ok(())
}

/// **A rendition that fails while it is prepared says so**, with the
/// sentence the viewer is shown.
#[test]
fn a_preparation_that_fails_reads_failed_with_its_sentence() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs {
        fail_on_run: Some((0, "This film cannot be repackaged.".into())),
        ..Knobs::default()
    })?;
    let token = fixture.publish(60_000, 7_500)?;
    assert!(fixture.handle.prepare_rendition(&token));
    until("the preparation fails", || {
        fixture.handle.rendition_readiness(&token)
            == RenditionReadiness::Failed {
                sentence: "This film cannot be repackaged.".into(),
            }
    });
    Ok(())
}
