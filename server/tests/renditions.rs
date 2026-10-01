//! Renditions (`stream_server::rendition`, `docs/design/renditions.md`,
//! steps F1-F2): `ServerHandle::publish_rendition`, the progressive
//! stream at `/cast/{token}/stream.mp4` on the LAN listener, the run task's
//! cut rule, lookahead, seeks, idle release, speed and cut, and the fMP4
//! muxer -- the run and the ring through the segment the stream asks for
//! (`ServerHandle::rendition_segment`), the stream over HTTP --
//! all driven by the test producer (`support/test_producer.rs`), a Rust
//! producer on a plain thread behind the same trait the embedder's is.
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
use test_producer::{ASC, Knobs, PPS, SPS, TestProducer, frame_of};

/// A bound on a mistake, never a wait a correct run spends.
const BOUND: Duration = Duration::from_secs(60);

/// The segment length most tests use: short, so a run covers several.
const T_MS: u32 = 1000;
const T_US: i64 = T_MS as i64 * 1000;

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
    /// A server with the LAN listener up, a local file registered, and a
    /// test producer with `knobs` installed.
    fn start(knobs: Knobs, tuning: RenditionTuning) -> anyhow::Result<Self> {
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
        std::fs::write(&path, vec![7u8; 1 << 20])?;
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

    fn quick(knobs: Knobs) -> anyhow::Result<Self> {
        Self::start(knobs, RenditionTuning::default())
    }

    fn publish(&self, duration_ms: u64, start_ms: u64) -> anyhow::Result<CastToken> {
        self.handle
            .publish_rendition(&self.id, spec(duration_ms, T_MS, start_ms), None)
    }

    /// The rendition's stream, from `from_ms` when it says.
    fn stream_url(&self, token: &CastToken, from_ms: Option<u64>) -> String {
        let from = from_ms.map(|ms| format!("?from={ms}")).unwrap_or_default();
        format!("{}/cast/{}/stream.mp4{from}", self.lan, token.as_str())
    }

    fn stream(&self, token: &CastToken, from_ms: Option<u64>) -> reqwest::blocking::Response {
        reqwest::blocking::Client::builder()
            .timeout(BOUND)
            .build()
            .expect("a client")
            .get(self.stream_url(token, from_ms))
            .send()
            .expect("the LAN listener answers")
    }

    /// The init segment, as the stream begins with it.
    fn init(&self, token: &CastToken) -> Vec<u8> {
        self.handle
            .rendition_init(token)
            .expect("the init segment")
            .to_vec()
    }

    /// Segment `n`, as the stream asks for it.
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

/// What segment `n` must hold, by the cut rule, for `knobs`' film.
fn expected(knobs: &Knobs, n: i64) -> (Vec<i64>, Vec<i64>) {
    let cut = knobs.key_at_or_after(n * T_US).unwrap_or(i64::MAX);
    let next = knobs.key_at_or_after((n + 1) * T_US).unwrap_or(i64::MAX);
    let video = knobs
        .video_frames()
        .into_iter()
        .map(|(pts, _)| pts)
        .filter(|pts| *pts >= cut && *pts < next)
        .collect();
    let audio = knobs
        .audio_frames()
        .into_iter()
        .filter(|pts| *pts >= n * T_US && *pts < (n + 1) * T_US)
        .collect();
    (video, audio)
}

// --- The playlist and the init segment -------------------------------------------

/// **The init segment is `ftyp` + `moov`** with the producer's parameter sets in
/// the `avcC` and its AudioSpecificConfig in the `esds`, and the first run
/// starts at the spec's start for it.
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
    assert_eq!(runs[0].from, Duration::from_secs(7), "start_ms's segment");
    Ok(())
}

/// **The init segment says how long the film is**, in the box a player
/// reads it from: each track's `mdhd` (version 1, on the track's clock) --
/// Chrome's MP4 demuxer (ffmpeg's) takes a fragmented file's duration from
/// there and nowhere else, and without it a receiver's `duration` is only
/// what has arrived -- and `mvhd` and `tkhd` on the movie clock.
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

// --- The cut rule ----------------------------------------------------------------

/// **Segment N starts at the first key at or after N x T** -- its `tfdt`
/// is that key -- and holds exactly the video up to the next cut, and the
/// audio whose presentation time falls in `[N x T, (N+1) x T)`.
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

/// **What precedes the cut is discarded**: a run from 5 x T, which the
/// producer starts at the key before it (4.80 s) and audio a frame before
/// it, puts nothing before the first key at or after 5 s in segment 5, and
/// no audio before 5 s.
#[test]
fn a_run_discards_what_precedes_its_cut() -> anyhow::Result<()> {
    let knobs = Knobs::default();
    let fixture = Fixture::quick(knobs.clone())?;
    let token = fixture.publish(30_000, 0)?;
    let segment = parse_segment(&fixture.segment(&token, 5));
    let runs = fixture.producer.runs();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].from, Duration::from_secs(5));
    let emitted = runs[0].emitted();
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

// --- Seeks, joins, the lookahead, the idle release -------------------------------

/// **A request far ahead is a seek**: segment 40 after 0-3 starts a new run
/// at 40 x T, and the old run's sink answers `Stopped`.
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
    assert_eq!(runs[1].from, Duration::from_secs(40));
    let probe = fixture.probe(&token);
    assert_eq!(probe.runs_started, 2);
    assert_eq!(probe.run_from, Some(40));
    until("the old run's sink answers Stopped", || runs[0].stopped());
    assert_eq!(
        segment.track(1).unwrap().tfdt,
        (knobs.key_at_or_after(40 * T_US).unwrap() * 90 / 1000) as u64
    );
    Ok(())
}

/// **A request for the segment in production joins it**: two requests for
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

/// **With nothing requested the producer blocks after L segments**: the
/// ring holds the requested segment and the two after it, and the sink's
/// next write waits; a request for the next segment lets one more through.
#[test]
fn the_lookahead_blocks_the_producer() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs::default())?;
    let token = fixture.publish(60_000, 0)?;
    fixture.init(&token);
    let run = fixture.producer.runs()[0].clone();
    // The sink blocks whenever the channel is full, which a producer this
    // fast makes it often; the lookahead is the state it stays in: the
    // ring at L ahead, the next segment in production, the sink blocked.
    let held = |ring: &[u64], next: u64| {
        let probe = fixture.probe(&token);
        probe.ring == ring && probe.in_production == Some(next) && run.probe.is_blocked()
    };
    until(
        "the producer blocks with L = 2 segments ahead of segment 0",
        || held(&[0, 1, 2], 3),
    );
    let furthest = |run: &test_producer::RunRecord| {
        run.emitted().iter().map(|(_, pts)| *pts).max().unwrap_or(0)
    };
    assert!(
        furthest(&run) < 5 * T_US,
        "the producer was held a channel's worth past segment 3's cut, not let run on: {}",
        furthest(&run)
    );

    fixture.segment(&token, 1);
    until(
        "one more segment is made, and the producer blocks again",
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
    let fixture = Fixture::start(
        Knobs::default(),
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
    assert_eq!(runs[1].from, Duration::from_secs(3), "at the ring's edge");
    assert_eq!(segment.sequence, 4);
    Ok(())
}

// --- The cut ---------------------------------------------------------------------

/// **Unpublish answers a stream still waiting for its first segment with
/// an error** -- a `503`, never a clean end -- and stops the producer.
#[test]
fn unpublish_wakes_a_waiting_stream_and_stops_the_producer() -> anyhow::Result<()> {
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
    let before = fixture.handle.lan_media_requests_served();
    let url = fixture.stream_url(&token, None);
    let waiting = std::thread::spawn(move || {
        reqwest::blocking::Client::builder()
            .timeout(BOUND)
            .build()
            .and_then(|client| client.get(&url).send())
            .map(|response| (response.status(), response.text().unwrap_or_default()))
    });
    until("the stream request reaches the listener", || {
        fixture.handle.lan_media_requests_served() > before
    });
    // The run is begun by the stream's request (nothing else asks), and
    // its producer has its job: the request holds the rendition.
    until("the producer has the stream's run", || {
        !fixture.producer.runs().is_empty()
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
        fixture.stream(&token, None).status(),
        reqwest::StatusCode::NOT_FOUND
    );
    Ok(())
}

/// **Unpublish breaks a stream partway with an error**, never a clean end
/// a receiver would read as the film being over.
#[test]
fn unpublish_breaks_a_stream_partway() -> anyhow::Result<()> {
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
    let mut response = fixture.stream(&token, None);
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
/// sentence**, and `rendition_state` says so; a stream asked for then is
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
    fixture.init(&token);
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
    let response = fixture.stream(&token, Some(5000));
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = response.json()?;
    assert_eq!(body["refused"], "renditionFailed");
    assert_eq!(body["message"], sentence.as_str());
    let run = fixture.producer.runs()[0].clone();
    until("the producer is stopped", || run.stopped());
    Ok(())
}

/// **A rendition that fails partway breaks its stream with an error**: the
/// stream had begun (`200`), and a clean end would tell the receiver the
/// film is over.
#[test]
fn a_rendition_that_fails_partway_breaks_its_stream() -> anyhow::Result<()> {
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
    let mut response = fixture.stream(&token, None);
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
/// window: one a little faster than real time makes segment after segment
/// with the rendition producing throughout.
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

// --- Formats, failures, the stream, routes, refusals -----------------------------------

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
    fixture.init(&token);
    fixture.segment(&token, 0);
    // A receiver's seek: a stream from 30 s, whose run comes back
    // different, answered before a byte of it is sent.
    let response = fixture.stream(&token, Some(30_000));
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let refused = fixture.handle.rendition_segment(&token, 30);
    assert!(matches!(refused, Err(NotServed::Failed(_))), "{refused:?}");
    let RenditionState::Failed { sentence } = fixture.handle.rendition_state(&token) else {
        panic!("the rendition did not fail");
    };
    assert!(sentence.contains("different format"), "{sentence}");
    Ok(())
}

/// **A producer's `fail` is the rendition's**, with its sentence.
#[test]
fn a_producers_failure_fails_the_rendition_with_its_sentence() -> anyhow::Result<()> {
    let sentence = "This phone has no decoder for this film's sound.";
    let fixture = Fixture::quick(Knobs {
        fail_on_run: Some((0, sentence.to_string())),
        ..Knobs::default()
    })?;
    let token = fixture.publish(60_000, 0)?;
    let response = fixture.stream(&token, None);
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        fixture.handle.rendition_state(&token),
        RenditionState::Failed {
            sentence: sentence.to_string()
        }
    );
    Ok(())
}

/// **The stream is the init segment and every segment in order**, to the
/// film's end: `video/mp4`, with no length and no ranges offered, one body
/// counted -- and each segment the bytes the run makes for it.
#[test]
fn the_stream_is_the_init_segment_and_every_segment_in_order() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs::default())?;
    let token = fixture.publish(5_500, 0)?;
    let bodies = fixture.handle.lan_media_bodies_served();
    let response = fixture.stream(&token, None);
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "video/mp4");
    assert!(response.headers().get("content-length").is_none());
    assert!(response.headers().get("accept-ranges").is_none());
    let body = response.bytes()?.to_vec();
    assert_eq!(fixture.handle.lan_media_bodies_served(), bodies + 1);

    let mut expected = fixture.init(&token);
    for n in 0..6 {
        expected.extend_from_slice(&fixture.segment(&token, n));
    }
    assert_eq!(body.len(), expected.len());
    assert!(body == expected, "the stream is not init + segments 0-5");
    let kinds: Vec<String> = Boxes::of(&body).into_iter().map(|(kind, _)| kind).collect();
    assert_eq!(&kinds[..2], ["ftyp", "moov"]);
    assert_eq!(kinds.iter().filter(|kind| *kind == "moof").count(), 6);
    Ok(())
}

/// **A stream from a time starts at the segment it falls in**, made by a
/// run from there, and runs on to the end; a `Range` changes nothing.
#[test]
fn a_stream_from_a_time_starts_at_its_segment() -> anyhow::Result<()> {
    let knobs = Knobs::default();
    let fixture = Fixture::quick(knobs.clone())?;
    let token = fixture.publish(5_500, 0)?;
    let response = reqwest::blocking::Client::builder()
        .timeout(BOUND)
        .build()?
        .get(fixture.stream_url(&token, Some(3_500)))
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body = response.bytes()?.to_vec();
    assert_eq!(fixture.producer.runs()[0].from, Duration::from_secs(3));
    let init = fixture.init(&token);
    assert_eq!(&body[..init.len()], init.as_slice());
    // The first segment: its `styp`, `moof` and `mdat`.
    let segments = &body[init.len()..];
    let first_len: usize = (0..3).fold(0, |at, _| at + u32_at(segments, at) as usize);
    let first = parse_segment(&segments[..first_len]);
    assert_eq!(first.sequence, 4, "segment 3 first");
    assert_eq!(
        first.track(1).unwrap().tfdt,
        (knobs.key_at_or_after(3 * T_US).unwrap() * 90 / 1000) as u64
    );
    let moofs = Boxes::of(&body)
        .into_iter()
        .filter(|(kind, _)| kind == "moof")
        .count();
    assert_eq!(moofs, 3, "segments 3, 4 and 5");
    Ok(())
}

/// **A `HEAD` answers the stream's headers and starts nothing.**
#[test]
fn a_head_of_the_stream_starts_no_run() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs::default())?;
    let token = fixture.publish(60_000, 0)?;
    let response = reqwest::blocking::Client::new()
        .head(fixture.stream_url(&token, None))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "video/mp4");
    assert_eq!(fixture.probe(&token).runs_started, 0);
    Ok(())
}

/// **A token names what it was published as**: a plain publication has no
/// stream; a rendition's serves its stream and the source as it is; there
/// is no HLS any more.
#[test]
fn a_plain_publication_has_no_stream() -> anyhow::Result<()> {
    let fixture = Fixture::quick(Knobs::default())?;
    let plain = fixture.handle.publish(&fixture.id, None)?;
    assert_eq!(
        fixture.stream(&plain, None).status(),
        reqwest::StatusCode::NOT_FOUND
    );
    let token = fixture.publish(2_000, 0)?;
    assert_eq!(
        fixture.stream(&token, None).status(),
        reqwest::StatusCode::OK
    );
    let as_is = reqwest::blocking::get(format!("{}/cast/{}", fixture.lan, token.as_str()))?;
    assert_eq!(as_is.status(), reqwest::StatusCode::OK);
    assert_eq!(as_is.bytes()?.len(), 1 << 20);
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

/// A by-hand check, not CI: with `RENDITION_DUMP=<dir>`, write the stream
/// of a 30 s rendition as `stream.mp4`, for `ffprobe`.
#[test]
#[ignore = "writes a file for a by-hand ffprobe; run with RENDITION_DUMP=<dir>"]
fn dump_for_ffprobe() -> anyhow::Result<()> {
    let dir = std::path::PathBuf::from(std::env::var("RENDITION_DUMP")?);
    std::fs::create_dir_all(&dir)?;
    let fixture = Fixture::quick(Knobs::default())?;
    let token = fixture.publish(30_000, 0)?;
    let body = fixture.stream(&token, None).bytes()?;
    std::fs::write(dir.join("stream.mp4"), &body)?;
    Ok(())
}
