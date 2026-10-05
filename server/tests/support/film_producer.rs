//! **A producer over a real film** (`docs/design/renditions.md`, §2.8 and
//! the interleaving in `mux.rs`): an MP4 that the system `ffmpeg` encodes
//! for the test, its packets listed by `ffprobe`, handed to the sink the
//! way the embedder's libavformat producer hands them -- in the file's
//! order, from the sync sample at or before the job's `from`, video in
//! Annex-B, the index of the video's sync samples when the job wants it.
//!
//! What the synthetic producer (`test_producer.rs`) cannot be: real H.264
//! and HEVC with B-frames (HEVC's open GOPs too), real AAC at 48 and
//! 44.1 kHz, and a film big enough per second that where the sound lies
//! beside the picture matters to a reader -- which is what the system
//! `ffmpeg` and `ffprobe` then read back, as a receiver's FFmpeg does.
//!
//! Every test using it skips, loudly, when `ffmpeg` or the encoder it
//! needs is not installed.

#![allow(dead_code)]

use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use stream_server::{IndexEntry, Job, Producer, ProducerRefusal, Sample, TrackFormat, TrackKind};

/// The picture's codec.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Picture {
    /// x264 with two B-frames: closed GOPs, composition offsets.
    H264,
    /// x265 with two B-frames and open GOPs: leading pictures shown before
    /// the sync sample they follow.
    Hevc,
}

/// What to encode.
#[derive(Clone, Copy, Debug)]
pub struct Recipe {
    pub picture: Picture,
    /// Seconds of film.
    pub seconds: u32,
    /// The picture's bitrate, kbit/s.
    pub kbps: u32,
    /// Frames between sync samples, at 24 frames a second.
    pub gop: u32,
    /// The sound's sample rate.
    pub sample_rate: u32,
}

/// One packet of the film, as `ffprobe` lists it.
#[derive(Clone, Debug)]
pub struct Packet {
    pub track: TrackKind,
    pub pts_us: i64,
    pub key: bool,
    pub pos: u64,
    pub size: usize,
}

/// An encoded film: its file, its bytes and its packets in the file's order.
pub struct Film {
    pub path: PathBuf,
    pub bytes: Bytes,
    pub packets: Vec<Packet>,
    pub video: TrackFormat,
    pub audio: TrackFormat,
    pub duration_ms: u64,
}

fn installed(tool: &str) -> bool {
    Command::new(tool)
        .arg("-version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn has_encoder(name: &str) -> bool {
    Command::new("ffmpeg")
        .args(["-hide_banner", "-encoders"])
        .output()
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .split_whitespace()
                .any(|word| word == name)
        })
        .unwrap_or(false)
}

/// Whether `ffmpeg` and `ffprobe` are here with what `picture` needs; says
/// so on stderr when not, for `test`, and the test returns.
pub fn tools_for(picture: Picture, test: &str) -> bool {
    let encoder = match picture {
        Picture::H264 => "libx264",
        Picture::Hevc => "libx265",
    };
    let ok = installed("ffmpeg") && installed("ffprobe") && has_encoder(encoder);
    if !ok {
        eprintln!("skipping {test}: ffmpeg, ffprobe or its {encoder} encoder is not installed");
    }
    ok
}

/// The boxes at one level of an ISO BMFF buffer: `(type, body)`.
fn boxes(data: &[u8]) -> Vec<([u8; 4], &[u8])> {
    let mut out = Vec::new();
    let mut at = 0;
    while at + 8 <= data.len() {
        let size = u32::from_be_bytes(data[at..at + 4].try_into().unwrap()) as usize;
        let size = if size == 1 {
            u64::from_be_bytes(data[at + 8..at + 16].try_into().unwrap()) as usize
        } else if size == 0 {
            data.len() - at
        } else {
            size
        };
        out.push((
            data[at + 4..at + 8].try_into().unwrap(),
            &data[at + 8..at + size],
        ));
        at += size;
    }
    out
}

/// The body of the first `kind` box inside `moov`, found by its name: the
/// `moov` holds nothing but boxes, so the name is never sample data.
fn in_moov<'a>(file: &'a [u8], kind: &[u8; 4]) -> &'a [u8] {
    let moov = boxes(file)
        .into_iter()
        .find(|(found, _)| found == b"moov")
        .expect("a moov")
        .1;
    let at = moov
        .windows(4)
        .position(|window| window == kind)
        .unwrap_or_else(|| panic!("no {} in the moov", String::from_utf8_lossy(kind)));
    let size = u32::from_be_bytes(moov[at - 4..at].try_into().unwrap()) as usize;
    &moov[at + 4..at - 4 + size]
}

fn annex_b(units: &[&[u8]]) -> Bytes {
    let mut out = Vec::new();
    for unit in units {
        out.extend_from_slice(&[0, 0, 0, 1]);
        out.extend_from_slice(unit);
    }
    Bytes::from(out)
}

/// `csd-0`/`csd-1` from an `avcC`: the SPS and the PPS, Annex-B.
fn avc_csd(avcc: &[u8]) -> (Bytes, Bytes) {
    let mut at = 5;
    let take = |count: usize, at: &mut usize| -> Vec<&[u8]> {
        (0..count)
            .map(|_| {
                let len = u16::from_be_bytes([avcc[*at], avcc[*at + 1]]) as usize;
                let unit = &avcc[*at + 2..*at + 2 + len];
                *at += 2 + len;
                unit
            })
            .collect()
    };
    let sps_count = usize::from(avcc[at] & 0x1f);
    at += 1;
    let sps = take(sps_count, &mut at);
    let pps_count = usize::from(avcc[at]);
    at += 1;
    let pps = take(pps_count, &mut at);
    (annex_b(&sps), annex_b(&pps))
}

/// `csd-0` from an `hvcC`: every array's units, Annex-B.
fn hevc_csd(hvcc: &[u8]) -> Bytes {
    let arrays = usize::from(hvcc[22]);
    let mut at = 23;
    let mut units = Vec::new();
    for _ in 0..arrays {
        let count = u16::from_be_bytes([hvcc[at + 1], hvcc[at + 2]]) as usize;
        at += 3;
        for _ in 0..count {
            let len = u16::from_be_bytes([hvcc[at], hvcc[at + 1]]) as usize;
            units.push(&hvcc[at + 2..at + 2 + len]);
            at += 2 + len;
        }
    }
    annex_b(&units)
}

/// An AAC-LC AudioSpecificConfig.
fn asc(sample_rate: u32, channels: u32) -> Bytes {
    let index = [
        96_000, 88_200, 64_000, 48_000, 44_100, 32_000, 24_000, 22_050, 16_000,
    ]
    .iter()
    .position(|rate| *rate == sample_rate)
    .expect("a standard sample rate") as u8;
    Bytes::from(vec![
        (2 << 3) | (index >> 1),
        ((index & 1) << 7) | ((channels as u8) << 3),
    ])
}

/// Run a command, failing with its stderr.
fn run(command: &mut Command) -> anyhow::Result<Vec<u8>> {
    let output = command.output()?;
    anyhow::ensure!(
        output.status.success(),
        "{command:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output.stdout)
}

impl Film {
    /// Encode `recipe` into `dir` and read it back.
    pub fn make(dir: &Path, recipe: Recipe) -> anyhow::Result<Self> {
        let path = dir.join("film.mp4");
        let source = "testsrc2=size=640x360:rate=24,noise=alls=60:allf=t";
        let kbps = recipe.kbps.to_string();
        let mut command = Command::new("ffmpeg");
        command.args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
        ]);
        command.arg(source);
        command.args(["-f", "lavfi", "-i"]);
        command.arg(format!(
            "sine=frequency=440:sample_rate={}",
            recipe.sample_rate
        ));
        command.args(["-t", &recipe.seconds.to_string()]);
        match recipe.picture {
            Picture::H264 => {
                command.args([
                    "-c:v",
                    "libx264",
                    "-preset",
                    "ultrafast",
                    "-bf",
                    "2",
                    "-x264-params",
                    "b-adapt=0",
                    "-g",
                    &recipe.gop.to_string(),
                    "-keyint_min",
                    &recipe.gop.to_string(),
                    "-sc_threshold",
                    "0",
                ]);
                command.args(["-b:v", &format!("{kbps}k"), "-maxrate", &format!("{kbps}k")]);
                command.args(["-bufsize", &format!("{kbps}k")]);
            }
            Picture::Hevc => {
                command.args([
                    "-c:v",
                    "libx265",
                    "-preset",
                    "ultrafast",
                    "-tag:v",
                    "hvc1",
                    "-b:v",
                    &format!("{kbps}k"),
                    "-x265-params",
                    &format!(
                        "keyint={gop}:min-keyint={gop}:scenecut=0:bframes=2:open-gop=1:\
                         vbv-maxrate={kbps}:vbv-bufsize={kbps}:log-level=error",
                        gop = recipe.gop
                    ),
                ]);
            }
        }
        command.args(["-c:a", "aac", "-b:a", "128k", "-ac", "2"]);
        command.arg(&path);
        run(&mut command)?;

        let bytes = Bytes::from(std::fs::read(&path)?);
        let probe: serde_json::Value = serde_json::from_slice(&run(Command::new("ffprobe")
            .args(["-v", "error", "-of", "json", "-show_entries"])
            .arg("packet=stream_index,pts,flags,pos,size:stream=index,codec_type,time_base,width,height,sample_rate,channels:format=duration")
            .arg(&path))?)?;
        let mut kinds = std::collections::HashMap::new();
        let mut time_bases = std::collections::HashMap::new();
        let (mut width, mut height, mut channels) = (0, 0, 2);
        for stream in probe["streams"].as_array().expect("streams") {
            let index = stream["index"].as_u64().unwrap();
            let kind = match stream["codec_type"].as_str() {
                Some("video") => {
                    width = stream["width"].as_u64().unwrap() as u32;
                    height = stream["height"].as_u64().unwrap() as u32;
                    TrackKind::Video
                }
                Some("audio") => {
                    channels = stream["channels"].as_u64().unwrap() as u32;
                    TrackKind::Audio
                }
                other => panic!("a stream of {other:?}"),
            };
            let (num, den) = stream["time_base"]
                .as_str()
                .unwrap()
                .split_once('/')
                .unwrap();
            kinds.insert(index, kind);
            time_bases.insert(index, (num.parse::<i64>()?, den.parse::<i64>()?));
        }
        let mut packets: Vec<Packet> = probe["packets"]
            .as_array()
            .expect("packets")
            .iter()
            .filter_map(|packet| {
                let index = packet["stream_index"].as_u64()?;
                let (num, den) = time_bases[&index];
                let pts = packet["pts"].as_i64()?;
                let scaled = i128::from(pts) * 1_000_000 * i128::from(num);
                let pts_us = ((scaled + i128::from(den) / 2).div_euclid(i128::from(den))) as i64;
                Some(Packet {
                    track: kinds[&index],
                    pts_us,
                    key: packet["flags"].as_str()?.starts_with('K'),
                    pos: packet["pos"].as_str()?.parse().ok()?,
                    size: packet["size"].as_str()?.parse().ok()?,
                })
            })
            .collect();
        packets.sort_by_key(|packet| packet.pos);
        let duration_ms = (probe["format"]["duration"]
            .as_str()
            .unwrap()
            .parse::<f64>()?
            * 1000.0) as u64;
        let video = match recipe.picture {
            Picture::H264 => {
                let (csd0, csd1) = avc_csd(in_moov(&bytes, b"avcC"));
                TrackFormat::H264 {
                    width,
                    height,
                    csd0,
                    csd1,
                }
            }
            Picture::Hevc => TrackFormat::Hevc {
                width,
                height,
                csd0: hevc_csd(in_moov(&bytes, b"hvcC")),
            },
        };
        let audio = TrackFormat::Aac {
            sample_rate: recipe.sample_rate,
            channels,
            csd0: asc(recipe.sample_rate, channels),
        };
        Ok(Self {
            path,
            bytes,
            packets,
            video,
            audio,
            duration_ms,
        })
    }

    /// The video's sync samples, where the file has them.
    pub fn index(&self) -> Vec<IndexEntry> {
        self.packets
            .iter()
            .filter(|packet| packet.track == TrackKind::Video && packet.key)
            .map(|packet| IndexEntry {
                pts_us: packet.pts_us,
                pos: packet.pos,
            })
            .collect()
    }

    /// Every packet's presentation time on `track`, sorted.
    pub fn times(&self, track: TrackKind) -> Vec<i64> {
        let mut times: Vec<i64> = self
            .packets
            .iter()
            .filter(|packet| packet.track == track)
            .map(|packet| packet.pts_us)
            .collect();
        times.sort_unstable();
        times
    }

    /// A packet's bytes as the producer hands them: the video's NAL units
    /// behind start codes (Annex-B), the sound's as they are.
    fn data(&self, packet: &Packet) -> Bytes {
        let raw = self
            .bytes
            .slice(packet.pos as usize..packet.pos as usize + packet.size);
        if packet.track == TrackKind::Audio {
            return raw;
        }
        let mut out = Vec::with_capacity(raw.len());
        let mut at = 0;
        while at + 4 <= raw.len() {
            let len = u32::from_be_bytes(raw[at..at + 4].try_into().unwrap()) as usize;
            out.extend_from_slice(&[0, 0, 0, 1]);
            out.extend_from_slice(&raw[at + 4..at + 4 + len]);
            at += 4 + len;
        }
        Bytes::from(out)
    }
}

/// The producer: one thread per run, the film's packets in its order.
pub struct FilmProducer {
    film: Arc<Film>,
}

impl FilmProducer {
    pub fn new(film: Arc<Film>) -> Arc<Self> {
        Arc::new(Self { film })
    }
}

impl Producer for FilmProducer {
    fn start(&self, job: Job) -> Result<(), ProducerRefusal> {
        let film = self.film.clone();
        std::thread::spawn(move || produce(&film, job));
        Ok(())
    }
}

fn produce(film: &Film, job: Job) {
    let Job {
        reader,
        from,
        sink,
        wants_index,
        ..
    } = job;
    for (track, format) in [
        (TrackKind::Video, film.video.clone()),
        (TrackKind::Audio, film.audio.clone()),
    ] {
        if sink.format(track, format).is_err() {
            return;
        }
    }
    if wants_index && sink.index(film.index()).is_err() {
        return;
    }
    // As a demuxer seeks: to the sync sample at or before `from`, and on
    // in the file's order from there; from the film's start, from the
    // first byte.
    let from_us = from.as_micros() as i64;
    let start = if from_us == 0 {
        0
    } else {
        film.packets
            .iter()
            .rposition(|packet| {
                packet.track == TrackKind::Video && packet.key && packet.pts_us <= from_us
            })
            .unwrap_or(0)
    };
    for packet in &film.packets[start..] {
        let sample = Sample {
            track: packet.track,
            pts_us: packet.pts_us,
            key: packet.key,
            data: film.data(packet),
        };
        if sink.sample(sample).is_err() {
            return;
        }
    }
    drop(reader);
    sink.end();
}
