//! Renditions of **real films** (`docs/design/renditions.md` §2.8): H.264
//! and HEVC with B-frames, AAC at 48 and 44.1 kHz, encoded by the system
//! `ffmpeg` and repackaged by the server through a producer over the file
//! (`support/film_producer.rs`) -- then read back by the system `ffmpeg`
//! and `ffprobe` over HTTP, as a receiver's FFmpeg reads them.
//!
//! Every test skips, loudly, when the tools are not installed.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use stream_server::rendition::RenditionTuning;
use stream_server::{
    AudioPlan, CastToken, LocalFile, MediaId, MediaSpec, RenditionSpec, ServerConfig, TrackKind,
    VideoPlan,
};

#[path = "support/torrent_fixtures.rs"]
mod torrent_fixtures;
use torrent_fixtures::offline_config;

#[path = "support/film_producer.rs"]
mod film_producer;
use film_producer::{Film, FilmProducer, Look, Picture, Recipe};

/// The segment length the app asks for.
const SEGMENT_MS: u32 = 6000;

struct Fixture {
    handle: stream_server::ServerHandle,
    lan: String,
    id: MediaId,
    film: Arc<Film>,
    _dirs: [tempfile::TempDir; 3],
}

impl Fixture {
    fn start(recipe: Recipe, tuning: RenditionTuning) -> anyhow::Result<Self> {
        let config_dir = tempfile::tempdir()?;
        let cache_dir = tempfile::tempdir()?;
        let files = tempfile::tempdir()?;
        let film = Arc::new(Film::make(files.path(), recipe)?);
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
        let id = handle.register(MediaSpec::Local {
            file: LocalFile::Path(film.path.clone()),
            name: None,
        })?;
        handle.set_rendition_tuning(tuning);
        handle.install_producer(FilmProducer::new(film.clone()));
        Ok(Self {
            handle,
            lan: lan.to_string(),
            id,
            film,
            _dirs: [config_dir, cache_dir, files],
        })
    }

    fn publish(&self) -> anyhow::Result<CastToken> {
        self.handle.publish_rendition(
            &self.id,
            RenditionSpec {
                duration_ms: self.film.duration_ms,
                segment_ms: SEGMENT_MS,
                start_ms: 0,
                video: VideoPlan::Copy,
                audio: AudioPlan::Copy,
                audio_track: 0,
            },
            None,
        )
    }

    fn path(token: &CastToken) -> String {
        format!("/cast/{}/stream.mp4", token.as_str())
    }

    fn url(&self, token: &CastToken) -> String {
        format!("http://{}{}", self.lan, Self::path(token))
    }

    fn get(&self, token: &CastToken, range: Option<(u64, u64)>) -> anyhow::Result<Vec<u8>> {
        let request = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?
            .get(self.url(token));
        let request = match range {
            Some((from, to)) => {
                request.header(reqwest::header::RANGE, format!("bytes={from}-{to}"))
            }
            None => request,
        };
        let response = request.send()?;
        anyhow::ensure!(response.status().is_success(), "{}", response.status());
        Ok(response.bytes()?.to_vec())
    }
}

/// The slots of a rendition's file by its `sidx`: `(offset, size, start in
/// microseconds)` each.
fn slots(file: &[u8]) -> Vec<(u64, u64, i64)> {
    let u32_at = |at: usize| u32::from_be_bytes(file[at..at + 4].try_into().unwrap());
    let mut at = 0;
    while &file[at + 4..at + 8] != b"sidx" {
        at += u32_at(at) as usize;
    }
    let sidx = at;
    let size = u32_at(sidx) as usize;
    let timescale = i64::from(u32_at(sidx + 16));
    let mut time = i64::from_be_bytes(file[sidx + 20..sidx + 28].try_into().unwrap());
    let first = u64::from_be_bytes(file[sidx + 28..sidx + 36].try_into().unwrap());
    let count = u16::from_be_bytes([file[sidx + 38], file[sidx + 39]]) as usize;
    let mut offset = (sidx + size) as u64 + first;
    (0..count)
        .map(|k| {
            let size = u64::from(u32_at(sidx + 40 + 12 * k) & 0x7fff_ffff);
            let slot = (offset, size, time * 1_000_000 / timescale);
            offset += size;
            time += i64::from(u32_at(sidx + 44 + 12 * k));
            slot
        })
        .collect()
}

/// Every packet `ffprobe` lists in `path`: `(track, presentation time in
/// microseconds)`, each track's sorted.
fn packets_of(path: &Path) -> anyhow::Result<Vec<(TrackKind, Vec<i64>)>> {
    let output = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-of",
            "csv=p=0",
            "-show_entries",
            "packet=stream_index,pts_time",
        ])
        .arg(path)
        .output()?;
    anyhow::ensure!(output.status.success(), "ffprobe failed");
    let mut video = Vec::new();
    let mut audio = Vec::new();
    for line in String::from_utf8(output.stdout)?.lines() {
        let Some((index, pts)) = line.split_once(',') else {
            continue;
        };
        let pts_us = (pts.parse::<f64>()? * 1_000_000.0).round() as i64;
        match index {
            "0" => video.push(pts_us),
            "1" => audio.push(pts_us),
            other => anyhow::bail!("a stream {other}"),
        }
    }
    video.sort_unstable();
    audio.sort_unstable();
    Ok(vec![(TrackKind::Video, video), (TrackKind::Audio, audio)])
}

/// **A relay that counts requests**: a TCP listener in front of `target`
/// that passes everything through and keeps the `Range` of each request
/// it sees going in -- how many requests a reader of the file makes, and
/// where.
struct Relay {
    addr: SocketAddr,
    ranges: Arc<Mutex<Vec<String>>>,
}

impl Relay {
    fn start(target: SocketAddr) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let seen = ranges.clone();
        std::thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { return };
                let Ok(upstream) = TcpStream::connect(target) else {
                    return;
                };
                let (Ok(mut back), Ok(mut down)) = (upstream.try_clone(), client.try_clone())
                else {
                    return;
                };
                std::thread::spawn(move || {
                    let _ = std::io::copy(&mut back, &mut down);
                    let _ = down.shutdown(std::net::Shutdown::Both);
                });
                let seen = seen.clone();
                std::thread::spawn(move || {
                    let mut up = upstream;
                    let mut reader = BufReader::new(client);
                    let mut range = None;
                    let mut line = String::new();
                    loop {
                        line.clear();
                        match reader.read_line(&mut line) {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {}
                        }
                        if let Some(value) = line
                            .split_once(':')
                            .filter(|(name, _)| name.eq_ignore_ascii_case("range"))
                        {
                            range = Some(value.1.trim().to_string());
                        }
                        if line == "\r\n" {
                            seen.lock()
                                .unwrap()
                                .push(range.take().unwrap_or_else(|| "-".to_string()));
                        }
                        if up.write_all(line.as_bytes()).is_err() {
                            break;
                        }
                    }
                    let _ = up.shutdown(std::net::Shutdown::Both);
                });
            }
        });
        Ok(Self { addr, ranges })
    }

    fn url(&self, token: &CastToken) -> String {
        format!("http://{}{}", self.addr, Fixture::path(token))
    }

    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.ranges.lock().unwrap())
    }
}

/// Read `url` with the system `ffmpeg` as a player does -- every packet
/// demuxed, nothing decoded -- with `before` ahead of the input; fails on
/// anything it says at `error`.
fn ffmpeg_reads(url: &str, before: &[&str], after: &[&str]) -> anyhow::Result<()> {
    let output = Command::new("ffmpeg")
        .args(["-hide_banner", "-v", "error"])
        .args(before)
        .args(["-i", url])
        .args(after)
        .args(["-map", "0", "-c", "copy", "-f", "null", "-"])
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "ffmpeg failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

/// A film as the field's: 11 Mbit/s with a sync sample every 8 s, so a
/// slot is some 11 MB -- past the 8 MiB at which FFmpeg stops growing its
/// read buffer to cover a picture and its sound that far apart
/// (`ff_configure_buffers_for_index`: twice the distance, under 16 MiB),
/// which is what made a picture-first slot cost a request per hop.
fn fat_film(picture: Picture) -> Recipe {
    Recipe {
        picture,
        look: Look::Noise,
        seconds: 32,
        kbps: 11_000,
        gop: 192,
        sample_rate: 48_000,
    }
}

/// **A straight read of a film whose slots are 11 MB reads it front to
/// back**: the requests go forward only, at most one per slot (the `free`
/// box between two slots is a forward skip a reader may make a request
/// of, as its socket's window says) -- not one per hop between a slot's
/// picture and its sound, the field bug: on zond's TV an 8 Mbit/s film
/// with 7.5 s GOPs made a request every 0.3 to 1 s, alternately creeping
/// through a slot's picture and parked at its sound, and the cast stopped
/// after a second. Laid picture-first, this film made `ffmpeg` (6.1) ask
/// 317 times, back and forth; chunked, once.
#[test]
fn a_straight_read_of_a_fat_film_reads_it_front_to_back() -> anyhow::Result<()> {
    if !film_producer::tools_for(
        Picture::H264,
        "a_straight_read_of_a_fat_film_reads_it_front_to_back",
    ) {
        return Ok(());
    }
    let fixture = Fixture::start(fat_film(Picture::H264), RenditionTuning::default())?;
    let token = fixture.publish()?;
    let file_slots = slots(&fixture.get(&token, Some((0, 64 * 1024 - 1)))?);
    let relay = Relay::start(fixture.lan.parse()?)?;
    ffmpeg_reads(&relay.url(&token), &[], &[])?;
    let starts = starts_of(&relay.take());
    assert!(
        starts.len() <= file_slots.len() && starts.windows(2).all(|pair| pair[0] < pair[1]),
        "a straight read asked for {starts:?}"
    );
    Ok(())
}

/// Where each `Range` a relay saw begins (`-` for none, the file's start).
fn starts_of(ranges: &[String]) -> Vec<u64> {
    ranges
        .iter()
        .map(|range| {
            range
                .strip_prefix("bytes=")
                .and_then(|range| range.split('-').next())
                .and_then(|start| start.parse().ok())
                .unwrap_or(0)
        })
        .collect()
}

/// **A seek reads on from where it lands**: `ffmpeg -ss` into a fat film's
/// third slot asks for the file's start, then the slot, then forward only,
/// at most once per slot boundary it reads across. Picture-first, the same
/// seek asked 47 times, back and forth.
#[test]
fn a_seek_into_a_fat_film_reads_on_from_where_it_lands() -> anyhow::Result<()> {
    if !film_producer::tools_for(
        Picture::H264,
        "a_seek_into_a_fat_film_reads_on_from_where_it_lands",
    ) {
        return Ok(());
    }
    let fixture = Fixture::start(fat_film(Picture::H264), RenditionTuning::default())?;
    let token = fixture.publish()?;
    let file_slots = slots(&fixture.get(&token, Some((0, 64 * 1024 - 1)))?);
    let relay = Relay::start(fixture.lan.parse()?)?;
    ffmpeg_reads(&relay.url(&token), &["-ss", "20"], &["-t", "3"])?;
    // 20 s is in slot 2 (16 to 24 s); three seconds on read into slot 3.
    let starts = starts_of(&relay.take());
    assert!(
        starts.len() >= 2 && starts[1] == file_slots[2].0,
        "the seek asked for {starts:?}"
    );
    assert!(
        starts.len() <= 4 && starts[1..].windows(2).all(|pair| pair[0] < pair[1]),
        "the seek asked for {starts:?}"
    );
    Ok(())
}

/// **A film's rendition decodes whole and keeps every sample's time**:
/// H.264 with 48 kHz sound and HEVC (open GOPs) with 44.1 kHz, each
/// decoded by `ffmpeg` without a complaint, and every packet `ffprobe`
/// reads back at its time in the film -- the sound exactly, the picture
/// late by no more than five frames and never less than before it: FFmpeg
/// shows a fragment's video late by the largest negative composition
/// offset it has read so far (`dts_shift`), so picture and sound are as
/// far apart as in the film whichever chunk or slot they are in, give or
/// take that. With open GOPs the offset takes in the leading pictures (a
/// slot's decode times start at its sync sample's time), as FFmpeg 4.4 --
/// timing a slot's first `moof` by its label -- always showed.
///
/// Except the first slot's sound, one AAC frame late throughout: the
/// film's priming frame is at -21 ms (an MP4's edit list, which leaves the
/// container's start at zero), a fragment's time cannot be below zero, so
/// that frame is put at zero and the slot's sound runs on from there. As
/// it was before the slots were chunked; pinned here so a change to it is
/// seen.
#[test]
fn a_film_decodes_whole_with_its_times() -> anyhow::Result<()> {
    for (picture, sample_rate) in [(Picture::H264, 48_000), (Picture::Hevc, 44_100)] {
        if !film_producer::tools_for(picture, "a_film_decodes_whole_with_its_times") {
            continue;
        }
        let fixture = Fixture::start(
            Recipe {
                picture,
                look: Look::Noise,
                seconds: 20,
                kbps: 4_000,
                gop: 96,
                sample_rate,
            },
            RenditionTuning::default(),
        )?;
        let token = fixture.publish()?;
        let file = fixture.get(&token, None)?;
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("rendition.mp4");
        std::fs::write(&path, &file)?;
        let decoded = Command::new("ffmpeg")
            .args(["-hide_banner", "-v", "error", "-i"])
            .arg(&path)
            .args(["-f", "null", "-"])
            .output()?;
        assert!(decoded.status.success(), "{picture:?}: ffmpeg failed");
        assert!(
            decoded.stderr.is_empty(),
            "{picture:?}: {}",
            String::from_utf8_lossy(&decoded.stderr)
        );
        let second_slot = slots(&file)[1].2;
        let priming = fixture.film.times(TrackKind::Audio)[0];
        assert!(
            priming < 0,
            "{picture:?}: the film's sound starts at {priming} us"
        );
        for (track, times) in packets_of(&path)? {
            let film = fixture.film.times(track);
            assert_eq!(times.len(), film.len(), "{picture:?}'s {track:?} packets");
            let shift = times[film.len() - 1] - film[film.len() - 1];
            if track == TrackKind::Audio {
                assert!(
                    shift.abs() <= 12,
                    "{picture:?}: the sound moved by {shift} us"
                );
            } else {
                assert!(
                    (0..=5 * 41_667).contains(&shift),
                    "{picture:?}: the picture moved by {shift} us"
                );
            }
            let mut moved = 0;
            for (at, (time, was)) in times.iter().zip(&film).enumerate() {
                if track == TrackKind::Video {
                    // FFmpeg's `dts_shift` is the largest negative offset
                    // read so far: the picture's lateness only grows.
                    let late = time - was;
                    assert!(
                        late >= moved - 12 && late <= shift + 12,
                        "{picture:?}'s video packet {at}: at {time} us, {was} us in the film"
                    );
                    moved = moved.max(late);
                    continue;
                }
                let late = if *was < second_slot - 64_000 {
                    -priming
                } else {
                    0
                };
                assert!(
                    (time - was - shift - late).abs() <= 12,
                    "{picture:?}'s {track:?} packet {at}: at {time} us, {was} us in the film"
                );
            }
        }
    }
    Ok(())
}

/// **Every slot is the same bytes whichever run makes it**: a film read
/// whole by one run from its start, then published again and read slot by
/// slot from the last to the first -- each slot made by a run started at
/// it, from two seconds before its cut -- is the same file, byte for byte.
/// The chunks a slot is cut into are a function of its samples alone.
#[test]
fn every_slot_is_the_same_bytes_whichever_run_makes_it() -> anyhow::Result<()> {
    for (picture, sample_rate) in [(Picture::H264, 48_000), (Picture::Hevc, 44_100)] {
        if !film_producer::tools_for(
            picture,
            "every_slot_is_the_same_bytes_whichever_run_makes_it",
        ) {
            continue;
        }
        let fixture = Fixture::start(
            Recipe {
                picture,
                look: Look::Noise,
                seconds: 30,
                kbps: 4_000,
                gop: 72,
                sample_rate,
            },
            RenditionTuning {
                // Nothing kept but what a run is making: every slot asked
                // again is made again.
                ring_cap: 1,
                ..RenditionTuning::default()
            },
        )?;
        let whole = fixture.get(&fixture.publish()?, None)?;
        let token = fixture.publish()?;
        let file_slots = slots(&whole);
        assert!(
            file_slots.len() >= 5,
            "{picture:?}: {} slots",
            file_slots.len()
        );
        let started = fixture
            .handle
            .rendition_probe(&token)
            .map_or(0, |probe| probe.runs_started);
        for (k, (offset, size, _)) in file_slots.iter().enumerate().rev() {
            let slot = fixture.get(&token, Some((*offset, offset + size - 1)))?;
            assert!(
                slot == whole[*offset as usize..(offset + size) as usize],
                "{picture:?}: slot {k} made by a run started at it is other bytes"
            );
        }
        let probe = fixture.handle.rendition_probe(&token).expect("published");
        assert!(
            probe.runs_started >= started + file_slots.len() as u64 - 1,
            "{picture:?}: each slot from a run of its own ({} runs)",
            probe.runs_started
        );
    }
    Ok(())
}

/// The first video packet's time after each of `seeks` (seconds), in one
/// `ffprobe` of `url` that reads `before` seconds first -- the TV's remote
/// seeks, forward then back.
fn ffprobe_lands(url: &str, before: Option<u32>, seeks: &[u32]) -> anyhow::Result<Vec<f64>> {
    let intervals: Vec<String> = before
        .map(|first| format!("%+{first}"))
        .into_iter()
        .chain(seeks.iter().map(|at| format!("{at}%+#1")))
        .collect();
    let output = Command::new("ffprobe")
        .args(["-v", "error", "-read_intervals", &intervals.join(",")])
        .args(["-select_streams", "v", "-show_entries", "packet=pts_time"])
        .args(["-of", "csv=p=0", url])
        .output()?;
    anyhow::ensure!(output.status.success(), "ffprobe failed");
    let times: Vec<f64> = String::from_utf8(output.stdout)?
        .lines()
        .filter_map(|line| line.trim().parse().ok())
        .collect();
    anyhow::ensure!(times.len() >= seeks.len(), "{times:?}");
    Ok(times[times.len() - seeks.len()..].to_vec())
}

/// **A seek lands on the sync sample at or before its target** -- the
/// app's long test film exactly (xtremio `rust/tests/support/film.rs`:
/// 6 minutes of `testsrc`, a key every 2.8 s, HEVC Main 10 HDR10 with
/// open GOPs, and H.264), published with the app's 6 s segments:
/// `ffprobe` seeking to 5:00 lands on the key at 299.6 s in a few
/// requests, a seek list read after the first 16 s lands each within a
/// GOP of its target, and `ffmpeg -ss 60` asks for the start and the
/// target and nothing else.
///
/// Cut on the 6 s grid, a slot held two or three GOPs and a seek landed
/// at the slot's start -- the demuxer knows only the sync samples of the
/// chunks it has read, and it reads the slot's first: 294.0 s for 5:00
/// (H.264). With HEVC's open GOPs FFmpeg 6.1 then also sought the sound to
/// the slot before, and the picture followed it: 288.4 s.
#[test]
fn a_seek_lands_on_the_sync_sample_at_or_before_it() -> anyhow::Result<()> {
    for picture in [Picture::Hevc, Picture::H264] {
        if !film_producer::tools_for(picture, "a_seek_lands_on_the_sync_sample_at_or_before_it") {
            continue;
        }
        let fixture = Fixture::start(
            Recipe {
                picture,
                look: Look::App,
                seconds: 360,
                kbps: 0,
                gop: 70,
                sample_rate: 48_000,
            },
            RenditionTuning::default(),
        )?;
        let token = fixture.publish()?;
        let keys: Vec<f64> = fixture
            .film
            .packets
            .iter()
            .filter(|packet| packet.track == TrackKind::Video && packet.key)
            .map(|packet| packet.pts_us as f64 / 1e6)
            .collect();
        let key_before = |at: f64| *keys.iter().rfind(|key| **key <= at).unwrap();
        let relay = Relay::start(fixture.lan.parse()?)?;
        let url = relay.url(&token);

        let landed = ffprobe_lands(&url, None, &[300])?[0];
        let asked = relay.take();
        let key = key_before(300.0);
        assert!(
            (key - 0.2..=300.0).contains(&landed),
            "{picture:?}: 5:00 landed at {landed} s, the key before it is at {key} s"
        );
        assert!(asked.len() <= 6, "{picture:?}: 5:00 asked {asked:?}");

        let seeks = [100, 250, 330, 43, 30];
        let lands = ffprobe_lands(&url, Some(16), &seeks)?;
        relay.take();
        for (at, landed) in seeks.iter().zip(&lands) {
            let key = key_before(f64::from(*at));
            assert!(
                (key - 0.2..=f64::from(*at)).contains(landed),
                "{picture:?}: seeks {seeks:?} landed at {lands:?}"
            );
        }

        ffmpeg_reads(&url, &["-ss", "60"], &["-t", "1"])?;
        let asked = relay.take();
        assert_eq!(asked.len(), 2, "{picture:?}: ffmpeg -ss 60 asked {asked:?}");
    }
    Ok(())
}

/// **By hand**: a rendition of a film written to `RENDITION_FILM_DUMP` for
/// FFmpeg probes of other versions (`docs/design/renditions.md`).
#[test]
#[ignore = "writes a file for by-hand probes; run with RENDITION_FILM_DUMP=<dir>"]
fn dump_a_film_rendition() -> anyhow::Result<()> {
    let dir = std::env::var("RENDITION_FILM_DUMP")?;
    let picture = match std::env::var("RENDITION_FILM_PICTURE").as_deref() {
        Ok("hevc") => Picture::Hevc,
        _ => Picture::H264,
    };
    let fixture = Fixture::start(
        Recipe {
            picture,
            look: match std::env::var("RENDITION_FILM_LOOK").as_deref() {
                Ok("app") => Look::App,
                _ => Look::Noise,
            },
            seconds: std::env::var("RENDITION_FILM_SECONDS").map_or(Ok(60), |s| s.parse())?,
            kbps: std::env::var("RENDITION_FILM_KBPS").map_or(Ok(8000), |kbps| kbps.parse())?,
            gop: std::env::var("RENDITION_FILM_GOP").map_or(Ok(180), |gop| gop.parse())?,
            sample_rate: 48_000,
        },
        RenditionTuning::default(),
    )?;
    let token = fixture.publish()?;
    let file = fixture.get(&token, None)?;
    std::fs::write(std::path::Path::new(&dir).join("stream.mp4"), &file)?;
    std::fs::copy(
        &fixture.film.path,
        std::path::Path::new(&dir).join("film.mp4"),
    )?;
    Ok(())
}
