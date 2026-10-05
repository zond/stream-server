//! Renditions of **real films** (`docs/design/renditions.md` §2.8): H.264
//! and HEVC with B-frames, AAC at 48 and 44.1 kHz, encoded by the system
//! `ffmpeg` and repackaged by the server through a producer over the file
//! (`support/film_producer.rs`) -- then read back by the system `ffmpeg`
//! and `ffprobe` over HTTP, as a receiver's FFmpeg reads them.
//!
//! Every test skips, loudly, when the tools are not installed.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
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
        Self::start_with(recipe, tuning, true)
    }

    /// [`Self::start`], the producer reporting the film's index or not.
    fn start_with(recipe: Recipe, tuning: RenditionTuning, indexed: bool) -> anyhow::Result<Self> {
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
        handle.install_producer(if indexed {
            FilmProducer::new(film.clone())
        } else {
            FilmProducer::without_index(film.clone())
        });
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

/// The slots of a rendition's file by its `sidx`: `(offset, size, label
/// in microseconds)` each.
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
    // Two references a slot: its first part, and the rest.
    (0..count / 2)
        .map(|k| {
            let at = sidx + 40 + 24 * k;
            let size = u64::from((u32_at(at) & 0x7fff_ffff) + (u32_at(at + 12) & 0x7fff_ffff));
            let slot = (offset, size, time * 1_000_000 / timescale);
            offset += size;
            time += i64::from(u32_at(at + 4)) + i64::from(u32_at(at + 16));
            slot
        })
        .collect()
}

/// The packets `ffprobe` reads from `input` -- with `-read_intervals
/// intervals` when given -- in the order it reads them: `(track,
/// presentation time in microseconds, key)`.
fn read_packets(
    input: &str,
    intervals: Option<&str>,
) -> anyhow::Result<Vec<(TrackKind, i64, bool)>> {
    let mut command = Command::new("ffprobe");
    command.args(["-v", "error"]);
    if let Some(intervals) = intervals {
        command.args(["-read_intervals", intervals]);
    }
    let output = command
        .args([
            "-of",
            "csv=p=0",
            "-show_entries",
            "packet=stream_index,pts_time,flags",
        ])
        .arg(input)
        .output()?;
    anyhow::ensure!(output.status.success(), "ffprobe failed");
    let mut out = Vec::new();
    for line in String::from_utf8(output.stdout)?.lines() {
        let fields: Vec<&str> = line.split(',').collect();
        let [index, pts, flags, ..] = fields[..] else {
            continue;
        };
        let track = match index {
            "0" => TrackKind::Video,
            "1" => TrackKind::Audio,
            other => anyhow::bail!("a stream {other}"),
        };
        let pts_us = (pts.parse::<f64>()? * 1_000_000.0).round() as i64;
        // A packet FFmpeg marks to be discarded (`D`) is one it has
        // indexed twice -- a fragment read again -- and a player drops.
        if flags.contains('D') {
            continue;
        }
        out.push((track, pts_us, flags.starts_with('K')));
    }
    Ok(out)
}

/// The film's packets of `track` in decode order -- the order a demuxer
/// hands them out -- with their key flags: the source a rendition's
/// packets must be, time for time. The sound from the film's start on: a
/// frame before it (an AAC encoder's priming) has no time a fragment can
/// say, and is not carried.
fn in_decode_order(film: &Film, track: TrackKind) -> Vec<(i64, bool)> {
    film.packets
        .iter()
        .filter(|packet| packet.track == track)
        .filter(|packet| track == TrackKind::Video || packet.pts_us >= 0)
        .map(|packet| (packet.pts_us, packet.key))
        .collect()
}

/// **`got` is a run of `film`, time for time**: from where its first
/// packet is in the film, every packet the next one's, at the same
/// presentation time (within the 90 kHz clock's tick) and with the same
/// key flag -- not merely a time the film has somewhere, which a picture
/// late by whole frames would still be.
fn assert_run(got: &[(i64, bool)], film: &[(i64, bool)], what: &str) {
    let Some(&(first, _)) = got.first() else {
        panic!("{what}: no packets");
    };
    let from = film
        .iter()
        .position(|(pts, _)| (pts - first).abs() <= 12)
        .unwrap_or_else(|| panic!("{what}: {first} us is no time of the film's"));
    for (at, ((pts, key), (want, want_key))) in got.iter().zip(&film[from..]).enumerate() {
        assert!(
            (pts - want).abs() <= 12 && key == want_key,
            "{what}: packet {at} after {first} us is at {pts} us ({key}), the film's at {want} us ({want_key})"
        );
    }
    assert!(
        got.len() <= film.len() - from,
        "{what}: more packets than the film has"
    );
}

/// The packets of `track` in `packets`, as [`assert_run`] takes them.
fn of_track(packets: &[(TrackKind, i64, bool)], track: TrackKind) -> Vec<(i64, bool)> {
    packets
        .iter()
        .filter(|packet| packet.0 == track)
        .map(|packet| (packet.1, packet.2))
        .collect()
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

/// **A film's rendition decodes whole and shows every sample at its own
/// time**: H.264 with 48 kHz sound and HEVC (open GOPs) with 44.1 kHz,
/// each decoded by `ffmpeg` without a complaint, and every packet `ffprobe`
/// reads back, in the order it reads them, the film's packet at the same
/// presentation time with the same key flag -- the picture and the sound
/// alike, so neither is ever late against the other. FFmpeg shows a
/// picture at its decode time, its composition offset and its
/// `dts_shift` (the largest negative offset it has read so far): every
/// offset is written at least `-D`, and the film's first sample at
/// exactly `-D`, so `dts_shift` is `D` from the first packet to the last
/// (`mux::DECODE_AHEAD_US`).
///
/// The sound's packets are the film's from its start: a frame before it
/// (the AAC encoder's priming at -21 ms, behind the MP4's edit list) has no
/// time a fragment can say, and is not carried.
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
        let read = read_packets(path.to_str().unwrap(), None)?;
        for track in [TrackKind::Video, TrackKind::Audio] {
            let film = in_decode_order(&fixture.film, track);
            let got = of_track(&read, track);
            assert_eq!(got.len(), film.len(), "{picture:?}'s {track:?} packets");
            assert_run(&got, &film, &format!("{picture:?}'s {track:?}"));
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
/// target and nothing else; and after a seek -- fresh, and back after
/// reading -- every packet of the picture and of the sound is the film's,
/// at its time.
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

        // **After a seek, picture and sound are each at their own times**:
        // into a slot first, and back to one after reading the first 16 s
        // -- each packet the film's at its time, the sound from at or
        // before the picture it plays with.
        let picture_film = in_decode_order(&fixture.film, TrackKind::Video);
        let sound_film = in_decode_order(&fixture.film, TrackKind::Audio);
        for (at, before) in [(300, false), (100, false), (43, true), (30, true)] {
            let intervals = if before {
                format!("%+16,{at}%+#150")
            } else {
                format!("{at}%+#150")
            };
            let read = read_packets(&url, Some(&intervals))?;
            relay.take();
            let read: Vec<_> = if before {
                let first = read
                    .iter()
                    .rposition(|packet| packet.1 < 17_000_000)
                    .unwrap();
                read[first + 1..].to_vec()
            } else {
                read
            };
            let what = format!("{picture:?} after a seek to {at} s");
            let picture_read = of_track(&read, TrackKind::Video);
            let sound_read = of_track(&read, TrackKind::Audio);
            let key = (key_before(f64::from(at)) * 1e6).round() as i64;
            assert!(
                (picture_read[0].0 - key).abs() <= 12 && picture_read[0].1,
                "{what}: the picture begins at {:?}, not the key at {key} us",
                picture_read[0]
            );
            assert_run(
                &picture_read,
                &picture_film,
                &format!("{what}, the picture"),
            );
            assert_run(&sound_read, &sound_film, &format!("{what}, the sound"));
            assert!(
                sound_read[0].0 <= key,
                "{what}: the sound begins at {}",
                sound_read[0].0
            );
        }
    }
    Ok(())
}

/// **An estimated layout shows every sample at its own time too**: the
/// app's long HEVC film laid out with no index (a transport stream's, a
/// Matroska file's without cues), read whole and after seeks -- every
/// packet of the picture and of the sound the film's at its time; each
/// seek lands on a key at or before its target, within a segment and the
/// assumed GOP (its slots are labelled 10 s late).
#[test]
fn an_estimated_layout_shows_every_sample_at_its_time() -> anyhow::Result<()> {
    if !film_producer::tools_for(
        Picture::Hevc,
        "an_estimated_layout_shows_every_sample_at_its_time",
    ) {
        return Ok(());
    }
    let fixture = Fixture::start_with(
        Recipe {
            picture: Picture::Hevc,
            look: Look::App,
            seconds: 120,
            kbps: 0,
            gop: 70,
            sample_rate: 48_000,
        },
        RenditionTuning::default(),
        false,
    )?;
    let token = fixture.publish()?;
    assert_eq!(
        fixture
            .handle
            .rendition_probe(&token)
            .and_then(|probe| probe.exact),
        None
    );
    let file = fixture.get(&token, None)?;
    assert_eq!(
        fixture.handle.rendition_probe(&token).unwrap().exact,
        Some(false)
    );
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("rendition.mp4");
    std::fs::write(&path, &file)?;
    let picture_film = in_decode_order(&fixture.film, TrackKind::Video);
    let sound_film = in_decode_order(&fixture.film, TrackKind::Audio);
    let read = read_packets(path.to_str().unwrap(), None)?;
    assert_run(
        &of_track(&read, TrackKind::Video),
        &picture_film,
        "the picture",
    );
    assert_run(&of_track(&read, TrackKind::Audio), &sound_film, "the sound");
    assert_eq!(of_track(&read, TrackKind::Video).len(), picture_film.len());
    assert_eq!(of_track(&read, TrackKind::Audio).len(), sound_film.len());
    for at in [60i64, 100, 30] {
        let read = read_packets(path.to_str().unwrap(), Some(&format!("{at}%+#150")))?;
        let picture_read = of_track(&read, TrackKind::Video);
        let landed = picture_read[0].0;
        assert!(
            picture_read[0].1 && landed <= at * 1_000_000 && landed >= (at - 16) * 1_000_000,
            "{at} s landed at {landed} us"
        );
        assert_run(
            &picture_read,
            &picture_film,
            &format!("after {at} s, the picture"),
        );
        assert_run(
            &of_track(&read, TrackKind::Audio),
            &sound_film,
            &format!("after {at} s, the sound"),
        );
    }
    Ok(())
}

/// The video packets' decode times `ffprobe` reads from `path`, in ticks
/// of the 90 kHz clock, in the order it reads them, with their durations.
fn video_decode_times(path: &str) -> anyhow::Result<Vec<(i64, i64)>> {
    let output = Command::new("ffprobe")
        .args(["-v", "error", "-select_streams", "v", "-of", "csv=p=0"])
        .args(["-show_entries", "packet=dts,duration,flags", path])
        .output()?;
    anyhow::ensure!(output.status.success(), "ffprobe failed");
    let mut out = Vec::new();
    for line in String::from_utf8(output.stdout)?.lines() {
        let fields: Vec<&str> = line.split(',').collect();
        let [dts, duration, flags, ..] = fields[..] else {
            continue;
        };
        if flags.contains('D') {
            continue;
        }
        out.push((dts.parse()?, duration.parse()?));
    }
    Ok(out)
}

/// **Decode times only ever increase, across every slot and every chunk**
/// -- the foreman's open-GOP clip (`testsrc2` 1280x720, x265 with open
/// GOPs every 2 s, whose leading pictures vary: 0, 3, 1, 0, 0, 1, 1, 4,
/// ...), in 6 s segments, read whole by `ffprobe`: every packet's decode
/// time after the one before's, every duration above nought, and every
/// packet of picture and sound the film's at its time. A slot whose GOP
/// has more leading pictures than the next has more samples than frames
/// between its first decode time and the next slot's; stepped by the
/// frames' durations from its own first decode time its times ran past the
/// next slot's -- back 7200 ticks at the boundaries of that clip.
#[test]
fn decode_times_only_ever_increase() -> anyhow::Result<()> {
    if !film_producer::tools_for(Picture::Hevc, "decode_times_only_ever_increase") {
        return Ok(());
    }
    let fixture = Fixture::start(
        Recipe {
            picture: Picture::Hevc,
            look: Look::OpenGop,
            seconds: 30,
            kbps: 0,
            gop: 50,
            sample_rate: 48_000,
        },
        RenditionTuning::default(),
    )?;
    let leading = fixture.film.leading_pictures();
    assert!(
        leading.windows(2).any(|pair| pair[0] > pair[1])
            && leading.windows(2).any(|pair| pair[0] < pair[1]),
        "the film's leading pictures must vary both ways: {leading:?}"
    );
    let token = fixture.publish()?;
    let file = fixture.get(&token, None)?;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("rendition.mp4");
    std::fs::write(&path, &file)?;
    let path = path.to_str().unwrap();
    let times = video_decode_times(path)?;
    for (at, pair) in times.windows(2).enumerate() {
        assert!(
            pair[1].0 > pair[0].0,
            "packet {}: decode time {:?} after {:?}",
            at + 1,
            pair[1],
            pair[0]
        );
    }
    assert!(
        times.iter().all(|(_, duration)| *duration > 0),
        "a duration of nought"
    );
    let read = read_packets(path, None)?;
    for track in [TrackKind::Video, TrackKind::Audio] {
        let film = in_decode_order(&fixture.film, track);
        assert_eq!(
            of_track(&read, track).len(),
            film.len(),
            "{track:?} packets"
        );
        assert_run(&of_track(&read, track), &film, &format!("{track:?}"));
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
    let fixture = Fixture::start_with(
        Recipe {
            picture,
            look: match std::env::var("RENDITION_FILM_LOOK").as_deref() {
                Ok("app") => Look::App,
                Ok("open") => Look::OpenGop,
                _ => Look::Noise,
            },
            seconds: std::env::var("RENDITION_FILM_SECONDS").map_or(Ok(60), |s| s.parse())?,
            kbps: std::env::var("RENDITION_FILM_KBPS").map_or(Ok(8000), |kbps| kbps.parse())?,
            gop: std::env::var("RENDITION_FILM_GOP").map_or(Ok(180), |gop| gop.parse())?,
            sample_rate: 48_000,
        },
        RenditionTuning::default(),
        std::env::var("RENDITION_FILM_NO_INDEX").is_err(),
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
