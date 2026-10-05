//! **Who keeps a torrent running is said, not guessed**
//! (`enginefs::retention::holds`): a player screen holds the torrent its
//! own requests named from its first request until the app says the screen
//! is gone (`ServerHandle::release_player`), paused, stalled or with nothing
//! open; a cast holds it from publish to unpublish, direct or rendition
//! alike, and takes the hold while the screen's is still there; a newer
//! screen of the viewer lets the older one's go; what the viewer left --
//! the screen released, the cast unpublished -- is their idle share,
//! which keeps it running while idle sharing is allowed, until they watch
//! something else; and once nothing holds a torrent -- and the cell names
//! something else -- the reconciler stops it.
//!
//! What this replaced, and what each test would have caught: the
//! reconciler read "playing" off the liveness cell -- the last entity a
//! stream opened, which any other open moves -- and the reads open at the
//! tick. A phone that handed its film to a television had no read open
//! between the receiver's requests, and an open of anything else stopped
//! the torrent the television was waiting on (measured on a phone:
//! `torrent_stopped_by_reconciler playing=false` right after a cast began),
//! and deleted its bytes with it.
//!
//! The reconciler's decision is read with `ServerHandle::reconcile_as_timer`
//! -- the verdict, before the actuator -- so nothing here sleeps past a
//! tick. Every server is offline, on ephemeral ports, with the pin set an
//! embedder that pinned nothing hands in (`offline_config`): a torrent
//! nobody holds is slack, which is the point.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use enginefs::reconcile::Decision;
use enginefs::retention::holds::Holder;
use stream_server::{
    AudioPlan, CastToken, MediaId, MediaSpec, PinKey, PlayToken, RenditionSpec, ServerConfig,
    VideoPlan,
};

#[path = "support/torrent_fixtures.rs"]
mod torrent_fixtures;
use torrent_fixtures::{bearer_client, offline_config, real_torrent, seed_single_file};

#[path = "support/test_producer.rs"]
mod test_producer;
use test_producer::{Knobs, TestProducer};

/// A bound on a mistake, never a wait a correct run spends.
const BOUND: Duration = Duration::from_secs(60);

const PIECE: usize = 16 * 1024;
const FILM_LEN: usize = 16 * PIECE;

/// A film's bytes, different for each `seed` so two films are two torrents.
fn film(seed: u8) -> Vec<u8> {
    (0..FILM_LEN)
        .map(|at| ((at / 7) as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

/// One torrent of the fixture: its hash and the id its streaming URL is.
struct Torrent {
    info_hash: String,
    id: MediaId,
    bytes: Vec<u8>,
}

/// A server with two one-file torrents added, checked and seeded, and the
/// LAN listener up. `a` is the film being watched; `b` is what moves the
/// liveness cell away from it.
struct Fixture {
    handle: stream_server::ServerHandle,
    lan: String,
    a: Torrent,
    b: Torrent,
    _dirs: Vec<tempfile::TempDir>,
}

impl Fixture {
    /// **Both torrents start pinned**, because a seeded fixture has no
    /// swarm: a torrent nothing holds loses its pieces at the first tick
    /// after its check (`support/fixture_pins.rs`), before a test could open
    /// it. `b` stays pinned -- it is only what moves the cell -- and `a` is
    /// unpinned (bytes kept) once the screen holds it ([`Self::watch_a`]).
    fn start() -> anyhow::Result<Self> {
        let config_dir = tempfile::tempdir()?;
        let cache_dir = tempfile::tempdir()?;
        let cache_root = stream_server::resolved_path(&cache_dir.path().join("cache"));
        let mut dirs = vec![config_dir, cache_dir];
        let mut built = Vec::new();
        for seed in [1u8, 2] {
            let src = tempfile::tempdir()?;
            let content: PathBuf = src.path().join(format!("Film{seed}"));
            std::fs::create_dir_all(&content)?;
            let bytes = film(seed);
            std::fs::write(content.join("film.mkv"), &bytes)?;
            let (torrent, info_hash) = real_torrent(&content);
            built.push((torrent, info_hash, bytes));
            dirs.push(src);
        }
        stream_server::pretend_volume_space(&cache_root, u64::MAX);
        let handle = stream_server::start(ServerConfig {
            http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            config_dir: Some(dirs[0].path().join("config")),
            cache_dir: Some(cache_root.clone()),
            lan_media_addr: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            pins: Some(
                built
                    .iter()
                    .map(|(_, info_hash, _)| PinKey::Torrent {
                        info_hash: info_hash.clone(),
                        file_idx: 0,
                    })
                    .collect(),
            ),
            ..offline_config()
        })?;
        handle.update_settings(serde_json::json!({ "lanMediaEnabled": true }))?;
        let lan = handle
            .set_lan_media(true)?
            .ok_or_else(|| anyhow::anyhow!("no LAN address"))?;
        let base = format!("http://{}", handle.http_addr());
        let mut torrents = Vec::new();
        for (torrent, info_hash, bytes) in built {
            seed_single_file(&cache_root, &torrent, &bytes);
            bearer_client(&handle)?
                .post(format!("{base}/create"))
                .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
                .send()?
                .error_for_status()?;
            let deadline = Instant::now() + BOUND;
            loop {
                let stats = serde_json::to_value(handle.engine_stats(&info_hash, &[])?)?;
                match stats["phase"].as_str() {
                    Some("checking") | Some("resolvingMetadata") => {
                        anyhow::ensure!(Instant::now() < deadline, "never checked: {stats}");
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    _ => break,
                }
            }
            let id = handle.register(MediaSpec::StreamingUrl(url::Url::parse(&format!(
                "{base}/{info_hash}/0"
            ))?))?;
            torrents.push(Torrent {
                info_hash,
                id,
                bytes,
            });
        }
        let b = torrents.pop().expect("two torrents");
        let a = torrents.pop().expect("two torrents");
        Ok(Self {
            handle,
            lan: format!("http://{lan}"),
            a,
            b,
            _dirs: dirs,
        })
    }

    /// The screen `token` playing `a`, and `a`'s pin let go (its bytes
    /// kept): from here only what holds it keeps it.
    fn watch_a(&self, token: &str) -> anyhow::Result<stream_server::MediaReader> {
        let reader = self.play(&self.a, token)?;
        self.handle.unpin(&self.a.id, false)?;
        Ok(reader)
    }

    /// The viewer's player screen `token` opening `torrent` and reading its
    /// first bytes, as the app's player does through the id.
    fn play(&self, torrent: &Torrent, token: &str) -> anyhow::Result<stream_server::MediaReader> {
        let mut reader = self.handle.open_reader(&torrent.id, Some(play(token)))?;
        let mut head = vec![0u8; 4096];
        read_exact(&mut reader, &mut head)?;
        assert_eq!(head, torrent.bytes[..4096]);
        Ok(reader)
    }

    /// Something else opened -- here `b`, read as an aside and closed --
    /// which moves the liveness cell off `a`: what used to decide that `a`
    /// was nobody's.
    fn open_something_else(&self) -> anyhow::Result<()> {
        let mut reader = self.handle.open_reader(&self.b.id, None)?;
        let mut head = vec![0u8; 1024];
        read_exact(&mut reader, &mut head)?;
        assert_eq!(head, self.b.bytes[..1024]);
        Ok(())
    }

    /// What the reconciler wants of `a`, asked as its timer would.
    fn verdict_on_a(&self) -> anyhow::Result<Decision> {
        Ok(self
            .handle
            .reconcile_as_timer(&self.a.info_hash)?
            .ok_or_else(|| anyhow::anyhow!("the server lost the torrent"))?
            .decision)
    }

    /// The reconciler asked about `a` until it answers `Stop` -- the cut
    /// body or run lets go of its stream on a task of its own -- then the
    /// server stopped. Bounded.
    fn until_a_stops(self) -> anyhow::Result<()> {
        let deadline = Instant::now() + BOUND;
        while self.verdict_on_a()? != Decision::Stop {
            anyhow::ensure!(
                Instant::now() < deadline,
                "a torrent nothing holds was never stopped"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        self.stop()
    }

    fn holders_of_a(&self) -> Vec<Holder> {
        self.handle.torrent_holders(&self.a.info_hash)
    }

    fn stop(self) -> anyhow::Result<()> {
        self.handle.shutdown()?;
        self.handle.join()?;
        Ok(())
    }
}

fn play(token: &str) -> PlayToken {
    PlayToken {
        token: token.to_string(),
        buffer: Default::default(),
    }
}

fn read_exact(reader: &mut stream_server::MediaReader, buf: &mut [u8]) -> anyhow::Result<()> {
    let mut at = 0;
    while at < buf.len() {
        let read = reader.read(&mut buf[at..])?;
        anyhow::ensure!(read > 0, "the reader ended at {at}");
        at += read;
    }
    Ok(())
}

/// **A player screen holds its torrent until it is left**: paused with no
/// read open, and with something else opened since, the torrent is still
/// the screen's and runs; the screen's goodbye leaves it as the viewer's
/// idle share, which runs it while idle sharing is allowed (the default)
/// and not while the app holds idle sharing back; and the viewer starting
/// something else lets it go, and the reconciler stops it.
#[test]
fn a_player_screen_holds_its_torrent_until_it_is_left() -> anyhow::Result<()> {
    let fixture = Fixture::start()?;
    drop(fixture.watch_a("viewer.1")?);
    fixture.open_something_else()?;

    assert_eq!(
        fixture.holders_of_a(),
        vec![Holder::Player("viewer.1".into())]
    );
    assert_eq!(
        fixture.verdict_on_a()?,
        Decision::Run,
        "a screen paused with nothing open, after another open, lost its torrent"
    );

    assert!(fixture.handle.release_player("viewer.1"));
    assert_eq!(
        fixture.holders_of_a(),
        vec![Holder::IdleShare("viewer".into())]
    );
    assert_eq!(
        fixture.verdict_on_a()?,
        Decision::Run,
        "what the viewer watched last stopped sharing while idle sharing was allowed"
    );
    fixture.handle.set_idle_sharing_held(true)?;
    assert_eq!(fixture.verdict_on_a()?, Decision::Stop, "held back");
    fixture.handle.set_idle_sharing_held(false)?;
    assert_eq!(fixture.verdict_on_a()?, Decision::Run, "and given back");

    drop(fixture.play(&fixture.b, "viewer.2")?);
    assert_eq!(fixture.holders_of_a(), Vec::<Holder>::new());
    fixture.until_a_stops()
}

/// **A newer screen of the viewer lets the older one's torrent go**: the
/// screen that never said goodbye holds nothing once the viewer plays
/// something else.
#[test]
fn a_newer_screen_of_the_viewer_lets_the_older_ones_torrent_go() -> anyhow::Result<()> {
    let fixture = Fixture::start()?;
    drop(fixture.watch_a("viewer.1")?);
    drop(fixture.play(&fixture.b, "viewer.2")?);
    assert_eq!(fixture.holders_of_a(), Vec::<Holder>::new());
    assert_eq!(fixture.verdict_on_a()?, Decision::Stop);
    // The old screen's goodbye, late, takes nothing from the new one.
    fixture.handle.release_player("viewer.1");
    assert_eq!(
        fixture.handle.torrent_holders(&fixture.b.info_hash),
        vec![Holder::Player("viewer.2".into())]
    );
    fixture.stop()
}

/// **A cast holds its torrent from publish to unpublish**, and the hand-over
/// from the phone has no gap: the cast is published while the screen still
/// holds the torrent, the screen goes, something else opens, and the
/// torrent runs and serves the receiver; the unpublish lets it go.
#[test]
fn a_cast_holds_its_torrent_from_publish_to_unpublish() -> anyhow::Result<()> {
    let fixture = Fixture::start()?;
    let reader = fixture.watch_a("viewer.1")?;
    let token = fixture
        .handle
        .publish(&fixture.a.id, Some(play("viewer.1")))?;
    drop(reader);
    fixture.handle.release_player("viewer.1");
    fixture.open_something_else()?;

    assert_eq!(fixture.holders_of_a(), vec![Holder::Cast]);
    assert_eq!(
        fixture.verdict_on_a()?,
        Decision::Run,
        "the torrent a television was handed was stopped under it"
    );
    let served = reqwest::blocking::Client::builder()
        .timeout(BOUND)
        .build()?
        .get(format!("{}/cast/{}", fixture.lan, token.as_str()))
        .header(reqwest::header::RANGE, format!("bytes={}-", 8 * PIECE))
        .send()?
        .bytes()?;
    assert_eq!(served.as_ref(), &fixture.a.bytes[8 * PIECE..]);

    assert!(fixture.handle.unpublish(&token));
    // The cast was what the viewer watched last: their idle share now.
    assert_eq!(
        fixture.holders_of_a(),
        vec![Holder::IdleShare("viewer".into())]
    );
    // Something else opened is not something else watched.
    fixture.open_something_else()?;
    assert_eq!(fixture.verdict_on_a()?, Decision::Run);
    // The viewer starting another film is.
    drop(fixture.play(&fixture.b, "viewer.2")?);
    fixture.until_a_stops()
}

/// **A rendition holds its torrent from publish to unpublish**, before the
/// receiver has asked for anything and between its requests -- and keeps
/// its bytes: with something else opened and the phone's player gone, the
/// rendition still makes its slots out of the torrent.
#[test]
fn a_rendition_holds_its_torrent_and_its_bytes_from_publish_to_unpublish() -> anyhow::Result<()> {
    let fixture = Fixture::start()?;
    let producer = TestProducer::new(Knobs {
        length: Duration::from_secs(10),
        // A read of the source at every sync sample, spread over the file.
        read_stride: Some(3 * PIECE as u64),
        ..Knobs::default()
    });
    fixture.handle.install_producer(producer.clone());
    let reader = fixture.watch_a("viewer.1")?;
    let token = fixture.handle.publish_rendition(
        &fixture.a.id,
        RenditionSpec {
            duration_ms: 10_000,
            segment_ms: 1000,
            start_ms: 0,
            video: VideoPlan::Copy,
            audio: AudioPlan::Copy,
            audio_track: 0,
        },
        Some(play("viewer.1")),
    )?;
    drop(reader);
    fixture.handle.release_player("viewer.1");
    fixture.open_something_else()?;

    assert_eq!(fixture.holders_of_a(), vec![Holder::Cast]);
    assert_eq!(
        fixture.verdict_on_a()?,
        Decision::Run,
        "a rendition nobody has asked for yet lost its torrent"
    );
    // A pass of the retention owner, as the tick runs one: nothing of a
    // held torrent is slack.
    fixture.handle.reconcile_as_timer(&fixture.b.info_hash)?;
    let slot = made(&fixture.handle, &token, 3)?;
    assert!(!slot.is_empty());

    assert!(fixture.handle.unpublish(&token));
    // The cast was what the viewer watched last: their idle share now.
    assert_eq!(
        fixture.holders_of_a(),
        vec![Holder::IdleShare("viewer".into())]
    );
    // Something else opened is not something else watched.
    fixture.open_something_else()?;
    assert_eq!(fixture.verdict_on_a()?, Decision::Run);
    // The viewer starting another film is.
    drop(fixture.play(&fixture.b, "viewer.2")?);
    fixture.until_a_stops()
}

/// Slot `slot` of the rendition, or an error once [`BOUND`] has passed: a
/// read parked on a piece the server let go of never ends by itself, so
/// the bound unpublishes, which cuts it.
fn made(
    handle: &stream_server::ServerHandle,
    token: &CastToken,
    slot: u64,
) -> anyhow::Result<bytes::Bytes> {
    std::thread::scope(|scope| {
        let (tx, rx) = std::sync::mpsc::channel();
        scope.spawn(move || {
            let made = handle
                .rendition_init(token)
                .and_then(|_| handle.rendition_segment(token, slot));
            let _ = tx.send(made);
        });
        match rx.recv_timeout(BOUND) {
            Ok(made) => made.map_err(|miss| anyhow::anyhow!("slot {slot}: {miss:?}")),
            Err(_) => {
                handle.unpublish(token);
                anyhow::bail!("slot {slot} never came: its source's bytes are gone")
            }
        }
    })
}

/// **A cast of something that is not a torrent, published with the
/// viewer's token, is the viewer watching something else**: their idle
/// share goes, and the torrent it kept stops.
#[test]
fn a_cast_of_a_link_ends_the_viewers_idle_share() -> anyhow::Result<()> {
    let fixture = Fixture::start()?;
    drop(fixture.watch_a("viewer.1")?);
    assert!(fixture.handle.release_player("viewer.1"));
    assert_eq!(
        fixture.holders_of_a(),
        vec![Holder::IdleShare("viewer".into())]
    );
    let link = fixture
        .handle
        .register(MediaSpec::StreamingUrl(url::Url::parse(
            "http://127.0.0.1:9/film.mkv",
        )?))?;
    let token = fixture.handle.publish(&link, Some(play("viewer.2")))?;
    assert_eq!(fixture.holders_of_a(), Vec::<Holder>::new());
    fixture.handle.unpublish(&token);
    // The liveness cell still names `a`, and keeps it running while the
    // reconciler reads it; something else opened moves it.
    fixture.open_something_else()?;
    fixture.until_a_stops()
}
