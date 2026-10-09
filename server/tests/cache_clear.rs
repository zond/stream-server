//! **A full clear of the cache** (`ServerHandle::clear_cache`): what the
//! user asks for when they want the space back now, whatever is playing.
//! Unlike `clean_cache_now`, which gives back only what nobody plays, it
//! stops every torrent that streams -- the one a player screen is reading,
//! the one a viewer's idle share keeps running -- and takes every piece no
//! download keeps, the window of the film being played included; a kept
//! download keeps every byte and goes on running. The player that was
//! reading gets a read error, and a later ask is served as a first one is.
//!
//! Every server is offline, on ephemeral ports, with the pin set an
//! embedder that pinned nothing hands in: a torrent nobody holds is slack,
//! which is the point. The reconciler's decision is read with
//! `ServerHandle::reconcile_as_timer`, so nothing here sleeps past a tick.

use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use enginefs::reconcile::Decision;
use enginefs::retention::holds::Holder;
use stream_server::{MediaId, MediaSpec, PinKey, PlayToken, ServerConfig};

#[path = "support/torrent_fixtures.rs"]
mod torrent_fixtures;
use torrent_fixtures::{
    bearer_client, offline_config, pieces_held, real_torrent, seed_single_file,
};

/// A bound on a mistake, never a wait a correct run spends.
const BOUND: Duration = Duration::from_secs(60);

const PIECE: usize = 16 * 1024;
const FILM_LEN: usize = 16 * PIECE;

/// A film's bytes, different for each `seed` so two films are two torrents.
fn film(seed: u8) -> Vec<u8> {
    (0..FILM_LEN)
        .map(|at| ((at / 7) as u8).wrapping_mul(29).wrapping_add(seed))
        .collect()
}

struct Torrent {
    info_hash: String,
    id: MediaId,
    bytes: Vec<u8>,
}

/// A server with four one-file torrents added, checked and seeded: `a` is
/// the film a player screen is reading, `b` what another viewer watched
/// last and left as their idle share, `c` a kept download, and `d` read by
/// a client with no player behind it, which shares nothing.
struct Fixture {
    handle: stream_server::ServerHandle,
    cache_root: PathBuf,
    a: Torrent,
    b: Torrent,
    c: Torrent,
    d: Torrent,
    _dirs: Vec<tempfile::TempDir>,
}

impl Fixture {
    /// **Every torrent starts pinned**, because a seeded fixture has no
    /// swarm: a torrent nothing holds loses its pieces at the first tick
    /// after its check (`support/fixture_pins.rs`). `c` stays pinned -- it
    /// is the kept download -- and the others are unpinned, bytes kept,
    /// once something reads each.
    fn start() -> anyhow::Result<Self> {
        let config_dir = tempfile::tempdir()?;
        let cache_dir = tempfile::tempdir()?;
        let cache_root = stream_server::resolved_path(&cache_dir.path().join("cache"));
        let mut dirs = vec![config_dir, cache_dir];
        let mut built = Vec::new();
        for seed in [1u8, 2, 3, 4] {
            let src = tempfile::tempdir()?;
            let content: PathBuf = src.path().join(format!("Clear{seed}"));
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
        let d = torrents.pop().expect("four torrents");
        let c = torrents.pop().expect("four torrents");
        let b = torrents.pop().expect("four torrents");
        let a = torrents.pop().expect("four torrents");
        Ok(Self {
            handle,
            cache_root,
            a,
            b,
            c,
            d,
            _dirs: dirs,
        })
    }

    /// The viewer's player screen `token` opening `torrent` and reading its
    /// first bytes, as the app's player does through the id; then the
    /// torrent's pin let go, bytes kept, so only what holds it keeps it.
    fn watch(&self, torrent: &Torrent, token: &str) -> anyhow::Result<stream_server::MediaReader> {
        self.read(torrent, Some(play(token)))
    }

    /// [`Self::watch`], for a player (`play`) or another client (`None`).
    fn read(
        &self,
        torrent: &Torrent,
        play: Option<PlayToken>,
    ) -> anyhow::Result<stream_server::MediaReader> {
        let mut reader = self.handle.open_reader(&torrent.id, play)?;
        let mut head = vec![0u8; 4096];
        read_exact(&mut reader, &mut head)?;
        assert_eq!(head, torrent.bytes[..4096]);
        self.handle.unpin(&torrent.id, false)?;
        Ok(reader)
    }

    /// What the reconciler wants of `torrent`, asked as its timer would.
    fn verdict(&self, torrent: &Torrent) -> anyhow::Result<Decision> {
        Ok(self
            .handle
            .reconcile_as_timer(&torrent.info_hash)?
            .ok_or_else(|| anyhow::anyhow!("the server lost the torrent"))?
            .decision)
    }

    fn holders(&self, torrent: &Torrent) -> Vec<Holder> {
        self.handle.torrent_holders(&torrent.info_hash)
    }

    fn pieces(&self, torrent: &Torrent) -> usize {
        pieces_held(&self.cache_root, &torrent.info_hash)
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

/// **A clear stops what streams, takes every piece no download keeps, and
/// leaves the download alone.** A player screen reading `a` with its window
/// on the disk, another viewer's idle share running `b`, a kept download
/// `c`, another client reading `d`: after the clear nothing holds `a`, `b`
/// or `d`, the reconciler stops all three although the readers of `a` and
/// `d` are still open, every piece of them is gone, and `c` runs with
/// every byte. The player's next read fails, and so does a
/// seek; the report says what left the disk and how many torrents stopped.
#[test]
fn a_clear_stops_what_streams_and_takes_every_piece_no_download_keeps() -> anyhow::Result<()> {
    let fixture = Fixture::start()?;
    drop(fixture.watch(&fixture.b, "other.1")?);
    assert!(fixture.handle.release_player("other.1"));
    let mut reader = fixture.watch(&fixture.a, "viewer.1")?;
    let aside = fixture.read(&fixture.d, None)?;

    assert_eq!(
        fixture.holders(&fixture.a),
        vec![Holder::Player("viewer.1".into())]
    );
    assert_eq!(
        fixture.holders(&fixture.b),
        vec![Holder::IdleShare("other".into())]
    );
    for torrent in [&fixture.a, &fixture.b, &fixture.c, &fixture.d] {
        assert_eq!(fixture.verdict(torrent)?, Decision::Run);
    }
    assert!(
        fixture.pieces(&fixture.d) > 0,
        "what the other client reads"
    );
    assert!(
        fixture.pieces(&fixture.a) > 0,
        "the film being played is on the disk"
    );
    assert!(
        fixture.pieces(&fixture.b) > 0,
        "and what the idle share keeps"
    );
    assert_eq!(fixture.pieces(&fixture.c), FILM_LEN / PIECE);
    // The gentle clean takes none of it: all three are somebody's.
    fixture.handle.clean_cache_now()?;
    assert!(fixture.pieces(&fixture.a) > 0 && fixture.pieces(&fixture.b) > 0);

    let report = fixture.handle.clear_cache()?;

    assert_eq!(
        report.stopped, 3,
        "the played torrent, the idle share and the other client's: {report:?}"
    );
    assert!(report.freed > 0, "{report:?}");
    assert_eq!(
        report.total,
        fixture.handle.cache_usage()?.total_bytes,
        "what is left is what the cache says it holds"
    );
    assert_eq!(fixture.holders(&fixture.a), Vec::<Holder>::new());
    assert_eq!(fixture.holders(&fixture.b), Vec::<Holder>::new());
    assert_eq!(
        fixture.verdict(&fixture.a)?,
        Decision::Stop,
        "a torrent the clear stopped runs again for the reader it cut"
    );
    assert_eq!(fixture.verdict(&fixture.b)?, Decision::Stop);
    assert_eq!(fixture.verdict(&fixture.d)?, Decision::Stop);
    assert_eq!(fixture.pieces(&fixture.d), 0, "what the other client read");
    assert_eq!(fixture.pieces(&fixture.a), 0, "the played window is gone");
    assert_eq!(fixture.pieces(&fixture.b), 0, "and the idle share's bytes");
    assert_eq!(
        fixture.verdict(&fixture.c)?,
        Decision::Run,
        "a kept download goes on"
    );
    assert_eq!(
        fixture.pieces(&fixture.c),
        FILM_LEN / PIECE,
        "with every byte"
    );

    // The player that was reading is told its stream ended, and stays told.
    let mut buf = vec![0u8; 4096];
    let error = reader.read(&mut buf).expect_err("a read after the clear");
    assert_eq!(error.kind(), ErrorKind::ConnectionAborted, "{error}");
    let error = reader.seek(0).expect_err("a seek after the clear");
    assert_eq!(error.kind(), ErrorKind::ConnectionAborted, "{error}");
    drop(reader);
    drop(aside);

    // A new ask is served as a first ask is: the viewer's next screen's
    // request holds the torrent again and the reconciler runs it. Asked on
    // the stream route, whose response is answered before its body parks
    // on a piece the clear took (there is no swarm to bring it back).
    let response = reqwest::blocking::Client::builder()
        .timeout(BOUND)
        .build()?
        .get(format!(
            "http://{}/{}/0?p=viewer.2",
            fixture.handle.http_addr(),
            fixture.a.info_hash
        ))
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    assert!(response.status().is_success(), "{}", response.status());
    assert_eq!(
        fixture.holders(&fixture.a),
        vec![Holder::Player("viewer.2".into())]
    );
    assert_eq!(fixture.verdict(&fixture.a)?, Decision::Run);
    drop(response);
    // And a stream with no player behind it -- another client -- counts as
    // any body being delivered does: what the clear set aside was the
    // bodies from before it.
    let response = reqwest::blocking::Client::builder()
        .timeout(BOUND)
        .build()?
        .get(format!(
            "http://{}/{}/0",
            fixture.handle.http_addr(),
            fixture.b.info_hash
        ))
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    assert!(response.status().is_success(), "{}", response.status());
    assert_eq!(fixture.holders(&fixture.b), Vec::<Holder>::new());
    assert_eq!(fixture.verdict(&fixture.b)?, Decision::Run);
    drop(response);
    fixture.stop()
}
