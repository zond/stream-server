//! **A download of what is not a torrent**, end to end: an addon URL pinned
//! through `ServerHandle::pin_proxy_download`, filled from a loopback origin into the proxy
//! cache, played back from the disk with the origin asked for nothing, kept
//! across a restart that names it and swept by one that does not, and
//! deleted on request. What `docs/generic-downloads.md` describes, measured.
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use stream_server::{ProxyDownloadRequest, ProxyPinKey, ServerConfig, ServerHandle};

fn byte_at(offset: usize) -> u8 {
    (offset % 251) as u8
}

const ORIGIN_LENGTH: usize = 3 * 256 * 1024 + 12_345;
const ORIGIN_ETAG: &str = "\"the-download\"";

#[derive(Clone, Debug)]
struct Request {
    headers: Vec<(String, String)>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
    fn range(&self) -> Option<&str> {
        self.header("range")
    }
}

/// A loopback origin that serves `ORIGIN_LENGTH` deterministic bytes by
/// range (or whole, when `ranges` is off -- an origin that will not range)
/// and records every request it was asked.
struct Origin {
    addr: SocketAddr,
    requests: std::sync::mpsc::Receiver<Request>,
}

impl Origin {
    fn start(ranges: bool) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (sender, requests) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let Ok(peer) = stream.try_clone() else { break };
                let sender = sender.clone();
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(peer);
                    let mut line = String::new();
                    if reader.read_line(&mut line).is_err() {
                        return;
                    }
                    let mut headers = vec![];
                    loop {
                        let mut header = String::new();
                        match reader.read_line(&mut header) {
                            Ok(0) => break,
                            Ok(_) if header.trim().is_empty() => break,
                            Ok(_) => {}
                            Err(_) => break,
                        }
                        if let Some((name, value)) = header.split_once(':') {
                            headers.push((name.trim().to_string(), value.trim().to_string()));
                        }
                    }
                    let request = Request { headers };
                    let _ = sender.send(request.clone());
                    answer(&request, &mut stream, ranges);
                });
            }
        });
        Ok(Self { addr, requests })
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    fn asked(&self) -> Vec<Request> {
        let mut asked = Vec::new();
        while let Ok(request) = self.requests.try_recv() {
            asked.push(request);
        }
        asked
    }
}

fn answer(request: &Request, socket: &mut TcpStream, ranges: bool) {
    let served = ranges.then(|| request.range()).flatten().and_then(|value| {
        let (first, last) = value.trim_start_matches("bytes=").split_once('-')?;
        let first: usize = first.parse().ok()?;
        let last: usize = if last.is_empty() {
            ORIGIN_LENGTH - 1
        } else {
            last.parse().ok()?
        };
        Some((first, last.min(ORIGIN_LENGTH - 1)))
    });
    let (head, body) = match served {
        Some((first, last)) => {
            let body: Vec<u8> = (first..=last).map(byte_at).collect();
            (
                format!(
                    "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\n\
                     Content-Type: video/mp4\r\nETag: {ORIGIN_ETAG}\r\n\
                     Content-Range: bytes {first}-{last}/{ORIGIN_LENGTH}\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                ),
                body,
            )
        }
        None => {
            let body: Vec<u8> = (0..ORIGIN_LENGTH).map(byte_at).collect();
            (
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nETag: {ORIGIN_ETAG}\r\n\
                     Content-Length: {ORIGIN_LENGTH}\r\nConnection: close\r\n\r\n"
                ),
                body,
            )
        }
    };
    let _ = socket.write_all(head.as_bytes());
    let _ = socket.write_all(&body);
    let _ = socket.flush();
}

fn offline_config() -> ServerConfig {
    ServerConfig {
        resolve_dht_bootstrap_names: false,
        use_public_trackers: false,
        pins: Some(Default::default()),
        // An embedder that keeps a proxy pin record and has pinned nothing.
        proxy_pins: Some(Vec::new()),
        ..ServerConfig::default()
    }
}

struct Fixture {
    handle: ServerHandle,
    cache_root: tempfile::TempDir,
    _config_dir: tempfile::TempDir,
}

impl Fixture {
    fn start(proxy_pins: Option<Vec<ProxyPinKey>>) -> anyhow::Result<Self> {
        let config_dir = tempfile::tempdir()?;
        let cache_root = tempfile::tempdir()?;
        Self::start_in(config_dir, cache_root, proxy_pins)
    }

    fn start_in(
        config_dir: tempfile::TempDir,
        cache_root: tempfile::TempDir,
        proxy_pins: Option<Vec<ProxyPinKey>>,
    ) -> anyhow::Result<Self> {
        let handle = stream_server::start(ServerConfig {
            http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            config_dir: Some(config_dir.path().join("config")),
            cache_dir: Some(cache_root.path().join("cache")),
            proxy_pins,
            ..offline_config()
        })?;
        Ok(Self {
            handle,
            cache_root,
            _config_dir: config_dir,
        })
    }

    /// Stops the server and answers the directories to start another over.
    fn stop(self) -> anyhow::Result<(tempfile::TempDir, tempfile::TempDir)> {
        self.handle.shutdown()?;
        self.handle.join()?;
        Ok((self._config_dir, self.cache_root))
    }

    fn proxy_root(&self) -> PathBuf {
        self.cache_root
            .path()
            .join("cache")
            .join("rqbit-downloads")
            .join(".proxy")
    }

    /// A pin of `url`, as the app asks for one; the row as JSON, which is
    /// how it crosses FFI.
    fn pin_url(&self, url: &str) -> anyhow::Result<serde_json::Value> {
        let row = self
            .handle
            .pin_proxy_download(url_request(url, Some("the film")))
            .map_err(|error| anyhow::anyhow!("pin refused: {error:#}"))?;
        Ok(serde_json::to_value(row)?)
    }

    fn downloads(&self) -> anyhow::Result<Vec<serde_json::Value>> {
        let rows = serde_json::to_value(self.handle.downloads()?)?;
        Ok(rows.as_array().cloned().unwrap_or_default())
    }

    /// Polls the listing until the row for `key` is complete.
    fn wait_complete(&self, key: &str) -> anyhow::Result<serde_json::Value> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let rows = self.downloads()?;
            if let Some(row) = rows
                .iter()
                .find(|row| row["infoHash"] == key && row["complete"] == true)
            {
                return Ok(row.clone());
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "the download did not complete: {rows:?}"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// What the app sends for an addon link: the URL, no headers, a name.
fn url_request(url: &str, name: Option<&str>) -> ProxyDownloadRequest {
    ProxyDownloadRequest {
        url: Some(url.to_string()),
        headers: BTreeMap::new(),
        drive_file_id: None,
        refresh_token: None,
        name: name.map(str::to_string),
    }
}

fn key_dir_exists(fixture: &Fixture, key: &str) -> bool {
    fixture.proxy_root().join(key).is_dir()
}

/// The whole life of a URL download: pinned, filled from the origin by
/// range, listed as complete with the bytes counted, played back off the
/// disk with the origin asked for nothing, and deleted on request.
#[test]
fn a_url_download_is_filled_played_from_disk_and_deleted() -> anyhow::Result<()> {
    let origin = Origin::start(true)?;
    let fixture = Fixture::start(Some(Vec::new()))?;
    let url = origin.url("/films/the-film.mp4");

    let row = fixture.pin_url(&url)?;
    let key = row["infoHash"].as_str().expect("a key").to_string();
    assert_eq!(
        key.len(),
        64,
        "a proxy download's key is the cache key: {key}"
    );
    assert_eq!(row["source"]["kind"], "url");
    assert_eq!(row["source"]["target"], url);
    assert_eq!(row["name"], "the film");
    assert!(
        row["playUrl"]
            .as_str()
            .is_some_and(|play| play.ends_with(&format!("/downloads/{key}/stream"))),
        "a download plays from its own media route: {}",
        row["playUrl"]
    );

    let done = fixture.wait_complete(&key)?;
    assert_eq!(done["length"], ORIGIN_LENGTH as u64);
    assert_eq!(
        done["downloaded"], ORIGIN_LENGTH as u64,
        "every byte is on the disk"
    );
    assert_eq!(done["phase"], "ready");
    let asked = origin.asked();
    assert!(
        asked.iter().all(|request| request.range().is_some()),
        "the filler asks by range and never for the whole file: {asked:?}"
    );
    assert!(!asked.is_empty(), "the origin was asked at least once");

    // Playback: the /proxy URL of the same stream, a player's ranged read,
    // served from the disk -- the origin hears nothing.
    let play = done["playUrl"].as_str().expect("a play URL").to_string();
    let response = reqwest::blocking::Client::new()
        .get(&play)
        .header(reqwest::header::RANGE, "bytes=0-")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let body = response.bytes()?;
    assert_eq!(body.len(), ORIGIN_LENGTH);
    assert!(
        body.iter().enumerate().all(|(i, byte)| *byte == byte_at(i)),
        "the bytes played are the bytes downloaded"
    );
    assert!(
        origin.asked().is_empty(),
        "a complete download plays without the origin"
    );
    assert!(key_dir_exists(&fixture, &key));

    // Deleted on request: the pin goes and so do the bytes.
    let outcome = fixture.handle.unpin_proxy_download(&key, true)?;
    assert!(outcome.unpinned);
    assert!(outcome.deleted_files);
    assert!(!key_dir_exists(&fixture, &key), "the key directory is gone");
    assert!(
        fixture
            .downloads()?
            .iter()
            .all(|row| row["infoHash"] != key),
        "and the listing no longer has it"
    );
    fixture.stop()?;
    Ok(())
}

/// A restart keeps what the embedder's record names and sweeps what it
/// does not; the kept download lists as complete and plays without the
/// origin ever being asked again.
#[test]
fn a_pinned_download_survives_a_restart_and_an_unpinned_one_is_swept() -> anyhow::Result<()> {
    let origin = Origin::start(true)?;
    let fixture = Fixture::start(Some(Vec::new()))?;
    let kept_url = origin.url("/kept.mp4");
    let dropped_url = origin.url("/dropped.mp4");
    let kept = fixture.pin_url(&kept_url)?["infoHash"]
        .as_str()
        .unwrap()
        .to_string();
    let dropped = fixture.pin_url(&dropped_url)?["infoHash"]
        .as_str()
        .unwrap()
        .to_string();
    fixture.wait_complete(&kept)?;
    fixture.wait_complete(&dropped)?;
    let (config_dir, cache_root) = fixture.stop()?;
    origin.asked();

    let record = vec![ProxyPinKey::Url {
        target: kept_url.clone(),
        headers: BTreeMap::new(),
    }];
    let fixture = Fixture::start_in(config_dir, cache_root, Some(record))?;
    assert!(
        key_dir_exists(&fixture, &kept),
        "the named download survived the sweep"
    );
    assert!(
        !key_dir_exists(&fixture, &dropped),
        "the unnamed one did not"
    );

    let rows = fixture.downloads()?;
    let row = rows
        .iter()
        .find(|row| row["infoHash"] == kept)
        .expect("the kept download is listed");
    assert_eq!(row["complete"], true, "{row}");
    assert_eq!(row["downloaded"], ORIGIN_LENGTH as u64);
    assert!(
        rows.iter().all(|row| row["infoHash"] != dropped),
        "the swept one is not"
    );
    assert!(origin.asked().is_empty(), "listing asks the origin nothing");

    let play = row["playUrl"].as_str().expect("a play URL").to_string();
    let response = reqwest::blocking::Client::new()
        .get(&play)
        .header(reqwest::header::RANGE, "bytes=1000-1999")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let body = response.bytes()?;
    assert!(
        body.iter()
            .enumerate()
            .all(|(i, byte)| *byte == byte_at(1000 + i))
    );
    assert!(
        origin.asked().is_empty(),
        "played off the disk after the restart"
    );

    // What the app does at every launch for what it kept: pin it again.
    // A whole entry has nothing to fetch, so the origin is not even probed
    // -- which is what makes the launch work with no network.
    let again = fixture.pin_url(&origin.url("/kept.mp4"))?;
    assert_eq!(again["infoHash"], kept);
    assert_eq!(again["complete"], true, "{again}");
    assert!(
        origin.asked().is_empty(),
        "a re-pin of a whole download asks the origin nothing"
    );
    fixture.stop()?;
    Ok(())
}

/// The piece store's silence rule, on this side: a boot that names no proxy
/// pin record at all sweeps nothing.
#[test]
fn a_boot_that_names_no_record_sweeps_nothing() -> anyhow::Result<()> {
    let origin = Origin::start(true)?;
    let fixture = Fixture::start(Some(Vec::new()))?;
    let key = fixture.pin_url(&origin.url("/kept.mp4"))?["infoHash"]
        .as_str()
        .unwrap()
        .to_string();
    fixture.wait_complete(&key)?;
    let (config_dir, cache_root) = fixture.stop()?;

    let fixture = Fixture::start_in(config_dir, cache_root, None)?;
    assert!(
        key_dir_exists(&fixture, &key),
        "nobody said what is pinned, so nothing was deleted"
    );
    assert!(
        fixture
            .downloads()?
            .iter()
            .all(|row| row["infoHash"] != key),
        "but nothing is listed as a download either: no record named it"
    );
    fixture.stop()?;
    Ok(())
}

/// An origin that answers a ranged request whole cannot be resumed and so
/// cannot be a download: refused at pin time, and nothing is fetched.
#[test]
fn an_origin_that_will_not_range_is_refused() -> anyhow::Result<()> {
    let origin = Origin::start(false)?;
    let fixture = Fixture::start(Some(Vec::new()))?;
    let refused = fixture
        .handle
        .pin_proxy_download(url_request(&origin.url("/whole.mp4"), None))
        .expect_err("an origin that answers a range whole is refused");
    assert!(
        matches!(
            refused.downcast_ref::<stream_server::ProxyPinError>(),
            Some(stream_server::ProxyPinError::Source(_))
        ),
        "{refused:#}"
    );
    assert!(!refused.to_string().is_empty());
    assert!(fixture.downloads()?.is_empty(), "nothing was pinned");
    assert_eq!(
        origin.asked().len(),
        1,
        "one probe, and no fill: {:?}",
        origin.asked()
    );
    fixture.stop()?;
    Ok(())
}

/// The key an embedder is told before it pins is the key the pin then has.
#[test]
fn the_key_is_known_before_the_pin() -> anyhow::Result<()> {
    let origin = Origin::start(true)?;
    let fixture = Fixture::start(Some(Vec::new()))?;
    let url = origin.url("/keyed.mp4");
    let foretold = fixture
        .handle
        .proxy_download_key(&ProxyPinKey::Url {
            target: url.clone(),
            headers: BTreeMap::new(),
        })
        .expect("a plain URL is keyable");
    let row = fixture.pin_url(&url)?;
    assert_eq!(row["infoHash"], foretold);
    assert!(
        fixture
            .handle
            .proxy_download_key(&ProxyPinKey::Url {
                target: "not a url".into(),
                headers: BTreeMap::new(),
            })
            .is_none(),
        "what cannot be keyed says so"
    );
    fixture.stop()?;
    Ok(())
}

/// A request that names neither a URL nor a Drive file is refused as
/// unkeyable, and one that names both is too.
#[test]
fn a_request_that_names_no_source_is_refused() -> anyhow::Result<()> {
    let fixture = Fixture::start(Some(Vec::new()))?;
    let neither = ProxyDownloadRequest {
        url: None,
        headers: BTreeMap::new(),
        drive_file_id: None,
        refresh_token: None,
        name: Some("nothing".into()),
    };
    let both = ProxyDownloadRequest {
        drive_file_id: Some("abc".into()),
        ..url_request("http://127.0.0.1:1/x", None)
    };
    for request in [neither, both] {
        let refused = fixture
            .handle
            .pin_proxy_download(request)
            .expect_err("a source that is not one thing is refused");
        assert!(
            matches!(
                refused.downcast_ref::<stream_server::ProxyPinError>(),
                Some(stream_server::ProxyPinError::Unkeyable(_))
            ),
            "{refused:#}"
        );
    }
    assert!(fixture.downloads()?.is_empty(), "nothing was pinned");
    fixture.stop()?;
    Ok(())
}
