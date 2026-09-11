// The archive-by-URL routes end to end: a `/create` that fetches an archive
// from an origin, a `/stream` that serves a member of it, and what the two
// leave on disk. Every server here binds ephemeral ports (see embed.rs).

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

fn offline_config() -> stream_server::ServerConfig {
    stream_server::ServerConfig {
        resolve_dht_bootstrap_names: false,
        // An embedder that keeps a pin record and has nothing in it yet.
        // `None` is not the same thing -- it is "nobody said", which keeps
        // every torrent's data and reports it all as pinned -- and it has a
        // test of its own; spreading it here would turn every retention and
        // idle-pause test in the file into one about a cache that may not be
        // touched.
        pins: Some(Default::default()),
        ..stream_server::ServerConfig::default()
    }
}

const FIRST_CONTENT: &[u8] = b"hello from the first entry\n";

/// Deterministic, mildly incompressible content large enough to span
/// several reads and a mid-file range.
fn second_content() -> Vec<u8> {
    (0..256 * 1024u32)
        .map(|i| (i.wrapping_mul(31) % 251) as u8)
        .collect()
}

/// A 7z archive with two members, as bytes. 7z because `sevenz-rust2` can
/// write one and is already a dependency.
fn fixture_7z(dir: &Path) -> Vec<u8> {
    use sevenz_rust2::{ArchiveEntry, ArchiveWriter};
    let path = dir.join("fixture.7z");
    let mut writer = ArchiveWriter::create(&path).expect("create 7z writer");
    writer
        .push_archive_entry(
            ArchiveEntry::new_file("first.txt"),
            Some(std::io::Cursor::new(FIRST_CONTENT.to_vec())),
        )
        .expect("push first entry");
    writer
        .push_archive_entry(
            ArchiveEntry::new_file("videos/second.bin"),
            Some(std::io::Cursor::new(second_content())),
        )
        .expect("push second entry");
    writer.finish().expect("finish 7z archive");
    std::fs::read(&path).expect("read fixture back")
}

/// The same two members as a `.tar.gz`, and an empty one.
fn fixture_tgz() -> Vec<u8> {
    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let mut builder = tar::Builder::new(encoder);
    for (name, data) in [
        ("first.txt", FIRST_CONTENT.to_vec()),
        ("videos/second.bin", second_content()),
        ("empty.txt", Vec::new()),
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, name, data.as_slice())
            .expect("append tar entry");
    }
    builder
        .into_inner()
        .expect("finish tar")
        .finish()
        .expect("finish gzip")
}

/// An HTTP/1.1 origin serving fixed bodies by path, counting the requests
/// for each. `Connection: close` on every response keeps it to one request
/// per socket.
struct Origin {
    addr: SocketAddr,
    requests: Arc<Mutex<HashMap<String, usize>>>,
}

impl Origin {
    fn start(bodies: HashMap<String, Vec<u8>>) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let requests = Arc::new(Mutex::new(HashMap::new()));
        let bodies = Arc::new(bodies);
        let seen = requests.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let bodies = bodies.clone();
                let seen = seen.clone();
                std::thread::spawn(move || serve_one(stream, &bodies, &seen));
            }
        });
        Ok(Self { addr, requests })
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    fn requests_for(&self, path: &str) -> usize {
        self.requests
            .lock()
            .unwrap()
            .get(path)
            .copied()
            .unwrap_or(0)
    }
}

fn serve_one(
    mut stream: TcpStream,
    bodies: &HashMap<String, Vec<u8>>,
    seen: &Mutex<HashMap<String, usize>>,
) {
    let mut reader = BufReader::new(stream.try_clone().expect("clone socket"));
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        return;
    }
    loop {
        let mut header = String::new();
        match reader.read_line(&mut header) {
            Ok(0) => break,
            Ok(_) if header.trim().is_empty() => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }
    let path = line.split_whitespace().nth(1).unwrap_or("/").to_string();
    *seen.lock().unwrap().entry(path.clone()).or_default() += 1;
    let (status, body) = match bodies.get(&path) {
        Some(body) => ("200 OK", body.as_slice()),
        None => ("404 Not Found", &b"nope"[..]),
    };
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/octet-stream\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
}

struct Fixture {
    handle: stream_server::ServerHandle,
    base: String,
    origin: Origin,
    /// `<cache root>/.archives`, where everything the archive routes write
    /// must land.
    scratch_dir: PathBuf,
    _cache_root: tempfile::TempDir,
    _config_dir: tempfile::TempDir,
}

fn fixture() -> anyhow::Result<Fixture> {
    let config_dir = tempfile::tempdir()?;
    let cache_root = tempfile::tempdir()?;
    let archive = fixture_7z(cache_root.path());
    let origin = Origin::start(HashMap::from([
        ("/fixture.7z".to_string(), archive.clone()),
        // The same archive behind a URL that names no format.
        ("/download?id=7".to_string(), archive),
        ("/fixture.tgz".to_string(), fixture_tgz()),
        (
            "/notes.txt".to_string(),
            b"just some text, not an archive".to_vec(),
        ),
        (
            "/broken.zip".to_string(),
            b"PK\x03\x04 and then garbage".to_vec(),
        ),
    ]))?;
    let cache_dir = cache_root.path().join("cache");
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.clone()),
        ..offline_config()
    })?;
    let base = format!("http://{}", handle.http_addr());
    Ok(Fixture {
        handle,
        base,
        origin,
        scratch_dir: cache_dir.join(".archives"),
        _cache_root: cache_root,
        _config_dir: config_dir,
    })
}

impl Fixture {
    /// `POST /7zip/create` for `url`; the session key on success.
    fn create(&self, url: &str) -> anyhow::Result<reqwest::blocking::Response> {
        Ok(reqwest::blocking::Client::new()
            .post(format!("{}/7zip/create", self.base))
            .json(&serde_json::json!({ "urls": [url] }))
            .send()?)
    }

    fn create_key(&self, url: &str) -> anyhow::Result<String> {
        let response = self.create(url)?;
        anyhow::ensure!(
            response.status() == reqwest::StatusCode::OK,
            "create answered {}: {}",
            response.status(),
            response.text()?
        );
        let body: serde_json::Value = response.json()?;
        Ok(body["key"].as_str().expect("a key").to_string())
    }

    /// The files under the scratch directory, by name.
    fn scratch_files(&self) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(&self.scratch_dir) else {
            return Vec::new();
        };
        let mut names: Vec<String> = entries
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    fn finish(self) -> anyhow::Result<()> {
        self.handle.shutdown()?;
        self.handle.join()?;
        Ok(())
    }
}

/// An archive fetched by URL is stored under the cache root with the suffix
/// the reader is chosen by -- so it can be opened at all, which a download
/// without one never could -- a member of it is served with ranges, and a
/// second create of the same URL reuses the download rather than fetching
/// it again.
#[test]
fn an_archive_by_url_is_kept_under_the_cache_root_and_fetched_once() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let url = fixture.origin.url("/fixture.7z");

    let key = fixture.create_key(&url)?;
    let files = fixture.scratch_files();
    assert_eq!(
        files.len(),
        1,
        "the download, under the cache root: {files:?}"
    );
    assert!(
        files[0].starts_with("archive_") && files[0].ends_with(".7z"),
        "{files:?}"
    );

    let client = reqwest::blocking::Client::new();
    let member = format!("{}/7zip/stream/{key}/videos/second.bin", fixture.base);
    let expected = second_content();
    let whole = client.get(&member).send()?;
    assert_eq!(whole.status(), reqwest::StatusCode::OK);
    assert_eq!(whole.bytes()?.as_ref(), expected.as_slice());

    let ranged = client
        .get(&member)
        .header(reqwest::header::RANGE, "bytes=40000-40999")
        .send()?;
    assert_eq!(ranged.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        ranged
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok()),
        Some(format!("bytes 40000-40999/{}", expected.len()).as_str())
    );
    assert_eq!(ranged.bytes()?.as_ref(), &expected[40000..41000]);

    let again = client.get(&member).send()?;
    assert_eq!(again.status(), reqwest::StatusCode::OK);

    // Three requests on the member -- the shape of a player's head, tail
    // and seek -- were one extraction, kept beside the download for the
    // next request, not one per request.
    let files = fixture.scratch_files();
    assert_eq!(
        files
            .iter()
            .filter(|name| name.starts_with("archive_extract_"))
            .count(),
        1,
        "one extraction for three requests: {files:?}"
    );
    assert_eq!(
        files.iter().filter(|name| name.ends_with(".7z")).count(),
        1,
        "{files:?}"
    );

    let second_key = fixture.create_key(&url)?;
    assert_ne!(second_key, key, "a session per create");
    assert_eq!(
        fixture.origin.requests_for("/fixture.7z"),
        1,
        "the second create reused the first's download"
    );
    assert_eq!(
        fixture
            .scratch_files()
            .iter()
            .filter(|name| name.ends_with(".7z"))
            .count(),
        1,
        "and downloaded nothing"
    );

    fixture.finish()
}

/// A URL that names no format is still an archive if its bytes are one:
/// the download is named by what it holds.
#[test]
fn a_url_without_a_suffix_is_named_by_its_bytes() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let key = fixture.create_key(&fixture.origin.url("/download?id=7"))?;
    let files = fixture.scratch_files();
    assert_eq!(files.len(), 1, "{files:?}");
    assert!(files[0].ends_with(".7z"), "{files:?}");

    let body = reqwest::blocking::get(format!("{}/7zip/stream/{key}/first.txt", fixture.base))?
        .error_for_status()?
        .bytes()?;
    assert_eq!(body.as_ref(), FIRST_CONTENT);
    fixture.finish()
}

/// Nothing a failed create fetched stays on disk: a body that is no archive
/// is refused before it is stored, and one that claims to be an archive and
/// will not open is deleted with the create that failed on it.
#[test]
fn a_failed_create_leaves_nothing_behind() -> anyhow::Result<()> {
    let fixture = fixture()?;

    let response = fixture.create(&fixture.origin.url("/notes.txt"))?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    assert!(
        fixture.scratch_files().is_empty(),
        "{:?}",
        fixture.scratch_files()
    );

    let response = fixture.create(&fixture.origin.url("/broken.zip"))?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(
        fixture.scratch_files().is_empty(),
        "{:?}",
        fixture.scratch_files()
    );

    let response = fixture.create(&fixture.origin.url("/missing.7z"))?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    assert!(fixture.scratch_files().is_empty());

    fixture.finish()
}

/// A member of a `.tar.gz` is served, whole and by range. Every request for
/// one used to be a 500: the extraction's cache was made without the
/// member's length, and the route's seek from the end to learn it failed.
#[test]
fn a_tgz_member_is_served_whole_and_by_range() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let key = fixture.create_key(&fixture.origin.url("/fixture.tgz"))?;
    let client = reqwest::blocking::Client::new();
    let member = format!("{}/tgz/stream/{key}/videos/second.bin", fixture.base);
    let expected = second_content();

    let whole = client.get(&member).send()?;
    assert_eq!(whole.status(), reqwest::StatusCode::OK);
    assert_eq!(whole.bytes()?.as_ref(), expected.as_slice());

    let ranged = client
        .get(&member)
        .header(reqwest::header::RANGE, "bytes=40000-40999")
        .send()?;
    assert_eq!(ranged.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(ranged.bytes()?.as_ref(), &expected[40000..41000]);

    fixture.finish()
}

/// An empty member is an empty body with a length of 0 -- it was answered
/// `Content-Length: 1` over a body that ended at once -- and a range past
/// the end of a member is a `416` naming its length, not the whole member
/// under a `200`.
#[test]
fn an_empty_member_is_empty_and_a_range_past_the_end_is_refused() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let key = fixture.create_key(&fixture.origin.url("/fixture.tgz"))?;
    let client = reqwest::blocking::Client::new();

    let empty = client
        .get(format!("{}/tgz/stream/{key}/empty.txt", fixture.base))
        .send()?;
    assert_eq!(empty.status(), reqwest::StatusCode::OK);
    assert_eq!(
        empty
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some("0")
    );
    assert!(empty.bytes()?.is_empty());

    let len = second_content().len();
    let past = client
        .get(format!(
            "{}/tgz/stream/{key}/videos/second.bin",
            fixture.base
        ))
        .header(reqwest::header::RANGE, format!("bytes={len}-"))
        .send()?;
    assert_eq!(past.status(), reqwest::StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        past.headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok()),
        Some(format!("bytes */{len}").as_str())
    );

    fixture.finish()
}
