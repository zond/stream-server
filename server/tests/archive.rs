// The archive-by-URL routes end to end: a `/create` that fetches an archive
// from an origin, a `/stream` that serves a member of it, and what the two
// leave on disk. Every server here binds ephemeral ports (see embed.rs).

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// The hand-built RAR archives (see the module), shared with the
/// translator's own tests.
#[cfg(feature = "rar")]
#[path = "support/rar_fixtures.rs"]
mod rar_fixtures;

fn offline_config() -> stream_server::ServerConfig {
    stream_server::ServerConfig {
        resolve_dht_bootstrap_names: false,
        // Neither file adds a BitTorrent torrent today, so nothing here
        // reaches `merged_trackers` -- but the two halves of "offline"
        // belong together, and `embed.rs` paid for having only one of them.
        use_public_trackers: false,
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

/// The same three members as a zip: one stored (a film does not compress,
/// so a film in a zip is stored), one deflated, and an empty one.
fn fixture_zip() -> Vec<u8> {
    let runtime = tokio::runtime::Runtime::new().expect("a runtime to write the fixture with");
    runtime.block_on(async {
        let mut writer = async_zip::base::write::ZipFileWriter::with_tokio(Vec::new());
        for (name, data, compression) in [
            (
                "first.txt",
                FIRST_CONTENT.to_vec(),
                async_zip::Compression::Stored,
            ),
            (
                "videos/second.bin",
                second_content(),
                async_zip::Compression::Stored,
            ),
            // Deliberately smaller than the stored member, so that the
            // member a create with no `fileIdx` picks -- the largest --
            // is the stored one, as it is in a real archive of a film.
            (
                "videos/packed.bin",
                second_content()[..64 * 1024].to_vec(),
                async_zip::Compression::Deflate,
            ),
            ("empty.txt", Vec::new(), async_zip::Compression::Stored),
        ] {
            writer
                .write_entry_whole(
                    async_zip::ZipEntryBuilder::new(name.into(), compression).build(),
                    &data,
                )
                .await
                .expect("write the member");
        }
        writer.close().await.expect("close the zip").into_inner()
    })
}

/// The same members as a plain `.tar`, where every one of them is stored.
fn fixture_tar() -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
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
    builder.into_inner().expect("finish tar")
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

/// Everything under this prefix is served **without** honouring `Range`:
/// an origin that answers a ranged request with the whole entity, which is
/// the one a translated source refuses rather than downloads.
const NO_RANGES: &str = "/whole-only";

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
    let mut range = None;
    loop {
        let mut header = String::new();
        match reader.read_line(&mut header) {
            Ok(0) => break,
            Ok(_) if header.trim().is_empty() => break,
            Ok(_) => {
                if let Some(value) = header.to_ascii_lowercase().strip_prefix("range:") {
                    range = parse_range(value.trim());
                }
            }
            Err(_) => break,
        }
    }
    let path = line.split_whitespace().nth(1).unwrap_or("/").to_string();
    *seen.lock().unwrap().entry(path.clone()).or_default() += 1;
    let Some(body) = bodies.get(&path) else {
        let body = &b"nope"[..];
        let head = format!(
            "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(head.as_bytes());
        let _ = stream.write_all(body);
        return;
    };
    // A real origin: ranges answered as ranges, with the validator the
    // cache files the entity by.
    let head = match range.filter(|_| !path.starts_with(NO_RANGES)) {
        Some((first, last)) if first < body.len() => {
            let last = last.min(body.len() - 1);
            let slice = &body[first..=last];
            let head = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\n\
                 ETag: \"the-archive\"\r\nAccept-Ranges: bytes\r\n\
                 Content-Range: bytes {first}-{last}/{}\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n",
                body.len(),
                slice.len()
            );
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(slice);
            let _ = stream.flush();
            return;
        }
        _ => format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n\
             ETag: \"the-archive\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        ),
    };
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
}

/// `bytes=first-last`, as this origin needs it.
fn parse_range(value: &str) -> Option<(usize, usize)> {
    let (first, last) = value.strip_prefix("bytes=")?.split_once('-')?;
    let first = first.parse().ok()?;
    let last = if last.is_empty() {
        usize::MAX
    } else {
        last.parse().ok()?
    };
    Some((first, last))
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
    #[allow(unused_mut)]
    let mut bodies = HashMap::from([
        ("/fixture.7z".to_string(), archive.clone()),
        // The same archive behind a URL that names no format.
        ("/download?id=7".to_string(), archive),
        ("/fixture.tgz".to_string(), fixture_tgz()),
        ("/fixture.zip".to_string(), fixture_zip()),
        ("/fixture.tar".to_string(), fixture_tar()),
        // The same zip behind an origin that answers a ranged request
        // with the whole entity.
        (format!("{NO_RANGES}/fixture.zip"), fixture_zip()),
        (
            "/notes.txt".to_string(),
            b"just some text, not an archive".to_vec(),
        ),
        (
            "/broken.zip".to_string(),
            b"PK\x03\x04 and then garbage".to_vec(),
        ),
        // Only the name matters to a build without RAR.
        (
            "/fixture.rar".to_string(),
            b"Rar!\x1a\x07\x01\x00 and then garbage".to_vec(),
        ),
    ]);
    #[cfg(feature = "rar")]
    bodies.extend(rar_bodies());
    let origin = Origin::start(bodies)?;
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

/// The RAR archives the origin serves: the same two members as the other
/// fixtures, stored in one volume and across three, and one archive for
/// each way a RAR is refused.
#[cfg(feature = "rar")]
fn rar_bodies() -> HashMap<String, Vec<u8>> {
    use rar_fixtures::{Method, Rar5Options};
    let second = second_content();
    let members = [
        ("first.txt", FIRST_CONTENT),
        ("videos/second.bin", &second[..]),
    ];
    let mut bodies = HashMap::from([
        ("/film.rar".to_string(), rar_fixtures::rar5_stored(&members)),
        (
            "/packed.rar".to_string(),
            rar_fixtures::rar5_archive(
                &members,
                &Rar5Options {
                    method: Method::Normal,
                    ..Default::default()
                },
            ),
        ),
        (
            "/locked.rar".to_string(),
            rar_fixtures::rar5_header_encrypted(),
        ),
        (
            "/solid.rar".to_string(),
            rar_fixtures::rar5_archive(
                &members,
                &Rar5Options {
                    solid: true,
                    method: Method::Normal,
                    ..Default::default()
                },
            ),
        ),
    ]);
    // Three volumes: the first holds `first.txt` and the head of the film,
    // the third its tail (see `RAR_VOLUME_BYTES`).
    let volumes = rar_fixtures::rar5_volumes(&members, RAR_VOLUME_BYTES);
    assert_eq!(volumes.len(), 3, "the fixture is a three-volume set");
    for (at, volume) in volumes.into_iter().enumerate() {
        bodies.insert(format!("/film.part{}.rar", at + 1), volume);
    }
    bodies
}

/// How much member data each volume of the RAR set holds.
#[cfg(feature = "rar")]
const RAR_VOLUME_BYTES: usize = 100_000;

impl Fixture {
    /// `POST /7zip/create` for `url`; the session key on success.
    fn create(&self, url: &str) -> anyhow::Result<reqwest::blocking::Response> {
        Ok(reqwest::blocking::Client::new()
            .post(format!("{}/7zip/create", self.base))
            .json(&serde_json::json!({ "urls": [url] }))
            .send()?)
    }

    /// `POST /{prefix}/create` for `url`.
    fn create_for(&self, prefix: &str, url: &str) -> anyhow::Result<reqwest::blocking::Response> {
        Ok(reqwest::blocking::Client::new()
            .post(format!("{}/{prefix}/create", self.base))
            .json(&serde_json::json!({ "urls": [url] }))
            .send()?)
    }

    fn create_key_for(&self, prefix: &str, url: &str) -> anyhow::Result<String> {
        let response = self.create_for(prefix, url)?;
        anyhow::ensure!(
            response.status() == reqwest::StatusCode::OK,
            "create answered {}: {}",
            response.status(),
            response.text()?
        );
        let body: serde_json::Value = response.json()?;
        Ok(body["key"].as_str().expect("a key").to_string())
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

/// **A create under a key that is in use does not swap the archive out from
/// under a player** (review #67). `/{fmt}/create/{key}` takes the key from
/// the caller, and every `/{fmt}/stream/{key}/...` after it reads whatever
/// that key now names -- so a second caller (on Android, any app on the
/// device) could point a live session at an archive of its own. A repeat of
/// the same create still lands: a re-play sends it again.
#[test]
fn a_create_cannot_take_over_another_archives_session_key() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let url = fixture.origin.url("/fixture.7z");
    let other = fixture.origin.url("/download?id=7");
    let client = reqwest::blocking::Client::new();
    let create_with_key = |key: &str, url: &str| -> anyhow::Result<reqwest::blocking::Response> {
        Ok(client
            .post(format!("{}/7zip/create/{key}", fixture.base))
            .json(&serde_json::json!({ "urls": [url] }))
            .send()?)
    };

    assert_eq!(
        create_with_key("mine", &url)?.status(),
        reqwest::StatusCode::OK
    );
    // The same archive again is a re-play, not a takeover.
    assert_eq!(
        create_with_key("mine", &url)?.status(),
        reqwest::StatusCode::OK
    );
    // A different one is refused, and the session still names the first.
    assert_eq!(
        create_with_key("mine", &other)?.status(),
        reqwest::StatusCode::CONFLICT
    );
    let response = client
        .get(format!("{}/7zip/stream/mine/first.txt", fixture.base))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.bytes()?.as_ref(), FIRST_CONTENT);

    fixture.finish()
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

/// An archive is fetched from a web address and from nowhere else.
///
/// The create routes are open to any loopback caller -- on Android, every
/// app on the device, and any page in a browser on it -- and a `url` that
/// was not http(s) was taken as a path on this machine: the members of any
/// archive the server could read, its own private storage included, were
/// served to whoever asked.
#[test]
fn an_archive_on_this_machines_disk_is_not_opened() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let bytes = reqwest::blocking::get(fixture.origin.url("/download?id=7"))?
        .error_for_status()?
        .bytes()?;
    let local = tempfile::tempdir()?;
    let path = local.path().join("private.7z");
    std::fs::write(&path, &bytes)?;

    for url in [
        path.to_string_lossy().into_owned(),
        format!("file://{}", path.display()),
    ] {
        let response = fixture.create(&url)?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "{url} was opened: {}",
            response.text()?
        );
    }
    assert!(fixture.scratch_files().is_empty());
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

/// **A stored member behind a web link is served as byte ranges of the
/// link**, whole and by range and backwards, and nothing is written
/// anywhere: the archive is never downloaded, the member is never
/// extracted, and `<cacheRoot>/.archives` -- which every archive this
/// server played used to put two copies of the film in -- is not so much
/// as created.
///
/// The seek backwards is the case the old shape could not do at all
/// without paying for the member again: a player opens, reads the head,
/// jumps to the tail for the index, and comes back. Here each of those is
/// one ranged read of the link.
#[test]
fn a_stored_member_behind_a_link_is_served_by_range_and_nothing_is_written() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let client = reqwest::blocking::Client::new();
    let expected = second_content();

    for (prefix, archive) in [("zip", "/fixture.zip"), ("tar", "/fixture.tar")] {
        let key = fixture.create_key_for(prefix, &fixture.origin.url(archive))?;
        let member = format!("{}/{prefix}/stream/{key}/videos/second.bin", fixture.base);

        let whole = client.get(&member).send()?;
        assert_eq!(whole.status(), reqwest::StatusCode::OK);
        assert_eq!(
            whole
                .headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok()),
            Some(expected.len().to_string().as_str())
        );
        assert_eq!(whole.bytes()?.as_ref(), expected.as_slice());

        // The tail, and then a seek back to the head: two ranges out of
        // the middle of one member, each answered as itself.
        let tail = client
            .get(&member)
            .header(
                reqwest::header::RANGE,
                format!("bytes={}-", expected.len() - 1024),
            )
            .send()?;
        assert_eq!(tail.status(), reqwest::StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            tail.headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok()),
            Some(
                format!(
                    "bytes {}-{}/{}",
                    expected.len() - 1024,
                    expected.len() - 1,
                    expected.len()
                )
                .as_str()
            )
        );
        assert_eq!(tail.bytes()?.as_ref(), &expected[expected.len() - 1024..]);

        let back = client
            .get(&member)
            .header(reqwest::header::RANGE, "bytes=4096-8191")
            .send()?;
        assert_eq!(back.status(), reqwest::StatusCode::PARTIAL_CONTENT);
        assert_eq!(back.bytes()?.as_ref(), &expected[4096..8192]);

        // And a `HEAD` promises exactly what the `GET` delivered.
        let head = client.head(&member).send()?;
        assert_eq!(head.status(), reqwest::StatusCode::OK);
        assert_eq!(
            head.headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok()),
            Some(expected.len().to_string().as_str())
        );
        assert!(head.bytes()?.is_empty());
    }

    assert!(
        !fixture.scratch_dir.exists(),
        "the translated path wrote under the cache root: {:?}",
        fixture.scratch_files()
    );
    fixture.finish()
}

/// **A compressed member is refused, with a sentence the player shows.**
/// It is not extracted, not partially decoded and not served
/// sequentially: reaching the end of a deflated film means inflating the
/// whole of it, which is the thing this server does not do.
#[test]
fn a_compressed_member_is_refused_with_a_sentence() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let url = fixture.origin.url("/fixture.zip");
    let client = reqwest::blocking::Client::new();

    // At the create, which is where a player learns it before it starts.
    let refused = client
        .post(format!("{}/zip/create", fixture.base))
        .json(&serde_json::json!({ "urls": [url], "fileMustInclude": ["packed.bin"] }))
        .send()?;
    assert_eq!(
        refused.status(),
        reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    let body: serde_json::Value = refused.json()?;
    assert_eq!(body["refused"], serde_json::json!("compressed"));
    let message = body["message"].as_str().unwrap_or_default();
    assert!(message.contains("deflate"), "{body}");

    // And at the member, for a client that asks for it by name anyway.
    let key = fixture.create_key_for("zip", &url)?;
    let member = client
        .get(format!(
            "{}/zip/stream/{key}/videos/packed.bin",
            fixture.base
        ))
        .send()?;
    assert_eq!(member.status(), reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(
        member.json::<serde_json::Value>()?["refused"],
        serde_json::json!("compressed")
    );

    assert!(!fixture.scratch_dir.exists(), "nothing was extracted");
    fixture.finish()
}

/// **A `tar.gz` is refused whole**: gzip is one stream with no way in at
/// the middle, so there is no member of it this server can point at. It
/// used to be extracted, every time, in full.
#[test]
fn a_tar_gz_is_refused_because_it_has_no_way_in() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let refused = reqwest::blocking::Client::new()
        .post(format!("{}/tgz/create", fixture.base))
        .json(&serde_json::json!({ "urls": [fixture.origin.url("/fixture.tgz")] }))
        .send()?;
    assert_eq!(
        refused.status(),
        reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    let body: serde_json::Value = refused.json()?;
    assert_eq!(body["refused"], serde_json::json!("noRandomAccess"));
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|message| message.contains("tar.gz")),
        "{body}"
    );
    assert!(!fixture.scratch_dir.exists(), "nothing was extracted");
    fixture.finish()
}

/// **An origin that will not serve ranges is refused**, and the refusal
/// says why in a sentence: serving a member out of it would mean
/// downloading the whole archive, which is what this design exists to
/// stop. `501`, because it is this server that declines to do the work.
#[test]
fn an_origin_that_will_not_range_is_refused() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let refused = reqwest::blocking::Client::new()
        .post(format!("{}/zip/create", fixture.base))
        .json(&serde_json::json!({
            "urls": [fixture.origin.url(&format!("{NO_RANGES}/fixture.zip"))]
        }))
        .send()?;
    assert_eq!(refused.status(), reqwest::StatusCode::NOT_IMPLEMENTED);
    let body: serde_json::Value = refused.json()?;
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("byte ranges")),
        "{body}"
    );
    assert!(!fixture.scratch_dir.exists());
    fixture.finish()
}

/// An empty member is an empty body with a length of 0 -- it was answered
/// `Content-Length: 1` over a body that ended at once -- and a range past
/// the end of a member is a `416` naming its length, not the whole member
/// under a `200`.
///
/// Over a `.tar` where it used to be over a `.tar.gz`: the claim is about
/// the framing every media response shares (`routes::util::MediaRange`),
/// and the container it is made through is now one whose members can be
/// pointed at.
#[test]
fn an_empty_member_is_empty_and_a_range_past_the_end_is_refused() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let key = fixture.create_key_for("tar", &fixture.origin.url("/fixture.tar"))?;
    let client = reqwest::blocking::Client::new();

    let empty = client
        .get(format!("{}/tar/stream/{key}/empty.txt", fixture.base))
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
            "{}/tar/stream/{key}/videos/second.bin",
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

/// A 7z inside a torrent is refused as a format the torrent form cannot
/// read, before anything looks for the torrent. It used to go through the
/// torrent's file list, register a stream -- which starts a torrent the
/// reconciler had stopped -- and open a reader, and then answer 404 for any
/// member at all, because the 7z handler refuses a stream at its first open.
#[test]
fn a_7z_inside_a_torrent_is_refused_as_unreadable_rather_than_missing() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let response = reqwest::blocking::get(format!(
        "{}/7zip/stream/torrent:{}%2Ffixture.7z/first.txt",
        fixture.base,
        "ab".repeat(20)
    ))?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_IMPLEMENTED);
    fixture.finish()
}

/// Whole, by range, backwards and `HEAD`: the same four requests the zip
/// and tar test makes, over `member`, against `expected`.
#[cfg(feature = "rar")]
fn assert_served_by_range(
    client: &reqwest::blocking::Client,
    member: &str,
    expected: &[u8],
) -> anyhow::Result<()> {
    let whole = client.get(member).send()?;
    assert_eq!(whole.status(), reqwest::StatusCode::OK);
    assert_eq!(
        whole
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(expected.len().to_string().as_str())
    );
    assert_eq!(whole.bytes()?.as_ref(), expected);

    let tail = client
        .get(member)
        .header(
            reqwest::header::RANGE,
            format!("bytes={}-", expected.len() - 1024),
        )
        .send()?;
    assert_eq!(tail.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        tail.headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok()),
        Some(
            format!(
                "bytes {}-{}/{}",
                expected.len() - 1024,
                expected.len() - 1,
                expected.len()
            )
            .as_str()
        )
    );
    assert_eq!(tail.bytes()?.as_ref(), &expected[expected.len() - 1024..]);

    let back = client
        .get(member)
        .header(reqwest::header::RANGE, "bytes=4096-8191")
        .send()?;
    assert_eq!(back.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(back.bytes()?.as_ref(), &expected[4096..8192]);

    let head = client.head(member).send()?;
    assert_eq!(head.status(), reqwest::StatusCode::OK);
    assert_eq!(
        head.headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(expected.len().to_string().as_str())
    );
    assert!(head.bytes()?.is_empty());
    Ok(())
}

/// **A stored RAR member behind a link is byte ranges of the archive**,
/// like a zip's: nothing downloaded, nothing extracted, nothing under
/// `.archives`. Until this step `/rar/create` fetched the whole archive
/// into the cache root before it could name a member.
#[cfg(feature = "rar")]
#[test]
fn a_stored_rar_member_behind_a_link_is_served_by_range_and_nothing_is_written()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let client = reqwest::blocking::Client::new();
    let key = fixture.create_key_for("rar", &fixture.origin.url("/film.rar"))?;
    let member = format!("{}/rar/stream/{key}/videos/second.bin", fixture.base);
    assert_served_by_range(&client, &member, &second_content())?;
    assert!(
        !fixture.scratch_dir.exists(),
        "the translated path wrote under the cache root: {:?}",
        fixture.scratch_files()
    );
    fixture.finish()
}

/// **A RAR that cannot be served by range is refused with a sentence**, at
/// the create: a compressed member (`rar`'s default), an archive whose
/// headers are encrypted, a solid archive. Each is `415` with the kind the
/// client switches on, and none of them puts a byte under `.archives`.
#[cfg(feature = "rar")]
#[test]
fn a_rar_that_cannot_be_served_by_range_is_refused_with_a_sentence() -> anyhow::Result<()> {
    let fixture = fixture()?;
    for (archive, kind, says) in [
        (
            "/packed.rar",
            "compressed",
            "compressed inside the rar (normal)",
        ),
        ("/locked.rar", "encrypted", "encrypted"),
        ("/solid.rar", "solid", "solid block"),
    ] {
        let response = fixture.create_for("rar", &fixture.origin.url(archive))?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "{archive}"
        );
        let body: serde_json::Value = response.json()?;
        assert_eq!(body["refused"], kind, "{archive}: {body}");
        assert!(
            body["message"]
                .as_str()
                .is_some_and(|message| message.contains(says)),
            "{archive}: {body}"
        );
    }
    assert!(
        !fixture.scratch_dir.exists(),
        "{:?}",
        fixture.scratch_files()
    );
    fixture.finish()
}

/// The MIT build, which leaves unrar-rs out, answers a RAR session with
/// `501` and says why, rather than failing it as some other error or
/// handing the archive to a reader that is not there.
#[cfg(not(feature = "rar"))]
#[test]
fn a_build_without_rar_refuses_a_rar_archive_as_not_implemented() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let response = reqwest::blocking::Client::new()
        .post(format!("{}/rar/create", fixture.base))
        .json(&serde_json::json!({ "urls": [fixture.origin.url("/fixture.rar")] }))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_IMPLEMENTED);
    let body: serde_json::Value = response.json()?;
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("RAR")),
        "{body}"
    );
    fixture.finish()
}
