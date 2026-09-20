// The `/iso` routes end to end: a disc image inside a torrent read through
// the `torrent:` key form, and one behind a URL read through
// `/iso/create` -- an ISO 9660 image, a UDF image, an image the parser
// refuses, and, where a real tool is installed, an image a real tool wrote.
// Every server here binds ephemeral ports (see embed.rs); nothing here goes
// near the network.
//
// The fixtures are the images module's own
// (`stream_server::images::fixtures`), which is why that module is not
// `#[cfg(test)]`: what is proved here is that the same bytes the parser's
// unit tests index are served back by the routes, byte for byte, with
// nothing written under the cache root.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use stream_server::images::fixtures::{iso, udf};
use stream_server::{ServerConfig, ServerHandle};

fn offline_config() -> ServerConfig {
    ServerConfig {
        resolve_dht_bootstrap_names: false,
        use_public_trackers: false,
        pins: Some(Default::default()),
        ..ServerConfig::default()
    }
}

/// How long a torrent's initial check is given (`embed.rs` waits the same).
const CHECK_WAIT_BOUND: std::time::Duration = std::time::Duration::from_secs(30);

/// The tiny origin the URL tests read from: ranges answered as ranges,
/// with a validator, which is what `ProxySource` needs of an origin.
struct Origin {
    addr: SocketAddr,
}

impl Origin {
    fn start(bodies: HashMap<String, Vec<u8>>) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let bodies = Arc::new(bodies);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let bodies = bodies.clone();
                std::thread::spawn(move || serve_one(stream, &bodies));
            }
        });
        Ok(Self { addr })
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }
}

fn serve_one(mut stream: TcpStream, bodies: &HashMap<String, Vec<u8>>) {
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
    let Some(body) = bodies.get(&path) else {
        let _ = stream.write_all(
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnope",
        );
        return;
    };
    match range {
        Some((first, last)) if first < body.len() => {
            let last = last.min(body.len() - 1);
            let slice = &body[first..=last];
            let head = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\n\
                 ETag: \"the-image\"\r\nAccept-Ranges: bytes\r\n\
                 Content-Range: bytes {first}-{last}/{}\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n",
                body.len(),
                slice.len()
            );
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(slice);
        }
        _ => {
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n\
                 ETag: \"the-image\"\r\nAccept-Ranges: bytes\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(body);
        }
    }
    let _ = stream.flush();
}

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

/// A server with nothing in it, its base URL and its cache root.
struct Server {
    handle: ServerHandle,
    base: String,
    cache_root: PathBuf,
    _config_dir: tempfile::TempDir,
    _cache_dir: tempfile::TempDir,
}

fn server() -> anyhow::Result<Server> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let cache_root = stream_server::resolved_path(&cache_dir.path().join("cache"));
    stream_server::pretend_volume_space(&cache_root, u64::MAX);
    let handle = stream_server::start(ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    })?;
    let base = format!("http://{}", handle.http_addr());
    Ok(Server {
        handle,
        base,
        cache_root,
        _config_dir: config_dir,
        _cache_dir: cache_dir,
    })
}

impl Server {
    /// A torrent whose one file is `fixture.iso` holding `image`, added and
    /// checked, its pieces already in the piece store -- the shape
    /// `/iso/stream/torrent:<hash>%2Ffixture.iso/<member>` reads.
    fn add_image_torrent(&self, src: &Path, image: &[u8]) -> anyhow::Result<String> {
        let content = src.join("Disc");
        std::fs::create_dir_all(&content)?;
        std::fs::write(content.join("fixture.iso"), image)?;
        let (torrent, info_hash) = real_torrent(&content);
        // After the start, never before: the launch-time sweep deletes a
        // piece directory the pin set does not name (see
        // `embed.rs::seed_piece_store_pieces`).
        seed_piece_store(&self.cache_root, &torrent, image);
        let client = bearer_client(&self.handle)?;
        client
            .post(format!("{}/create", self.base))
            .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
            .send()?
            .error_for_status()?;
        let deadline = std::time::Instant::now() + CHECK_WAIT_BOUND;
        loop {
            let stats: serde_json::Value = client
                .get(format!("{}/{info_hash}/stats.json", self.base))
                .send()?
                .error_for_status()?
                .json()?;
            match stats["phase"].as_str() {
                Some("checking") | Some("resolvingMetadata") => {
                    anyhow::ensure!(
                        std::time::Instant::now() < deadline,
                        "still {} after {CHECK_WAIT_BOUND:?}: {stats}",
                        stats["phase"]
                    );
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                _ => break,
            }
        }
        Ok(info_hash)
    }

    /// `torrent:<info hash>/<path in the torrent>` is one path segment of
    /// the member URL, so the separator inside it is encoded.
    fn torrent_member_url(&self, info_hash: &str, member: &str) -> String {
        format!(
            "{}/iso/stream/torrent:{info_hash}%2Ffixture.iso/{member}",
            self.base
        )
    }

    /// `POST /iso/create` for `url`.
    fn create(&self, url: &str) -> anyhow::Result<reqwest::blocking::Response> {
        Ok(reqwest::blocking::Client::new()
            .post(format!("{}/iso/create", self.base))
            .json(&serde_json::json!({ "urls": [url] }))
            .send()?)
    }

    /// `POST /iso/create` for `url`, which must succeed; the member URL
    /// under the key it answered.
    fn create_member_url(&self, url: &str, member: &str) -> anyhow::Result<String> {
        let response = self.create(url)?;
        anyhow::ensure!(
            response.status() == reqwest::StatusCode::OK,
            "create answered {}: {}",
            response.status(),
            response.text()?
        );
        let body: serde_json::Value = response.json()?;
        let key = body["key"].as_str().expect("a key");
        Ok(format!("{}/iso/stream/{key}/{member}", self.base))
    }

    /// **Nothing under the cache root but the fetchers' own stores**: not
    /// an extraction, not the directory extractions used to land in.
    fn assert_nothing_extracted(&self) {
        assert!(
            !self.cache_root.join(".archives").exists(),
            "something wrote under the cache root"
        );
    }

    fn finish(self) -> anyhow::Result<()> {
        self.handle.shutdown()?;
        self.handle.join()?;
        Ok(())
    }
}

fn bearer_client(handle: &ServerHandle) -> anyhow::Result<reqwest::blocking::Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    let token = handle.auth_token().expect("every launch generates a token");
    headers.insert(
        reqwest::header::AUTHORIZATION,
        format!("Bearer {token}").parse().expect("valid header"),
    );
    Ok(reqwest::blocking::Client::builder()
        .default_headers(headers)
        .build()?)
}

/// A real `.torrent` of `dir`, and its info hash.
fn real_torrent(dir: &Path) -> (Vec<u8>, String) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let t = librqbit::create_torrent(
            dir,
            librqbit::CreateTorrentOptions {
                name: None,
                trackers: Vec::new(),
                piece_length: Some(16384),
            },
            &librqbit::spawn_utils::BlockingSpawner::new(1),
        )
        .await
        .expect("create torrent");
        (
            t.as_bytes().expect("serialize").to_vec(),
            t.info_hash().as_string(),
        )
    })
}

/// Pre-seed a one-file torrent's every piece where the server reads them:
/// the piece store. The single-file case of `embed.rs`'s
/// `seed_piece_store_pieces`, with the layout asked of the store.
fn seed_piece_store(cache_root: &Path, torrent_bytes: &[u8], content: &[u8]) {
    let meta = librqbit::torrent_from_bytes(torrent_bytes).expect("parse the torrent back");
    let info_hash = meta.info_hash.as_string();
    let info = meta.info.data.validate().expect("validated metainfo");
    let piece_length = info.lengths().default_piece_length() as u64;
    assert_eq!(
        content.len() as u64,
        info.lengths().total_length(),
        "the fixture and the torrent disagree about the payload"
    );
    let store =
        enginefs::piece_store::StoreRoot::in_download_dir(&cache_root.join("rqbit-downloads"));
    let layout = enginefs::piece_store::PieceLayout::new(
        piece_length,
        content.len() as u64,
        [enginefs::piece_store::FileSpec {
            len: content.len() as u64,
            padding: false,
        }],
    )
    .expect("a layout for the fixture");
    let pieces =
        enginefs::piece_store::PieceStore::new(store.torrent_dir(&info_hash), Arc::new(layout));
    for (index, piece) in content.chunks(piece_length as usize).enumerate() {
        let path = pieces.piece_path(index as u32);
        std::fs::create_dir_all(path.parent().expect("a bucket")).expect("piece bucket");
        std::fs::write(&path, piece).expect("write a piece");
    }
}

/// What a member of an image answers, whichever way the image came:
/// served whole, by a range in its middle, by a range back at its head,
/// and a `HEAD` that promises the same length -- exactly what a plain file
/// answers, since the framing is the stream route's own.
fn assert_member_served(url: &str, expected: &[u8]) -> anyhow::Result<()> {
    let anonymous = reqwest::blocking::Client::new();

    let whole = anonymous.get(url).send()?;
    assert_eq!(whole.status(), reqwest::StatusCode::OK, "{url}");
    assert_eq!(whole.bytes()?.as_ref(), expected);

    // Clamped into the file: a range past its end is answered clamped, as
    // for a plain file, and this is a test of the bytes, not of that.
    let last = expected.len() - 1;
    let (from, to) = (expected.len() / 2, (expected.len() / 2 + 99).min(last));
    let middle = anonymous
        .get(url)
        .header(reqwest::header::RANGE, format!("bytes={from}-{to}"))
        .send()?;
    assert_eq!(middle.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        middle
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok()),
        Some(format!("bytes {from}-{to}/{}", expected.len()).as_str())
    );
    assert_eq!(middle.bytes()?.as_ref(), &expected[from..=to]);

    let (from, to) = (7.min(last), 70.min(last));
    let back = anonymous
        .get(url)
        .header(reqwest::header::RANGE, format!("bytes={from}-{to}"))
        .send()?;
    assert_eq!(back.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(back.bytes()?.as_ref(), &expected[from..=to]);

    let head = anonymous.head(url).send()?;
    assert_eq!(head.status(), reqwest::StatusCode::OK);
    assert_eq!(
        head.headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(expected.len().to_string().as_str())
    );
    Ok(())
}

/// The file's bytes as the fixture wrote them: the range the fixture
/// states, out of the image.
fn file_bytes(image: &[u8], range: (u64, u64)) -> Vec<u8> {
    image[range.0 as usize..(range.0 + range.1) as usize].to_vec()
}

/// **An ISO 9660 image inside a torrent is served from the torrent** by the
/// `torrent:` key form, with no `/create`: a file of the image is a byte
/// range of the image, so a range request on it is a range read of the
/// piece store, and nothing is written under the cache root.
#[test]
fn a_9660_image_inside_a_torrent_is_served_by_range() -> anyhow::Result<()> {
    let src = tempfile::tempdir()?;
    let server = server()?;
    let image = iso::minimal_iso();
    let info_hash = server.add_image_torrent(src.path(), &image)?;

    assert_member_served(
        &server.torrent_member_url(&info_hash, "HELLO.TXT"),
        &file_bytes(&image, iso::data_range(&image)),
    )?;
    server.assert_nothing_extracted();
    server.finish()
}

/// The same image behind a URL: `/iso/create` reads the descriptors and
/// the directory through the proxy cache's ranged reads, and the member is
/// served out of the same cache. Nothing lands under the cache root but
/// the cache's own entity.
#[test]
fn a_9660_image_behind_a_link_is_served_by_range() -> anyhow::Result<()> {
    let image = iso::minimal_iso();
    let origin = Origin::start(HashMap::from([("/disc.iso".to_string(), image.clone())]))?;
    let server = server()?;

    let url = server.create_member_url(&origin.url("/disc.iso"), "HELLO.TXT")?;
    assert_member_served(&url, &file_bytes(&image, iso::data_range(&image)))?;
    server.assert_nothing_extracted();
    server.finish()
}

/// **A UDF image with no ISO 9660 tree** -- the Blu-ray shape -- goes the
/// same two ways: the 9660 parser finds no `CD001` and the UDF parser
/// takes over, and the file is served by range from the torrent.
#[test]
fn a_udf_image_inside_a_torrent_is_served_by_range() -> anyhow::Result<()> {
    let src = tempfile::tempdir()?;
    let server = server()?;
    let image = udf::minimal_udf();
    let info_hash = server.add_image_torrent(src.path(), &image)?;

    assert_member_served(
        &server.torrent_member_url(&info_hash, "MOVIE.BIN"),
        &file_bytes(&image, udf::data_range(&image)),
    )?;
    server.assert_nothing_extracted();
    server.finish()
}

/// A UDF image behind a link, and a file in a directory named by its path
/// -- `BDMV/STREAM/00000.m2ts`, written in two extents, read back as one
/// file.
#[test]
fn a_udf_image_behind_a_link_is_served_by_range() -> anyhow::Result<()> {
    let image = udf::udf_with_subdirectory();
    let origin = Origin::start(HashMap::from([("/bd.iso".to_string(), image.clone())]))?;
    let server = server()?;

    // What the file's bytes are is asked of the parser here, through the
    // library rather than the route: the route is what is under test, and
    // the parser has its own tests against this fixture.
    let expected = {
        let rt = tokio::runtime::Runtime::new()?;
        let indexed = rt.block_on(stream_server::images::index(
            &stream_server::images::MemoryImage::new(image.clone()),
        ))?;
        let file = indexed
            .files
            .iter()
            .find(|file| file.path == "/BDMV/STREAM/00000.m2ts")
            .expect("the fixture's file");
        assert_eq!(file.extents.len(), 2, "{:?}", file.extents);
        file.extents
            .iter()
            .flat_map(|extent| file_bytes(&image, (extent.offset, extent.len)))
            .collect::<Vec<u8>>()
    };
    let url = server.create_member_url(&origin.url("/bd.iso"), "BDMV/STREAM/00000.m2ts")?;
    assert_member_served(&url, &expected)?;
    server.assert_nothing_extracted();
    server.finish()
}

/// **An image the parser refuses is a status and a sentence**, in the
/// route's own mapping: a UDF metadata partition map -- what a real Blu-ray
/// image hits first -- is `415 unsupported` with the map named, and bytes
/// that are no image at all are `422 malformed`. Both ways in answer the
/// same, and neither writes anything.
#[test]
fn an_image_the_parser_refuses_answers_the_status_and_the_sentence() -> anyhow::Result<()> {
    let src = tempfile::tempdir()?;
    let origin = Origin::start(HashMap::from([
        (
            "/bluray.iso".to_string(),
            udf::udf_with_metadata_partition(),
        ),
        ("/notes.iso".to_string(), vec![0u8; 600 * 1024]),
    ]))?;
    let server = server()?;
    let anonymous = reqwest::blocking::Client::new();

    let refused = server.create(&origin.url("/bluray.iso"))?;
    assert_eq!(
        refused.status(),
        reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    let body: serde_json::Value = refused.json()?;
    assert_eq!(body["refused"], serde_json::json!("unsupported"));
    let message = body["message"].as_str().expect("a sentence");
    assert!(message.starts_with("this UDF image uses"), "{message}");
    assert!(message.contains("partition map"), "{message}");

    let not_an_image = server.create(&origin.url("/notes.iso"))?;
    assert_eq!(
        not_an_image.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );
    let body: serde_json::Value = not_an_image.json()?;
    assert_eq!(body["refused"], serde_json::json!("malformed"));
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|message| message.contains("not a disc image")),
        "{body}"
    );

    // The `torrent:` form has no create: the refusal is the answer to the
    // first member request.
    let info_hash = server.add_image_torrent(src.path(), &udf::udf_with_metadata_partition())?;
    let refused = anonymous
        .get(server.torrent_member_url(&info_hash, "BDMV/index.bdmv"))
        .send()?;
    assert_eq!(
        refused.status(),
        reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    let body: serde_json::Value = refused.json()?;
    assert_eq!(body["refused"], serde_json::json!("unsupported"));

    server.assert_nothing_extracted();
    server.finish()
}

/// A member the image does not have is a `404`, not a refusal and not an
/// empty body.
#[test]
fn a_file_the_image_does_not_have_is_not_found() -> anyhow::Result<()> {
    let image = iso::minimal_iso();
    let origin = Origin::start(HashMap::from([("/disc.iso".to_string(), image)]))?;
    let server = server()?;
    let url = server.create_member_url(&origin.url("/disc.iso"), "GOODBYE.TXT")?;
    let missing = reqwest::blocking::Client::new().get(url).send()?;
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    server.finish()
}

/// `genisoimage`, `mkisofs`, or `xorriso` in its mkisofs persona.
fn image_writer() -> Option<(String, Vec<String>)> {
    for (tool, args) in [
        ("genisoimage", vec![]),
        ("mkisofs", vec![]),
        ("xorriso", vec!["-as".to_string(), "mkisofs".to_string()]),
    ] {
        let found = std::process::Command::new("which")
            .arg(tool)
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false);
        if found {
            return Some((tool.to_string(), args));
        }
    }
    None
}

/// **An image a real tool wrote**, inside a torrent, served by range: a
/// fixture built in the test proves the route matches this reading of the
/// standard, and only a tool's image proves it matches what tools write.
/// Rock Ridge, Joliet and UDF all on, which is what a DVD-shaped image
/// carries, and a nested file large enough to span pieces.
///
/// Skipped, loudly, when no tool is installed: it is on the machine this
/// was written on and not on CI.
#[test]
fn a_real_image_inside_a_torrent_is_served_by_range() -> anyhow::Result<()> {
    let Some((tool, args)) = image_writer() else {
        eprintln!(
            "skipping a_real_image_inside_a_torrent_is_served_by_range: none of genisoimage, \
             mkisofs or xorriso is installed"
        );
        return Ok(());
    };
    let scratch = tempfile::tempdir()?;
    let tree = scratch.path().join("tree");
    std::fs::create_dir_all(tree.join("VIDEO_TS"))?;
    let vob: Vec<u8> = (0..200 * 1024u32)
        .map(|i| (i.wrapping_mul(31) % 251) as u8)
        .collect();
    std::fs::write(tree.join("VIDEO_TS").join("VTS_01_1.VOB"), &vob)?;
    std::fs::write(tree.join("README.TXT"), b"a real image\n")?;
    let image_path = scratch.path().join("real.iso");
    let status = std::process::Command::new(&tool)
        .args(&args)
        .args(["-quiet", "-r", "-J", "-udf", "-o"])
        .arg(&image_path)
        .arg(&tree)
        .status()?;
    anyhow::ensure!(status.success(), "{tool} failed: {status}");
    let image = std::fs::read(&image_path)?;

    let src = tempfile::tempdir()?;
    let server = server()?;
    let info_hash = server.add_image_torrent(src.path(), &image)?;
    assert_member_served(
        &server.torrent_member_url(&info_hash, "VIDEO_TS/VTS_01_1.VOB"),
        &vob,
    )?;
    assert_member_served(
        &server.torrent_member_url(&info_hash, "README.TXT"),
        b"a real image\n",
    )?;
    server.assert_nothing_extracted();
    server.finish()
}
