//! **The container sniff in `resolve`** (`docs/design/media-pipeline.md`
//! §2.9): a torrent file, a `/proxy` link or a file on this device whose
//! first bytes are a container's resolves to the member an archive URL
//! would have named -- with no `/create` and no `torrent:` form -- and a
//! film resolves as itself, saying whether its head was looked at.
//!
//! Every server here is offline and on ephemeral ports; the one origin is
//! a loopback fake of this file's own. A [`MediaReader`] blocks its caller
//! and refuses to be called from inside a runtime, so the tests read from
//! threads of their own.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use stream_server::media::MemberInfo;
use stream_server::{LocalFile, MediaId, MediaReader, MediaSpec, Resolved, ServerConfig};

#[path = "support/fixture_pins.rs"]
mod fixture_pins;

#[cfg(feature = "rar")]
#[path = "support/rar_fixtures.rs"]
mod rar_fixtures;

#[path = "support/torrent_fixtures.rs"]
mod torrent_fixtures;
use torrent_fixtures::{bearer_client, offline_config, piece_store, real_torrent};

/// A bound on a mistake, never a wait a correct run spends.
const BOUND: Duration = Duration::from_secs(120);

/// The fixtures' piece length (`real_torrent`'s).
const PIECE: usize = 16 * 1024;

/// A film: an EBML head, as a Matroska file starts, then a pattern that
/// says where each byte came from.
fn film(len: usize) -> Vec<u8> {
    let mut bytes: Vec<u8> = (0..len)
        .map(|at| (at.wrapping_mul(7) % 251) as u8)
        .collect();
    bytes[..4].copy_from_slice(&[0x1A, 0x45, 0xDF, 0xA3]);
    bytes
}

/// The member every archive here holds, and what to pick it by: the
/// largest file, as `/create` with no `fileIdx` picks.
const MEMBER: &str = "film.mkv";

fn member_bytes() -> Vec<u8> {
    film(5 * PIECE + 321)
}

fn stored_zip(members: &[(&str, &[u8])]) -> Vec<u8> {
    let runtime = tokio::runtime::Runtime::new().expect("a runtime to write the fixture with");
    runtime.block_on(async {
        let mut writer = async_zip::base::write::ZipFileWriter::with_tokio(Vec::new());
        for (name, data) in members {
            writer
                .write_entry_whole(
                    async_zip::ZipEntryBuilder::new((*name).into(), async_zip::Compression::Stored)
                        .build(),
                    data,
                )
                .await
                .expect("write the member");
        }
        writer.close().await.expect("close the zip").into_inner()
    })
}

fn stored_7z(members: &[(&str, &[u8])]) -> Vec<u8> {
    use sevenz_rust2::{ArchiveEntry, ArchiveWriter};
    let mut writer =
        ArchiveWriter::new(std::io::Cursor::new(Vec::new())).expect("create 7z writer");
    writer.set_content_methods(vec![sevenz_rust2::EncoderConfiguration::new(
        sevenz_rust2::EncoderMethod::COPY,
    )]);
    writer.set_encrypt_header(false);
    for (name, data) in members {
        writer
            .push_archive_entry(
                ArchiveEntry::new_file(name),
                Some(std::io::Cursor::new(data.to_vec())),
            )
            .expect("push an entry");
    }
    writer.finish().expect("finish 7z archive").into_inner()
}

fn plain_tar(members: &[(&str, &[u8])]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for (name, data) in members {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, name, *data)
            .expect("append tar entry");
    }
    builder.into_inner().expect("finish tar")
}

/// A self-extracting ZIP: a Windows executable's stub, then one stored
/// member, every offset counted from the start of the file (as `zip -A`
/// leaves one), written field by field.
fn self_extracting_zip(name: &str, data: &[u8]) -> Vec<u8> {
    let mut out = b"MZ".to_vec();
    out.resize(4096, 0x90);
    let local_at = out.len() as u32;
    let le16 = |out: &mut Vec<u8>, value: u16| out.extend_from_slice(&value.to_le_bytes());
    let le32 = |out: &mut Vec<u8>, value: u32| out.extend_from_slice(&value.to_le_bytes());
    le32(&mut out, 0x0403_4b50);
    for field in [20u16, 0, 0, 0, 0] {
        le16(&mut out, field);
    }
    le32(&mut out, 0); // CRC: nothing here checks a member's
    le32(&mut out, data.len() as u32);
    le32(&mut out, data.len() as u32);
    le16(&mut out, name.len() as u16);
    le16(&mut out, 0);
    out.extend_from_slice(name.as_bytes());
    out.extend_from_slice(data);
    let directory_at = out.len() as u32;
    le32(&mut out, 0x0201_4b50);
    for field in [20u16, 20, 0, 0, 0, 0] {
        le16(&mut out, field);
    }
    le32(&mut out, 0);
    le32(&mut out, data.len() as u32);
    le32(&mut out, data.len() as u32);
    le16(&mut out, name.len() as u16);
    for field in [0u16, 0, 0, 0] {
        le16(&mut out, field);
    }
    le32(&mut out, 0);
    le32(&mut out, local_at);
    out.extend_from_slice(name.as_bytes());
    let directory_len = out.len() as u32 - directory_at;
    le32(&mut out, 0x0605_4b50);
    for field in [0u16, 0, 1, 1] {
        le16(&mut out, field);
    }
    le32(&mut out, directory_len);
    le32(&mut out, directory_at);
    le16(&mut out, 0);
    out
}

// --- A server with a torrent ------------------------------------------------

/// Seed the pieces of `torrent` whose index `keep` accepts, from the files
/// under `content` in the order the metainfo lists them. After the server
/// has started: the launch sweep deletes a piece directory seeded before.
fn seed(cache_root: &Path, torrent: &[u8], content: &Path, keep: impl Fn(usize) -> bool) {
    let meta = librqbit::torrent_from_bytes(torrent).expect("parse the torrent back");
    let info_hash = meta.info_hash.as_string();
    let info = meta.info.data.validate().expect("validated metainfo");
    let piece_length = info.lengths().default_piece_length() as u64;
    let mut blob = Vec::new();
    for file in info.iter_file_details() {
        let path = content.join(file.filename.to_pathbuf());
        blob.extend(std::fs::read(&path).expect("a fixture file the torrent names"));
    }
    let layout = enginefs::piece_store::PieceLayout::new(
        piece_length,
        blob.len() as u64,
        [enginefs::piece_store::FileSpec {
            len: blob.len() as u64,
            padding: false,
        }],
    )
    .expect("a layout for the fixture");
    let pieces = enginefs::piece_store::PieceStore::new(
        piece_store(cache_root).torrent_dir(&info_hash),
        Arc::new(layout),
    );
    for (index, piece) in blob.chunks(piece_length as usize).enumerate() {
        if !keep(index) {
            continue;
        }
        let path = pieces.piece_path(index as u32);
        std::fs::create_dir_all(path.parent().expect("a bucket")).expect("piece bucket");
        std::fs::write(&path, piece).expect("write a piece");
    }
}

/// A server with one torrent of `files` added and checked, the pieces
/// `keep` accepts seeded -- and kept (`fixture_pins`): the claims here are
/// about bytes, not about retention.
struct TorrentFixture {
    handle: stream_server::ServerHandle,
    base: String,
    info_hash: String,
    files: Vec<String>,
    _dirs: [tempfile::TempDir; 3],
}

impl TorrentFixture {
    fn start(files: &[(&str, Vec<u8>)], keep: impl Fn(usize) -> bool) -> anyhow::Result<Self> {
        Self::start_in(
            fixture_pins::keep_what_the_fixture_seeded(offline_config()),
            files,
            keep,
        )
    }

    /// [`Self::start`] under `config`'s pin set: a pin test's, which must
    /// keep the empty record (`fixture_pins`).
    fn start_in(
        config: ServerConfig,
        files: &[(&str, Vec<u8>)],
        keep: impl Fn(usize) -> bool,
    ) -> anyhow::Result<Self> {
        let config_dir = tempfile::tempdir()?;
        let cache_dir = tempfile::tempdir()?;
        let src = tempfile::tempdir()?;
        let content = src.path().join("Films");
        std::fs::create_dir_all(&content)?;
        for (name, bytes) in files {
            std::fs::write(content.join(name), bytes)?;
        }
        let (torrent, info_hash) = real_torrent(&content);
        let cache_root = stream_server::resolved_path(&cache_dir.path().join("cache"));
        stream_server::pretend_volume_space(&cache_root, u64::MAX);
        let handle = stream_server::start(ServerConfig {
            http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            config_dir: Some(config_dir.path().join("config")),
            cache_dir: Some(cache_root.clone()),
            ..config
        })?;
        seed(&cache_root, &torrent, &content, keep);
        let base = format!("http://{}", handle.http_addr());
        bearer_client(&handle)?
            .post(format!("{base}/create"))
            .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
            .send()?
            .error_for_status()?;
        let deadline = Instant::now() + BOUND;
        let stats = loop {
            let stats = serde_json::to_value(handle.engine_stats(&info_hash, &[])?)?;
            match stats["phase"].as_str() {
                Some("checking") | Some("resolvingMetadata") => {
                    anyhow::ensure!(Instant::now() < deadline, "never checked: {stats}");
                    std::thread::sleep(Duration::from_millis(50));
                }
                _ => break stats,
            }
        };
        let files = stats["files"]
            .as_array()
            .expect("files")
            .iter()
            .map(|file| file["name"].as_str().expect("a name").to_string())
            .collect();
        Ok(Self {
            handle,
            base,
            info_hash,
            files,
            _dirs: [config_dir, cache_dir, src],
        })
    }

    /// The file's index, looked up by name (never assumed: AGENTS.md).
    fn index(&self, name: &str) -> usize {
        self.files
            .iter()
            .position(|file| file == name)
            .unwrap_or_else(|| panic!("no file {name} in {:?}", self.files))
    }

    /// The file's plain streaming URL, as stremio-core builds it: no
    /// archive prefix, no `torrent:` form.
    fn register(&self, name: &str) -> anyhow::Result<MediaId> {
        self.handle
            .register(MediaSpec::StreamingUrl(url::Url::parse(&format!(
                "{}/{}/{}",
                self.base,
                self.info_hash,
                self.index(name)
            ))?))
    }

    /// `member` of the torrent's file `archive`, read whole through the
    /// archive route's `torrent:` form.
    fn by_route(&self, prefix: &str, archive: &str, member: &str) -> anyhow::Result<Vec<u8>> {
        let response = reqwest::blocking::get(format!(
            "{}/{prefix}/stream/torrent:{}%2F{}/{}",
            self.base,
            self.info_hash,
            urlencoding::encode(archive),
            member
                .split('/')
                .map(|segment| urlencoding::encode(segment).into_owned())
                .collect::<Vec<_>>()
                .join("/")
        ))?
        .error_for_status()?;
        Ok(response.bytes()?.to_vec())
    }

    fn stop(self) -> anyhow::Result<()> {
        self.handle.shutdown()?;
        self.handle.join()?;
        Ok(())
    }
}

/// `id` read whole by a reader, on a thread with no runtime.
fn read_by_id(handle: &stream_server::ServerHandle, id: &MediaId) -> anyhow::Result<Vec<u8>> {
    let reader = handle.open_reader(id, None)?;
    within("the reader", move || {
        let mut reader = reader;
        read_to_end(&mut reader)
    })?
    .map_err(Into::into)
}

fn read_to_end(reader: &mut MediaReader) -> std::io::Result<Vec<u8>> {
    let mut read = Vec::new();
    let mut buf = vec![0u8; 7 * 1024];
    loop {
        match reader.read(&mut buf)? {
            0 => return Ok(read),
            n => read.extend_from_slice(&buf[..n]),
        }
    }
}

/// `f` on a thread of its own, answered within [`BOUND`].
fn within<T: Send + 'static>(
    what: &str,
    f: impl FnOnce() -> T + Send + 'static,
) -> anyhow::Result<T> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(BOUND)
        .map_err(|_| anyhow::anyhow!("{what} did not answer within {BOUND:?}"))
}

/// What a member resolves to: its name and length, as an archive URL's.
fn assert_member(resolved: &Resolved, name: &str, len: usize) {
    assert_eq!(resolved.name, name, "{resolved:?}");
    assert_eq!(resolved.len, len as u64, "{resolved:?}");
    assert_eq!(
        resolved.member,
        Some(MemberInfo {
            name: name.to_string(),
            len: len as u64,
        }),
        "not resolved to the member: {resolved:?}"
    );
    assert!(resolved.sniffed, "{resolved:?}");
}

// --- The loopback origin ----------------------------------------------------

/// The refresh token the fake Drive account is reachable by.
const REFRESH_TOKEN: &str = "refresh-tok-sniff-41d2";
/// Drive's id for the fake file.
const FILE_ID: &str = "1SniffFixtureFile";

/// One loopback listener standing in for a CDN, Google Drive and the
/// pairing service: fixed bodies by path, by range, with a validator;
/// `/refresh` mints a Drive token, and `/drive/v3/files/{FILE_ID}` is the
/// body filed under `"drive"`.
struct Origin {
    addr: SocketAddr,
    /// Set by [`Origin::kill`]: every connection after it is dropped.
    dead: Arc<std::sync::atomic::AtomicBool>,
    /// Set by [`Origin::revoke`]: `/refresh` answers `pairAgain` from then
    /// on.
    revoked: Arc<std::sync::atomic::AtomicBool>,
}

impl Origin {
    fn start(bodies: HashMap<String, Vec<u8>>) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let addr = listener.local_addr()?;
        let bodies = Arc::new(bodies);
        let dead = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let revoked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (killed, gone) = (dead.clone(), revoked.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                if killed.load(std::sync::atomic::Ordering::SeqCst) {
                    continue;
                }
                let (bodies, gone) = (bodies.clone(), gone.clone());
                std::thread::spawn(move || serve(stream, &bodies, &gone));
            }
        });
        Ok(Self {
            addr,
            dead,
            revoked,
        })
    }

    /// The Drive grant is gone: every refresh answers `pairAgain`.
    fn revoke(&self) {
        self.revoked
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    /// From now on nothing is served.
    fn kill(&self) {
        self.dead.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

fn serve(
    mut stream: TcpStream,
    bodies: &HashMap<String, Vec<u8>>,
    revoked: &std::sync::atomic::AtomicBool,
) {
    let Ok(second) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(second);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
        return;
    }
    let mut range = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("range: ") {
            range = Some(value.trim().to_string());
        }
    }
    let path = request_line.split_whitespace().nth(1).unwrap_or("");
    if path.starts_with("/refresh") {
        // Sixty seconds is the credential's renew margin, so this token is
        // renewed at every request: a revocation is met at the next read.
        let (status, body): (&str, &[u8]) = match revoked.load(std::sync::atomic::Ordering::SeqCst)
        {
            true => ("401 Unauthorized", b"{\"pairAgain\":true}"),
            false => (
                "200 OK",
                b"{\"accessToken\":\"access-tok-sniff\",\"expiresIn\":60}",
            ),
        };
        let head = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(head.as_bytes());
        let _ = stream.write_all(body);
        return;
    }
    let path = match path.starts_with(&format!("/drive/v3/files/{FILE_ID}")) {
        true => "drive",
        false => path,
    };
    let Some(body) = bodies.get(path) else {
        let _ = stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        return;
    };
    let total = body.len();
    let (first, last) = range
        .as_deref()
        .and_then(|header| header.trim_start_matches("bytes=").split_once('-'))
        .map(|(first, last)| {
            (
                first.parse().unwrap_or(0),
                last.parse::<usize>().unwrap_or(total - 1).min(total - 1),
            )
        })
        .unwrap_or((0, total - 1));
    let head = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\n\
         ETag: \"sniff-fixture\"\r\nAccept-Ranges: bytes\r\n\
         Content-Range: bytes {first}-{last}/{total}\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n",
        last + 1 - first
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(&body[first..=last]);
    let _ = stream.flush();
}

/// A server with nothing in it, its Drive at `origin`.
fn bare_server(
    origin: Option<&Origin>,
) -> anyhow::Result<(stream_server::ServerHandle, [tempfile::TempDir; 2])> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let cache_root = stream_server::resolved_path(&cache_dir.path().join("cache"));
    stream_server::pretend_volume_space(&cache_root, u64::MAX);
    let handle = stream_server::start(ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root),
        drive_refresh_endpoint: origin
            .map(|origin| url::Url::parse(&origin.url("/refresh")))
            .transpose()?,
        drive_api_base: origin
            .map(|origin| url::Url::parse(&origin.url("/")))
            .transpose()?,
        ..offline_config()
    })?;
    Ok((handle, [config_dir, cache_dir]))
}

/// The `/proxy` URL stremio-core builds for `target`.
fn proxy_url(handle: &stream_server::ServerHandle, target: &str) -> url::Url {
    url::Url::parse(&format!(
        "http://{}/proxy/?d={}",
        handle.http_addr(),
        urlencoding::encode(target)
    ))
    .expect("a proxy URL")
}

/// A file of `bytes` on this device, registered by its path.
fn local_id(
    handle: &stream_server::ServerHandle,
    name: &str,
    bytes: &[u8],
) -> anyhow::Result<(tempfile::TempDir, MediaId)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join(name);
    std::fs::write(&path, bytes)?;
    let id = handle.register(MediaSpec::Local {
        file: LocalFile::Path(path),
        name: None,
    })?;
    Ok((dir, id))
}

// --- The tests --------------------------------------------------------------

/// **A torrent's ZIP, 7z, TAR and disc image each resolve to their member
/// by their first bytes**, registered by the plain streaming URL: the
/// member's name and length, read by id, and the same bytes the archive
/// route's `torrent:` form serves. Each is named `.bin`, so nothing but
/// the content can have said what it is.
#[test]
fn a_torrent_file_resolves_to_its_member_by_its_head() -> anyhow::Result<()> {
    let member = member_bytes();
    let notes = b"a few words".to_vec();
    let entries: [(&str, &[u8]); 2] = [("notes.txt", &notes), (MEMBER, &member)];
    let image = stream_server::images::fixtures::iso::minimal_iso();
    let (data_at, data_len) = stream_server::images::fixtures::iso::data_range(&image);
    let disc_member = image[data_at as usize..(data_at + data_len) as usize].to_vec();
    let fixture = TorrentFixture::start(
        &[
            ("zipped.bin", stored_zip(&entries)),
            ("sevenz.bin", stored_7z(&entries)),
            ("tarred.bin", plain_tar(&entries)),
            ("disc.bin", image.clone()),
        ],
        |_| true,
    )?;
    for (file, prefix) in [
        ("zipped.bin", "zip"),
        ("sevenz.bin", "7zip"),
        ("tarred.bin", "tar"),
    ] {
        let id = fixture.register(file)?;
        let resolved = fixture.handle.resolve(&id)?;
        assert_member(&resolved, MEMBER, member.len());
        assert_eq!(resolved.content_type, "video/x-matroska", "{file}");
        assert_eq!(read_by_id(&fixture.handle, &id)?, member, "{file} by id");
        assert_eq!(
            fixture.by_route(prefix, file, MEMBER)?,
            member,
            "{file} by the route"
        );
    }
    let id = fixture.register("disc.bin")?;
    let resolved = fixture.handle.resolve(&id)?;
    assert_member(&resolved, &resolved.name.clone(), disc_member.len());
    assert_eq!(read_by_id(&fixture.handle, &id)?, disc_member);
    assert_eq!(
        fixture.by_route("iso", "disc.bin", &resolved.name)?,
        disc_member
    );
    fixture.stop()
}

/// **A stored single-file RAR in a torrent** resolves to its member by
/// its head, as the `torrent:` form would have named it.
#[cfg(feature = "rar")]
#[test]
fn a_torrent_rar_resolves_to_its_member_by_its_head() -> anyhow::Result<()> {
    let member = member_bytes();
    let rar = rar_fixtures::rar5_stored(&[(MEMBER, &member), ("extras.nfo", b"words")]);
    let fixture = TorrentFixture::start(&[("film.rar", rar)], |_| true)?;
    let id = fixture.register("film.rar")?;
    let resolved = fixture.handle.resolve(&id)?;
    assert_member(&resolved, MEMBER, member.len());
    assert_eq!(read_by_id(&fixture.handle, &id)?, member);
    assert_eq!(fixture.by_route("rar", "film.rar", MEMBER)?, member);
    fixture.stop()
}

/// **A multi-volume RAR in a torrent resolves by the head of its first
/// volume, with every volume**: the siblings found by the naming rules, as
/// the `torrent:` form finds them, and a read by id crossing every
/// boundary.
#[cfg(feature = "rar")]
#[test]
fn a_rar_set_in_a_torrent_resolves_by_its_first_volume_with_all_of_them() -> anyhow::Result<()> {
    let member = rar_fixtures::signposted(9 * PIECE);
    let volumes = rar_fixtures::rar5_volumes(&[(MEMBER, &member)], 4 * PIECE);
    assert!(volumes.len() >= 3, "a set of {} volumes", volumes.len());
    let names = rar_fixtures::part_names("film", volumes.len());
    let files = names
        .iter()
        .map(String::as_str)
        .zip(volumes)
        .collect::<Vec<_>>();
    let fixture = TorrentFixture::start(&files, |_| true)?;
    let id = fixture.register(&names[0])?;
    let resolved = fixture.handle.resolve(&id)?;
    assert_member(&resolved, MEMBER, member.len());
    assert_eq!(read_by_id(&fixture.handle, &id)?, member);
    fixture.stop()
}

/// **A film resolves as itself, and says its head was looked at.**
#[test]
fn a_film_resolves_as_itself_sniffed() -> anyhow::Result<()> {
    let bytes = film(3 * PIECE);
    let fixture = TorrentFixture::start(&[("film.mkv", bytes.clone())], |_| true)?;
    let id = fixture.register("film.mkv")?;
    let resolved = fixture.handle.resolve(&id)?;
    assert_eq!(resolved.name, "film.mkv");
    assert_eq!(resolved.len, bytes.len() as u64);
    assert_eq!(resolved.member, None);
    assert!(resolved.sniffed, "{resolved:?}");
    assert_eq!(read_by_id(&fixture.handle, &id)?, bytes);
    fixture.stop()
}

/// **A torrent whose first piece is not here resolves as the plain file,
/// unsniffed, within the bound** -- offline, nothing will ever bring it,
/// and the sniff must not turn a playable file into a wait. A ZIP's bytes
/// in fact, so a sniff that did read it would answer the member instead.
#[test]
fn a_torrent_whose_head_is_missing_resolves_unsniffed_within_the_bound() -> anyhow::Result<()> {
    const SNIFF_BOUND: Duration = Duration::from_millis(300);
    let member = member_bytes();
    let zip = stored_zip(&[(MEMBER, &member)]);
    let len = zip.len();
    let fixture = TorrentFixture::start(&[("film.zip", zip)], |piece| piece != 0)?;
    fixture.handle.set_media_sniff_bound(SNIFF_BOUND);
    let id = fixture.register("film.zip")?;
    let fixture = Arc::new(fixture);
    let started = Instant::now();
    let resolved = within("the resolve", {
        let fixture = fixture.clone();
        move || fixture.handle.resolve(&id)
    })??;
    let took = started.elapsed();
    assert_eq!(resolved.name, "film.zip");
    assert_eq!(resolved.len, len as u64);
    assert_eq!(resolved.member, None);
    assert!(!resolved.sniffed, "{resolved:?}");
    assert!(
        took < SNIFF_BOUND * 20,
        "the resolve took {took:?} for a bound of {SNIFF_BOUND:?}"
    );
    Arc::into_inner(fixture)
        .expect("the resolve's thread has let the fixture go")
        .stop()
}

/// **A `/proxy` link to a RAR resolves to its member by its head**, read
/// through the proxy cache, and reads by id.
#[cfg(feature = "rar")]
#[test]
fn a_proxy_link_to_a_rar_resolves_to_its_member() -> anyhow::Result<()> {
    let member = member_bytes();
    let rar = rar_fixtures::rar5_stored(&[(MEMBER, &member), ("extras.nfo", b"words")]);
    let origin = Origin::start(HashMap::from([("/download/archive".to_string(), rar)]))?;
    let (handle, _dirs) = bare_server(None)?;
    let id = handle.register(MediaSpec::StreamingUrl(proxy_url(
        &handle,
        &origin.url("/download/archive"),
    )))?;
    let resolved = handle.resolve(&id)?;
    assert_member(&resolved, MEMBER, member.len());
    assert_eq!(read_by_id(&handle, &id)?, member);
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **A file on this device that is a ZIP resolves to its member**, and so
/// does a self-extracting one, whose head is an executable's: the ZIP
/// reader finds its end record at the tail.
#[test]
fn a_local_zip_resolves_to_its_member() -> anyhow::Result<()> {
    let member = member_bytes();
    let (handle, _dirs) = bare_server(None)?;
    for (name, bytes) in [
        ("archive.dat", stored_zip(&[(MEMBER, &member)])),
        ("setup.exe", self_extracting_zip(MEMBER, &member)),
    ] {
        let (_dir, id) = local_id(&handle, name, &bytes)?;
        let resolved = handle.resolve(&id)?;
        assert_member(&resolved, MEMBER, member.len());
        assert_eq!(read_by_id(&handle, &id)?, member, "{name}");
        // Played, the viewer's session is off every torrent file, as for
        // the file played as itself.
        let token = format!("tv.{name}");
        let played = handle.open_reader(
            &id,
            Some(stream_server::PlayToken {
                token: token.clone(),
                buffer: Default::default(),
            }),
        )?;
        assert_eq!(
            handle.play_session_of(&token),
            Some(enginefs::retention::sessions::Played::Elsewhere)
        );
        drop(played);
    }
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **A container's signature that no translator indexes is the
/// translator's refusal**, never a film: a ZIP's local header with no end
/// record behind it is `malformed`, with the ZIP reader's sentence.
#[test]
fn a_signature_nothing_indexes_is_refused_as_malformed() -> anyhow::Result<()> {
    let (handle, _dirs) = bare_server(None)?;
    let mut bytes = b"PK\x03\x04".to_vec();
    bytes.extend(film(4 * PIECE));
    let (_dir, id) = local_id(&handle, "film.mkv", &bytes)?;
    let refusal = handle.resolve(&id).expect_err("a broken zip resolved");
    assert_eq!(refusal.kind(), "malformed", "{refusal}");
    assert!(
        refusal.to_string().contains("zip"),
        "not the ZIP reader's sentence: {refusal}"
    );
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **A Google Drive file that is a ZIP resolves to its member**, read
/// under the grant through the proxy cache, as a link's does; and once
/// the grant is found dead, the next resolve asks for it again, as the
/// plain file's would, rather than answering the member it can no longer
/// read.
#[test]
fn a_drive_zip_resolves_to_its_member() -> anyhow::Result<()> {
    let member = member_bytes();
    let origin = Origin::start(HashMap::from([(
        "drive".to_string(),
        stored_zip(&[(MEMBER, &member)]),
    )]))?;
    let (handle, _dirs) = bare_server(Some(&origin))?;
    let id = handle.register(MediaSpec::Drive {
        file_id: FILE_ID.to_string(),
        name: Some("Season.zip".to_string()),
        grant: Arc::new(|| Some(REFRESH_TOKEN.to_string())),
    })?;
    let resolved = handle.resolve(&id)?;
    assert_member(&resolved, MEMBER, member.len());
    assert_eq!(read_by_id(&handle, &id)?, member);

    origin.revoke();
    assert!(read_by_id(&handle, &id).is_err(), "read under a dead grant");
    assert_eq!(
        handle.resolve(&id).err(),
        Some(stream_server::Refusal::PairAgain),
        "the member was answered again under a dead grant"
    );
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **A finished download of a link to a ZIP resolves to its member with
/// the origin gone**: the sniff reads the download off the disk, as the
/// file itself is read.
#[test]
fn a_downloaded_zip_resolves_to_its_member_with_the_origin_gone() -> anyhow::Result<()> {
    let member = member_bytes();
    let origin = Origin::start(HashMap::from([(
        "/season.zip".to_string(),
        stored_zip(&[(MEMBER, &member)]),
    )]))?;
    let (handle, _dirs) = bare_server(None)?;
    let target = origin.url("/season.zip");
    let row = handle.pin_proxy_download(stream_server::ProxyDownloadRequest {
        url: Some(target.clone()),
        headers: Default::default(),
        drive_file_id: None,
        refresh_token: None,
        name: None,
    })?;
    let deadline = Instant::now() + BOUND;
    while !handle
        .downloads()?
        .iter()
        .any(|download| download.info_hash == row.info_hash && download.complete)
    {
        anyhow::ensure!(Instant::now() < deadline, "the download never completed");
        std::thread::sleep(Duration::from_millis(20));
    }
    origin.kill();
    let id = handle.register(MediaSpec::StreamingUrl(proxy_url(&handle, &target)))?;
    let resolved = handle.resolve(&id)?;
    assert_member(&resolved, MEMBER, member.len());
    assert_eq!(read_by_id(&handle, &id)?, member);

    // Unpinned, its bytes are no longer kept: the next resolve goes to
    // the origin, which is gone, rather than answering the member.
    assert!(handle.unpin(&id, false)?.unpinned);
    assert!(
        handle.resolve(&id).is_err(),
        "the member was answered off a download no longer pinned"
    );
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **A torrent file found to be a set's first volume pins every volume,
/// and unpins every volume** -- by the plain URL with an index, which an
/// unpin otherwise answers without a resolve: once the id has resolved to
/// the member, its pin was the set's.
#[cfg(feature = "rar")]
#[test]
fn a_sniffed_rar_set_pins_and_unpins_every_volume() -> anyhow::Result<()> {
    let member = rar_fixtures::signposted(9 * PIECE);
    let volumes = rar_fixtures::rar5_volumes(&[(MEMBER, &member)], 4 * PIECE);
    let names = rar_fixtures::part_names("film", volumes.len());
    let files = names
        .iter()
        .map(String::as_str)
        .zip(volumes)
        .collect::<Vec<_>>();
    let fixture = TorrentFixture::start_in(offline_config(), &files, |_| true)?;
    let id = fixture.register(&names[0])?;
    assert!(fixture.handle.resolve(&id)?.member.is_some());
    let mut pinned = fixture
        .handle
        .pin(&id)?
        .into_iter()
        .map(|row| row.file_idx)
        .collect::<Vec<_>>();
    pinned.sort_unstable();
    let mut expected = names
        .iter()
        .map(|name| fixture.index(name))
        .collect::<Vec<_>>();
    expected.sort_unstable();
    assert!(expected.len() > 1, "a set of one volume proves nothing");
    assert_eq!(pinned, expected);
    let listed = |file_idx: usize| -> anyhow::Result<bool> {
        Ok(fixture.handle.downloads()?.iter().any(|row| {
            row.source.is_none() && row.info_hash == fixture.info_hash && row.file_idx == file_idx
        }))
    };
    for file_idx in &expected {
        assert!(listed(*file_idx)?, "volume {file_idx} not pinned");
    }
    assert!(fixture.handle.unpin(&id, false)?.unpinned);
    for file_idx in &expected {
        assert!(!listed(*file_idx)?, "volume {file_idx} still pinned");
    }
    fixture.stop()
}

/// **A sniffed container's session let go is made again** from the file
/// it was found in: there is no create to run and no `torrent:` key, so
/// the id keeps the file. Here the archive map's cap lets it go -- as many
/// newer containers resolved, none of them read.
#[test]
fn a_sniffed_session_let_go_is_made_again() -> anyhow::Result<()> {
    let member = member_bytes();
    let zip = stored_zip(&[(MEMBER, &member)]);
    let (handle, _dirs) = bare_server(None)?;
    let (_first_dir, first) = local_id(&handle, "first.zip", &zip)?;
    assert_member(&handle.resolve(&first)?, MEMBER, member.len());
    let mut kept = Vec::new();
    for at in 0..stream_server::translators::session::SESSION_CAP + 1 {
        let (dir, id) = local_id(&handle, &format!("later-{at}.zip"), &zip)?;
        handle.resolve(&id)?;
        kept.push(dir);
    }
    assert_eq!(read_by_id(&handle, &first)?, member);
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **The same for a finished download of a Drive file**, with Drive gone:
/// the sniff reads the download off the disk, and asks for no grant.
#[test]
fn a_downloaded_drive_zip_resolves_to_its_member_with_drive_gone() -> anyhow::Result<()> {
    let member = member_bytes();
    let origin = Origin::start(HashMap::from([(
        "drive".to_string(),
        stored_zip(&[(MEMBER, &member)]),
    )]))?;
    let (handle, _dirs) = bare_server(Some(&origin))?;
    let row = handle.pin_proxy_download(stream_server::ProxyDownloadRequest {
        url: None,
        headers: Default::default(),
        drive_file_id: Some(FILE_ID.to_string()),
        refresh_token: Some(REFRESH_TOKEN.to_string()),
        name: Some("Season.zip".to_string()),
    })?;
    let deadline = Instant::now() + BOUND;
    while !handle
        .downloads()?
        .iter()
        .any(|download| download.info_hash == row.info_hash && download.complete)
    {
        anyhow::ensure!(Instant::now() < deadline, "the download never completed");
        std::thread::sleep(Duration::from_millis(20));
    }
    origin.kill();
    let id = handle.register(MediaSpec::Drive {
        file_id: FILE_ID.to_string(),
        name: Some("Season.zip".to_string()),
        grant: Arc::new(|| None),
    })?;
    let resolved = handle.resolve(&id)?;
    assert_member(&resolved, MEMBER, member.len());
    assert_eq!(read_by_id(&handle, &id)?, member);
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}
