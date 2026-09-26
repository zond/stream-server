// The Google Drive layer end to end: `ServerHandle::open_drive_file`,
// which opens a file in somebody's Drive under a refresh token, the
// `GET /drive/stream/{key}` that serves its bytes by range, and what each
// of them says when the pairing is dead.
//
// Both Google and the pairing service are one loopback fake here (see
// `Fake`), which is why the server is configured with a `drive_api_base`
// at all -- a shipped build has Google's own, fixed, because an origin a
// caller could name is a credentialed relay with a cache behind it.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// The grant this test's fake account is reachable by. A string that
/// exists nowhere else, so "it did not leak" is a substring search.
const REFRESH_TOKEN: &str = "refresh-tok-8b21-never-log-me";

/// Drive's id for the film. Not a secret -- it is the cache key.
const FILE_ID: &str = "1AbCdEfGhIjKlMnOpQrStUvWxYz";

const FILE_LENGTH: usize = 2 * 1024 * 1024;

/// The film's bytes: a pattern, so a range can be checked to have come
/// from the offset it claims rather than merely to be the right length.
fn byte_at(offset: usize) -> u8 {
    (offset.wrapping_mul(11) % 251) as u8
}

/// How the fake pairing service answers a refresh.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Refreshes {
    /// A fresh access token every time.
    Yes,
    /// `401` with `pairAgain` -- and, so that the wording cannot have come
    /// from here, the refresh token in the body, which is what a naive
    /// service would do.
    PairAgain,
    /// One token with barely more life than the source's renew margin,
    /// and then the grant is gone: a pairing revoked from the phone while
    /// the film was playing, which is what the stream route's own check
    /// exists for. The first token outlives the create by about a second
    /// and nothing more, so the next read renews and finds nothing there.
    ShortThenGone,
}

/// One loopback listener standing in for both Google and the pairing
/// service, the same way `sources::drive`'s own tests do it: `/refresh`
/// mints tokens, `/drive/v3/files/...` serves ranges to whoever holds one.
struct Fake {
    addr: SocketAddr,
    refreshes: Arc<AtomicUsize>,
    /// Every `Range` the film was asked for, in order.
    ranges: Arc<Mutex<Vec<String>>>,
}

impl Fake {
    fn start(mode: Refreshes) -> anyhow::Result<Fake> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let addr = listener.local_addr()?;
        let refreshes = Arc::new(AtomicUsize::new(0));
        let issued = Arc::new(Mutex::new(Vec::<String>::new()));
        let ranges = Arc::new(Mutex::new(Vec::<String>::new()));
        let fake = Fake {
            addr,
            refreshes: refreshes.clone(),
            ranges: ranges.clone(),
        };
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let (refreshes, issued, ranges) =
                    (refreshes.clone(), issued.clone(), ranges.clone());
                // A thread per connection: a player's opening is several
                // reads at once and a serial fake would turn the test's
                // question into a queue.
                std::thread::spawn(move || serve(stream, mode, &refreshes, &issued, &ranges));
            }
        });
        Ok(fake)
    }

    fn refresh_endpoint(&self) -> url::Url {
        url::Url::parse(&format!("http://{}/refresh", self.addr)).expect("a literal URL")
    }

    fn api_base(&self) -> url::Url {
        url::Url::parse(&format!("http://{}/", self.addr)).expect("a literal URL")
    }

    fn refreshes(&self) -> usize {
        self.refreshes.load(Ordering::SeqCst)
    }

    /// The `Range` headers the film has been asked with so far.
    fn ranges(&self) -> Vec<String> {
        self.ranges.lock().expect("the ranges").clone()
    }
}

fn serve(
    mut stream: TcpStream,
    mode: Refreshes,
    refreshes: &AtomicUsize,
    issued: &Mutex<Vec<String>>,
    ranges: &Mutex<Vec<String>>,
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
    let mut authorization = None;
    let mut length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if let Some(value) = lower.strip_prefix("range: ") {
            range = Some(value.trim().to_string());
        } else if let Some(value) = lower.strip_prefix("content-length: ") {
            length = value.trim().parse().unwrap_or(0);
        }
        if lower.starts_with("authorization: ") {
            authorization = Some(line["authorization: ".len()..].trim().to_string());
        }
    }
    if request_line.starts_with("POST /refresh") {
        let mut body = vec![0u8; length];
        let _ = std::io::Read::read_exact(&mut reader, &mut body);
        let count = refreshes.fetch_add(1, Ordering::SeqCst) + 1;
        match mode {
            Refreshes::Yes => {
                let minted = format!("access-tok-{count:04}");
                issued.lock().expect("the tokens").push(minted.clone());
                respond(
                    &mut stream,
                    "200 OK",
                    format!("{{\"accessToken\":\"{minted}\",\"expiresIn\":3599}}").as_bytes(),
                );
            }
            Refreshes::PairAgain => respond(
                &mut stream,
                "401 Unauthorized",
                format!("{{\"error\":\"invalid_grant for {REFRESH_TOKEN}\",\"pairAgain\":true}}")
                    .as_bytes(),
            ),
            // 61 seconds is one second more than the source's renew
            // margin, so the create's own reads go out under it and the
            // read after that renews -- into a grant that is gone.
            Refreshes::ShortThenGone if count == 1 => {
                let minted = format!("access-tok-{count:04}");
                issued.lock().expect("the tokens").push(minted.clone());
                respond(
                    &mut stream,
                    "200 OK",
                    format!("{{\"accessToken\":\"{minted}\",\"expiresIn\":61}}").as_bytes(),
                );
            }
            Refreshes::ShortThenGone => respond(
                &mut stream,
                "401 Unauthorized",
                b"{\"error\":\"invalid_grant\",\"pairAgain\":true}",
            ),
        }
        return;
    }
    let known = authorization
        .as_deref()
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|bearer| {
            issued
                .lock()
                .expect("the tokens")
                .iter()
                .any(|t| t == bearer)
        });
    if !known {
        respond(
            &mut stream,
            "401 Unauthorized",
            b"{\"error\":{\"code\":401}}",
        );
        return;
    }
    if let Some(header) = range.as_deref() {
        ranges.lock().expect("the ranges").push(header.to_string());
    }
    let (first, last) = match range.as_deref() {
        Some(header) => {
            let Some((first, last)) = header.trim_start_matches("bytes=").split_once('-') else {
                respond(&mut stream, "400 Bad Request", b"");
                return;
            };
            let first: usize = first.parse().unwrap_or(0);
            let last: usize = last
                .parse::<usize>()
                .unwrap_or(FILE_LENGTH - 1)
                .min(FILE_LENGTH - 1);
            (first, last)
        }
        None => (0, FILE_LENGTH - 1),
    };
    let body: Vec<u8> = (first..=last).map(byte_at).collect();
    let head = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Type: video/x-matroska\r\nETag: \"the-film\"\r\n\
         Accept-Ranges: bytes\r\nContent-Range: bytes {first}-{last}/{FILE_LENGTH}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(&body);
    let _ = stream.flush();
}

fn respond(stream: &mut TcpStream, status: &str, body: &[u8]) {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
}

/// A server with nothing of its own on the network: no DHT names resolved,
/// no public trackers, no multicast (see `archive.rs`, where two test runs
/// found each other).
struct Fixture {
    handle: stream_server::ServerHandle,
    base: String,
    _cache: tempfile::TempDir,
    _config: tempfile::TempDir,
}

fn fixture(fake: &Fake) -> anyhow::Result<Fixture> {
    fixture_on(
        tempfile::tempdir()?,
        tempfile::tempdir()?,
        (fake.refresh_endpoint(), fake.api_base()),
        Some(Vec::new()),
    )
}

/// A server over the given directories, against the given pairing service
/// and Drive origin, told `proxy_pins` -- which is how a second launch
/// over a first one's cache is made, and how one is pointed at a Drive
/// that is not there.
fn fixture_on(
    cache: tempfile::TempDir,
    config: tempfile::TempDir,
    (refresh, api_base): (url::Url, url::Url),
    proxy_pins: Option<Vec<stream_server::ProxyPinKey>>,
) -> anyhow::Result<Fixture> {
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config.path().join("config")),
        cache_dir: Some(cache.path().join("cache")),
        pins: Some(Default::default()),
        proxy_pins,
        resolve_dht_bootstrap_names: false,
        use_public_trackers: false,
        enable_local_service_discovery: false,
        drive_refresh_endpoint: Some(refresh),
        drive_api_base: Some(api_base),
        ..stream_server::ServerConfig::default()
    })?;
    Ok(Fixture {
        base: format!("http://{}", handle.http_addr()),
        handle,
        _cache: cache,
        _config: config,
    })
}

/// A pairing service at an address nothing listens on: a port the OS just
/// handed out and gave back. What a device with no network sees, near
/// enough -- the connection is refused rather than timing out, which keeps
/// the test quick and the claim the same. It is the *refresh* endpoint
/// that is dead and not the Drive origin, because the origin's address is
/// half of the cache key (`ProxyPinKey::Drive` keys the file's media URL,
/// which in a shipped build is Google's fixed one) and a second launch
/// against a different origin would be asking about a different file.
/// Every Drive read renews its token before its first byte, so a dead
/// pairing service is enough to make the origin unreachable.
fn dead_refresh_endpoint() -> anyhow::Result<url::Url> {
    let taken = TcpListener::bind(("127.0.0.1", 0))?;
    let addr = taken.local_addr()?;
    drop(taken);
    Ok(url::Url::parse(&format!("http://{addr}/refresh"))?)
}

impl Fixture {
    /// Stops the server and hands back its directories, for a second
    /// launch over them.
    fn stop(self) -> anyhow::Result<(tempfile::TempDir, tempfile::TempDir)> {
        self.handle.shutdown()?;
        self.handle.join()?;
        Ok((self._cache, self._config))
    }

    /// Pins the fake's film as an offline download, as the app asks for it
    /// (`ServerHandle::pin_proxy_download` with a Drive file and the
    /// grant), and answers the row.
    fn download(&self, refresh_token: &str) -> anyhow::Result<stream_server::DownloadInfo> {
        self.handle
            .pin_proxy_download(stream_server::ProxyDownloadRequest {
                url: None,
                headers: Default::default(),
                drive_file_id: Some(FILE_ID.to_string()),
                refresh_token: Some(refresh_token.to_string()),
                name: Some("A Film.mkv".to_string()),
            })
    }

    /// Polls the listing until the download of `key` is complete.
    fn wait_complete(&self, key: &str) -> anyhow::Result<stream_server::DownloadInfo> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let rows = self.handle.downloads()?;
            if let Some(row) = rows.iter().find(|row| row.info_hash == key && row.complete) {
                return Ok(row.clone());
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "the Drive download did not complete: {rows:?}"
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    /// The open, as the app asks for it: the grant is an argument to a call
    /// in this process, which is the whole of the module's rule.
    fn open(
        &self,
        refresh_token: &str,
    ) -> anyhow::Result<Result<stream_server::DriveFileOpened, stream_server::DriveOpenError>> {
        self.handle
            .open_drive_file(FILE_ID, refresh_token, Some("A Film.mkv".to_string()))
    }

    /// An open that is expected to pass, as the JSON it crosses FFI as.
    fn create(&self, refresh_token: &str) -> anyhow::Result<serde_json::Value> {
        let opened = self
            .open(refresh_token)?
            .map_err(|error| anyhow::anyhow!("the open was refused: {error}"))?;
        Ok(serde_json::to_value(opened)?)
    }

    /// A player's fetch: `url` as the open answered it (absolute, on this
    /// server) or a path of this server's.
    fn get(&self, url: &str, range: Option<&str>) -> anyhow::Result<reqwest::blocking::Response> {
        let url = if url.starts_with("http") {
            url.to_string()
        } else {
            format!("{}{}", self.base, url)
        };
        let mut request = reqwest::blocking::Client::new().get(url);
        if let Some(range) = range {
            request = request.header(reqwest::header::RANGE, range);
        }
        Ok(request.send()?)
    }
}

/// A create opens the file and answers something the stream path can use,
/// and that answer is a key rather than anything about the account.
#[test]
fn a_create_answers_a_stream_path_the_player_can_fetch() -> anyhow::Result<()> {
    let fake = Fake::start(Refreshes::Yes)?;
    let fixture = fixture(&fake)?;

    let body = fixture.create(REFRESH_TOKEN)?;

    let key = body["key"].as_str().expect("a session key").to_string();
    assert!(!key.is_empty());
    assert_eq!(body["url"], format!("{}/drive/stream/{key}", fixture.base));
    assert_eq!(body["length"], FILE_LENGTH as u64);
    assert_eq!(body["contentType"], "video/x-matroska");
    assert_eq!(body["name"], "A Film.mkv");
    // The pairing is built already expired -- the app keeps no access
    // token -- so opening it is what mints the first one.
    assert_eq!(fake.refreshes(), 1, "the open renewed once");

    // And the path it named really serves the file.
    let response = fixture.get(body["url"].as_str().expect("the url"), None)?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(FILE_LENGTH.to_string().as_str())
    );
    Ok(())
}

/// The stream path serves the bytes that were asked for, from the offset
/// that was asked for -- the framing a player seeks with.
#[test]
fn a_stream_path_serves_exact_ranges() -> anyhow::Result<()> {
    let fake = Fake::start(Refreshes::Yes)?;
    let fixture = fixture(&fake)?;
    let body = fixture.create(REFRESH_TOKEN)?;
    let path = body["url"].as_str().expect("the url").to_string();

    // A mid-file span, which is what a seek is.
    let response = fixture.get(&path, Some("bytes=100000-100255"))?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok()),
        Some(format!("bytes 100000-100255/{FILE_LENGTH}").as_str())
    );
    let bytes = response.bytes()?;
    assert_eq!(bytes.len(), 256);
    let expected: Vec<u8> = (100_000..=100_255).map(byte_at).collect();
    assert_eq!(bytes.as_ref(), expected.as_slice(), "the bytes at 100000");

    // The tail, open-ended, which is what a player asks for on an open.
    let response = fixture.get(&path, Some(&format!("bytes={}-", FILE_LENGTH - 16)))?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let tail: Vec<u8> = (FILE_LENGTH - 16..FILE_LENGTH).map(byte_at).collect();
    assert_eq!(response.bytes()?.as_ref(), tail.as_slice(), "the last 16");

    // And a range nothing can satisfy is a `416` naming the length, not a
    // `200` over the whole film.
    let response = fixture.get(&path, Some("bytes=999999999-"))?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::RANGE_NOT_SATISFIABLE
    );
    Ok(())
}

/// A dead pairing arrives as itself: a kind the app switches on without
/// reading English, and **not** the plain failure that "Google is down"
/// would be.
#[test]
fn a_dead_pairing_answers_as_itself_and_not_as_a_gateway_failure() -> anyhow::Result<()> {
    let fake = Fake::start(Refreshes::PairAgain)?;
    let fixture = fixture(&fake)?;

    let error = fixture
        .open(REFRESH_TOKEN)?
        .expect_err("a grant that is gone refuses the open");
    assert!(error.is_pair_again(), "{error}");
    assert_eq!(error.refused(), Some("pairAgain"));
    // The sentence is written in `sources::drive`, so it says what the
    // viewer has to do and carries nothing from the service's own body.
    let message = error.to_string();
    assert!(message.contains("scan the code again"), "{message}");
    assert!(!message.contains(REFRESH_TOKEN));
    Ok(())
}

/// A pairing that dies **while the film is playing** reaches the player's
/// next request as itself.
///
/// The real flow, and the reason the stream route asks at all: a viewer
/// revokes the pairing from their phone, the renewal the next read needs
/// fails, that body stops -- and mpv does what mpv does, which is to open
/// the same URL again with a `Range`. What it opens into has to be the
/// reason, or the app shows a spinner for a film that will never resume.
#[test]
fn a_pairing_that_dies_mid_film_answers_the_next_request_as_itself() -> anyhow::Result<()> {
    let fake = Fake::start(Refreshes::ShortThenGone)?;
    let fixture = fixture(&fake)?;
    let body = fixture.create(REFRESH_TOKEN)?;
    let path = body["url"].as_str().expect("the url").to_string();

    // The one token this pairing ever got outlives the create by about a
    // second. Waiting it out is what a film does.
    std::thread::sleep(std::time::Duration::from_millis(1_500));

    // The read that needs a new token: whatever this answers, it is the
    // renewal behind it that kills the pairing.
    let dying = fixture.get(&path, Some("bytes=300000-"))?;
    let _ = dying.bytes();

    // And the re-open, which is the assertion.
    let response = fixture.get(&path, Some("bytes=300000-"))?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "a re-open after the grant died is not a bad gateway"
    );
    let body: serde_json::Value = response.json()?;
    assert_eq!(body["refused"], "pairAgain");
    assert!(!body.to_string().contains(REFRESH_TOKEN));
    Ok(())
}

/// A server whose embedder named no pairing service says so about the
/// build, rather than blaming the account.
#[test]
fn a_build_with_no_pairing_service_refuses_by_name() -> anyhow::Result<()> {
    let cache = tempfile::tempdir()?;
    let config = tempfile::tempdir()?;
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config.path().join("config")),
        cache_dir: Some(cache.path().join("cache")),
        pins: Some(Default::default()),
        resolve_dht_bootstrap_names: false,
        use_public_trackers: false,
        enable_local_service_discovery: false,
        ..stream_server::ServerConfig::default()
    })?;
    let error = handle
        .open_drive_file(FILE_ID, REFRESH_TOKEN, None)?
        .expect_err("a build with no pairing service opens nothing");
    assert_eq!(error.refused(), Some("noPairingService"));
    assert!(!error.is_pair_again(), "the account is not what is wrong");
    assert!(!error.to_string().contains(REFRESH_TOKEN));
    Ok(())
}

/// The stream is open and the key is the whole of what it takes -- which
/// is the arrangement: the half that carries a credential is a call inside
/// the process, and the half a player fetches is open and carries a key.
#[test]
fn the_stream_is_open_and_a_key_nobody_was_given_is_a_404() -> anyhow::Result<()> {
    let fake = Fake::start(Refreshes::Yes)?;
    let fixture = fixture(&fake)?;

    let body = fixture.create(REFRESH_TOKEN)?;
    // No bearer on this one, because mpv cannot send one.
    let response = fixture.get(body["url"].as_str().expect("the url"), None)?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // A key nobody was given is a `404`, not a hint.
    let response = fixture.get("/drive/stream/not-a-session", None)?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    Ok(())
}

/// **No token in any URL, and none in anything a caller is handed.** The
/// open's answer and the stream URL are searched for the grant; the log
/// files are the other half of this and live in `drive_secrecy.rs`, which
/// needs a logging subscriber of its own.
#[test]
fn no_url_or_answer_carries_the_refresh_token() -> anyhow::Result<()> {
    let fake = Fake::start(Refreshes::Yes)?;
    let fixture = fixture(&fake)?;

    let created = fixture.create(REFRESH_TOKEN)?.to_string();
    assert!(
        !created.contains(REFRESH_TOKEN),
        "the open's answer carried the grant: {created}"
    );
    let body: serde_json::Value = serde_json::from_str(&created)?;
    let url = body["url"].as_str().expect("the url");
    assert!(!url.contains(REFRESH_TOKEN), "the stream URL: {url}");
    assert!(
        !url.contains(FILE_ID),
        "the stream URL names the file: {url}"
    );

    // And the URL as a typed answer, which is the string that reaches mpv.
    let opened = fixture
        .handle
        .open_drive_file(FILE_ID, REFRESH_TOKEN, Some("A Film.mkv".to_string()))?
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    assert!(opened.url.starts_with("http://"));
    assert!(!opened.url.contains(REFRESH_TOKEN), "{}", opened.url);
    assert!(!opened.url.contains(FILE_ID), "{}", opened.url);
    assert!(
        opened
            .url
            .ends_with(&format!("/drive/stream/{}", opened.key))
    );
    Ok(())
}

/// **A Drive file downloads, and once it is whole it opens off the disk.**
///
/// The download is the proxy cache's pin and filler (`proxy_downloads`),
/// fed by the same `DriveSource` a stream is read through. What this pins
/// down is the other half: a finished download is what the app plays when
/// there is no network, so opening the file must then cost **no request
/// at all** -- not the probe `DriveSource::open` makes, and not the token
/// renewal before it. The open answers the download's own media route,
/// and a second launch that is told the pin -- against a pairing service
/// and a Drive that are not there -- lists it and plays it the same way.
#[test]
fn a_drive_download_plays_from_the_disk_with_no_network() -> anyhow::Result<()> {
    let fake = Fake::start(Refreshes::Yes)?;
    let fixture = fixture(&fake)?;

    let row = fixture.download(REFRESH_TOKEN)?;
    let key = row.info_hash.clone();
    assert_eq!(key.len(), 64, "keyed like every proxy download: {row:?}");
    assert_eq!(row.name, "A Film.mkv");
    assert!(
        matches!(&row.source, Some(stream_server::ProxyPinKey::Drive { file_id }) if file_id == FILE_ID),
        "{row:?}"
    );
    let done = fixture.wait_complete(&key)?;
    assert_eq!(done.length, FILE_LENGTH as u64);
    assert_eq!(done.downloaded, FILE_LENGTH as u64);
    let renewed = fake.refreshes();
    assert!(renewed >= 1, "the fill renewed the token at least once");

    // The open, now: the download's route, the disk's facts, and nothing
    // asked of the pairing service.
    let opened = fixture
        .open(REFRESH_TOKEN)?
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    assert_eq!(opened.key, key);
    assert!(
        opened.url.ends_with(&format!("/downloads/{key}/stream")),
        "{}",
        opened.url
    );
    assert_eq!(opened.length, FILE_LENGTH as u64);
    assert_eq!(opened.content_type, "video/x-matroska");
    assert_eq!(opened.name.as_deref(), Some("A Film.mkv"));
    assert_eq!(
        fake.refreshes(),
        renewed,
        "a finished download opens without the grant"
    );

    // And pinning it again -- what the app does at every launch for what
    // it kept -- opens nothing either.
    let again = fixture.download(REFRESH_TOKEN)?;
    assert_eq!(again.info_hash, key);
    assert!(again.complete);
    assert_eq!(
        fake.refreshes(),
        renewed,
        "a re-pin of a whole download probes nothing"
    );

    let response = fixture.get(&opened.url, Some("bytes=100000-100255"))?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let expected: Vec<u8> = (100_000..=100_255).map(byte_at).collect();
    assert_eq!(response.bytes()?.as_ref(), expected.as_slice());

    // A second launch over the same cache, told the pin, with the pairing
    // service unreachable: the device is offline (see
    // [`dead_refresh_endpoint`] for why the origin's address stays).
    let (cache, config) = fixture.stop()?;
    let offline = fixture_on(
        cache,
        config,
        (dead_refresh_endpoint()?, fake.api_base()),
        Some(vec![stream_server::ProxyPinKey::Drive {
            file_id: FILE_ID.to_string(),
        }]),
    )?;
    let before = fake.refreshes();
    let listed = offline.handle.downloads()?;
    let row = listed
        .iter()
        .find(|row| row.info_hash == key)
        .expect("the kept download is listed");
    assert!(row.complete, "{row:?}");
    assert_eq!(row.downloaded, FILE_LENGTH as u64);

    let opened = offline
        .open(REFRESH_TOKEN)?
        .map_err(|error| anyhow::anyhow!("offline, the download did not open: {error}"))?;
    assert!(
        opened.url.ends_with(&format!("/downloads/{key}/stream")),
        "{}",
        opened.url
    );
    assert_eq!(opened.length, FILE_LENGTH as u64);
    let response = offline.get(&opened.url, Some(&format!("bytes={}-", FILE_LENGTH - 16)))?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let tail: Vec<u8> = (FILE_LENGTH - 16..FILE_LENGTH).map(byte_at).collect();
    assert_eq!(response.bytes()?.as_ref(), tail.as_slice());

    // A re-pin offline is the same nothing.
    let again = offline.download(REFRESH_TOKEN)?;
    assert!(again.complete, "{again:?}");
    assert_eq!(
        fake.refreshes(),
        before,
        "nothing offline reached for the grant"
    );

    // And a file that is *not* downloaded is refused as unreachable, not
    // answered from somewhere -- offline means offline for what is not
    // on the disk.
    let other = offline
        .handle
        .open_drive_file("not-downloaded", REFRESH_TOKEN, None)?
        .expect_err("a file the disk does not hold needs Drive");
    assert!(!other.is_pair_again(), "{other}");
    offline.stop()?;
    Ok(())
}

/// **A Drive file is read ahead of while it plays**, through the same
/// source the open made, renewing the same grant: the passes' want-window
/// is fetched by the proxy backing (`proxy_retention::Prefetcher`), so a
/// reader consuming the film finds later chunks already on the disk --
/// the buffer a torrent stream has always had.
///
/// The reader takes the film's first three chunks one after another --
/// a pass runs once a reader has moved a chunk, and a rate takes two
/// timed reads -- and never asks past them; what follows is then fetched
/// from Drive by something that is not the reader.
#[test]
fn a_playing_drive_file_is_read_ahead_of() -> anyhow::Result<()> {
    const CHUNK: usize = stream_server::PROXY_CACHE_CHUNK_BYTES as usize;
    let fake = Fake::start(Refreshes::Yes)?;
    let fixture = fixture(&fake)?;
    // A stated cache size publishes the budget now; the passes that size
    // and fill the window run against a published budget, which a fresh
    // server otherwise states on its own timer.
    fixture
        .handle
        .update_settings(serde_json::json!({ "cacheSize": (64u64 * 1024 * 1024) as f64 }))?;
    let body = fixture.create(REFRESH_TOKEN)?;
    let path = body["url"].as_str().expect("the url").to_string();

    let played = 3usize;
    for chunk in 0..played {
        let response = fixture.get(
            &path,
            Some(&format!(
                "bytes={}-{}",
                chunk * CHUNK,
                (chunk + 1) * CHUNK - 1
            )),
        )?;
        assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.bytes()?.len(), CHUNK);
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    // What Drive was asked for reaches past everything the reader asked
    // for: the read-ahead took the rest of the file from the head, so the
    // reader's own later chunks were answered off the disk.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let ahead = loop {
        let ranges = fake.ranges();
        let ahead: Vec<String> = ranges
            .iter()
            .filter(|range| {
                range
                    .trim_start_matches("bytes=")
                    .split_once('-')
                    .and_then(|(_, last)| last.parse::<usize>().ok())
                    .is_some_and(|last| last >= played * CHUNK)
            })
            .cloned()
            .collect();
        if !ahead.is_empty() {
            break ahead;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "Drive was never asked past what the reader requested: {ranges:?}"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    };
    assert!(
        ahead.iter().all(|range| range.starts_with("bytes=")
            && range.ends_with(&format!("-{}", FILE_LENGTH - 1))),
        "the rest of the file, to its end: {ahead:?}"
    );
    Ok(())
}
