// What the log files may not contain about a Google Drive pairing.
//
// The refresh token is the longest-lived secret this device holds: it does
// not expire on its own and it reaches every file the account has picked
// through this OAuth client. The process keeps the last ten launches'
// logs, so a token written to one at a level that is always on is that
// credential filed on the device -- and the file id is written *on
// purpose*, because it names content and is the cache key already.
//
// A binary of its own for the same reason `log_redaction.rs` is one:
// `init_logging` installs the process's subscriber once, and a second
// logging test in the same binary would write into whichever tempdir won
// the race.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

/// A string that appears nowhere but in this test's own requests.
const REFRESH_TOKEN: &str = "refresh-tok-c07e-never-log-me";
const FILE_ID: &str = "1ZyXwVuTsRqPoNmLkJiHgFeDcBa";

fn log_text(config_dir: &std::path::Path) -> anyhow::Result<String> {
    let mut all = String::new();
    for entry in std::fs::read_dir(config_dir.join("logs"))? {
        let path = entry?.path();
        if path.is_file() {
            let mut text = String::new();
            std::fs::File::open(&path)?.read_to_string(&mut text)?;
            all.push_str(&text);
        }
    }
    Ok(all)
}

/// A pairing service that refuses every grant -- and echoes it, which is
/// what a naive one would do and what makes this test worth having: if any
/// of that body reached a log line or an error, the search below finds it.
fn refusing_service() -> anyhow::Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    std::thread::spawn(move || {
        while let Ok((stream, _)) = listener.accept() {
            std::thread::spawn(move || answer(stream));
        }
    });
    Ok(addr)
}

fn answer(mut stream: TcpStream) {
    let Ok(second) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(second);
    let mut line = String::new();
    let mut length = 0usize;
    while reader.read_line(&mut line).unwrap_or(0) > 0 {
        if line == "\r\n" || line == "\n" {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length: ") {
            length = value.trim().parse().unwrap_or(0);
        }
        line.clear();
    }
    let mut body = vec![0u8; length];
    let _ = reader.read_exact(&mut body);
    let payload = format!("{{\"error\":\"invalid_grant for {REFRESH_TOKEN}\",\"pairAgain\":true}}");
    let head = format!(
        "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n",
        payload.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(payload.as_bytes());
    let _ = stream.flush();
}

/// The grant reaches no log line -- not from the create's request, not
/// from the refresh that failed, and not from the service's own body.
#[test]
fn a_drive_grant_never_reaches_the_log() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let config_dir = dir.path().join("config");
    let service = refusing_service()?;
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.clone()),
        cache_dir: Some(dir.path().join("cache")),
        init_logging: true,
        pins: Some(Default::default()),
        resolve_dht_bootstrap_names: false,
        use_public_trackers: false,
        enable_local_service_discovery: false,
        drive_refresh_endpoint: Some(url::Url::parse(&format!("http://{service}/refresh"))?),
        ..stream_server::ServerConfig::default()
    })?;
    let base = format!("http://{}", handle.http_addr());
    let client = reqwest::blocking::Client::new();
    let token = handle.auth_token().expect("a token").to_string();

    // The create, which fails at the refresh -- the loudest path there is,
    // because it logs and it answers.
    let response = client
        .post(format!("{base}/drive/create"))
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "fileId": FILE_ID,
            "refreshToken": REFRESH_TOKEN,
            "name": "A Film.mkv",
        }))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    let answered = response.text()?;
    assert!(!answered.contains(REFRESH_TOKEN), "{answered}");

    // And a request for a route nothing serves, whose whole path and query
    // are logged at ERROR: a build that ever put the grant in a URL would
    // be caught here even if the route above never logged a thing.
    let response = client
        .get(format!("{base}/no-such-route?refreshToken={REFRESH_TOKEN}"))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    handle.shutdown()?;
    let _ = handle.join();

    let text = log_text(&config_dir)?;
    assert!(!text.is_empty(), "the test wrote no log at all");
    // The one line this asserts the *presence* of would be the file id,
    // which is deliberately logged -- so the search is for the grant, and
    // the unhandled-request line above is the proof the search had
    // something to find.
    assert!(
        !text.contains(REFRESH_TOKEN),
        "the refresh token reached a log file"
    );
    Ok(())
}
