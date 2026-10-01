//! What a *debug* line may say about a caller's credentials: nothing.
//! `log_redaction.rs` holds the lines the default filter writes; this is the
//! rest of them, because a field report is taken with `RUST_LOG` turned up
//! and the log files this process keeps for the last ten launches are the
//! one place a signed URL's token, an FTP login, an `h=` header value or the
//! proxy password in a settings patch must not turn up.
//!
//! A binary of its own because it installs the process's global
//! subscriber, at `trace` for this server's crates -- and `log_redaction.rs`
//! installs the server's own.
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Every line the subscriber wrote, as text.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Captured {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl Write for Captured {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

const LENGTH: usize = 2 * 1024 * 1024;

/// A ranged origin with an `ETag`, so what it serves is cached.
fn origin() -> anyhow::Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            std::thread::spawn(move || {
                let Ok(peer) = stream.try_clone() else { return };
                let mut reader = BufReader::new(peer);
                let mut range = None;
                loop {
                    let mut line = String::new();
                    match reader.read_line(&mut line) {
                        Ok(0) | Err(_) => break,
                        Ok(_) if line.trim().is_empty() => break,
                        Ok(_) => {}
                    }
                    if let Some((name, value)) = line.split_once(':')
                        && name.trim().eq_ignore_ascii_case("range")
                    {
                        range = value
                            .trim()
                            .trim_start_matches("bytes=")
                            .split_once('-')
                            .and_then(|(first, last)| {
                                Some((first.parse::<usize>().ok()?, last.parse::<usize>().ok()?))
                            });
                    }
                }
                let (first, last) = range.unwrap_or((0, LENGTH - 1));
                let last = last.min(LENGTH - 1);
                let body: Vec<u8> = (first..=last).map(|i| (i % 251) as u8).collect();
                let head = format!(
                    "HTTP/1.1 206 Partial Content\r\nAccept-Ranges: bytes\r\n\
                     Content-Type: video/mp4\r\nETag: \"redaction\"\r\n\
                     Content-Range: bytes {first}-{last}/{LENGTH}\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes());
                let _ = stream.write_all(&body);
            });
        }
    });
    Ok(addr)
}

fn encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

#[test]
fn no_debug_line_carries_a_callers_credentials() -> anyhow::Result<()> {
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "stream_server=trace,enginefs=trace",
        ))
        .with_ansi(false)
        .with_writer({
            let captured = captured.clone();
            move || captured.clone()
        })
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let dir = tempfile::tempdir()?;
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(dir.path().join("config")),
        cache_dir: Some(dir.path().join("cache")),
        resolve_dht_bootstrap_names: false,
        use_public_trackers: false,
        enable_local_service_discovery: false,
        enable_dht: false,
        torrent_listen_port: stream_server::TorrentListenPort::Loopback,
        pins: Some(Default::default()),
        proxy_pins: Some(Vec::new()),
        lan_media_addr: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
        ..stream_server::ServerConfig::default()
    })?;
    let base = format!("http://{}", handle.http_addr());
    let client = reqwest::blocking::Client::new();

    // A settings patch with the proxy password in it.
    let _ = handle.update_settings(serde_json::json!({ "btProxyPassword": "SECRET-PATCH" }));

    // An FTP login, to a port nothing listens on: the transfer fails.
    let ftp = serde_json::json!({ "ftpUrl": "ftp://viewer:SECRET-FTP@127.0.0.1:1/film.mkv" });
    let lz = lz_str::compress_to_encoded_uri_component(ftp.to_string().as_str());
    let response = client.get(format!("{base}/ftp/film.mkv?lz={lz}")).send()?;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_GATEWAY);

    // An `h=` value the header rules refuse.
    let origin = format!("http://{}", origin()?);
    let bad_header = encode("X-Api-Key:\u{7f}SECRET-HEADER");
    let _ = client
        .get(format!(
            "{base}/proxy/d={}&h={bad_header}/film.mp4",
            encode(&origin)
        ))
        .header(reqwest::header::RANGE, "bytes=0-1023")
        .send()?
        .bytes();

    // A signed URL, read until a range is answered from the cache.
    let signed = format!(
        "{base}/proxy/d={}/film.mp4?token=SECRET-QUERY",
        encode(&origin)
    );
    let range = format!("bytes=0-{}", 1024 * 1024 - 1);
    let deadline = Instant::now() + Duration::from_secs(20);
    while !captured
        .text()
        .contains("answering a proxied range from the cache")
    {
        anyhow::ensure!(Instant::now() < deadline, "no read was a cache hit");
        let _ = client
            .get(&signed)
            .header(reqwest::header::RANGE, &range)
            .send()?
            .bytes();
        std::thread::sleep(Duration::from_millis(50));
    }

    // A cast token, which is a URL into this device for as long as it is
    // published: a body served under it, and a request after it is gone.
    handle.update_settings(serde_json::json!({ "lanMediaEnabled": true }))?;
    let lan = handle
        .set_lan_media(true)?
        .ok_or_else(|| anyhow::anyhow!("the LAN listener answered with no address"))?;
    let id = handle.register(stream_server::MediaSpec::StreamingUrl(
        stream_server::Url::parse(&format!("{base}/proxy/d={}/film.mp4", encode(&origin)))?,
    ))?;
    let token = handle.publish(&id, None)?;
    let cast = format!("http://{lan}/cast/{}", token.as_str());
    let response = client
        .get(&cast)
        .header(reqwest::header::RANGE, "bytes=0-1023")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.bytes()?.len(), 1024);
    assert!(handle.unpublish(&token));
    assert_eq!(
        client.get(&cast).send()?.status(),
        reqwest::StatusCode::NOT_FOUND
    );

    handle.shutdown()?;
    handle.join()?;
    let text = captured.text();
    assert!(
        text.contains("cast_body_start") && text.contains("cast unpublished"),
        "the cast lines this test is about were written"
    );
    let line = text.lines().find(|line| line.contains(token.as_str()));
    assert!(line.is_none(), "a cast token was logged: {line:?}");
    assert!(
        text.contains("FTP transfer failed")
            && text.contains("Skipping invalid custom request header")
            && text.contains("update_settings: received"),
        "every line this test is about was written"
    );
    for secret in [
        "SECRET-PATCH",
        "SECRET-FTP",
        "SECRET-HEADER",
        "SECRET-QUERY",
    ] {
        let line = text.lines().find(|line| line.contains(secret));
        assert!(line.is_none(), "{secret} was logged: {line:?}");
    }
    Ok(())
}
