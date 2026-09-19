//! What the log files may not contain (review #17).
//!
//! `/proxy` takes the URL it fetches -- and the request headers it attaches
//! -- from the caller, in the *path* as well as the query, and an addon's
//! stream URL is where a debrid token or a signed CDN link lives. The
//! process keeps the last ten launches' logs, so a URL written to one at a
//! level that is always on is that credential filed on the device.
//!
//! A binary of its own because `init_logging` installs the process's
//! subscriber once: a second logging test in the same binary would write
//! into whichever tempdir won the race.

use std::io::Read;

/// A string that appears nowhere but in the URLs this test sends.
const SECRET: &str = "s3cr3t-token-do-not-log";

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

/// An origin that answers every request with a `302` back to itself,
/// carrying the caller's token in the `Location` -- a redirect loop, which
/// is what makes `/proxy` give up and say so.
fn looping_origin() -> std::net::SocketAddr {
    use std::io::{Read as _, Write as _};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        while let Ok((mut socket, _)) = listener.accept() {
            let mut head = [0u8; 4096];
            let _ = socket.read(&mut head);
            let _ = socket.write_all(
                format!(
                    "HTTP/1.1 302 Found\r\nLocation: http://{addr}/again?token={SECRET}\r\n\
                     Content-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            );
            let _ = socket.flush();
        }
    });
    addr
}

/// The proxied URL, the credentials beside it and the archive link never
/// reach the log; what is written is the origin and the route.
#[test]
fn a_caller_supplied_url_is_logged_as_its_origin_only() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let config_dir = dir.path().join("config");
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.clone()),
        cache_dir: Some(dir.path().join("cache")),
        lan_media_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0))),
        init_logging: true,
        resolve_dht_bootstrap_names: false,
        use_public_trackers: false,
        pins: Some(Default::default()),
        ..stream_server::ServerConfig::default()
    })?;
    let base = format!("http://{}", handle.http_addr());
    let client = reqwest::blocking::Client::new();

    // A dead port, so every fetch fails and every failure is logged.
    let target = format!("http://127.0.0.1:1/film.mkv?token={SECRET}");
    let encoded = urlencoding::encode(&target).into_owned();

    // The Core spelling, which carries the target and the caller's headers
    // in the path.
    let response = client
        .get(format!(
            "{base}/proxy/d={encoded}&h={}/film.mkv",
            urlencoding::encode(&format!("Authorization:Bearer {SECRET}"))
        ))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_GATEWAY);

    // And the query spelling.
    let response = client.get(format!("{base}/proxy/?d={encoded}")).send()?;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_GATEWAY);

    // A redirect loop, which `/proxy` gives up on at WARN -- with the URL
    // it gave up on, which is the caller's.
    let looping = looping_origin();
    let response = client
        .get(format!(
            "{base}/proxy/?d={}",
            urlencoding::encode(&format!("http://{looping}/film.mkv?token={SECRET}"))
        ))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_GATEWAY);

    // A request nothing serves, which is logged at ERROR with everything
    // about it (review #93): the path and the query used to go in whole.
    let response = client
        .get(format!(
            "{base}/no-such-route?d={encoded}&h={}",
            urlencoding::encode(&format!("Authorization:Bearer {SECRET}"))
        ))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    // And one whose *path* carries the target: `/proxy` is not mounted on
    // the LAN media listener, so a request for it there is unhandled -- and
    // that path is the Core spelling, target and `h=` headers included.
    handle.update_settings(serde_json::json!({ "lanMediaEnabled": true }))?;
    let lan = handle
        .set_lan_media(true)?
        .ok_or_else(|| anyhow::anyhow!("the LAN listener answered with no address"))?;
    let response = client
        .get(format!(
            "http://{lan}/proxy/d={encoded}&h={}/film.mkv",
            urlencoding::encode(&format!("Authorization:Bearer {SECRET}"))
        ))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    // An archive create, whose download is logged at INFO and whose failure
    // at ERROR.
    let response = client
        .post(format!("{base}/zip/create"))
        .json(&serde_json::json!({ "urls": [target] }))
        .send()?;
    assert!(!response.status().is_success(), "{}", response.status());

    handle.shutdown()?;
    handle.join()?;

    let logs = log_text(&config_dir)?;
    assert!(!logs.is_empty(), "the process wrote logs at all");
    assert!(
        logs.contains("too many redirects"),
        "the relay said it gave up: {logs}"
    );
    assert!(
        logs.contains("unhandled request") && logs.contains("query_keys"),
        "the unhandled request is still reported, with the shape of what was asked: {logs}"
    );
    assert!(
        logs.contains("\"path\":\"/proxy\""),
        "the request span names the route it was: {logs}"
    );
    assert!(
        logs.contains("http://127.0.0.1:1\""),
        "and the archive's origin is what a field report is read for: {logs}"
    );
    assert!(
        !logs.contains(SECRET),
        "a caller's credential reached the log files: {}",
        logs.lines()
            .filter(|line| line.contains(SECRET))
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert!(
        !logs.contains("film.mkv"),
        "and so did the path it came in: {}",
        logs.lines()
            .filter(|line| line.contains("film.mkv"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    Ok(())
}
