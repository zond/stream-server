//! What the log files may not contain.
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

/// The rendition producer the renditions tests drive, for a rendition
/// token's requests here.
#[path = "support/test_producer.rs"]
mod test_producer;

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
        enable_dht: false,
        torrent_listen_port: stream_server::TorrentListenPort::Loopback,
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
    // about it: the query's key names, never its values.
    let response = client
        .get(format!(
            "{base}/no-such-route?d={encoded}&h={}",
            urlencoding::encode(&format!("Authorization:Bearer {SECRET}"))
        ))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    // And one whose *path* carries the target: `/proxy` is not mounted on
    // the LAN media listener (it serves `/cast/{token}` alone), so a request
    // for it there is unhandled -- and that path is the Core spelling,
    // target and `h=` headers included.
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

    // A cast token is a URL into this device for as long as it is
    // published: requested on the LAN listener -- answered (the id's origin
    // is dead, so a refusal), with a method the route does not take,
    // unpublished -- it must reach no line, and neither may an unknown one.
    let id = handle.register(stream_server::MediaSpec::StreamingUrl(
        stream_server::Url::parse(&format!("{base}/proxy/?d={encoded}"))?,
    ))?;
    let token = handle.publish(&id, None)?;
    let cast = format!("http://{lan}/cast/{}", token.as_str());
    assert_eq!(
        client.get(&cast).send()?.status(),
        reqwest::StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        client.head(&cast).send()?.status(),
        reqwest::StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        client.post(&cast).send()?.status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED
    );
    assert!(handle.unpublish(&token));
    assert_eq!(
        client.get(&cast).send()?.status(),
        reqwest::StatusCode::NOT_FOUND
    );
    let unknown = "0123456789abcdef0123456789abcdef";
    assert_eq!(
        client
            .get(format!("http://{lan}/cast/{unknown}"))
            .send()?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );

    // A rendition's token, under its `hls/` paths: the playlist, the init
    // segment and a segment produced for it, a `HEAD`, one past the end,
    // and the same after the unpublish.
    let film = dir.path().join("film.mkv");
    std::fs::write(&film, vec![7u8; 64 * 1024])?;
    let local = handle.register(stream_server::MediaSpec::Local {
        file: stream_server::LocalFile::Path(film),
        name: None,
    })?;
    handle.install_producer(test_producer::TestProducer::new(
        test_producer::Knobs::default(),
    ));
    let rendition = handle.publish_rendition(
        &local,
        stream_server::RenditionSpec {
            duration_ms: 20_000,
            segment_ms: 1000,
            start_ms: 0,
            video: stream_server::VideoPlan::Copy,
            audio: stream_server::AudioPlan::Copy,
            audio_track: 0,
        },
        None,
    )?;
    let hls = format!("http://{lan}/cast/{}/hls", rendition.as_str());
    for file in ["index.m3u8", "media.m3u8", "init.mp4", "0.m4s"] {
        assert_eq!(
            client.get(format!("{hls}/{file}")).send()?.status(),
            reqwest::StatusCode::OK,
            "{file}"
        );
    }
    assert_eq!(
        client.head(format!("{hls}/1.m4s")).send()?.status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        client.get(format!("{hls}/20.m4s")).send()?.status(),
        reqwest::StatusCode::NOT_FOUND
    );
    assert!(handle.unpublish(&rendition));
    assert_eq!(
        client.get(format!("{hls}/2.m4s")).send()?.status(),
        reqwest::StatusCode::NOT_FOUND
    );

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
        logs.contains("\"path\":\"/cast\""),
        "and the LAN request span names the cast route: {logs}"
    );
    assert!(
        logs.contains("rendition_run_start"),
        "the rendition's run was logged at all: {logs}"
    );
    for token in [token.as_str(), rendition.as_str(), unknown] {
        assert!(
            !logs.contains(token),
            "a cast token reached the log files: {}",
            logs.lines()
                .filter(|line| line.contains(token))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
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
