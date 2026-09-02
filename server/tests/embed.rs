// Every test in this file starts a full embedded server. The HTTP port is
// ephemeral (`http_addr` port 0), but the server's librqbit session binds one
// of the 10 fixed BitTorrent listen ports 42000..42009 (`LISTEN_PORT_RANGE`
// in enginefs/src/backend/librqbit.rs, tried in order) -- so at most 10
// embedded servers can coexist, 9 if a desktop instance is running on the
// same machine. `cargo test` runs the tests of one binary in parallel, so
// keep the number of server-spawning tests here at 9 or below (or restrict
// parallelism) until that bound is lifted.
//
// TODO: let an embedded server ask for an ephemeral torrent listen port
// (a `ServerConfig` option mapping to port 0 in `LibrqbitBackend::new`) so
// tests are not limited by the fixed range at all.
#[test]
fn starts_and_stops_embedded_server() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;

    let handle = stream_server::start(stream_server::ServerConfig {
        // Tests must not compete with a running desktop instance (or another
        // test process) for the production port.
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..stream_server::ServerConfig::default()
    })?;

    let response = reqwest::blocking::get(format!("http://{}/heartbeat", handle.http_addr()))?
        .error_for_status()?;
    let body: serde_json::Value = response.json()?;
    assert_eq!(body["success"], true);

    handle.shutdown()?;
    assert_eq!(
        handle.join()?,
        Some(stream_server::ShutdownSource::External)
    );

    Ok(())
}

/// stremio-core probes `/device-info` at startup expecting
/// `{"availableHardwareAccelerations": [...]}`. This fork does no
/// transcoding, so the honest answer is an empty list — but the route must
/// exist (200, not 404) or every client boot logs an ERROR-level 404 in
/// diagnostics::logging.
#[test]
fn device_info_reports_no_hardware_accelerations() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..stream_server::ServerConfig::default()
    })?;

    let response = reqwest::blocking::get(format!("http://{}/device-info", handle.http_addr()))?
        .error_for_status()?;
    let body: serde_json::Value = response.json()?;
    assert_eq!(
        body.get("availableHardwareAccelerations"),
        Some(&serde_json::json!([]))
    );

    handle.shutdown()?;
    handle.join()?;

    Ok(())
}

/// stremio-core's play_on_device (models/streaming_server.rs:716-744) POSTs
/// to `casting/{device}/player` and treats any 2xx response as
/// `PlayingOnDevice`. Casting isn't implemented, so the endpoint must fail
/// visibly (non-2xx) instead of the official client silently believing
/// playback started on the device.
#[test]
fn casting_player_reports_failure_since_casting_is_not_implemented() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..stream_server::ServerConfig::default()
    })?;

    let client = reqwest::blocking::Client::new();
    let response = client
        .post(format!(
            "http://{}/casting/some-device/player",
            handle.http_addr()
        ))
        .json(&serde_json::json!({ "source": "http://example.com/video.mp4", "time": 0 }))
        .send()?;

    assert!(
        !response.status().is_success(),
        "expected a non-2xx status, got {}",
        response.status()
    );
    assert_eq!(response.status(), reqwest::StatusCode::NOT_IMPLEMENTED);
    let body: serde_json::Value = response.json()?;
    assert!(body.get("error").is_some(), "expected an error body");

    handle.shutdown()?;
    handle.join()?;

    Ok(())
}

/// stremio-video's createTorrent.js checks `resp.ok` before reading the
/// body (createTorrent.js:62); a 200 on failure leaves `guessedFileIdx`
/// undefined downstream and produces a broken `/{infoHash}/undefined`
/// stream URL. `POST /create` must fail with a non-2xx status for
/// malformed requests instead.
#[test]
fn create_engine_reports_failure_with_non_2xx_status() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..stream_server::ServerConfig::default()
    })?;

    let client = reqwest::blocking::Client::new();
    let base = format!("http://{}", handle.http_addr());

    // Neither `from` nor `torrent` given.
    let response = client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({}))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json()?;
    assert!(body.get("error").is_some(), "expected an error body");

    // `torrent` blob is not valid hex.
    let response = client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": "not-hex!" }))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json()?;
    assert!(body.get("error").is_some(), "expected an error body");

    handle.shutdown()?;
    handle.join()?;

    Ok(())
}

/// Builds a minimal valid multi-file .torrent (bencoded metainfo) so the
/// create endpoint can resolve metadata without touching the network.
/// File order: 0 = S01E01 (largest video), 1 = S01E02, 2 = readme.txt.
fn season_pack_torrent_bytes() -> Vec<u8> {
    fn bstr(value: &str) -> String {
        format!("{}:{}", value.len(), value)
    }
    fn file_entry(length: u64, name: &str) -> String {
        format!(
            "d{}i{}e{}l{}ee",
            bstr("length"),
            length,
            bstr("path"),
            bstr(name)
        )
    }

    // Total length 1700 < piece length, so exactly one (dummy) piece hash.
    let files = [
        file_entry(900, "Show.S01E01.1080p.mkv"),
        file_entry(700, "Show.S01E02.1080p.mkv"),
        file_entry(100, "readme.txt"),
    ]
    .concat();
    let info = format!(
        "d{}l{}e{}{}{}i16384e{}20:{}e",
        bstr("files"),
        files,
        bstr("name"),
        bstr("Show Season 1"),
        bstr("piece length"),
        bstr("pieces"),
        "A".repeat(20),
    );
    format!("d{}{}e", bstr("info"), info).into_bytes()
}

/// stremio-video's createTorrent.js:41-53 sends
/// `guessFileIdx: {season, episode}` when playing an episode without a known
/// fileIdx, and streams `/{infoHash}/{resp.guessedFileIdx}`. For a season
/// pack the server must return the file matching the episode, not the
/// largest file (which is a different episode here).
#[test]
fn create_engine_guesses_episode_from_season_pack() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..stream_server::ServerConfig::default()
    })?;

    let client = reqwest::blocking::Client::new();
    let blob = hex::encode(season_pack_torrent_bytes());

    // Episode hints pick S01E02 (file 1) even though S01E01 (file 0) is larger.
    let response = client
        .post(format!("http://{}/create", handle.http_addr()))
        .json(&serde_json::json!({
            "torrent": blob,
            "guessFileIdx": { "season": 1, "episode": 2 }
        }))
        .send()?
        .error_for_status()?;
    let body: serde_json::Value = response.json()?;
    assert_eq!(
        body["guessedFileIdx"], 1,
        "expected the S01E02 file, got: {body}"
    );

    // Without hints the guess falls back to the largest media file.
    let response = client
        .post(format!("http://{}/create", handle.http_addr()))
        .json(&serde_json::json!({ "torrent": blob, "guessFileIdx": {} }))
        .send()?
        .error_for_status()?;
    let body: serde_json::Value = response.json()?;
    assert_eq!(
        body["guessedFileIdx"], 0,
        "expected the largest video file, got: {body}"
    );

    handle.shutdown()?;
    handle.join()?;

    Ok(())
}

/// `GET /stats.json?sys=1` is polled roughly once a second by players.
/// Confirms the response still carries the `sys.loadavg`/`sys.cpus` shape
/// after moving the sysinfo sweep to a cached spawn_blocking call.
#[test]
fn stats_json_sys_reports_loadavg_and_cpus() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..stream_server::ServerConfig::default()
    })?;

    let response =
        reqwest::blocking::get(format!("http://{}/stats.json?sys=1", handle.http_addr()))?
            .error_for_status()?;
    let body: serde_json::Value = response.json()?;
    let loadavg = body["sys"]["loadavg"]
        .as_array()
        .expect("sys.loadavg array");
    assert_eq!(loadavg.len(), 3);
    assert!(
        body["sys"]["cpus"]
            .as_array()
            .is_some_and(|c| !c.is_empty()),
        "expected at least one reported CPU"
    );

    handle.shutdown()?;
    handle.join()?;

    Ok(())
}

/// On Android the embedding process has no usable home directory: `HOME` is
/// unset and there is no passwd fallback, so every `dirs`/`directories`
/// lookup fails. The embedded server must derive every path from the
/// `ServerConfig` it is given and come up regardless.
///
/// Env vars are process-global, so the assertion runs in a re-exec of this
/// test binary with a cleared environment (the parent only checks the exit
/// status). On Linux `dirs` silently falls back to the passwd entry when
/// `HOME` is unset, which would hide the bug, so the child gets a `HOME`
/// under `/proc` instead: it resolves, but nothing can be created there,
/// which is exactly what a `directories`-derived default path hits on
/// Android.
#[test]
fn starts_without_home_env() -> anyhow::Result<()> {
    const CHILD_MARKER: &str = "STREAM_SERVER_TEST_NO_HOME_CHILD";
    const UNUSABLE_HOME: &str = "/proc/stream-server-no-such-home";

    if std::env::var_os(CHILD_MARKER).is_none() {
        let status = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", "starts_without_home_env", "--nocapture"])
            // Scrub only the home-related variables. Clearing the whole
            // environment breaks Winsock on Windows (it needs SYSTEMROOT),
            // which made this test fail there with os error 10106.
            .env_remove("XDG_CONFIG_HOME")
            .env_remove("XDG_CACHE_HOME")
            .env_remove("XDG_DATA_HOME")
            .env_remove("XDG_STATE_HOME")
            .env("USERPROFILE", UNUSABLE_HOME)
            .env(CHILD_MARKER, "1")
            .env("HOME", UNUSABLE_HOME)
            .status()?;
        assert!(
            status.success(),
            "embedded server failed to start without a usable HOME: {status}"
        );
        return Ok(());
    }

    assert_eq!(
        std::env::var_os("HOME").as_deref(),
        Some(std::ffi::OsStr::new(UNUSABLE_HOME)),
        "child must run with the unusable HOME"
    );
    assert!(std::env::var_os("XDG_CACHE_HOME").is_none());

    let config_dir = tempfile::tempdir()?;
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        // No cache_dir: it must fall back to a location inside config_dir.
        cache_dir: None,
        ..stream_server::ServerConfig::default()
    })?;

    let response = reqwest::blocking::get(format!("http://{}/heartbeat", handle.http_addr()))?
        .error_for_status()?;
    let body: serde_json::Value = response.json()?;
    assert_eq!(body["success"], true);
    assert!(
        config_dir.path().join("config").join("cache").is_dir(),
        "cache dir must be created inside config_dir when unset"
    );

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// `/{infoHash}/stats.json` contract for the startup-phase fields: they are
/// additive (every server.js-compatible key stremio-core's `Statistics`
/// parses is still there), camelCase, and describe the guessed stream file;
/// `/{infoHash}/{fileIdx}/stats.json` describes the requested file instead.
/// The torrent has a dummy piece hash and no peers, so after the (instant)
/// hash check it must sit in `buffering` with nothing of the window on disk.
#[test]
fn stats_json_exposes_startup_phase_fields_additively() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..stream_server::ServerConfig::default()
    })?;
    let base = format!("http://{}", handle.http_addr());
    let client = reqwest::blocking::Client::new();

    let created: serde_json::Value = client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(season_pack_torrent_bytes()) }))
        .send()?
        .error_for_status()?
        .json()?;
    let info_hash = created["infoHash"]
        .as_str()
        .expect("create returns infoHash")
        .to_string();

    // Poll past the hash check (bounded); `checking` is legal in between.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let stats = loop {
        let stats: serde_json::Value = client
            .get(format!("{base}/{info_hash}/stats.json"))
            .send()?
            .error_for_status()?
            .json()?;
        match stats["phase"].as_str() {
            Some("checking") if std::time::Instant::now() < deadline => {
                assert!(
                    stats["checkedBytes"].is_u64(),
                    "checking exposes checkedBytes: {stats}"
                );
                assert_eq!(stats["checkTotalBytes"], 1700, "{stats}");
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            _ => break stats,
        }
    };

    // Legacy keys untouched.
    let obj = stats.as_object().unwrap();
    for key in [
        "name",
        "infoHash",
        "files",
        "sources",
        "opts",
        "downloadSpeed",
        "uploadSpeed",
        "downloaded",
        "uploaded",
        "unchoked",
        "peers",
        "queued",
        "unique",
        "connectionTries",
        "peerSearchRunning",
        "streamLen",
        "streamName",
        "streamProgress",
        "swarmConnections",
        "swarmPaused",
        "swarmSize",
    ] {
        assert!(obj.contains_key(key), "legacy key {key} missing: {stats}");
    }
    assert_eq!(stats["infoHash"], info_hash);
    assert_eq!(stats["streamName"], "Show.S01E01.1080p.mkv");

    // New fields, describing the guessed stream file (900 bytes < window).
    assert_eq!(stats["phase"], "buffering", "{stats}");
    assert_eq!(stats["checkedBytes"], serde_json::Value::Null);
    assert_eq!(stats["checkTotalBytes"], serde_json::Value::Null);
    assert_eq!(stats["initialWindowBytes"], 900, "{stats}");
    assert_eq!(stats["initialWindowReadyBytes"], 0, "{stats}");
    let discovery = stats["peerDiscovery"]
        .as_object()
        .expect("peerDiscovery object");
    for key in ["seen", "queued", "connecting", "live"] {
        assert!(discovery[key].is_u64(), "peerDiscovery.{key}: {stats}");
    }
    assert_eq!(stats["files"][1]["initialWindowBytes"], 700);
    assert_eq!(stats["files"][1]["initialWindowReadyBytes"], 0);

    // Per-file stats focus the requested file.
    let file_stats: serde_json::Value = client
        .get(format!("{base}/{info_hash}/1/stats.json"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(file_stats["streamName"], "Show.S01E02.1080p.mkv");
    assert_eq!(file_stats["phase"], "buffering");
    assert_eq!(file_stats["initialWindowBytes"], 700, "{file_stats}");
    assert_eq!(file_stats["initialWindowReadyBytes"], 0);

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// A progress overlay polls `stats.json` from the moment playback is
/// requested -- typically before the first stream request, and while the
/// magnet is still resolving its metadata. Both stats routes must then (a)
/// start the engine the way the stream route does, with the addon's `tr=`
/// trackers (librqbit cannot add trackers later, and the stream request will
/// reuse this engine), and (b) answer immediately with 200 and the
/// torrent-level `resolvingMetadata` phase rather than blocking on metadata
/// or 404ing the per-file route. A 404 is reserved for an index that does
/// not exist once metadata is known.
///
/// The same holds with the roles reversed: stremio-core's
/// `/{infoHash}/create` (POST, with the stream's `peerSearch.sources`) must
/// join the shared magnet add rather than start a private one with
/// `EngineFS::add_torrent`, so a stats poll arriving while the create is
/// still resolving sees that very add, with the create's trackers. (One
/// server instance for both flows: each embedded server takes one of only
/// ten librqbit listen ports, and the embed tests run in parallel.)
#[test]
fn stats_json_reports_resolving_metadata_with_the_requests_trackers() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..stream_server::ServerConfig::default()
    })?;
    let base = format!("http://{}", handle.http_addr());
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    // No peers and an unreachable tracker: this magnet never resolves.
    let unresolved = "00112233445566778899aabbccddeeff00112233";
    let tracker = "udp://stats-first.invalid:6969/announce";
    let tr = format!("tr=tracker%3A{}", urlencoding::encode(tracker));
    // The per-file route is polled first here, so it is the one creating the engine.
    for path in [
        format!("{unresolved}/0/stats.json?{tr}"),
        format!("{unresolved}/-1/stats.json?{tr}"),
        format!("{unresolved}/stats.json?{tr}"),
        // Later polls without trackers still see the tracker set used.
        format!("{unresolved}/stats.json"),
    ] {
        let response = client.get(format!("{base}/{path}")).send()?;
        assert_eq!(response.status(), reqwest::StatusCode::OK, "{path}");
        let stats: serde_json::Value = response.json()?;
        assert_eq!(stats["phase"], "resolvingMetadata", "{path}: {stats}");
        assert_eq!(stats["hasMetadata"], false, "{path}: {stats}");
        assert_eq!(stats["infoHash"], unresolved, "{path}: {stats}");
        assert_eq!(stats["files"], serde_json::json!([]), "{path}: {stats}");
        assert!(stats["peerDiscovery"].is_object(), "{path}: {stats}");
        let sources: Vec<&str> = stats["sources"]
            .as_array()
            .expect("sources array")
            .iter()
            .filter_map(|s| s["url"].as_str())
            .collect();
        assert!(
            sources.contains(&tracker),
            "{path}: tr= tracker missing from sources {sources:?}"
        );
    }

    // Create-first: this magnet never resolves either, so the create request
    // blocks past the client timeout while its add lives on in the registry.
    let create_first = "445566778899aabbccddeeff0011223344556677";
    let create_tracker = "udp://create-first.invalid:6969/announce";
    let created = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()?
        .post(format!("{base}/{create_first}/create"))
        .json(&serde_json::json!({
            "peerSearch": { "sources": [format!("tracker:{create_tracker}")] },
            "guessFileIdx": {}
        }))
        .send();
    assert!(
        created.is_err(),
        "create must wait for metadata, got {created:?}"
    );
    let stats: serde_json::Value = client
        .get(format!("{base}/{create_first}/stats.json"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(stats["phase"], "resolvingMetadata", "{stats}");
    let sources: Vec<&str> = stats["sources"]
        .as_array()
        .expect("sources array")
        .iter()
        .filter_map(|s| s["url"].as_str())
        .collect();
    assert!(
        sources.contains(&create_tracker),
        "stats must report the add started by /create, with its trackers; got {sources:?}"
    );

    // Once metadata is known, a file index that does not exist is still 404.
    let created: serde_json::Value = client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(season_pack_torrent_bytes()) }))
        .send()?
        .error_for_status()?
        .json()?;
    let known = created["infoHash"]
        .as_str()
        .expect("create returns infoHash");
    let response = client.get(format!("{base}/{known}/99/stats.json")).send()?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    let response = client.get(format!("{base}/{known}/1/stats.json")).send()?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    handle.shutdown()?;
    handle.join()?;

    Ok(())
}
