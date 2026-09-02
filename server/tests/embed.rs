// Every test in this file starts a full embedded server. Both of its ports
// are ephemeral: the HTTP port (`http_addr` port 0) and, through
// `ServerConfig::embedded`'s `TorrentListenPort::Ephemeral`, librqbit's
// BitTorrent listener -- so any number of these servers coexist with each
// other and with a desktop instance on its fixed 42000..42010 range, and
// `cargo test`'s parallelism needs no limiting.

use stream_server::{ServerAuth, ServerConfig, ServerHandle, TorrentListenPort};

/// Client builder that sends the server's bearer token (if it has one) on
/// every request -- every control route requires it.
fn bearer_client_builder(handle: &ServerHandle) -> reqwest::blocking::ClientBuilder {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(token) = handle.auth_token() {
        headers.insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token}").parse().expect("valid header"),
        );
    }
    reqwest::blocking::Client::builder().default_headers(headers)
}

fn bearer_client(handle: &ServerHandle) -> anyhow::Result<reqwest::blocking::Client> {
    Ok(bearer_client_builder(handle).build()?)
}

/// Both stock configurations generate a per-launch token; opening the
/// control API is an explicit opt-out (`ServerAuth::Disabled`, `--no-auth`).
#[test]
fn stock_configs_default_to_a_generated_token() {
    assert_eq!(ServerConfig::embedded().auth, ServerAuth::Generated);
    assert_eq!(ServerConfig::binary_default().auth, ServerAuth::Generated);
    assert_eq!(ServerConfig::default().auth, ServerAuth::Generated);
}

/// An embedded server takes an OS-assigned BitTorrent listen port; only the
/// desktop binary keeps the fixed, forwardable range.
#[test]
fn embedded_config_uses_an_ephemeral_torrent_port_the_binary_a_fixed_range() {
    assert_eq!(
        ServerConfig::embedded().torrent_listen_port,
        TorrentListenPort::Ephemeral
    );
    assert_eq!(
        ServerConfig::binary_default().torrent_listen_port,
        TorrentListenPort::Fixed(42000..42010)
    );
}

/// Two embedded servers started at the same time both come up: neither the
/// HTTP listener nor the librqbit session competes for a fixed port.
#[test]
fn two_embedded_servers_start_concurrently() -> anyhow::Result<()> {
    let dirs: Vec<_> = (0..2)
        .map(|_| Ok((tempfile::tempdir()?, tempfile::tempdir()?)))
        .collect::<anyhow::Result<_>>()?;
    let handles = dirs
        .iter()
        .map(|(config_dir, cache_dir)| {
            stream_server::start(stream_server::ServerConfig {
                http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
                config_dir: Some(config_dir.path().join("config")),
                cache_dir: Some(cache_dir.path().join("cache")),
                ..stream_server::ServerConfig::default()
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    assert_ne!(handles[0].http_addr(), handles[1].http_addr());
    for handle in &handles {
        let heartbeat: serde_json::Value = bearer_client(handle)?
            .get(format!("http://{}/heartbeat", handle.http_addr()))
            .send()?
            .error_for_status()?
            .json()?;
        assert_eq!(heartbeat["success"], true);
    }
    for handle in handles {
        handle.shutdown()?;
        handle.join()?;
    }
    Ok(())
}

/// Start/stop round trip, plus the auth contract of the default
/// (`ServerAuth::Generated`) server: the handle exposes the token, control
/// routes 401 without it or with a wrong one (fixed body, no hint), accept
/// it in `Authorization: Bearer`, and media routes stay open -- `/ftp/...`
/// without a token gets its ordinary 400 for a missing `lz`, never a 401.
///
/// Also open: the `/local-addon` stub. stremio-core's default profile carries
/// the protected `http://127.0.0.1:11470/local-addon/manifest.json` addon and
/// requests `/local-addon/stream/{type}/{id}.json` on every details page, so
/// the stub must answer the manifest and an empty stream list without a token
/// (legacy clients call it too); `meta` stays a 404.
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
    let base = format!("http://{}", handle.http_addr());

    let token = handle.auth_token().expect("generated token").to_string();
    assert_eq!(token.len(), 64, "32 random bytes as hex");
    assert!(token.bytes().all(|c| c.is_ascii_hexdigit()));

    let anonymous = reqwest::blocking::Client::new();
    let response = anonymous.get(format!("{base}/heartbeat")).send()?;
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok()),
        Some("Bearer")
    );
    assert_eq!(response.text()?, "unauthorized");

    let response = anonymous
        .get(format!("{base}/heartbeat"))
        .bearer_auth(format!("{}0", &token[1..]))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(response.text()?, "unauthorized");

    // Token in the query string is not accepted: header only.
    let response = anonymous
        .get(format!("{base}/heartbeat?token={token}"))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);

    // A nested control router (`/casting`, the path stremio-core requests) is
    // behind the same middleware; `/casting/` is no route at all, so it falls
    // through to the (open) 404 fallback like any unknown path.
    let response = anonymous.get(format!("{base}/casting")).send()?;
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(response.text()?, "unauthorized");
    let response = anonymous.get(format!("{base}/casting/")).send()?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    let response = bearer_client(&handle)?
        .get(format!("{base}/heartbeat"))
        .send()?
        .error_for_status()?;
    let body: serde_json::Value = response.json()?;
    assert_eq!(body["success"], true);

    let response = anonymous.get(format!("{base}/ftp/movie.mkv")).send()?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "media routes are open (400 = missing lz parameter, not 401)"
    );

    let manifest: serde_json::Value = anonymous
        .get(format!("{base}/local-addon/manifest.json"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(manifest["id"], "org.stremio.local");
    assert_eq!(manifest["name"], "Local Files");
    assert_eq!(manifest["resources"], serde_json::json!([]));
    assert_eq!(manifest["types"], serde_json::json!([]));
    assert_eq!(manifest["catalogs"], serde_json::json!([]));
    assert!(manifest["version"].is_string());
    for path in [
        "/local-addon/stream/movie/tt0111161.json",
        "/local-addon/stream/series/tt0903747:1:1.json",
        "/local-addon/stream/movie/tt0111161",
    ] {
        let response = anonymous.get(format!("{base}{path}")).send()?;
        assert_eq!(response.status(), reqwest::StatusCode::OK, "{path}");
        let body: serde_json::Value = response.json()?;
        assert_eq!(body, serde_json::json!({ "streams": [] }), "{path}");
    }
    let response = anonymous
        .get(format!("{base}/local-addon/meta/movie/local:abc.json"))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    handle.shutdown()?;
    assert_eq!(
        handle.join()?,
        Some(stream_server::ShutdownSource::External)
    );

    Ok(())
}

/// The two status probes clients poll, on one server.
///
/// stremio-core probes `/device-info` at startup expecting
/// `{"availableHardwareAccelerations": [...]}`. This fork does no
/// transcoding, so the honest answer is an empty list — but the route must
/// exist (200, not 404) or every client boot logs an ERROR-level 404 in
/// diagnostics::logging.
///
/// `GET /stats.json?sys=1` is polled roughly once a second by players.
/// Confirms the response still carries the `sys.loadavg`/`sys.cpus` shape
/// after moving the sysinfo sweep to a cached spawn_blocking call.
#[test]
fn device_info_and_stats_json_sys_probes_keep_their_shapes() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..stream_server::ServerConfig::default()
    })?;
    let client = bearer_client(&handle)?;

    let response = client
        .get(format!("http://{}/device-info", handle.http_addr()))
        .send()?
        .error_for_status()?;
    let body: serde_json::Value = response.json()?;
    assert_eq!(
        body.get("availableHardwareAccelerations"),
        Some(&serde_json::json!([]))
    );

    let response = client
        .get(format!("http://{}/stats.json?sys=1", handle.http_addr()))
        .send()?
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

/// stremio-core's play_on_device (models/streaming_server.rs:716-744) POSTs
/// to `casting/{device}/player` and treats any 2xx response as
/// `PlayingOnDevice`. Casting isn't implemented, so the endpoint must fail
/// visibly (non-2xx) instead of the official client silently believing
/// playback started on the device.
///
/// This server runs with `ServerAuth::Disabled` (the binary's `--no-auth`):
/// the handle has no token and control routes answer without a header.
#[test]
fn casting_player_reports_failure_since_casting_is_not_implemented() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        auth: ServerAuth::Disabled,
        ..stream_server::ServerConfig::default()
    })?;
    assert_eq!(handle.auth_token(), None);

    // No Authorization header anywhere in this test.
    let client = reqwest::blocking::Client::new();
    let heartbeat: serde_json::Value = client
        .get(format!("http://{}/heartbeat", handle.http_addr()))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(heartbeat["success"], true);

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

    let client = bearer_client(&handle)?;
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

    let client = bearer_client(&handle)?;
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

/// The library API on `ServerHandle` is the same code the control routes
/// run, so an embedder (FFI, no HTTP client) sees exactly what a client
/// polling over HTTP would: `settings()` is `GET /settings`' `values`,
/// `update_settings` is `POST /settings` (same merge/validation, visible to
/// the next GET), `engine_stats` is `/{infoHash}/stats.json` -- including
/// creating the engine with the given trackers on first sight -- normalised
/// like the route's `tr=` values: `tracker:` stripped, `dht:` dropped -- and
/// answering `resolvingMetadata` at once -- and `file_stats` is
/// `/{infoHash}/{fileIdx}/stats.json`, with the route's 404 as `FileNotFound`.
#[test]
fn library_api_matches_the_http_control_routes() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..stream_server::ServerConfig::default()
    })?;
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client_builder(&handle)
        .timeout(std::time::Duration::from_secs(5))
        .build()?;
    assert_eq!(handle.base_url(), base);

    // settings() == GET /settings values.
    let http: serde_json::Value = client
        .get(format!("{base}/settings"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(http["baseUrl"], base);
    assert_eq!(serde_json::to_value(handle.settings()?)?, http["values"]);

    // update_settings() == POST /settings: applied, validated, persisted.
    let updated = handle.update_settings(serde_json::json!({
        "btMaxConnections": 77,
        "seedingEnabled": false,
        // Wrong type: left unchanged, as the HTTP route leaves it.
        "btHandshakeTimeout": "not-a-number"
    }))?;
    assert_eq!(updated.bt_max_connections, 77);
    assert!(!updated.seeding_enabled);
    assert_eq!(updated.bt_handshake_timeout, 20000);
    let http: serde_json::Value = client
        .get(format!("{base}/settings"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(serde_json::to_value(&updated)?, http["values"]);
    let persisted: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        config_dir.path().join("config").join("settings.json"),
    )?)?;
    assert_eq!(persisted["btMaxConnections"], 77);
    // And the other way round: a POST is visible to settings().
    client
        .post(format!("{base}/settings"))
        .json(&serde_json::json!({ "btMaxConnections": 55 }))
        .send()?
        .error_for_status()?;
    assert_eq!(handle.settings()?.bt_max_connections, 55);

    // engine_stats() creates the engine with the trackers on first sight and
    // answers resolvingMetadata at once, exactly like the route; a later poll
    // over HTTP sees that very engine.
    let unresolved = "8899aabbccddeeff00112233445566778899aabb";
    let tracker = "udp://library-first.invalid:6969/announce";
    // The sources exactly as a stream's `sources` array carries them: the
    // library normalises them the way the route normalises `tr=`.
    let raw_sources = [format!(" tracker:{tracker}"), format!("dht:{unresolved}")];
    let api = handle.engine_stats(unresolved, &raw_sources)?;
    assert_eq!(api.info_hash, unresolved);
    let api_json = serde_json::to_value(&api)?;
    assert_eq!(api_json["phase"], "resolvingMetadata", "{api_json}");
    let sources: Vec<&str> = api_json["sources"]
        .as_array()
        .expect("sources array")
        .iter()
        .filter_map(|s| s["url"].as_str())
        .collect();
    // (The engine merges its default tracker list in as well, so check for
    // the normalised entry and the absence of the raw ones.)
    assert!(sources.contains(&tracker), "{sources:?}");
    assert!(
        sources
            .iter()
            .all(|s| !s.starts_with("tracker:") && !s.starts_with("dht:") && *s == s.trim()),
        "raw sources must not reach the engine: {sources:?}"
    );
    let http: serde_json::Value = client
        .get(format!("{base}/{unresolved}/stats.json"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(api_json, http);

    // file_stats() == /{infoHash}/{fileIdx}/stats.json for a known torrent.
    let created: serde_json::Value = client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(season_pack_torrent_bytes()) }))
        .send()?
        .error_for_status()?
        .json()?;
    let info_hash = created["infoHash"].as_str().expect("infoHash").to_string();
    let api = handle.file_stats(&info_hash, 1, &[])?;
    let http: serde_json::Value = client
        .get(format!("{base}/{info_hash}/1/stats.json"))
        .send()?
        .error_for_status()?
        .json()?;
    // The hash check may finish between the two calls, so compare the
    // fields that do not depend on timing.
    let api_json = serde_json::to_value(&api)?;
    for key in ["infoHash", "streamName", "streamLen", "files", "sources"] {
        assert_eq!(api_json[key], http[key], "{key}");
    }
    assert_eq!(api.stream_name, "Show.S01E02.1080p.mkv");
    let api = handle.engine_stats(&info_hash, &[])?;
    assert_eq!(api.stream_name, "Show.S01E01.1080p.mkv");

    let missing = handle.file_stats(&info_hash, 99, &[]);
    let err = missing.expect_err("index 99 does not exist");
    assert!(
        err.downcast_ref::<stream_server::FileNotFound>().is_some(),
        "{err:#}"
    );
    let response = client
        .get(format!("{base}/{info_hash}/99/stats.json"))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

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

    let response = bearer_client(&handle)?
        .get(format!("http://{}/heartbeat", handle.http_addr()))
        .send()?
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
    let client = bearer_client(&handle)?;

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
/// still resolving sees that very add, with the create's trackers.
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
    let client = bearer_client_builder(&handle)
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
    let created = bearer_client_builder(&handle)
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
