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

/// The base config every test here spreads from: `ServerConfig::embedded()`
/// with DHT bootstrap name resolution turned off, so starting a server makes
/// no DNS query and no DNS-over-HTTPS request. The stock configs leave it on
/// (that is asserted below); tests must stay offline, and on a runner with
/// no DNS at all the resolution ladder would otherwise spend its whole
/// budget failing, once per server.
fn offline_config() -> ServerConfig {
    ServerConfig {
        resolve_dht_bootstrap_names: false,
        // An embedder that keeps a pin record and has nothing in it yet.
        // `None` is not the same thing -- it is "nobody said", which keeps
        // every torrent's data and reports it all as pinned -- and it has a
        // test of its own; spreading it here would turn every retention and
        // idle-pause test in the file into one about a cache that may not be
        // touched.
        pins: Some(Default::default()),
        ..ServerConfig::default()
    }
}

/// A build without the `tui` feature has no terminal UI to start, and says
/// so instead of starting without one.
#[cfg(not(feature = "tui"))]
#[test]
fn a_build_without_the_tui_refuses_a_config_that_asks_for_it() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = ServerConfig {
        use_tui: true,
        config_dir: Some(dir.path().to_path_buf()),
        ..offline_config()
    };
    let error = stream_server::start(cfg)
        .err()
        .expect("the start is refused");
    assert!(error.to_string().contains("tui"), "{error}");
}

/// Resolving bootstrap names is on for both shipped configurations -- the
/// Android embed, which uses `embedded()`, is the case it exists for.
#[test]
fn stock_configs_resolve_dht_bootstrap_names() {
    assert!(ServerConfig::embedded().resolve_dht_bootstrap_names);
    assert!(ServerConfig::binary_default().resolve_dht_bootstrap_names);
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
                ..offline_config()
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
/// (legacy clients call it too). A profile synced from an account carries a
/// descriptor for the same addon that also declares an `other`/`local`
/// catalog, so `catalog/{type}/{id}.json` and the extra-args
/// `catalog/{type}/{id}/{extra}.json` shape must answer an empty but valid
/// catalog; `meta` stays a 404, and so does every other resource under the
/// prefix.
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
        ..offline_config()
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
    // The catalog resource: both shapes `AddonHTTPTransport::resource` builds
    // (`{id}.json`, and `{id}/{extra}.json` once paging/filtering kicks in),
    // plus the suffix-less spelling the stream stub also accepts.
    for path in [
        "/local-addon/catalog/other/local.json",
        "/local-addon/catalog/other/local",
        "/local-addon/catalog/other/local/skip=100.json",
        "/local-addon/catalog/other/local/genre=Action&skip=100.json",
        "/local-addon/catalog/other/local/search=the%20matrix.json",
    ] {
        let response = anonymous.get(format!("{base}{path}")).send()?;
        assert_eq!(response.status(), reqwest::StatusCode::OK, "{path}");
        let body: serde_json::Value = response.json()?;
        assert_eq!(body, serde_json::json!({ "metas": [] }), "{path}");
    }

    let response = anonymous
        .get(format!("{base}/local-addon/meta/movie/local:abc.json"))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    // Anything else under the prefix is a deliberate 404 from the stub's own
    // fallback -- never the ERROR-level unhandled-request fallback.
    for path in [
        "/local-addon/subtitles/movie/tt0111161.json",
        "/local-addon/addon_catalog/all/local.json",
        "/local-addon/",
    ] {
        let response = anonymous.get(format!("{base}{path}")).send()?;
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND, "{path}");
    }

    handle.shutdown()?;
    assert_eq!(
        handle.join()?,
        Some(stream_server::ShutdownSource::External)
    );

    Ok(())
}

/// CORS has to name what a Cast receiver asks for.
///
/// A Google Cast receiver plays through a browser media element, so the media
/// request is a CORS request (Google's receiver docs are explicit that even a
/// plain MP4 needs CORS once tracks are involved) and its preflight asks for
/// `Content-Type`, `Accept-Encoding` and `Range`. The `*` wildcard
/// `CorsLayer::permissive()` answered is not a guarantee -- and it never
/// covers `Authorization`, which a browser-hosted client needs for the control
/// API -- so the allow-list is spelled out. Seeking needs `Content-Range`,
/// `Content-Length` and `Accept-Ranges` readable from script, so those are
/// exposed by name -- and `Location` with them, which `/proxy` relays for a
/// `3xx` it will not follow and which is unreadable from script otherwise.
///
/// The CORS layer answers a preflight itself, before routing, so this holds
/// for every path on both listeners.
#[test]
fn cors_names_the_request_and_response_headers_a_cast_receiver_needs() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..offline_config()
    })?;
    let base = format!("http://{}", handle.http_addr());
    let anonymous = reqwest::blocking::Client::new();

    let response = anonymous
        .request(
            reqwest::Method::OPTIONS,
            format!("{base}/0123456789abcdef0123456789abcdef01234567/0"),
        )
        .header(reqwest::header::ORIGIN, "https://example.org")
        .header("access-control-request-method", "GET")
        .header("access-control-request-headers", "range")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        header_value(&response, "access-control-allow-origin"),
        "*",
        "a receiver's origin is opaque"
    );
    let allowed = header_value(&response, "access-control-allow-headers");
    for header in ["accept-encoding", "authorization", "content-type", "range"] {
        assert!(
            allowed.contains(header),
            "{header:?} must be an allowed request header, got {allowed:?}"
        );
    }

    let response = anonymous
        .get(format!("{base}/heartbeat"))
        .header(reqwest::header::ORIGIN, "https://example.org")
        .send()?;
    let exposed = header_value(&response, "access-control-expose-headers");
    for header in [
        "accept-ranges",
        "content-length",
        "content-range",
        "content-type",
        "location",
    ] {
        assert!(
            exposed.contains(header),
            "{header:?} must be exposed to script, got {exposed:?}"
        );
    }

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// One response header, lowercased, or `""` when it is absent.
fn header_value(response: &reqwest::blocking::Response, name: &str) -> String {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase()
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
        ..offline_config()
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

    // The DHT state a client needs to say "DHT unavailable, using trackers
    // only" instead of pretending peer discovery is healthy. Always present,
    // `sys=1` or not: a missing `dht` key means an older server, which is a
    // different thing from a DHT that never came up.
    let response = client
        .get(format!("http://{}/stats.json", handle.http_addr()))
        .send()?
        .error_for_status()?;
    let body: serde_json::Value = response.json()?;
    let dht = &body["dht"];
    assert!(dht["enabled"].is_boolean(), "dht.enabled: {dht}");
    assert!(dht["nodes"].is_u64(), "dht.nodes: {dht}");
    assert!(dht["nodesV6"].is_u64(), "dht.nodesV6: {dht}");
    assert!(
        dht["everBootstrapped"].is_boolean(),
        "dht.everBootstrapped: {dht}"
    );
    // The library API is the same call (`routes::system::dht_status`).
    let status = handle.dht_status();
    assert_eq!(dht["enabled"], serde_json::json!(status.enabled));

    handle.shutdown()?;
    handle.join()?;

    Ok(())
}

/// The signal a client's "working in the background" indicator reads. An
/// idle server is dark, and asking is not itself an event: it starts no
/// torrent, so a light can never be the reason there is something to report.
#[test]
fn background_traffic_is_dark_on_an_idle_server() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..offline_config()
    })?;

    let traffic = handle.background_traffic()?;
    assert!(!traffic.active, "nothing has been asked of this server yet");
    assert!(!traffic.downloading && !traffic.uploading && !traffic.playing);
    assert_eq!(traffic.bytes_downloaded, 0);
    assert_eq!(traffic.bytes_uploaded, 0);
    assert!(traffic.window_secs > 0, "the window has to be a duration");

    // It crosses FFI as JSON like every other type on this boundary, and
    // these are the names the app reads.
    let json = serde_json::to_value(&traffic)?;
    for half in ["active", "downloading", "uploading", "playing"] {
        assert_eq!(json[half], serde_json::json!(false), "{half}");
    }
    assert_eq!(json["bytes_downloaded"], serde_json::json!(0));
    assert_eq!(json["bytes_uploaded"], serde_json::json!(0));

    // And it created nothing on the way: `/stats.json` still knows no torrent
    // (its non-torrent keys are `dht`, and `sys` only when asked for).
    let response = bearer_client(&handle)?
        .get(format!("http://{}/stats.json", handle.http_addr()))
        .send()?
        .error_for_status()?;
    let body: serde_json::Value = response.json()?;
    let torrents: Vec<&String> = body
        .as_object()
        .expect("object")
        .keys()
        .filter(|key| key.len() == 40)
        .collect();
    assert!(torrents.is_empty(), "asking lit an engine: {torrents:?}");

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
        ..offline_config()
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
        ..offline_config()
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

/// A magnet in `POST /create`'s `from` -- a link or a bare info hash --
/// goes through the shared, timed magnet registry like `/{infoHash}/create`
/// and the stream route, never through `EngineFS::add_torrent`: a stats
/// poll arriving while the create still resolves sees that very add, with
/// the link's own `tr=` trackers. (Through `add_torrent` the create would
/// hang unbounded on librqbit's own resolve and the poll would start a
/// second, tracker-less one.)
#[test]
fn create_from_a_magnet_joins_the_shared_registry_add() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..offline_config()
    })?;
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    let impatient = bearer_client_builder(&handle)
        .timeout(std::time::Duration::from_secs(2))
        .build()?;

    // Neither magnet resolves -- no peers, an unreachable tracker -- so each
    // create blocks past the client's patience while its add lives on in
    // the registry, where the stats poll finds it.
    let link_hash = "aabbccddeeff00112233445566778899aabbccdd";
    let link_tracker = "udp://from-link.invalid:6969/announce";
    let bare_hash = "bbccddeeff00112233445566778899aabbccddee";
    let bare_tracker = "udp://from-bare.invalid:6969/announce";
    for (body, hash, tracker) in [
        (
            serde_json::json!({
                "from": format!(
                    "magnet:?xt=urn:btih:{link_hash}&tr={}",
                    urlencoding::encode(link_tracker)
                )
            }),
            link_hash,
            link_tracker,
        ),
        (
            serde_json::json!({
                "from": bare_hash,
                "peerSearch": { "sources": [format!("tracker:{bare_tracker}")] }
            }),
            bare_hash,
            bare_tracker,
        ),
    ] {
        let created = impatient.post(format!("{base}/create")).json(&body).send();
        assert!(
            created.is_err(),
            "create must wait for metadata, got {created:?}"
        );
        let stats: serde_json::Value = client
            .get(format!("{base}/{hash}/stats.json"))
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
            sources.contains(&tracker),
            "the stats poll must find the add /create started, with its trackers; got {sources:?}"
        );
    }

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
        ..offline_config()
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
        ..offline_config()
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
    // Let the hash check finish first: the per-file initial-window fields
    // only exist once it has, and the two calls below must see one state.
    stats_after_check(&client, &base, &info_hash)?;
    let api = handle.file_stats(&info_hash, 1, &[])?;
    let http: serde_json::Value = client
        .get(format!("{base}/{info_hash}/1/stats.json"))
        .send()?
        .error_for_status()?
        .json()?;
    // Compare the fields that do not depend on timing (peer counts move).
    let api_json = serde_json::to_value(&api)?;
    for key in [
        "infoHash",
        "streamName",
        "streamLen",
        "files",
        "sources",
        "pieceLength",
        "inFlightPiece",
    ] {
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

/// `POST /settings` answers with `btSettings`, the report of what the torrent
/// session actually did with the `bt*` values the client sent: `appliedLive`
/// (changed on the running session now), `pendingRestart` (a session-start
/// setting whose value differs from the one the session opened with), and
/// `notHonoured` (every setting the backend has no knob for, always the full
/// list). Without it a 200 would read as "everything applied", which for most
/// of these settings is not true. The library exposes the same report through
/// `ServerHandle::update_settings_with_report`.
#[test]
fn post_settings_reports_which_bt_settings_took_effect() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let handle = stream_server::start(ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..offline_config()
    })?;
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;

    let resp: serde_json::Value = client
        .post(format!("{base}/settings"))
        .json(&serde_json::json!({
            // Live: the two settings the running session can change.
            "btDownloadSpeedHardLimit": 1_000_000.0,
            "btMaxConnections": 400,
            // Session-start: read once when the session opened, so a change
            // waits for the next start.
            "btEnableDht": false,
        }))
        .send()?
        .error_for_status()?
        .json()?;

    assert_eq!(resp["success"], true, "{resp}");
    let report = &resp["btSettings"];
    let names = |key: &str| -> Vec<String> {
        report[key]
            .as_array()
            .unwrap_or_else(|| panic!("{key} is an array in {report}"))
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect()
    };
    let applied_live = names("appliedLive");
    let pending_restart = names("pendingRestart");
    let not_honoured = names("notHonoured");

    // The download limit is applied to the running session.
    assert!(
        applied_live.contains(&"btDownloadSpeedHardLimit".to_string()),
        "{report}"
    );
    // So is the peer limit: it used to be `pendingRestart`, because the cap
    // was a session option, and it is now the same runtime lever the
    // background footprint uses. A client that shows "restart to apply"
    // from this list must stop showing it for this setting.
    assert!(
        applied_live.contains(&"btMaxConnections".to_string()),
        "{report}"
    );
    // btEnableDht is read once at session start, so changing it is pending a
    // restart.
    assert!(
        pending_restart.contains(&"btEnableDht".to_string()),
        "{report}"
    );
    // btEnablePex has no librqbit knob and is always listed as not honoured,
    // whether or not this call changed it.
    assert!(
        not_honoured.contains(&"btEnablePex".to_string()),
        "{report}"
    );
    // The three sets are disjoint: a setting is in exactly one place.
    for name in &applied_live {
        assert!(!pending_restart.contains(name) && !not_honoured.contains(name));
    }

    // The library method returns the same report.
    let (_settings, rep) =
        handle.update_settings_with_report(serde_json::json!({ "btEnablePex": false }))?;
    assert!(rep.not_honoured.contains(&"btEnablePex"), "{rep:?}");
    assert!(
        rep.applied_live.contains(&"btDownloadSpeedHardLimit"),
        "{rep:?}"
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
        ..offline_config()
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
        ..offline_config()
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
    let deadline = std::time::Instant::now() + CHECK_WAIT_BOUND;
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
    // Nobody is seeding this fixture torrent and the embedded session has no
    // peer to connect to, so the honest answer is 0 -- but the field is there.
    assert_eq!(stats["connectedSeeders"], 0, "{stats}");
    // Swarm-wide scrape figures. This fixture torrent has no trackers, so
    // there is nothing to scrape and all three are null -- present in the
    // shape, never a stand-in 0.
    for key in ["swarmSeeders", "swarmLeechers", "swarmScrapeAgeSecs"] {
        assert!(obj.contains_key(key), "{key} missing: {stats}");
        assert_eq!(stats[key], serde_json::Value::Null, "{key}: {stats}");
    }
    assert_eq!(stats["files"][1]["initialWindowBytes"], 700);
    assert_eq!(stats["files"][1]["initialWindowReadyBytes"], 0);
    // Sub-piece progress. Nothing has opened a stream on this torrent, so
    // there is no piece anybody is waiting on: `null` at the top level and
    // the key omitted per file -- absence, never a zeroed piece a client
    // would draw as an empty bar.
    assert!(obj.contains_key("inFlightPiece"), "{stats}");
    assert_eq!(stats["inFlightPiece"], serde_json::Value::Null, "{stats}");
    assert!(
        !stats["files"][1]
            .as_object()
            .unwrap()
            .contains_key("inFlightPiece"),
        "{stats}"
    );

    // Per-file stats focus the requested file.
    let file_stats: serde_json::Value = client
        .get(format!("{base}/{info_hash}/1/stats.json"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(file_stats["streamName"], "Show.S01E02.1080p.mkv");
    assert_eq!(file_stats["phase"], "buffering");
    assert_eq!(file_stats["connectedSeeders"], 0, "{file_stats}");
    assert_eq!(file_stats["initialWindowBytes"], 700, "{file_stats}");
    assert_eq!(file_stats["initialWindowReadyBytes"], 0);
    assert_eq!(
        file_stats["inFlightPiece"],
        serde_json::Value::Null,
        "{file_stats}"
    );

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
        ..offline_config()
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
        // A magnet still resolving has no peers, so 0 -- never a missing field.
        assert_eq!(stats["connectedSeeders"], 0, "{path}: {stats}");
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

/// A real multi-file torrent (correct piece hashes, 16 KiB pieces) built
/// from the files under `dir`, whose name becomes the torrent name --
/// librqbit's `<root>/<name>` folder in the cache root. Returns the
/// metainfo bytes and the info hash.
fn real_torrent(dir: &std::path::Path) -> (Vec<u8>, String) {
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

/// Pre-seed a whole torrent's data where the server actually reads it: the
/// piece store. See [`seed_piece_store_pieces`] for the rules.
fn seed_piece_store(cache_root: &std::path::Path, torrent_bytes: &[u8], content: &std::path::Path) {
    seed_piece_store_files(cache_root, torrent_bytes, content, None);
}

/// Pre-seed the named files of a torrent, whole pieces only. See
/// [`seed_piece_store_pieces`] for the rules; this is that function with no
/// hole in it.
fn seed_piece_store_files(
    cache_root: &std::path::Path,
    torrent_bytes: &[u8],
    content: &std::path::Path,
    only: Option<&[&str]>,
) {
    seed_piece_store_pieces(cache_root, torrent_bytes, content, only, None);
}

/// Pre-seed a torrent's data one file per piece under
/// `<cacheRoot>/rqbit-downloads/.pieces/<info hash>/<bucket>/<piece>`, which
/// is where the session's default storage keeps it.
///
/// These fixtures used to copy whole files into
/// `rqbit-downloads/<torrent name>/` and let librqbit's filesystem storage
/// find them at the initial check. The session's default storage is
/// `PieceStoreFactory` now, so a whole `.mkv` there is bytes nothing reads:
/// the check would find every piece missing and report the torrent empty.
///
/// `content` is the directory `real_torrent` was pointed at, and each file is
/// read back through the path the *metainfo* gives, in the order the metainfo
/// gives -- never the fixture's own write order. `create_torrent` walks with
/// `walkdir` and does not sort, so the torrent's file order is the
/// filesystem's readdir order, and a concatenation built any other way
/// produces pieces whose hashes are wrong on some machines and right on
/// others.
///
/// `only` names the files (by their last path component) to seed, for a
/// fixture that means to leave part of a torrent missing; `None` seeds all of
/// them. A piece is written only when *every* byte of it belongs to a named
/// file, so a fixture whose named files share a boundary piece with an
/// unnamed one leaves that piece out rather than writing a piece whose hash
/// cannot check -- and the assertion below says how many pieces were seeded,
/// so a fixture that meant to seed something and seeded nothing fails here.
///
/// `hole` is the other way to leave something out: a byte range of that same
/// flat layout, every piece overlapping which is skipped. `only` picks whole
/// files; this picks bytes, which is what a fixture needs when the gap has to
/// be *inside* one file -- an archive whose ends are present, so its
/// directory can be read, and whose middle never arrives, so a read of a
/// member parks there for ever. Nothing seeds these fixtures, so a piece left
/// out either way is a piece that never comes.
///
/// **Call this after the server has started**, never before. The launch-time
/// sweep (`enginefs::piece_store::sweep_before_session`) deletes every piece
/// directory the embedder's pin set does not name, and one seeded before the
/// process comes up is exactly that: the server would start, delete it, and
/// the torrent would then check as empty. Which is the sweep working.
fn seed_piece_store_pieces(
    cache_root: &std::path::Path,
    torrent_bytes: &[u8],
    content: &std::path::Path,
    only: Option<&[&str]>,
    hole: Option<std::ops::Range<u64>>,
) {
    let meta = librqbit::torrent_from_bytes(torrent_bytes).expect("parse the torrent back");
    let info_hash = meta.info_hash.as_string();
    let info = meta.info.data.validate().expect("validated metainfo");
    let piece_length = info.lengths().default_piece_length() as u64;

    let mut blob = Vec::new();
    // Byte ranges of the torrent's flat layout that the fixture is seeding.
    let mut seeded: Vec<(u64, u64)> = Vec::new();
    for file in info.iter_file_details() {
        let relative = file.filename.to_pathbuf();
        let path = content.join(&relative);
        let bytes = std::fs::read(&path).unwrap_or_else(|e| {
            panic!("the fixture file {} the torrent names: {e}", path.display())
        });
        let wanted = only.is_none_or(|names| {
            relative
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| names.contains(&n))
        });
        if wanted {
            let from = blob.len() as u64;
            let to = from + bytes.len() as u64;
            // Merged as they are collected: two seeded files that follow one
            // another share a boundary piece, and a piece that straddles two
            // ranges is inside neither of them.
            match seeded.last_mut() {
                Some(last) if last.1 == from => last.1 = to,
                _ => seeded.push((from, to)),
            }
        }
        blob.extend_from_slice(&bytes);
    }
    assert_eq!(
        blob.len() as u64,
        info.lengths().total_length(),
        "the fixture and the torrent disagree about the payload"
    );

    // Where each piece goes is asked of the store, never spelled out here:
    // the bucketed layout is `enginefs::piece_store`'s, and a fixture with a
    // second copy of it would go on seeding happily after it changed.
    let store = piece_store(cache_root);
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
        store.torrent_dir(&info_hash),
        std::sync::Arc::new(layout),
    );
    let mut written = 0usize;
    for (index, piece) in blob.chunks(piece_length as usize).enumerate() {
        let start = index as u64 * piece_length;
        let end = start + piece.len() as u64;
        if !seeded.iter().any(|(from, to)| *from <= start && end <= *to) {
            continue;
        }
        if hole
            .as_ref()
            .is_some_and(|hole| start < hole.end && hole.start < end)
        {
            continue;
        }
        let path = pieces.piece_path(index as u32);
        std::fs::create_dir_all(path.parent().expect("a bucket")).expect("piece bucket");
        std::fs::write(&path, piece).expect("write a piece");
        written += 1;
    }
    assert!(written > 0, "the fixture seeded no piece at all");
}

/// The session's piece store, where all of a torrent's data is -- the
/// streaming cache and an offline download alike.
fn piece_store(cache_root: &std::path::Path) -> enginefs::piece_store::StoreRoot {
    enginefs::piece_store::StoreRoot::in_download_dir(&cache_root.join("rqbit-downloads"))
}

/// How many pieces the store holds for a torrent -- asked of the store, so
/// nothing here has to know how they are laid out.
fn pieces_held(cache_root: &std::path::Path, info_hash: &str) -> usize {
    piece_store(cache_root).stat(info_hash).pieces.len()
}

/// Deterministic, non-trivial payload so piece hashes mean something.
fn write_payload(path: &std::path::Path, len: usize) {
    let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    std::fs::write(path, data).expect("write payload");
}

/// A `TempDir`-derived path in the spelling the server answers with.
///
/// `prepare_torrent_data_root` stores and reports the *resolved* `cacheRoot`,
/// so an expectation built from a `TempDir` path has to be resolved the same
/// way or it compares two spellings of the same directory: on Windows
/// `TempDir` hands back the 8.3 short name (`C:\Users\RUNNER~1\...`) while
/// the server answers with the long one (`C:\Users\runneradmin\...`), and on
/// macOS `/var/...` is a symlink to `/private/var/...`. Both sides go through
/// the server's own `resolved_path` rather than a copy of it.
fn resolved(path: &std::path::Path) -> std::path::PathBuf {
    stream_server::resolved_path(path)
}

/// Index of the file called `name` in `stats.files` (torrent file order is
/// whatever `create_torrent`'s directory walk produced).
fn file_index(stats: &serde_json::Value, name: &str) -> usize {
    stats["files"]
        .as_array()
        .expect("files")
        .iter()
        .position(|f| f["name"] == name)
        .unwrap_or_else(|| panic!("no file {name} in {stats}"))
}

/// The torrent pieces one file of `stats.files` occupies, read off the
/// stats the server itself answers with.
///
/// Only for a fixture whose files are whole numbers of pieces -- which the
/// assertion below says -- because a file that shares a boundary piece with
/// its neighbour has no range of pieces that are only its own.
fn file_pieces(stats: &serde_json::Value, idx: usize, piece_length: u64) -> std::ops::Range<u32> {
    let number = |key: &str| {
        stats["files"][idx][key]
            .as_u64()
            .unwrap_or_else(|| panic!("file {idx} has no {key} in {stats}"))
    };
    let (offset, length) = (number("offset"), number("length"));
    assert_eq!(
        (offset % piece_length, length % piece_length),
        (0, 0),
        "file {idx} does not sit on whole pieces: {stats}"
    );
    (offset / piece_length) as u32..((offset + length) / piece_length) as u32
}

/// How long a hash check (or a metadata resolve that has the metadata
/// already) may take before a test gives up on it. Not a timing
/// assertion -- only there so a regression fails instead of hanging -- so
/// it is far above anything a correct run needs: the whole suite runs its
/// servers in parallel, and a loaded two-core runner is an order of
/// magnitude slower than an idle machine.
const CHECK_WAIT_BOUND: std::time::Duration = std::time::Duration::from_secs(120);

/// Poll `/{infoHash}/stats.json` until the torrent is out of `checking`
/// (bounded), returning the last stats.
fn stats_after_check(
    client: &reqwest::blocking::Client,
    base: &str,
    info_hash: &str,
) -> anyhow::Result<serde_json::Value> {
    poll_stats(client, &format!("{base}/{info_hash}/stats.json"))
}

/// The same for `/{infoHash}/{fileIdx}/stats.json`.
///
/// Torrent-wide `phase` describes the *guessed* stream file, which for a
/// multi-file torrent is whichever file the guess picks out of an order
/// the fixture does not control (see `file_index`). A test that cares
/// about one particular file's readiness must ask about that file.
fn file_stats_after_check(
    client: &reqwest::blocking::Client,
    base: &str,
    info_hash: &str,
    file_idx: usize,
) -> anyhow::Result<serde_json::Value> {
    poll_stats(client, &format!("{base}/{info_hash}/{file_idx}/stats.json"))
}

fn poll_stats(client: &reqwest::blocking::Client, url: &str) -> anyhow::Result<serde_json::Value> {
    let deadline = std::time::Instant::now() + CHECK_WAIT_BOUND;
    loop {
        let stats: serde_json::Value = client.get(url).send()?.error_for_status()?.json()?;
        match stats["phase"].as_str() {
            Some("checking") | Some("resolvingMetadata") => {
                // Say so rather than handing back `checking` stats the
                // caller will then assert on: every caller needs the check
                // to be over, so an expired bound is the failure, and a
                // mystery `complete: false` three lines later is not the
                // way to report it.
                anyhow::ensure!(
                    std::time::Instant::now() < deadline,
                    "{url} was still {} after {CHECK_WAIT_BOUND:?}: {stats}",
                    stats["phase"]
                );
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            _ => return Ok(stats),
        }
    }
}

/// `ServerHandle::set_background` is app-lifecycle wiring, and this is
/// where the wiring is pinned: the call reaches the torrent session, the
/// peer cap on the torrent moves with it both ways, nothing is paused, and
/// a stream request that arrives while lean is still served.
///
/// No swarm, no network, no peer at all. The footprint is read back through
/// the engine (`is_background` asks `EngineFS`, which asks the backend, so
/// a `set_background` that only flipped a flag on the handle fails here),
/// the cap is read off the very torrent the server holds
/// (`torrent_peer_limit`, the same librqbit `peer_limit` the enginefs tests
/// read), and the bytes come from a torrent whose data is already on disk.
///
/// Deliberately not here: what the cap does to individual peers -- hanging
/// up on the surplus but *parking* it rather than forgetting it, and
/// re-dialling the parked ones on the way back. That needs a swarm and is
/// enginefs's `lean_parks_the_surplus_and_full_re_dials_it`, which dials
/// out to seeders whose addresses it was handed instead of waiting to be
/// dialled. This test used to grow a swarm of its own -- a dozen loopback
/// seeders dialling *in*, because the server cannot be told peer addresses
/// over its API -- and on the Windows CI runner not one of them ever
/// connected (`peers=0`), so its precondition, more live peers than the
/// cap, could not be met there at all. The cap's arithmetic
/// (`LEAN_PEER_LIMIT` on every torrent the session holds and on the next
/// one added, the *configured* limit back on `Full`) is enginefs's
/// `footprint_caps_every_torrent_and_the_next_one_added`, which needs no
/// peers either.
#[test]
fn set_background_caps_the_torrent_and_still_streams() -> anyhow::Result<()> {
    const LEAN: usize = enginefs::backend::LEAN_PEER_LIMIT;

    // Before any torrent exists: safe, idempotent, and only the footprint
    // moves. Its own server, because the fixture below hands one back with
    // its torrent already created -- and stopped at the end with that one
    // rather than here, because a server stopped within milliseconds of
    // starting trips the tracker-refresher shutdown race
    // `TrackerManager::take_refresh_task` documents: noise on a worker
    // thread, but noise a real panic could hide in.
    let bare_config = tempfile::tempdir()?;
    let bare_cache = tempfile::tempdir()?;
    let bare = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(bare_config.path().join("config")),
        cache_dir: Some(bare_cache.path().join("cache")),
        ..offline_config()
    })?;
    assert!(!bare.is_background());
    bare.set_background(true);
    bare.set_background(true);
    assert!(bare.is_background());
    bare.set_background(false);
    assert!(!bare.is_background());

    // A server holding one torrent whose data is already on disk and
    // hash-checked, so every byte below is served without a peer.
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let (handle, base, info_hash, idx, payload) =
        lan_media_server(config_dir.path(), cache_dir.path(), src.path(), None)?;
    let client = bearer_client(&handle)?;

    // The cap the torrent carries in the foreground is the session's
    // configured one, which every real profile puts well above the lean
    // limit -- otherwise going lean would not be a shrink and the rest of
    // this test would pass on a torrent nothing ever capped.
    let configured = handle
        .torrent_peer_limit(&info_hash)
        .expect("the server holds the torrent it was just given");
    assert!(
        configured > LEAN,
        "the configured cap ({configured}) must be above the lean one ({LEAN})"
    );

    // Background: the footprint reaches the engine and the cap on the
    // torrent moves with it. Both are read straight after the call rather
    // than waited for -- it is synchronous down to librqbit.
    handle.set_background(true);
    assert!(handle.is_background());
    assert_eq!(handle.torrent_peer_limit(&info_hash), Some(LEAN));

    // And nothing was paused or dropped on the way: the torrent is still
    // held, still complete, still ready to play.
    let lean = file_stats_after_check(&client, &base, &info_hash, idx)?;
    assert_eq!(lean["phase"], "ready", "{lean}");
    assert_eq!(lean["files"][idx]["complete"], true, "{lean}");

    // A stream request arriving while lean is served like any other.
    let anonymous = reqwest::blocking::Client::new();
    let response = anonymous
        .get(format!("{base}/{info_hash}/{idx}"))
        .header(reqwest::header::RANGE, "bytes=0-15")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.bytes()?.as_ref(), &payload[0..16]);
    assert!(
        handle.is_background(),
        "serving a stream does not end the background"
    );
    assert_eq!(
        handle.torrent_peer_limit(&info_hash),
        Some(LEAN),
        "nor lift the cap"
    );

    // Foreground: the configured cap comes back -- the value the session
    // opened with, so a `Full` that restored the backend's own default
    // would be caught -- and the next range is served the same way.
    handle.set_background(false);
    assert!(!handle.is_background());
    assert_eq!(handle.torrent_peer_limit(&info_hash), Some(configured));
    let response = anonymous
        .get(format!("{base}/{info_hash}/{idx}"))
        .header(reqwest::header::RANGE, "bytes=16-31")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.bytes()?.as_ref(), &payload[16..32]);

    handle.shutdown()?;
    handle.join()?;
    bare.shutdown()?;
    bare.join()?;
    Ok(())
}

/// `cacheRoot` is the one torrent-data root, and the only thing that decides
/// where a torrent's bytes go.
///
/// A librqbit session's storage is fixed when the session opens, so a
/// What the archive routes write under `<cacheRoot>/.archives` is unlinked
/// when the session holding it drops, and a killed process drops nothing.
/// Session keys are per process, so nothing a previous run left there can
/// be asked for again -- and the launch sweeps it, before the router opens.
#[test]
fn a_killed_process_s_archive_scratch_is_gone_at_the_next_start() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let cache_root = cache_dir.path().join("cache");
    // The name is the on-disk contract (`archives::SCRATCH_DIR_NAME`), and
    // a test of what is on disk spells it.
    let scratch = cache_root.join(".archives");
    std::fs::create_dir_all(&scratch)?;
    let leftover = scratch.join("archive_abc123.zip");
    write_payload(&leftover, 4 * 1024);
    // Beside it, what the sweep must leave alone: the root itself and a
    // neighbour that is not the archive layer's.
    let neighbour = cache_root.join("keep.txt");
    std::fs::write(&neighbour, b"not the archive layer's")?;

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    })?;
    assert!(
        !leftover.exists(),
        "the leftover archive is swept at launch"
    );
    assert!(
        !scratch.exists(),
        "the directory goes with it; the next play recreates it"
    );
    assert!(neighbour.is_file(), "and nothing beside it is touched");
    handle.shutdown()?;
    Ok(())
}

/// `cacheRoot` set through `POST /settings` is where the data lives *from the
/// next start*: the running server keeps writing where it opened. At that
/// next start the setting is prepared before anything opens on it, and one
/// that cannot be used any more (its path is a file now) falls back to the
/// configured cache directory -- in the settings file too, so an embedder
/// reading the file sees what `settings()` says and the next boot does not
/// warn about it again.
#[test]
fn the_cache_root_setting_decides_where_the_next_session_opens() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let elsewhere = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let default_root = resolved(&cache_dir.path().join("cache"));

    let content = src.path().join("Show");
    std::fs::create_dir_all(&content)?;
    write_payload(&content.join("e1.bin"), 32 * 1024);
    let (torrent, info_hash) = real_torrent(&content);
    let config = || stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..offline_config()
    };
    let settings_file = config_dir.path().join("config").join("settings.json");
    let read_persisted = || -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::from_str(&std::fs::read_to_string(
            &settings_file,
        )?)?)
    };
    // Where a session keeps its own records for the torrents it holds, and
    // so the mark that a session opened on a root and put a torrent there.
    let opened_on = |root: &std::path::Path| root.join("rqbit-downloads").join("session.json");
    // The torrent that makes it write them.
    let add_a_torrent = |handle: &stream_server::ServerHandle| -> anyhow::Result<()> {
        let base = format!("http://{}", handle.http_addr());
        let created: serde_json::Value = bearer_client(handle)?
            .post(format!("{base}/create"))
            .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
            .send()?
            .error_for_status()?
            .json()?;
        assert_eq!(created["infoHash"], info_hash);
        Ok(())
    };

    let handle = stream_server::start(config())?;
    add_a_torrent(&handle)?;
    assert_eq!(
        handle.settings()?.cache_root,
        default_root.to_str().unwrap(),
        "with nothing configured the root is the cache directory the embedder gave"
    );
    assert!(opened_on(&default_root).is_file());

    // Validated like any path setting, and a refusal fails the whole update.
    assert!(
        handle
            .update_settings(serde_json::json!({ "cacheRoot": "relative/path" }))
            .is_err(),
        "a relative cacheRoot is refused"
    );
    assert!(
        handle
            .update_settings(serde_json::json!({ "cacheRoot": 123 }))
            .is_err(),
        "a cacheRoot that is not a string is refused, not ignored"
    );
    assert_eq!(
        handle.settings()?.cache_root,
        default_root.to_str().unwrap(),
        "and a refused update changes nothing"
    );

    let moved = resolved(&elsewhere.path().join("torrent-data"));
    let updated = handle.update_settings(serde_json::json!({
        "cacheRoot": moved.to_str().unwrap()
    }))?;
    assert_eq!(updated.cache_root, moved.to_str().unwrap());
    assert!(moved.is_dir(), "created on the spot");
    assert_eq!(read_persisted()?["cacheRoot"], moved.to_str().unwrap());
    assert!(
        !opened_on(&moved).exists(),
        "the running session cannot be moved onto it"
    );

    // And the cache figure goes on describing the root the session opened
    // on, not the one the setting now names: it takes the root from the
    // engine. Read from the setting instead, this would answer about a
    // directory nothing has ever written to and report a cache of nothing
    // while the disk fills up.
    let stray = default_root
        .join("rqbit-downloads")
        .join(".pieces")
        .join("ffffffffffffffffffffffffffffffffffffffff")
        .join("00")
        .join("0");
    std::fs::create_dir_all(stray.parent().unwrap())?;
    write_payload(&stray, 16 * 1024);
    assert!(
        handle.cache_usage()?.total_bytes >= 16 * 1024,
        "the piece directory under the live root is in the figure: {:?}",
        handle.cache_usage()?
    );
    handle.shutdown()?;
    handle.join()?;

    // The next start opens there instead.
    let handle = stream_server::start(config())?;
    assert_eq!(handle.settings()?.cache_root, moved.to_str().unwrap());
    add_a_torrent(&handle)?;
    assert!(opened_on(&moved).is_file(), "and this one did");
    handle.shutdown()?;
    handle.join()?;

    // The directory is a file now: unusable, so the configured default is
    // what the session opens on and what the setting says afterwards.
    std::fs::remove_dir_all(&moved)?;
    std::fs::write(&moved, b"in the way")?;
    let handle = stream_server::start(config())?;
    assert_eq!(
        handle.settings()?.cache_root,
        default_root.to_str().unwrap()
    );
    assert_eq!(
        read_persisted()?["cacheRoot"],
        default_root.to_str().unwrap(),
        "corrected in the settings file too"
    );
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **A pin moves nothing.** A torrent that was streamed first is pinned
/// exactly where it is: `pin_download` reports the same path the backend
/// reported before, every piece it had it still has, and no second copy
/// appears anywhere -- there is one root and a pin is not a location. A
/// restart on the same dirs, with the embedder handing the pin back in,
/// restores the torrent with its pin and its pieces.
#[test]
fn a_pin_moves_nothing_and_survives_a_restart() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;

    let content = src.path().join("Show Season 1");
    std::fs::create_dir_all(&content)?;
    write_payload(&content.join("e1.bin"), 40 * 1024);
    write_payload(&content.join("e2.bin"), 24 * 1024);
    let (torrent, info_hash) = real_torrent(&content);

    let cache_root = resolved(&cache_dir.path().join("cache"));
    let root_folder = cache_root.join("rqbit-downloads").join("Show Season 1");

    let config =
        |pins: std::collections::BTreeMap<String, Vec<usize>>| stream_server::ServerConfig {
            http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            config_dir: Some(config_dir.path().join("config")),
            cache_dir: Some(cache_root.clone()),
            pins: Some(pins),
            ..offline_config()
        };
    let handle = stream_server::start(config(Default::default()))?;
    // "Streamed before": the data already sits in the piece store, which is
    // where a torrent added without a placement puts it -- and where one
    // *with* a placement puts it too.
    seed_piece_store(&cache_root, &torrent, &content);
    let seeded_pieces = pieces_held(&cache_root, &info_hash);
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;

    let created: serde_json::Value = client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(created["infoHash"], info_hash);
    let stats = stats_after_check(&client, &base, &info_hash)?;
    assert_eq!(stats["files"][0]["complete"], true, "{stats}");
    assert_eq!(stats["files"][1]["complete"], true, "{stats}");
    assert_eq!(stats["pinnedFiles"], serde_json::json!([]));
    let idx = file_index(&stats, "e2.bin");

    // Pin: nothing moves, and the name the backend gives the file is the
    // one it gave it before.
    let before = handle.download_path(&info_hash, idx)?;
    assert_eq!(
        before.as_deref(),
        Some(root_folder.join("e2.bin").to_str().unwrap()),
        "librqbit's own folder, which nothing above it chooses"
    );
    let info = handle.pin_download(&info_hash.to_uppercase(), idx, &[])?;
    assert_eq!(info.info_hash, info_hash);
    assert_eq!(info.file_idx, idx);
    assert_eq!(info.name, "e2.bin");
    assert_eq!(info.length, 24 * 1024);
    assert_eq!(info.path, before, "the pin did not move the torrent");
    // A whole file is produced nowhere at all: the torrent's bytes are
    // piece files under the store's one root.
    assert!(!root_folder.join("e2.bin").exists(), "no whole file");
    assert_eq!(
        pieces_held(&cache_root, &info_hash),
        seeded_pieces,
        "the pin kept every piece it had"
    );
    let stats = stats_after_check(&client, &base, &info_hash)?;
    assert_eq!(
        stats["files"][idx]["complete"], true,
        "and the data it had is still complete: {stats}"
    );
    assert_eq!(stats["files"][idx]["pinned"], true);
    assert_eq!(stats["pinnedFiles"], serde_json::json!([idx]));
    assert_eq!(stats["checkedBytes"], serde_json::Value::Null);
    let file_stats = handle.file_stats(&info_hash, idx, &[])?;
    assert!(file_stats.files[idx].complete && file_stats.files[idx].pinned);
    let again = handle.pin_download(&info_hash, idx, &[])?;
    assert!(again.complete, "idempotent: {again:?}");
    let missing = handle.pin_download(&info_hash, 9, &[]);
    assert!(
        missing
            .as_ref()
            .is_err_and(|e| e.to_string().contains("out of range")),
        "{missing:?}"
    );

    handle.shutdown()?;
    handle.join()?;

    // Restart on the same dirs: librqbit restores the torrent where it
    // always was (its persisted output folder / only_files) and the pin
    // comes back because the embedder names it again.
    let handle = stream_server::start(config(std::collections::BTreeMap::from([(
        info_hash.clone(),
        vec![idx],
    )])))?;
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    let stats = handle.engine_stats(&info_hash, &[])?;
    assert_ne!(
        stats.phase,
        stream_server::EngineStats::resolving_metadata(&info_hash, &[]).phase,
        "restored from the session, not re-added"
    );
    assert_eq!(stats.pinned_files, vec![idx], "pin restored");
    let stats = stats_after_check(&client, &base, &info_hash)?;
    assert_eq!(stats["files"][idx]["complete"], true, "{stats}");
    assert_eq!(stats["files"][idx]["pinned"], true, "{stats}");
    assert_eq!(stats["pinnedFiles"], serde_json::json!([idx]));
    let info = handle.pin_download(&info_hash, idx, &[])?;
    assert_eq!(info.path, before, "still where it always was");
    assert!(info.complete);
    assert_eq!(
        pieces_held(&cache_root, &info_hash),
        seeded_pieces,
        "the restart found the same pieces"
    );

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **An embedder that names no pin set takes nothing.**
///
/// The launch sweep deletes every piece directory the set does not name, so
/// a boot told nothing is one keystroke away from deleting every offline
/// download the user has. It does not: nothing is swept, the disk keeps what
/// it held, and `GET /downloads.json` lists every file of every restored
/// torrent, because that is what is being kept -- reporting the empty
/// in-memory pin set instead would tell the caller their downloads are gone
/// while the bytes are still on the disk.
///
/// There is no way out of it within the process: the embedder holds the
/// record, and a boot it said nothing to learns nothing later.
#[test]
fn an_unnamed_pin_set_keeps_every_download_and_lists_it() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;

    let content = src.path().join("Show Season 1");
    std::fs::create_dir_all(&content)?;
    write_payload(&content.join("e1.bin"), 40 * 1024);
    write_payload(&content.join("e2.bin"), 24 * 1024);
    let (torrent, info_hash) = real_torrent(&content);

    let cache_root = resolved(&cache_dir.path().join("cache"));
    let config = |pins: Option<std::collections::BTreeMap<String, Vec<usize>>>| {
        stream_server::ServerConfig {
            http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            config_dir: Some(config_dir.path().join("config")),
            cache_dir: Some(cache_root.clone()),
            pins,
            ..offline_config()
        }
    };

    let handle = stream_server::start(config(Some(Default::default())))?;
    seed_piece_store(&cache_root, &torrent, &content);
    let seeded_pieces = pieces_held(&cache_root, &info_hash);
    assert!(seeded_pieces > 0, "the fixture seeded something");
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?;
    let stats = stats_after_check(&client, &base, &info_hash)?;
    let pinned_idx = file_index(&stats, "e2.bin");
    handle.pin_download(&info_hash, pinned_idx, &[])?;
    handle.shutdown()?;
    handle.join()?;

    // What an embedder whose own record would not read hands in: nothing.
    let handle = stream_server::start(config(None))?;
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    assert_eq!(
        pieces_held(&cache_root, &info_hash),
        seeded_pieces,
        "nothing was swept on a boot nobody named a pin set to"
    );

    let listed: Vec<serde_json::Value> = client
        .get(format!("{base}/downloads.json"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(
        listed.len(),
        2,
        "every file of the restored torrent: {listed:?}"
    );
    for entry in &listed {
        assert_eq!(entry["infoHash"], info_hash);
    }

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// The `.bitv` bitfields librqbit's persistent BitV factory writes next to
/// the session state (`<cacheRoot>/rqbit-downloads/<infoHash>.bitv`).
fn bitv_files(session_dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files: Vec<_> = std::fs::read_dir(session_dir)
        .expect("session dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "bitv"))
        .collect();
    files.sort();
    files
}

/// With fastresume on, librqbit persists each torrent's verified-piece
/// bitfield (`<infoHash>.bitv` beside the session state) instead of
/// re-hashing every file on every launch: after two complete torrents,
/// both bitfields exist, and a restart brings both back ready and complete
/// with the bitfields still in place (the `.bitv` + `overwrite: true`
/// combination librqbit needs to resume on top of existing files). One
/// root, because a pin is not a place: one torrent is pinned by the file
/// the user asked for, the other whole, and neither moves a byte.
///
/// Both are pinned, and the second one is pinned *for the fixture*: this
/// test is about fastresume and not about retention, and a torrent nobody
/// is playing and nobody has pinned is one the owner empties within a tick
/// -- an emptied torrent is not a complete one, and a bitfield read back
/// against no data says nothing about fastresume. What is being kept
/// apart here is the pin's shape, not the pin: pinned per file, and pinned
/// whole.
#[test]
fn fastresume_persists_piece_bitfields_for_a_pinned_torrent_too() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;

    let streamed = src.path().join("Streamed");
    std::fs::create_dir_all(&streamed)?;
    write_payload(&streamed.join("s1.bin"), 48 * 1024);
    write_payload(&streamed.join("s2.bin"), 20 * 1024);
    let (streamed_torrent, streamed_hash) = real_torrent(&streamed);
    let pinned = src.path().join("Pinned");
    std::fs::create_dir_all(&pinned)?;
    // Whole pieces per file (16 KiB pieces): only p2 gets data, and it
    // must not share a boundary piece with p1, whatever the file order.
    write_payload(&pinned.join("p1.bin"), 32 * 1024);
    write_payload(&pinned.join("p2.bin"), 48 * 1024);
    let (pinned_torrent, pinned_hash) = real_torrent(&pinned);

    let cache_root = resolved(&cache_dir.path().join("cache"));
    let session_dir = cache_root.join("rqbit-downloads");

    let config =
        |pins: std::collections::BTreeMap<String, Vec<usize>>| stream_server::ServerConfig {
            http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            config_dir: Some(config_dir.path().join("config")),
            cache_dir: Some(cache_root.clone()),
            pins: Some(pins),
            ..offline_config()
        };
    let handle = stream_server::start(config(Default::default()))?;
    // Both torrents' data is in the one piece store, seeded after the launch
    // sweep: the streamed torrent whole, the pinned one only where the pin
    // will be. There is no second store anywhere -- the store takes one
    // root, and it is the cache root.
    seed_piece_store(&cache_root, &streamed_torrent, &streamed);
    seed_piece_store_files(&cache_root, &pinned_torrent, &pinned, Some(&["p2.bin"]));
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;

    client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&streamed_torrent) }))
        .send()?
        .error_for_status()?;
    let stats = stats_after_check(&client, &base, &streamed_hash)?;
    assert_eq!(stats["files"][0]["complete"], true, "{stats}");
    assert_eq!(stats["files"][1]["complete"], true, "{stats}");
    // Every file of it, so the whole torrent is kept: see the note above.
    for idx in 0..2 {
        handle.pin_download(&streamed_hash, idx, &[])?;
    }

    // A pin by hash needs the metadata: /create supplies it. Nothing about
    // the pin is a location -- the torrent stays where librqbit has it and
    // its data stays where the store has it.
    client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&pinned_torrent) }))
        .send()?
        .error_for_status()?;
    let stats = stats_after_check(&client, &base, &pinned_hash)?;
    let p2 = file_index(&stats, "p2.bin");
    let info = handle.pin_download(&pinned_hash, p2, &[])?;
    assert_eq!(
        info.path.as_deref(),
        Some(session_dir.join("Pinned").join("p2.bin").to_str().unwrap()),
        "librqbit's own folder for the torrent, pinned or not"
    );
    let stats = stats_after_check(&client, &base, &pinned_hash)?;
    assert_eq!(stats["files"][p2]["complete"], true, "{stats}");
    assert_eq!(stats["files"][1 - p2]["complete"], false, "{stats}");

    let bitvs = bitv_files(&session_dir);
    assert_eq!(
        bitvs.len(),
        2,
        "one persisted bitfield per torrent (fastresume): {bitvs:?}"
    );
    for bitv in &bitvs {
        let name = bitv.file_stem().unwrap().to_string_lossy().to_lowercase();
        assert!(name == streamed_hash || name == pinned_hash, "{bitv:?}");
        assert!(std::fs::metadata(bitv)?.len() > 0);
    }

    handle.shutdown()?;
    handle.join()?;

    // Restart: both torrents come back from the session with their
    // bitfields, ready and complete. The embedder names the pins it kept,
    // so the launch sweep leaves both torrents' pieces where they are.
    let handle = stream_server::start(config(std::collections::BTreeMap::from([
        (streamed_hash.clone(), vec![0, 1]),
        (pinned_hash.clone(), vec![p2]),
    ])))?;
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    let stats = stats_after_check(&client, &base, &streamed_hash)?;
    assert_eq!(stats["phase"], "ready", "{stats}");
    assert_eq!(stats["files"][0]["complete"], true, "{stats}");
    assert_eq!(stats["files"][1]["complete"], true, "{stats}");
    // The pinned file's own stats, not the torrent's: only the pinned file
    // was ever seeded, and torrent-wide `phase` describes whichever file
    // the stream guess picks -- the other one, in half the file orders
    // `create_torrent`'s directory walk can produce.
    let stats = file_stats_after_check(&client, &base, &pinned_hash, p2)?;
    assert_eq!(stats["phase"], "ready", "{stats}");
    assert_eq!(stats["files"][p2]["complete"], true, "{stats}");
    assert_eq!(stats["pinnedFiles"], serde_json::json!([p2]));
    assert_eq!(
        bitv_files(&session_dir).len(),
        2,
        "bitfields survive the restart"
    );
    // No whole file anywhere; both torrents' bytes are pieces in the one
    // store, and the restart read the bitfields against those.
    assert!(!session_dir.join("Pinned").join("p2.bin").exists());
    assert!(!session_dir.join("Streamed").exists());
    assert!(pieces_held(&cache_root, &streamed_hash) > 0);
    assert!(pieces_held(&cache_root, &pinned_hash) > 0);

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// The download control routes and the `ServerHandle` methods behind them
/// are one implementation: `POST /{infoHash}/{fileIdx}/download` (optional
/// `{"trackers":[..]}` body), `DELETE` of the same path (`?deleteFiles=1`)
/// and `GET /downloads.json` answer exactly what `pin_download`,
/// `unpin_download`, `downloads` and `download_path` return. They are
/// control routes, so they need the bearer token.
#[test]
fn download_routes_match_the_library_api() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;

    let content = src.path().join("Show Season 2");
    std::fs::create_dir_all(&content)?;
    write_payload(&content.join("e1.bin"), 40 * 1024);
    write_payload(&content.join("e2.bin"), 24 * 1024);
    let (torrent, info_hash) = real_torrent(&content);

    let cache_root = resolved(&cache_dir.path().join("cache"));
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    })?;
    // The data is already in the piece store, as after streaming it -- which
    // is the only place a torrent's bytes are now, downloads included.
    seed_piece_store(&cache_root, &torrent, &content);
    let seeded_pieces = pieces_held(&cache_root, &info_hash);
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;

    let created: serde_json::Value = client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(created["infoHash"], info_hash);
    let stats = stats_after_check(&client, &base, &info_hash)?;
    let first = file_index(&stats, "e1.bin");
    let second = file_index(&stats, "e2.bin");

    // Where librqbit says the torrent's files are, which the pin does not
    // change: `<cacheRoot>/rqbit-downloads/<torrent name>`.
    let named = cache_root.join("rqbit-downloads").join("Show Season 2");

    // The routes are token-protected, like every other control route.
    let anonymous = reqwest::blocking::Client::new();
    for response in [
        anonymous
            .post(format!("{base}/{info_hash}/{first}/download"))
            .json(&serde_json::json!({ "trackers": [] }))
            .send()?,
        anonymous
            .delete(format!("{base}/{info_hash}/{first}/download"))
            .send()?,
        anonymous.get(format!("{base}/downloads.json")).send()?,
    ] {
        assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    }
    assert!(
        handle.downloads()?.is_empty(),
        "nothing pinned by an unauthorized call"
    );

    // POST == pin_download: the file is pinned where it already is, and the
    // answer is a DownloadInfo.
    let pinned: serde_json::Value = client
        .post(format!("{base}/{info_hash}/{first}/download"))
        .json(&serde_json::json!({ "trackers": ["udp://pin.invalid:6969/announce"] }))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(pinned["infoHash"], info_hash);
    assert_eq!(pinned["fileIdx"], first);
    assert_eq!(pinned["name"], "e1.bin");
    assert_eq!(pinned["length"], 40 * 1024);
    assert_eq!(pinned["error"], serde_json::Value::Null);
    assert_eq!(
        pinned["path"],
        named.join("e1.bin").to_str().unwrap(),
        "{pinned}"
    );
    // `path` is where the file *would* be, and no longer where any byte is:
    // a pinned download is piece files like everything else, and the folder
    // in that name holds none of them.
    assert!(!named.join("e1.bin").exists(), "no whole file is produced");
    assert_eq!(
        pieces_held(&cache_root, &info_hash),
        seeded_pieces,
        "the pin did not move, lose or duplicate the data"
    );

    // An empty body is a pin with no extra trackers, not a 400.
    let again = client
        .post(format!("{base}/{info_hash}/{first}/download"))
        .send()?;
    assert!(again.status().is_success(), "{:?}", again.status());
    let again: serde_json::Value = again.json()?;
    for key in ["infoHash", "fileIdx", "name", "length", "path", "error"] {
        assert_eq!(again[key], pinned[key], "{key}");
    }

    // The library pins the second file; both are listed, by HTTP and API
    // alike.
    let api = handle.pin_download(&info_hash, second, &[])?;
    assert_eq!(api.name, "e2.bin");
    stats_after_check(&client, &base, &info_hash)?;
    let listed: serde_json::Value = client
        .get(format!("{base}/downloads.json"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(listed, serde_json::to_value(handle.downloads()?)?);
    let indices: Vec<u64> = listed
        .as_array()
        .expect("array")
        .iter()
        .map(|item| item["fileIdx"].as_u64().expect("fileIdx"))
        .collect();
    assert_eq!(indices.len(), 2, "{listed}");
    assert!(indices.contains(&(first as u64)) && indices.contains(&(second as u64)));
    assert!(
        listed
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["infoHash"] == info_hash.as_str() && item["complete"] == true),
        "{listed}"
    );

    // download_path is the same path the listing reports.
    assert_eq!(
        handle.download_path(&info_hash, second)?.as_deref(),
        Some(named.join("e2.bin").to_str().unwrap())
    );
    assert_eq!(handle.download_path(&info_hash, 99)?, None);
    assert_eq!(handle.download_path(&"a".repeat(40), 0)?, None);

    // A file the torrent does not have is a 404 on both sides.
    let missing = client
        .post(format!("{base}/{info_hash}/9/download"))
        .send()?;
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    let body: serde_json::Value = missing.json()?;
    assert!(
        body["error"].as_str().unwrap_or_default().contains("range"),
        "{body}"
    );
    assert_eq!(
        client
            .post(format!("{base}/{info_hash}/nope/download"))
            .send()?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    assert!(handle.pin_download(&info_hash, 9, &[]).is_err());

    // And a *destructive* DELETE for an index the torrent does not have is
    // the same 404, not a request to delete every file of it.
    let missing = client
        .delete(format!("{base}/{info_hash}/9/download?deleteFiles=1"))
        .send()?;
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    let body: serde_json::Value = missing.json()?;
    assert!(
        body["error"].as_str().unwrap_or_default().contains("range"),
        "{body}"
    );
    assert!(handle.unpin_download(&info_hash, 9, true).is_err());
    assert_eq!(
        pieces_held(&cache_root, &info_hash),
        seeded_pieces,
        "no byte of the torrent is touched"
    );
    assert_eq!(handle.downloads()?.len(), 2, "and both pins stand");

    // DELETE without deleteFiles: the pin goes, the data stays.
    let removed: serde_json::Value = client
        .delete(format!("{base}/{info_hash}/{first}/download"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(
        removed,
        serde_json::json!({
            "infoHash": info_hash,
            "fileIdx": first,
            "unpinned": true,
            "deletedFiles": false,
        })
    );
    assert_eq!(
        pieces_held(&cache_root, &info_hash),
        seeded_pieces,
        "the bytes stay"
    );
    let listed: serde_json::Value = client
        .get(format!("{base}/downloads.json"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(listed, serde_json::to_value(handle.downloads()?)?);
    assert_eq!(listed.as_array().expect("array").len(), 1, "{listed}");
    assert_eq!(listed[0]["fileIdx"], second);
    // Nothing to unpin twice, over either surface.
    assert_eq!(
        client
            .delete(format!("{base}/{info_hash}/{first}/download"))
            .send()?
            .error_for_status()?
            .json::<serde_json::Value>()?["unpinned"],
        false
    );
    assert!(!handle.unpin_download(&info_hash, first, false)?.unpinned);

    // The last pin, with the files: the torrent goes with it, and the
    // answer says the data really went.
    assert_eq!(
        handle.unpin_download(&info_hash.to_uppercase(), second, true)?,
        stream_server::UnpinOutcome {
            unpinned: true,
            deleted_files: true,
        }
    );
    assert!(!named.exists(), "the torrent's folder is gone");
    assert_eq!(
        pieces_held(&cache_root, &info_hash),
        0,
        "and the bytes with it: the pieces are where the data was"
    );
    assert!(handle.downloads()?.is_empty());
    assert_eq!(handle.download_path(&info_hash, second)?, None);
    let listed: serde_json::Value = client
        .get(format!("{base}/downloads.json"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(listed, serde_json::json!([]));

    // `deletedFiles` reports what happened, not what was asked for: an
    // unmanaged hash nothing ever downloaded has no pieces in the store, so
    // the answer says nothing was deleted instead of echoing the query flag
    // back. ("Nothing there" is not "freed", and under this storage it is
    // the ordinary answer.)
    let unmanaged = "b".repeat(40);
    let nothing: serde_json::Value = client
        .delete(format!("{base}/{unmanaged}/0/download?deleteFiles=1"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(
        nothing,
        serde_json::json!({
            "infoHash": unmanaged,
            "fileIdx": 0,
            "unpinned": false,
            "deletedFiles": false,
        })
    );
    assert_eq!(
        handle.unpin_download(&unmanaged, 0, true)?,
        stream_server::UnpinOutcome {
            unpinned: false,
            deleted_files: false,
        }
    );

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// `GET /stream-numbers.json?url=...` is `ServerHandle::stream_numbers`, and
/// **a URL this server is not holding is `200 null`, not a `404`.**
///
/// The client is not asking whether a resource exists here; it is asking
/// what we hold of the stream its player is on, and "nothing" is a complete
/// answer to that -- the ordinary case for every stream this server neither
/// torrents nor proxies. A `404` would have a panel showing an error for a
/// film that is playing perfectly.
#[test]
fn the_stream_numbers_route_matches_the_library_api() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..offline_config()
    })?;
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;

    // A control route, so it takes the token like every other one.
    assert_eq!(
        reqwest::blocking::Client::new()
            .get(format!("{base}/stream-numbers.json?url=/x/0"))
            .send()?
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );

    // A stream this server does not hold: `null`, and the library says the
    // same.
    let url = format!("http://127.0.0.1:11470/{}/0", "f".repeat(40));
    // The URL carries no `&` or `=`, so it needs no escaping to survive one
    // query parameter.
    let response = client
        .get(format!("{base}/stream-numbers.json?url={url}"))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.json::<serde_json::Value>()?,
        serde_json::Value::Null
    );
    assert_eq!(handle.stream_numbers(&url)?, None);

    // And a request that names no stream at all is the client's mistake.
    assert_eq!(
        client
            .get(format!("{base}/stream-numbers.json"))
            .send()?
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **A panel's numbers are about the file its URL really resolves to.**
///
/// `/{infoHash}/-1` is the documented auto-select -- "pick the file
/// yourself", narrowed by the `f=` filters -- and it is what a client hands
/// its player whenever it does not name the file itself. The stream route
/// serves it and `/{infoHash}/-1/stats.json` reports on it, so a panel
/// holding that URL must be told about the file the player is being served
/// rather than told this server holds nothing.
///
/// **Which file that is only shows where the two files carry different
/// numbers**, so the fixture is a season pack: two episodes, both already on
/// the disk, both far larger than the budget, and only ever one of them
/// bounded -- the one a reader is inside. The file being played carries a
/// window and the other one does not, so a resolution that answers about the
/// wrong file puts the window on the wrong URL. That is what a constant file
/// index does, on `/{infoHash}/{fileIdx}` and on `-1` alike, and it is what
/// an auto-select that drops its `f=` filters does to a client narrowing a
/// pack to the episode it is playing.
///
/// And the window is measured over the played episode's own pieces. The rest
/// of the pack is on the same disk under the same info hash, so a window
/// counted over the torrent's holdings would offer a viewer the other
/// episodes' bytes as what they can scrub back into.
#[test]
fn a_panels_numbers_are_about_the_file_the_url_resolved_to() -> anyhow::Result<()> {
    /// The piece length `real_torrent` builds with.
    const PIECE: u64 = 16 * 1024;
    /// The episode the auto-select lands on: the largest video in the pack.
    const PICKED_PIECES: u64 = 100;
    /// The one only the `f=` filters reach. Smaller, so it is never the
    /// auto-select, and still far above the budget, so a policy over it is a
    /// bounded one.
    const FILTERED_PIECES: u64 = 60;
    /// Thirty-two pieces of budget: sixteen committed for sharing and
    /// sixteen of window, of which the 10% behind the playhead is one. Well
    /// under either episode, or the budget would cover the file being played
    /// and nothing would be bounding it.
    const BUDGET: u64 = 32 * PIECE;
    /// The piece of the file the player is on. Far enough in that the window
    /// has a piece behind it rather than sitting against the start.
    const PLAYING: u64 = 4;

    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let content = src.path().join("Show S01");
    std::fs::create_dir_all(&content)?;
    // Two episodes, each a whole number of pieces, so neither shares a
    // boundary piece with the other whatever order `create_torrent`'s
    // directory walk produced -- and so each one's piece range is the range
    // its own bytes are in.
    write_payload(
        &content.join("Show.S01E01.mkv"),
        (PICKED_PIECES * PIECE) as usize,
    );
    write_payload(
        &content.join("Show.S01E02.mkv"),
        (FILTERED_PIECES * PIECE) as usize,
    );
    let (torrent, info_hash) = real_torrent(&content);

    let cache_root = cache_dir.path().join("cache");
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    })?;
    seed_piece_store(&cache_root, &torrent, &content);
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?;
    let stats = stats_after_check(&client, &base, &info_hash)?;
    let picked_idx = file_index(&stats, "Show.S01E01.mkv");
    let filtered_idx = file_index(&stats, "Show.S01E02.mkv");
    let picked_pieces = file_pieces(&stats, picked_idx, PIECE);
    let filtered_pieces = file_pieces(&stats, filtered_idx, PIECE);
    complete_file_stats(&client, &base, &info_hash, picked_idx)?;
    complete_file_stats(&client, &base, &info_hash, filtered_idx)?;

    let anonymous = reqwest::blocking::Client::new();
    let auto_url = format!("{base}/{info_hash}/-1");
    // The filters a client narrowing a season pack sends, which reach the
    // same resolution here as they do on the stream route.
    let filtered_url = format!(
        "{base}/{info_hash}/-1?f={}",
        urlencoding::encode("/S01E02/i")
    );
    // Play, and answer which file was served: the whole length in the
    // Content-Range, because both episodes start with the same bytes.
    let play = |url: &str, from: u64| -> anyhow::Result<u64> {
        let response = anonymous
            .get(url)
            .header(
                reqwest::header::RANGE,
                format!("bytes={from}-{}", from + 15),
            )
            .send()?;
        anyhow::ensure!(
            response.status() == reqwest::StatusCode::PARTIAL_CONTENT,
            "a read of {url} at {from} answered {}",
            response.status()
        );
        let total = content_range_total(&response);
        anyhow::ensure!(response.bytes()?.len() == 16, "the player read its bytes");
        Ok(total)
    };
    // What this torrent holds of one file, taken off the store's directory
    // itself: the complete piece files, never a count anything keeps.
    let held_in = |pieces: &std::ops::Range<u32>| -> u64 {
        piece_store(&cache_root)
            .stat(&info_hash)
            .pieces
            .iter()
            .filter(|piece| piece.complete.is_some())
            .filter(|piece| u32::try_from(piece.index).is_ok_and(|index| pieces.contains(&index)))
            .count() as u64
            * PIECE
    };
    // The window a panel is given for the file it is playing, checked
    // against that file's own piece files -- either side of the question,
    // because a retention pass may reclaim between the two readings and may
    // only reclaim, nothing here downloading a piece back.
    let window_of = |url: &str, pieces: &std::ops::Range<u32>| -> anyhow::Result<()> {
        let before = held_in(pieces);
        let numbers = handle
            .stream_numbers(url)?
            .ok_or_else(|| anyhow::anyhow!("this server is holding the stream {url} names"))?;
        let after = held_in(pieces);
        let window = numbers
            .window
            .ok_or_else(|| anyhow::anyhow!("{url} names the bounded file, so it has a window"))?;
        let sum = window.behind_bytes + window.ahead_bytes;
        anyhow::ensure!(
            after <= sum && sum <= before,
            "the window is this episode's own pieces and not the pack's: {window:?} against \
             {after}..={before} bytes of this file, out of {} bytes of pieces the torrent holds",
            pieces_held(&cache_root, &info_hash) as u64 * PIECE
        );
        anyhow::ensure!(
            window.behind_bytes >= PIECE,
            "the piece behind the playhead is inside the window, so a player can scrub back \
             into what it has just played: {window:?}"
        );
        anyhow::ensure!(
            window.ahead_bytes >= PIECE,
            "and the piece under the playhead is in hand: {window:?}"
        );
        anyhow::ensure!(
            numbers.sharing.is_some(),
            "a torrent stream's bytes are seeded, so there is a sharing row: {numbers:?}"
        );
        Ok(())
    };
    // And the other file: this server holds it, and nothing is bounding it,
    // which is a stream with no window rather than a stream with no rows.
    let no_window_for = |url: &str| -> anyhow::Result<()> {
        let numbers = handle
            .stream_numbers(url)?
            .ok_or_else(|| anyhow::anyhow!("this server is holding the stream {url} names"))?;
        anyhow::ensure!(
            numbers.window.is_none(),
            "no reader is inside the file {url} names, so it has no window: {numbers:?}"
        );
        Ok(())
    };

    // A first read, so this torrent has an engine streaming it before the
    // budget arrives. Nothing bounds it yet, so it announces everything it
    // holds and nothing takes any of it -- which is what leaves the cache
    // the setting below is about to be measured against whole.
    assert_eq!(
        play(&auto_url, 0)?,
        PICKED_PIECES * PIECE,
        "the auto-select serves the largest video of the pack"
    );
    handle.update_settings(serde_json::json!({ "cacheSize": BUDGET as f64 }))?;
    assert_eq!(
        handle.cache_usage()?.limit_bytes,
        Some(BUDGET),
        "the process states a different cap than the one configured; the \
         volume this test runs on cannot give {BUDGET} bytes"
    );
    assert_eq!(
        pieces_held(&cache_root, &info_hash) as u64,
        PICKED_PIECES + FILTERED_PIECES,
        "and stating it took nothing: what bounds a torrent's bytes is the \
         pass over the file being played"
    );

    // The player seeks on and reads. Opening the reader is what installs a
    // policy under the budget the process has now published, and the byte
    // reaching the player is what moves the playhead -- into the file the
    // auto-select resolved to, and no other.
    play(&auto_url, PLAYING * PIECE)?;
    window_of(&auto_url, &picked_pieces)?;
    window_of(&format!("{base}/{info_hash}/{picked_idx}"), &picked_pieces)?;
    no_window_for(&format!("{base}/{info_hash}/{filtered_idx}"))?;

    // Now the filters, on the URL a client narrowing the pack hands its
    // player. The read moves the policy and the playhead to that episode,
    // and the numbers follow it: the filtered URL is the bounded one now and
    // the bare auto-select, which resolves to the episode nobody is inside,
    // has no window.
    assert_eq!(
        play(&filtered_url, PLAYING * PIECE)?,
        FILTERED_PIECES * PIECE,
        "the filter picked the other episode, as it does on the stream route"
    );
    window_of(&filtered_url, &filtered_pieces)?;
    window_of(
        &format!("{base}/{info_hash}/{filtered_idx}"),
        &filtered_pieces,
    )?;
    no_window_for(&auto_url)?;
    no_window_for(&format!("{base}/{info_hash}/{picked_idx}"))?;

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}
/// **What a playback panel is told about a torrent stream, end to end.**
///
/// The mirror of `proxy.rs`'s
/// `a_panel_asking_about_a_proxied_stream_is_told_what_is_on_the_disk`, over
/// the other store. A client holding the URL it handed its player asks one
/// question -- through the library call and through the route a client
/// really uses -- and gets the window round the playhead. It is a reading of
/// the piece store it gets and not the policy's intentions: the two halves
/// sum to the piece files really under the torrent's directory, which is
/// what the bound below says, a retention pass being the only thing that can
/// move that number and being able only to lower it.
///
/// Every other test of this window is a unit test over a fake backend, so
/// none of them would notice the window failing to reach `stream_numbers` at
/// all -- and a torrent stream's window is the row this whole feature was
/// asked for.
#[test]
fn a_panel_asking_about_a_torrent_stream_is_told_what_is_on_the_disk() -> anyhow::Result<()> {
    /// The piece length `real_torrent` builds with.
    const PIECE: u64 = 16 * 1024;
    /// A hundred pieces of file, so a window of sixteen sits inside it with
    /// pieces on both sides.
    const PIECES: u64 = 100;
    /// Thirty-two pieces of budget: sixteen committed for sharing and
    /// sixteen of window, of which the 10% behind the playhead is one. Well
    /// under the file, or the budget would cover it and nothing would be
    /// bounding this stream at all.
    const BUDGET: u64 = 32 * PIECE;
    /// The piece the player is on. Far enough in that the window has a
    /// piece behind it rather than sitting against the start of the file.
    const PLAYING: u64 = 4;

    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let content = src.path().join("Film");
    std::fs::create_dir_all(&content)?;
    // One file, so every piece the store holds for this torrent is a piece
    // of the stream the panel is asking about and the sum below is one the
    // test can take off the disk.
    write_payload(&content.join("film.bin"), (PIECES * PIECE) as usize);
    let (torrent, info_hash) = real_torrent(&content);

    let cache_root = cache_dir.path().join("cache");
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    })?;
    seed_piece_store(&cache_root, &torrent, &content);
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?;
    let stats = stats_after_check(&client, &base, &info_hash)?;
    let idx = file_index(&stats, "film.bin");
    complete_file_stats(&client, &base, &info_hash, idx)?;

    let anonymous = reqwest::blocking::Client::new();
    let player_url = format!("{base}/{info_hash}/{idx}");
    let play = |from: u64| -> anyhow::Result<()> {
        let response = anonymous
            .get(&player_url)
            .header(
                reqwest::header::RANGE,
                format!("bytes={from}-{}", from + 15),
            )
            .send()?;
        anyhow::ensure!(
            response.status() == reqwest::StatusCode::PARTIAL_CONTENT,
            "a read at {from} answered {}",
            response.status()
        );
        anyhow::ensure!(response.bytes()?.len() == 16, "the player read its bytes");
        Ok(())
    };

    // A first read, so this torrent has an engine streaming it before the
    // budget arrives. Nothing bounds it yet, so it announces everything it
    // holds and nothing takes any of it -- which is what leaves the cache
    // the setting below is about to be measured against whole.
    play(0)?;
    handle.update_settings(serde_json::json!({ "cacheSize": BUDGET as f64 }))?;
    assert_eq!(
        handle.cache_usage()?.limit_bytes,
        Some(BUDGET),
        "the process states a different cap than the one configured; the \
         volume this test runs on cannot give {BUDGET} bytes"
    );
    assert_eq!(
        pieces_held(&cache_root, &info_hash) as u64,
        PIECES,
        "and stating it took nothing: what bounds a torrent's bytes is the \
         pass over the file being played"
    );

    // The player seeks on and reads. Opening the reader is what installs a
    // policy under the budget the process has now published, and the byte
    // reaching the player is what moves the playhead.
    play(PLAYING * PIECE)?;

    // Either side of the question, because a retention pass may reclaim
    // between the two -- and may only reclaim, nothing here downloading a
    // piece back. So the sum the panel was given lies between them.
    let before = pieces_held(&cache_root, &info_hash) as u64 * PIECE;
    let numbers = handle
        .stream_numbers(&player_url)?
        .expect("this server is holding that stream");
    let after = pieces_held(&cache_root, &info_hash) as u64 * PIECE;
    let window = numbers.window.expect("a bounded stream has a window");
    let held = window.behind_bytes + window.ahead_bytes;
    assert!(
        after <= held && held <= before,
        "the two halves are the piece files really on the disk: {window:?} \
         against {after}..={before} bytes of pieces"
    );
    assert!(
        window.behind_bytes >= PIECE,
        "the piece behind the playhead is inside the window, so a player can \
         scrub back into what it has just played: {window:?}"
    );
    assert!(
        window.ahead_bytes >= PIECE,
        "and the piece under the playhead is in hand: {window:?}"
    );
    assert!(
        numbers.sharing.is_some(),
        "a torrent stream's bytes are seeded, so there is a sharing row: {numbers:?}"
    );

    // And the route a client asks with answers the same stream: the window
    // is a number there too, not a `null` a panel draws no row for.
    let answered: serde_json::Value = client
        .get(format!(
            "{base}/stream-numbers.json?url={}",
            urlencoding::encode(&player_url)
        ))
        .send()?
        .error_for_status()?
        .json()?;
    assert!(
        answered["window"]["behindBytes"]
            .as_u64()
            .is_some_and(|behind| behind >= PIECE)
            && answered["window"]["aheadBytes"]
                .as_u64()
                .is_some_and(|ahead| ahead >= PIECE),
        "the window reaches the route a panel really asks with: {answered}"
    );

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// The total length out of a `Content-Range: bytes a-b/total`.
fn content_range_total(response: &reqwest::blocking::Response) -> u64 {
    response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.rsplit_once('/').map(|(_, total)| total.to_string()))
        .expect("a partial response says what it is part of")
        .parse()
        .expect("the total is a number")
}

/// **The cap the publisher states follows what the owners hold.**
///
/// The disk arm of the cap is `occupied + available - floor`, and
/// `occupied` used to be whatever an eviction pass had last counted -- 0
/// until the first walk of the root finished. That is not a stale figure
/// that is close: a four-gigabyte television already holding four gigabytes
/// with six hundred megabytes free stated a cap of eighty-eight megabytes,
/// and every stream opened in the minutes a sixteen-thousand-file walk
/// takes ran under a window that size. The publisher reads the owners now,
/// and this is the moment that proves it: bytes that were on the volume
/// before the process started -- so the free-space arm has already lost
/// them -- become *counted* when the session registers the store over them,
/// and the very next publication states a cap that much larger.
///
/// The cap is read where the retention policies read it
/// (`ServerHandle::published_cache_budget`), not from `GET /cache.json`,
/// which runs the arithmetic again for the client: a publisher that stopped
/// reading the owners would still be answering that route correctly while
/// sizing every window on free space alone.
#[test]
fn the_minute_publishers_cap_follows_the_owners_occupancy() -> anyhow::Result<()> {
    /// Enough that the difference is nothing like the noise of a session
    /// writing its own records while this runs.
    const SEEDED: usize = 16 * 1024 * 1024;
    /// `cacheSize` above anything a volume could offer, so the cap under
    /// test is the disk arm and not the setting.
    const UNCAPPED: f64 = 1024.0 * 1024.0 * 1024.0 * 1024.0;

    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let content = src.path().join("Movie");
    std::fs::create_dir_all(&content)?;
    write_payload(&content.join("film.mkv"), SEEDED);
    let (torrent, info_hash) = real_torrent(&content);

    let cache_root = resolved(&cache_dir.path().join("cache"));
    let handle = stream_server::start(ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    })?;
    // Already "streamed": the data sits in the piece store, as it would
    // after playback, and no store speaks for it until the session picks it
    // up below. The bytes are on the volume before the reading below, which
    // is the whole point -- whatever `statvfs` answers has already lost
    // them, so nothing but the count can put them back into the cap. (The
    // seed goes after the launch sweep, which removes every store directory
    // the session does not claim.)
    seed_piece_store(&cache_root, &torrent, &content);
    handle.update_settings(serde_json::json!({ "cacheSize": UNCAPPED }))?;
    let before = handle
        .published_cache_budget()
        .expect("a cap from the volume");

    // The session picks the seeded pieces up: `init` seeds the store's held
    // set from what is on the disk and registers it, and from that instant
    // the process knows it holds those bytes.
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    let created: serde_json::Value = client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(created["infoHash"], info_hash);
    let stats = stats_after_check(&client, &base, &info_hash)?;
    // Pinned, so nothing is playing it and nothing takes it either: the
    // count under test stays still while the two publications straddle it.
    handle.pin_download(&info_hash, file_index(&stats, "film.mkv"), &[])?;

    handle.update_settings(serde_json::json!({ "cacheSize": UNCAPPED }))?;
    let after = handle
        .published_cache_budget()
        .expect("a cap from the volume");
    let usage = handle.cache_usage()?;
    assert!(
        usage.total_bytes >= SEEDED as u64,
        "the store counts what it registered: {usage:?}"
    );
    assert!(
        after > before,
        "the cap grew by what the store now says it holds: {before} -> {after}"
    );
    // And the two readings of the same cache agree. `GET /cache.json` runs
    // the same arithmetic over the same owners a moment later, so the only
    // difference between them is whatever the volume did in between -- a
    // publisher reading no occupancy at all would be short by the whole of
    // the seeded film.
    let reported = usage.limit_bytes.expect("a cap from the volume");
    assert!(
        after.abs_diff(reported) < SEEDED as u64 / 2,
        "the cap in force and the cap reported are one number: {after} against {reported}"
    );

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **A clean restates the cap before it reports one.**
///
/// The number a clean answers with is the number the owners are now sized
/// against, and that is only true if the clean says it: the cap is
/// published by the minute timer and by `POST /settings`, and a clean that
/// moved the occupancy without restating it would answer a client a figure
/// nothing in the process was enforcing -- for up to a minute, which is
/// several windows.
///
/// Read where the policies read it (`ServerHandle::published_cache_budget`)
/// and not off `GET /cache.json`, which runs the arithmetic again for the
/// client: a clean that stopped restating would still answer that route
/// correctly while every window stayed sized to the cap of a minute ago.
///
/// The occupancy is moved here by the session registering a store over
/// bytes that were already on the volume -- the same motion
/// `the_minute_publishers_cap_follows_the_owners_occupancy` uses, and for
/// the same reason: it is a change no `statvfs` can see, so only a
/// publication that reads the owners can carry it.
///
/// The claim is the *agreement* of the two numbers and not the direction
/// either moved: the disk arm is `occupied + available - floor`, and
/// `available` is a real volume that the rest of this test binary is
/// writing to meanwhile. Both readings here are taken inside the one call,
/// microseconds apart, so nothing the volume does can separate them --
/// while a cap last stated before the store registered is short by the
/// whole of the seeded film.
#[test]
fn a_clean_restates_the_cap_before_it_reports_it() -> anyhow::Result<()> {
    /// Enough that the difference is nothing like the noise of a session
    /// writing its own records while this runs.
    const SEEDED: usize = 16 * 1024 * 1024;
    /// `cacheSize` above anything a volume could offer, so the cap under
    /// test is the disk arm -- the one occupancy is a term of.
    const UNCAPPED: f64 = 1024.0 * 1024.0 * 1024.0 * 1024.0;

    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let content = src.path().join("Movie");
    std::fs::create_dir_all(&content)?;
    write_payload(&content.join("film.mkv"), SEEDED);
    let (torrent, info_hash) = real_torrent(&content);

    let cache_root = resolved(&cache_dir.path().join("cache"));
    let handle = stream_server::start(ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    })?;
    seed_piece_store(&cache_root, &torrent, &content);
    handle.update_settings(serde_json::json!({ "cacheSize": UNCAPPED }))?;
    assert!(
        handle.published_cache_budget().is_some(),
        "a cap from the volume, stated before the store that holds the film exists"
    );

    // The session picks the seeded pieces up, which is the whole of what
    // moves the occupancy. Pinned, so the clean below has nothing to take
    // and the only thing it can change is the published number.
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?;
    let stats = stats_after_check(&client, &base, &info_hash)?;
    handle.pin_download(&info_hash, file_index(&stats, "film.mkv"), &[])?;

    let report = handle.clean_cache_now()?;
    assert_eq!(report.freed, 0, "a pin is nobody's slack: {report:?}");
    let in_force = handle
        .published_cache_budget()
        .expect("a cap from the volume");
    let reported = report.limit.expect("a cap from the volume");
    assert!(
        in_force.abs_diff(reported) < SEEDED as u64 / 2,
        "the cap in force is not the cap the clean reported: {in_force} against \
         {reported}, which is the film's {SEEDED} bytes of occupancy missing from \
         a cap nothing restated"
    );

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// `GET /cache.json` and `POST /cache/clean` share their functions with
/// `ServerHandle::{cache_usage, clean_cache_now}`, and the two surfaces
/// answer the same bytes: the report the route returns is the report the
/// library call returns, field for field.
///
/// What a clean does is give back both owners' slack, so on this fixture it
/// gives back **nothing**, and that is the claim. A pinned download is kept
/// until it is unpinned, however far over the cap the cache is; and the
/// **whole-file copies an earlier version of this server left behind**
/// belong to no owner at all -- nothing here booked those bytes, nothing
/// counts them and nothing takes them. There is no migration by decision: a
/// plain-file download is neither converted nor read, and the torrent that
/// owns it re-downloads as pieces. Nothing old matters.
///
/// So the honest answer to "clean now" here is `overLimit`, and a client
/// that wants those bytes back unpins something.
#[test]
fn cache_routes_match_the_library_api() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;

    let content = src.path().join("Movie");
    std::fs::create_dir_all(&content)?;
    // Two files, each a whole number of 16 KiB pieces (as `lan_media_server`
    // does). Pinning only `movie.mkv` keeps the whole torrent's data from
    // this clean: a pinned torrent keeps everything it holds, and a piece
    // store is not divisible by file at that level. What `GET /cache.json`
    // reports as protected is the pinned file alone -- a pin is per file,
    // and a file is the unit a client can put in front of a user -- so the
    // two figures below are two honest answers to two different questions
    // and are not read against each other.
    write_payload(&content.join("movie.mkv"), 64 * 1024);
    write_payload(&content.join("subtitle.srt"), 16 * 1024);
    let (torrent, info_hash) = real_torrent(&content);

    let cache_root = resolved(&cache_dir.path().join("cache"));

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    })?;
    // Written *after* the server opened, and that is the whole of why it is
    // here rather than above: a previous release's whole-file data is the
    // one category with no owner, and the launch sweep
    // (`piece_store::sweep_legacy_downloads`) is what takes it -- at launch,
    // and nowhere else. Put here, these stand for the life of the process
    // exactly as they would in one that had already booted, which is what
    // makes them a witness that no *route* takes them.
    //
    // An idle leftover with no engine managing it at all -- ordinary cache
    // from a torrent nothing is tracking any more.
    let idle = cache_root
        .join("rqbit-downloads")
        .join("Leftover")
        .join("old.mkv");
    std::fs::create_dir_all(idle.parent().unwrap())?;
    write_payload(&idle, 16 * 1024);
    // And the legacy whole-file copy of *this* torrent, exactly where the
    // filesystem storage used to put a directory torrent's data.
    let root_folder = cache_root.join("rqbit-downloads").join("Movie");
    std::fs::create_dir_all(&root_folder)?;
    std::fs::copy(content.join("movie.mkv"), root_folder.join("movie.mkv"))?;
    std::fs::copy(
        content.join("subtitle.srt"),
        root_folder.join("subtitle.srt"),
    )?;
    // Already "streamed": the data sits in the piece store, as it would
    // after playback, and `POST /create` below picks it up from there.
    seed_piece_store(&cache_root, &torrent, &content);
    let seeded_pieces = pieces_held(&cache_root, &info_hash);
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    let anonymous = reqwest::blocking::Client::new();

    // Both routes are token-protected, like every other control route.
    for response in [
        anonymous.get(format!("{base}/cache.json")).send()?,
        anonymous.post(format!("{base}/cache/clean")).send()?,
    ] {
        assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    }
    assert!(idle.is_file(), "an unauthorized call cleans nothing");

    let created: serde_json::Value = client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(created["infoHash"], info_hash);
    let stats = stats_after_check(&client, &base, &info_hash)?;
    let idx = file_index(&stats, "movie.mkv");
    handle.pin_download(&info_hash, idx, &[])?;
    stats_after_check(&client, &base, &info_hash)?;

    // Read usage() before touching the limit, to learn how many bytes the
    // pinned file occupies. The limit below sits one byte over that, so the
    // cache is really over its cap while the clean runs -- otherwise
    // "nothing was taken" would be the uninteresting answer of a cache with
    // room to spare rather than the answer of one that has nothing
    // disposable in it.
    let baseline = handle.cache_usage()?;
    assert_eq!(
        baseline.protected_files, 1,
        "the pinned file, counted as a file and not as its piece files: {baseline:?}"
    );
    let limit = baseline.protected_bytes + 1;
    handle.update_settings(serde_json::json!({ "cacheSize": limit as f64 }))?;

    // cache_usage() == GET /cache.json, and reading it evicts nothing.
    let api_usage = handle.cache_usage()?;
    let http_usage: serde_json::Value = client
        .get(format!("{base}/cache.json"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(serde_json::to_value(&api_usage)?, http_usage);
    assert!(idle.is_file(), "usage() must not touch the filesystem");
    assert!(root_folder.join("movie.mkv").is_file());
    assert_eq!(http_usage["limitBytes"], limit, "{http_usage}");
    assert!(
        http_usage["totalBytes"].as_u64().unwrap() > limit,
        "the torrent's own unpinned pieces push the cache over the limit -- \
         the legacy whole-file copies belong to no owner and are in no \
         owner's count: {http_usage}"
    );
    assert_eq!(
        http_usage["protectedFiles"], 1,
        "the pinned file: a pin is per file, and nothing else here is played \
         or pinned: {http_usage}"
    );
    assert_eq!(
        http_usage["protectedBytes"], baseline.protected_bytes,
        "{http_usage}"
    );

    // POST /cache/clean over HTTP: both owners give back their slack, and
    // there is none to give. The pin is kept, and the whole-file copies an
    // earlier version left behind are nobody's -- in no count and in no
    // pass -- so every byte this test put on the disk is still there
    // afterwards.
    let report: serde_json::Value = client
        .post(format!("{base}/cache/clean"))
        .send()?
        .error_for_status()?
        .json()?;
    assert!(
        idle.is_file(),
        "a byte no owner ever booked is no clean's to take: {report}"
    );
    assert!(
        root_folder.join("movie.mkv").is_file() && root_folder.join("subtitle.srt").is_file(),
        "the legacy whole-file copies with it: {report}"
    );
    assert_eq!(
        pieces_held(&cache_root, &info_hash),
        seeded_pieces,
        "and the pin's real bytes are untouched: {report}"
    );
    assert_eq!(report["deleted"], 0, "{report}");
    assert_eq!(report["freed"], 0, "{report}");
    // The figures are the owners' own, which is what makes them the same
    // figures `GET /cache.json` answers: the pinned file, counted as a
    // file, and the cap the process has just restated.
    assert_eq!(report["protectedFiles"], 1, "{report}");
    assert_eq!(report["protected"], baseline.protected_bytes, "{report}");
    assert_eq!(report["limit"], limit, "{report}");
    assert!(
        report["overLimit"].as_u64().unwrap() > 0,
        "and it says it is still over the cap rather than pretending otherwise: {report}"
    );
    assert_eq!(
        report["total"],
        handle.cache_usage()?.total_bytes,
        "the clean's total is the figure the usage route answers: {report}"
    );

    // clean_cache_now() == POST /cache/clean, run right after over the
    // library instead -- the same function underneath both surfaces, so the
    // two can never disagree about a field.
    let api_report = handle.clean_cache_now()?;
    assert_eq!(serde_json::to_value(&api_report)?, report);
    assert_eq!(pieces_held(&cache_root, &info_hash), seeded_pieces);

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// A server whose torrent is pre-seeded in the cache root, so media
/// requests answer real bytes with no peer anywhere -- and a LAN media
/// listener on `lan_media_addr` when one is given.
///
/// Returns the handle, the loopback base URL, the info hash, the file index
/// and the payload the file holds.
fn lan_media_server(
    config_dir: &std::path::Path,
    cache_dir: &std::path::Path,
    src: &std::path::Path,
    lan_media_addr: Option<std::net::SocketAddr>,
) -> anyhow::Result<(ServerHandle, String, String, usize, Vec<u8>)> {
    let content = src.join("Movie");
    std::fs::create_dir_all(&content)?;
    // Two files, each a whole number of 16 KiB pieces, so neither shares a
    // boundary piece with the other whatever order `create_torrent`'s
    // directory walk produced.
    write_payload(&content.join("movie.bin"), 64 * 1024);
    write_payload(&content.join("extra.bin"), 16 * 1024);
    let payload = std::fs::read(content.join("movie.bin"))?;
    let (torrent, info_hash) = real_torrent(&content);

    let cache_root = resolved(&cache_dir.join("cache"));
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        lan_media_addr,
        config_dir: Some(config_dir.join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    })?;
    // After the start, and before the add: the launch sweep has run and has
    // nothing to say about a torrent that does not exist yet.
    seed_piece_store(&cache_root, &torrent, &content);
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?;
    let stats = stats_after_check(&client, &base, &info_hash)?;
    let idx = file_index(&stats, "movie.bin");
    complete_file_stats(&client, &base, &info_hash, idx)?;

    Ok((handle, base, info_hash, idx, payload))
}

/// Wait until the pre-seeded file really is complete, and answer its stats.
///
/// The hash check of the pre-seeded copy is what makes the bytes servable,
/// and `phase` can already read `buffering` while that check is still queued
/// -- so this waits on the observable state a media request needs (bounded,
/// not a timing assertion), never on a sleep of its own choosing.
fn complete_file_stats(
    client: &reqwest::blocking::Client,
    base: &str,
    info_hash: &str,
    idx: usize,
) -> anyhow::Result<serde_json::Value> {
    let deadline = std::time::Instant::now() + CHECK_WAIT_BOUND;
    loop {
        let stats = file_stats_after_check(client, base, info_hash, idx)?;
        if stats["files"][idx]["complete"] == true {
            return Ok(stats);
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the pre-seeded file was still incomplete after {CHECK_WAIT_BOUND:?}: {stats}"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// Permit and start the LAN media listener the way a cast session does: the
/// setting first, because the veto is the default, then the start. Nothing
/// binds the listener at startup, however it is configured, so every test
/// that wants one up asks for it here.
fn start_lan_media(handle: &ServerHandle) -> anyhow::Result<std::net::SocketAddr> {
    handle.update_settings(serde_json::json!({ "lanMediaEnabled": true }))?;
    handle
        .set_lan_media(true)?
        .ok_or_else(|| anyhow::anyhow!("set_lan_media(true) answered with no address"))
}

/// The whole free-space loop, inside a real server: the reconciler `run()`
/// starts stops a torrent that is writing when its volume falls under the
/// floor, the statistics say so, and it runs again when the space comes
/// back.
///
/// Every unit test of the arm drives `reconcile_tick` by hand, so none of
/// them would notice the two lines in `run()` that start the loop going
/// away -- which is the whole of what makes the floor hold on a real
/// device. A volume cannot be filled on demand, so the engine's own probe
/// is declared through `pretend_volume_space`, keyed by this test's cache
/// root so no other server in the run sees it.
///
/// The last part is the window the hysteresis opens. Between the floor and
/// the resume margin a timer leaves a stopped torrent alone -- but the
/// stream route no longer has a refusal of its own keyed on "this torrent
/// is stopped", so a request landing there must be what starts it, or the
/// reader would park on a torrent nothing is fetching for.
#[test]
fn the_servers_own_reconciler_stops_a_torrent_under_the_floor_and_starts_it_again()
-> anyhow::Result<()> {
    const FLOOR: u64 = enginefs::CACHE_FREE_SPACE_FLOOR;
    const MARGIN: u64 = enginefs::FREE_SPACE_RESUME_MARGIN;
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let cache_root = resolved(&cache_dir.path().join("cache"));
    // Declared before the server starts, so its reconciler never reads the
    // machine's real disk for this root.
    stream_server::pretend_volume_space(&cache_root, u64::MAX);

    let content = src.path().join("Wanted");
    std::fs::create_dir_all(&content)?;
    write_payload(&content.join("wanted.bin"), 64 * 1024);
    let (torrent, info_hash) = real_torrent(&content);

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    })?;
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?;
    let stats = stats_after_check(&client, &base, &info_hash)?;
    let idx = file_index(&stats, "wanted.bin");

    // Nothing of the file is on disk, so it wants every byte it has.
    let stopped_message =
        "the torrent is stopped for want of disk space; free some space and it will resume";
    let error_of = |client: &reqwest::blocking::Client| -> anyhow::Result<Option<String>> {
        let stats: serde_json::Value = client
            .get(format!("{base}/{info_hash}/stats.json"))
            .send()?
            .error_for_status()?
            .json()?;
        Ok(stats["error"].as_str().map(str::to_owned))
    };
    assert_eq!(error_of(&client)?, None, "nothing is wrong with it yet");

    // The volume fills. Bounded poll on what the client can see, never a
    // sleep: the loop runs on its own two-second interval.
    stream_server::pretend_volume_space(&cache_root, 0);
    let deadline = std::time::Instant::now() + CHECK_WAIT_BOUND;
    loop {
        if error_of(&client)?.as_deref() == Some(stopped_message) {
            break;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the server's reconciler never stopped the torrent"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Room above the floor, but inside the resume margin: the timer leaves
    // it stopped, and a stream request is what starts it. The route's own
    // probe reads the same number, so its floor check passes.
    stream_server::pretend_volume_space(&cache_root, FLOOR + MARGIN - 1);
    stream_server::pretend_available_space(&cache_root, FLOOR + MARGIN - 1);
    let anonymous = reqwest::blocking::Client::new();
    let served = anonymous
        .get(format!("{base}/{info_hash}/{idx}"))
        .header(reqwest::header::RANGE, "bytes=0-1023")
        .timeout(std::time::Duration::from_secs(3))
        .send();
    match served {
        Ok(response) => assert_ne!(
            response.status(),
            reqwest::StatusCode::INSUFFICIENT_STORAGE,
            "with room above the floor the request is not refused"
        ),
        // No peer will ever bring these bytes, so the request waits until
        // this client gives up -- which is the proof it was not refused.
        Err(error) => assert!(error.is_timeout(), "{error}"),
    }
    let deadline = std::time::Instant::now() + CHECK_WAIT_BOUND;
    loop {
        if error_of(&client)? != Some(stopped_message.to_string()) {
            break;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the playback start never started the torrent again"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// A restart, over the real persisted librqbit session the server keeps,
/// with a torrent the previous process had stopped -- and the stream
/// request that starts it again.
///
/// This is the whole of the design, from outside. The pause is in
/// `session.json` and survived the process; nothing in the new one knows
/// it exists, let alone why. On master three of the four sites that could
/// have lifted it read `if idle_paused.swap(false) && resume()`, which is
/// `false && ...` in a fresh process, and the fourth -- the stream route's
/// own metadata-resolution guard, which this test drives -- was gated on
/// the same flag.
///
/// Observed through `swarmPaused`, which the backend answers from its state
/// machine: it is the one field of the stats shape that says whether the
/// torrent is running.
///
/// The volume is held **inside the hysteresis band** -- room above the
/// free-space floor, but under the resume margin -- for the middle of the
/// test, because that is the window in which nothing *but* a request will
/// start the torrent: the reconciler's timer deliberately leaves a stopped
/// torrent alone until the volume has cleared the margin. Without that the
/// timer would start it a couple of seconds later and the test would pass
/// whatever the route did. The route's own floor check reads a separate
/// declaration (`pretend_available_space`), so the request is not refused.
#[test]
fn a_restart_leaves_a_torrent_stopped_and_a_stream_request_starts_it() -> anyhow::Result<()> {
    const FLOOR: u64 = enginefs::CACHE_FREE_SPACE_FLOOR;
    const MARGIN: u64 = enginefs::FREE_SPACE_RESUME_MARGIN;
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let cache_root = resolved(&cache_dir.path().join("cache"));

    let content = src.path().join("Wanted");
    std::fs::create_dir_all(&content)?;
    write_payload(&content.join("wanted.bin"), 64 * 1024);
    let (torrent, info_hash) = real_torrent(&content);

    let start = || {
        stream_server::start(stream_server::ServerConfig {
            http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            config_dir: Some(config_dir.path().join("config")),
            cache_dir: Some(cache_root.clone()),
            ..offline_config()
        })
    };
    let stopped_message =
        "the torrent is stopped for want of disk space; free some space and it will resume";

    // The process before this one: it adds the torrent, its volume fills,
    // and its reconciler stops it. librqbit persists that.
    stream_server::pretend_volume_space(&cache_root, u64::MAX);
    let handle = start()?;
    {
        let base = format!("http://{}", handle.http_addr());
        let client = bearer_client(&handle)?;
        client
            .post(format!("{base}/create"))
            .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
            .send()?
            .error_for_status()?;
        stats_after_check(&client, &base, &info_hash)?;

        stream_server::pretend_volume_space(&cache_root, 0);
        let deadline = std::time::Instant::now() + CHECK_WAIT_BOUND;
        loop {
            let stats: serde_json::Value = client
                .get(format!("{base}/{info_hash}/stats.json"))
                .send()?
                .error_for_status()?
                .json()?;
            if stats["error"].as_str() == Some(stopped_message) {
                assert_eq!(stats["swarmPaused"], serde_json::json!(true));
                break;
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "the first process never stopped the torrent: {stats}"
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    handle.shutdown()?;
    handle.join()?;

    // This one. Room above the floor, inside the resume margin: nothing on
    // a timer will start the torrent here.
    stream_server::pretend_volume_space(&cache_root, FLOOR + MARGIN - 1);
    stream_server::pretend_available_space(&cache_root, FLOOR + MARGIN - 1);
    let handle = start()?;
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    let swarm_paused = |client: &reqwest::blocking::Client| -> anyhow::Result<bool> {
        let stats: serde_json::Value = client
            .get(format!("{base}/{info_hash}/stats.json"))
            .send()?
            .error_for_status()?
            .json()?;
        Ok(stats["swarmPaused"] == serde_json::json!(true))
    };
    // The initial check has to finish before the state machine can say
    // anything settled about the torrent at all -- and `phase` leaving
    // `checking` is not that moment, so this is a bounded poll on the
    // reading the rest of the test depends on rather than one assertion
    // taken the instant `phase` moves.
    stats_after_check(&client, &base, &info_hash)?;
    let deadline = std::time::Instant::now() + CHECK_WAIT_BOUND;
    while !swarm_paused(&client)? {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the torrent did not come back stopped, as the last process left it"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    // And it stays that way while nobody asks: several reconciler ticks
    // (its interval is two seconds) go by.
    std::thread::sleep(enginefs::reconcile::RECONCILE_INTERVAL * 3);
    assert!(
        swarm_paused(&client)?,
        "the timer must not start a torrent into a volume inside the resume margin"
    );

    // A player's first request for a file it has not seen before, which is
    // a HEAD: it asks how long the file is and whether ranges work. That
    // path resolves the torrent's file list and nothing else -- no stream
    // is started, so the metadata-resolution guard is the only thing on it
    // that asks the reconciler anything. It is also the request that must
    // work on a torrent whose metadata is not resolved at all, which is a
    // torrent that has to be running to resolve it.
    let anonymous = reqwest::blocking::Client::new();
    let head = anonymous
        .head(format!("{base}/{info_hash}/0"))
        .timeout(std::time::Duration::from_secs(10))
        .send()?
        .error_for_status()?;
    assert_eq!(
        header_value(&head, "content-length"),
        (64 * 1024).to_string()
    );
    assert!(
        swarm_paused(&client)?,
        "a HEAD reads the file list and opens no stream, so it starts nothing"
    );

    // And the GET that follows is not refused: there is room above the
    // floor. It is also what really opens a stream on this torrent, which
    // is what makes it the one being played and so the one the ladder
    // starts. No peer will ever bring these bytes, so the client giving up
    // is the expected end.
    match anonymous
        .get(format!("{base}/{info_hash}/0"))
        .header(reqwest::header::RANGE, "bytes=0-1023")
        .timeout(std::time::Duration::from_secs(3))
        .send()
    {
        Ok(response) => assert_ne!(
            response.status(),
            reqwest::StatusCode::INSUFFICIENT_STORAGE,
            "with room above the floor the request is not refused"
        ),
        Err(error) => assert!(error.is_timeout(), "{error}"),
    }
    assert!(
        !swarm_paused(&client)?,
        "the request that opened a stream started the torrent it reads from"
    );

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// An archive member served out of a live torrent is a playback too, and
/// the route that serves it says so.
///
/// `routes::archive::stream_file`'s `torrent:` form opens a file reader on
/// a torrent exactly the way the stream route does, and registered nothing
/// at all: no `on_stream_start`, no reconcile. Two failures follow, and
/// this test drives the one that can be observed from outside without
/// racing a timer -- an archive request landing on a torrent the last
/// reconciler pass already stopped, which is left parked on pieces nobody
/// is fetching, with no end and no error, because nothing asked the
/// reconciler to start it. (The other is the mirror image: with seeding
/// off and the grace elapsed, the timer pauses the torrent an archive
/// response body is streaming from, mid-body.)
///
/// The volume is held **inside the hysteresis band** for the archive
/// request, exactly as `a_restart_leaves_a_torrent_stopped_and_a_stream_request_starts_it`
/// holds it: room above the floor, but under the resume margin, is the one
/// window in which nothing *but* a request will start the torrent.
/// Without it the timer would start it a couple of seconds later and the
/// test would pass whatever the route did.
///
/// Nothing seeds this fixture, so the member is never read and the client
/// gives up -- which is the point. What the request has to leave behind is
/// a torrent that is running.
#[test]
fn an_archive_member_request_starts_the_torrent_it_reads_from() -> anyhow::Result<()> {
    const FLOOR: u64 = enginefs::CACHE_FREE_SPACE_FLOOR;
    const MARGIN: u64 = enginefs::FREE_SPACE_RESUME_MARGIN;
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let cache_root = resolved(&cache_dir.path().join("cache"));
    stream_server::pretend_volume_space(&cache_root, u64::MAX);

    // A torrent whose one file is an archive. Its bytes are never read --
    // no peer will bring them -- so what is in it does not matter, but the
    // suffix does: it is what picks the reader, and only the two formats
    // `archives::get_archive_reader_from_stream` can drive from a stream
    // get as far as reading the torrent at all.
    let content = src.path().join("Wanted");
    std::fs::create_dir_all(&content)?;
    write_payload(&content.join("fixture.zip"), 64 * 1024);
    let (torrent, info_hash) = real_torrent(&content);

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    })?;
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?;
    stats_after_check(&client, &base, &info_hash)?;

    let swarm_paused = |client: &reqwest::blocking::Client| -> anyhow::Result<bool> {
        let stats: serde_json::Value = client
            .get(format!("{base}/{info_hash}/stats.json"))
            .send()?
            .error_for_status()?
            .json()?;
        Ok(stats["swarmPaused"] == serde_json::json!(true))
    };

    // The volume fills and the server's own reconciler stops the torrent.
    stream_server::pretend_volume_space(&cache_root, 0);
    let deadline = std::time::Instant::now() + CHECK_WAIT_BOUND;
    while !swarm_paused(&client)? {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the reconciler never stopped the torrent"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Room above the floor, inside the resume margin: several ticks go by
    // and the timer leaves it stopped.
    stream_server::pretend_volume_space(&cache_root, FLOOR + MARGIN - 1);
    stream_server::pretend_available_space(&cache_root, FLOOR + MARGIN - 1);
    std::thread::sleep(enginefs::reconcile::RECONCILE_INTERVAL * 3);
    assert!(
        swarm_paused(&client)?,
        "the timer must not start a torrent into a volume inside the resume margin"
    );

    // `torrent:<info hash>/<path in the torrent>` is one path segment, so
    // the separator inside it is encoded; the member after it is the
    // wildcard. The zip reader goes looking for the central directory and
    // parks there for ever, so the client giving up is the expected end --
    // and the timeout is generous rather than tight because what is being
    // waited for is the *server* reaching its registration, on a loaded
    // machine running the whole suite in parallel.
    let anonymous = reqwest::blocking::Client::new();
    match anonymous
        .get(format!(
            "{base}/zip/stream/torrent:{info_hash}%2Ffixture.zip/first.txt"
        ))
        .timeout(std::time::Duration::from_secs(10))
        .send()
    {
        Ok(response) => assert_ne!(
            response.status(),
            reqwest::StatusCode::NOT_FOUND,
            "the route found neither the torrent nor the archive member"
        ),
        Err(error) => assert!(error.is_timeout(), "{error}"),
    }
    assert!(
        !swarm_paused(&client)?,
        "the archive request started the torrent it was about to read from"
    );

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// The bytes of the member `stored_zip` puts in its archive: deterministic,
/// and mildly incompressible so nothing along the way can shorten it.
fn member_payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i.wrapping_mul(31) % 251) as u8).collect()
}

/// A zip holding one member, **stored** rather than deflated.
///
/// Stored because a fixture that leaves a hole in the middle of the archive
/// needs to know where the member's bytes are: uncompressed, they run from a
/// local header at the front to the central directory at the back, so a hole
/// anywhere in the middle of the file is a hole in the member's data and
/// nowhere else.
fn stored_zip(member: &str, len: usize) -> Vec<u8> {
    let data = member_payload(len);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let mut writer = async_zip::base::write::ZipFileWriter::with_tokio(Vec::new());
        writer
            .write_entry_whole(
                async_zip::ZipEntryBuilder::new(member.into(), async_zip::Compression::Stored)
                    .build(),
                &data,
            )
            .await
            .expect("write the member");
        writer.close().await.expect("close the zip").into_inner()
    })
}

/// A server holding one torrent whose only file is `fixture.zip`, a zip with
/// one stored member -- the shape `/zip/stream/torrent:<hash>/<archive>/<member>`
/// reads.
///
/// `hole` is a byte range of that file the piece store is *not* seeded with.
/// Nothing seeds these fixtures and no peer will ever bring the missing
/// pieces, so a read that reaches the hole parks there for as long as the
/// test wants, which is how a response body is held open.
///
/// The volume is declared roomy: every arm of the reconciler's ladder above
/// the idle one is about the disk, and a test about the idle one wants none
/// of them.
fn archive_member_server(
    config_dir: &std::path::Path,
    cache_dir: &std::path::Path,
    src: &std::path::Path,
    member: &str,
    member_len: usize,
    hole: Option<std::ops::Range<u64>>,
) -> anyhow::Result<(ServerHandle, String, String)> {
    let content = src.join("Wanted");
    std::fs::create_dir_all(&content)?;
    std::fs::write(content.join("fixture.zip"), stored_zip(member, member_len))?;
    let (torrent, info_hash) = real_torrent(&content);

    let cache_root = resolved(&cache_dir.join("cache"));
    stream_server::pretend_volume_space(&cache_root, u64::MAX);
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    })?;
    // After the start, never before (see `seed_piece_store_pieces`).
    seed_piece_store_pieces(&cache_root, &torrent, &content, None, hole);
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?;
    stats_after_check(&client, &base, &info_hash)?;
    Ok((handle, base, info_hash))
}

/// `torrent:<info hash>/<path in the torrent>` is one path segment of the
/// archive URL, so the separator inside it is encoded; the member after it is
/// the wildcard.
fn archive_member_url(base: &str, info_hash: &str, member: &str) -> String {
    format!("{base}/zip/stream/torrent:{info_hash}%2Ffixture.zip/{member}")
}

/// Whether the reconciler has this torrent stopped, as the server reports it:
/// `swarmPaused` is `run_state() == Paused` read off librqbit, not anything
/// the route under test writes.
fn swarm_paused(
    client: &reqwest::blocking::Client,
    base: &str,
    info_hash: &str,
) -> anyhow::Result<bool> {
    let stats: serde_json::Value = client
        .get(format!("{base}/{info_hash}/stats.json"))
        .send()?
        .error_for_status()?
        .json()?;
    Ok(stats["swarmPaused"] == serde_json::json!(true))
}

/// The stream an archive member read registers lasts as long as the
/// **response body**, so the reconciler leaves that torrent running while a
/// player is still reading out of it.
///
/// `routes::archive::stream_file` puts the registration guard inside the
/// body's closure for exactly this reason, and what the guard's `start` does
/// first is tell the server a stream opened on this torrent -- which is what
/// makes it the entity being played (`enginefs::retention::live`). Every
/// other torrent is then one nobody is playing, and the ladder's bottom arm
/// stops those at once.
///
/// Two torrents, because "it is still running" only means something if the
/// arm was firing at all in that window: the second one is read by nobody,
/// and the test waits for the reconciler to stop *it* before asking about the
/// first. That is the observable proof the policy was armed and running --
/// no counter the route writes is read anywhere here.
///
/// The body is held open by the fixture rather than by the client's reading
/// pace: the middle of the archive is missing from the piece store and no
/// peer will bring it, so the extraction parks there and the response stays
/// open however slowly or quickly the client reads.
#[test]
fn an_archive_body_keeps_its_torrent_running_while_it_is_open() -> anyhow::Result<()> {
    const MEMBER: &str = "member.bin";
    const MEMBER_LEN: usize = 512 * 1024;
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;

    // The hole sits well inside the member's own bytes: the local header at
    // the front and the central directory at the back are both seeded, so the
    // zip opens and the extraction gets going before it stalls.
    let (handle, base, info_hash) = archive_member_server(
        config_dir.path(),
        cache_dir.path(),
        src.path(),
        MEMBER,
        MEMBER_LEN,
        Some(128 * 1024..192 * 1024),
    )?;
    let client = bearer_client(&handle)?;

    // The control: a torrent of the same server that nobody reads.
    let idle_content = src.path().join("Idle");
    std::fs::create_dir_all(&idle_content)?;
    write_payload(&idle_content.join("idle.bin"), 16 * 1024);
    let (idle_torrent, idle_hash) = real_torrent(&idle_content);
    client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&idle_torrent) }))
        .send()?
        .error_for_status()?;
    stats_after_check(&client, &base, &idle_hash)?;

    // The read. The response arrives -- there are bytes to send before the
    // hole -- and the body is then left open, unread, for the rest of the
    // test.
    let anonymous = reqwest::blocking::Client::new();
    let response = anonymous
        .get(archive_member_url(&base, &info_hash, MEMBER))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let opened = std::time::Instant::now();

    // Seeding off, which used to be half of what armed the arm and is now
    // neither here nor there: a torrent nobody is playing is stopped either
    // way. Left in as the setting a viewer of this feature would have.
    handle.update_settings(serde_json::json!({ "seedingEnabled": false }))?;

    // Wait until both are true: the control torrent has been stopped, and
    // enough ticks have passed that a torrent this one's size would have
    // been stopped several times over.
    let would_have_stopped = opened + 3 * enginefs::FREE_SPACE_WATCH_INTERVAL;
    let deadline = std::time::Instant::now() + CHECK_WAIT_BOUND;
    loop {
        if swarm_paused(&client, &base, &idle_hash)?
            && std::time::Instant::now() >= would_have_stopped
        {
            break;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the reconciler never stopped the torrent nobody was reading"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(
        !swarm_paused(&client, &base, &info_hash)?,
        "the torrent an archive body is still reading from was stopped under it"
    );

    drop(response);
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// And the torrent an archive member was read out of is stopped again once
/// something else is the one being played.
///
/// The read is a playback: `routes::archive::stream_file` tells the server
/// a stream opened, which makes that torrent the entity being played and
/// so one the ladder keeps running -- with no clock on it, so a viewer who
/// pauses keeps it. What ends that is not the body ending but *another*
/// stream opening, and from that moment the torrent behind the archive is
/// one nobody is playing: stopped, and its cache the retention owner's.
///
/// So there are two claims here, and the second is the one that is easy to
/// leave out of the code. The first is the switch: the pair of readings
/// either side of it, which is the policy a viewer can see. The second is
/// still `TorrentMemberStream::drop`'s spawned `on_stream_end` -- a `Drop`
/// cannot await the async locks itself -- and without it every archive
/// member ever read leaves a stream registered for the life of the process.
/// That no longer stops the reconciler from doing anything (what it reads
/// is the cell), so the oracle for it is the register itself, on the wire
/// the embedder reads it off: `background_traffic().playing` is
/// `enginefs::EngineFS::playback_is_live`, which is the open file readers
/// **or** the stream registrations. The reader goes with the body either
/// way, so a light that goes out when the body ends is one where the
/// registration was ended too.
#[test]
fn an_archive_member_read_lets_the_torrent_be_stopped_again_when_it_is_done() -> anyhow::Result<()>
{
    const MEMBER: &str = "member.bin";
    const MEMBER_LEN: usize = 64 * 1024;
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;

    let (handle, base, info_hash) = archive_member_server(
        config_dir.path(),
        cache_dir.path(),
        src.path(),
        MEMBER,
        MEMBER_LEN,
        None,
    )?;
    let client = bearer_client(&handle)?;

    // A second torrent, seeded whole, for the viewer to move on to.
    let other_content = src.path().join("Other");
    std::fs::create_dir_all(&other_content)?;
    write_payload(&other_content.join("other.bin"), 64 * 1024);
    let (other_torrent, other_hash) = real_torrent(&other_content);
    let cache_root = resolved(&cache_dir.path().join("cache"));
    seed_piece_store(&cache_root, &other_torrent, &other_content);
    client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&other_torrent) }))
        .send()?
        .error_for_status()?;
    let other_stats = stats_after_check(&client, &base, &other_hash)?;
    let other_idx = file_index(&other_stats, "other.bin");

    // The member, read to its end out of the torrent.
    let anonymous = reqwest::blocking::Client::new();
    let response = anonymous
        .get(archive_member_url(&base, &info_hash, MEMBER))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.bytes()?.as_ref(),
        member_payload(MEMBER_LEN).as_slice(),
        "the member served out of the torrent"
    );
    std::thread::sleep(enginefs::reconcile::RECONCILE_INTERVAL * 3);
    assert!(
        !swarm_paused(&client, &base, &info_hash)?,
        "the read left the torrent running, and stopping reading is not \
         playing something else"
    );

    // The body is over, so the registration it made is over: the drop
    // spawns the `on_stream_end` that ends it, and nothing else ever will.
    // Bounded rather than immediate because that end is spawned.
    let deadline = std::time::Instant::now() + CHECK_WAIT_BOUND;
    while handle.background_traffic()?.playing {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the finished archive read left a stream registered: this \
             server still says a player is reading from it"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // The viewer moves on: the second torrent is the one being played now.
    anonymous
        .get(format!("{base}/{other_hash}/{other_idx}"))
        .send()?
        .error_for_status()?;
    let deadline = std::time::Instant::now() + CHECK_WAIT_BOUND;
    while !swarm_paused(&client, &base, &info_hash)? {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the torrent the viewer left is still running"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// A stream request below the free-space floor is refused with a `507`
/// once a retention pass has had its chance -- not "degraded to memory-only",
/// which re-selected the same disk-backed engine and streamed to the disk
/// the check had just refused.
///
/// The refusal is keyed on what the engine's reconciler keys its free-space
/// arm on, and on nothing else: **a torrent that still wants bytes**, on a
/// volume under the floor. So the two halves are tested apart. A torrent
/// that has everything it wants writes nothing, and is served from the disk
/// it has already filled -- refusing it would take a finished film off a
/// device at exactly the moment its owner wanted to watch something without
/// downloading anything, and the engine would not have stopped that torrent
/// either. A torrent that still has data to fetch is the one the floor is
/// about, and it is refused.
///
/// A volume cannot be filled on demand, so the reading is declared through
/// `pretend_available_space`, keyed by this test's own cache root so no
/// other server in the run sees it.
#[test]
fn a_stream_below_the_free_space_floor_is_refused_not_degraded() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let (handle, base, info_hash, idx, payload) =
        lan_media_server(config_dir.path(), cache_dir.path(), src.path(), None)?;
    let cache_root = resolved(&cache_dir.path().join("cache"));
    let client = bearer_client(&handle)?;
    let anonymous = reqwest::blocking::Client::new();
    let url = format!("{base}/{info_hash}/{idx}");
    // The finished torrent is pinned, at once, because this test is about
    // the floor and not about retention: a torrent nobody is playing and
    // nobody has pinned is one the retention owner empties within a tick,
    // and an emptied torrent is not a finished one any more.
    let pinned = handle.pin_download(&info_hash, idx, &[])?;
    assert!(pinned.complete, "the fixture's finished file is kept whole");

    // A second torrent, whose data is deliberately *not* seeded into the
    // cache: it still wants every byte it has, which is what the floor is
    // about.
    let wanting = src.path().join("Wanted");
    std::fs::create_dir_all(&wanting)?;
    write_payload(&wanting.join("wanted.bin"), 64 * 1024);
    let (wanting_torrent, wanting_hash) = real_torrent(&wanting);
    client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&wanting_torrent) }))
        .send()?
        .error_for_status()?;
    let wanting_stats = stats_after_check(&client, &base, &wanting_hash)?;
    let wanting_idx = file_index(&wanting_stats, "wanted.bin");
    let wanting_url = format!("{base}/{wanting_hash}/{wanting_idx}");

    // Nothing free: refused, whatever the range, with a fixed body -- the
    // check's own message names the cache root, and no response may.
    stream_server::pretend_available_space(&cache_root, 0);
    for range in [None, Some("bytes=0-1023")] {
        let mut request = anonymous.get(&wanting_url);
        if let Some(range) = range {
            request = request.header(reqwest::header::RANGE, range);
        }
        let response = request.send()?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::INSUFFICIENT_STORAGE,
            "range {range:?}"
        );
        assert_eq!(
            response.text()?,
            "Insufficient disk space for this stream; free some space and retry",
            "range {range:?}"
        );
    }

    // The torrent that has everything it wants is served from the same full
    // volume: it writes nothing, so there is nothing for the floor to
    // protect against.
    //
    let response = anonymous.get(&url).send()?.error_for_status()?;
    assert_eq!(response.bytes()?.as_ref(), payload.as_slice());

    // A HEAD probe writes nothing and is not refused either: the player
    // learns the length and the range support, and the GET that follows is
    // what the floor is judged on.
    let response = anonymous.head(&wanting_url).send()?.error_for_status()?;
    assert_eq!(
        header_value(&response, "content-length"),
        (64 * 1024).to_string()
    );

    // Room again: the same request is no longer refused, and it is the
    // same engine it always was -- there was never another.
    stream_server::pretend_available_space(&cache_root, u64::MAX);
    let refused_again = anonymous
        .get(&wanting_url)
        .header(reqwest::header::RANGE, "bytes=0-1023")
        .timeout(std::time::Duration::from_secs(2))
        .send();
    match refused_again {
        Ok(response) => assert_ne!(
            response.status(),
            reqwest::StatusCode::INSUFFICIENT_STORAGE,
            "with room on the volume the floor refuses nothing"
        ),
        // No peer will ever bring these bytes, so a request that got past
        // the floor waits for them until this client gives up -- which is
        // itself the proof that it was not refused.
        Err(error) => assert!(error.is_timeout(), "{error}"),
    }

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **A stream refused for space takes back what the viewer just left, and
/// asks the volume again.**
///
/// This request is the switch: registering the stream is what makes the
/// film the viewer was watching disposable, and those bytes are ours. So a
/// gate that answered `507` from the reading it took first would refuse a
/// stream for room it was about to have -- which on a full phone is every
/// switch, for ever. It drops both cache owners' slack instead, re-reads
/// the volume, and refuses only what is still short.
///
/// And when it is still short, it refuses. Nothing below the slack is ever
/// taken: no pin, no window somebody is inside. When what is left does not
/// fit, `507` is the answer and a viewer who wants that stream unpins
/// something -- and the answer comes back rather than hanging on the disk
/// (`SLACK_DROP_BOUND`).
///
/// The volume is declared as a *sequence* (`pretend_available_space_readings`),
/// which is the only honest way to say "it read short, and by the time it
/// was read again the bytes had come back": flipping one declared number
/// from another thread would pin the race rather than the rule.
#[test]
fn a_stream_refused_for_space_frees_the_slack_and_asks_again() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let (handle, base, info_hash, idx, payload) =
        lan_media_server(config_dir.path(), cache_dir.path(), src.path(), None)?;
    let cache_root = resolved(&cache_dir.path().join("cache"));
    let client = bearer_client(&handle)?;
    let anonymous = reqwest::blocking::Client::new();

    // The film the viewer is watching. It is the live one when its response
    // ends, so its bytes stay on the disk until something else opens.
    let response = anonymous
        .get(format!("{base}/{info_hash}/{idx}"))
        .send()?
        .error_for_status()?;
    assert_eq!(response.bytes()?.as_ref(), payload.as_slice());
    let watched = pieces_held(&cache_root, &info_hash);
    assert!(watched > 0, "the film is on the disk to be given back");

    // A second torrent, whose data is deliberately not in the cache: it
    // still wants every byte of itself, which is what the floor is about.
    let wanting = src.path().join("Wanted");
    std::fs::create_dir_all(&wanting)?;
    write_payload(&wanting.join("wanted.bin"), 64 * 1024);
    let (wanting_torrent, wanting_hash) = real_torrent(&wanting);
    client
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&wanting_torrent) }))
        .send()?
        .error_for_status()?;
    let wanting_stats = stats_after_check(&client, &base, &wanting_hash)?;
    let wanting_idx = file_index(&wanting_stats, "wanted.bin");
    let wanting_url = format!("{base}/{wanting_hash}/{wanting_idx}");

    // Nothing free when the gate first asks; room by the time it asks
    // again, which is what dropping the slack did.
    stream_server::pretend_available_space_readings(&cache_root, vec![0, u64::MAX]);
    match anonymous
        .get(&wanting_url)
        .timeout(std::time::Duration::from_secs(3))
        .send()
    {
        Ok(response) => assert_ne!(
            response.status(),
            reqwest::StatusCode::INSUFFICIENT_STORAGE,
            "the gate refused a stream for room it had just taken back"
        ),
        // No peer will ever bring these bytes, so a request that got past
        // the gate waits for them until this client gives up -- which is
        // itself the proof that it was not refused.
        Err(error) => assert!(error.is_timeout(), "{error}"),
    }
    assert_eq!(
        pieces_held(&cache_root, &info_hash),
        0,
        "and what it took back was the film the viewer left"
    );

    // Still short however often it is asked, and there is nothing
    // disposable left: the answer is `507`, with the fixed body, and it
    // comes back rather than sitting on the disk.
    stream_server::pretend_available_space(&cache_root, 0);
    let started = std::time::Instant::now();
    let response = anonymous
        .get(&wanting_url)
        .timeout(std::time::Duration::from_secs(30))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::INSUFFICIENT_STORAGE);
    assert_eq!(
        response.text()?,
        "Insufficient disk space for this stream; free some space and retry"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "the refusal waited out the whole slack-drop bound: {:?}",
        started.elapsed()
    );

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// `stats.json` reports the piece the open reader is waiting on, in bytes.
///
/// Whole verified pieces are all the have-bitfield can show, and a piece on
/// a real torrent is 8-16 MiB -- bigger than the startup window -- so a
/// player waiting on its first piece could only ever be shown 0% or 100%.
/// `inFlightPiece` is the sub-piece view: `downloadedBytes` of
/// `totalBytes`, plus `verified`, which is the only field that means the
/// piece can actually be served.
///
/// Absence is a state of its own: before anything opens the file there is
/// no reader and so no piece anybody waits on, and that must read as `null`
/// rather than as a piece with nothing downloaded. The library API sees
/// exactly what the route serves.
#[test]
fn stats_json_reports_the_piece_the_open_reader_waits_for() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let (handle, base, info_hash, idx, payload) =
        lan_media_server(config_dir.path(), cache_dir.path(), src.path(), None)?;
    let client = bearer_client(&handle)?;

    // Polling stats is not opening a stream: nothing is in flight yet.
    let stats = file_stats_after_check(&client, &base, &info_hash, idx)?;
    assert_eq!(stats["inFlightPiece"], serde_json::Value::Null, "{stats}");
    let piece_length = stats["pieceLength"].as_u64().expect("metadata resolved");
    // The file's own offset decides which torrent piece its head is in, and
    // the fixture does not control the torrent's file order.
    let file_offset = stats["files"][idx]["offset"].as_u64().expect("file offset");
    assert_eq!(
        serde_json::to_value(handle.file_stats(&info_hash, idx, &[])?)?["inFlightPiece"],
        serde_json::Value::Null,
        "the library API reports the same absence"
    );

    // A `Range` request opens a reader at the file's head, which is what
    // makes a piece the one being waited for.
    let anonymous = reqwest::blocking::Client::new();
    let response = anonymous
        .get(format!("{base}/{info_hash}/{idx}"))
        .header(reqwest::header::RANGE, "bytes=0-15")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.bytes()?.as_ref(), &payload[0..16]);

    let stats = file_stats_after_check(&client, &base, &info_hash, idx)?;
    let piece = stats["inFlightPiece"]
        .as_object()
        .unwrap_or_else(|| panic!("a reader is open: {stats}"));
    assert_eq!(piece["index"], file_offset / piece_length, "{stats}");
    assert_eq!(piece["totalBytes"], piece_length, "{stats}");
    // The fixture is pre-seeded, so the piece is on disk and hash-checked:
    // the one state in which a client may treat it as ready.
    assert_eq!(piece["downloadedBytes"], piece_length, "{stats}");
    assert_eq!(piece["verified"], true, "{stats}");
    // Per file as well as at the top level, for the same file.
    assert_eq!(stats["files"][idx]["inFlightPiece"], stats["inFlightPiece"]);

    // Library API == route, for the field and the whole file list.
    let api = serde_json::to_value(handle.file_stats(&info_hash, idx, &[])?)?;
    for key in ["inFlightPiece", "pieceLength", "files"] {
        assert_eq!(api[key], stats[key], "{key}");
    }

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// The LAN media listener hands out media bytes and nothing else.
///
/// It exists so a Chromecast can fetch a stream from a server that otherwise
/// binds loopback only. What it must NOT expose is the control API: the
/// control router is not mounted on it at all, so a control path is an
/// unknown path there -- `404`, never the `401` that would tell the LAN the
/// route exists and only a token is missing. The loopback listener keeps
/// serving both, and range and HEAD requests -- what a receiver actually
/// issues -- work through the LAN listener too.
///
/// It must also NOT expose anything a stranger on the network could make
/// this device *do*: `/proxy` and `/ftp` fetch an arbitrary caller-supplied
/// remote URL; the archive `/create` routes download an archive from a
/// caller-named URL; and the loopback stream route's first request for a hash starts a torrent
/// with the caller's trackers. All of that is loopback only. The LAN gets the
/// byte-serving halves alone -- a torrent that exists, a member of an
/// archive session loopback already created -- and an unknown torrent
/// there is a `404` that starts nothing. The `/local-addon` stub is not on
/// the LAN either: no receiver calls it.
#[test]
fn lan_media_listener_serves_media_but_no_control_route() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let (handle, base, info_hash, idx, payload) = lan_media_server(
        config_dir.path(),
        cache_dir.path(),
        src.path(),
        // Port 0: the OS picks, so any number of these run in parallel.
        Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0))),
    )?;

    let lan_addr = start_lan_media(&handle)?;
    assert!(handle.lan_media_running());
    assert_ne!(
        lan_addr,
        handle.http_addr(),
        "the LAN media listener is a second socket, not a rebind of the first"
    );
    let lan = format!("http://{lan_addr}");

    let anonymous = reqwest::blocking::Client::new();
    let with_token = bearer_client(&handle)?;
    for path in [
        "/heartbeat",
        "/stats.json",
        "/settings",
        "/network-info",
        "/device-info",
        "/downloads.json",
        "/cache.json",
        "/stream-numbers.json?url=/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/0",
        "/get-https?ipAddress=127.0.0.1",
        "/casting",
    ] {
        for client in [&anonymous, &with_token] {
            let response = client.get(format!("{lan}{path}")).send()?;
            assert_eq!(
                response.status(),
                reqwest::StatusCode::NOT_FOUND,
                "{path} must not exist on the LAN listener"
            );
            assert!(
                response
                    .headers()
                    .get(reqwest::header::WWW_AUTHENTICATE)
                    .is_none(),
                "{path} must not even advertise that a token would help"
            );
        }
        // The same path on the loopback listener is a control route that
        // exists: it answers the request rather than the 404 fallback.
        // (`/get-https` is a 400 without its `authKey`; what matters is that
        // it is not a 404.)
        let response = with_token.get(format!("{base}{path}")).send()?;
        assert_ne!(response.status(), reqwest::StatusCode::NOT_FOUND, "{path}");
        assert_ne!(
            response.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "{path}"
        );
    }
    // `POST /settings` is not a route on the LAN listener either -- the
    // fallback answers whatever the method.
    assert_eq!(
        anonymous
            .post(format!("{lan}/settings"))
            .json(&serde_json::json!({ "cacheSize": 1.0 }))
            .send()?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    // `POST /cache/clean` is not a route on the LAN listener either -- but
    // its two segments match the stream route's `/{infoHash}/{fileIdx}`
    // pattern (`"cache"`/`"clean"` parse as neither, so it would 404 on
    // content, but routing happens on shape first), and that route only
    // answers GET/HEAD, so this is the same collision as
    // `/{infoHash}/create` below: `405`, not `404`, with the control
    // handler still never reached.
    assert_eq!(
        anonymous
            .post(format!("{lan}/cache/clean"))
            .send()?
            .status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED
    );
    // `POST /proxy-streams/{token}/close` ends a player's proxied stream.
    // The LAN listener serves no `/proxy` and no control route, so there is
    // nothing there to close and no route to ask: cutting another device's
    // playback is not something to hand the network.
    assert_eq!(
        anonymous
            .post(format!("{lan}/proxy-streams/player-one/close"))
            .send()?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    // `POST /{infoHash}/create` collides with the two-segment media route's
    // pattern, so it answers as that route would (`405`, GET and HEAD only)
    // rather than 404 -- but it still never reaches the control handler, and
    // no engine is created.
    assert_eq!(
        anonymous
            .post(format!("{lan}/{info_hash}/create"))
            .send()?
            .status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED
    );

    // `/proxy` and `/ftp` are open on the loopback listener too -- players
    // cannot attach headers, same as every other media route -- but neither
    // serves bytes *from this server*: each fetches an arbitrary
    // caller-supplied remote URL (`/proxy` via `reqwest`, `/ftp` via
    // `reqwest` or a spawned `curl`), which makes it an open proxy rather
    // than "media bytes". The
    // LAN listener's allow-list (`lan_media_routes`) excludes both. The
    // requests below are malformed just enough to prove the routing
    // decision (an invalid target URL, a missing `lz` parameter) without
    // either handler ever reaching out over the network.
    for path in ["/proxy/not-a-url", "/ftp/movie.mkv"] {
        let response = anonymous.get(format!("{lan}{path}")).send()?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::NOT_FOUND,
            "{path} must not exist on the LAN listener -- it is an open proxy, not media bytes"
        );
        let response = anonymous.get(format!("{base}{path}")).send()?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "{path} is a real route on the loopback listener"
        );
    }

    // The archive session-create routes fetch a caller-named URL (an
    // archive to download whole), so they are loopback only: a real route there (a
    // `400` for the missing payload) and absent from the LAN. `GET` on the
    // LAN is the two-segment collision answered by the LAN stream handler
    // looking `"rar"` up as an info hash and finding nothing; the keyed
    // form has three segments and reaches the fallback; `POST` is a method
    // the stream route does not take.
    for prefix in ["/rar", "/zip", "/7zip", "/tar", "/tgz"] {
        let create = format!("{prefix}/create");
        assert_eq!(
            anonymous.get(format!("{base}{create}")).send()?.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "{create} is a real route on the loopback listener"
        );
        assert_eq!(
            anonymous.get(format!("{lan}{create}")).send()?.status(),
            reqwest::StatusCode::NOT_FOUND,
            "{create} must not exist on the LAN listener"
        );
        assert_eq!(
            anonymous
                .get(format!("{lan}{create}/some-key"))
                .send()?
                .status(),
            reqwest::StatusCode::NOT_FOUND,
            "{create}/some-key must not exist on the LAN listener"
        );
        assert_eq!(
            anonymous
                .post(format!("{lan}{create}"))
                .json(&serde_json::json!({ "urls": ["http://127.0.0.1:9/x.zip"] }))
                .send()?
                .status(),
            reqwest::StatusCode::METHOD_NOT_ALLOWED,
            "POST {create} must not reach a handler on the LAN listener"
        );
        // The byte-serving half of the same prefix *is* on the LAN: a
        // request with no session key is refused by the route itself, on
        // both listeners alike, and a key nobody created is a `404` that
        // opened nothing.
        let stream = format!("{prefix}/stream");
        for origin in [&lan, &base] {
            assert_eq!(
                anonymous.get(format!("{origin}{stream}")).send()?.status(),
                reqwest::StatusCode::BAD_REQUEST,
                "{origin}{stream} is a real route on both listeners"
            );
            assert_eq!(
                anonymous
                    .get(format!("{origin}{stream}/no-such-session/movie.mkv"))
                    .send()?
                    .status(),
                reqwest::StatusCode::NOT_FOUND,
                "{origin}{stream}/no-such-session/movie.mkv"
            );
        }
    }
    // The `/local-addon` stub is loopback only too -- not a hazard, just
    // nothing a receiver asks for, and the allow-list is what a receiver
    // needs rather than what is harmless.
    let manifest: serde_json::Value = anonymous
        .get(format!("{base}/local-addon/manifest.json"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(manifest["id"], "org.stremio.local");
    assert_eq!(
        anonymous
            .get(format!("{lan}/local-addon/manifest.json"))
            .send()?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );

    // A torrent this server does not have is a `404` on the LAN, at once,
    // and nothing is started: on loopback the same request would create the
    // torrent with the caller's `tr=` trackers and wait up to the metadata
    // timeout for a swarm that does not exist. The stats poll afterwards
    // starts its own registry add for the hash -- that is what a stats poll
    // does -- so what it proves is that the LAN request left no add of its
    // own behind: the attacker's tracker is in no source list.
    let unknown = "00112233445566778899aabbccddeeff00112233";
    let attacker_tracker = "udp://attacker.invalid:6969/announce";
    let prompt = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    for method in [reqwest::Method::GET, reqwest::Method::HEAD] {
        let response = prompt
            .request(
                method.clone(),
                format!(
                    "{lan}/{unknown}/0?tr={}",
                    urlencoding::encode(attacker_tracker)
                ),
            )
            .send()?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::NOT_FOUND,
            "{method} for an unknown torrent on the LAN listener"
        );
    }
    let response = prompt
        .get(format!(
            "{lan}/stream/{unknown}/0?tr={}",
            urlencoding::encode(attacker_tracker)
        ))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    let stats: serde_json::Value = with_token
        .get(format!("{base}/{unknown}/stats.json"))
        .send()?
        .error_for_status()?
        .json()?;
    let sources = stats["sources"].to_string();
    assert!(
        !sources.contains("attacker.invalid"),
        "the LAN request started an add carrying its own tracker: {stats}"
    );

    // Media bytes, on both listeners, from the same engine.
    for origin in [&lan, &base] {
        let response = anonymous
            .get(format!("{origin}/{info_hash}/{idx}"))
            .send()?
            .error_for_status()?;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(
            header_value(&response, "accept-ranges"),
            "bytes",
            "{origin}"
        );
        assert_eq!(response.bytes()?.as_ref(), payload.as_slice(), "{origin}");
    }

    // A receiver seeks with byte ranges, so the LAN listener has to answer
    // `206` with a `Content-Range` and exactly the requested bytes.
    let response = anonymous
        .get(format!("{lan}/{info_hash}/{idx}"))
        .header(reqwest::header::RANGE, "bytes=1024-2047")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        header_value(&response, "content-range"),
        format!("bytes 1024-2047/{}", payload.len())
    );
    assert_eq!(header_value(&response, "content-length"), "1024");
    assert_eq!(response.bytes()?.as_ref(), &payload[1024..2048]);

    // And it probes with HEAD first: the headers, no body.
    let response = anonymous
        .head(format!("{lan}/{info_hash}/{idx}"))
        .send()?
        .error_for_status()?;
    assert_eq!(
        header_value(&response, "content-length"),
        payload.len().to_string()
    );
    assert_eq!(header_value(&response, "accept-ranges"), "bytes");
    assert!(response.bytes()?.is_empty());

    // CORS reaches the LAN listener as well -- a receiver preflights the
    // media request before it fetches a byte.
    let response = anonymous
        .request(reqwest::Method::OPTIONS, format!("{lan}/{info_hash}/{idx}"))
        .header(reqwest::header::ORIGIN, "https://example.org")
        .header("access-control-request-method", "GET")
        .header("access-control-request-headers", "range")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(header_value(&response, "access-control-allow-origin"), "*");
    let allowed = header_value(&response, "access-control-allow-headers");
    for header in ["accept-encoding", "content-type", "range"] {
        assert!(allowed.contains(header), "{header:?} in {allowed:?}");
    }
    let response = anonymous
        .get(format!("{lan}/{info_hash}/{idx}"))
        .header(reqwest::header::ORIGIN, "https://example.org")
        .header(reqwest::header::RANGE, "bytes=0-15")
        .send()?;
    let exposed = header_value(&response, "access-control-expose-headers");
    for header in ["accept-ranges", "content-length", "content-range"] {
        assert!(exposed.contains(header), "{header:?} in {exposed:?}");
    }

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// `set_lan_media` is the per-cast-session switch, and `lanMediaEnabled` is
/// the operator's veto over it.
///
/// Stopping is an abort: the socket is closed by the time the call returns.
/// It must leave the loopback listener alone, so a media request loop runs
/// against loopback across every toggle and every one of them has to succeed.
#[test]
fn set_lan_media_toggles_the_listener_and_the_setting_can_forbid_it() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let (handle, base, info_hash, idx, payload) = lan_media_server(
        config_dir.path(),
        cache_dir.path(),
        src.path(),
        Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0))),
    )?;

    // Loopback media, hammered from another thread for as long as the
    // toggling below takes. Not a timing assertion -- it just has to be
    // in flight while the LAN listener starts and stops.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let probe = {
        let stop = stop.clone();
        let url = format!("{base}/{info_hash}/{idx}");
        let expected = payload[..1024].to_vec();
        std::thread::spawn(move || -> anyhow::Result<u64> {
            let client = reqwest::blocking::Client::new();
            let mut served = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let response = client
                    .get(&url)
                    .header(reqwest::header::RANGE, "bytes=0-1023")
                    .send()?;
                anyhow::ensure!(
                    response.status() == reqwest::StatusCode::PARTIAL_CONTENT,
                    "loopback media answered {} while the LAN listener was toggling",
                    response.status()
                );
                anyhow::ensure!(response.bytes()?.as_ref() == expected.as_slice());
                served += 1;
            }
            Ok(served)
        })
    };

    // Nothing is bound at startup, however the address is configured: the
    // listener is a cast session's, and there is none yet.
    let peer = std::net::IpAddr::from([127, 0, 0, 1]);
    assert!(!handle.lan_media_running());
    assert_eq!(handle.lan_media_addr(), None);
    assert_eq!(handle.lan_media_base_url(peer), None);

    // Starting is refused while the setting forbids it (the default).
    assert!(!handle.settings()?.lan_media_enabled);
    let error = handle.set_lan_media(true).unwrap_err().to_string();
    assert_eq!(
        error,
        "the lanMediaEnabled setting forbids the LAN media listener; \
         set it through POST /settings (or update_settings) first",
        "the whole sentence, so a lost line continuation cannot leave a gap in it"
    );
    assert!(!handle.lan_media_running());

    // Permitted, it starts and advertises the address it bound.
    handle.update_settings(serde_json::json!({ "lanMediaEnabled": true }))?;
    let started = handle.set_lan_media(true)?.expect("bound");
    assert!(handle.lan_media_running());
    assert_eq!(handle.lan_media_addr(), Some(started));
    assert_eq!(
        handle.lan_media_base_url(peer).map(|url| url.to_string()),
        Some(format!("http://{started}/")),
        "a listener bound to one address advertises that address"
    );
    assert_eq!(
        handle.set_lan_media(true)?,
        Some(started),
        "starting an already-running listener is a no-op"
    );

    // Stop: the socket is gone when the call returns, and so is the URL.
    assert_eq!(handle.set_lan_media(false)?, None);
    assert!(!handle.lan_media_running());
    assert_eq!(handle.lan_media_addr(), None);
    assert_eq!(handle.lan_media_base_url(peer), None);
    let refused = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?
        .get(format!("http://{started}/{info_hash}/{idx}"))
        .send();
    assert!(
        refused.is_err(),
        "the stopped LAN listener still answered: {refused:?}"
    );

    // Still permitted, it comes back -- on a fresh OS-assigned port -- and
    // serves media again.
    let restarted = handle.set_lan_media(true)?.expect("bound");
    assert!(handle.lan_media_running());
    assert_eq!(handle.lan_media_addr(), Some(restarted));
    let response = reqwest::blocking::Client::new()
        .get(format!("http://{restarted}/{info_hash}/{idx}"))
        .send()?
        .error_for_status()?;
    assert_eq!(response.bytes()?.as_ref(), payload.as_slice());

    // Revoking the setting stops a listener that is already running --
    // otherwise "forbid it" would only mean "forbid the next one".
    handle.update_settings(serde_json::json!({ "lanMediaEnabled": false }))?;
    assert!(!handle.lan_media_running());
    assert_eq!(handle.lan_media_base_url(peer), None);

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let served = probe
        .join()
        .map_err(|_| anyhow::anyhow!("loopback probe thread panicked"))??;
    assert!(
        served > 0,
        "the loopback probe never completed a request, so it proved nothing"
    );

    // And the loopback listener still serves control routes too.
    let heartbeat: serde_json::Value = bearer_client(&handle)?
        .get(format!("{base}/heartbeat"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(heartbeat["success"], true);

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// A certificate on disk that will not load costs the HTTPS listener and
/// nothing else: the server starts and serves plain HTTP. It used to refuse
/// to start at all, at every launch, over the remote-access feature.
#[test]
fn a_certificate_that_will_not_load_leaves_the_plain_server_up() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let config_path = config_dir.path().join("config");
    std::fs::create_dir_all(&config_path)?;
    std::fs::write(config_path.join("https-cert.pem"), "not a certificate")?;
    std::fs::write(config_path.join("https-key.pem"), "not a key")?;

    let handle = stream_server::start(ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        https_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0))),
        config_dir: Some(config_path),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..offline_config()
    })?;
    assert_eq!(handle.https_addr(), None);
    let heartbeat: serde_json::Value = bearer_client(&handle)?
        .get(format!("http://{}/heartbeat", handle.http_addr()))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(heartbeat["success"], true);

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// `/get-https` answers with a port a TLS handshake succeeds on, because the
/// listener behind the answer is started by the same call that writes the
/// certificate -- not the plain-HTTP port, and not a port that only exists
/// after a restart.
///
/// The fetch from Stremio's API cannot run here, so the test drives the
/// serving half through the library method that shares it,
/// `install_https_certificate`, with the throwaway self-signed PEMs beside
/// this file: nothing is bound before a certificate exists; installing one
/// binds the configured address and a TLS client reaches the control API
/// there with the bearer token; a restart on the same config dir finds the
/// certificate and comes back up on its own; and a server with no HTTPS
/// address configured refuses -- the route with `501` before any network
/// call, the method with an error -- rather than writing a key nothing
/// would serve.
#[test]
fn get_https_serves_the_certificate_on_the_port_it_answers_with() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let config = || ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        https_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0))),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..offline_config()
    };
    let cert = include_str!("selfsigned-cert.pem");
    let key = include_str!("selfsigned-key.pem");
    // The fixture is self-signed, so the client has to be told to accept
    // it; what the handshake proves is that *this* certificate is served.
    let tls_client = |handle: &ServerHandle| -> anyhow::Result<reqwest::blocking::Client> {
        Ok(bearer_client_builder(handle)
            .danger_accept_invalid_certs(true)
            .build()?)
    };

    let handle = stream_server::start(config())?;
    assert_eq!(
        handle.https_addr(),
        None,
        "no certificate yet, so nothing to serve it on"
    );

    let bound = handle.install_https_certificate(cert, key)?;
    assert_eq!(handle.https_addr(), Some(bound));
    assert_ne!(
        bound,
        handle.http_addr(),
        "the HTTPS listener is its own socket, not the plain one renamed"
    );
    let heartbeat: serde_json::Value = tls_client(&handle)?
        .get(format!("https://{bound}/heartbeat"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(heartbeat["success"], true);
    // And it is the control API, behind the same token.
    assert_eq!(
        reqwest::blocking::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()?
            .get(format!("https://{bound}/heartbeat"))
            .send()?
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    // Installing again -- a renewed certificate -- restarts the listener
    // and still answers with a port that serves.
    let renewed = handle.install_https_certificate(cert, key)?;
    assert_eq!(handle.https_addr(), Some(renewed));
    tls_client(&handle)?
        .get(format!("https://{renewed}/heartbeat"))
        .send()?
        .error_for_status()?;
    handle.shutdown()?;
    handle.join()?;

    // A restart on the same config dir finds the certificate on disk and
    // serves it from the start.
    let handle = stream_server::start(config())?;
    let restarted = handle
        .https_addr()
        .expect("the certificate is on disk, so the listener is up at boot");
    tls_client(&handle)?
        .get(format!("https://{restarted}/heartbeat"))
        .send()?
        .error_for_status()?;
    handle.shutdown()?;
    handle.join()?;

    // No HTTPS address configured -- the embedded default -- is a refusal,
    // before any certificate is written and before any network call.
    let plain_config_dir = tempfile::tempdir()?;
    let plain_cache_dir = tempfile::tempdir()?;
    let handle = stream_server::start(ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(plain_config_dir.path().join("config")),
        cache_dir: Some(plain_cache_dir.path().join("cache")),
        ..offline_config()
    })?;
    assert_eq!(ServerConfig::embedded().https_addr, None);
    let error = handle
        .install_https_certificate(cert, key)
        .unwrap_err()
        .to_string();
    assert!(error.contains("https_addr"), "{error}");
    assert!(
        !plain_config_dir
            .path()
            .join("config")
            .join("https-key.pem")
            .exists(),
        "no key is written for a listener that can never run"
    );
    let response = bearer_client(&handle)?
        .get(format!(
            "http://{}/get-https?authKey=not-a-real-key&ipAddress=127.0.0.1",
            handle.http_addr()
        ))
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_IMPLEMENTED);
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// A configured LAN media address is a place, not a running listener: the
/// server comes up with nothing bound there, so the persisted veto is never
/// bypassed by the boot, and a port already in use is the cast caller's
/// error rather than the server's.
///
/// The address is pre-bound by this test, which is what the fixed port an
/// embedder might configure runs into when a previous instance -- or any
/// other program -- still holds it. Once the port is free the same handle
/// starts the listener without a restart.
#[test]
fn a_configured_lan_media_address_binds_nothing_until_a_cast_asks() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let taken = std::net::TcpListener::bind("127.0.0.1:0")?;
    let contested = taken.local_addr()?;
    let (handle, base, info_hash, idx, payload) = lan_media_server(
        config_dir.path(),
        cache_dir.path(),
        src.path(),
        Some(contested),
    )?;

    // The server is up and serving, on loopback, with the LAN address held
    // by somebody else the whole time.
    assert!(!handle.lan_media_running());
    assert_eq!(handle.lan_media_addr(), None);
    let heartbeat: serde_json::Value = bearer_client(&handle)?
        .get(format!("{base}/heartbeat"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(heartbeat["success"], true);

    // Asking for the listener while the port is taken fails the ask, and
    // only the ask.
    let error = start_lan_media(&handle).unwrap_err().to_string();
    assert!(
        error.contains(&format!(
            "failed to bind the LAN media listener on {contested}"
        )),
        "{error}"
    );
    assert!(!handle.lan_media_running());
    let heartbeat: serde_json::Value = bearer_client(&handle)?
        .get(format!("{base}/heartbeat"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(heartbeat["success"], true);

    // Port released: the same server binds it on the next ask.
    drop(taken);
    let bound = handle
        .set_lan_media(true)?
        .expect("bound once the port is free");
    assert_eq!(bound, contested);
    let response = reqwest::blocking::Client::new()
        .get(format!("http://{bound}/{info_hash}/{idx}"))
        .send()?
        .error_for_status()?;
    assert_eq!(response.bytes()?.as_ref(), payload.as_slice());

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// The LAN listener counts what has reached it, per cast session.
///
/// This is the only way to tell a receiver that never fetched the stream
/// from one that fetched it and could not play it, and the two need
/// different words: the first says this device could not be reached at the
/// address it handed out, the second says nothing about the network at all.
/// A receiver told an unroutable address raises no error -- the connect
/// hangs -- so nothing else distinguishes them.
///
/// The count is therefore per session and not cumulative: every start
/// resets it -- including one that finds the listener already up, which is
/// what casting to a second receiver mid-session does -- and so does a stop,
/// or the previous cast would answer for this one. Only the LAN listener
/// counts; loopback traffic is this host's own client, not a receiver.
#[test]
fn the_lan_listener_counts_the_requests_that_reach_it() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let (handle, base, info_hash, idx, payload) = lan_media_server(
        config_dir.path(),
        cache_dir.path(),
        src.path(),
        Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0))),
    )?;
    let lan_addr = start_lan_media(&handle)?;
    let lan = format!("http://{lan_addr}");
    let anonymous = reqwest::blocking::Client::new();

    assert_eq!(
        handle.lan_media_requests_served(),
        0,
        "a listener nothing has fetched from yet"
    );

    // Loopback is not the LAN listener, however much it is served.
    anonymous
        .get(format!("{base}/{info_hash}/{idx}"))
        .send()?
        .error_for_status()?;
    assert_eq!(
        handle.lan_media_requests_served(),
        0,
        "a request to the loopback listener is not a receiver fetching"
    );

    // What a receiver actually does: probe with HEAD, then read a range.
    // The count is bumped when the request arrives, so it is already up to
    // date by the time the response is in hand -- nothing to wait for.
    anonymous
        .head(format!("{lan}/{info_hash}/{idx}"))
        .send()?
        .error_for_status()?;
    assert_eq!(handle.lan_media_requests_served(), 1);
    let response = anonymous
        .get(format!("{lan}/{info_hash}/{idx}"))
        .header(reqwest::header::RANGE, "bytes=0-15")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.bytes()?.as_ref(), &payload[0..16]);
    assert_eq!(handle.lan_media_requests_served(), 2);

    // A request the fallback answers still reached us, which is the whole
    // question the count exists to answer.
    assert_eq!(
        anonymous
            .get(format!("{lan}/no-such-route"))
            .send()?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    assert_eq!(handle.lan_media_requests_served(), 3);

    // Tapping a second receiver starts a cast on a listener that is already
    // bound, and the question that cast asks is about itself: the first
    // receiver's requests must not answer for it.
    assert_eq!(
        handle.set_lan_media(true)?,
        Some(lan_addr),
        "an already-running listener keeps the socket it has"
    );
    assert_eq!(
        handle.lan_media_requests_served(),
        0,
        "a start resets the count whether or not it had to bind"
    );
    anonymous
        .head(format!("{lan}/{info_hash}/{idx}"))
        .send()?
        .error_for_status()?;
    assert_eq!(handle.lan_media_requests_served(), 1);

    // Nothing is listening once a stop returns, so nothing can have reached
    // us: a count left standing would report a receiver fetching from a
    // socket that is closed.
    handle.set_lan_media(false)?;
    assert!(!handle.lan_media_running());
    assert_eq!(
        handle.lan_media_requests_served(),
        0,
        "zero whenever nothing is listening"
    );

    // A new session starts from nothing, so a cast that is never fetched
    // from reads zero however busy the one before it was.
    let restarted = handle.set_lan_media(true)?.expect("bound again");
    assert_eq!(
        handle.lan_media_requests_served(),
        0,
        "the count belongs to the session, not to the process"
    );
    assert_eq!(
        anonymous
            .get(format!("http://{restarted}/{info_hash}/{idx}"))
            .send()?
            .error_for_status()?
            .bytes()?
            .as_ref(),
        payload.as_slice()
    );
    assert_eq!(handle.lan_media_requests_served(), 1);

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// With no `lan_media_addr` configured -- the default for both stock
/// configurations -- there is no LAN listener and nothing to start, whatever
/// the setting says.
#[test]
fn without_a_configured_address_there_is_no_lan_media_listener() -> anyhow::Result<()> {
    assert_eq!(ServerConfig::embedded().lan_media_addr, None);
    assert_eq!(ServerConfig::binary_default().lan_media_addr, None);

    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..offline_config()
    })?;

    assert!(!handle.lan_media_running());
    assert_eq!(
        handle.lan_media_base_url(std::net::IpAddr::from([127, 0, 0, 1])),
        None
    );
    handle.update_settings(serde_json::json!({ "lanMediaEnabled": true }))?;
    let error = handle.set_lan_media(true).unwrap_err().to_string();
    assert!(error.contains("lan_media_addr"), "{error}");
    assert!(!handle.lan_media_running());
    // Stopping something that never ran is fine.
    assert_eq!(handle.set_lan_media(false)?, None);

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// The buffer profile: a server-wide default and a per-request override.
///
/// The setting is `bufferProfile` (`normal` by default), and the stream route
/// takes `?buffer=` alongside its existing parameters. Both are additive, and
/// neither can fail a playback request: a value this build does not know
/// falls back -- the query parameter to the setting, the setting to what it
/// already was -- rather than answering an error to a player that guessed.
#[test]
fn buffer_profile_is_a_setting_and_a_stream_query_override() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let (handle, base, info_hash, idx, payload) =
        lan_media_server(config_dir.path(), cache_dir.path(), src.path(), None)?;
    let client = bearer_client(&handle)?;

    // The default is today's behaviour, and the library agrees with the route.
    let values: serde_json::Value = client
        .get(format!("{base}/settings"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(values["values"]["bufferProfile"], "normal");
    assert_eq!(
        serde_json::to_value(handle.settings()?)?["bufferProfile"],
        "normal"
    );

    // Every profile is accepted, persisted and visible over both surfaces.
    for profile in ["large", "maximum", "normal"] {
        let updated = handle.update_settings(serde_json::json!({ "bufferProfile": profile }))?;
        assert_eq!(serde_json::to_value(&updated)?["bufferProfile"], profile);
        let values: serde_json::Value = client
            .get(format!("{base}/settings"))
            .send()?
            .error_for_status()?
            .json()?;
        assert_eq!(values["values"]["bufferProfile"], profile);
    }
    handle.update_settings(serde_json::json!({ "bufferProfile": "large" }))?;
    let persisted: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        config_dir.path().join("config").join("settings.json"),
    )?)?;
    assert_eq!(persisted["bufferProfile"], "large");

    // An unknown or wrong-typed value leaves the setting as it was, exactly
    // like every other unrecognised value in the payload -- and takes the
    // rest of the payload with it, so the update still applies.
    let updated = handle.update_settings(serde_json::json!({
        "bufferProfile": "gigantic",
        "btMaxConnections": 61,
    }))?;
    assert_eq!(serde_json::to_value(&updated)?["bufferProfile"], "large");
    assert_eq!(updated.bt_max_connections, 61);
    let updated = handle.update_settings(serde_json::json!({ "bufferProfile": 4 }))?;
    assert_eq!(serde_json::to_value(&updated)?["bufferProfile"], "large");

    // The stream route takes the override next to the parameters it already
    // had, and serves the same bytes whichever profile is asked for -- the
    // profile changes how far ahead the engine reads, never the response.
    // An unknown value is not a client error: it falls back to the setting.
    let media = format!("{base}/{info_hash}/{idx}");
    for query in [
        String::new(),
        "?buffer=normal".to_string(),
        "?buffer=large".to_string(),
        "?buffer=MAXIMUM".to_string(),
        "?buffer=gigantic".to_string(),
        "?buffer=".to_string(),
        format!("?tr=udp%3A%2F%2Fone&buffer=large&f={}", "movie.bin"),
    ] {
        let response = client.get(format!("{media}{query}")).send()?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "GET /{{infoHash}}/{{fileIdx}}{query}"
        );
        assert_eq!(response.bytes()?.as_ref(), payload.as_slice(), "{query}");
    }

    // And a ranged request, which is what a player actually issues on a seek.
    let response = client
        .get(format!("{media}?buffer=maximum"))
        .header(reqwest::header::RANGE, "bytes=0-1023")
        .send()?;
    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.bytes()?.len(), 1024);

    Ok(())
}
