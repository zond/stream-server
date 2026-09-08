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
        ..ServerConfig::default()
    }
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

/// Deterministic, non-trivial payload so piece hashes mean something.
fn write_payload(path: &std::path::Path, len: usize) {
    let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    std::fs::write(path, data).expect("write payload");
}

/// A `TempDir`-derived path in the spelling the server answers with.
///
/// `prepare_downloads_dir` stores and reports the *resolved* `downloadsDir`,
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

/// A persisted `downloadsDir` that is unusable at startup (its path is a
/// file now) is cleared in the settings file, not only in memory: an
/// embedder reading the file sees what `settings()` says, and the next
/// boot does not warn about it again.
#[test]
fn unusable_persisted_downloads_dir_is_cleared_on_disk() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let downloads_dir = tempfile::tempdir()?;
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
    let downloads = resolved(&downloads_dir.path().join("offline"));

    let handle = stream_server::start(config())?;
    handle.update_settings(serde_json::json!({ "downloadsDir": downloads.to_str().unwrap() }))?;
    assert_eq!(
        read_persisted()?["downloadsDir"],
        downloads.to_str().unwrap()
    );
    handle.shutdown()?;
    handle.join()?;

    // The directory is a file now: unusable.
    std::fs::remove_dir_all(&downloads)?;
    std::fs::write(&downloads, b"in the way")?;
    let handle = stream_server::start(config())?;
    assert_eq!(handle.settings()?.downloads_dir, None);
    assert_eq!(
        read_persisted()?["downloadsDir"],
        serde_json::Value::Null,
        "cleared in the settings file too"
    );
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// Offline downloads with a `downloadsDir`: a torrent that was streamed
/// first (managed in the cache root, data verified there) is relocated by
/// the pin into `<downloadsDir>/<infoHash>` -- files moved, cache-root
/// folder gone, re-checked in place (`checking`) and complete again --
/// `pin_download` on the handle reports the file's new path, the setting
/// is validated (`POST /settings` semantics) and persisted, and a restart
/// on the same dirs restores the torrent where it is with its pin.
#[test]
fn pinned_download_relocates_into_downloads_dir_and_survives_a_restart() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let downloads_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;

    let content = src.path().join("Show Season 1");
    std::fs::create_dir_all(&content)?;
    write_payload(&content.join("e1.bin"), 40 * 1024);
    write_payload(&content.join("e2.bin"), 24 * 1024);
    let (torrent, info_hash) = real_torrent(&content);

    // "Streamed before": the data already sits in the cache root, where
    // librqbit puts a multi-file torrent added without a placement.
    let cache_root = cache_dir.path().join("cache");
    let root_folder = cache_root.join("rqbit-downloads").join("Show Season 1");
    std::fs::create_dir_all(&root_folder)?;
    std::fs::copy(content.join("e1.bin"), root_folder.join("e1.bin"))?;
    std::fs::copy(content.join("e2.bin"), root_folder.join("e2.bin"))?;

    let config = || stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    };
    let handle = stream_server::start(config())?;
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

    // The setting: validated like the route, persisted, reported.
    assert_eq!(handle.settings()?.downloads_dir, None);
    assert!(
        handle
            .update_settings(serde_json::json!({ "downloadsDir": "relative/path" }))
            .is_err(),
        "a relative downloadsDir is refused"
    );
    assert!(
        handle
            .update_settings(serde_json::json!({ "downloadsDir": 123 }))
            .is_err(),
        "a downloadsDir that is neither a string nor null is refused, not ignored"
    );
    assert_eq!(handle.settings()?.downloads_dir, None);
    let downloads = resolved(&downloads_dir.path().join("offline"));
    let updated = handle.update_settings(serde_json::json!({
        "downloadsDir": downloads.to_str().unwrap()
    }))?;
    assert_eq!(
        updated.downloads_dir.as_deref(),
        Some(downloads.to_str().unwrap())
    );
    assert!(downloads.is_dir(), "created on the spot");
    let http: serde_json::Value = client
        .get(format!("{base}/settings"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(http["values"]["downloadsDir"], downloads.to_str().unwrap());

    // Pin: relocated into <downloadsDir>/<infoHash>, re-checked there.
    let target = downloads.join(&info_hash);
    let info = handle.pin_download(&info_hash.to_uppercase(), idx, &[])?;
    assert_eq!(info.info_hash, info_hash);
    assert_eq!(info.file_idx, idx);
    assert_eq!(info.name, "e2.bin");
    assert_eq!(info.length, 24 * 1024);
    assert_eq!(
        info.path.as_deref(),
        Some(target.join("e2.bin").to_str().unwrap()),
        "{info:?}"
    );
    assert!(target.join("e2.bin").is_file());
    assert!(target.join("e1.bin").is_file(), "the whole torrent moves");
    assert!(!root_folder.exists(), "nothing left in the cache root");
    let stats = stats_after_check(&client, &base, &info_hash)?;
    assert_eq!(
        stats["files"][idx]["complete"], true,
        "re-check found the moved data: {stats}"
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

    let pins_file = cache_root
        .join("rqbit-downloads")
        .join("pinned-downloads.json");
    let pins: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&pins_file)?)?;
    assert_eq!(pins, serde_json::json!({ &info_hash: [idx] }));

    handle.shutdown()?;
    handle.join()?;

    // Restart on the same dirs: librqbit restores the torrent in its new
    // place (persisted output_folder / only_files), the pin comes back
    // from the persisted pin set, the setting from settings.json.
    let handle = stream_server::start(config())?;
    let base = format!("http://{}", handle.http_addr());
    let client = bearer_client(&handle)?;
    assert_eq!(
        handle.settings()?.downloads_dir.as_deref(),
        Some(downloads.to_str().unwrap())
    );
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
    assert_eq!(
        info.path.as_deref(),
        Some(target.join("e2.bin").to_str().unwrap())
    );
    assert!(info.complete);
    assert!(target.join("e2.bin").is_file());

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
/// re-hashing every file on every launch: after a complete torrent in the
/// cache root and a complete pinned one under `downloadsDir`, both
/// bitfields exist, and a restart brings both torrents back ready and
/// complete with the bitfields still in place (the `.bitv` + `overwrite:
/// true` combination librqbit needs to resume on top of existing files).
#[test]
fn fastresume_persists_piece_bitfields_on_both_roots() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let downloads_dir = tempfile::tempdir()?;
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

    let cache_root = cache_dir.path().join("cache");
    let session_dir = cache_root.join("rqbit-downloads");
    let root_folder = session_dir.join("Streamed");
    std::fs::create_dir_all(&root_folder)?;
    std::fs::copy(streamed.join("s1.bin"), root_folder.join("s1.bin"))?;
    std::fs::copy(streamed.join("s2.bin"), root_folder.join("s2.bin"))?;
    let downloads = resolved(&downloads_dir.path().join("offline"));
    let pinned_folder = downloads.join(&pinned_hash);
    std::fs::create_dir_all(&pinned_folder)?;
    std::fs::copy(pinned.join("p2.bin"), pinned_folder.join("p2.bin"))?;

    let config = || stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    };
    let handle = stream_server::start(config())?;
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

    handle.update_settings(serde_json::json!({ "downloadsDir": downloads.to_str().unwrap() }))?;
    // Only a pin places a torrent under the downloads dir, and a pin by
    // hash needs the metadata: /create supplies it (cache root, no data
    // there) and the pin relocates the torrent onto the pre-seeded file.
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
        Some(pinned_folder.join("p2.bin").to_str().unwrap())
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
    // bitfields, ready and complete, in both roots.
    let handle = stream_server::start(config())?;
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
    assert!(pinned_folder.join("p2.bin").is_file());
    assert!(root_folder.join("s1.bin").is_file());

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
    let downloads_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;

    let content = src.path().join("Show Season 2");
    std::fs::create_dir_all(&content)?;
    write_payload(&content.join("e1.bin"), 40 * 1024);
    write_payload(&content.join("e2.bin"), 24 * 1024);
    let (torrent, info_hash) = real_torrent(&content);

    // The data is already in the cache root, as after streaming it.
    let cache_root = cache_dir.path().join("cache");
    let root_folder = cache_root.join("rqbit-downloads").join("Show Season 2");
    std::fs::create_dir_all(&root_folder)?;
    std::fs::copy(content.join("e1.bin"), root_folder.join("e1.bin"))?;
    std::fs::copy(content.join("e2.bin"), root_folder.join("e2.bin"))?;

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    })?;
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

    let downloads = resolved(&downloads_dir.path().join("offline"));
    handle.update_settings(serde_json::json!({ "downloadsDir": downloads.to_str().unwrap() }))?;
    let target = downloads.join(&info_hash);

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

    // POST == pin_download: the file is pinned, the torrent relocated into
    // <downloadsDir>/<infoHash>, and the answer is a DownloadInfo.
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
    // `complete` is whatever the re-check of the moved data has reached by
    // now (the relocation puts the torrent back into `checking`); the
    // listing below asserts it once the check is done.
    assert_eq!(
        pinned["path"],
        target.join("e1.bin").to_str().unwrap(),
        "{pinned}"
    );
    assert!(target.join("e1.bin").is_file());

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
        Some(target.join("e2.bin").to_str().unwrap())
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
    assert!(
        target.join("e1.bin").is_file() && target.join("e2.bin").is_file(),
        "no file of the torrent is touched"
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
    assert!(target.join("e1.bin").is_file(), "the bytes stay");
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

    // The last pin, with the files: the torrent's folder goes with it, and
    // the answer says the data really went.
    assert_eq!(
        handle.unpin_download(&info_hash.to_uppercase(), second, true)?,
        stream_server::UnpinOutcome {
            unpinned: true,
            deleted_files: true,
        }
    );
    assert!(!target.exists(), "the whole placed torrent folder is gone");
    assert!(handle.downloads()?.is_empty());
    assert_eq!(handle.download_path(&info_hash, second)?, None);
    let listed: serde_json::Value = client
        .get(format!("{base}/downloads.json"))
        .send()?
        .error_for_status()?
        .json()?;
    assert_eq!(listed, serde_json::json!([]));

    // `deletedFiles` reports what happened, not what was asked for. With
    // no downloadsDir, an unmanaged torrent has no folder this layer named
    // and could remove -- its bytes, if any, are in the cache root, which
    // the cleaner does walk -- so the answer says nothing was deleted
    // instead of echoing the query flag back.
    handle.update_settings(serde_json::json!({ "downloadsDir": null }))?;
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

/// `GET /cache.json` and `POST /cache/clean` share their functions with
/// `ServerHandle::{cache_usage, clean_cache_now}` -- the replacement for a
/// client restarting the server just to make the cache cleaner's start-up
/// tick fire. A pinned download's engine stays live and protects its file;
/// an idle leftover with no engine at all is ordinary, evictable cache.
#[test]
fn cache_routes_match_the_library_api() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;

    let content = src.path().join("Movie");
    std::fs::create_dir_all(&content)?;
    // Two files, each a whole number of 16 KiB pieces (as `lan_media_server`
    // does), so the torrent is a *directory* torrent whose data lands in
    // `rqbit-downloads/Movie/` -- a single-file torrent instead would place
    // its data directly at `rqbit-downloads/movie.mkv`, with no folder.
    // Pinning only `movie.mkv` still protects both: the engine stays live
    // for as long as it has any pinned file, and protection covers every
    // file that live engine reports, not only the pinned index.
    write_payload(&content.join("movie.mkv"), 64 * 1024);
    write_payload(&content.join("subtitle.srt"), 16 * 1024);
    let (torrent, info_hash) = real_torrent(&content);

    // Already "streamed": the data sits in the cache root, as it would
    // after playback, and `POST /create` below picks it up from there.
    let cache_root = cache_dir.path().join("cache");
    let root_folder = cache_root.join("rqbit-downloads").join("Movie");
    std::fs::create_dir_all(&root_folder)?;
    std::fs::copy(content.join("movie.mkv"), root_folder.join("movie.mkv"))?;
    std::fs::copy(
        content.join("subtitle.srt"),
        root_folder.join("subtitle.srt"),
    )?;

    // An idle leftover with no engine managing it at all -- ordinary cache
    // from a torrent nothing is tracking any more.
    let idle = cache_root
        .join("rqbit-downloads")
        .join("Leftover")
        .join("old.mkv");
    std::fs::create_dir_all(idle.parent().unwrap())?;
    write_payload(&idle, 16 * 1024);

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_root.clone()),
        ..offline_config()
    })?;
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

    // Read usage() before touching the limit, to learn exactly how many
    // bytes the pinned torrent's two files occupy: `evict` never takes a
    // single file whose own size exceeds the limit (see
    // `cache_soft_limit_exceeded_by_single_retained_file` in
    // `cache_cleaner::evict`), so a limit of, say, `1` would make every
    // real file -- pinned or not -- too big to touch and evict nothing at
    // all. The limit below sits just above the protected bytes: over what
    // the pin alone needs, but under the idle leftover's own size added to
    // it, so only that leftover is evictable.
    let baseline = handle.cache_usage()?;
    assert_eq!(baseline.protected_files, 2, "{baseline:?}");
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
        "the idle leftover pushes the cache over the limit: {http_usage}"
    );
    assert_eq!(
        http_usage["protectedFiles"], 2,
        "both of the pinned torrent's files, not only the pinned index: {http_usage}"
    );
    assert_eq!(
        http_usage["protectedBytes"], baseline.protected_bytes,
        "{http_usage}"
    );

    // clean_cache_now() over HTTP: the idle file goes, the pinned one does
    // not, and the run lands exactly at the limit -- nothing but the idle
    // leftover was ever evictable.
    let report: serde_json::Value = client
        .post(format!("{base}/cache/clean"))
        .send()?
        .error_for_status()?
        .json()?;
    assert!(!idle.exists(), "unpinned idle cache is evictable: {report}");
    assert!(
        root_folder.join("movie.mkv").is_file() && root_folder.join("subtitle.srt").is_file(),
        "a pinned download's files are never touched: {report}"
    );
    assert_eq!(report["deleted"], 1, "{report}");
    assert_eq!(report["total"], baseline.protected_bytes, "{report}");
    assert_eq!(
        report["freed"],
        http_usage["totalBytes"].as_u64().unwrap() - baseline.protected_bytes,
        "{report}"
    );
    assert_eq!(report["protectedFiles"], 2, "{report}");
    assert_eq!(report["protected"], baseline.protected_bytes, "{report}");

    // clean_cache_now() == POST /cache/clean, run right after over the
    // library instead: nothing is left to evict, but the pinned torrent's
    // bytes still show up as protected -- the same function underneath
    // both surfaces, so the two can never disagree about it.
    let api_report = handle.clean_cache_now()?;
    assert_eq!(api_report.deleted, 0, "nothing left to evict");
    assert_eq!(api_report.freed, 0);
    assert_eq!(api_report.protected_files, 2);
    assert!(root_folder.join("movie.mkv").is_file());

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
    // directory walk produced -- and so the torrent is a directory torrent
    // whose data lands in `rqbit-downloads/Movie/`.
    write_payload(&content.join("movie.bin"), 64 * 1024);
    write_payload(&content.join("extra.bin"), 16 * 1024);
    let payload = std::fs::read(content.join("movie.bin"))?;
    let (torrent, info_hash) = real_torrent(&content);

    let cache_root = cache_dir.join("cache");
    let seeded = cache_root.join("rqbit-downloads").join("Movie");
    std::fs::create_dir_all(&seeded)?;
    std::fs::copy(content.join("movie.bin"), seeded.join("movie.bin"))?;
    std::fs::copy(content.join("extra.bin"), seeded.join("extra.bin"))?;

    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        lan_media_addr,
        config_dir: Some(config_dir.join("config")),
        cache_dir: Some(cache_root),
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
    let idx = file_index(&stats, "movie.bin");
    // The hash check of the pre-seeded copy is what makes the bytes
    // servable, and `phase` can already read `buffering` while it is still
    // queued -- so wait on the observable state the media requests need
    // (bounded, not a timing assertion), never on a sleep.
    let deadline = std::time::Instant::now() + CHECK_WAIT_BOUND;
    let stats = loop {
        let stats = file_stats_after_check(&client, &base, &info_hash, idx)?;
        if stats["files"][idx]["complete"] == true {
            break stats;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the pre-seeded file was still incomplete after {CHECK_WAIT_BOUND:?}: {stats}"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    };
    anyhow::ensure!(stats["files"][idx]["complete"] == true, "{stats}");

    Ok((handle, base, info_hash, idx, payload))
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
    let cache_root = cache_dir.path().join("cache");
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
    let cache_root = cache_dir.path().join("cache");

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
    // anything settled about the torrent at all.
    stats_after_check(&client, &base, &info_hash)?;
    assert!(
        swarm_paused(&client)?,
        "the torrent came back stopped, exactly as the last process left it"
    );
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
        !swarm_paused(&client)?,
        "the request started the torrent it was about to read from"
    );

    // And the GET that follows is not refused: there is room above the
    // floor. No peer will ever bring these bytes, so the client giving up
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

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// A stream request below the free-space floor is refused with a `507`
/// once a cleaner pass has had its chance -- not "degraded to memory-only",
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
    let cache_root = cache_dir.path().join("cache");
    let client = bearer_client(&handle)?;
    let anonymous = reqwest::blocking::Client::new();
    let url = format!("{base}/{info_hash}/{idx}");

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
/// remote URL; the archive and NZB `/create` routes download an archive from
/// a caller-named URL or open TCP connections to caller-named news servers;
/// and the loopback stream route's first request for a hash starts a torrent
/// with the caller's trackers. All of that is loopback only. The LAN gets the
/// byte-serving halves alone -- a torrent that exists, a member of an
/// archive or NZB session loopback already created -- and an unknown torrent
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

    // The archive and NZB session-create routes fetch a caller-named URL
    // (an archive to download whole, an NZB plus the news servers to open
    // connections to), so they are loopback only: a real route there (a
    // `400` for the missing payload) and absent from the LAN. `GET` on the
    // LAN is the two-segment collision answered by the LAN stream handler
    // looking `"rar"` up as an info hash and finding nothing; the keyed
    // form has three segments and reaches the fallback; `POST` is a method
    // the stream route does not take.
    for prefix in ["/rar", "/zip", "/7zip", "/tar", "/tgz", "/nzb"] {
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
