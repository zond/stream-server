//! Embedding the server: start it on its own thread, read back the address
//! it bound, ask it something over HTTP the way stremio-core does, and stop
//! it. CI runs this (`cargo run -p server --example embed_thread`), so a
//! route it asks for that goes away fails the build, not the next reader.
fn main() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;

    let handle = stream_server::start(stream_server::ServerConfig {
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        // Port 0, not the default 11470: an embedder that reads the bound
        // address back -- which is the whole of what this example does --
        // needs no fixed port, and asking for one only risks losing the
        // bind to a desktop Stremio or to a second copy of this example.
        http_addr: std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
        ..stream_server::ServerConfig::default()
    })?;

    println!("embedded server listening at {}", handle.base_url());

    // `GET /settings`, as stremio-core asks it: a control route, so it
    // carries this launch's bearer token -- read from the handle, sent in
    // the header and never printed.
    let token = handle
        .auth_token()
        .ok_or_else(|| anyhow::anyhow!("a started server has a token"))?;
    let settings: serde_json::Value = reqwest::blocking::Client::new()
        .get(format!("{}/settings", handle.base_url()))
        .bearer_auth(token)
        .send()?
        .error_for_status()?
        .json()?;
    println!(
        "serverVersion over HTTP: {}",
        settings["values"]["serverVersion"]
    );
    // And the same answer from the handle, which is how the app asks.
    println!(
        "serverVersion by call: {}",
        handle.settings()?.server_version
    );

    handle.shutdown()?;
    handle.join()?;

    Ok(())
}
