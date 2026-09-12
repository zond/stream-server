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

    println!("embedded server listening at http://{}", handle.http_addr());

    let response = reqwest::blocking::get(format!("http://{}/heartbeat", handle.http_addr()))?
        .error_for_status()?;
    println!("heartbeat: {}", response.text()?);

    handle.shutdown()?;
    handle.join()?;

    Ok(())
}
