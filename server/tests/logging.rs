//! The log files of a process that starts its server more than once.

use stream_server::ServerConfig;

/// A second start in one process -- an embedder's stop, then start --
/// leaves the first start's log files alone. The first start's subscriber
/// is the process's for good and goes on writing `server_current.log`;
/// when the second start rotated that file, its lines went on into the
/// archive the rename made, and the fresh `server_current.log` stayed
/// empty.
#[test]
fn a_second_start_in_one_process_rotates_nothing() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = || ServerConfig {
        config_dir: Some(dir.path().join("config")),
        cache_dir: Some(dir.path().join("cache")),
        init_logging: true,
        resolve_dht_bootstrap_names: false,
        pins: Some(Default::default()),
        ..ServerConfig::default()
    };
    for _ in 0..2 {
        let handle = stream_server::start(config())?;
        handle.shutdown()?;
        handle.join()?;
    }

    let mut names: Vec<String> = std::fs::read_dir(dir.path().join("config").join("logs"))?
        .flatten()
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.starts_with("server_"))
        .collect();
    names.sort();
    assert_eq!(names.len(), 2, "one text log and one JSON log: {names:?}");
    assert!(
        names.contains(&"server_current.log".to_string()),
        "{names:?}"
    );
    Ok(())
}
