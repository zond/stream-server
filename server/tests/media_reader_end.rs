//! The lines a played media reader leaves when it closes.
//!
//! A reader over a played torrent id makes the stream route's own open
//! (`open_torrent_stream`), so it registers a stream and its end is logged
//! by the same guard. The route's end line (`http_stream_end`) reports what
//! a response body delivered against the range it promised; a reader has
//! neither, and written through that line it said `requested_len=0` and a
//! disconnect for every playback. So a reader's stream ends with a line of
//! its own (`reader_stream_end`) carrying no HTTP numbers, and the reader
//! task's close line (`media_reader_closed`) says what it delivered.
//!
//! A binary of its own because `init_logging` installs the process's
//! subscriber once (see `stream_body_end.rs`).

use std::path::Path;
use std::time::{Duration, Instant};

#[path = "support/fixture_pins.rs"]
mod fixture_pins;

#[path = "support/torrent_fixtures.rs"]
mod torrent_fixtures;
use torrent_fixtures::{bearer_client, real_torrent, seed_single_file};

#[path = "support/log_lines.rs"]
mod log_lines;

const CHECK_WAIT_BOUND: Duration = Duration::from_secs(60);
const PAYLOAD: usize = 4 * 16 * 1024;

fn byte_at(offset: usize) -> u8 {
    (offset % 251) as u8
}

/// The torrent added and checked, and its one file's streaming URL.
fn seeded_stream_url(
    handle: &stream_server::ServerHandle,
    base: &str,
    cache_root: &Path,
    src: &Path,
    payload: &[u8],
) -> anyhow::Result<(String, url::Url)> {
    let content = src.join("Feature");
    std::fs::create_dir_all(&content)?;
    std::fs::write(content.join("movie.bin"), payload)?;
    let (torrent, info_hash) = real_torrent(&content);
    seed_single_file(cache_root, &torrent, payload);
    bearer_client(handle)?
        .post(format!("{base}/create"))
        .json(&serde_json::json!({ "torrent": hex::encode(&torrent) }))
        .send()?
        .error_for_status()?;
    let deadline = Instant::now() + CHECK_WAIT_BOUND;
    let stats = loop {
        let stats = serde_json::to_value(handle.engine_stats(&info_hash, &[])?)?;
        match stats["phase"].as_str() {
            Some("checking") | Some("resolvingMetadata") => {
                anyhow::ensure!(Instant::now() < deadline, "never checked: {stats}");
                std::thread::sleep(Duration::from_millis(50));
            }
            _ => break stats,
        }
    };
    let idx = stats["files"]
        .as_array()
        .expect("the stats name the torrent's files")
        .iter()
        .position(|file| file["name"] == "movie.bin")
        .expect("the fixture's own file");
    Ok((
        info_hash.clone(),
        url::Url::parse(&format!("{base}/{info_hash}/{idx}"))?,
    ))
}

/// A played reader, read whole and dropped, ends its stream with the
/// reader's line and not the route's.
#[test]
fn a_played_reader_ends_its_stream_with_a_reader_line_and_no_http_numbers() -> anyhow::Result<()> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let src = tempfile::tempdir()?;
    let config_root = config_dir.path().join("config");
    let cache_root = stream_server::resolved_path(&cache_dir.path().join("cache"));
    stream_server::pretend_volume_space(&cache_root, u64::MAX);
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_root.clone()),
        cache_dir: Some(cache_root.clone()),
        init_logging: true,
        ..fixture_pins::keep_what_the_fixture_seeded(torrent_fixtures::offline_config())
    })?;
    let base = format!("http://{}", handle.http_addr());
    let payload: Vec<u8> = (0..PAYLOAD).map(byte_at).collect();
    let (info_hash, url) = seeded_stream_url(&handle, &base, &cache_root, src.path(), &payload)?;

    let id = handle.register(stream_server::MediaSpec::StreamingUrl(url))?;
    let mut reader = handle.open_reader(
        &id,
        Some(stream_server::PlayToken {
            token: "tv.1".to_string(),
            buffer: Default::default(),
        }),
    )?;
    let read = std::thread::spawn(move || {
        let mut read = Vec::new();
        let mut buf = vec![0u8; 8192];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => return Ok(read),
                Ok(n) => read.extend_from_slice(&buf[..n]),
                Err(error) => return Err(error),
            }
        }
    })
    .join()
    .map_err(|_| anyhow::anyhow!("the reading thread panicked"))??;
    assert_eq!(read, payload);

    let ended = log_lines::wait_for_line(
        &config_root,
        "reader_stream_end",
        "the played reader's stream",
        |fields| fields["info_hash"] == info_hash,
    )?;
    let fields = &ended["fields"];
    assert!(fields["duration_ms"].is_u64(), "{fields}");
    assert!(
        fields.get("requested_len").is_none() && fields.get("bytes_sent").is_none(),
        "a reader's end line carries a response's numbers: {fields}"
    );
    let closed = log_lines::wait_for_line(
        &config_root,
        "media_reader_closed",
        "the reader task's close",
        |fields| fields["source"] == "torrent",
    )?;
    assert_eq!(closed["fields"]["delivered"], PAYLOAD, "{closed}");
    assert!(
        !log_lines::lines_at_stage(&config_root, "http_stream_end")
            .iter()
            .any(|line| line["fields"]["info_hash"] == info_hash),
        "the reader's stream ended with the route's line"
    );

    handle.shutdown()?;
    handle.join()?;
    Ok(())
}
