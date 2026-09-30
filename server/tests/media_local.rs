//! A file on this device played by id (`MediaSpec::Local`,
//! `docs/design/media-pipeline.md` §2.3, step B): a path or an fd is
//! registered with no I/O, resolved by opening it, and read through the
//! same reader task as every other source. A pipe is refused at resolve;
//! and no HTTP route can name a local file, because none takes a spec.
//!
//! Every server here is offline and on an ephemeral port. A
//! [`MediaReader`] refuses to be called from inside a runtime, so the reads
//! are made from the test's own thread, which has none.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use stream_server::{LocalFile, MediaReader, MediaSpec, PlayToken, Refusal, ServerConfig};

#[path = "support/torrent_fixtures.rs"]
mod torrent_fixtures;
use torrent_fixtures::offline_config;

/// A film whose every byte says where it is.
fn film(len: usize) -> Vec<u8> {
    (0..len).map(|at| (at % 251) as u8).collect()
}

/// An offline server and the directories it lives in.
fn server() -> anyhow::Result<(stream_server::ServerHandle, [tempfile::TempDir; 2])> {
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let handle = stream_server::start(ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(stream_server::resolved_path(
            &cache_dir.path().join("cache"),
        )),
        ..offline_config()
    })?;
    Ok((handle, [config_dir, cache_dir]))
}

/// `bytes` written to `name` in a directory of the test's own.
fn written(name: &str, bytes: &[u8]) -> anyhow::Result<(tempfile::TempDir, PathBuf)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join(name);
    std::fs::write(&path, bytes)?;
    Ok((dir, path))
}

/// Everything from the reader's position to its end.
fn read_to_end(reader: &mut MediaReader) -> std::io::Result<Vec<u8>> {
    let mut read = Vec::new();
    let mut buf = vec![0u8; 7 * 1024];
    loop {
        match reader.read(&mut buf)? {
            0 => return Ok(read),
            n => read.extend_from_slice(&buf[..n]),
        }
    }
}

/// Read `reader` whole, seek to `seek_to` and read the rest, then read
/// once more at the end: the three things a player does.
fn read_seek_end(mut reader: MediaReader, bytes: &[u8], seek_to: u64) -> anyhow::Result<()> {
    assert_eq!(read_to_end(&mut reader)?, bytes, "read from the top");
    assert_eq!(reader.seek(seek_to)?, seek_to);
    assert_eq!(
        read_to_end(&mut reader)?,
        bytes[seek_to as usize..],
        "read from after the seek"
    );
    assert_eq!(reader.read(&mut [0u8; 8])?, 0, "a read at the end");
    Ok(())
}

const LEN: usize = 700 * 1024 + 13;
const SEEK_TO: u64 = 333_333;

/// **A path registers, resolves and reads**: exact bytes from the top and
/// from after a seek, then the end. Its name is the file name and its type
/// follows the extension.
#[test]
fn a_local_path_resolves_and_reads_seeks_and_ends() -> anyhow::Result<()> {
    let (handle, _dirs) = server()?;
    let bytes = film(LEN);
    let (_dir, path) = written("Home Video.mp4", &bytes)?;

    let id = handle.register(MediaSpec::Local {
        file: LocalFile::Path(path),
        name: None,
    })?;
    let resolved = handle.resolve(&id)?;
    assert_eq!(resolved.name, "Home Video.mp4");
    assert_eq!(resolved.content_type, "video/mp4");
    assert_eq!(resolved.len, LEN as u64);
    assert!(resolved.in_process);
    assert_eq!(resolved.member, None);

    read_seek_end(handle.open_reader(&id, None)?, &bytes, SEEK_TO)?;
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **The content type follows the name's extension**, the name the app
/// gave outranking the file's own.
#[test]
fn the_content_type_follows_the_extension() -> anyhow::Result<()> {
    let (handle, _dirs) = server()?;
    let (_dir, path) = written("clip.bin", &film(10))?;
    for (name, content_type) in [
        ("a.mkv", "video/x-matroska"),
        ("b.webm", "video/webm"),
        ("c.MP4", "video/mp4"),
    ] {
        let id = handle.register(MediaSpec::Local {
            file: LocalFile::Path(path.clone()),
            name: Some(name.to_string()),
        })?;
        let resolved = handle.resolve(&id)?;
        assert_eq!(resolved.name, name);
        assert_eq!(resolved.content_type, content_type, "{name}");
    }
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **A path that is not there is a refusal at resolve**, and registering
/// it was not: `register` does no I/O.
#[test]
fn a_missing_path_is_refused_at_resolve() -> anyhow::Result<()> {
    let (handle, _dirs) = server()?;
    let dir = tempfile::tempdir()?;
    let id = handle.register(MediaSpec::Local {
        file: LocalFile::Path(dir.path().join("gone.mkv")),
        name: None,
    })?;
    let refusal = handle.resolve(&id).expect_err("a missing file resolved");
    assert_eq!(refusal.kind(), "openFailed", "{refusal}");
    assert!(
        refusal
            .to_string()
            .starts_with("the file on this device could not be opened: "),
        "{refusal}"
    );
    assert_eq!(handle.open_reader(&id, None).err(), Some(refusal));
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **An fd reads as its path does**: an open file handed over as an
/// `OwnedFd`, named by the app.
#[cfg(unix)]
#[test]
fn a_local_fd_resolves_and_reads_seeks_and_ends() -> anyhow::Result<()> {
    let (handle, _dirs) = server()?;
    let bytes = film(LEN);
    let (_dir, path) = written("picked", &bytes)?;
    let fd: std::os::fd::OwnedFd = std::fs::File::open(&path)?.into();

    let id = handle.register(MediaSpec::Local {
        file: LocalFile::Fd(fd),
        name: Some("Picked.mkv".to_string()),
    })?;
    let resolved = handle.resolve(&id)?;
    assert_eq!(resolved.name, "Picked.mkv");
    assert_eq!(resolved.content_type, "video/x-matroska");
    assert_eq!(resolved.len, LEN as u64);

    read_seek_end(handle.open_reader(&id, None)?, &bytes, SEEK_TO)?;
    // Two readers of the one fd, each with its own place.
    let first = handle.open_reader(&id, None)?;
    read_seek_end(handle.open_reader(&id, None)?, &bytes, 5)?;
    read_seek_end(first, &bytes, SEEK_TO)?;
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **A pipe is refused at resolve, with the sentence**, never streamed
/// forward: what a cloud provider behind Android's storage framework may
/// hand out for a document. The sentence is pinned: it is what a viewer
/// is shown.
#[cfg(unix)]
#[test]
fn a_pipe_fd_is_refused_at_resolve() -> anyhow::Result<()> {
    let (handle, _dirs) = server()?;
    let (reader, mut writer) = std::io::pipe()?;
    std::io::Write::write_all(&mut writer, b"forward only")?;
    let id = handle.register(MediaSpec::Local {
        file: LocalFile::Fd(reader.into()),
        name: Some("cloud.mkv".to_string()),
    })?;
    let refusal = handle.resolve(&id).expect_err("a pipe resolved");
    assert_eq!(refusal, Refusal::NotSeekable);
    assert_eq!(refusal.kind(), "notSeekable");
    assert_eq!(
        refusal.to_string(),
        "this file can only be read once from start to end (its provider streams it rather \
         than handing over the file), and playing it needs to seek: save it to this device first"
    );
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// **A local id read with a play puts the viewer's session on
/// `Elsewhere`**, as a `p=` request through `/proxy` does: the viewer is
/// on no torrent file now, so one played before is left to go slack. An
/// aside moves nothing.
#[test]
fn a_played_local_reader_puts_the_session_elsewhere() -> anyhow::Result<()> {
    let (handle, _dirs) = server()?;
    let (_dir, path) = written("film.mkv", &film(1024))?;
    let id = handle.register(MediaSpec::Local {
        file: LocalFile::Path(path),
        name: None,
    })?;

    let aside = handle.open_reader(&id, None)?;
    assert_eq!(
        handle.play_session_of("tv.1"),
        None,
        "an aside moved a session"
    );
    drop(aside);

    let played = handle.open_reader(
        &id,
        Some(PlayToken {
            token: "tv.1".to_string(),
            buffer: Default::default(),
        }),
    )?;
    assert_eq!(
        handle.play_session_of("tv.1"),
        Some(enginefs::retention::sessions::Played::Elsewhere)
    );
    drop(played);
    handle.shutdown()?;
    handle.join()?;
    Ok(())
}

/// Whether `T` can be read out of a request body. The two impls make a
/// call with the parameter left to inference ambiguous exactly when both
/// apply, i.e. when `T` is `DeserializeOwned` -- a compile error, which is
/// the failure.
trait NotFromABody<A> {
    fn check() {}
}
impl<T: ?Sized> NotFromABody<()> for T {}
#[allow(dead_code)]
struct FromABody;
impl<T: serde::de::DeserializeOwned> NotFromABody<FromABody> for T {}

/// The text of `fn name` at the top level of `source`: its signature to
/// its closing brace, the first line that is exactly `}`.
fn function<'a>(source: &'a str, name: &str) -> &'a str {
    let start = source
        .find(&format!("fn {name}("))
        .unwrap_or_else(|| panic!("no fn {name} in lib.rs"));
    let len = source[start..]
        .find("\n}\n")
        .unwrap_or_else(|| panic!("fn {name} does not end"));
    &source[start..start + len]
}

/// Every `.rs` file under `dir`.
fn rust_files(dir: &Path, into: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("a source directory") {
        let path = entry.expect("an entry").path();
        if path.is_dir() {
            rust_files(&path, into);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            into.push(path);
        }
    }
}

/// **No HTTP route takes a `MediaSpec`, or reaches the id registry**: a
/// `local:` spec is constructible only through `ServerHandle::register`,
/// over FFI, so no URL or request body can name a file on this device
/// (design §2.3). Two halves, each able to fail:
///
/// * `MediaSpec` cannot be deserialized, so no `Json<_>` or `Query<_>`
///   extractor can take one -- a compile-time check;
/// * no handler under `routes/`, nothing in the LAN listener, and none of
///   the router builders in `lib.rs` names `MediaSpec`, `LocalFile`, a
///   `MediaId` or the registry (`.media.`), comments aside.
#[test]
fn no_http_route_takes_a_media_spec() {
    <MediaSpec as NotFromABody<_>>::check();

    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&src.join("routes"), &mut files);
    files.push(src.join("lan_media.rs"));
    let mut texts: Vec<(String, String)> = files
        .iter()
        .map(|path| {
            (
                path.display().to_string(),
                std::fs::read_to_string(path).expect("a source file"),
            )
        })
        .collect();
    let lib = std::fs::read_to_string(src.join("lib.rs")).expect("lib.rs");
    for router in [
        "build_router",
        "media_router",
        "lan_media_routes",
        "control_router",
    ] {
        texts.push((
            format!("lib.rs {router}"),
            function(&lib, router).to_string(),
        ));
    }
    assert!(
        texts.len() > 10,
        "the routes were not found: {}",
        texts.len()
    );
    for (where_, text) in &texts {
        for (number, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap_or_default();
            for needle in ["MediaSpec", "LocalFile", "MediaId", ".media."] {
                assert!(
                    !code.contains(needle),
                    "{where_}:{}: a route names `{needle}`: {line}",
                    number + 1
                );
            }
        }
    }
}
