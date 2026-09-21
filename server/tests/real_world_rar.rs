//! Our RAR reader against an archive we did not make.
//!
//! Every other RAR test here is answered by `support/rar_fixtures.rs`,
//! which writes the headers this workspace believes in. That proves the
//! reader against its own author. A set packed years ago by WinRAR, split
//! across four volumes, served by somebody else's HTTP server, proves it
//! against the world -- which is the only thing that can find a header
//! this repository has never written.
//!
//! **Ignored, and it stays ignored.** It fetches over the internet, so it
//! is not a gate; `cargo test` must make no request of its own. Run it by
//! hand when the RAR reader or the translated-sources seam changes:
//!
//! ```text
//! cargo test -p server --test real_world_rar -- --ignored --nocapture
//! ```
//!
//! Two tests, because the world has two kinds of RAR in it.
//!
//! The first is a four-volume set on archive.org -- freely distributed,
//! served with byte ranges, somebody's real WinRAR output. It is packed
//! the way WinRAR packs by default, which is to say **compressed**, so
//! what it proves is the half that runs before the refusal: four volumes
//! of headers parsed, the member found, its method read, and the right
//! refusal given in the viewer's words. That is the half our own fixtures
//! could be wrong about together with the reader.
//!
//! The second needs a **stored** set, which is what a scene release is and
//! what our fixtures imitate. No freely-distributable one has turned up,
//! so it takes its volumes from `XTREMIO_RAR_VOLUMES` (comma-separated
//! URLs, in volume order) and skips when that is unset. Point it at any
//! stored set and it reads the member's head and then a span across the
//! seam between two volumes -- the one thing a single-volume archive
//! cannot test at all.
#![cfg(feature = "rar")]

use std::net::SocketAddr;

/// The volumes, in order. **Order is the whole of what a set is**: an
/// extent names the volume it lives in by position in this list.
const VOLUMES: [&str; 4] = [
    "https://archive.org/download/Quran.Giants/Quran.Giants.CD.part1.rar",
    "https://archive.org/download/Quran.Giants/Quran.Giants.CD.part2.rar",
    "https://archive.org/download/Quran.Giants/Quran.Giants.CD.part3.rar",
    "https://archive.org/download/Quran.Giants/Quran.Giants.CD.part4.rar",
];

fn offline_config() -> stream_server::ServerConfig {
    stream_server::ServerConfig {
        resolve_dht_bootstrap_names: false,
        use_public_trackers: false,
        enable_local_service_discovery: false,
        pins: Some(Default::default()),
        ..stream_server::ServerConfig::default()
    }
}

/// Four volumes of real WinRAR headers, read, and the member refused for
/// what it actually is.
#[test]
#[ignore = "fetches over the internet; run it by hand"]
fn a_compressed_set_packed_by_somebody_else_is_refused_for_being_compressed() -> anyhow::Result<()>
{
    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..offline_config()
    })?;
    let base = format!("http://{}", handle.http_addr());
    let client = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(120))
        .build()?;

    // Resolved here rather than by the server: archive.org answers
    // `/download/...` with a 302 to whichever storage node holds the item,
    // and what is under test is the reading of a RAR set, not our handling
    // of somebody's redirect. (Worth knowing that it is not nothing: the
    // first run of this test, handing the server the `/download/` URLs,
    // came back `malformed` -- "archive.org answered 500 Internal Server
    // Error to a ranged read".)
    let following = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?;
    let mut volumes = Vec::new();
    for volume in VOLUMES {
        let resolved = following.head(volume).send()?;
        anyhow::ensure!(
            resolved.status().is_success(),
            "{volume} answered {}",
            resolved.status()
        );
        volumes.push(resolved.url().to_string());
    }
    println!("resolved to {}", volumes[0]);

    // Indexing reads headers, not archives: four volumes of 200 MB each,
    // and what leaves the origin is the few kilobytes the headers sit in.
    let created = client
        .post(format!("{base}/rar/create"))
        .json(&serde_json::json!({ "urls": volumes }))
        .send()?;

    // The origin's own bad day is not a verdict on the reader. archive.org
    // answers a burst of ranged reads with a 500 often enough that a test
    // which called that `malformed` would be crying wolf: indexing four
    // volumes is four bursts, and this test has had both answers from the
    // same URL a minute apart. Said out loud and skipped, never passed
    // quietly.
    let status = created.status();
    let body: serde_json::Value = created.json()?;
    if status == reqwest::StatusCode::UNPROCESSABLE_ENTITY
        && body["message"]
            .as_str()
            .is_some_and(|message| message.contains("Internal Server Error"))
    {
        println!("the origin refused to serve its own file: {body}");
        println!("this says nothing about the reader; run it again later");
        handle.shutdown()?;
        return Ok(());
    }

    // 415 and `compressed`, not 422 and `malformed`: the difference is the
    // whole point. `malformed` would mean the headers beat us; this means
    // they were read, the member was found, and its method was understood
    // -- and then policy, not the reader, said no. Nothing is unpacked to
    // play a film, ever.
    anyhow::ensure!(
        status == reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "a compressed set answered {status} instead of 415: {body}"
    );
    println!("refused: {body}");
    anyhow::ensure!(
        body["refused"] == "compressed",
        "refused as {} rather than as compressed",
        body["refused"]
    );

    handle.shutdown()?;
    Ok(())
}

/// The stored half, against whatever stored set the runner can point at.
///
/// ```text
/// XTREMIO_RAR_VOLUMES=https://host/rel.part1.rar,https://host/rel.part2.rar \
///   cargo test -p server --test real_world_rar -- --ignored --nocapture
/// ```
#[test]
#[ignore = "fetches over the internet; run it by hand"]
fn a_stored_set_is_read_across_its_volumes() -> anyhow::Result<()> {
    let Ok(listed) = std::env::var("XTREMIO_RAR_VOLUMES") else {
        println!("XTREMIO_RAR_VOLUMES is unset; nothing to read");
        return Ok(());
    };
    let volumes: Vec<String> = listed
        .split(',')
        .map(|url| url.trim().to_string())
        .collect();
    anyhow::ensure!(!volumes.is_empty(), "no volumes given");

    let config_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let handle = stream_server::start(stream_server::ServerConfig {
        http_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        config_dir: Some(config_dir.path().join("config")),
        cache_dir: Some(cache_dir.path().join("cache")),
        ..offline_config()
    })?;
    let base = format!("http://{}", handle.http_addr());
    let client = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(120))
        .build()?;

    let created = client
        .post(format!("{base}/rar/create"))
        .json(&serde_json::json!({ "urls": volumes }))
        .send()?;
    anyhow::ensure!(
        created.status() == reqwest::StatusCode::OK,
        "create answered {}: {}",
        created.status(),
        created.text()?
    );
    let key = created.json::<serde_json::Value>()?["key"]
        .as_str()
        .expect("a key")
        .to_string();

    // The redirect is where a member's name is told: nothing else in the
    // API hands one out, which is why the player asks for it here.
    let redirect = client.get(format!("{base}/rar/stream/{key}")).send()?;
    anyhow::ensure!(
        redirect.status().is_redirection(),
        "the set named no member: {} {}",
        redirect.status(),
        redirect.text()?
    );
    let member = redirect
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|location| location.to_str().ok())
        .expect("a member to play")
        .to_string();
    let member = if member.starts_with("http") {
        member
    } else {
        format!("{base}{member}")
    };
    println!("member: {member}");

    // A stored member is bytes of the volumes, so its first sixteen are
    // the file's own -- a signature, a header, something that is not a
    // hole. A reader that mis-stated an extent's offset answers zeros
    // here and a length there, both plausibly.
    let head = client
        .get(&member)
        .header(reqwest::header::RANGE, "bytes=0-15")
        .send()?;
    anyhow::ensure!(
        head.status() == reqwest::StatusCode::PARTIAL_CONTENT,
        "the head of the member answered {}",
        head.status()
    );
    let head = head.bytes()?;
    println!("first bytes: {:02x?}", &head[..]);
    anyhow::ensure!(head.len() == 16, "asked for 16 bytes, got {}", head.len());
    anyhow::ensure!(
        head.iter().any(|byte| *byte != 0),
        "the first sixteen bytes of the member are a hole"
    );

    // And across the seam, if the caller said where it is. The member
    // begins a little way into volume one (its headers come first), so a
    // read that starts just before the volume's end and runs past it is
    // served out of two volumes.
    if let Ok(seam) = std::env::var("XTREMIO_RAR_SEAM") {
        let seam: u64 = seam.parse()?;
        let across = client
            .get(&member)
            .header(
                reqwest::header::RANGE,
                format!("bytes={seam}-{}", seam + 511),
            )
            .send()?;
        anyhow::ensure!(
            across.status() == reqwest::StatusCode::PARTIAL_CONTENT,
            "the read across the seam answered {}",
            across.status()
        );
        let across = across.bytes()?;
        anyhow::ensure!(
            across.len() == 512,
            "asked for 512 bytes across the seam, got {}",
            across.len()
        );
        anyhow::ensure!(
            across.iter().any(|byte| *byte != 0),
            "the read across the volume seam is a hole"
        );
    }

    handle.shutdown()?;
    Ok(())
}
