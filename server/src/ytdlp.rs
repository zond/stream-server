//! Locates (and, when necessary, downloads) the `yt-dlp` binary used to resolve
//! YouTube stream URLs.
//!
//! YouTube rotates its player signature scheme every few weeks, so a copy of the
//! extractor pinned at build time stops working long before the next server
//! release. The binary is therefore treated as refreshable data rather than as a
//! build artifact: it lives next to the other downloaded tools and is re-fetched
//! once it goes stale.

use anyhow::{Context, Result, anyhow};
use once_cell::sync::Lazy;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Overrides discovery entirely; intended for packagers who ship their own copy.
const PATH_OVERRIDE_ENV: &str = "STREMIO_YTDLP_PATH";

/// Re-download the managed copy once it is older than this. YouTube breaks
/// extractors far more often than the server ships, so this is deliberately
/// shorter than a typical release cycle.
const REFRESH_AFTER: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Resolution touches the filesystem and the network, so it is serialised: a
/// burst of trailer hovers would otherwise start one download per request.
static RESOLVE_LOCK: Lazy<Mutex<Option<PathBuf>>> = Lazy::new(|| Mutex::new(None));

/// Returns a path to a usable `yt-dlp`, downloading it if there is no copy yet.
pub async fn resolve(config_dir: &Path) -> Result<PathBuf> {
    let mut cached = RESOLVE_LOCK.lock().await;
    if let Some(path) = cached.as_ref()
        && path.exists()
    {
        return Ok(path.clone());
    }

    let resolved = discover(config_dir).await?;
    *cached = Some(resolved.clone());
    Ok(resolved)
}

/// The path a media player should be pointed at explicitly, or [`None`] when
/// `yt-dlp` is on `PATH` and the player can find it unaided.
///
/// Unlike [`resolve`] this never downloads and never blocks, and it deliberately
/// returns the managed path even before the file exists: players resolve the
/// binary when a URL is loaded, by which time the startup warm-up has usually
/// finished the download.
pub fn player_path(config_dir: &Path) -> Option<PathBuf> {
    if let Some(path) = env_override() {
        return Some(path);
    }
    if on_path().is_some() {
        return None;
    }
    managed_path(config_dir).ok()
}

async fn discover(config_dir: &Path) -> Result<PathBuf> {
    if let Some(path) = env_override() {
        info!(path = %path.display(), "using yt-dlp from {PATH_OVERRIDE_ENV}");
        return Ok(path);
    }

    if let Some(path) = on_path() {
        info!(path = %path.display(), "using yt-dlp from PATH");
        return Ok(path);
    }

    let managed = managed_path(config_dir)?;
    if managed.exists() {
        // A refresh failure is not fatal: the copy on disk still resolves most
        // videos, and the next request will try again.
        if is_stale(&managed) {
            info!(path = %managed.display(), "managed yt-dlp is stale, refreshing");
            if let Err(error) = download(&managed).await {
                warn!(%error, "could not refresh yt-dlp, keeping the existing copy");
            }
        }
        return Ok(managed);
    }

    info!(path = %managed.display(), "yt-dlp not found, downloading");
    download(&managed).await?;
    Ok(managed)
}

fn env_override() -> Option<PathBuf> {
    let raw = std::env::var_os(PATH_OVERRIDE_ENV)?;
    let path = PathBuf::from(raw);
    path.exists().then_some(path)
}

fn on_path() -> Option<PathBuf> {
    let name = if cfg!(windows) {
        "yt-dlp.exe"
    } else {
        "yt-dlp"
    };
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}

fn is_stale(path: &Path) -> bool {
    let Ok(modified) = path.metadata().and_then(|meta| meta.modified()) else {
        return false;
    };
    SystemTime::now()
        .duration_since(modified)
        .is_ok_and(|age| age > REFRESH_AFTER)
}

/// Where the server keeps its own copy of the yt-dlp binary.
fn managed_path(config_dir: &Path) -> Result<PathBuf> {
    let file_name = if cfg!(windows) {
        "yt-dlp.exe"
    } else {
        "yt-dlp"
    };

    let mut candidates = vec![config_dir.join("tools")];
    if let Some(data_dir) = dirs::data_local_dir() {
        candidates.push(data_dir.join("stremio-server").join("tools"));
    }
    if let Some(cache_dir) = dirs::cache_dir() {
        candidates.push(cache_dir.join("stremio-server").join("tools"));
    }
    candidates.push(std::env::temp_dir().join("stremio-server").join("tools"));

    // An existing copy wins over directory ordering, so a tool downloaded into
    // a fallback directory is not orphaned once the preferred one appears.
    for dir in &candidates {
        let candidate = dir.join(file_name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    let mut failures = Vec::new();
    for dir in &candidates {
        match std::fs::create_dir_all(dir) {
            Ok(()) => return Ok(dir.join(file_name)),
            Err(error) => failures.push(format!("{} ({error})", dir.display())),
        }
    }

    Err(anyhow!(
        "no writable directory for yt-dlp. Tried: {}",
        failures.join(", ")
    ))
}

/// The release asset that is a self-contained executable on this platform. The
/// bare `yt-dlp` asset is a Python zipapp and would need an interpreter, so the
/// per-platform standalone builds are used instead.
fn asset_name() -> Result<&'static str> {
    Ok(match (std::env::consts::OS, std::env::consts::ARCH) {
        ("windows", "x86_64") => "yt-dlp.exe",
        ("windows", "x86") => "yt-dlp_x86.exe",
        ("macos", _) => "yt-dlp_macos",
        ("linux", "x86_64") => "yt-dlp_linux",
        ("linux", "aarch64") => "yt-dlp_linux_aarch64",
        ("linux", "arm") => "yt-dlp_linux_armv7l",
        (os, arch) => {
            return Err(anyhow!(
                "no prebuilt yt-dlp for {os}/{arch}; install yt-dlp and set {PATH_OVERRIDE_ENV}"
            ));
        }
    })
}

async fn download(target: &Path) -> Result<()> {
    let asset = asset_name()?;
    let url = format!("https://github.com/yt-dlp/yt-dlp/releases/latest/download/{asset}");

    info!(%url, "downloading yt-dlp");
    let response = reqwest::get(&url)
        .await
        .with_context(|| format!("could not reach {url}"))?
        .error_for_status()
        .with_context(|| format!("unexpected response from {url}"))?;
    let bytes = response.bytes().await.context("could not read yt-dlp")?;

    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Written beside the target and renamed so a download interrupted halfway
    // can never leave a truncated executable behind.
    let staging = target.with_extension("partial");
    std::fs::write(&staging, &bytes)
        .with_context(|| format!("could not write {}", staging.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(0o755))?;
    }

    std::fs::rename(&staging, target)
        .with_context(|| format!("could not install {}", target.display()))?;

    info!(path = %target.display(), bytes = bytes.len(), "yt-dlp installed");
    Ok(())
}

/// Builds a `yt-dlp` invocation that stays silent and, on Windows, does not flash
/// a console window — the server runs embedded in a GUI process.
pub fn command(binary: &Path) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(binary);
    command.kill_on_drop(true);

    #[cfg(windows)]
    {
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.creation_flags(CREATE_NO_WINDOW);
    }

    command
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_is_known_for_the_host_platform() {
        // The server only ships for platforms with a standalone build.
        assert!(asset_name().is_ok());
    }

    #[test]
    fn managed_path_prefers_the_config_dir() {
        let temp = tempfile::tempdir().unwrap();
        let path = managed_path(temp.path()).unwrap();
        assert_eq!(path.parent().unwrap(), temp.path().join("tools"));
    }

    #[test]
    fn a_missing_binary_is_not_treated_as_stale() {
        assert!(!is_stale(Path::new("does-not-exist")));
    }
}
