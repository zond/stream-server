use std::{
    backtrace::Backtrace,
    future::Future,
    path::{Path, PathBuf},
    sync::OnceLock,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use chrono::Local;
use tokio::task::JoinHandle;
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};

static LOG_GUARDS: OnceLock<Vec<WorkerGuard>> = OnceLock::new();
static PROCESS_START: OnceLock<Instant> = OnceLock::new();
static ACTIVE_DIRECT_STREAMS: AtomicU64 = AtomicU64::new(0);

pub struct LogWriters {
    pub human_writer: NonBlocking,
    pub json_writer: NonBlocking,
    pub human_path: PathBuf,
    pub json_path: PathBuf,
    pub guards: Vec<WorkerGuard>,
}

/// The human-readable log of the running launch. The one before it is
/// renamed to `server_<when it last wrote>.log` as this one opens.
const CURRENT_LOG: &str = "server_current.log";

/// How many launches' logs a start leaves on disk, per kind (text and
/// JSON), this launch's included. Nothing else deletes them.
pub const KEPT_LAUNCHES: usize = 10;

pub fn init_process_start() {
    let _ = PROCESS_START.set(Instant::now());
}

pub fn uptime_secs() -> u64 {
    PROCESS_START.get_or_init(Instant::now).elapsed().as_secs()
}

/// Render every request header as `name=value` pairs for diagnostic logging.
/// Credential headers are redacted to their byte length so logs can be shared
/// without leaking secrets; non-UTF8 values are shown as `<binary:N bytes>` so
/// the line always stays printable.
fn format_headers(headers: &axum::http::HeaderMap) -> String {
    fn is_sensitive(name: &axum::http::HeaderName) -> bool {
        matches!(
            name.as_str(),
            "authorization" | "proxy-authorization" | "cookie" | "set-cookie"
        )
    }

    let mut out = String::new();
    for (name, value) in headers {
        if !out.is_empty() {
            out.push_str(", ");
        }
        out.push_str(name.as_str());
        out.push('=');
        if is_sensitive(name) {
            out.push_str(&format!("<redacted:{} bytes>", value.len()));
        } else {
            match value.to_str() {
                Ok(v) => out.push_str(v),
                Err(_) => out.push_str(&format!("<binary:{} bytes>", value.len())),
            }
        }
    }
    out
}

/// Emit an ERROR log describing an unhandled route or request (a 404 fallback,
/// a 405 method mismatch, or a catch-all route that matched but could not be
/// served) with as much request context as is available: peer address, method,
/// full URI, HTTP version, the common diagnostic headers broken out as their
/// own fields, and a dump of every header. Centralised so that every
/// "we did not serve this" code path logs identically and greppably.
pub fn log_unhandled(
    reason: &str,
    status: u16,
    peer: Option<std::net::SocketAddr>,
    method: &axum::http::Method,
    uri: &axum::http::Uri,
    version: Option<axum::http::Version>,
    headers: &axum::http::HeaderMap,
) {
    use axum::http::header;
    let header_value = |name: &header::HeaderName| -> String {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string()
    };
    let peer = peer
        .map(|p| p.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    tracing::error!(
        reason,
        status,
        peer = %peer,
        method = %method,
        uri = %uri,
        path = uri.path(),
        query = uri.query().unwrap_or(""),
        version = ?version,
        host = %header_value(&header::HOST),
        user_agent = %header_value(&header::USER_AGENT),
        referer = %header_value(&header::REFERER),
        origin = %header_value(&header::ORIGIN),
        range = %header_value(&header::RANGE),
        content_type = %header_value(&header::CONTENT_TYPE),
        content_length = %header_value(&header::CONTENT_LENGTH),
        header_count = headers.len(),
        headers = %format_headers(headers),
        "unhandled request"
    );
}

pub fn active_direct_streams() -> u64 {
    ACTIVE_DIRECT_STREAMS.load(Ordering::Relaxed)
}

pub fn direct_stream_started() {
    ACTIVE_DIRECT_STREAMS.fetch_add(1, Ordering::Relaxed);
}

pub fn direct_stream_ended() {
    ACTIVE_DIRECT_STREAMS
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
            Some(count.saturating_sub(1))
        })
        .ok();
}

/// Open this launch's two log files: `server_current.log` for people and
/// `server_<launch>.jsonl` for tools.
///
/// Nothing else ever deletes a log, so this start does: the previous
/// launch's `server_current.log` becomes its archive by rename (so a text
/// line is written once, not again into a per-launch copy), and all but the
/// newest [`KEPT_LAUNCHES`] launches of either kind are removed. What one
/// launch writes is not bounded.
pub fn open_log_writers(log_dir: &Path) -> std::io::Result<LogWriters> {
    std::fs::create_dir_all(log_dir)?;

    let human_path = log_dir.join(CURRENT_LOG);
    let append_to_current = rotate_current_log(log_dir, &human_path);
    let json_path = log_dir.join(format!(
        "server_{}.jsonl",
        Local::now().format("%Y-%m-%d_%H-%M-%S")
    ));
    let human_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(append_to_current)
        .truncate(!append_to_current)
        .open(&human_path)?;
    let json_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&json_path)?;

    // After the opens, so this launch's own `.jsonl` is one of the kept,
    // and the text archives are the kept less the one `server_current.log`
    // now is.
    prune_launches(log_dir, ".jsonl", KEPT_LAUNCHES);
    prune_launches(log_dir, ".log", KEPT_LAUNCHES - 1);

    let (human_writer, human_guard) = tracing_appender::non_blocking(human_file);
    let (json_writer, json_guard) = tracing_appender::non_blocking(json_file);

    Ok(LogWriters {
        human_writer,
        json_writer,
        human_path,
        json_path,
        guards: vec![human_guard, json_guard],
    })
}

/// Rename the previous launch's `server_current.log` to
/// `server_<its last write>.log`. Answers whether the new launch has to
/// append to it instead: when the rename failed (on Windows, another
/// process still has the file open), truncating would destroy that
/// launch's log rather than archive it.
fn rotate_current_log(log_dir: &Path, current: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(current) else {
        return false;
    };
    let last_write: chrono::DateTime<Local> = metadata
        .modified()
        .map(Into::into)
        .unwrap_or_else(|_| Local::now());
    let stamp = last_write.format("%Y-%m-%d_%H-%M-%S").to_string();
    // Two launches that end in the same second would otherwise have the
    // second rename replace the first's archive.
    let mut archive = log_dir.join(format!("server_{stamp}.log"));
    let mut n = 1;
    while archive.exists() {
        archive = log_dir.join(format!("server_{stamp}-{n}.log"));
        n += 1;
    }
    std::fs::rename(current, &archive).is_err()
}

/// Delete all but the newest `keep` files named `server_*<suffix>` in
/// `log_dir`, newest by name: every such name carries a time as
/// `%Y-%m-%d_%H-%M-%S`, which sorts as the time does.
/// `server_current.log` is never one of them.
fn prune_launches(log_dir: &Path, suffix: &str, keep: usize) {
    let Ok(entries) = std::fs::read_dir(log_dir) else {
        return;
    };
    let mut launches: Vec<String> = entries
        .flatten()
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.starts_with("server_") && name.ends_with(suffix))
        .filter(|name| name != CURRENT_LOG)
        .collect();
    launches.sort_unstable();
    let excess = launches.len().saturating_sub(keep);
    for name in &launches[..excess] {
        let _ = std::fs::remove_file(log_dir.join(name));
    }
}

pub fn store_log_guards(guards: Vec<WorkerGuard>) {
    let _ = LOG_GUARDS.set(guards);
}

/// Whether this process has already installed its log files. Once it has,
/// they are the process's for good: the global subscriber cannot be
/// replaced, and it goes on appending to the file it opened. So a second
/// start in one process (the JNI surface's stop, then start) must not open
/// them again -- the rotation would rename `server_current.log` out from
/// under that writer, leaving the fresh `server_current.log` empty and the
/// live log under an archive's name for the prune to delete.
pub fn log_files_installed() -> bool {
    LOG_GUARDS.get().is_some()
}

pub fn install_panic_hook() {
    static PANIC_HOOK_INSTALLED: OnceLock<()> = OnceLock::new();
    if PANIC_HOOK_INSTALLED.set(()).is_err() {
        return;
    }

    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let current_thread = std::thread::current();
        let thread_name = current_thread.name().unwrap_or("unnamed");
        let location = panic_info
            .location()
            .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()))
            .unwrap_or_else(|| "unknown".to_string());
        let message = panic_message(panic_info);
        let memory = crate::diagnostics::process_memory_snapshot();
        let backtrace = Backtrace::force_capture();

        tracing::error!(
            panic.message = %message,
            panic.location = %location,
            thread.name = %thread_name,
            thread.id = ?current_thread.id(),
            process.id = std::process::id(),
            uptime_secs = uptime_secs(),
            active_direct_streams = active_direct_streams(),
            rss_bytes = memory.rss_bytes,
            virtual_memory_bytes = memory.virtual_memory_bytes,
            backtrace = %backtrace,
            "process panic captured"
        );

        previous(panic_info);
    }));
}

fn panic_message(panic_info: &std::panic::PanicHookInfo<'_>) -> String {
    if let Some(message) = panic_info.payload().downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic_info.payload().downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

pub fn spawn_logged<F>(name: &'static str, fut: F) -> JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let joined = tokio::spawn(fut).await;
        match joined {
            Ok(()) => tracing::warn!(task = name, "long-running task returned"),
            Err(err) if err.is_panic() => {
                tracing::error!(task = name, error = %err, "long-running task panicked")
            }
            Err(err) => tracing::warn!(task = name, error = %err, "long-running task cancelled"),
        }
    })
}

/// The one INFO line that says what this process is: version, paths, and the
/// command line it was started with. Written to both log files.
pub fn log_startup_context(
    config_dir: &Path,
    cache_dir: &Path,
    log_dir: &Path,
    human_log: &Path,
    json_log: &Path,
) {
    log_startup_context_with_args(
        config_dir,
        cache_dir,
        log_dir,
        human_log,
        json_log,
        std::env::args(),
    );
}

/// [`log_startup_context`] over a given command line rather than the
/// process's own, so a test can see what the line would carry.
///
/// The command line goes through [`redacted_args`] first. `main`'s
/// `parse_cli` consumes `--token`, but this reads argv from the OS again, so
/// nothing upstream has scrubbed it; the value is a bearer token that grants
/// the whole control API, and the diagnostics log is what a user pastes to
/// a stranger when asking for help. Everything else on the line is a path
/// or a version, all of which the log names elsewhere already.
fn log_startup_context_with_args(
    config_dir: &Path,
    cache_dir: &Path,
    log_dir: &Path,
    human_log: &Path,
    json_log: &Path,
    args: impl IntoIterator<Item = String>,
) {
    let exe_path = std::env::current_exe()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let args = redacted_args(args);

    tracing::info!(
        server.version = env!("CARGO_PKG_VERSION"),
        git_sha = option_env!("GIT_SHA").unwrap_or("unknown"),
        process.id = std::process::id(),
        executable = %exe_path,
        config_dir = %config_dir.display(),
        cache_dir = %cache_dir.display(),
        log_dir = %log_dir.display(),
        human_log = %human_log.display(),
        json_log = %json_log.display(),
        args = ?args,
        "server startup context"
    );
}

/// What stands in for a secret in the log.
const REDACTED: &str = "<redacted>";

/// The command line as the log may show it: the value of `--token`, in
/// either spelling (`--token <t>`, `--token=<t>`), replaced by
/// [`REDACTED`]. Redaction is by flag name, never by what the value looks
/// like -- a token an operator chose can be short, or a word.
fn redacted_args(args: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut redacted = Vec::new();
    let mut value_is_secret = false;
    for arg in args {
        if value_is_secret {
            redacted.push(REDACTED.to_string());
            value_is_secret = false;
        } else if arg == "--token" {
            value_is_secret = true;
            redacted.push(arg);
        } else if arg.starts_with("--token=") {
            redacted.push(format!("--token={REDACTED}"));
        } else {
            redacted.push(arg);
        }
    }
    redacted
}

pub fn install_native_crash_handler(log_dir: &Path) {
    let crash_dir = log_dir.join("crashes");
    if let Err(err) = std::fs::create_dir_all(&crash_dir) {
        tracing::warn!(error = %err, path = %crash_dir.display(), "failed to create crash dump directory");
        return;
    }

    log_existing_crash_dumps(&crash_dir);

    #[cfg(windows)]
    unsafe {
        install_windows_exception_filter(crash_dir);
    }
}

fn log_existing_crash_dumps(crash_dir: &Path) {
    let Ok(entries) = std::fs::read_dir(crash_dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("dmp") {
            tracing::warn!(dump_path = %path.display(), "previous crash dump found");
        }
    }
}

#[cfg(windows)]
static WINDOWS_CRASH_DIR: OnceLock<PathBuf> = OnceLock::new();

#[cfg(windows)]
unsafe fn install_windows_exception_filter(crash_dir: PathBuf) {
    use windows::Win32::System::Diagnostics::Debug::SetUnhandledExceptionFilter;

    let _ = WINDOWS_CRASH_DIR.set(crash_dir);
    unsafe {
        SetUnhandledExceptionFilter(Some(windows_exception_filter));
    }
}

#[cfg(windows)]
unsafe extern "system" fn windows_exception_filter(
    exception_info: *const windows::Win32::System::Diagnostics::Debug::EXCEPTION_POINTERS,
) -> i32 {
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::{
        Foundation::HANDLE,
        System::{
            Diagnostics::Debug::{
                EXCEPTION_EXECUTE_HANDLER, MINIDUMP_EXCEPTION_INFORMATION,
                MiniDumpWithFullMemoryInfo, MiniDumpWithHandleData, MiniDumpWithThreadInfo,
                MiniDumpWithUnloadedModules, MiniDumpWriteDump,
            },
            Threading::{GetCurrentProcess, GetCurrentProcessId, GetCurrentThreadId},
        },
    };

    if let Some(crash_dir) = WINDOWS_CRASH_DIR.get() {
        let path = crash_dir.join(format!(
            "server_{}_pid{}.dmp",
            Local::now().format("%Y-%m-%d_%H-%M-%S"),
            std::process::id()
        ));

        if let Ok(file) = std::fs::File::create(&path) {
            let exception = MINIDUMP_EXCEPTION_INFORMATION {
                ThreadId: unsafe { GetCurrentThreadId() },
                ExceptionPointers: exception_info as *mut _,
                ClientPointers: false.into(),
            };
            let dump_type = MiniDumpWithThreadInfo
                | MiniDumpWithUnloadedModules
                | MiniDumpWithFullMemoryInfo
                | MiniDumpWithHandleData;

            let result = unsafe {
                MiniDumpWriteDump(
                    GetCurrentProcess(),
                    GetCurrentProcessId(),
                    HANDLE(file.as_raw_handle()),
                    dump_type,
                    Some(&exception),
                    None,
                    None,
                )
            };

            match result {
                Ok(()) => tracing::error!(dump_path = %path.display(), "native crash dump written"),
                Err(err) => {
                    tracing::error!(dump_path = %path.display(), error = %err, "failed to write native crash dump")
                }
            }
        }
    }

    EXCEPTION_EXECUTE_HANDLER
}

pub const MEMORY_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(60);
pub const MEMORY_GROWTH_ALERT_BYTES: u64 = 128 * 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// Everything a scoped subscriber wrote, so a test can grep the line
    /// exactly as it would land in the log files.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn strings(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| arg.to_string()).collect()
    }

    fn names_in(dir: &Path, suffix: &str) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name.ends_with(suffix))
            .collect();
        names.sort();
        names
    }

    /// A start archives the previous launch's `server_current.log` rather
    /// than appending to it, and opens nothing else that repeats its text.
    #[test]
    fn a_start_archives_the_previous_current_log_and_starts_it_empty() {
        let dir = tempfile::tempdir().unwrap();
        let current = dir.path().join(CURRENT_LOG);
        std::fs::write(&current, "the previous launch\n").unwrap();

        let writers = open_log_writers(dir.path()).unwrap();
        drop(writers);

        assert_eq!(std::fs::read_to_string(&current).unwrap(), "");
        let archives: Vec<String> = names_in(dir.path(), ".log")
            .into_iter()
            .filter(|name| name != CURRENT_LOG)
            .collect();
        assert_eq!(archives.len(), 1, "{archives:?}");
        assert_eq!(
            std::fs::read_to_string(dir.path().join(&archives[0])).unwrap(),
            "the previous launch\n"
        );
    }

    /// Two launches whose logs end in the same second both keep their
    /// archive.
    #[test]
    fn an_archive_with_the_same_stamp_is_not_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let current = dir.path().join(CURRENT_LOG);
        std::fs::write(&current, "the second launch\n").unwrap();
        let when = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_600_000_000);
        std::fs::File::options()
            .write(true)
            .open(&current)
            .unwrap()
            .set_modified(when)
            .unwrap();
        let stamp = chrono::DateTime::<Local>::from(when).format("%Y-%m-%d_%H-%M-%S");
        let first = dir.path().join(format!("server_{stamp}.log"));
        std::fs::write(&first, "the first launch\n").unwrap();

        drop(open_log_writers(dir.path()).unwrap());

        assert_eq!(
            std::fs::read_to_string(&first).unwrap(),
            "the first launch\n"
        );
        let second = dir.path().join(format!("server_{stamp}-1.log"));
        assert_eq!(
            std::fs::read_to_string(&second).unwrap(),
            "the second launch\n"
        );
    }

    /// Each start leaves [`KEPT_LAUNCHES`] launches of each kind, the
    /// newest, and deletes the rest.
    #[test]
    fn a_start_keeps_only_the_newest_launches() {
        let dir = tempfile::tempdir().unwrap();
        for second in 10..30 {
            for suffix in [".log", ".jsonl"] {
                let name = format!("server_2020-01-01_00-00-{second}{suffix}");
                std::fs::write(dir.path().join(name), "old").unwrap();
            }
        }

        let writers = open_log_writers(dir.path()).unwrap();
        drop(writers);

        let texts = names_in(dir.path(), ".log");
        assert_eq!(texts.len(), KEPT_LAUNCHES, "{texts:?}");
        assert!(texts.contains(&CURRENT_LOG.to_string()), "{texts:?}");
        assert!(
            texts.contains(&"server_2020-01-01_00-00-29.log".to_string()),
            "the newest archive stays: {texts:?}"
        );
        assert!(
            !texts.contains(&"server_2020-01-01_00-00-20.log".to_string()),
            "{texts:?}"
        );
        let jsons = names_in(dir.path(), ".jsonl");
        assert_eq!(jsons.len(), KEPT_LAUNCHES, "{jsons:?}");
        assert!(
            !jsons
                .iter()
                .any(|name| name.starts_with("server_2020-01-01_00-00-20")),
            "{jsons:?}"
        );
    }

    #[test]
    fn the_token_is_redacted_in_both_spellings_and_nothing_else_is_touched() {
        assert_eq!(
            redacted_args(strings(&["server", "--token", "hunter2", "--verbose"])),
            strings(&["server", "--token", REDACTED, "--verbose"])
        );
        assert_eq!(
            redacted_args(strings(&["--token=hunter2", "--no-auth"])),
            strings(&["--token=<redacted>", "--no-auth"])
        );
        // A trailing `--token` with no value (parse_cli rejects it, but the
        // process gets this far first) leaves nothing to redact and adds
        // nothing.
        assert_eq!(redacted_args(strings(&["--token"])), strings(&["--token"]));
    }

    /// The startup line, rendered by a real subscriber the way the log
    /// files render it, carries the flag and not the secret -- the check
    /// the README's "the token never passes through `tracing`" rests on.
    #[test]
    fn the_startup_line_does_not_carry_the_token() {
        let captured = Captured::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        let dir = Path::new("nowhere");
        tracing::subscriber::with_default(subscriber, || {
            log_startup_context_with_args(
                dir,
                dir,
                dir,
                dir,
                dir,
                strings(&[
                    "server",
                    "--token",
                    "s3cret-flag-value",
                    "--token=s3cret-eq-value",
                ]),
            );
        });
        let line = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
        assert!(line.contains("server startup context"), "{line}");
        assert!(line.contains("--token"), "the flag itself stays: {line}");
        assert!(!line.contains("s3cret-flag-value"), "{line}");
        assert!(!line.contains("s3cret-eq-value"), "{line}");
    }
}
