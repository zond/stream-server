pub mod dht_health;
pub mod logging;

use std::{collections::HashSet, time::Instant};

use serde::Serialize;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

use crate::state::AppState;

#[derive(Debug, Clone, Serialize)]
pub struct ProcessMemorySnapshot {
    pub pid: u32,
    pub rss_bytes: u64,
    pub virtual_memory_bytes: u64,
    pub thread_count: u64,
}

/// What the cache cleaner's last pass found, as the sampler reports it: the
/// occupancy of the walked roots, how much of it protection holds, and how
/// long ago the pass finished. `None` until the first pass has run.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct CacheFigures {
    pub total_bytes: u64,
    pub protected_bytes: u64,
    pub report_age_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemorySnapshot {
    pub process: ProcessMemorySnapshot,
    pub engine: enginefs::EngineDiagnosticsSnapshot,
    /// The cache's size as the cleaner last counted it (see
    /// `cache_cleaner::LastEviction`). The sampler used to walk the whole
    /// download dir for this itself, synchronously, on the runtime, every
    /// thirty seconds -- twice the cleaner's debounce and a hundred and
    /// twenty times its idle fallback -- for two numbers it logged once a
    /// minute at most. The cleaner's count is the same tree, at most a
    /// minute old while anything is writing and exactly current while
    /// nothing is, and it costs this task nothing.
    pub cache: Option<CacheFigures>,
    pub active_disk_downloads: u64,
    pub disk_download_root: String,
    pub archive_session_count: usize,
    pub nzb_session_count: usize,
    pub active_direct_streams: u64,
}

/// This process's memory, and nothing else's.
///
/// `System::new_all()` + `refresh_all()` enumerated every process on the
/// machine through `/proc` -- CPU, memory, disks, networks, the lot -- to
/// read one pid's RSS, and did it every thirty seconds. Refreshing this pid
/// alone, for memory alone, is a handful of reads of `/proc/self`.
pub fn process_memory_snapshot() -> ProcessMemorySnapshot {
    let pid_u32 = std::process::id();
    let pid = Pid::from_u32(pid_u32);
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing().with_memory(),
    );

    let process = system.process(pid);
    ProcessMemorySnapshot {
        pid: pid_u32,
        rss_bytes: process.map(|process| process.memory()).unwrap_or(0),
        virtual_memory_bytes: process.map(|process| process.virtual_memory()).unwrap_or(0),
        thread_count: current_thread_count(),
    }
}

fn current_thread_count() -> u64 {
    current_thread_count_impl()
}

#[cfg(windows)]
fn current_thread_count_impl() -> u64 {
    use windows::Win32::{
        Foundation::CloseHandle,
        System::Diagnostics::ToolHelp::{
            CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
        },
    };

    unsafe {
        let Ok(snapshot) = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) else {
            return 0;
        };

        let mut entry = THREADENTRY32 {
            dwSize: std::mem::size_of::<THREADENTRY32>() as u32,
            ..Default::default()
        };
        let pid = std::process::id();
        let mut count = 0u64;

        if Thread32First(snapshot, &mut entry).is_ok() {
            loop {
                if entry.th32OwnerProcessID == pid {
                    count += 1;
                }

                if Thread32Next(snapshot, &mut entry).is_err() {
                    break;
                }
            }
        }

        let _ = CloseHandle(snapshot);
        count
    }
}

#[cfg(not(windows))]
fn current_thread_count_impl() -> u64 {
    0
}

/// Everything the periodic line reports besides the process figures, which
/// the caller has already taken (they decide whether a line is logged at
/// all). Async engine snapshots and a mutex read; no filesystem.
async fn memory_snapshot_for_state(
    state: &AppState,
    process: ProcessMemorySnapshot,
) -> MemorySnapshot {
    let engine = state.engine.diagnostics_snapshot().await;
    let mut active_disk_files = HashSet::new();
    for stream in &engine.streams.active_file_streams {
        if stream.count > 0 {
            active_disk_files.insert((stream.info_hash.clone(), stream.file_idx));
        }
    }
    for selection in &engine.streams.active_multifile_selections {
        active_disk_files.insert((selection.info_hash.clone(), selection.file_idx));
    }
    let active_disk_downloads = active_disk_files.len() as u64;

    MemorySnapshot {
        process,
        engine,
        cache: cache_figures(&state.last_eviction),
        active_disk_downloads,
        disk_download_root: state.engine.download_dir.display().to_string(),
        archive_session_count: state.archive_cache.len(),
        nzb_session_count: state.nzb_sessions.len(),
        active_direct_streams: logging::active_direct_streams(),
    }
}

/// The cleaner's last count, in the shape the line logs it.
fn cache_figures(last_eviction: &crate::cache_cleaner::LastEviction) -> Option<CacheFigures> {
    let (age, report) = last_eviction.get()?;
    Some(CacheFigures {
        total_bytes: report.total,
        protected_bytes: report.protected,
        report_age_secs: age.as_secs(),
    })
}

pub fn start_memory_sampler(state: AppState) -> tokio::task::JoinHandle<()> {
    logging::spawn_logged("memory-sampler", async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        let mut last_snapshot_log = Instant::now()
            .checked_sub(logging::MEMORY_SNAPSHOT_INTERVAL)
            .unwrap_or_else(Instant::now);
        let mut last_rss = 0u64;

        loop {
            interval.tick().await;
            // The process figures decide whether anything is logged this
            // tick, so they are all that is read on a tick that logs
            // nothing -- which in steady state is every other one.
            let process = process_memory_snapshot();
            let rss = process.rss_bytes;
            let growth = rss.saturating_sub(last_rss);
            let should_log_periodic =
                last_snapshot_log.elapsed() >= logging::MEMORY_SNAPSHOT_INTERVAL;
            let should_log_growth = growth >= logging::MEMORY_GROWTH_ALERT_BYTES;

            if should_log_periodic || should_log_growth {
                let snapshot = memory_snapshot_for_state(&state, process).await;
                tracing::info!(
                    rss_bytes = snapshot.process.rss_bytes,
                    virtual_memory_bytes = snapshot.process.virtual_memory_bytes,
                    thread_count = snapshot.process.thread_count,
                    engine_count = snapshot.engine.streams.engine_count,
                    engine_active_streams = snapshot.engine.streams.engine_active_streams,
                    active_file_priority_generation =
                        snapshot.engine.streams.active_file_priority_generation,
                    active_stream_hashes = snapshot.engine.streams.active_streams.len(),
                    active_file_streams = snapshot.engine.streams.active_file_streams.len(),
                    active_multifile_selections =
                        snapshot.engine.streams.active_multifile_selections.len(),
                    paused_torrents = snapshot.engine.streams.paused_torrents.len(),
                    rust_piece_cache_entries = snapshot.engine.memory.rust_piece_cache_entries,
                    rust_piece_cache_bytes = snapshot.engine.memory.rust_piece_cache_bytes,
                    native_storage_bytes = snapshot.engine.memory.native_storage_bytes,
                    native_storage_pieces = snapshot.engine.memory.native_storage_pieces,
                    cache_bytes = snapshot.cache.map_or(0, |cache| cache.total_bytes),
                    cache_protected_bytes = snapshot.cache.map_or(0, |cache| cache.protected_bytes),
                    cache_report_age_secs = ?snapshot.cache.map(|cache| cache.report_age_secs),
                    active_disk_downloads = snapshot.active_disk_downloads,
                    disk_download_root = %snapshot.disk_download_root,
                    waiter_keys = snapshot.engine.memory.waiter_keys,
                    waiter_wakers = snapshot.engine.memory.waiter_wakers,
                    archive_session_count = snapshot.archive_session_count,
                    nzb_session_count = snapshot.nzb_session_count,
                    active_direct_streams = snapshot.active_direct_streams,
                    growth_bytes = growth,
                    growth_alert = should_log_growth,
                    "memory diagnostics snapshot"
                );
                last_snapshot_log = Instant::now();
                last_rss = rss;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The single-process refresh reads what the periodic line needs: this
    /// process's memory. A refresh kind without memory in it would leave the
    /// figure at zero and the growth alert blind, quietly.
    ///
    /// Both figures are asserted non-zero and nothing more. On Unix the
    /// virtual size is at least the resident set, but on Windows sysinfo's
    /// `virtual_memory()` is the pagefile commit, which a process whose pages
    /// are mostly file-backed keeps *below* its working set -- CI's first
    /// Windows run of this test failed on exactly that relation.
    #[test]
    fn the_process_snapshot_reads_this_process_s_memory() {
        let snapshot = process_memory_snapshot();
        assert_eq!(snapshot.pid, std::process::id());
        assert!(snapshot.rss_bytes > 0, "a running process occupies memory");
        assert!(snapshot.virtual_memory_bytes > 0, "and has a virtual size");
    }

    /// The cache figures are the cleaner's, read back: nothing before a pass,
    /// and the pass's own totals with their age after one.
    #[test]
    fn the_cache_figures_are_the_cleaners_last_report() {
        let last = crate::cache_cleaner::LastEviction::default();
        assert!(cache_figures(&last).is_none(), "no pass has run yet");

        last.record(&crate::cache_cleaner::EvictionReport {
            total: 3_850_000_000,
            protected: 700_000_000,
            ..Default::default()
        });
        let figures = cache_figures(&last).expect("a pass has run");
        assert_eq!(figures.total_bytes, 3_850_000_000);
        assert_eq!(figures.protected_bytes, 700_000_000);
        assert!(figures.report_age_secs < 60);
    }
}
