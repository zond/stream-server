pub mod logging;

use std::{collections::HashSet, time::Instant};

use serde::Serialize;
use sysinfo::{Pid, System};

use crate::state::AppState;

#[derive(Debug, Clone, Serialize)]
pub struct ProcessMemorySnapshot {
    pub pid: u32,
    pub rss_bytes: u64,
    pub virtual_memory_bytes: u64,
    pub thread_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemorySnapshot {
    pub process: ProcessMemorySnapshot,
    pub engine: enginefs::EngineDiagnosticsSnapshot,
    pub download_engine: enginefs::EngineDiagnosticsSnapshot,
    pub download_disk_cache_bytes: u64,
    pub download_disk_cache_files: u64,
    pub active_disk_downloads: u64,
    pub disk_download_root: String,
    pub download_storage_mode: &'static str,
    pub download_disk_backed_available: bool,
    pub archive_session_count: usize,
    pub nzb_session_count: usize,
    pub active_direct_streams: u64,
}

pub fn process_memory_snapshot() -> ProcessMemorySnapshot {
    let pid_u32 = std::process::id();
    let mut system = System::new_all();
    system.refresh_all();

    let process = system.process(Pid::from_u32(pid_u32));
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

async fn memory_snapshot_for_state(state: &AppState) -> MemorySnapshot {
    let stream_engine = state.stream_engine();
    let stream_engine_snapshot = stream_engine.diagnostics_snapshot().await;
    let download_engine = state.download_engine.diagnostics_snapshot().await;
    let (download_disk_cache_bytes, download_disk_cache_files) =
        disk_tree_stats(&state.download_engine.download_dir);
    let mut active_disk_files = HashSet::new();
    for stream in &download_engine.streams.active_file_streams {
        if stream.count > 0 {
            active_disk_files.insert((stream.info_hash.clone(), stream.file_idx));
        }
    }
    for lease in &download_engine.streams.active_playback_leases {
        active_disk_files.insert((lease.info_hash.clone(), lease.file_idx));
    }
    for selection in &download_engine.streams.active_multifile_selections {
        active_disk_files.insert((selection.info_hash.clone(), selection.file_idx));
    }
    let active_disk_downloads = active_disk_files.len() as u64;

    MemorySnapshot {
        process: process_memory_snapshot(),
        engine: stream_engine_snapshot,
        download_engine,
        download_disk_cache_bytes,
        download_disk_cache_files,
        active_disk_downloads,
        disk_download_root: state.download_engine.download_dir.display().to_string(),
        download_storage_mode: "dynamic",
        download_disk_backed_available: state.download_engine_disk_backed,
        archive_session_count: state.archive_cache.len(),
        nzb_session_count: state.nzb_sessions.len(),
        active_direct_streams: logging::active_direct_streams(),
    }
}

fn disk_tree_stats(root: &std::path::Path) -> (u64, u64) {
    if !root.exists() {
        return (0, 0);
    }

    let mut bytes = 0u64;
    let mut files = 0u64;
    for entry in walkdir::WalkDir::new(root).into_iter().flatten() {
        if !entry.file_type().is_file() {
            continue;
        }
        if entry
            .path()
            .components()
            .any(|component| component.as_os_str() == ".metadata")
        {
            continue;
        }
        if let Ok(metadata) = entry.metadata() {
            // Occupancy, not apparent length -- librqbit pre-allocates
            // wanted files at full size, so `len()` here reported a phone's
            // cache at four times what was on the disk. See
            // `cache_cleaner::occupied_bytes`.
            bytes = bytes.saturating_add(crate::cache_cleaner::occupied_bytes(&metadata));
            files = files.saturating_add(1);
        }
    }
    (bytes, files)
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
            let snapshot = memory_snapshot_for_state(&state).await;
            let rss = snapshot.process.rss_bytes;
            let growth = rss.saturating_sub(last_rss);
            let should_log_periodic =
                last_snapshot_log.elapsed() >= logging::MEMORY_SNAPSHOT_INTERVAL;
            let should_log_growth = growth >= logging::MEMORY_GROWTH_ALERT_BYTES;

            if should_log_periodic || should_log_growth {
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
                    idle_paused_torrents = snapshot.engine.streams.idle_paused_torrents.len(),
                    download_active_multifile_selections =
                        snapshot.download_engine.streams.active_multifile_selections.len(),
                    download_idle_paused_torrents =
                        snapshot.download_engine.streams.idle_paused_torrents.len(),
                    rust_piece_cache_entries = snapshot.engine.memory.rust_piece_cache_entries,
                    rust_piece_cache_bytes = snapshot.engine.memory.rust_piece_cache_bytes,
                    native_storage_bytes = snapshot.engine.memory.native_storage_bytes,
                    native_storage_pieces = snapshot.engine.memory.native_storage_pieces,
                    download_disk_cache_bytes = snapshot.download_disk_cache_bytes,
                    download_disk_cache_files = snapshot.download_disk_cache_files,
                    active_disk_downloads = snapshot.active_disk_downloads,
                    disk_download_root = %snapshot.disk_download_root,
                    download_storage_mode = snapshot.download_storage_mode,
                    download_disk_backed_available = snapshot.download_disk_backed_available,
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
