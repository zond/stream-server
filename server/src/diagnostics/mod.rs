pub mod dht_health;
pub mod logging;

use serde::Serialize;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

#[derive(Debug, Clone, Serialize)]
pub struct ProcessMemorySnapshot {
    pub pid: u32,
    pub rss_bytes: u64,
    pub virtual_memory_bytes: u64,
    pub thread_count: u64,
}

/// This process's memory, and nothing else's. Read by the panic hook, so it
/// runs while something is already going wrong.
///
/// `System::new_all()` + `refresh_all()` enumerated every process on the
/// machine through `/proc` -- CPU, memory, disks, networks, the lot -- to
/// read one pid's RSS. Refreshing this pid alone, for memory alone, is a
/// handful of reads of `/proc/self`.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The single-process refresh reads what the panic line needs: this
    /// process's memory. A refresh kind without memory in it would leave
    /// both figures at zero, quietly -- and a panic report that says a
    /// process used no memory is worse than one that says nothing.
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
}
