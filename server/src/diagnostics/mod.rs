pub mod dht_health;
pub mod logging;

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// What the panic line reports of this process's memory.
#[derive(Debug, Clone)]
pub struct ProcessMemorySnapshot {
    pub rss_bytes: u64,
    pub virtual_memory_bytes: u64,
}

/// This process's memory, and nothing else's. Read by the panic hook, so it
/// runs while something is already going wrong.
///
/// `System::new_all()` + `refresh_all()` would enumerate every process on
/// the machine through `/proc` -- CPU, memory, disks, networks, the lot --
/// to read one pid's RSS. Refreshing this pid alone, for memory alone, is a
/// handful of reads of `/proc/self`.
pub fn process_memory_snapshot() -> ProcessMemorySnapshot {
    let pid = Pid::from_u32(std::process::id());
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing().with_memory(),
    );

    let process = system.process(pid);
    ProcessMemorySnapshot {
        rss_bytes: process.map(|process| process.memory()).unwrap_or(0),
        virtual_memory_bytes: process.map(|process| process.virtual_memory()).unwrap_or(0),
    }
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
    /// are mostly file-backed keeps *below* its working set -- so the two
    /// are never compared against each other here, only against zero.
    #[test]
    fn the_process_snapshot_reads_this_process_s_memory() {
        let snapshot = process_memory_snapshot();
        assert!(snapshot.rss_bytes > 0, "a running process occupies memory");
        assert!(snapshot.virtual_memory_bytes > 0, "and has a virtual size");
    }
}
