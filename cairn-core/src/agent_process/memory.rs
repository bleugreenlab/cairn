//! Memory probe: per-process resident memory and system-wide memory
//! availability, used to size the warm-process pool against real headroom
//! rather than an arbitrary count (CAIRN-2543).
//!
//! Direct `libc` FFI, following the `services/reaper.rs` precedent (which
//! deliberately rejected pulling in a system-info crate). macOS reads task and
//! host statistics through mach; Linux reads `/proc` (and cgroup v2 limits when
//! running memory-capped in a container). Every other platform returns `None`,
//! which makes the GC fall back to its count cap.

/// A snapshot of system-wide memory in bytes.
pub use cairn_common::memory::SystemMemory;

/// A source of memory measurements. Abstracted behind a trait so the GC's
/// budget policy is unit-testable with a stub, without spawning real processes.
pub trait MemoryProbe: Send + Sync {
    /// Resident set size of `pid` in bytes, or `None` when it cannot be
    /// measured (process gone, not inspectable, or unsupported platform).
    fn process_rss_bytes(&self, pid: u32) -> Option<u64>;

    /// System-wide memory, or `None` when it cannot be measured. `None` makes
    /// the GC size the pool by count instead of by headroom.
    fn system_memory(&self) -> Option<SystemMemory>;
}

/// Production probe backed by OS calls.
pub struct OsMemoryProbe;

impl MemoryProbe for OsMemoryProbe {
    fn process_rss_bytes(&self, pid: u32) -> Option<u64> {
        #[cfg(target_os = "macos")]
        {
            process_rss_macos(pid)
        }
        #[cfg(target_os = "linux")]
        {
            process_rss_linux(pid)
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            let _ = pid;
            None
        }
    }

    fn system_memory(&self) -> Option<SystemMemory> {
        cairn_common::memory::system_memory()
    }
}

// ============================================================================
// macOS
// ============================================================================

/// RSS via `proc_pidinfo(PROC_PIDTASKINFO)`. `None` when the process is gone
/// (ESRCH), not inspectable (EPERM), or the call returns a short read.
#[cfg(target_os = "macos")]
fn process_rss_macos(pid: u32) -> Option<u64> {
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    let ret = unsafe {
        libc::proc_pidinfo(
            pid as libc::c_int,
            libc::PROC_PIDTASKINFO,
            0,
            &mut info as *mut _ as *mut libc::c_void,
            size,
        )
    };
    // proc_pidinfo returns bytes written; a short read means the struct is not
    // reliably populated, and <= 0 is an error.
    if ret < size {
        return None;
    }
    Some(info.pti_resident_size)
}

// ============================================================================
// Linux
// ============================================================================

/// RSS via `VmRSS` in `/proc/<pid>/status` (reported in kB).
#[cfg(target_os = "linux")]
fn process_rss_linux(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn system_memory_linux() -> Option<SystemMemory> {
    // A memory-capped container reports host RAM in /proc/meminfo but is killed
    // against its cgroup limit, so prefer the cgroup v2 accounting when present.
    if let Some(sm) = cgroup_v2_memory() {
        return Some(sm);
    }
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let mut total = None;
    let mut available = None;
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            total = rest
                .split_whitespace()
                .next()
                .and_then(|v| v.parse::<u64>().ok())
                .map(|kb| kb * 1024);
        } else if let Some(rest) = line.strip_prefix("MemAvailable:") {
            available = rest
                .split_whitespace()
                .next()
                .and_then(|v| v.parse::<u64>().ok())
                .map(|kb| kb * 1024);
        }
    }
    Some(SystemMemory {
        total: total?,
        available: available?,
    })
}

/// cgroup v2 memory accounting. `None` when the files are absent (cgroup v1 or
/// no container) or the limit is unbounded (`max`), so the caller falls back to
/// host `/proc/meminfo`.
#[cfg(target_os = "linux")]
fn cgroup_v2_memory() -> Option<SystemMemory> {
    let max_raw = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    let max_raw = max_raw.trim();
    if max_raw == "max" {
        return None;
    }
    let total: u64 = max_raw.parse().ok()?;
    let current: u64 = std::fs::read_to_string("/sys/fs/cgroup/memory.current")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(SystemMemory {
        total,
        available: total.saturating_sub(current),
    })
}

// ============================================================================
// Test stub
// ============================================================================

/// Deterministic probe for unit tests: canned system memory and per-pid RSS.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone, Default)]
pub struct StubMemoryProbe {
    pub(crate) system: Option<SystemMemory>,
    pub(crate) rss: std::collections::HashMap<u32, u64>,
}

#[cfg(any(test, feature = "test-utils"))]
impl StubMemoryProbe {
    /// A probe reporting the given system memory (or `None` to force the GC's
    /// count-based fallback path) and no measurable per-process RSS.
    pub fn new(system: Option<SystemMemory>) -> Self {
        Self {
            system,
            rss: std::collections::HashMap::new(),
        }
    }

    /// Register a measurable RSS for `pid`.
    pub fn with_rss(mut self, pid: u32, bytes: u64) -> Self {
        self.rss.insert(pid, bytes);
        self
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl MemoryProbe for StubMemoryProbe {
    fn process_rss_bytes(&self, pid: u32) -> Option<u64> {
        self.rss.get(&pid).copied()
    }

    fn system_memory(&self) -> Option<SystemMemory> {
        self.system
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The real probe can measure this test process and the host on the
    /// platforms it supports. Mirrors `reaper.rs`'s real-process integration
    /// test: a smoke check that the FFI is wired correctly.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn os_probe_measures_self_and_system() {
        let probe = OsMemoryProbe;
        let rss = probe.process_rss_bytes(std::process::id());
        assert!(
            rss.map(|b| b > 0).unwrap_or(false),
            "self RSS should be a positive measurement, got {rss:?}"
        );
        let sys = probe.system_memory().expect("system memory measurable");
        assert!(sys.total > 0, "total memory should be positive");
        assert!(sys.available > 0, "available memory should be positive");
        assert!(
            sys.available <= sys.total,
            "available ({}) must not exceed total ({})",
            sys.available,
            sys.total
        );
    }

    #[test]
    fn stub_probe_reports_canned_values() {
        let probe = StubMemoryProbe::new(Some(SystemMemory {
            total: 16,
            available: 8,
        }))
        .with_rss(42, 1234);
        assert_eq!(probe.process_rss_bytes(42), Some(1234));
        assert_eq!(probe.process_rss_bytes(99), None);
        assert_eq!(probe.system_memory().map(|s| s.available), Some(8));
    }
}
