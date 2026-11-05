//! Single-process and system memory probes for warm-pool garbage collection.
//!
//! Tree measurement belongs to cairn-procmem and registry-owned shared watches.
pub use cairn_common::memory::SystemMemory;

/// GC policy input, independently stubbed without spawning a process.
pub trait MemoryProbe: Send + Sync {
    fn process_rss_bytes(&self, pid: u32) -> Option<u64>;
    fn system_memory(&self) -> Option<SystemMemory>;
}
pub struct OsMemoryProbe;
impl MemoryProbe for OsMemoryProbe {
    fn process_rss_bytes(&self, pid: u32) -> Option<u64> {
        cairn_procmem::platform::process_rss_bytes(pid)
    }
    fn system_memory(&self) -> Option<SystemMemory> {
        cairn_common::memory::system_memory()
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone, Default)]
pub struct StubMemoryProbe {
    pub system: Option<SystemMemory>,
    pub rss: std::collections::HashMap<u32, u64>,
}
#[cfg(any(test, feature = "test-utils"))]
impl StubMemoryProbe {
    pub fn new(system: Option<SystemMemory>) -> Self {
        Self {
            system,
            ..Self::default()
        }
    }
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
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn os_probe_measures_self_and_system() {
        let probe = OsMemoryProbe;
        assert!(probe
            .process_rss_bytes(std::process::id())
            .is_some_and(|bytes| bytes > 0));
        let system = probe.system_memory().expect("system memory");
        assert!(system.total > 0 && system.available > 0 && system.available <= system.total);
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
        assert_eq!(probe.system_memory().unwrap().available, 8);
    }
}
