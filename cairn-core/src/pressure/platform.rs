//! Per-process resource reads: memory footprint, RSS, cumulative CPU time, and
//! (macOS) billed energy.
//!
//! macOS reads `proc_pid_rusage(RUSAGE_INFO_V4)`, whose `ri_phys_footprint` is
//! the compressed-memory-aware footprint Activity Monitor reports — plain RSS
//! underestimates on Apple Silicon. That single call also yields RSS, user +
//! system CPU time (nanoseconds), and billed energy. Linux reads
//! `/proc/self/statm` for RSS and `getrusage(RUSAGE_SELF)` for CPU; it has no
//! phys-footprint analogue. Other targets (incl. Windows) return an empty
//! reading — the alloc counters and tokio metrics still carry useful signal
//! there, and a native Windows memory path is future work.

/// A single read of this process's resource usage.
#[derive(Debug, Clone, Copy, Default)]
pub struct ResourceReading {
    /// Compressed-memory-aware physical footprint, bytes (macOS only).
    pub phys_footprint_bytes: Option<u64>,
    /// Resident set size, bytes.
    pub rss_bytes: u64,
    /// Cumulative user + system CPU time across all threads, nanoseconds.
    pub cpu_time_nanos: u64,
    /// Cumulative billed energy, nanojoules (macOS; `None` when 0/unavailable —
    /// it reads 0 on some macOS versions, so 0 is treated as "no signal").
    pub energy_nanojoules: Option<u64>,
}

impl ResourceReading {
    /// CPU utilization between two readings taken `elapsed_nanos` apart,
    /// expressed as a percentage of ONE core (can exceed 100 across cores).
    /// `None` when no wall time elapsed.
    pub fn cpu_percent_since(&self, prev: &ResourceReading, elapsed_nanos: u128) -> Option<f64> {
        if elapsed_nanos == 0 {
            return None;
        }
        let delta = self.cpu_time_nanos.saturating_sub(prev.cpu_time_nanos) as f64;
        Some(delta / elapsed_nanos as f64 * 100.0)
    }
}

#[cfg(target_os = "macos")]
pub fn read_process_resources() -> ResourceReading {
    // SAFETY: `proc_pid_rusage` fills a zeroed `rusage_info_v4` for our own pid;
    // the cast matches the C `(rusage_info_t *)&buf` calling convention.
    unsafe {
        let mut info: libc::rusage_info_v4 = std::mem::zeroed();
        let rc = libc::proc_pid_rusage(
            libc::getpid(),
            libc::RUSAGE_INFO_V4,
            &mut info as *mut _ as *mut libc::rusage_info_t,
        );
        if rc != 0 {
            return ResourceReading::default();
        }
        ResourceReading {
            phys_footprint_bytes: Some(info.ri_phys_footprint),
            rss_bytes: info.ri_resident_size,
            cpu_time_nanos: info.ri_user_time.saturating_add(info.ri_system_time),
            energy_nanojoules: (info.ri_billed_energy > 0).then_some(info.ri_billed_energy),
        }
    }
}

#[cfg(target_os = "linux")]
pub fn read_process_resources() -> ResourceReading {
    ResourceReading {
        phys_footprint_bytes: None,
        rss_bytes: read_linux_rss_bytes().unwrap_or(0),
        cpu_time_nanos: read_getrusage_cpu_nanos(),
        energy_nanojoules: None,
    }
}

#[cfg(target_os = "linux")]
fn read_linux_rss_bytes() -> Option<u64> {
    // /proc/self/statm fields are in pages; field index 1 is resident pages.
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    // SAFETY: sysconf with a constant name; returns the page size or -1.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page = if page > 0 { page as u64 } else { 4096 };
    Some(resident_pages.saturating_mul(page))
}

#[cfg(target_os = "linux")]
fn read_getrusage_cpu_nanos() -> u64 {
    // SAFETY: getrusage fills a zeroed rusage for RUSAGE_SELF.
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) != 0 {
            return 0;
        }
        timeval_nanos(usage.ru_utime).saturating_add(timeval_nanos(usage.ru_stime))
    }
}

#[cfg(target_os = "linux")]
fn timeval_nanos(tv: libc::timeval) -> u64 {
    (tv.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add((tv.tv_usec as u64).saturating_mul(1_000))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn read_process_resources() -> ResourceReading {
    ResourceReading::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn reads_nonzero_process_resources_on_host() {
        let reading = read_process_resources();
        assert!(
            reading.rss_bytes > 0,
            "RSS should be nonzero for this process"
        );
        assert!(
            reading.cpu_time_nanos > 0,
            "cumulative CPU time should be nonzero for this process"
        );
        #[cfg(target_os = "macos")]
        assert!(
            reading.phys_footprint_bytes.unwrap_or(0) > 0,
            "phys_footprint should be nonzero on macOS"
        );
    }

    #[test]
    fn cpu_percent_since_computes_utilization() {
        let prev = ResourceReading {
            cpu_time_nanos: 1_000_000_000,
            ..Default::default()
        };
        let now = ResourceReading {
            cpu_time_nanos: 1_500_000_000,
            ..Default::default()
        };
        // 0.5s of CPU over 1s of wall time = 50% of one core.
        assert_eq!(now.cpu_percent_since(&prev, 1_000_000_000), Some(50.0));
        assert_eq!(now.cpu_percent_since(&prev, 0), None);
    }
}
