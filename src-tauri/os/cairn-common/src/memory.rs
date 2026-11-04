//! Canonical system-wide memory availability measurement.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemMemory {
    pub total: u64,
    pub available: u64,
}

/// What a [`SystemMemory`] reading measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryScope {
    /// The whole machine's physical memory.
    Host,
    /// The memory limit on this process's cgroup, reported in place of the
    /// host's because it is what bounds this process. It does not bound what a
    /// device driver allocates for other processes, so it is not a unified
    /// GPU's pool.
    CgroupLimit,
}

pub fn system_memory() -> Option<SystemMemory> {
    system_memory_scoped().map(|(memory, _)| memory)
}

/// This process's own resident memory, in bytes: the physical footprint on
/// macOS (`proc_pid_rusage`'s `ri_phys_footprint`, which is what `footprint`
/// reports and what memory-pressure kills count, compressed pages included)
/// and `VmRSS` on Linux. `None` where it cannot be read; a caller enforcing a
/// memory cap should treat that as a reason to stop, not to carry on.
pub fn own_resident_bytes() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        // SAFETY: `proc_pid_rusage` fills a zeroed `rusage_info_v4` for our own
        // pid; the cast matches the C `(rusage_info_t *)&buf` convention.
        let mut info: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
        let rc = unsafe {
            libc::proc_pid_rusage(
                libc::getpid(),
                libc::RUSAGE_INFO_V4,
                &mut info as *mut _ as *mut libc::rusage_info_t,
            )
        };
        return (rc == 0).then_some(info.ri_phys_footprint);
    }
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let line = status
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))?;
        let kib: u64 = line.split_whitespace().next()?.parse().ok()?;
        return Some(kib.saturating_mul(1024));
    }
    #[allow(unreachable_code)]
    None
}

/// [`system_memory`] together with what it measured. The one implementation
/// of the host-or-cgroup choice, so a reader asking whether this reading is
/// the host's cannot come to answer it differently from the reading itself.
pub fn system_memory_scoped() -> Option<(SystemMemory, MemoryScope)> {
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};

        let mut status = MEMORYSTATUSEX {
            dwLength: std::mem::size_of::<MEMORYSTATUSEX>() as u32,
            ..unsafe { std::mem::zeroed() }
        };
        if unsafe { GlobalMemoryStatusEx(&mut status) } == 0 {
            return None;
        }
        return Some((
            SystemMemory {
                total: status.ullTotalPhys,
                available: status.ullAvailPhys.min(status.ullTotalPhys),
            },
            MemoryScope::Host,
        ));
    }
    #[cfg(target_os = "macos")]
    {
        let mut mib = [libc::CTL_HW, libc::HW_MEMSIZE];
        let mut total = 0_u64;
        let mut len = std::mem::size_of::<u64>();
        if unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                mib.len() as libc::c_uint,
                &mut total as *mut _ as *mut libc::c_void,
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        } != 0
        {
            return None;
        }
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            return None;
        }
        let mut stats: libc::vm_statistics64 = unsafe { std::mem::zeroed() };
        let mut count = (std::mem::size_of::<libc::vm_statistics64>()
            / std::mem::size_of::<libc::integer_t>())
            as libc::mach_msg_type_number_t;
        #[allow(deprecated)]
        let result = unsafe {
            libc::host_statistics64(
                libc::mach_host_self(),
                libc::HOST_VM_INFO64,
                &mut stats as *mut _ as *mut libc::integer_t,
                &mut count,
            )
        };
        if result != 0 {
            return None;
        }
        return Some((
            SystemMemory {
                total,
                available: (stats.free_count as u64 + stats.inactive_count as u64)
                    .saturating_mul(page_size as u64),
            },
            MemoryScope::Host,
        ));
    }
    #[cfg(target_os = "linux")]
    {
        if let (Ok(max), Ok(current)) = (
            std::fs::read_to_string("/sys/fs/cgroup/memory.max"),
            std::fs::read_to_string("/sys/fs/cgroup/memory.current"),
        ) {
            if max.trim() != "max" {
                let total = max.trim().parse::<u64>().ok()?;
                let current = current.trim().parse::<u64>().ok()?;
                return Some((
                    SystemMemory {
                        total,
                        available: total.saturating_sub(current),
                    },
                    MemoryScope::CgroupLimit,
                ));
            }
        }
        let content = std::fs::read_to_string("/proc/meminfo").ok()?;
        let mut total = None;
        let mut available = None;
        for line in content.lines() {
            if let Some(value) = line.strip_prefix("MemTotal:") {
                total = value.split_whitespace().next()?.parse::<u64>().ok();
            } else if let Some(value) = line.strip_prefix("MemAvailable:") {
                available = value.split_whitespace().next()?.parse::<u64>().ok();
            }
        }
        return Some((
            SystemMemory {
                total: total?.saturating_mul(1024),
                available: available?.saturating_mul(1024),
            },
            MemoryScope::Host,
        ));
    }
    #[allow(unreachable_code)]
    None
}

#[cfg(test)]
mod tests {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn reads_this_process_resident_memory() {
        let resident = super::own_resident_bytes().expect("readable on this platform");
        assert!(resident > 1024 * 1024, "{resident} bytes is not a process");
    }
}
