//! Canonical system-wide memory availability measurement.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemMemory {
    pub total: u64,
    pub available: u64,
}

pub fn system_memory() -> Option<SystemMemory> {
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
        return Some(SystemMemory {
            total: status.ullTotalPhys,
            available: status.ullAvailPhys.min(status.ullTotalPhys),
        });
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
        return Some(SystemMemory {
            total,
            available: (stats.free_count as u64 + stats.inactive_count as u64)
                .saturating_mul(page_size as u64),
        });
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
                return Some(SystemMemory {
                    total,
                    available: total.saturating_sub(current),
                });
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
        return Some(SystemMemory {
            total: total?.saturating_mul(1024),
            available: available?.saturating_mul(1024),
        });
    }
    #[allow(unreachable_code)]
    None
}
