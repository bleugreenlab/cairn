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
        return cairn_procmem::platform::process_footprint_bytes(std::process::id());
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
        // A container's cgroup namespace makes its own cgroup the root, so a
        // limit there is the machine as far as this process can tell.
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
        let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
        return parse_meminfo(&meminfo).map(|memory| (memory, MemoryScope::Host));
    }
    #[allow(unreachable_code)]
    None
}

/// The memory this process's own work may still use: [`system_memory`],
/// narrowed by the tightest memory limit on this process's cgroup ancestry
/// when one is tighter than it.
///
/// The difference from [`system_memory`] is a limit below the root, such as
/// the `MemoryMax=` on the executor's own systemd unit. That limit bounds the
/// executor and every command it runs, and nothing else on the machine (a
/// model server in a container, the operator's own processes), so it is what
/// the executor admits its own work against, while the machine reading it
/// reports to the runner, which places work that is not the executor's too,
/// stays the machine's.
pub fn executor_admissible_memory() -> Option<SystemMemory> {
    #[cfg(target_os = "linux")]
    {
        let bound = std::fs::read_to_string("/proc/self/cgroup")
            .ok()
            .and_then(|cgroup| cgroup_memory_bound(std::path::Path::new(CGROUP_ROOT), &cgroup));
        return within_bound(system_memory(), bound);
    }
    #[allow(unreachable_code)]
    system_memory()
}

/// The whole machine's physical memory, never a cgroup's: what a bound is
/// computed from, and what a result names as the machine. `MemTotal` on
/// Linux; elsewhere the host reading, which is already the machine's.
pub fn host_total_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
        return parse_meminfo(&meminfo).map(|memory| memory.total);
    }
    #[allow(unreachable_code)]
    system_memory().map(|memory| memory.total)
}

/// Where the unified cgroup hierarchy is mounted.
pub const CGROUP_ROOT: &str = "/sys/fs/cgroup";

/// `MemTotal` and `MemAvailable` from `/proc/meminfo`, in bytes.
#[cfg(any(target_os = "linux", test))]
fn parse_meminfo(meminfo: &str) -> Option<SystemMemory> {
    let mut total = None;
    let mut available = None;
    for line in meminfo.lines() {
        let kib = |value: &str| value.split_whitespace().next()?.parse::<u64>().ok();
        if let Some(value) = line.strip_prefix("MemTotal:") {
            total = kib(value);
        } else if let Some(value) = line.strip_prefix("MemAvailable:") {
            available = kib(value);
        }
    }
    Some(SystemMemory {
        total: total?.saturating_mul(1024),
        available: available?.saturating_mul(1024),
    })
}

/// The cgroup v2 path `/proc/self/cgroup` names for this process: its `0::`
/// line. `None` on a v1-only host.
pub fn unified_cgroup_path(proc_self_cgroup: &str) -> Option<&str> {
    proc_self_cgroup
        .lines()
        .find_map(|line| line.trim().strip_prefix("0::"))
        .filter(|path| path.starts_with('/'))
}

/// The memory limit that bounds this process, and what is charged against it.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CgroupMemoryBound {
    /// The tightest `memory.max` from this process's cgroup up to the root.
    max: u64,
    /// `memory.current` of the cgroup that set it.
    current: u64,
    /// Its `memory.stat` `inactive_file`: cache the kernel reclaims first,
    /// before it would kill anything inside the cgroup.
    inactive_file: u64,
}

/// Walk from the cgroup `/proc/self/cgroup` names up to `root`, and take the
/// tightest `memory.max` on the way. A unit's bound sits on the unit, which
/// may be this process's own cgroup or an ancestor of it, and the root
/// cgroup has no limit, so reading only the root (or only the leaf) misses
/// it. In a container whose cgroup namespace makes its own cgroup `/`, the
/// walk is one step.
#[cfg(any(target_os = "linux", test))]
fn cgroup_memory_bound(
    root: &std::path::Path,
    proc_self_cgroup: &str,
) -> Option<CgroupMemoryBound> {
    let relative = unified_cgroup_path(proc_self_cgroup)?.trim_start_matches('/');
    let read = |dir: &std::path::Path, file: &str| std::fs::read_to_string(dir.join(file)).ok();
    let leaf = root.join(relative);
    let (max, dir) = leaf
        .ancestors()
        .take_while(|dir| dir.starts_with(root))
        .filter_map(|dir| {
            let max = read(dir, "memory.max")?.trim().parse::<u64>().ok()?;
            Some((max, dir))
        })
        .min_by_key(|(max, _)| *max)?;
    let current = read(dir, "memory.current")?.trim().parse::<u64>().ok()?;
    let inactive_file = read(dir, "memory.stat")
        .and_then(|stat| {
            stat.lines().find_map(|line| {
                let (key, value) = line.split_once(' ')?;
                (key == "inactive_file").then(|| value.trim().parse::<u64>().ok())?
            })
        })
        .unwrap_or(0);
    Some(CgroupMemoryBound {
        max,
        current,
        inactive_file,
    })
}

/// `memory` narrowed by `bound` when the bound is tighter: the limit is the
/// total, and what is left under it caps what is available. What is left is
/// counted against the working set rather than everything charged, since the
/// kernel reclaims inactive file cache inside the cgroup before it kills
/// anything there.
#[cfg(any(target_os = "linux", test))]
fn within_bound(
    memory: Option<SystemMemory>,
    bound: Option<CgroupMemoryBound>,
) -> Option<SystemMemory> {
    let bound = bound.filter(|bound| memory.is_none_or(|memory| bound.max < memory.total));
    let Some(bound) = bound else {
        return memory;
    };
    let working_set = bound.current.saturating_sub(bound.inactive_file);
    let under_bound = bound.max.saturating_sub(working_set);
    Some(SystemMemory {
        total: bound.max,
        available: memory.map_or(under_bound, |memory| memory.available.min(under_bound)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;
    const UNIT: &str =
        "0::/user.slice/user-1000.slice/user@1000.service/app.slice/cairn-exec.service\n";

    /// A cgroup tree under a temporary root: each `(path, files)` is a
    /// directory relative to the root and the files it holds.
    fn tree(dirs: &[(&str, &[(&str, &str)])]) -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        for (dir, files) in dirs {
            let dir = root.path().join(dir);
            std::fs::create_dir_all(&dir).unwrap();
            for (name, body) in *files {
                std::fs::write(dir.join(name), body).unwrap();
            }
        }
        root
    }

    #[test]
    fn an_unbounded_hierarchy_reads_as_the_host() {
        let root = tree(&[
            ("", &[("memory.current", "1000\n")]),
            (
                "user.slice/user-1000.slice/user@1000.service/app.slice/cairn-exec.service",
                &[("memory.max", "max\n"), ("memory.current", "500\n")],
            ),
        ]);
        assert_eq!(cgroup_memory_bound(root.path(), UNIT), None);
        let host = SystemMemory {
            total: 16 * GIB,
            available: 9 * GIB,
        };
        assert_eq!(within_bound(Some(host), None), Some(host));
    }

    /// The executor's unit bound sits two levels below the slice that
    /// holds it, and is what the reading reports.
    #[test]
    fn a_bounded_unit_below_the_root_is_found() {
        let root = tree(&[
            ("user.slice", &[("memory.max", "max\n")]),
            (
                "user.slice/user-1000.slice/user@1000.service/app.slice/cairn-exec.service",
                &[
                    ("memory.max", "12079595520\n"),
                    ("memory.current", "2147483648\n"),
                    (
                        "memory.stat",
                        "anon 1073741824\nfile 1073741824\ninactive_file 536870912\n",
                    ),
                ],
            ),
        ]);
        assert_eq!(
            cgroup_memory_bound(root.path(), UNIT),
            Some(CgroupMemoryBound {
                max: 12_079_595_520,
                current: 2 * GIB,
                inactive_file: GIB / 2,
            })
        );
    }

    /// A slice above the unit with a tighter limit is the one that binds.
    #[test]
    fn a_tighter_ancestor_wins() {
        let root = tree(&[
            (
                "user.slice/user-1000.slice",
                &[
                    ("memory.max", "4294967296\n"),
                    ("memory.current", "1073741824\n"),
                ],
            ),
            (
                "user.slice/user-1000.slice/user@1000.service/app.slice/cairn-exec.service",
                &[
                    ("memory.max", "12079595520\n"),
                    ("memory.current", "536870912\n"),
                ],
            ),
        ]);
        assert_eq!(
            cgroup_memory_bound(root.path(), UNIT),
            Some(CgroupMemoryBound {
                max: 4 * GIB,
                current: GIB,
                inactive_file: 0,
            })
        );
        // A container whose namespace roots it at its own cgroup.
        let container = tree(&[(
            "",
            &[("memory.max", "2147483648\n"), ("memory.current", "0\n")],
        )]);
        assert_eq!(
            cgroup_memory_bound(container.path(), "0::/\n").map(|bound| bound.max),
            Some(2 * GIB)
        );
    }

    /// bgsrv-ub2's shape: most of what is charged to the unit is cache the
    /// kernel would reclaim before killing, so it is not counted as used.
    #[test]
    fn cache_under_the_bound_is_still_available() {
        let bound = CgroupMemoryBound {
            max: 11 * GIB + GIB / 4,
            current: 10 * GIB,
            inactive_file: 9 * GIB + GIB / 2,
        };
        let host = SystemMemory {
            total: 15 * GIB,
            available: 12 * GIB,
        };
        assert_eq!(
            within_bound(Some(host), Some(bound)),
            Some(SystemMemory {
                total: 11 * GIB + GIB / 4,
                available: 11 * GIB + GIB / 4 - GIB / 2,
            })
        );
        // The host's own shortage still caps it.
        let starved = SystemMemory {
            available: GIB,
            ..host
        };
        assert_eq!(
            within_bound(Some(starved), Some(bound)).map(|memory| memory.available),
            Some(GIB)
        );
        // A limit no tighter than the machine bounds nothing.
        let loose = CgroupMemoryBound {
            max: 64 * GIB,
            ..bound
        };
        assert_eq!(within_bound(Some(host), Some(loose)), Some(host));
    }

    #[test]
    fn meminfo_is_read_in_bytes() {
        assert_eq!(
            parse_meminfo(
                "MemTotal:       16116228 kB\nMemFree: 1 kB\nMemAvailable:    9000000 kB\n"
            ),
            Some(SystemMemory {
                total: 16_116_228 * 1024,
                available: 9_000_000 * 1024,
            })
        );
        assert_eq!(parse_meminfo("MemFree: 1 kB\n"), None);
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn reads_this_process_resident_memory() {
        let resident = super::own_resident_bytes().expect("readable on this platform");
        assert!(resident > 1024 * 1024, "{resident} bytes is not a process");
    }
}
