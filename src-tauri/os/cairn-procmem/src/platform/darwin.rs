use crate::census::{Census, CensusRow, SchedulingTimes};
use crate::identity::{KernelProcess, KernelRead, StartKey};
/// `PROC_PIDTBSDINFO` for one pid. A nonzero `arg` asks the kernel to
/// look among zombies too; with zero it answers `ESRCH` for one.
fn bsd_info(pid: u32, include_zombie: bool) -> Result<libc::proc_bsdinfo, std::io::Error> {
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    let read = unsafe {
        libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDTBSDINFO,
            u64::from(include_zombie),
            (&mut info as *mut libc::proc_bsdinfo).cast(),
            size,
        )
    };
    if read == size {
        Ok(info)
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn kernel_process(pid: u32, info: &libc::proc_bsdinfo) -> KernelProcess {
    KernelProcess {
        pid,
        parent_pid: info.pbi_ppid,
        process_group_id: info.pbi_pgid,
        session_id: Some(unsafe { libc::getsid(pid as i32) })
            .filter(|id| *id >= 0)
            .map(|id| id as u32),
        start: StartKey(info.pbi_start_tvsec, info.pbi_start_tvusec),
    }
}

pub fn read_kernel_state(pid: u32) -> KernelRead {
    // Zombies are excluded here for the same reason the Linux census
    // excludes them: a process awaiting its parent's `wait` holds nothing
    // and can write nothing, but keeps its pid and start token, so counting
    // it as live would strand a draining cell permanently. Asked without
    // the zombie flag the kernel already answers `ESRCH` for one; the
    // status check states the rule here rather than leaving it to a flag.
    match bsd_info(pid, false) {
        Ok(info) if info.pbi_status == libc::SZOMB => KernelRead::Gone,
        Ok(info) => KernelRead::Live(kernel_process(pid, &info)),
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => KernelRead::Gone,
        Err(_) => KernelRead::Unreadable,
    }
}

pub fn read_unreaped_child(pid: u32) -> Result<KernelProcess, String> {
    bsd_info(pid, true)
        .map(|info| kernel_process(pid, &info))
        .map_err(|error| format!("read process info for pid {pid}: {error}"))
}
/// `proc_taskinfo`'s CPU totals are Mach absolute-time ticks, not nanoseconds.
/// One tick is `numer / denom` ns: 1/1 on Intel and 125/3 on Apple silicon, so
/// reading ticks as nanoseconds undercounts by about 42x there. The timebase is
/// fixed for the machine's life, so it is read once.
#[cfg(target_os = "macos")]
pub fn mach_timebase() -> (u32, u32) {
    // Declared here because libc's binding is deprecated in favour of a crate
    // this one call does not justify.
    #[repr(C)]
    struct MachTimebaseInfo {
        numer: u32,
        denom: u32,
    }
    extern "C" {
        fn mach_timebase_info(info: *mut MachTimebaseInfo) -> libc::c_int;
    }
    static TIMEBASE: std::sync::OnceLock<(u32, u32)> = std::sync::OnceLock::new();
    *TIMEBASE.get_or_init(|| {
        let mut info = MachTimebaseInfo { numer: 0, denom: 0 };
        // SAFETY: fills the two fields of a caller-owned struct.
        let status = unsafe { mach_timebase_info(&mut info) };
        if status == 0 && info.numer > 0 && info.denom > 0 {
            (info.numer, info.denom)
        } else {
            // Documented never to fail; if it does, Intel's timebase is the
            // only guess, and on Apple silicon it undercounts.
            tracing::warn!(
                status,
                "mach_timebase_info failed; CPU time assumes a 1/1 timebase"
            );
            (1, 1)
        }
    })
}

#[cfg(any(target_os = "macos", test))]
fn mach_ticks_to_ms(ticks: u64, (numer, denom): (u32, u32)) -> u64 {
    let nanos = u128::from(ticks) * u128::from(numer) / u128::from(denom.max(1));
    u64::try_from(nanos / 1_000_000).unwrap_or(u64::MAX)
}

/// Every process's identity facts, resident size and CPU time from the kernel.
///
/// On macOS the identity comes from the reaper's own `proc_pidinfo` reader, so
/// a start token recorded here is the one an identity check compares, and the
/// readings from `PROC_PIDTASKINFO`: a few syscalls per process and not one
/// allocation.
#[cfg(target_os = "macos")]
pub fn census(out: &mut Census) -> bool {
    let timebase = mach_timebase();
    let needed = unsafe { libc::proc_listallpids(std::ptr::null_mut(), 0) };
    if needed <= 0 {
        return false;
    }
    thread_local! {
        static PIDS: std::cell::RefCell<Vec<i32>> = const { std::cell::RefCell::new(Vec::new()) };
    }
    PIDS.with(|buffer| {
        let mut pids = buffer.borrow_mut();
        let capacity = pids.len().max(needed as usize + 64);
        pids.resize(capacity, 0);
        let count = unsafe {
            libc::proc_listallpids(
                pids.as_mut_ptr().cast(),
                (pids.len() * std::mem::size_of::<i32>()) as libc::c_int,
            )
        };
        if count <= 0 {
            return false;
        }
        out.enumeration_incomplete |= count as usize >= pids.len();
        for pid in pids
            .iter()
            .copied()
            .take(count as usize)
            .filter(|pid| *pid > 0)
        {
            let process = match read_kernel_state(pid as u32) {
                KernelRead::Live(process) => process,
                KernelRead::Gone => continue,
                KernelRead::Unreadable => {
                    out.unreadable.push(pid as u32);
                    continue;
                }
            };
            // A process whose identity read but whose usage did not is still a
            // row: it is present, only unmeasured.
            let task = macos_task_info(pid);
            // One rusage read answers both footprint and storage, so footprint
            // is read for every row rather than only for wanted pids.
            let usage = macos_rusage(pid);
            let storage_io_bytes = usage
                .as_ref()
                .filter(|_| out.wants_storage(pid as u32))
                .map(rusage_storage_bytes);
            out.rows.push(CensusRow {
                process,
                resident_bytes: task.map(|task| task.pti_resident_size),
                physical_footprint_bytes: usage.as_ref().map(|usage| usage.info.ri_phys_footprint),
                cpu_ms: task.map(|task| macos_task_cpu_ms(&task, timebase)),
                // Darwin keeps no per-process uninterruptible state to read, so
                // on macOS only storage bytes speak for a disk-bound tree.
                uninterruptible: false,
                storage_io_bytes,
                scheduling: usage
                    .as_ref()
                    .map(|usage| rusage_scheduling(usage, timebase)),
            });
        }
        true
    })
}

/// Identity-only census of one process group, with the same live-state reader
/// and completeness contract as the machine census. Unrelated protected system
/// processes cannot make this group's membership unreadable.
pub fn census_process_group(process_group_id: u32, out: &mut Census) -> bool {
    // PROC_PGRP_ONLY from the macOS SDK's sys/proc_info.h.
    const PROC_PGRP_ONLY: u32 = 2;
    let pid_size = std::mem::size_of::<i32>();
    let needed =
        unsafe { libc::proc_listpids(PROC_PGRP_ONLY, process_group_id, std::ptr::null_mut(), 0) };
    if needed <= 0 {
        return false;
    }
    let mut pids = vec![0_i32; needed as usize / pid_size + 64];
    let filled = unsafe {
        libc::proc_listpids(
            PROC_PGRP_ONLY,
            process_group_id,
            pids.as_mut_ptr().cast(),
            (pids.len() * pid_size) as i32,
        )
    };
    // Zero can mean either empty or failure. The caller can establish absence
    // independently with ESRCH, but this census must not invent completeness.
    if filled <= 0 {
        return false;
    }
    out.enumeration_incomplete |=
        filled as usize >= pids.len() * pid_size || !(filled as usize).is_multiple_of(pid_size);
    for pid in pids
        .into_iter()
        .take(filled as usize / pid_size)
        .filter(|pid| *pid > 0)
    {
        match read_kernel_state(pid as u32) {
            KernelRead::Live(process) => out.rows.push(CensusRow {
                process,
                resident_bytes: None,
                physical_footprint_bytes: None,
                cpu_ms: None,
                uninterruptible: false,
                storage_io_bytes: None,
                scheduling: None,
            }),
            KernelRead::Gone => {}
            KernelRead::Unreadable => out.unreadable.push(pid as u32),
        }
    }
    true
}

/// `PROX_FDTYPE_PIPE` from the macOS SDK's sys/proc_info.h.
const PROX_FDTYPE_PIPE: u32 = 6;
/// `PROC_PIDPATHINFO_MAXSIZE` from libproc.h.
const PROC_PIDPATHINFO_MAXSIZE: usize = 4 * 1024;

/// Every same-uid process's pipe and total descriptors: one `PROC_PIDLISTFDS`
/// listing per process, stopping at `deadline`.
pub fn fd_census_into(deadline: std::time::Instant, out: &mut crate::fd_census::FdCensus) -> bool {
    // SAFETY: getuid cannot fail.
    let uid = unsafe { libc::getuid() };
    let needed = unsafe { libc::proc_listallpids(std::ptr::null_mut(), 0) };
    if needed <= 0 {
        out.partial = true;
        return true;
    }
    let mut pids = vec![0_i32; needed as usize + 64];
    // SAFETY: the buffer holds `pids.len()` pids and the kernel is told so.
    let count = unsafe {
        libc::proc_listallpids(
            pids.as_mut_ptr().cast(),
            (pids.len() * std::mem::size_of::<i32>()) as libc::c_int,
        )
    };
    if count <= 0 {
        out.partial = true;
        return true;
    }
    out.partial |= count as usize >= pids.len();
    for pid in pids.into_iter().take(count as usize).filter(|pid| *pid > 0) {
        if std::time::Instant::now() >= deadline {
            out.partial = true;
            break;
        }
        // Another user's process may refuse; it is not this census's.
        let Ok(info) = bsd_info(pid as u32, false) else {
            continue;
        };
        if info.pbi_uid != uid || info.pbi_status == libc::SZOMB {
            continue;
        }
        let (pipe_fds, total_fds) = match descriptor_counts(pid) {
            Ok(counts) => counts,
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => continue,
            Err(_) => {
                out.unreadable += 1;
                out.partial = true;
                continue;
            }
        };
        let name = crate::fd_census::c_name(&info.pbi_name);
        out.processes.push(crate::fd_census::FdCensusRow {
            pid: pid as u32,
            start: StartKey(info.pbi_start_tvsec, info.pbi_start_tvusec),
            parent_pid: info.pbi_ppid,
            comm: if name.is_empty() {
                crate::fd_census::c_name(&info.pbi_comm)
            } else {
                name
            },
            path_basename: executable_basename(pid),
            pipe_fds,
            total_fds,
        });
    }
    true
}

/// (pipe descriptors, all descriptors) for one process.
fn descriptor_counts(pid: i32) -> std::io::Result<(u32, u32)> {
    let entry_size = std::mem::size_of::<libc::proc_fdinfo>();
    // SAFETY: a null buffer asks only for the size.
    let needed =
        unsafe { libc::proc_pidinfo(pid, libc::PROC_PIDLISTFDS, 0, std::ptr::null_mut(), 0) };
    if needed < 0 {
        return Err(std::io::Error::last_os_error());
    }
    if needed == 0 {
        let error = std::io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(0) | None => Ok((0, 0)),
            _ => Err(error),
        };
    }
    // Room for descriptors opened between the size query and the listing.
    let capacity = needed as usize / entry_size + 32;
    let mut entries: Vec<libc::proc_fdinfo> = Vec::with_capacity(capacity);
    // SAFETY: the buffer has room for `capacity` entries and the kernel is
    // told that many bytes.
    let filled = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDLISTFDS,
            0,
            entries.as_mut_ptr().cast(),
            (capacity * entry_size) as libc::c_int,
        )
    };
    if filled <= 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: the kernel initialized `filled` bytes of whole entries.
    unsafe { entries.set_len(filled as usize / entry_size) };
    let pipes = entries
        .iter()
        .filter(|entry| entry.proc_fdtype == PROX_FDTYPE_PIPE)
        .count();
    Ok((pipes as u32, entries.len() as u32))
}

fn executable_basename(pid: i32) -> Option<String> {
    let mut buffer = vec![0_u8; PROC_PIDPATHINFO_MAXSIZE];
    // SAFETY: the buffer is as large as the kernel is told.
    let length =
        unsafe { libc::proc_pidpath(pid, buffer.as_mut_ptr().cast(), buffer.len() as u32) };
    if length <= 0 {
        return None;
    }
    buffer.truncate(length as usize);
    let path = String::from_utf8_lossy(&buffer).into_owned();
    std::path::Path::new(&path)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
}

#[cfg(target_os = "macos")]
pub fn macos_task_info(pid: i32) -> Option<libc::proc_taskinfo> {
    let mut task: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let task_size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    // SAFETY: the kernel fills a caller-owned struct of the size it is told.
    let read = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTASKINFO,
            0,
            (&mut task as *mut libc::proc_taskinfo).cast(),
            task_size,
        )
    } == task_size;
    read.then_some(task)
}

#[cfg(target_os = "macos")]
pub fn macos_task_cpu_ms(task: &libc::proc_taskinfo, timebase: (u32, u32)) -> u64 {
    mach_ticks_to_ms(
        task.pti_total_user.saturating_add(task.pti_total_system),
        timebase,
    )
}

/// `RUSAGE_INFO_V6` from the macOS SDK's sys/resource.h, which libc does not
/// define.
const RUSAGE_INFO_V6: libc::c_int = 6;

/// `struct rusage_info_v6` from the macOS SDK's sys/resource.h. Its prefix is
/// `rusage_info_v4` field for field, so a V4 answer fills the same struct and
/// leaves the V6 tail zero. Fields this crate does not read are kept as
/// arrays, named in the comment beside each, so the layout stays exact.
#[repr(C)]
#[derive(Clone, Copy)]
struct RusageInfo {
    _ri_uuid: [u8; 16],
    ri_user_time: u64,
    ri_system_time: u64,
    /// `ri_pkg_idle_wkups`, `ri_interrupt_wkups`, `ri_pageins`,
    /// `ri_wired_size`, `ri_resident_size`.
    _wakeups_to_resident: [u64; 5],
    ri_phys_footprint: u64,
    /// `ri_proc_start_abstime`, `ri_proc_exit_abstime`, the five `ri_child_*`
    /// counters, `ri_child_elapsed_abstime`.
    _lifetimes: [u64; 8],
    ri_diskio_bytesread: u64,
    ri_diskio_byteswritten: u64,
    /// The seven `ri_cpu_time_qos_*` counters, `ri_billed_system_time`,
    /// `ri_serviced_system_time`, `ri_logical_writes`,
    /// `ri_lifetime_max_phys_footprint`, `ri_instructions`, `ri_cycles`,
    /// `ri_billed_energy`, `ri_serviced_energy`,
    /// `ri_interval_max_phys_footprint`.
    _qos_to_interval_footprint: [u64; 16],
    /// The last V4 field.
    ri_runnable_time: u64,
    _ri_flags: u64,
    ri_user_ptime: u64,
    ri_system_ptime: u64,
    /// `ri_pinstructions`, `ri_pcycles`, `ri_energy_nj`, `ri_penergy_nj`,
    /// `ri_secure_time_in_system`, `ri_secure_ptime_in_system`, the three
    /// neural footprints, then `ri_reserved[9]`.
    _performance_to_reserved: [u64; 18],
}

/// One `proc_pid_rusage` answer, and whether it came from the V6 flavor, the
/// only one carrying performance-core time.
struct Rusage {
    info: RusageInfo,
    performance_cores: bool,
}

/// Whether this kernel refused the V6 flavor, after which every read asks for
/// V4. Learned once, so a process the census may not read (another user's)
/// costs one failed syscall, not two.
static RUSAGE_V6_UNSUPPORTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// The kernel's resource accounting for one process: the one reader of
/// `proc_pid_rusage` in Cairn. Fails for another user's process.
fn macos_rusage(pid: i32) -> Option<Rusage> {
    use std::sync::atomic::Ordering;
    let mut info: RusageInfo = unsafe { std::mem::zeroed() };
    loop {
        let v6 = !RUSAGE_V6_UNSUPPORTED.load(Ordering::Relaxed);
        let flavor = if v6 {
            RUSAGE_INFO_V6
        } else {
            libc::RUSAGE_INFO_V4
        };
        // SAFETY: the kernel fills a caller-owned struct at least as large as
        // the flavor named.
        let status =
            unsafe { libc::proc_pid_rusage(pid, flavor, (&mut info as *mut RusageInfo).cast()) };
        if status == 0 {
            return Some(Rusage {
                info,
                performance_cores: v6,
            });
        }
        // An unknown flavor is EINVAL; anything else is this process's.
        if !(v6 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINVAL)) {
            return None;
        }
        RUSAGE_V6_UNSUPPORTED.store(true, Ordering::Relaxed);
        tracing::info!(
            "proc_pid_rusage refused RUSAGE_INFO_V6; reading V4, without performance-core time"
        );
    }
}

fn rusage_scheduling(usage: &Rusage, timebase: (u32, u32)) -> SchedulingTimes {
    let info = &usage.info;
    SchedulingTimes {
        runnable_ms: mach_ticks_to_ms(info.ri_runnable_time, timebase),
        cpu_ms: mach_ticks_to_ms(
            info.ri_user_time.saturating_add(info.ri_system_time),
            timebase,
        ),
        performance_cpu_ms: usage.performance_cores.then(|| {
            mach_ticks_to_ms(
                info.ri_user_ptime.saturating_add(info.ri_system_ptime),
                timebase,
            )
        }),
    }
}

fn rusage_storage_bytes(usage: &Rusage) -> u64 {
    usage
        .info
        .ri_diskio_bytesread
        .saturating_add(usage.info.ri_diskio_byteswritten)
}

/// Bytes the process has read from and written to storage, from the kernel's
/// own resource accounting.
#[cfg(target_os = "macos")]
pub fn macos_storage_io_bytes(pid: i32) -> Option<u64> {
    macos_rusage(pid).as_ref().map(rusage_storage_bytes)
}

/// The process's physical footprint, `ri_phys_footprint`: what `footprint`
/// and `vmmap -summary` report and what memory-pressure kills count, with
/// compressed pages included and shared pages excluded.
pub fn process_footprint_bytes(pid: u32) -> Option<u64> {
    macos_rusage(pid as i32).map(|usage| usage.info.ri_phys_footprint)
}

pub fn process_rss_bytes(pid: u32) -> Option<u64> {
    macos_task_info(pid as i32).map(|task| task.pti_resident_size)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mach_conversion_preserves_apple_silicon_timebase() {
        assert_eq!(mach_ticks_to_ms(24_000_000, (125, 3)), 1000);
        assert_eq!(mach_ticks_to_ms(1_000_000_000, (1, 1)), 1000);
        assert_eq!(mach_ticks_to_ms(u64::MAX, (u32::MAX, 1)), u64::MAX);
    }

    /// The SDK's `sizeof(struct rusage_info_v6)` and field offsets (Xcode 26
    /// SDK, read with clang): the kernel fills as many bytes as its flavor
    /// says, so a layout drift would read the wrong counters silently.
    #[test]
    fn the_v6_struct_matches_the_sdk_layout() {
        assert_eq!(std::mem::size_of::<RusageInfo>(), 464);
        assert_eq!(std::mem::offset_of!(RusageInfo, ri_runnable_time), 288);
        assert_eq!(std::mem::offset_of!(RusageInfo, ri_user_ptime), 304);
        assert_eq!(std::mem::offset_of!(RusageInfo, ri_phys_footprint), 72);
        // Everything through the runnable time is exactly libc's V4.
        assert_eq!(
            std::mem::offset_of!(RusageInfo, _ri_flags),
            std::mem::size_of::<libc::rusage_info_v4>()
        );
    }

    /// A busy loop's runnable time covers its CPU time: on Darwin runnable
    /// includes running, which is what the lowered-wait arm's arithmetic
    /// (waiting = runnable less CPU) depends on.
    #[test]
    fn own_scheduling_reads_runnable_covering_cpu() {
        let started = std::time::Instant::now();
        let mut spin = 0_u64;
        while started.elapsed() < std::time::Duration::from_millis(200) {
            spin = std::hint::black_box(spin.wrapping_add(1));
        }
        let usage = macos_rusage(std::process::id() as i32).expect("own rusage");
        let times = rusage_scheduling(&usage, mach_timebase());
        assert!(times.cpu_ms >= 150, "{times:?}");
        // Allow for accounting granularity between the two counters.
        assert!(times.runnable_ms + 50 >= times.cpu_ms, "{times:?}");
        if let Some(performance) = times.performance_cpu_ms {
            assert!(performance <= times.cpu_ms, "{times:?}");
        }
    }

    #[test]
    fn own_footprint_is_read_and_bounded_by_rss_scale() {
        let pid = std::process::id();
        let footprint = process_footprint_bytes(pid).expect("own footprint");
        let rss = process_rss_bytes(pid).expect("own rss");
        assert!(footprint > 0);
        // Footprint counts compressed pages RSS does not, so it can exceed
        // RSS, but not by an order of magnitude for a test binary.
        assert!(
            footprint <= rss.saturating_mul(4),
            "{footprint} vs rss {rss}"
        );
        let mut out = Census::default();
        assert!(census(&mut out));
        let row = out.rows.iter().find(|row| row.process.pid == pid).unwrap();
        assert!(row.physical_footprint_bytes.is_some_and(|bytes| bytes > 0));
    }
}
