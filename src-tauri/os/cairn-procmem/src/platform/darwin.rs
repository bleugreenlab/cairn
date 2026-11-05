use crate::census::{Census, CensusRow};
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
            let storage_io_bytes = if out.wants_storage(pid as u32) {
                macos_storage_io_bytes(pid)
            } else {
                None
            };
            out.rows.push(CensusRow {
                process,
                resident_bytes: task.map(|task| task.pti_resident_size),
                cpu_ms: task.map(|task| macos_task_cpu_ms(&task, timebase)),
                // Darwin keeps no per-process uninterruptible state to read, so
                // on macOS only storage bytes speak for a disk-bound tree.
                uninterruptible: false,
                storage_io_bytes,
            });
        }
        true
    })
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

/// Bytes the process has read from and written to storage, from the kernel's
/// own resource accounting.
#[cfg(target_os = "macos")]
pub fn macos_storage_io_bytes(pid: i32) -> Option<u64> {
    let mut info: libc::rusage_info_v2 = unsafe { std::mem::zeroed() };
    // SAFETY: the kernel fills a caller-owned struct of the flavor named.
    let status = unsafe {
        libc::proc_pid_rusage(
            pid,
            libc::RUSAGE_INFO_V2,
            (&mut info as *mut libc::rusage_info_v2).cast(),
        )
    };
    (status == 0).then(|| {
        info.ri_diskio_bytesread
            .saturating_add(info.ri_diskio_byteswritten)
    })
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
}
