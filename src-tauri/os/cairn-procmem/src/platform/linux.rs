use crate::census::{Census, CensusRow};
use crate::identity::{KernelProcess, KernelRead, StartKey};
/// Parse `/proc/<pid>/stat` into identity facts and cumulative CPU ticks
/// (user plus system), or `None` for a zombie or a malformed line.
///
/// The comm field may contain spaces and parentheses, so fields are counted
/// from after its final `)`: index 0 is the state, 1 the parent, 2 the group,
/// 3 the session, 11 and 12 user and system time, 19 the start time.
#[cfg(any(target_os = "linux", test))]
pub fn parse_linux_stat(pid: u32, stat: &str) -> Option<(KernelProcess, u64)> {
    // A zombie is not a process this census is about. It has released every
    // resource it held — no descriptors, no working directory, nothing it can
    // still write — and lingers only until its parent waits, keeping its pid
    // and its start token the whole time. Reading it as live makes an identity
    // check answer "still there" forever, which would strand a cell whose
    // survivor this executor killed but has not yet reaped.
    if linux_stat_is_zombie(stat) {
        return None;
    }
    parse_linux_stat_record(pid, stat)
}

/// The identity facts a stat line records, a zombie's included. Only
/// child identity capture wants a zombie's record; live measurement goes
/// through [`parse_linux_stat`].
#[cfg(any(target_os = "linux", test))]
pub fn parse_linux_stat_record(pid: u32, stat: &str) -> Option<(KernelProcess, u64)> {
    let mut fields = stat.rsplit_once(')')?.1.split_whitespace();
    let _state = fields.next()?;
    let parent_pid = fields.next()?.parse().ok()?;
    let process_group_id = fields.next()?.parse().ok()?;
    let session_id = fields.next()?.parse().ok()?;
    let user: u64 = fields.nth(7)?.parse().ok()?;
    let system: u64 = fields.next()?.parse().ok()?;
    let start = fields.nth(6)?.parse().ok()?;
    Some((
        KernelProcess {
            pid,
            parent_pid,
            process_group_id,
            session_id: Some(session_id),
            start: StartKey(start, 0),
        },
        user.saturating_add(system),
    ))
}
/// Whether a `/proc/<pid>/stat` line describes a zombie.
#[cfg(any(target_os = "linux", test))]
pub fn linux_stat_is_zombie(stat: &str) -> bool {
    stat.rsplit_once(')')
        .and_then(|(_, rest)| rest.split_whitespace().next())
        == Some("Z")
}
pub fn read_kernel_state(pid: u32) -> KernelRead {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => linux_stat_state(pid, &stat).0,
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                || error.raw_os_error() == Some(libc::ESRCH) =>
        {
            KernelRead::Gone
        }
        Err(_) => KernelRead::Unreadable,
    }
}

pub fn read_unreaped_child(pid: u32) -> Result<KernelProcess, String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .map_err(|error| format!("read /proc/{pid}/stat: {error}"))?;
    parse_linux_stat_record(pid, &stat)
        .map(|(process, _)| process)
        .ok_or_else(|| format!("/proc/{pid}/stat is malformed"))
}

/// The state a stat line describes, with its cumulative CPU ticks when live.
pub fn linux_stat_state(pid: u32, stat: &str) -> (KernelRead, Option<u64>) {
    match parse_linux_stat(pid, stat) {
        Some((process, ticks)) => (KernelRead::Live(process), Some(ticks)),
        None if linux_stat_is_zombie(stat) => (KernelRead::Gone, None),
        None => (KernelRead::Unreadable, None),
    }
}
/// The state letter of a `/proc/<pid>/stat` line: the first field after the
/// parenthesized command, which may itself contain `)`.
#[cfg(any(target_os = "linux", test))]
fn linux_stat_state_letter(stat: &str) -> Option<&str> {
    stat.rsplit_once(')')
        .and_then(|(_, rest)| rest.split_whitespace().next())
}

/// Storage bytes from `/proc/<pid>/io`: what the process caused to be read
/// from storage, written to it, and cancelled before writeback. Each is
/// cumulative, so their sum moves exactly when any of them does. The
/// syscall-level `rchar`/`wchar` are left out: they count pipe and page-cache
/// traffic a wedged runner's own heartbeat can make.
#[cfg(any(target_os = "linux", test))]
pub fn parse_linux_storage_io(io: &str) -> Option<u64> {
    let mut total = 0_u64;
    let mut fields = 0;
    for line in io.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if matches!(key, "read_bytes" | "write_bytes" | "cancelled_write_bytes") {
            total = total.saturating_add(value.trim().parse().ok()?);
            fields += 1;
        }
    }
    (fields == 3).then_some(total)
}

/// The page size and clock-tick rate, constant for the daemon's lifetime and
/// cached so CPU measurement adds no syscall to the per-census or per-process
/// path. A tick rate of zero is no rate.
#[cfg(target_os = "linux")]
pub fn linux_units() -> (u64, u64) {
    static UNITS: std::sync::OnceLock<(u64, u64)> = std::sync::OnceLock::new();
    *UNITS.get_or_init(|| {
        // SAFETY: sysconf with constant names; each returns its unit or -1.
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        (
            if page_size > 0 {
                page_size as u64
            } else {
                4096
            },
            ticks.max(0) as u64,
        )
    })
}

/// On Linux one read of `stat` yields the identity facts, the CPU and the
/// state letter, via the reaper's own parser, `statm` the resident size, and
/// `io` (for watched members only) the storage bytes, through buffers reused
/// across every process in a census.
#[cfg(target_os = "linux")]
pub fn census(out: &mut Census) -> bool {
    use std::io::Read;

    let Ok(entries) = std::fs::read_dir("/proc") else {
        return false;
    };
    let (page_size, ticks_per_second) = linux_units();
    let mut buffer = String::new();
    let read_into = |path: String, buffer: &mut String| -> std::io::Result<()> {
        buffer.clear();
        std::fs::File::open(path).and_then(|mut file| file.read_to_string(buffer).map(|_| ()))
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => {
                out.enumeration_incomplete = true;
                continue;
            }
        };
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        match read_into(format!("/proc/{pid}/stat"), &mut buffer) {
            Ok(()) => {}
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    || error.raw_os_error() == Some(libc::ESRCH) =>
            {
                continue
            }
            Err(_) => {
                out.unreadable.push(pid);
                continue;
            }
        }
        let (state, ticks) = linux_stat_state(pid, &buffer);
        let process = match state {
            KernelRead::Live(process) => process,
            KernelRead::Gone => continue,
            KernelRead::Unreadable => {
                out.unreadable.push(pid);
                continue;
            }
        };
        let uninterruptible = linux_stat_state_letter(&buffer) == Some("D");
        // A process whose identity read but whose resident size did not is
        // still a row: it is present, only partly measured.
        let resident_bytes = read_into(format!("/proc/{pid}/statm"), &mut buffer)
            .ok()
            .and_then(|()| buffer.split_whitespace().nth(1)?.parse::<u64>().ok())
            .map(|pages| pages.saturating_mul(page_size));
        let storage_io_bytes = if out.wants_storage(pid) {
            read_into(format!("/proc/{pid}/io"), &mut buffer)
                .ok()
                .and_then(|()| parse_linux_storage_io(&buffer))
        } else {
            None
        };
        out.rows.push(CensusRow {
            process,
            resident_bytes,
            cpu_ms: ticks
                .filter(|_| ticks_per_second > 0)
                .map(|ticks| ticks_to_ms(ticks, ticks_per_second)),
            uninterruptible,
            storage_io_bytes,
        });
    }
    true
}

#[cfg(any(target_os = "linux", test))]
pub fn ticks_to_ms(ticks: u64, ticks_per_second: u64) -> u64 {
    (u128::from(ticks) * 1_000)
        .checked_div(u128::from(ticks_per_second))
        .map_or(0, |milliseconds| {
            milliseconds.min(u128::from(u64::MAX)) as u64
        })
}

pub fn process_rss_bytes(pid: u32) -> Option<u64> {
    let statm = std::fs::read_to_string(format!("/proc/{pid}/statm")).ok()?;
    Some(
        statm
            .split_whitespace()
            .nth(1)?
            .parse::<u64>()
            .ok()?
            .saturating_mul(linux_units().0),
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn linux_stat_parses_identity_and_cpu_after_the_complete_name() {
        let stat = "42 (a name ) with spaces) S 1 42 7 0 0 0 0 0 0 0 125 75 0 0 20 0 1 0 8812 0 0";
        let (process, ticks) = parse_linux_stat(42, stat).unwrap();
        assert_eq!(process.parent_pid, 1);
        assert_eq!(process.process_group_id, 42);
        assert_eq!(process.session_id, Some(7));
        assert_eq!(process.start, StartKey(8812, 0));
        assert_eq!(ticks, 200);
        let zombie = stat.replace(") S ", ") Z ");
        assert!(parse_linux_stat(42, &zombie).is_none());
        assert_eq!(
            parse_linux_stat_record(42, &zombie).map(|(process, _)| process),
            Some(process),
            "a zombie's record keeps the identity it had while running"
        );
        assert!(parse_linux_stat(42, "42 (name) S 1 42 7").is_none());
    }

    #[test]
    fn tick_conversion_is_saturating() {
        assert_eq!(ticks_to_ms(125, 100), 1250);
        assert_eq!(ticks_to_ms(u64::MAX, 1), u64::MAX);
        assert_eq!(ticks_to_ms(10, 0), 0);
    }
    #[test]
    fn storage_counts_only_physical_bytes() {
        assert_eq!(
            parse_linux_storage_io(
                "rchar: 999
read_bytes: 10
write_bytes: 20
cancelled_write_bytes: 3"
            ),
            Some(33)
        );
        assert_eq!(parse_linux_storage_io("read_bytes: 1"), None);
    }
}
