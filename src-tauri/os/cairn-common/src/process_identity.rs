//! Which kernel process a pid names, and signalling only that one.
//!
//! A pid is reused once its process is reaped, so a recorded pid on its own
//! proves nothing about the process it names now. A process's start time is
//! fixed by the kernel at creation and never repeats for a reused pid, so the
//! pair is an identity. Every place Cairn signals a process it did not just
//! spawn checks that pair immediately before the signal, and treats a changed
//! start time as "the process I meant is gone", never as a target.

/// The kernel's start time for `pid`, rendered as an opaque token, or `None`
/// when no such process exists or this platform cannot say.
#[cfg(target_os = "linux")]
pub fn process_start_token(pid: u32) -> Option<String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The comm field may contain spaces and parentheses. Fields after its final
    // ')' begin at proc field 3; starttime is field 22, index 19 from there.
    stat.rsplit_once(')')?
        .1
        .split_whitespace()
        .nth(19)
        .map(str::to_owned)
}

#[cfg(target_os = "macos")]
pub fn process_start_token(pid: u32) -> Option<String> {
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    let read = unsafe {
        libc::proc_pidinfo(
            pid as libc::c_int,
            libc::PROC_PIDTBSDINFO,
            0,
            &mut info as *mut _ as *mut libc::c_void,
            size,
        )
    };
    (read == size).then(|| format!("{}:{}", info.pbi_start_tvsec, info.pbi_start_tvusec))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn process_start_token(_pid: u32) -> Option<String> {
    None
}

/// Whether [`process_start_token`] can answer on this platform at all. Where it
/// cannot, `None` means "unknown", not "gone".
pub const START_TOKENS_SUPPORTED: bool = cfg!(any(target_os = "macos", target_os = "linux"));

/// Whether `pid` names a live process, of any identity.
#[cfg(unix)]
pub fn process_is_alive(pid: u32) -> bool {
    // Signal 0 runs the existence and permission checks without delivering
    // anything. `EPERM` means the process exists under another user, which is
    // still alive for this question.
    let probed = unsafe { libc::kill(pid as libc::pid_t, 0) };
    probed == 0 || std::io::Error::last_os_error().kind() == std::io::ErrorKind::PermissionDenied
}

#[cfg(windows)]
pub fn process_is_alive(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_ACCESS_DENIED, STILL_ACTIVE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        // A process this user may not query still exists.
        return std::io::Error::last_os_error().raw_os_error() == Some(ERROR_ACCESS_DENIED as i32);
    }
    // An exited process stays openable while anyone holds a handle to it.
    let mut code = 0;
    let alive =
        unsafe { GetExitCodeProcess(handle, &mut code) } != 0 && code == STILL_ACTIVE as u32;
    unsafe { CloseHandle(handle) };
    alive
}

/// What a recorded (pid, start token) pair names right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessPresence {
    /// The recorded process is still running.
    Present,
    /// No process has this pid.
    Gone,
    /// A different process has this pid now. The recorded one is gone, and this
    /// one must never be signalled on the record's behalf.
    Reused,
    /// Something has this pid, and this platform cannot say whether it is the
    /// recorded process.
    Unverifiable,
}

impl ProcessPresence {
    /// Whether the recorded process is certainly no longer running.
    pub fn is_gone(self) -> bool {
        matches!(self, Self::Gone | Self::Reused)
    }
}

pub fn process_presence(pid: u32, expected_start_token: &str) -> ProcessPresence {
    match process_start_token(pid) {
        Some(actual) if actual == expected_start_token => ProcessPresence::Present,
        Some(_) => ProcessPresence::Reused,
        None if START_TOKENS_SUPPORTED => ProcessPresence::Gone,
        None if process_is_alive(pid) => ProcessPresence::Unverifiable,
        None => ProcessPresence::Gone,
    }
}

/// What [`signal_verified`] did.
#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalOutcome {
    /// The process was verified and signalled.
    Delivered {
        /// The whole process group was signalled, not only the pid.
        group: bool,
    },
    /// The recorded process no longer exists; nothing was signalled.
    Gone,
    /// The pid names another process now; nothing was signalled.
    Reused,
}

/// Signal the process recorded as (`pid`, `expected_start_token`), checking
/// that identity immediately before the signal.
///
/// With `process_group_id`, the group is signalled too, but only while it is
/// still the group the recorded process leads: a group id is a pid, and a group
/// whose leader has gone can be reused just as a pid can. A process that moved
/// itself out of that group is still reached through its own pid.
#[cfg(unix)]
pub fn signal_verified(
    pid: u32,
    expected_start_token: &str,
    process_group_id: Option<u32>,
    signal: libc::c_int,
) -> Result<SignalOutcome, String> {
    match process_presence(pid, expected_start_token) {
        ProcessPresence::Present => {}
        ProcessPresence::Gone => return Ok(SignalOutcome::Gone),
        ProcessPresence::Reused => return Ok(SignalOutcome::Reused),
        ProcessPresence::Unverifiable => {
            return Err(format!(
            "cannot verify which process pid {pid} names on this platform; refusing to signal it"
        ))
        }
    }
    let group = process_group_id.filter(|group| {
        *group == pid && unsafe { libc::getpgid(pid as libc::pid_t) } == *group as libc::pid_t
    });
    if let Some(group) = group {
        if unsafe { libc::kill(-(group as libc::pid_t), signal) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(format!("signal process group {group}: {error}"));
            }
        }
    }
    if unsafe { libc::kill(pid as libc::pid_t, signal) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            return Ok(SignalOutcome::Gone);
        }
        return Err(format!("signal pid {pid}: {error}"));
    }
    Ok(SignalOutcome::Delivered {
        group: group.is_some(),
    })
}

/// Whether any process is still a member of `process_group_id`.
#[cfg(unix)]
pub fn process_group_is_alive(process_group_id: u32) -> bool {
    let probed = unsafe { libc::kill(-(process_group_id as libc::pid_t), 0) };
    probed == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

#[cfg(all(test, any(target_os = "macos", target_os = "linux")))]
mod tests {
    use super::*;

    #[test]
    fn the_running_process_is_present_under_its_own_token() {
        let pid = std::process::id();
        let token = process_start_token(pid).expect("own start token");
        assert_eq!(process_presence(pid, &token), ProcessPresence::Present);
        assert_eq!(
            process_presence(pid, "not-the-token"),
            ProcessPresence::Reused
        );
        assert!(process_is_alive(pid));
    }

    #[test]
    fn a_reaped_child_reads_gone_and_is_never_signalled() {
        let mut child = std::process::Command::new("sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let pid = child.id();
        let token = process_start_token(pid).unwrap_or_default();
        child.wait().unwrap();
        assert!(process_presence(pid, &token).is_gone());
        assert!(matches!(
            signal_verified(pid, &token, Some(pid), libc::SIGKILL).unwrap(),
            SignalOutcome::Gone | SignalOutcome::Reused
        ));
    }

    #[test]
    fn a_verified_signal_reaches_the_recorded_group() {
        use std::os::unix::process::CommandExt;
        let mut child = std::process::Command::new("sh")
            .args(["-c", "sleep 60 & wait"])
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = child.id();
        let token = process_start_token(pid).unwrap();
        assert_eq!(
            signal_verified(pid, &token, Some(pid), libc::SIGKILL).unwrap(),
            SignalOutcome::Delivered { group: true }
        );
        child.wait().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while process_group_is_alive(pid) {
            assert!(
                std::time::Instant::now() < deadline,
                "the group's sleep survived"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
