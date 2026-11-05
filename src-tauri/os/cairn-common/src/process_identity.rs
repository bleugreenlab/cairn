//! Which kernel process a pid names, and signalling only that one.
//!
//! A pid is reused once its process is reaped, so a recorded pid on its own
//! proves nothing about the process it names now. A process's start time is
//! fixed by the kernel at creation and never repeats for a reused pid, so the
//! pair is an identity. Every place Cairn signals a process it did not just
//! spawn checks that pair immediately before the signal, and treats a changed
//! start time as "the process I meant is gone", never as a target.

/// The kernel's start time for `pid`, including an exited, unreaped child.
/// A token identifies a lifetime, not liveness; use [`process_presence`] when
/// deciding whether a recorded process is still present.
pub fn process_start_token(pid: u32) -> Option<String> {
    cairn_procmem::platform::read_start_token(pid)
}

/// Whether start tokens are available from this platform's kernel.
pub const START_TOKENS_SUPPORTED: bool = cairn_procmem::platform::MEMORY_SUPPORTED;

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
    #[cfg(any(target_os = "macos", windows))]
    {
        // Capture identity survives exit for Darwin zombies and held Windows
        // process objects. Only the live-state reader distinguishes those from
        // running processes and preserves unreadable identity as uncertainty.
        presence_from_kernel_read(
            cairn_procmem::platform::read_kernel_state(pid),
            expected_start_token,
        )
    }
    #[cfg(not(any(target_os = "macos", windows)))]
    {
        // Linux can retain a zombie thread-group leader while other threads
        // still hold resources, so presence keeps its token-based contract.
        match process_start_token(pid) {
            Some(actual) if actual == expected_start_token => ProcessPresence::Present,
            Some(_) => ProcessPresence::Reused,
            None if START_TOKENS_SUPPORTED => ProcessPresence::Gone,
            None if process_is_alive(pid) => ProcessPresence::Unverifiable,
            None => ProcessPresence::Gone,
        }
    }
}

#[cfg(any(target_os = "macos", windows))]
fn presence_from_kernel_read(
    reading: cairn_procmem::KernelRead,
    expected_start_token: &str,
) -> ProcessPresence {
    match reading {
        cairn_procmem::KernelRead::Live(process)
            if process.start.token() == expected_start_token =>
        {
            ProcessPresence::Present
        }
        cairn_procmem::KernelRead::Live(_) => ProcessPresence::Reused,
        cairn_procmem::KernelRead::Gone => ProcessPresence::Gone,
        cairn_procmem::KernelRead::Unreadable => ProcessPresence::Unverifiable,
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

/// What the kernel says a process's threads are doing: what a pid's presence
/// cannot say about a process that has gone silent, or that will not finish
/// exiting.
///
/// A signalled process is gone only once every one of its threads has left the
/// kernel. A thread in uninterruptible sleep (`D`, almost always waiting on
/// storage) leaves when that wait returns and not before, SIGKILL included.
/// Until then the leader can read as a zombie that `waitpid` does not report,
/// and the group's open files, its locks among them, stay open. Telling that
/// apart from a zombie nobody reaped is what this is for.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KernelThreadStates {
    /// The thread-group leader's state letter (`R`, `S`, `D`, `Z`, ...).
    pub leader: Option<char>,
    /// Every thread the kernel still lists, the leader included.
    pub threads: usize,
    /// The threads in uninterruptible sleep, each with the kernel function it
    /// is waiting in where the kernel will say.
    pub uninterruptible: Vec<(u32, Option<String>)>,
}

impl KernelThreadStates {
    /// Whether any thread is in uninterruptible sleep: the process is in the
    /// kernel's hands, and nothing sent to it, SIGKILL included, acts until
    /// that wait returns.
    pub fn waiting_in_kernel(&self) -> bool {
        !self.uninterruptible.is_empty()
    }
}

impl std::fmt::Display for KernelThreadStates {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let leader_exited = self.leader == Some('Z');
        if self.uninterruptible.is_empty() {
            return match (leader_exited, self.threads) {
                (true, 0 | 1) => formatter
                    .write_str("a zombie: it has exited, and only its parent's reap is outstanding"),
                (true, threads) => write!(
                    formatter,
                    "its main thread has exited and {} other thread(s) have not, none in uninterruptible sleep",
                    threads - 1
                ),
                (false, threads) => write!(
                    formatter,
                    "{threads} thread(s), none in uninterruptible sleep; main thread state {}",
                    self.leader.unwrap_or('?')
                ),
            };
        }
        if leader_exited {
            formatter.write_str("its main thread has exited; ")?;
        }
        write!(
            formatter,
            "{} of {} thread(s) in uninterruptible sleep, which no signal interrupts (",
            self.uninterruptible.len(),
            self.threads
        )?;
        for (index, (tid, wchan)) in self.uninterruptible.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            match wchan {
                Some(wchan) => write!(formatter, "{tid} in {wchan}")?,
                None => write!(formatter, "{tid}")?,
            }
        }
        formatter.write_str(")")
    }
}

/// The state of every thread of `pid`, or `None` when the kernel no longer
/// lists it or this platform cannot say.
#[cfg(target_os = "linux")]
pub fn kernel_thread_states(pid: u32) -> Option<KernelThreadStates> {
    let tasks = std::fs::read_dir(format!("/proc/{pid}/task")).ok()?;
    let mut states = KernelThreadStates {
        leader: None,
        threads: 0,
        uninterruptible: Vec::new(),
    };
    for tid in tasks
        .flatten()
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
    {
        let Some(state) = std::fs::read_to_string(format!("/proc/{pid}/task/{tid}/stat"))
            .ok()
            .as_deref()
            .and_then(thread_state)
        else {
            continue;
        };
        states.threads += 1;
        if tid == pid {
            states.leader = Some(state);
        }
        if state == 'D' {
            // `0` is the kernel declining to name the function.
            let wchan = std::fs::read_to_string(format!("/proc/{pid}/task/{tid}/wchan"))
                .ok()
                .map(|wchan| wchan.trim().to_owned())
                .filter(|wchan| !wchan.is_empty() && wchan != "0");
            states.uninterruptible.push((tid, wchan));
        }
    }
    states.uninterruptible.sort();
    (states.threads > 0).then_some(states)
}

#[cfg(not(target_os = "linux"))]
pub fn kernel_thread_states(_pid: u32) -> Option<KernelThreadStates> {
    None
}

/// The state letter of a `/proc/.../stat` line: the first field after the
/// comm's final `)`.
#[cfg(target_os = "linux")]
fn thread_state(stat: &str) -> Option<char> {
    stat.rsplit_once(')')?
        .1
        .split_whitespace()
        .next()?
        .chars()
        .next()
}

/// How long, in total since boot, every non-idle task on this host was stalled
/// on I/O at once: the `full` line of `/proc/pressure/io`, whose `total` the
/// kernel keeps in microseconds. `None` where the kernel keeps no pressure
/// accounting (built without PSI, or booted with it off) or the platform has
/// none.
///
/// `full` rather than `some` because only `full` is time in which nothing on
/// the host could have run: a process that was runnable during a `some` stall
/// was runnable, and a host where something always waits on its disk would
/// otherwise read as permanently stalled.
#[cfg(target_os = "linux")]
pub fn host_io_full_stall() -> Option<std::time::Duration> {
    io_full_stall_total(&std::fs::read_to_string("/proc/pressure/io").ok()?)
}

#[cfg(not(target_os = "linux"))]
pub fn host_io_full_stall() -> Option<std::time::Duration> {
    None
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn io_full_stall_total(pressure: &str) -> Option<std::time::Duration> {
    let micros = pressure
        .lines()
        .find(|line| line.starts_with("full "))?
        .split_whitespace()
        .find_map(|field| field.strip_prefix("total="))?
        .parse::<u64>()
        .ok()?;
    Some(std::time::Duration::from_micros(micros))
}

/// An identifier the kernel mints once per boot, or `None` where this platform
/// cannot say. Two different values prove a restart in between, and so that
/// no process running before it still runs.
#[cfg(target_os = "linux")]
pub fn boot_id() -> Option<String> {
    let id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
    let id = id.trim();
    (!id.is_empty()).then(|| id.to_owned())
}

#[cfg(target_os = "macos")]
pub fn boot_id() -> Option<String> {
    let mut buffer = [0_u8; 64];
    let mut size = buffer.len();
    let read = unsafe {
        libc::sysctlbyname(
            c"kern.bootsessionuuid".as_ptr().cast(),
            buffer.as_mut_ptr().cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if read != 0 {
        return None;
    }
    let id = std::str::from_utf8(buffer.get(..size)?)
        .ok()?
        .trim_end_matches('\0')
        .trim();
    (!id.is_empty()).then(|| id.to_owned())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn boot_id() -> Option<String> {
    None
}

/// A process identified by its pid and kernel start time, kept as evidence
/// about the process group it was seen in.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessAnchor {
    pub pid: u32,
    pub start_token: String,
}

/// Every live process now in `process_group_id`. Zombies are left out: they
/// hold nothing, cannot be signalled into exiting, and leave once reaped.
#[cfg(target_os = "linux")]
pub fn process_group_members(process_group_id: u32) -> Result<Vec<ProcessAnchor>, String> {
    let entries =
        std::fs::read_dir("/proc").map_err(|error| format!("enumerate /proc: {error}"))?;
    Ok(entries
        .flatten()
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
        .filter_map(|pid| {
            let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
            let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
            if *fields.first()? == "Z" || fields.get(2)?.parse::<u32>().ok()? != process_group_id {
                return None;
            }
            Some(ProcessAnchor {
                pid,
                start_token: (*fields.get(19)?).to_owned(),
            })
        })
        .collect())
}

/// `proc_listpids` selector for "members of process group N", from the macOS
/// SDK's `sys/proc_info.h`. The `libc` crate does not export it.
#[cfg(target_os = "macos")]
const PROC_PGRP_ONLY: u32 = 2;

#[cfg(target_os = "macos")]
pub fn process_group_members(process_group_id: u32) -> Result<Vec<ProcessAnchor>, String> {
    let pid_size = std::mem::size_of::<libc::c_int>();
    let needed =
        unsafe { libc::proc_listpids(PROC_PGRP_ONLY, process_group_id, std::ptr::null_mut(), 0) };
    if needed < 0 {
        return Err(format!(
            "list process group {process_group_id}: {}",
            std::io::Error::last_os_error()
        ));
    }
    // Room for members that join between the size query and the listing.
    let mut pids = vec![0 as libc::c_int; needed as usize / pid_size + 64];
    let filled = unsafe {
        libc::proc_listpids(
            PROC_PGRP_ONLY,
            process_group_id,
            pids.as_mut_ptr().cast(),
            (pids.len() * pid_size) as libc::c_int,
        )
    };
    if filled < 0 {
        return Err(format!(
            "list process group {process_group_id}: {}",
            std::io::Error::last_os_error()
        ));
    }
    pids.truncate(filled as usize / pid_size);
    Ok(pids
        .into_iter()
        .filter(|pid| *pid > 0)
        .filter_map(|pid| {
            let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
            let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
            let read = unsafe {
                libc::proc_pidinfo(
                    pid,
                    libc::PROC_PIDTBSDINFO,
                    0,
                    (&mut info as *mut libc::proc_bsdinfo).cast(),
                    size,
                )
            };
            (read == size && info.pbi_status != libc::SZOMB && info.pbi_pgid == process_group_id)
                .then(|| ProcessAnchor {
                    pid: pid as u32,
                    start_token: format!("{}:{}", info.pbi_start_tvsec, info.pbi_start_tvusec),
                })
        })
        .collect())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn process_group_members(_process_group_id: u32) -> Result<Vec<ProcessAnchor>, String> {
    Err("process group membership cannot be read on this platform".into())
}

/// Signal a process group whose leader is gone, but only while one of
/// `anchors`, processes seen in that group while its leader was verified
/// alive, is verified to still be in it. Returns the anchor that justified the
/// signal, or `None` when no anchor remains in the group and nothing was sent.
///
/// Without a live leader a group id fences nothing on its own: once the group
/// empties, the number can be handed out again. But the kernel never hands out
/// a pid that is still in use as a group id, so a group a recorded member has
/// stayed in has existed continuously since that member was seen there, and is
/// the group the leader led.
///
/// The fence is a check, not a handle, and the gap between the last check and
/// the signal is not closed. If the anchor is the group's last member and exits
/// inside it, the group ends and its number becomes free; signalling a
/// stranger then needs the kernel to hand the number to a new group before the
/// very next syscall. Both kernels allocate pids in sequence and skip numbers in
/// use, so that is a wrap of the whole pid space (99,999 on macOS, `pid_max` on
/// Linux) between two adjacent syscalls. The anchor's start token is therefore
/// checked last, after its group, to keep the gap to exactly those two calls.
/// Darwin offers no unprivileged process handle that would close it; on Linux
/// a caller that can hold a pidfd should signal through that instead.
#[cfg(unix)]
pub fn signal_anchored_group(
    process_group_id: u32,
    anchors: &[ProcessAnchor],
    signal: libc::c_int,
) -> Result<Option<u32>, String> {
    let Some(anchor) = anchors.iter().find(|anchor| {
        process_presence(anchor.pid, &anchor.start_token) == ProcessPresence::Present
            && unsafe { libc::getpgid(anchor.pid as libc::pid_t) }
                == process_group_id as libc::pid_t
            // Again, immediately before the signal: an anchor that exited
            // between the first check and `getpgid` is caught here, so the
            // pid `getpgid` answered for is the anchor's.
            && process_presence(anchor.pid, &anchor.start_token) == ProcessPresence::Present
    }) else {
        return Ok(None);
    };
    if unsafe { libc::kill(-(process_group_id as libc::pid_t), signal) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(format!("signal process group {process_group_id}: {error}"));
        }
    }
    Ok(Some(anchor.pid))
}

#[cfg(all(test, any(target_os = "macos", windows)))]
mod kernel_presence_tests {
    use super::*;

    #[test]
    fn an_unreadable_identity_is_not_proof_of_exit() {
        let presence = presence_from_kernel_read(cairn_procmem::KernelRead::Unreadable, "123");
        assert_eq!(presence, ProcessPresence::Unverifiable);
        assert!(!presence.is_gone());
        assert_eq!(
            presence_from_kernel_read(cairn_procmem::KernelRead::Gone, "123"),
            ProcessPresence::Gone
        );
    }
}

#[cfg(all(test, windows))]
mod windows_presence_tests {
    use super::*;

    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn a_retained_exited_child_keeps_its_token_but_is_not_present() {
        let mut child = Child(
            std::process::Command::new("bun")
                .args(["-e", "setInterval(() => {}, 1000)"])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("spawn a long-lived child"),
        );
        let pid = child.0.id();
        let token = process_start_token(pid).expect("live child token");
        assert_eq!(process_presence(pid, &token), ProcessPresence::Present);
        assert_eq!(
            process_presence(pid, "not-the-token"),
            ProcessPresence::Reused
        );
        child.0.kill().expect("terminate the child");
        child.0.wait().expect("wait for child exit");
        assert!(!process_is_alive(pid));
        assert_eq!(
            process_start_token(pid),
            Some(token.clone()),
            "the retained process handle must preserve capture identity"
        );
        assert_eq!(process_presence(pid, &token), ProcessPresence::Gone);
    }
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

    #[cfg(target_os = "macos")]
    #[test]
    fn an_unreaped_child_keeps_its_token_but_is_not_present() {
        struct Child(std::process::Child);
        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let mut child = Child(
            std::process::Command::new("sleep")
                .arg("600")
                .spawn()
                .unwrap(),
        );
        let pid = child.0.id();
        let token = process_start_token(pid).expect("live child token");
        child.0.kill().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !matches!(
            cairn_procmem::platform::read_kernel_state(pid),
            cairn_procmem::KernelRead::Gone
        ) {
            assert!(
                std::time::Instant::now() < deadline,
                "child did not finish exiting"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(
            process_start_token(pid),
            Some(token.clone()),
            "capture must include unreaped children"
        );
        assert_eq!(process_presence(pid, &token), ProcessPresence::Gone);
        assert_eq!(
            signal_verified(pid, &token, None, libc::SIGKILL).unwrap(),
            SignalOutcome::Gone
        );
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

    #[test]
    fn a_thread_stuck_in_the_kernel_is_named_with_where_it_waits() {
        let states = KernelThreadStates {
            leader: Some('Z'),
            threads: 2,
            uninterruptible: vec![(4242, Some("btrfs_sync_file".into()))],
        };
        assert_eq!(
            states.to_string(),
            "its main thread has exited; 1 of 2 thread(s) in uninterruptible sleep, which no signal interrupts (4242 in btrfs_sync_file)"
        );
    }

    #[test]
    fn the_host_io_stall_is_the_full_lines_cumulative_total() {
        let pressure = "some avg10=41.20 avg60=38.00 avg300=12.00 total=93000000\n\
                        full avg10=22.50 avg60=19.00 avg300=6.00 total=51000000\n";
        assert_eq!(
            io_full_stall_total(pressure),
            Some(std::time::Duration::from_secs(51))
        );
        // A kernel that keeps only `some` (memory pressure on old kernels,
        // for one) has no answer here, which is not a zero.
        assert_eq!(
            io_full_stall_total("some avg10=0.00 avg60=0.00 avg300=0.00 total=12\n"),
            None
        );
    }

    #[test]
    fn only_a_thread_in_uninterruptible_sleep_puts_a_process_in_the_kernels_hands() {
        let zombie = KernelThreadStates {
            leader: Some('Z'),
            threads: 1,
            uninterruptible: Vec::new(),
        };
        assert!(!zombie.waiting_in_kernel());
        let syncing = KernelThreadStates {
            leader: Some('S'),
            threads: 7,
            uninterruptible: vec![(4243, Some("ext4_sync_file".into()))],
        };
        assert!(syncing.waiting_in_kernel());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_running_process_lists_its_threads_under_a_live_leader() {
        let states = kernel_thread_states(std::process::id()).expect("own threads");
        assert!(states.threads >= 1, "{states:?}");
        assert!(!matches!(states.leader, None | Some('Z')), "{states:?}");
    }

    /// The case a SIGKILLed executor that will not finish exiting has to be
    /// told apart from: a child that has exited completely and that only a
    /// missed `waitpid` keeps listed.
    #[cfg(target_os = "linux")]
    #[test]
    fn an_exited_child_nobody_has_reaped_reads_as_a_zombie_with_nothing_left_waiting() {
        let mut child = std::process::Command::new("sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let pid = child.id();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let states = loop {
            let states = kernel_thread_states(pid).expect("an unreaped child is still listed");
            if states.leader == Some('Z') {
                break states;
            }
            assert!(std::time::Instant::now() < deadline, "{states:?}");
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        assert_eq!(states.threads, 1, "{states:?}");
        assert!(states.uninterruptible.is_empty(), "{states:?}");
        assert!(states.to_string().contains("zombie"), "{states}");
        child.wait().unwrap();
        assert_eq!(kernel_thread_states(pid), None);
    }
}
