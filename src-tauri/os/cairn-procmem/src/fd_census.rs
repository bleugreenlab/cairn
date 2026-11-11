//! Which of this user's processes hold how many pipe descriptors, read
//! in-process from the kernel.
//!
//! In-process because the census is taken when fresh pipes are exhausted:
//! spawning `lsof` and reading its output through a pipe would depend on the
//! very resource being measured. Same-uid only, so it needs no privilege and
//! never reads another user's descriptors. Bounded by a caller's deadline; a
//! census cut short says so rather than passing for a complete one.

use std::time::Instant;

use crate::identity::{ProcessIdentity, StartKey};

/// One process's descriptors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdCensusRow {
    pub pid: u32,
    pub start: StartKey,
    pub parent_pid: u32,
    /// The kernel's short process name, which for some programs is not their
    /// file name (a Claude CLI reports its version string).
    pub comm: String,
    /// The executable's file name, when its path could be read.
    pub path_basename: Option<String>,
    /// Pipe descriptors, both ends counted.
    pub pipe_fds: u32,
    pub total_fds: u32,
}

impl FdCensusRow {
    pub fn identity(&self) -> ProcessIdentity {
        ProcessIdentity {
            pid: self.pid,
            start_time: self.start,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct FdCensus {
    /// Sorted by pid.
    pub processes: Vec<FdCensusRow>,
    /// Whether this platform can take the census at all.
    pub supported: bool,
    /// The census stopped at its deadline, the process listing overflowed, or
    /// some process's descriptors could not be listed.
    pub partial: bool,
    /// Same-user processes present but unreadable.
    pub unreadable: u32,
    pub took_ms: u64,
}

impl FdCensus {
    pub fn row(&self, pid: u32) -> Option<&FdCensusRow> {
        self.processes
            .binary_search_by_key(&pid, |row| row.pid)
            .ok()
            .map(|index| &self.processes[index])
    }

    pub fn total_pipe_fds(&self) -> u64 {
        self.processes
            .iter()
            .map(|row| u64::from(row.pipe_fds))
            .sum()
    }
}

/// Count every same-user process's pipe descriptors, stopping at `deadline`.
pub fn fd_census(deadline: Instant) -> FdCensus {
    let started = Instant::now();
    let mut out = FdCensus::default();
    out.supported = crate::platform::fd_census_into(deadline, &mut out);
    out.processes.sort_by_key(|row| row.pid);
    out.took_ms = started.elapsed().as_millis() as u64;
    out
}

/// A fixed-size, NUL-padded kernel name as text.
#[cfg(any(target_os = "macos", test))]
pub(crate) fn c_name(bytes: &[libc::c_char]) -> String {
    let bytes: Vec<u8> = bytes
        .iter()
        .take_while(|byte| **byte != 0)
        .map(|byte| byte.to_ne_bytes()[0])
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

#[cfg(all(test, any(target_os = "macos", target_os = "linux")))]
mod tests {
    use super::*;
    use std::os::fd::{FromRawFd, OwnedFd};
    use std::process::{Command, Stdio};
    use std::time::Duration;

    #[test]
    fn a_child_holding_three_pipes_is_counted_with_both_ends() {
        let mut held = Vec::new();
        for _ in 0..3 {
            let mut fds = [0 as libc::c_int; 2];
            // SAFETY: `pipe` writes two descriptors into the array. Left
            // inheritable so the child holds both ends.
            assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
            // SAFETY: just created, owned by nothing else.
            held.extend(fds.map(|fd| unsafe { OwnedFd::from_raw_fd(fd) }));
        }
        let mut child = Command::new("sleep")
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        drop(held);
        let deadline = Instant::now() + Duration::from_secs(3);
        let census = fd_census(deadline);
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(census.supported);
        assert!(census.took_ms <= 3_500, "{}", census.took_ms);
        let row = census.row(child.id()).expect("the child is counted");
        assert_eq!(row.pipe_fds, 6, "{row:?}");
        assert!(row.total_fds >= 6);
        assert_eq!(row.parent_pid, std::process::id());
        assert_eq!(row.path_basename.as_deref(), Some("sleep"));
        assert!(census.row(std::process::id()).is_some());
    }

    #[test]
    fn a_spent_deadline_is_a_partial_census() {
        let census = fd_census(Instant::now());
        assert!(census.partial);
        assert!(census.supported);
    }

    #[test]
    fn kernel_names_stop_at_the_first_nul() {
        let raw = [
            b'z' as libc::c_char,
            b's' as libc::c_char,
            0,
            b'x' as libc::c_char,
        ];
        assert_eq!(c_name(&raw), "zs");
    }
}
