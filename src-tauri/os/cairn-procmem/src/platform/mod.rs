use crate::identity::{KernelProcess, KernelRead};
#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(windows)]
mod windows;
#[cfg(target_os = "macos")]
pub use darwin::*;
#[cfg(target_os = "linux")]
pub use linux::*;
#[cfg(windows)]
pub use windows::*;

pub const MEMORY_SUPPORTED: bool = cfg!(any(target_os = "linux", target_os = "macos", windows));
pub const CPU_SUPPORTED: bool = MEMORY_SUPPORTED;
pub const STORAGE_SUPPORTED: bool = cfg!(any(target_os = "linux", target_os = "macos"));
/// Whether census rows carry [`crate::SchedulingTimes`]: only Darwin's
/// `proc_pid_rusage` reports runnable time per process.
pub const SCHEDULING_SUPPORTED: bool = cfg!(target_os = "macos");

pub fn read_kernel_process(pid: u32) -> Option<KernelProcess> {
    match read_kernel_state(pid) {
        KernelRead::Live(process) => Some(process),
        KernelRead::Gone | KernelRead::Unreadable => None,
    }
}
/// Includes zombies so a parent can capture an exited, unreaped child.
#[cfg(not(windows))]
pub fn read_start_token(pid: u32) -> Option<String> {
    read_unreaped_child(pid)
        .ok()
        .map(|process| process.start.token())
}
#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub fn census(_: &mut crate::census::Census) -> bool {
    false
}
/// No descriptor census off macOS and Linux: the result says unsupported.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn fd_census_into(_: std::time::Instant, _: &mut crate::fd_census::FdCensus) -> bool {
    false
}
#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub fn read_kernel_state(_: u32) -> KernelRead {
    KernelRead::Unreadable
}
#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub fn read_unreaped_child(_: u32) -> Result<KernelProcess, String> {
    Err("process identity unsupported".into())
}
#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub fn process_rss_bytes(_: u32) -> Option<u64> {
    None
}

/// Windows reports no footprint distinct from its working set yet, so its rows
/// carry RSS alone.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn process_footprint_bytes(_: u32) -> Option<u64> {
    None
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::census::Census;
    #[test]
    fn native_census_reads_own_identity_and_rss() {
        let pid = std::process::id();
        let process = read_kernel_process(pid).expect("own identity");
        assert_eq!(read_start_token(pid), Some(process.start.token()));
        let mut out = Census::default();
        out.storage_wanted.push(pid);
        out.footprint_wanted.push(pid);
        assert!(census(&mut out));
        let row = out
            .rows
            .iter()
            .find(|row| row.process.pid == pid)
            .expect("own row");
        assert_eq!(row.process.start, process.start);
        assert!(row.resident_bytes.is_some_and(|bytes| bytes > 0));
        assert!(row.cpu_ms.is_some());
        assert!(row.storage_io_bytes.is_some());
        assert!(process_rss_bytes(pid).is_some_and(|bytes| bytes > 0));
    }
    #[test]
    fn exited_unreaped_child_preserves_start_token_but_is_not_live() {
        let mut child = std::process::Command::new("sleep")
            .arg("0.1")
            .spawn()
            .unwrap();
        let pid = child.id();
        let token = read_start_token(pid).expect("child token");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !matches!(read_kernel_state(pid), KernelRead::Gone)
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let gone = matches!(read_kernel_state(pid), KernelRead::Gone);
        let after_exit = read_start_token(pid);
        child.wait().unwrap();
        assert!(gone);
        assert_eq!(after_exit, Some(token));
        assert!(read_start_token(pid).is_none());
    }
}
