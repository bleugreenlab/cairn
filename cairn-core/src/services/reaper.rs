//! Process reaper: SIGKILL every process whose current working directory is
//! rooted under a target directory.
//!
//! ## Why cwd, and why this exists
//!
//! Worktree teardown and the worktree GC remove a worktree directory, but a
//! process still *running* inside it is orphaned rather than stopped. The
//! motivating failure (CAIRN-2390): `bun dev:instance` launched from a worktree
//! spawns `cargo run -p cairn-runner` and `bunx tauri dev` with `detached: true`
//! (see `scripts/dev-instance.ts`) — each in its own session / process group.
//! When the GC sweeps the worktree, vite dies (its cwd vanished) but the
//! launcher-owned runner and the window-less app keep running.
//!
//! Nothing keyed on a PTY shell child or its process group can reach such a
//! tree: those dev processes are detached grandchildren that escaped into new
//! groups. The one handle that still catches them is their **current working
//! directory** — the runner (cwd `<worktree>/src-tauri`), tauri, vite, and the
//! app all have a cwd inside the worktree. This reaper enumerates processes by
//! cwd and SIGKILLs the process group of any rooted at or under the target dir.
//!
//! It is strictly best-effort: enumeration or kill failures are logged and
//! skipped, never propagated. A process that dies between enumeration and kill
//! (ESRCH) is a no-op success. The calling process is always excluded so the
//! very process performing cleanup never SIGKILLs itself.
//!
//! ## Platforms
//!
//! macOS enumerates via `proc_listallpids` and resolves each pid's cwd through
//! `proc_pidinfo(PROC_PIDVNODEPATHINFO)`; Linux reads the `/proc/<pid>/cwd`
//! symlink. Both go through direct `libc` FFI. `libproc` was evaluated first but
//! its `pidcwd` is hardcoded to return an error on macOS — the platform this fix
//! targets — so it cannot resolve a cwd where it matters. Other targets (Windows)
//! compile a no-op that reaps nothing: dev instances and the worktree filesystem
//! sweep are unix-only.

use std::path::{Path, PathBuf};

/// Enumerate and kill processes by their current working directory.
pub trait ProcessReaper: Send + Sync {
    /// SIGKILL the process group of every running process whose cwd is `dir`
    /// itself or a descendant of it, excluding the calling process. Returns the
    /// pids that were targeted. Best-effort: never blocks, never fails.
    fn reap_under(&self, dir: &Path) -> Vec<u32>;
}

/// Pure filter over an injected `(pid, cwd)` list: the pids whose cwd is `dir`
/// itself or a descendant, excluding `self_pid`. Separated from OS enumeration
/// so the selection logic is unit-testable without spawning real processes.
///
/// Uses `Path::starts_with`, which matches whole path components — so
/// `/base/wt` matches `/base/wt` and `/base/wt/src-tauri` but never the sibling
/// `/base/wt-2`.
pub fn pids_rooted_under(entries: &[(u32, PathBuf)], dir: &Path, self_pid: u32) -> Vec<u32> {
    entries
        .iter()
        .filter(|(pid, cwd)| *pid != self_pid && cwd.starts_with(dir))
        .map(|(pid, _)| *pid)
        .collect()
}

/// Production reaper backed by OS process enumeration.
pub struct OsProcessReaper;

#[cfg(any(target_os = "macos", target_os = "linux"))]
impl ProcessReaper for OsProcessReaper {
    fn reap_under(&self, dir: &Path) -> Vec<u32> {
        // Canonicalize so a symlinked base (macOS `/var` -> `/private/var`)
        // still prefix-matches the kernel-resolved cwds. The dir still exists at
        // reap time (we reap before deletion), so this resolves; if it does not,
        // fall back to the raw path.
        let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
        let entries = enumerate_process_cwds();
        let pids = pids_rooted_under(&entries, &dir, std::process::id());
        for &pid in &pids {
            kill_process_group(pid);
        }
        pids
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
impl ProcessReaper for OsProcessReaper {
    fn reap_under(&self, _dir: &Path) -> Vec<u32> {
        Vec::new()
    }
}

/// SIGKILL the process group led by `pid`, falling back to the bare pid if no
/// group with that id exists (the process was not a group leader). ESRCH on an
/// already-dead target is harmless. Every rooted pid is signalled individually,
/// so even a non-leader is reaped; the group signal is a bonus that also catches
/// a leader's not-independently-rooted children.
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn kill_process_group(pid: u32) {
    use nix::errno::Errno;
    use nix::sys::signal::{kill, killpg, Signal};
    use nix::unistd::Pid;
    let p = Pid::from_raw(pid as i32);
    match killpg(p, Signal::SIGKILL) {
        Ok(()) => {}
        Err(Errno::ESRCH) => {
            // Not a group leader (or the group is already gone): signal the pid.
            let _ = kill(p, Signal::SIGKILL);
        }
        Err(e) => log::debug!("Reaper: killpg({pid}) failed: {e}"),
    }
}

#[cfg(target_os = "linux")]
fn enumerate_process_cwds() -> Vec<(u32, PathBuf)> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir("/proc") else {
        return out;
    };
    for entry in rd.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|n| n.parse::<u32>().ok())
        else {
            continue;
        };
        // `/proc/<pid>/cwd` is a symlink to the process's working directory.
        if let Ok(cwd) = std::fs::read_link(format!("/proc/{pid}/cwd")) {
            out.push((pid, cwd));
        }
    }
    out
}

#[cfg(target_os = "macos")]
fn enumerate_process_cwds() -> Vec<(u32, PathBuf)> {
    list_all_pids()
        .into_iter()
        .filter_map(|pid| process_cwd(pid).map(|cwd| (pid, cwd)))
        .collect()
}

/// Every active pid, via `proc_listallpids`. It returns a byte count, so the
/// buffer is sized in bytes and the pid count derived from the returned bytes.
#[cfg(target_os = "macos")]
fn list_all_pids() -> Vec<u32> {
    // First call with a null buffer returns the number of BYTES needed.
    let needed = unsafe { libc::proc_listallpids(std::ptr::null_mut(), 0) };
    if needed <= 0 {
        return Vec::new();
    }
    // Slack for pids that appear between the sizing and fill calls.
    let cap = needed as usize / std::mem::size_of::<i32>() + 64;
    let mut pids = vec![0i32; cap];
    let filled = unsafe {
        libc::proc_listallpids(
            pids.as_mut_ptr() as *mut libc::c_void,
            (cap * std::mem::size_of::<i32>()) as libc::c_int,
        )
    };
    if filled <= 0 {
        return Vec::new();
    }
    let count = filled as usize / std::mem::size_of::<i32>();
    pids.into_iter()
        .take(count)
        .filter(|&p| p > 0)
        .map(|p| p as u32)
        .collect()
}

/// The working directory of `pid` via `proc_pidinfo(PROC_PIDVNODEPATHINFO)`.
/// `None` when the process is gone (ESRCH) or not inspectable (EPERM), or the
/// call returns a short read.
#[cfg(target_os = "macos")]
fn process_cwd(pid: u32) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStrExt;
    let mut info: libc::proc_vnodepathinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_vnodepathinfo>() as libc::c_int;
    let ret = unsafe {
        libc::proc_pidinfo(
            pid as libc::c_int,
            libc::PROC_PIDVNODEPATHINFO,
            0,
            &mut info as *mut _ as *mut libc::c_void,
            size,
        )
    };
    // proc_pidinfo returns the number of bytes written; a short read means the
    // cwd is not reliably populated, and <= 0 is an error.
    if ret < size {
        return None;
    }
    // `vip_path` is `[[c_char; 32]; 32]` — a contiguous MAXPATHLEN (1024) byte,
    // NUL-terminated buffer. Read it as a flat C string.
    let raw = &info.pvi_cdir.vip_path;
    let bytes = unsafe {
        std::slice::from_raw_parts(raw.as_ptr() as *const u8, std::mem::size_of_val(raw))
    };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    if end == 0 {
        return None;
    }
    Some(PathBuf::from(std::ffi::OsStr::from_bytes(&bytes[..end])))
}

/// Test reaper that records the dirs it was asked to reap and returns a canned
/// pid list, without touching any real process. Mirrors
/// [`super::process::RecordingProcessSpawner`]'s permissive-default style.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone, Default)]
pub struct RecordingReaper {
    calls: std::sync::Arc<std::sync::Mutex<Vec<PathBuf>>>,
    returns: Vec<u32>,
}

#[cfg(any(test, feature = "test-utils"))]
impl RecordingReaper {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// A reaper that reports `pids` as reaped on every call.
    pub fn returning(pids: Vec<u32>) -> Self {
        Self {
            calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            returns: pids,
        }
    }

    /// The dirs `reap_under` was called with, in order.
    pub fn calls(&self) -> Vec<PathBuf> {
        self.calls.lock().unwrap().clone()
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl ProcessReaper for RecordingReaper {
    fn reap_under(&self, dir: &Path) -> Vec<u32> {
        self.calls.lock().unwrap().push(dir.to_path_buf());
        self.returns.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pids_rooted_under_selects_dir_and_descendants_excluding_self() {
        let dir = PathBuf::from("/base/wt");
        let entries = vec![
            (10, PathBuf::from("/base/wt")),           // equal -> match
            (11, PathBuf::from("/base/wt/src-tauri")), // descendant -> match
            (12, PathBuf::from("/base/other")),        // unrelated -> no
            (13, PathBuf::from("/base/wt-sibling")),   // string-prefix, not a path child -> no
            (99, PathBuf::from("/base/wt/here")),      // self -> excluded
        ];
        assert_eq!(pids_rooted_under(&entries, &dir, 99), vec![10, 11]);
    }

    #[test]
    fn pids_rooted_under_empty_when_nothing_matches() {
        let entries = vec![(1, PathBuf::from("/somewhere/else"))];
        assert!(pids_rooted_under(&entries, Path::new("/base/wt"), 1).is_empty());
    }

    // Real-process integration: a `sleep` whose cwd is under the target dir is
    // reaped, while a sibling `sleep` outside it survives. Mirrors
    // `services::process::tests::kill_on_drop_reaps_when_armed`.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn reaps_process_rooted_under_dir_but_spares_sibling() {
        use std::os::unix::process::CommandExt;

        let base = tempfile::tempdir().unwrap();
        let inside = base.path().join("CAIRN-1-builder-0");
        std::fs::create_dir_all(&inside).unwrap();
        let outside = tempfile::tempdir().unwrap();

        let spawn_sleep = |cwd: &Path| {
            let mut cmd = std::process::Command::new("sleep");
            cmd.arg("30").current_dir(cwd);
            // New process group (pgid == pid) so killpg reaps it as a leader,
            // matching the detached dev-instance processes this targets.
            cmd.process_group(0);
            cmd.spawn().unwrap()
        };

        let mut victim = spawn_sleep(&inside);
        let mut bystander = spawn_sleep(outside.path());
        // Let the children settle so their cwd is enumerable.
        std::thread::sleep(std::time::Duration::from_millis(200));

        let reaped = OsProcessReaper.reap_under(&inside);
        assert!(
            reaped.contains(&victim.id()),
            "victim pid {} should be in the reaped set {reaped:?}",
            victim.id()
        );

        std::thread::sleep(std::time::Duration::from_millis(400));
        assert!(
            victim.try_wait().unwrap().is_some(),
            "process rooted under the dir must be killed"
        );
        assert!(
            bystander.try_wait().unwrap().is_none(),
            "sibling outside the dir must survive"
        );
        let _ = bystander.kill();
        let _ = bystander.wait();
    }
}
