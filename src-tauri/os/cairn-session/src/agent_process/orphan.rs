//! Stopping agent processes orphaned by a host that died without cleaning up.
//!
//! `lifecycle::stop_agents_for_host_shutdown` covers the graceful case: a runner
//! that gets SIGTERM stops its agents before it exits. This module covers the
//! other case — SIGKILL, a panic, an OOM kill — where the runner never got the
//! chance. Its agents survive, reparented to launchd/systemd, and keep calling
//! tools that nothing records. The successor runner's ownership fence refuses
//! those calls, so the work stops being harmful; this is what stops it burning
//! tokens against a wall (CAIRN-3287).
//!
//! ## Identity is proven, not guessed
//!
//! A stale run is matched to a process by the **session UUID in the process's
//! argv**: the Claude CLI is spawned with `--session-id <uuid>` (or `--resume
//! <uuid>`), a UUID match is unambiguous, and unlike a stored pid it cannot be
//! defeated by pid reuse. Deliberately no pid column exists for this reason.
//!
//! This runs only from the startup sweep, when the runner owns no agent processes
//! of its own, so a match is by definition someone else's orphan. Calling it
//! anywhere else would risk signalling a live agent and is not supported.
//!
//! ## Known gap
//!
//! A Codex backend spawns `codex app-server` with no session id in its argv, so
//! its processes are not reachable this way and are left to the fence. Closing
//! that needs a durable process identity the app-server pool does not record.

/// A running process as the sweep sees it: its pid and its full command line.
pub type ProcessEntry = (u32, String);

/// The OS interactions the sweep needs, behind a trait so a test can supply a
/// process table and observe which pids were signalled without spawning
/// anything.
pub trait ProcessTable: Send + Sync {
    /// Every running process's pid and command line. Best-effort: an
    /// unenumerable platform returns nothing rather than failing.
    fn list(&self) -> Vec<ProcessEntry>;

    /// Stop `pid`, returning whether the process is CONFIRMED gone afterwards.
    ///
    /// The return value is load-bearing, not a courtesy: its caller records
    /// `orphan_reaped` on the strength of it, and that row is the difference
    /// between a death Cairn caused and one it assumed. `false` therefore means
    /// "could not confirm" and covers every unconfirmed case — a signal that
    /// failed (no permission), a process that outlived escalation, and a pid that
    /// was already gone before the first signal (which is dead, but not by our
    /// hand). Only a process observed to disappear after a signal we sent counts.
    fn stop(&self, pid: u32) -> bool;
}

/// The `(pid, session_id)` pairs among `entries` whose command line names one of
/// `session_ids`, excluding this process.
///
/// Pure, so the selection is testable without any process at all. An empty
/// session id is skipped rather than matching every command line.
pub fn orphans_naming_sessions(
    entries: &[ProcessEntry],
    session_ids: &[String],
    self_pid: u32,
) -> Vec<(u32, String)> {
    entries
        .iter()
        .filter(|(pid, _)| *pid != self_pid)
        .filter_map(|(pid, command)| {
            session_ids
                .iter()
                .find(|session_id| !session_id.is_empty() && command.contains(session_id.as_str()))
                .map(|session_id| (*pid, session_id.clone()))
        })
        .collect()
}

/// Stop every running process that names one of `session_ids`, and report the
/// session ids actually signalled so their rows can record that they were
/// stopped rather than assumed dead.
pub fn reap_sessions(table: &dyn ProcessTable, session_ids: &[String]) -> Vec<String> {
    if session_ids.is_empty() {
        return Vec::new();
    }
    let orphans = orphans_naming_sessions(&table.list(), session_ids, std::process::id());
    let mut reaped = Vec::new();
    for (pid, session_id) in orphans {
        log::warn!(
            "orphan_reap: stopping pid={pid} left over from session {session_id}; \
             its host exited without stopping it"
        );
        if !table.stop(pid) {
            // Say so rather than recording a stop that did not happen. A zombie
            // we could not kill is worse news than one we could, and it must not
            // be reported as reaped.
            log::warn!(
                "orphan_reap: could NOT confirm pid={pid} (session {session_id}) stopped; \
                 it may still be running"
            );
            continue;
        }
        if !reaped.contains(&session_id) {
            reaped.push(session_id);
        }
    }
    reaped
}

/// Production table over the OS process list.
pub struct OsProcessTable;

#[cfg(unix)]
impl ProcessTable for OsProcessTable {
    fn list(&self) -> Vec<ProcessEntry> {
        // One `ps` covers macOS and Linux identically and needs no new
        // dependency. `pid=,command=` suppresses the header, so every line is
        // `<pid> <argv...>`.
        let output = match std::process::Command::new("ps")
            .args(["-Ao", "pid=,command="])
            .output()
        {
            Ok(output) => output,
            Err(error) => {
                log::warn!("orphan_reap: could not list processes: {error}");
                return Vec::new();
            }
        };
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| {
                let line = line.trim_start();
                let (pid, command) = line.split_once(char::is_whitespace)?;
                Some((pid.parse().ok()?, command.trim_start().to_string()))
            })
            .collect()
    }

    fn stop(&self, pid: u32) -> bool {
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::Pid;
        let pid = Pid::from_raw(pid as i32);
        // A SIGTERM that cannot be delivered confirms nothing. ESRCH means the
        // process went away on its own and EPERM means it was never ours to stop;
        // neither is a stop we performed, so neither may be reported as one.
        if let Err(error) = kill(pid, Signal::SIGTERM) {
            log::warn!("orphan_reap: SIGTERM to pid={pid} failed: {error}");
            return false;
        }
        // Escalate rather than assume. Bounded tightly because this is on the
        // startup path; with graceful shutdown stopping agents there should be
        // nothing here to wait for at all.
        for _ in 0..10 {
            std::thread::sleep(std::time::Duration::from_millis(100));
            if kill(pid, None).is_err() {
                return true;
            }
        }
        if let Err(error) = kill(pid, Signal::SIGKILL) {
            log::warn!("orphan_reap: SIGKILL to pid={pid} failed: {error}");
            return false;
        }
        // SIGKILL is not synchronous: the process is dead only once the kernel has
        // reaped it, so confirm disappearance instead of trusting the signal.
        for _ in 0..10 {
            std::thread::sleep(std::time::Duration::from_millis(50));
            if kill(pid, None).is_err() {
                return true;
            }
        }
        false
    }
}

#[cfg(not(unix))]
impl ProcessTable for OsProcessTable {
    // Windows has no equivalent seam here yet, and the runner service on Windows
    // does not orphan agents through reparenting the way a unix daemon does.
    // Reaping nothing leaves the ownership fence as the whole defense there.
    fn list(&self) -> Vec<ProcessEntry> {
        Vec::new()
    }

    fn stop(&self, pid: u32) -> bool {
        log::warn!("orphan_reap: cannot stop pid={pid} on this platform");
        false
    }
}

/// Test table over an injected process list that records what it was asked to
/// stop, so a test asserts on signalled pids without touching a real process.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Default)]
pub struct RecordingProcessTable {
    entries: Vec<ProcessEntry>,
    stopped: std::sync::Mutex<Vec<u32>>,
    stop_confirms: bool,
}

#[cfg(any(test, feature = "test-utils"))]
impl RecordingProcessTable {
    /// A table whose stops all succeed.
    pub fn new(entries: Vec<ProcessEntry>) -> Self {
        Self {
            entries,
            stopped: std::sync::Mutex::new(Vec::new()),
            stop_confirms: true,
        }
    }

    /// A table that is asked to stop processes and cannot — a signal it has no
    /// permission to send, or a process that survives escalation.
    pub fn unable_to_stop(entries: Vec<ProcessEntry>) -> Self {
        Self {
            stop_confirms: false,
            ..Self::new(entries)
        }
    }

    /// The pids `stop` was attempted on, in order.
    pub fn stopped(&self) -> Vec<u32> {
        self.stopped.lock().unwrap().clone()
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl ProcessTable for RecordingProcessTable {
    fn list(&self) -> Vec<ProcessEntry> {
        self.entries.clone()
    }

    fn stop(&self, pid: u32) -> bool {
        self.stopped.lock().unwrap().push(pid);
        self.stop_confirms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries() -> Vec<ProcessEntry> {
        vec![
            (10, "/bin/claude --session-id 934d794a --print".to_string()),
            (11, "/bin/claude --resume 4fac7e94 --print".to_string()),
            (12, "/usr/bin/rg --files".to_string()),
            (13, "cairn-runner run --port 3849".to_string()),
        ]
    }

    #[test]
    fn selects_only_processes_naming_a_stale_session() {
        let stale = vec!["934d794a".to_string()];
        assert_eq!(
            orphans_naming_sessions(&entries(), &stale, 99),
            vec![(10, "934d794a".to_string())]
        );
    }

    #[test]
    fn matches_a_resumed_session_as_well_as_a_fresh_one() {
        let stale = vec!["4fac7e94".to_string()];
        assert_eq!(
            orphans_naming_sessions(&entries(), &stale, 99),
            vec![(11, "4fac7e94".to_string())]
        );
    }

    #[test]
    fn never_selects_this_process() {
        let stale = vec!["3849".to_string()];
        assert!(orphans_naming_sessions(&entries(), &stale, 13).is_empty());
    }

    #[test]
    fn an_empty_session_id_matches_nothing() {
        // `contains("")` is true for every string, so a NULL-ish session id must
        // be skipped or the sweep would signal every process on the machine.
        assert!(orphans_naming_sessions(&entries(), &[String::new()], 99).is_empty());
    }

    #[test]
    fn reap_stops_matching_pids_only_and_reports_their_sessions() {
        let table = RecordingProcessTable::new(entries());
        let reaped = reap_sessions(
            &table,
            &["934d794a".to_string(), "deadbeef-not-running".to_string()],
        );
        assert_eq!(table.stopped(), vec![10]);
        assert_eq!(reaped, vec!["934d794a".to_string()]);
    }

    #[test]
    fn a_process_that_could_not_be_stopped_is_never_reported_as_reaped() {
        // The forensic claim of `orphan_reaped` is that Cairn stopped the process.
        // An unstoppable one (no permission, or it survived escalation) must fall
        // back to the assumed-death reason, even though we tried.
        let table = RecordingProcessTable::unable_to_stop(entries());
        let reaped = reap_sessions(&table, &["934d794a".to_string()]);
        assert_eq!(table.stopped(), vec![10], "the stop is still attempted");
        assert!(
            reaped.is_empty(),
            "an unconfirmed stop must not be recorded as a reap: {reaped:?}"
        );
    }

    #[test]
    fn reap_with_no_stale_sessions_does_not_even_list_processes() {
        let table = RecordingProcessTable::new(entries());
        assert!(reap_sessions(&table, &[]).is_empty());
        assert!(table.stopped().is_empty());
    }
}
