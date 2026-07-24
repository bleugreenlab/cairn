//! Shared `ps`-based process-tree sampling.
//!
//! This is the ONE canonical implementation of the `ps` parsing + descendant
//! tree-selection logic used by BOTH the desktop webview-pressure sampler
//! (`src-tauri/src/commands/profiler.rs`) and the backend resource sampler
//! ([`super`]). It is pure and unit-tested; the only side effect is shelling out
//! to `ps`, isolated in [`sample_ps_rows`].

use std::collections::{HashMap, HashSet};
use std::process::Command;

/// One row of `ps` output: a process with its parent, CPU%, RSS, and command.
#[derive(Debug, Clone)]
pub struct ProcessRow {
    pub pid: u32,
    pub ppid: u32,
    pub cpu_percent: f64,
    pub rss_kb: u64,
    pub command: String,
}

/// Run `ps` over the whole process table and parse it into rows.
///
/// Uses the same column set as the desktop sampler
/// (`pid,ppid,pcpu,rss,comm`). Returns an `Err` string on any spawn/exit
/// failure (e.g. `ps` absent on Windows) so the caller can degrade gracefully.
pub fn sample_ps_rows() -> Result<Vec<ProcessRow>, String> {
    let output = Command::new("ps")
        .args(["-axo", "pid=,ppid=,pcpu=,rss=,comm="])
        .output()
        .map_err(|error| format!("failed to run ps: {error}"))?;

    if !output.status.success() {
        return Err(format!("ps exited with status {}", output.status));
    }

    Ok(parse_ps_rows(&String::from_utf8_lossy(&output.stdout)))
}

/// Parse `ps -axo pid=,ppid=,pcpu=,rss=,comm=` output into rows. The command
/// column may contain spaces, so it is rejoined from the trailing fields.
fn parse_ps_rows(output: &str) -> Vec<ProcessRow> {
    output
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let pid = parts.next()?.parse::<u32>().ok()?;
            let ppid = parts.next()?.parse::<u32>().ok()?;
            let cpu_percent = parts.next()?.parse::<f64>().ok()?;
            let rss_kb = parts.next()?.parse::<u64>().ok()?;
            let command = parts.collect::<Vec<_>>().join(" ");
            Some(ProcessRow {
                pid,
                ppid,
                cpu_percent,
                rss_kb,
                command,
            })
        })
        .collect()
}

/// Select `root_pid` and all of its transitive descendants from the full table.
pub fn select_process_tree(rows: &[ProcessRow], root_pid: u32) -> Vec<ProcessRow> {
    let by_pid = rows
        .iter()
        .map(|row| (row.pid, row))
        .collect::<HashMap<_, _>>();
    rows.iter()
        .filter(|row| row.pid == root_pid || has_ancestor(row.ppid, root_pid, &by_pid))
        .cloned()
        .collect()
}

/// Walk the ppid chain from `pid` looking for `root_pid`. The `seen` set breaks
/// any cycle a reused-pid race could momentarily produce.
fn has_ancestor(mut pid: u32, root_pid: u32, by_pid: &HashMap<u32, &ProcessRow>) -> bool {
    let mut seen = HashSet::new();
    while pid != 0 && seen.insert(pid) {
        if pid == root_pid {
            return true;
        }
        let Some(row) = by_pid.get(&pid) else {
            return false;
        };
        pid = row.ppid;
    }
    false
}

/// Shorten a command to its basename, capped at 120 characters, so a single
/// process line cannot bloat an emitted event.
pub fn shorten_command(command: &str) -> String {
    let tail = command.rsplit('/').next().unwrap_or(command);
    if tail.len() <= 120 {
        tail.to_string()
    } else {
        format!("{}...", &tail[..117])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ps_rows_with_command_spaces() {
        let rows = parse_ps_rows("123 1 4.2 2048 /Applications/Cairn App\n");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pid, 123);
        assert_eq!(rows[0].ppid, 1);
        assert_eq!(rows[0].cpu_percent, 4.2);
        assert_eq!(rows[0].rss_kb, 2048);
        assert_eq!(rows[0].command, "/Applications/Cairn App");
    }

    #[test]
    fn selects_descendant_process_tree() {
        let rows = vec![
            row(10, 1, "root"),
            row(11, 10, "child"),
            row(12, 11, "grandchild"),
            row(13, 1, "other"),
        ];
        let selected = select_process_tree(&rows, 10);
        let pids = selected.iter().map(|row| row.pid).collect::<Vec<_>>();
        assert_eq!(pids, vec![10, 11, 12]);
    }

    #[test]
    fn shorten_command_keeps_basename() {
        assert_eq!(shorten_command("/usr/local/bin/claude"), "claude");
        assert_eq!(shorten_command("codex"), "codex");
    }

    fn row(pid: u32, ppid: u32, command: &str) -> ProcessRow {
        ProcessRow {
            pid,
            ppid,
            cpu_percent: 1.0,
            rss_kb: 100,
            command: command.into(),
        }
    }
}
