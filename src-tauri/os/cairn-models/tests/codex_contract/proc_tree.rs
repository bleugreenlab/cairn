//! The processes under one app-server, read from the kernel census: what a
//! case counts shims and code-mode hosts with, and measures footprint by.
//!
//! Two census passes: the first finds the tree from parent pids, the second
//! reads footprint for exactly those pids, because Linux reads it (a page
//! table walk) only for the pids a caller names.

use std::collections::{BTreeMap, BTreeSet};

use cairn_procmem::Census;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeProcess {
    pub pid: u32,
    pub parent_pid: u32,
    /// The executable's file name, as `ps` reports it.
    pub command: String,
    /// `ri_phys_footprint` on macOS, `Pss` on Linux.
    pub footprint_bytes: Option<u64>,
    pub resident_bytes: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct ProcessTree {
    pub root: u32,
    /// The root first, then its descendants by pid.
    pub processes: Vec<TreeProcess>,
}

/// What changed between two readings of one tree.
#[derive(Clone, Debug, Default)]
pub struct TreeDiff {
    pub appeared: Vec<TreeProcess>,
    pub departed: Vec<TreeProcess>,
}

fn census(footprint_for: &[u32]) -> Result<Census, String> {
    let mut census = Census::default();
    census.footprint_wanted = footprint_for.to_vec();
    census.footprint_wanted.sort_unstable();
    if !cairn_procmem::platform::census(&mut census) {
        return Err("this platform has no process census".to_string());
    }
    Ok(census)
}

/// The process's name from the kernel. Not `ps`: a sandboxed executor may
/// refuse to run it.
#[cfg(target_os = "linux")]
fn command_of(pid: u32) -> String {
    std::fs::read_to_string(format!("/proc/{pid}/comm"))
        .map(|name| name.trim().to_string())
        .unwrap_or_default()
}

#[cfg(target_os = "macos")]
fn command_of(pid: u32) -> String {
    let mut buffer = [0u8; 256];
    let written = unsafe {
        libc::proc_name(
            pid as libc::c_int,
            buffer.as_mut_ptr().cast(),
            buffer.len() as u32,
        )
    };
    match usize::try_from(written) {
        Ok(len) if len > 0 => {
            String::from_utf8_lossy(&buffer[..len.min(buffer.len())]).into_owned()
        }
        _ => String::new(),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn command_of(_pid: u32) -> String {
    String::new()
}

impl ProcessTree {
    /// `root` and everything descended from it now. Empty when `root` is gone.
    pub fn under(root: u32) -> Result<Self, String> {
        let first = census(&[])?;
        let mut children: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        for row in &first.rows {
            children
                .entry(row.process.parent_pid)
                .or_default()
                .push(row.process.pid);
        }
        let mut members = BTreeSet::new();
        if first.rows.iter().any(|row| row.process.pid == root) {
            let mut frontier = vec![root];
            while let Some(pid) = frontier.pop() {
                if members.insert(pid) {
                    frontier.extend(children.get(&pid).into_iter().flatten().copied());
                }
            }
        }
        let wanted: Vec<u32> = members.iter().copied().collect();
        let second = census(&wanted)?;
        let mut processes: Vec<TreeProcess> = second
            .rows
            .iter()
            .filter(|row| members.contains(&row.process.pid))
            .map(|row| TreeProcess {
                pid: row.process.pid,
                parent_pid: row.process.parent_pid,
                command: command_of(row.process.pid),
                footprint_bytes: row.physical_footprint_bytes,
                resident_bytes: row.resident_bytes,
            })
            .collect();
        processes.sort_by_key(|process| (process.pid != root, process.pid));
        Ok(Self { root, processes })
    }

    pub fn pids(&self) -> BTreeSet<u32> {
        self.processes.iter().map(|process| process.pid).collect()
    }

    /// Every process whose command contains `needle`.
    pub fn named(&self, needle: &str) -> Vec<&TreeProcess> {
        self.processes
            .iter()
            .filter(|process| process.command.contains(needle))
            .collect()
    }

    /// The tree's summed footprint, counting a process with no reading as 0.
    pub fn footprint_bytes(&self) -> u64 {
        self.processes
            .iter()
            .filter_map(|process| process.footprint_bytes)
            .sum()
    }

    pub fn diff(&self, later: &Self) -> TreeDiff {
        let before = self.pids();
        let after = later.pids();
        TreeDiff {
            appeared: later
                .processes
                .iter()
                .filter(|process| !before.contains(&process.pid))
                .cloned()
                .collect(),
            departed: self
                .processes
                .iter()
                .filter(|process| !after.contains(&process.pid))
                .cloned()
                .collect(),
        }
    }

    /// One line per process, for a failure report or a probe's record.
    pub fn render(&self) -> String {
        self.processes
            .iter()
            .map(|process| {
                format!(
                    "{} (parent {}) {} footprint {} resident {}",
                    process.pid,
                    process.parent_pid,
                    process.command,
                    mib(process.footprint_bytes),
                    mib(process.resident_bytes),
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

pub fn mib(bytes: Option<u64>) -> String {
    match bytes {
        Some(bytes) => format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0)),
        None => "unread".to_string(),
    }
}
