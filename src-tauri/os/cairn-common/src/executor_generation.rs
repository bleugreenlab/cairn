//! The process-wide fence one Cairn home's executor generation holds.
//!
//! Cell locks prevent two processes from touching the same checkout, but they
//! are too fine-grained to define a handoff, so every `cairn-exec` takes this one
//! lock at bootstrap and holds it for its whole life. The executor acquires it;
//! the runner that supervises a colocated executor only ever probes it, to learn
//! whether the generation it is about to start would have to wait and on whom.
//! Both sides name the same file through this module, which is the contract.

use std::path::{Path, PathBuf};

use crate::executor_protocol::ExecutorGenerationHolder;
use crate::kernel_lock::{read_lock_holder, try_lock_exclusive, unix_time_ms};

/// Where the generation lock lives in a Cairn home.
pub fn executor_generation_lock_path(cairn_home: &Path) -> PathBuf {
    executor_authority_dir(cairn_home).join("cairn-executor-generation.lock")
}

/// The directory holding the executor's home-wide authority files.
pub fn executor_authority_dir(cairn_home: &Path) -> PathBuf {
    cairn_home.join("build-slots").join(".authority")
}

/// Which live process holds this Cairn home's executor generation, without
/// taking it.
///
/// One non-blocking attempt, released the moment it succeeds: a status probe
/// that kept the lock would itself become the holder a starting executor waits
/// out. Nothing is stamped on success either, so the record a live executor
/// wrote stays its own.
///
/// The pid is what the holder wrote, not proof of identity. A caller that is
/// about to act on it must establish identity some other way.
pub fn executor_generation_holder(
    cairn_home: &Path,
) -> Result<Option<ExecutorGenerationHolder>, String> {
    let path = executor_generation_lock_path(cairn_home);
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
    {
        Ok(file) => file,
        // Nothing has ever held it.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "open executor generation authority {}: {error}",
                path.display()
            ))
        }
    };
    match try_lock_exclusive(&file) {
        // Free. Dropping the handle on return releases the attempt.
        Ok(true) => Ok(None),
        Ok(false) => {
            let record = read_lock_holder(&path);
            Ok(Some(ExecutorGenerationHolder {
                pid: record.map(|record| record.pid),
                recorded_ago_ms: record
                    .map(|record| unix_time_ms().saturating_sub(record.acquired_at_unix_ms)),
            }))
        }
        Err(error) => Err(format!("lock {}: {error}", path.display())),
    }
}

/// The file each build cell's authority directory keeps its persisted state in.
pub const CELL_STATE_FILE_NAME: &str = "cairn-build-slot-state.json";
/// The kernel lock an executor holds on each cell it manages.
pub const CELL_LOCK_FILE_NAME: &str = "cairn-build-slot.lock";

/// Every build cell authority directory in a Cairn home: the home-wide ones and
/// each project's.
pub fn cell_authority_dirs(cairn_home: &Path) -> Vec<PathBuf> {
    let root = cairn_home.join("build-slots");
    let mut roots = vec![root.join(".authority")];
    if let Ok(projects) = std::fs::read_dir(&root) {
        roots.extend(
            projects
                .flatten()
                .filter(|entry| entry.path().is_dir() && entry.file_name() != ".authority")
                .map(|entry| entry.path().join(".authority")),
        );
    }
    roots
        .into_iter()
        .filter_map(|root| std::fs::read_dir(root).ok())
        .flat_map(|entries| entries.flatten().map(|entry| entry.path()))
        .filter(|path| path.is_dir())
        .collect()
}

/// Cell authorities whose kernel lock some live process still holds. Probed
/// without keeping any: each attempt is released at once.
pub fn held_cell_authorities(cairn_home: &Path) -> Vec<PathBuf> {
    cell_authority_dirs(cairn_home)
        .into_iter()
        .filter(|authority| {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(authority.join(CELL_LOCK_FILE_NAME))
                .ok()
                .is_some_and(|file| matches!(try_lock_exclusive(&file), Ok(false)))
        })
        .collect()
}

/// What kind of work a recorded process is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadKind {
    /// A resident process: a terminal, service, dev instance, or workflow
    /// runtime.
    Resident,
    /// A member of a command batch, current or draining from a prior epoch.
    Command,
}

/// One process an executor recorded launching or observing in a cell, as the
/// kernel identifies it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedWorkloadProcess {
    pub pid: u32,
    pub start_token: String,
    /// The process group this process leads, when it leads one. A group is
    /// signalled only through a leader whose identity was just verified.
    pub leads_group: Option<u32>,
    pub kind: WorkloadKind,
    pub cell_authority: PathBuf,
}

/// Every live-recorded workload process in a Cairn home, read from the cell
/// state the executor persists.
///
/// Executors launch resident processes and command batches into process groups
/// (or, on Linux, sessions) of their own, so they are not in the executor's
/// group and do not die with it. This is how a party other than the executor
/// that launched them finds them: by the identities that executor recorded,
/// each of which still has to be checked against the kernel before anything
/// acts on it.
pub fn recorded_workload(cairn_home: &Path) -> Vec<RecordedWorkloadProcess> {
    use crate::executor_protocol::{
        CommandBatchOwnership, PersistentCellState, ResidentProcessStatus,
    };
    let mut workload = Vec::new();
    for authority in cell_authority_dirs(cairn_home) {
        let Some(state) = std::fs::read(authority.join(CELL_STATE_FILE_NAME))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<PersistentCellState>(&bytes).ok())
        else {
            continue;
        };
        let occupancy = &state.occupancy;
        for process in occupancy.processes.values() {
            if let ResidentProcessStatus::Running {
                ownership: Some(ownership),
                ..
            } = &process.status
            {
                workload.push(RecordedWorkloadProcess {
                    pid: ownership.pid,
                    start_token: ownership.start_token.clone(),
                    leads_group: (ownership.process_group_id == ownership.pid)
                        .then_some(ownership.process_group_id),
                    kind: WorkloadKind::Resident,
                    cell_authority: authority.clone(),
                });
            }
        }
        let batches: [Option<&CommandBatchOwnership>; 2] = [
            occupancy.command_ownership.as_ref(),
            occupancy
                .draining
                .as_ref()
                .map(|draining| &draining.ownership),
        ];
        for batch in batches.into_iter().flatten() {
            for identity in batch
                .items
                .iter()
                .flat_map(|item| &item.attempts)
                .flat_map(|attempt| &attempt.processes)
            {
                // A session leader also leads its process group.
                let leads_group = identity
                    .process_group_id
                    .or(identity.session_id)
                    .filter(|group| *group == identity.pid);
                workload.push(RecordedWorkloadProcess {
                    pid: identity.pid,
                    start_token: identity.start_token.clone(),
                    leads_group,
                    kind: WorkloadKind::Command,
                    cell_authority: authority.clone(),
                });
            }
        }
    }
    workload
}
