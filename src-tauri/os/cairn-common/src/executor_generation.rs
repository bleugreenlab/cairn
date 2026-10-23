//! The process-wide fence one Cairn home's executor generation holds.
//!
//! Cell locks prevent two processes from touching the same checkout, but they
//! are too fine-grained to define a handoff, so every `cairn-exec` takes this one
//! lock at bootstrap and holds it for its whole life. The executor acquires it;
//! the runner that supervises a colocated executor only ever probes it, to learn
//! whether the generation it is about to start would have to wait and on whom.
//! Both sides name the same file through this module, which is the contract.

use std::path::{Path, PathBuf};

use crate::executor_paths::{machine_authority_dir, ExecutorPaths};
use crate::executor_protocol::{ExecutorGenerationHolder, PersistentCellState};
use crate::kernel_lock::{read_lock_holder, try_lock_exclusive, unix_time_ms};

/// Where the generation lock lives in a Cairn home.
pub fn executor_generation_lock_path(cairn_home: &Path) -> PathBuf {
    machine_authority_dir(cairn_home).join("cairn-executor-generation.lock")
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

/// Every build cell authority directory an executor's layout holds: the
/// machine-wide ones and each project's.
pub fn cell_authority_dirs(paths: &ExecutorPaths) -> Vec<PathBuf> {
    paths
        .cell_authority_roots()
        .into_iter()
        .filter_map(|root| std::fs::read_dir(root).ok())
        .flat_map(|entries| entries.flatten().map(|entry| entry.path()))
        .filter(|path| path.is_dir())
        .collect()
}

/// Cell authorities whose kernel lock some live process still holds. Probed
/// without keeping any: each attempt is released at once.
pub fn held_cell_authorities(paths: &ExecutorPaths) -> Vec<PathBuf> {
    cell_authority_dirs(paths)
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

/// Decode a cell's persisted state exactly as the executor that wrote it does.
///
/// Disk-only migration: lifecycle names an older executor wrote are mapped
/// onto the current ones before the strict decode, while wire decoding stays
/// strict at the protocol boundary. The executor and the runner share this one
/// decoder, so a cell the executor would adopt is never one the runner reads
/// as empty.
pub fn decode_persistent_cell_state(bytes: &[u8]) -> Result<PersistentCellState, String> {
    let mut value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|error| format!("unparseable persistent slot state: {error}"))?;
    let lifecycle = value
        .get("lifecycle")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "persistent slot lifecycle is missing or not a string".to_string())?;
    let migrated = match lifecycle {
        "provisioning" | "recovering" | "materializing" => "materializing",
        "idle" | "queued" | "awaitingReclaim" | "releasing" => "idle",
        "running" | "active" => "active",
        "retired" | "deleting" => "deleting",
        "quarantined" => "quarantined",
        other => return Err(format!("unknown persistent slot lifecycle: {other:?}")),
    };
    value["lifecycle"] = serde_json::Value::String(migrated.into());
    serde_json::from_value(value).map_err(|error| format!("invalid persistent slot state: {error}"))
}

/// What one cell authority's state file says right now.
#[derive(Debug)]
pub enum CellStateRead {
    /// No state file: nothing has been recorded in this cell.
    Absent,
    /// A state file exists and could not be read or decoded. Its workload is
    /// unknown, which is never the same as none.
    Unreadable(String),
    Recorded(Box<PersistentCellState>),
}

pub fn read_cell_state(cell_authority: &Path) -> CellStateRead {
    match std::fs::read(cell_authority.join(CELL_STATE_FILE_NAME)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => CellStateRead::Absent,
        Err(error) => CellStateRead::Unreadable(error.to_string()),
        Ok(bytes) => match decode_persistent_cell_state(&bytes) {
            Ok(state) => CellStateRead::Recorded(Box::new(state)),
            Err(error) => CellStateRead::Unreadable(error),
        },
    }
}

/// What kind of work a recorded process is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkloadKind {
    /// A resident process: a terminal, service, dev instance, or workflow
    /// runtime.
    Resident,
    /// A member of the cell's current command batch.
    Command,
    /// A member of a prior epoch's batch the cell has not yet proven dead.
    Draining,
}

impl WorkloadKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Resident => "resident",
            Self::Command => "command",
            Self::Draining => "draining command",
        }
    }
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
    /// The cell's epoch when this was read. A later generation that adopts the
    /// cell advances it, which is how its own work is told apart from what the
    /// previous generation left behind.
    pub cell_epoch: u64,
    /// Members of the group it leads, seen while it was verified alive. They
    /// are what lets that group be signalled once the leader is gone. The
    /// reader leaves this empty; it is evidence only whoever observed the live
    /// group can supply.
    pub group_anchors: Vec<crate::process_identity::ProcessAnchor>,
}

/// Every process `state` records as live in the cell at `cell_authority`.
///
/// Only identities carrying a kernel start token are returned: a resident
/// recorded before ownership was persisted names nothing that can be verified,
/// and the executor itself never signals one.
pub fn cell_workload(
    cell_authority: &Path,
    state: &PersistentCellState,
) -> Vec<RecordedWorkloadProcess> {
    use crate::executor_protocol::ResidentProcessStatus;
    let process =
        |pid: u32, start_token: &str, leads_group: Option<u32>, kind| RecordedWorkloadProcess {
            pid,
            start_token: start_token.to_owned(),
            leads_group,
            kind,
            cell_authority: cell_authority.to_path_buf(),
            cell_epoch: state.cell_epoch,
            group_anchors: Vec::new(),
        };
    let occupancy = &state.occupancy;
    let mut workload = Vec::new();
    for resident in occupancy.processes.values() {
        if let ResidentProcessStatus::Running {
            ownership: Some(ownership),
            ..
        } = &resident.status
        {
            workload.push(process(
                ownership.pid,
                &ownership.start_token,
                (ownership.process_group_id == ownership.pid).then_some(ownership.pid),
                WorkloadKind::Resident,
            ));
        }
    }
    let batches = [
        (occupancy.command_ownership.as_ref(), WorkloadKind::Command),
        (
            occupancy
                .draining
                .as_ref()
                .map(|draining| &draining.ownership),
            WorkloadKind::Draining,
        ),
    ];
    for (batch, kind) in batches {
        for identity in batch
            .into_iter()
            .flat_map(|batch| &batch.items)
            .flat_map(|item| &item.attempts)
            .flat_map(|attempt| &attempt.processes)
            // Tree-scan evidence of a prior epoch names a process nothing
            // proves Cairn launched, so it is never anyone's to kill.
            .filter(|identity| !identity.is_prior_epoch_survivor_evidence())
        {
            // A session leader also leads its process group.
            let leads_group = identity
                .process_group_id
                .or(identity.session_id)
                .filter(|group| *group == identity.pid);
            workload.push(process(
                identity.pid,
                &identity.start_token,
                leads_group,
                kind,
            ));
        }
    }
    workload
}

/// The workload recorded across an executor's cells, and the cells whose record could
/// not be read.
#[derive(Debug, Default)]
pub struct RecordedWorkload {
    pub processes: Vec<RecordedWorkloadProcess>,
    /// Cell authorities whose state file exists and could not be decoded, with
    /// why. What runs in them is unknown.
    pub unreadable: Vec<(PathBuf, String)>,
}

/// Every live-recorded workload process in an executor's cells, read from the cell
/// state the executor persists.
///
/// Executors launch resident processes and command batches into process groups
/// (or, on Linux, sessions) of their own, so they are not in the executor's
/// group and do not die with it. This is how a party other than the executor
/// that launched them finds them: by the identities that executor recorded,
/// each of which still has to be checked against the kernel before anything
/// acts on it.
pub fn recorded_workload(paths: &ExecutorPaths) -> RecordedWorkload {
    let mut workload = RecordedWorkload::default();
    for authority in cell_authority_dirs(paths) {
        match read_cell_state(&authority) {
            CellStateRead::Absent => {}
            CellStateRead::Unreadable(error) => workload.unreadable.push((authority, error)),
            CellStateRead::Recorded(state) => {
                workload.processes.extend(cell_workload(&authority, &state))
            }
        }
    }
    workload
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cell state file as the executor writes one: a terminal resident with
    /// its launch ownership, a resident recorded before ownership existed, the
    /// current batch's session leader and a descendant, and a draining member
    /// of a prior epoch.
    fn fixture(lifecycle: &str) -> serde_json::Value {
        let identity = |pid: u32, session: Option<u32>, token: &str| {
            let mut identity = serde_json::json!({
                "pid": pid,
                "startToken": token,
                "launchNonce": format!("nonce-{pid}"),
                "observedAtUnixMs": 5,
                "parent": {"kind": "leader"},
            });
            if let Some(session) = session {
                identity["processGroupId"] = session.into();
                identity["sessionId"] = session.into();
            }
            identity
        };
        let batch = |members: Vec<serde_json::Value>| {
            serde_json::json!({
                "executorId": "exec",
                "cellId": "slot-3",
                "requestId": "request",
                "attemptId": "attempt",
                "batchLaunchNonce": "batch",
                "launchedAtUnixMs": 4,
                "items": [{
                    "itemIndex": 0,
                    "streamId": "stream",
                    "attempts": [{
                        "attemptIndex": 0,
                        "state": "running",
                        "launchNonce": "attempt-nonce",
                        "launchedAtUnixMs": 4,
                        "processes": members,
                    }],
                }],
            })
        };
        serde_json::json!({
            "executorId": "exec",
            "projectId": "project",
            "slotId": "slot-3",
            "path": "/cells/slot-3",
            "repository": "/repo",
            "lifecycle": lifecycle,
            "cellEpoch": 9,
            "lastSealedCommit": null,
            "lastUsedUnixMs": 1,
            "occupancy": {
                "commandOwnership": batch(vec![
                    identity(500, Some(500), "500:1"),
                    identity(501, None, "501:1"),
                ]),
                "draining": {
                    "cellEpoch": 8,
                    "releasedAtUnixMs": 3,
                    "ownership": batch(vec![identity(600, Some(600), "600:1")]),
                },
                "processes": {
                    "shell": {
                        "generation": 1,
                        "kind": {"kind": "terminal", "slug": "dev"},
                        "status": {
                            "status": "running",
                            "startedAtUnixMs": 2,
                            "processGroupId": 400,
                            "ownership": {
                                "pid": 400,
                                "processGroupId": 400,
                                "launchNonce": "terminal",
                                "startToken": "400:1",
                                "launchedAtUnixMs": 2,
                            },
                        },
                    },
                    "legacy": {
                        "generation": 1,
                        "kind": {"kind": "terminal", "slug": "old"},
                        "status": {
                            "status": "running",
                            "startedAtUnixMs": 2,
                            "processGroupId": 401,
                        },
                    },
                },
            },
        })
    }

    fn home_with(state: &serde_json::Value) -> (tempfile::TempDir, PathBuf) {
        let home = tempfile::tempdir().unwrap();
        let authority = home
            .path()
            .join("build-slots")
            .join("project")
            .join(".authority")
            .join("slot-3");
        std::fs::create_dir_all(&authority).unwrap();
        std::fs::write(
            authority.join(CELL_STATE_FILE_NAME),
            serde_json::to_vec(state).unwrap(),
        )
        .unwrap();
        (home, authority)
    }

    #[test]
    fn the_recorded_workload_is_every_verifiable_identity_in_the_cell() {
        let (home, authority) = home_with(&fixture("active"));
        let workload = recorded_workload(&ExecutorPaths::new(home.path()));
        assert!(workload.unreadable.is_empty(), "{:?}", workload.unreadable);
        let mut found: Vec<_> = workload
            .processes
            .iter()
            .map(|process| (process.pid, process.leads_group, process.kind))
            .collect();
        found.sort_by_key(|(pid, ..)| *pid);
        assert_eq!(
            found,
            vec![
                (400, Some(400), WorkloadKind::Resident),
                (500, Some(500), WorkloadKind::Command),
                // Recorded as a descendant, so it leads nothing and only its own
                // pid is ever signalled.
                (501, None, WorkloadKind::Command),
                (600, Some(600), WorkloadKind::Draining),
            ],
            "the resident with no recorded ownership names nothing verifiable"
        );
        assert!(workload
            .processes
            .iter()
            .all(|process| process.cell_authority == authority && process.cell_epoch == 9));
    }

    /// An older executor wrote lifecycle names the strict decode refuses. The
    /// runner must read that cell the way the executor's adoption does, or its
    /// workload is silently skipped.
    #[test]
    fn a_cell_an_older_executor_wrote_is_still_read() {
        let (home, _) = home_with(&fixture("running"));
        let workload = recorded_workload(&ExecutorPaths::new(home.path()));
        assert!(workload.unreadable.is_empty(), "{:?}", workload.unreadable);
        assert_eq!(workload.processes.len(), 4);
    }

    /// A survivor the executor found by scanning a cell's tree is recorded in
    /// its drain as evidence. Retiring the executor must not turn that record
    /// into a kill list: nothing proves Cairn started the process.
    #[test]
    fn prior_epoch_survivor_evidence_is_not_workload() {
        let mut state = fixture("quarantined");
        state["occupancy"]["draining"]["ownership"]["items"][0]["attempts"][0]["processes"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "pid": 700,
                "processGroupId": 700,
                "startToken": "700:1",
                "launchNonce": crate::executor_protocol::PRIOR_EPOCH_SURVIVOR_NONCE,
                "observedAtUnixMs": 5,
                "parent": {"kind": "leader"},
            }));
        let (home, _) = home_with(&state);
        let workload = recorded_workload(&ExecutorPaths::new(home.path()));
        assert!(workload.unreadable.is_empty(), "{:?}", workload.unreadable);
        let pids: Vec<_> = workload
            .processes
            .iter()
            .map(|process| process.pid)
            .collect();
        assert!(
            pids.contains(&600),
            "a drain identity recorded at launch is still workload: {pids:?}"
        );
        assert!(
            !pids.contains(&700),
            "tree-scan evidence must never be signalled: {pids:?}"
        );
    }

    #[test]
    fn an_undecodable_cell_is_reported_unknown_not_empty() {
        let (home, authority) = home_with(&fixture("active"));
        std::fs::write(authority.join(CELL_STATE_FILE_NAME), b"{\"lifecycle\":").unwrap();
        let workload = recorded_workload(&ExecutorPaths::new(home.path()));
        assert!(workload.processes.is_empty());
        assert_eq!(workload.unreadable.len(), 1);
        assert_eq!(workload.unreadable[0].0, authority);
    }
}
