//! Shared PTY and persisted terminal host logic for Cairn hosts.

use crate::db::DbState;
use crate::fleet::SnapshotReceipt;
use crate::mcp::handlers::slug::slugify;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_common::executor_protocol::{
    PersistentCellState, ResidencyFailureKind, ResidencyFence, ResidencyHolder, ResidencyOperation,
    ResidencyResult, ResidentProcessStatus,
};
use cairn_db::turso::params;
use cairn_session::terminals::clear_unadvertised_exited_terminal_lease_bindings;
pub use cairn_session::terminals::{
    mark_terminal_recovering, resolve_missing_terminal_lease, TerminalRecovery,
};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PtyCommandSnapshot {
    busy: bool,
    exit_code: Option<i32>,
    duration_ms: Option<u64>,
}

fn emit_terminal_change(orch: &Orchestrator, action: &str) {
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_terminals", "action": action}),
    );
}

/// Durable running rows are attachable only while their session exists in this
/// host. A new host starts with an empty PtyState, so settle runner-local orphans
/// as exited and fence executor-backed orphans into recovery before any terminal
/// query can advertise them.
pub async fn prepare_terminal_recovery(orch: &Orchestrator) -> Result<u64, String> {
    let sessions: HashSet<String> = orch
        .pty_state
        .sessions
        .lock()
        .map_err(|error| error.to_string())?
        .keys()
        .cloned()
        .collect();
    let updated = orch.db.local.write(|conn| {
        let sessions = sessions.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, session_id, residency_holder FROM job_terminals
                     WHERE status IN ('running', 'recovering')",
                    (),
                )
                .await?;
            let mut local_orphans = Vec::new();
            let mut resident_orphans = Vec::new();
            while let Some(row) = rows.next().await? {
                let id = row.text(0)?;
                if !sessions.contains(&row.text(1)?) {
                    if row.opt_text(2)?.is_some() {
                        resident_orphans.push(id);
                    } else {
                        local_orphans.push(id);
                    }
                }
            }
            drop(rows);
            let mut updated = 0;
            let now = chrono::Utc::now().timestamp();
            for id in local_orphans {
                updated += conn.execute(
                    "UPDATE job_terminals
                     SET status = 'exited', exited_at = COALESCE(exited_at, ?2)
                     WHERE id = ?1 AND status IN ('running', 'recovering')",
                    params![id.as_str(), now],
                ).await?;
            }
            for id in resident_orphans {
                updated += conn.execute(
                    "UPDATE job_terminals SET status = 'recovering' WHERE id = ?1 AND status = 'running'",
                    (id.as_str(),),
                ).await?;
            }
            Ok(updated)
        })
    }).await.map_err(|error| error.to_string())?;
    if updated > 0 {
        emit_terminal_change(orch, "update");
    }
    crate::mcp::handlers::terminal::reconcile_exited_terminal_wakes(orch).await?;
    Ok(updated)
}

#[derive(Clone)]
struct RecoverableTerminal {
    id: String,
    session_id: String,
    status: String,
    residency_holder: String,
    incarnation_id: String,
    cell_epoch: u64,
    transition_started_at: Option<i64>,
}

async fn recoverable_terminals(db: &LocalDb) -> DbResult<Vec<RecoverableTerminal>> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, session_id, status, residency_holder, residency_incarnation_id, cell_epoch, exited_at
             FROM job_terminals
             WHERE status IN ('recovering', 'closing') AND residency_holder IS NOT NULL",
                    (),
                )
                .await?;
            let mut terminals = Vec::new();
            while let Some(row) = rows.next().await? {
                terminals.push(RecoverableTerminal {
                    id: row.text(0)?,
                    session_id: row.text(1)?,
                    status: row.text(2)?,
                    residency_holder: row.text(3)?,
                    incarnation_id: row.text(4)?,
                    cell_epoch: row.i64(5)? as u64,
                    transition_started_at: row.opt_i64(6)?,
                });
            }
            Ok(terminals)
        })
    })
    .await
}

async fn delete_closing_terminal(db: &LocalDb, terminal: &RecoverableTerminal) -> DbResult<bool> {
    let terminal = terminal.clone();
    db.write(|conn| {
        let terminal = terminal.clone();
        Box::pin(async move {
            Ok(conn
                .execute(
                    "DELETE FROM job_terminals
             WHERE id = ?1 AND status = 'closing' AND residency_holder = ?2
               AND residency_incarnation_id = ?3 AND cell_epoch = ?4",
                    params![
                        terminal.id,
                        terminal.residency_holder,
                        terminal.incarnation_id,
                        terminal.cell_epoch as i64
                    ],
                )
                .await?
                > 0)
        })
    })
    .await
}

/// Reconcile durable recovery/close intents against the fleet's complete
/// advertised fence. Missing owners remain fenced until the full recovery grace.
async fn reconcile_terminal_lifecycle(orch: &Orchestrator) -> Result<u64, String> {
    let rows = recoverable_terminals(&orch.db.local)
        .await
        .map_err(|error| error.to_string())?;
    let snapshot = orch.fleet.snapshot();
    let mut changed = 0;
    for row in rows {
        let fence = snapshot.cells.iter().find_map(|cell| {
            let residency = cell.residency.as_ref()?;
            (residency.holder.storage_key() == row.residency_holder
                && residency.incarnation_id == row.incarnation_id
                && cell.cell_epoch == row.cell_epoch)
                .then(|| ResidencyFence {
                    holder: residency.holder.clone(),
                    incarnation_id: row.incarnation_id.clone(),
                    cell_epoch: row.cell_epoch,
                })
        });
        if row.status == "recovering" {
            if let Some(fence) = fence {
                match crate::mcp::handlers::terminal::restart_residency_terminals(orch, fence).await
                {
                    Ok(_) => changed += 1,
                    Err(error) => {
                        tracing::warn!(terminal_id = %row.id, %error, "terminal recovery retry failed")
                    }
                }
            } else if terminal_owner_recovery_window_elapsed()
                && resolve_missing_terminal_lease(
                    &orch.db.local,
                    &row.residency_holder,
                    &row.incarnation_id,
                    row.cell_epoch,
                )
                .await
                .map_err(|error| error.to_string())?
            {
                changed += 1;
            }
        } else if let Some(_fence) = fence {
            match crate::mcp::handlers::terminal::release_terminal_by_session(orch, &row.session_id)
                .await
            {
                Ok(()) => {
                    if delete_closing_terminal(&orch.db.local, &row)
                        .await
                        .map_err(|e| e.to_string())?
                    {
                        changed += 1;
                    }
                }
                Err(error) => {
                    tracing::warn!(terminal_id = %row.id, %error, "terminal close retry failed")
                }
            }
        } else if row.transition_started_at.is_some_and(|started| {
            chrono::Utc::now().timestamp().saturating_sub(started)
                >= terminal_owner_recovery_duration().as_secs() as i64
        }) && delete_closing_terminal(&orch.db.local, &row)
            .await
            .map_err(|e| e.to_string())?
        {
            changed += 1;
        }
    }
    if changed > 0 {
        emit_terminal_change(orch, "update");
        crate::mcp::handlers::terminal::reconcile_exited_terminal_wakes(orch).await?;
    }
    Ok(changed)
}

pub fn schedule_terminal_lifecycle_recovery(orch: Orchestrator) {
    TERMINAL_OWNER_RECOVERY_STARTED.get_or_init(Instant::now);
    tokio::spawn(async move {
        let mut liveness = ProcessLossWatch::default();
        loop {
            if let Err(error) = reconcile_terminal_lifecycle(&orch).await {
                tracing::warn!(%error, "terminal lifecycle reconciliation failed");
            }
            if let Err(error) = reconcile_running_terminal_liveness(&orch, &mut liveness).await {
                tracing::warn!(%error, "terminal liveness reconciliation failed");
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

/// Why Cairn knows a running terminal's process no longer exists even though no
/// exit for it ever arrived.
///
/// Every variant is something an executor stated about the environment the
/// terminal was bound to. None is inferred from silence: a process that has
/// printed nothing for an hour is still `Running` in its executor's state and
/// is never one of these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProcessLoss {
    /// The environment's cell is at a newer epoch under the same incarnation.
    /// An epoch advances only when an executor re-adopts the cell after it
    /// restarted or leases it afresh, and no process survives either.
    EpochAdvanced {
        executor: String,
        from: u64,
        to: u64,
    },
    /// The owner's environment exists under another incarnation: the one this
    /// process ran in was released and replaced.
    EnvironmentReplaced { executor: String },
    /// The cell holds a newer generation of the process at this key.
    GenerationSuperseded {
        executor: String,
        from: u64,
        to: u64,
    },
    /// The cell no longer lists the process at all.
    ProcessAbsent { executor: String },
    /// The executor says the process is no longer running, but that exit never
    /// reached the terminal.
    ExitUndelivered {
        executor: String,
        exit_code: Option<i32>,
    },
    /// The environment is held nowhere: the executor that held it answered
    /// that it no longer does (`Some`), or no connected executor advertises it
    /// and no route records one as holding it (`None`).
    LeaseGone { executor: Option<String> },
}

impl ProcessLoss {
    /// The typed reason recorded on the terminal and carried by its exit wake,
    /// a `waitFor` on its exit, and a read of it.
    pub(crate) fn reason(&self) -> String {
        match self {
            Self::EpochAdvanced { executor, from, to } => format!(
                "process lost: executor {executor} restarted or re-leased this terminal's \
                 environment (cell epoch {from} became {to}), and no process survives that"
            ),
            Self::EnvironmentReplaced { executor } => format!(
                "process lost: this terminal's environment on executor {executor} was \
                 released and replaced"
            ),
            Self::GenerationSuperseded { executor, from, to } => format!(
                "process lost: executor {executor} replaced this terminal's process \
                 (generation {from} became {to})"
            ),
            Self::ProcessAbsent { executor } => {
                format!(
                    "process lost: executor {executor} no longer reports this terminal's process"
                )
            }
            Self::ExitUndelivered {
                executor,
                exit_code,
            } => {
                let code = exit_code
                    .map(|code| format!(" with exit code {code}"))
                    .unwrap_or_default();
                format!(
                    "process lost: executor {executor} reports this terminal's process ended{code}, \
                     but its exit never reached Cairn"
                )
            }
            Self::LeaseGone {
                executor: Some(executor),
            } => format!(
                "process lost: executor {executor} no longer holds this terminal's environment"
            ),
            Self::LeaseGone { executor: None } => {
                "process lost: no executor holds this terminal's environment any more".into()
            }
        }
    }

    /// The exit status to record. Only an executor that reported the exit
    /// itself knows one; every other loss has no status to give.
    pub(crate) fn exit_code(&self) -> Option<i32> {
        match self {
            Self::ExitUndelivered { exit_code, .. } => *exit_code,
            _ => None,
        }
    }

    fn executor(&self) -> Option<&str> {
        match self {
            Self::EpochAdvanced { executor, .. }
            | Self::EnvironmentReplaced { executor }
            | Self::GenerationSuperseded { executor, .. }
            | Self::ProcessAbsent { executor }
            | Self::ExitUndelivered { executor, .. } => Some(executor),
            Self::LeaseGone { executor } => executor.as_deref(),
        }
    }
}

/// What a set of advertised cells says about one running terminal's process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TerminalLiveness {
    /// The executor holding the terminal's environment reports it running.
    Alive,
    /// Nothing here can answer, or the answer belongs to another path: the
    /// executor is away, the advertisement predates the process, or recovery
    /// owns the death. The terminal stays running.
    Unknown,
    /// The advertisement contradicts the terminal being alive.
    Lost(ProcessLoss),
}

/// A running terminal's complete binding to the executor process behind it.
#[derive(Debug, Clone)]
pub(crate) struct ResidentTerminalBinding {
    pub(crate) id: String,
    /// Also the executor's process key for this terminal.
    pub(crate) session_id: String,
    pub(crate) holder: String,
    pub(crate) incarnation_id: String,
    pub(crate) cell_epoch: u64,
    pub(crate) process_generation: u64,
}

fn cell_executor_name(cell: &PersistentCellState) -> String {
    cell.executor_display_name
        .clone()
        .unwrap_or_else(|| cell.executor_id.clone())
}

/// Judge one running terminal against a set of advertised cells.
///
/// The cells may be the fleet's cached snapshot, which can predate the
/// terminal's own start, so a `Lost` verdict from them is a suspicion that
/// [`reconcile_running_terminal_liveness`] confirms against fresh evidence
/// before acting. Judged against the cell a fenced probe just returned, the
/// same verdict is that evidence.
///
/// `holder_routed` says whether the fleet still records some executor as
/// holding the terminal's environment. A route outlives its executor's link, so
/// a recorded route with no advertised cell is a machine that is away and may
/// return with the process intact.
pub(crate) fn judge_terminal_process(
    terminal: &ResidentTerminalBinding,
    cells: &[PersistentCellState],
    holder_routed: bool,
) -> TerminalLiveness {
    let held: Vec<(&PersistentCellState, &str)> = cells
        .iter()
        .filter_map(|cell| {
            let residency = cell.residency.as_ref()?;
            (residency.holder.storage_key() == terminal.holder)
                .then_some((cell, residency.incarnation_id.as_str()))
        })
        .collect();

    if let Some((cell, _)) = held.iter().find(|(cell, incarnation)| {
        *incarnation == terminal.incarnation_id && cell.cell_epoch == terminal.cell_epoch
    }) {
        let executor = cell_executor_name(cell);
        let Some(process) = cell.occupancy.processes.get(&terminal.session_id) else {
            return TerminalLiveness::Lost(ProcessLoss::ProcessAbsent { executor });
        };
        if process.generation > terminal.process_generation {
            return TerminalLiveness::Lost(ProcessLoss::GenerationSuperseded {
                executor,
                from: terminal.process_generation,
                to: process.generation,
            });
        }
        if process.generation < terminal.process_generation {
            // An advertisement older than the start that bound this generation.
            return TerminalLiveness::Unknown;
        }
        return match &process.status {
            ResidentProcessStatus::Starting | ResidentProcessStatus::Running { .. } => {
                TerminalLiveness::Alive
            }
            // A death the executor attributes to losing itself, inside the
            // epoch the terminal is fenced to, reaches the terminal's own
            // handler, which hands it to recovery to be started again.
            // Settling it here would take that restart away.
            ResidentProcessStatus::Exited {
                executor_lost: true,
                ..
            } => TerminalLiveness::Unknown,
            ResidentProcessStatus::Exited { exit_code, .. } => {
                TerminalLiveness::Lost(ProcessLoss::ExitUndelivered {
                    executor,
                    exit_code: *exit_code,
                })
            }
            ResidentProcessStatus::Stopped => {
                TerminalLiveness::Lost(ProcessLoss::ExitUndelivered {
                    executor,
                    exit_code: None,
                })
            }
        };
    }

    if let Some((cell, _)) = held.iter().find(|(cell, incarnation)| {
        *incarnation == terminal.incarnation_id && cell.cell_epoch > terminal.cell_epoch
    }) {
        return TerminalLiveness::Lost(ProcessLoss::EpochAdvanced {
            executor: cell_executor_name(cell),
            from: terminal.cell_epoch,
            to: cell.cell_epoch,
        });
    }

    if let Some((cell, _)) = held
        .iter()
        .find(|(_, incarnation)| *incarnation != terminal.incarnation_id)
    {
        return TerminalLiveness::Lost(ProcessLoss::EnvironmentReplaced {
            executor: cell_executor_name(cell),
        });
    }

    if held.is_empty() && !holder_routed {
        return TerminalLiveness::Lost(ProcessLoss::LeaseGone { executor: None });
    }
    TerminalLiveness::Unknown
}

/// Confirm a suspected loss from the executor's answer to a fenced renewal sent
/// after the suspicion arose.
///
/// The answer is authoritative and causally fresh: the executor reads its own
/// current state under the fence, so it reflects every start it has already
/// acknowledged. A renewal is what the terminal's heartbeat sends anyway, and
/// it changes nothing about a residency that is not active, so asking cannot
/// revive an environment awaiting reclaim.
pub(crate) fn confirm_suspected_loss(
    terminal: &ResidentTerminalBinding,
    suspected: &ProcessLoss,
    answer: &ResidencyResult,
) -> Option<ProcessLoss> {
    match answer {
        // The fence still matches: judge the process in the cell as it is now.
        ResidencyResult::State { cell } => {
            // An executor's own answer does not name the machine; the fleet
            // stamps that on advertisements, like the one behind the suspicion.
            let mut cell = cell.clone();
            if cell.executor_display_name.is_none() && cell.executor_id.is_empty() {
                cell.executor_display_name = suspected.executor().map(ToOwned::to_owned);
            }
            match judge_terminal_process(terminal, std::slice::from_ref(&cell), true) {
                TerminalLiveness::Lost(loss) => Some(loss),
                TerminalLiveness::Alive | TerminalLiveness::Unknown => None,
            }
        }
        // The executor holds this environment under a newer epoch or another
        // incarnation, so nothing from the terminal's own can still run.
        ResidencyResult::Failed {
            kind: ResidencyFailureKind::StaleEpoch,
            ..
        } => Some(match suspected {
            ProcessLoss::EpochAdvanced { .. } | ProcessLoss::EnvironmentReplaced { .. } => {
                suspected.clone()
            }
            other => ProcessLoss::EnvironmentReplaced {
                executor: other.executor().unwrap_or("unknown").to_string(),
            },
        }),
        // The executor the fleet routed to holds no such environment at all.
        ResidencyResult::Failed {
            kind: ResidencyFailureKind::NotFound,
            ..
        } => Some(ProcessLoss::LeaseGone {
            executor: suspected.executor().map(ToOwned::to_owned),
        }),
        // Unreachable, not active, refused for admission: none of these says
        // anything about whether the process runs.
        _ => None,
    }
}

/// The fresh evidence a lease that nothing advertises must wait for.
///
/// A loss with an advertised cell is confirmed by probing that cell's executor.
/// A lease no connected executor advertises and no route records cannot be
/// probed, because there is nowhere to send the probe, and the cached snapshot
/// that shows its absence may predate the terminal's start. So the absence must
/// survive [`Self::FRESH_SNAPSHOTS`] further snapshots from every connected
/// executor. An executor keeps at most one outbound snapshot, superseded rather
/// than queued and taken by its single writer only when sent, so the first
/// snapshot to arrive after the suspicion may be one that was already waiting,
/// but the second was built after the first had been sent, and therefore after
/// the start response that preceded the suspicion.
#[derive(Debug, Default)]
pub(crate) struct ProcessLossWatch {
    unroutable_since: HashMap<String, HashMap<String, SnapshotReceipt>>,
}

impl ProcessLossWatch {
    const FRESH_SNAPSHOTS: u64 = 2;

    /// Record that `terminal_id`'s lease looks gone with nowhere to probe, and
    /// answer whether every connected executor has since delivered enough
    /// fresh snapshots for that absence to be true.
    pub(crate) fn unroutable_loss_confirmed(
        &mut self,
        terminal_id: &str,
        receipts: &HashMap<String, SnapshotReceipt>,
    ) -> bool {
        let Some(baseline) = self.unroutable_since.get(terminal_id) else {
            self.unroutable_since
                .insert(terminal_id.to_string(), receipts.clone());
            return false;
        };
        !receipts.is_empty()
            && receipts.iter().all(|(executor, now)| {
                let delivered = match baseline.get(executor) {
                    Some(then) if then.generation == now.generation => {
                        now.received.saturating_sub(then.received)
                    }
                    // A link opened since the suspicion: everything it
                    // delivered postdates it.
                    _ => now.received,
                };
                delivered >= Self::FRESH_SNAPSHOTS
            })
    }

    pub(crate) fn clear(&mut self, terminal_id: &str) {
        self.unroutable_since.remove(terminal_id);
    }

    fn retain_running(&mut self, running: &HashSet<&str>) {
        self.unroutable_since
            .retain(|id, _| running.contains(id.as_str()));
    }
}

async fn running_resident_terminals(db: &LocalDb) -> DbResult<Vec<ResidentTerminalBinding>> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, session_id, residency_holder, residency_incarnation_id, cell_epoch,
                            process_generation
                     FROM job_terminals
                     WHERE status = 'running' AND residency_holder IS NOT NULL
                       AND residency_incarnation_id IS NOT NULL AND cell_epoch IS NOT NULL
                       AND process_generation IS NOT NULL",
                    (),
                )
                .await?;
            let mut terminals = Vec::new();
            while let Some(row) = rows.next().await? {
                terminals.push(ResidentTerminalBinding {
                    id: row.text(0)?,
                    session_id: row.text(1)?,
                    holder: row.text(2)?,
                    incarnation_id: row.text(3)?,
                    cell_epoch: row.i64(4)? as u64,
                    process_generation: row.i64(5)? as u64,
                });
            }
            Ok(terminals)
        })
    })
    .await
}

/// Settle every running executor-backed terminal whose process its executor has
/// shown to be gone.
///
/// A terminal's exit normally arrives as a process event fenced to the exact
/// cell epoch the terminal started in. Some deaths never produce such an event:
/// an executor that restarts reports its lost processes only under the cell's
/// new epoch, and an environment released or replaced underneath a terminal
/// reports nothing for it at all. Without this pass those rows stayed `running`
/// indefinitely, and a `waitFor` on their exit never resolved.
///
/// The fleet's cached snapshot only raises a suspicion, because it can predate
/// the terminal's start. A suspicion is acted on only once fresh evidence
/// confirms it: a fenced probe of the executor that advertises the environment,
/// or, where nothing advertises it, absence that persists through snapshots
/// provably built after the suspicion ([`ProcessLossWatch`]).
///
/// Returns how many terminals this pass settled.
pub(crate) async fn reconcile_running_terminal_liveness(
    orch: &Orchestrator,
    watch: &mut ProcessLossWatch,
) -> Result<u64, String> {
    let terminals = running_resident_terminals(&orch.db.local)
        .await
        .map_err(|error| error.to_string())?;
    watch.retain_running(&terminals.iter().map(|t| t.id.as_str()).collect());
    if terminals.is_empty() {
        return Ok(0);
    }
    let cells = orch.fleet.snapshot().cells;
    let mut settled = 0;
    for terminal in terminals {
        let holder = ResidencyHolder::parse_storage_key(&terminal.holder);
        // A holder this build cannot parse is one it cannot look up a route
        // for, so it is treated as routed: unknown, never lost.
        let holder_routed = holder
            .as_ref()
            .is_none_or(|holder| orch.fleet.residency_route_executor(holder).is_some());
        let confirmed = match judge_terminal_process(&terminal, &cells, holder_routed) {
            TerminalLiveness::Alive | TerminalLiveness::Unknown => {
                watch.clear(&terminal.id);
                None
            }
            TerminalLiveness::Lost(loss @ ProcessLoss::LeaseGone { executor: None }) => watch
                .unroutable_loss_confirmed(&terminal.id, &orch.fleet.snapshot_receipts())
                .then_some(loss),
            TerminalLiveness::Lost(suspected) => {
                watch.clear(&terminal.id);
                let Some(holder) = holder else {
                    continue;
                };
                let answer = orch
                    .fleet
                    .operate_residency(
                        orch,
                        ResidencyOperation::Renew {
                            fence: ResidencyFence {
                                holder,
                                incarnation_id: terminal.incarnation_id.clone(),
                                cell_epoch: terminal.cell_epoch,
                            },
                        },
                    )
                    .await;
                confirm_suspected_loss(&terminal, &suspected, &answer)
            }
        };
        let Some(loss) = confirmed else {
            continue;
        };
        let reason = loss.reason();
        match crate::mcp::handlers::terminal::settle_lost_terminal(
            orch,
            &terminal.session_id,
            loss.exit_code(),
            &reason,
        )
        .await
        {
            Ok(true) => {
                settled += 1;
                watch.clear(&terminal.id);
                tracing::warn!(terminal_id = %terminal.id, %reason, "settled a running terminal whose process is gone");
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(terminal_id = %terminal.id, %error, "could not settle a terminal whose process is gone");
            }
        }
    }
    Ok(settled)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SearchIndex;

    async fn seeded_lease_db(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(
            "INSERT INTO job_terminals
                 (id, session_id, command, status, created_at, slug,
                  residency_holder, residency_incarnation_id, cell_epoch, process_generation)
             VALUES
                 ('exited', 's-exited', 'true', 'exited', 1, 'exited', 'lease-exited', 'inc-exited', 1, 4),
                 ('running', 's-running', 'true', 'running', 1, 'running', 'lease-running', 'inc-running', 2, 5);",
        )
        .await
        .unwrap();
        db
    }

    async fn active_terminal_db() -> DbState {
        let local = Arc::new(crate::storage::migrated_test_db("terminal-host-active.db").await);
        local
            .execute_script(
                "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
                 INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p', 'w', 'P', 'p', '/tmp', 1, 1);
                 INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i', 'p', 1, 'I', 'active', 'active', 'none', 1, 1);
                 INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at) VALUES('j', 'p', 'i', 'running', 'job-session', 1, 1);
                 INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at) VALUES('t', 'p', 'thread-ux', 'active', 'none', 1, 1);
                 INSERT INTO jobs(id, project_id, thread_id, status, uri_segment, node_name, created_at, updated_at) VALUES('t-job', 'p', 't', 'idle', 'thread', 'Thread', 1, 1);
                 INSERT INTO job_terminals(id, job_id, session_id, command, status, created_at, slug) VALUES('thread-terminal', 't-job', 'thread-session', 'thread command', 'running', 10, 'thread-term');
                 INSERT INTO job_terminals(id, job_id, session_id, command, status, created_at, slug) VALUES('job-terminal', 'j', 'job-session', 'job command', 'running', 20, 'job');
                 INSERT INTO job_terminals(id, project_id, session_id, command, status, created_at, slug) VALUES('project-terminal', 'p', 'project-session', 'project command', 'running', 30, 'project');",
            )
            .await
            .unwrap();
        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        DbState::new(local, index)
    }

    #[tokio::test]
    async fn startup_cleanup_clears_only_exited_terminal_fences() {
        let db = seeded_lease_db("terminal-host-startup-cleanup.db").await;
        let advertised = HashSet::from([("lease-exited".to_string(), "inc-exited".to_string(), 1)]);
        assert_eq!(
            clear_unadvertised_exited_terminal_lease_bindings(&db, &advertised)
                .await
                .unwrap(),
            0
        );

        assert_eq!(
            clear_unadvertised_exited_terminal_lease_bindings(&db, &HashSet::new())
                .await
                .unwrap(),
            1
        );

        let exited: (String, Option<String>) = db
            .query_one(
                "SELECT status, residency_holder FROM job_terminals WHERE id = 'exited'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .unwrap();
        let running: (String, Option<String>) = db
            .query_one(
                "SELECT status, residency_holder FROM job_terminals WHERE id = 'running'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(exited, ("exited".to_string(), None));
        assert_eq!(
            running,
            ("running".to_string(), Some("lease-running".to_string()))
        );
    }

    #[tokio::test]
    async fn missing_lease_resolution_is_fenced_and_idempotent() {
        let db = seeded_lease_db("terminal-host-missing-lease.db").await;
        db.execute_script("UPDATE job_terminals SET status = 'recovering' WHERE id = 'running';")
            .await
            .unwrap();
        assert!(
            !resolve_missing_terminal_lease(&db, "lease-running", "old-incarnation", 2)
                .await
                .unwrap()
        );
        assert!(
            resolve_missing_terminal_lease(&db, "lease-running", "inc-running", 2)
                .await
                .unwrap()
        );
        assert!(
            !resolve_missing_terminal_lease(&db, "lease-running", "inc-running", 2)
                .await
                .unwrap()
        );

        let (status, residency_holder, exited_at): (String, Option<String>, Option<i64>) = db
            .query_one(
                "SELECT status, residency_holder, exited_at FROM job_terminals WHERE id = 'running'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?, row.opt_i64(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(status, "exited");
        assert_eq!(residency_holder, None);
        assert!(exited_at.is_some());
    }

    /// The transition that makes recovery reachable at all. Before this, a
    /// terminal whose executor went away was finalized as exited, so the
    /// reconcile loop — which acts only on `recovering` rows — never saw it and
    /// the restart had to be asked for by hand.
    #[tokio::test]
    async fn an_executor_loss_fences_a_running_terminal_into_recovery() {
        let db = Arc::new(seeded_lease_db("terminal-host-mark-recovering.db").await);

        assert_eq!(
            mark_terminal_recovering(db.clone(), "s-running".into())
                .await
                .unwrap(),
            TerminalRecovery::Fenced
        );
        // A second delivery of the same death moves nothing — but it must say
        // that recovery already holds the terminal, not merely that this call
        // changed no row. The caller releases the environment on the second
        // answer and retains it on the first, so collapsing them costs the
        // pending restart the cell it was going to start in.
        assert_eq!(
            mark_terminal_recovering(db.clone(), "s-running".into())
                .await
                .unwrap(),
            TerminalRecovery::AlreadyRecovering
        );
        // A terminal that already ended is not dragged back into recovery, and
        // is reported as something recovery does not hold.
        assert_eq!(
            mark_terminal_recovering(db.clone(), "s-exited".into())
                .await
                .unwrap(),
            TerminalRecovery::NotRecoverable
        );
        // A session with no row at all is likewise nothing to recover.
        assert_eq!(
            mark_terminal_recovering(db.clone(), "s-absent".into())
                .await
                .unwrap(),
            TerminalRecovery::NotRecoverable
        );

        let (status, holder): (String, Option<String>) = db
            .query_one(
                "SELECT status, residency_holder FROM job_terminals WHERE id = 'running'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(status, "recovering");
        // The environment binding survives: it is what the restart starts into,
        // and clearing it here would leave recovery with nowhere to go.
        assert_eq!(holder, Some("lease-running".to_string()));
    }

    #[test]
    fn owner_recovery_window_uses_startup_not_terminal_exit_time() {
        let started = Instant::now();
        assert!(!owner_recovery_window_elapsed(
            started,
            started + Duration::from_secs(599)
        ));
        assert!(owner_recovery_window_elapsed(
            started,
            started + Duration::from_secs(600)
        ));
    }

    #[tokio::test]
    async fn running_terminals_include_scope_creation_time_in_newest_first_order() {
        let db = active_terminal_db().await;

        let terminals = get_running_terminals(&db).await.unwrap();

        assert_eq!(terminals.len(), 3);
        assert_eq!(terminals[0].id, "project-terminal");
        assert_eq!(terminals[0].job_id, None);
        assert_eq!(terminals[0].created_at, 30);
        assert_eq!(terminals[1].id, "job-terminal");
        assert_eq!(terminals[1].job_id.as_deref(), Some("j"));
        assert_eq!(terminals[1].created_at, 20);
        assert_eq!(terminals[2].id, "thread-terminal");
        assert_eq!(terminals[2].created_at, 10);
    }

    /// This listing is what draws the facet strip, so a terminal missing from it
    /// is a terminal the user cannot reach. A thread's job has no issue and no
    /// execution, and joining them inwards dropped every thread-owned terminal:
    /// the operator's thread terminal ran, accepted typing, and had no chip
    /// (CAIRN-3841).
    #[tokio::test]
    async fn a_threads_running_terminal_is_listed_and_names_its_thread() {
        let db = active_terminal_db().await;

        let terminals = get_running_terminals(&db).await.unwrap();

        let thread = terminals
            .iter()
            .find(|terminal| terminal.id == "thread-terminal")
            .expect("a thread's running terminal belongs in the running listing");
        assert_eq!(thread.job_id.as_deref(), Some("t-job"));
        assert_eq!(thread.slug.as_deref(), Some("thread-term"));
        assert_eq!(thread.project_key, "p");
        assert_eq!(thread.project_id, "p");
        // No issue coordinate to report, and the thread named instead of it.
        assert_eq!(thread.issue_id, None);
        assert_eq!(thread.issue_number, None);
        assert_eq!(thread.exec_seq, None);
        assert_eq!(thread.thread_name.as_deref(), Some("thread-ux"));

        // An issue node's terminal keeps its own coordinate and claims no thread.
        let node = terminals
            .iter()
            .find(|terminal| terminal.id == "job-terminal")
            .expect("an issue node's terminal is still listed");
        assert_eq!(node.issue_number, Some(1));
        assert_eq!(node.thread_name, None);
    }
}

pub(crate) const TERMINAL_HEARTBEAT_TIMEOUT_MS: u64 = 5 * 60 * 1000;
pub(crate) const TERMINAL_RECLAIM_GRACE_MS: u64 = 5 * 60 * 1000;
static TERMINAL_OWNER_RECOVERY_STARTED: OnceLock<Instant> = OnceLock::new();

fn terminal_owner_recovery_duration() -> Duration {
    Duration::from_millis(TERMINAL_HEARTBEAT_TIMEOUT_MS + TERMINAL_RECLAIM_GRACE_MS)
}

fn owner_recovery_window_elapsed(started: Instant, now: Instant) -> bool {
    now.duration_since(started) >= terminal_owner_recovery_duration()
}

pub(crate) fn terminal_owner_recovery_window_elapsed() -> bool {
    TERMINAL_OWNER_RECOVERY_STARTED
        .get()
        .is_some_and(|started| owner_recovery_window_elapsed(*started, Instant::now()))
}

/// Start the owner-loss clock for leases retained by executors across this host's
/// restart. Cleanup waits the full heartbeat plus reclaim window regardless of
/// when the terminal process itself exited.
pub fn schedule_exited_terminal_lease_recovery(orch: Orchestrator) {
    let started = *TERMINAL_OWNER_RECOVERY_STARTED.get_or_init(Instant::now);
    tokio::spawn(async move {
        let deadline = started + terminal_owner_recovery_duration();
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
        let advertised: HashSet<(String, String, u64)> = orch
            .fleet
            .snapshot()
            .cells
            .into_iter()
            .filter_map(|cell| {
                let residency = cell.residency.as_ref()?;
                Some((
                    residency.holder.storage_key(),
                    residency.incarnation_id.clone(),
                    cell.cell_epoch,
                ))
            })
            .collect();
        match clear_unadvertised_exited_terminal_lease_bindings(&orch.db.local, &advertised).await {
            Ok(count) if count > 0 => log::info!(
                "Cleared {count} terminal lease binding(s) after owner-recovery grace elapsed"
            ),
            Ok(_) => {}
            Err(error) => log::warn!("Could not clear terminal lease bindings: {error}"),
        }
    });
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobTerminal {
    id: String,
    job_id: Option<String>,
    project_id: Option<String>,
    run_id: Option<String>,
    session_id: String,
    command: String,
    title: Option<String>,
    description: Option<String>,
    status: String,
    exit_code: Option<i32>,
    created_at: i64,
    exited_at: Option<i64>,
    pub slug: Option<String>,
    residency_holder: Option<String>,
    residency_incarnation_id: Option<String>,
    cell_epoch: Option<u64>,
    process_generation: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActiveTerminal {
    id: String,
    session_id: String,
    command: String,
    job_id: Option<String>,
    issue_id: Option<String>,
    issue_number: Option<i32>,
    project_id: String,
    slug: Option<String>,
    node_name: Option<String>,
    exec_seq: Option<i32>,
    project_key: String,
    created_at: i64,
    /// The thread this terminal's job belongs to, when its owner is a thread
    /// rather than an issue node. A thread has no issue, so `issue_number` is
    /// then `None` and there is no issue coordinate to route or label by; this
    /// names where the terminal lives instead. It takes precedence over the
    /// other coordinates rather than being a fallback for their absence: a
    /// thread's *task* runs in a synthetic delegation execution and so still
    /// reports an `exec_seq`.
    thread_name: Option<String>,
}

fn terminal_from_row(row: &cairn_db::turso::Row) -> DbResult<JobTerminal> {
    Ok(JobTerminal {
        id: row.text(0)?,
        job_id: row.opt_text(1)?,
        project_id: row.opt_text(2)?,
        run_id: row.opt_text(3)?,
        session_id: row.text(4)?,
        command: row.text(5)?,
        title: row.opt_text(6)?,
        description: row.opt_text(7)?,
        status: row.text(8)?,
        exit_code: row.opt_i64(9)?.map(|value| value as i32),
        created_at: row.i64(10)?,
        exited_at: row.opt_i64(11)?,
        slug: row.opt_text(12)?,
        residency_holder: row.opt_text(13)?,
        residency_incarnation_id: row.opt_text(14)?,
        cell_epoch: row.opt_i64(15)?.map(|value| value as u64),
        process_generation: row.opt_i64(16)?.map(|value| value as u64),
    })
}

const TERMINAL_SELECT: &str = "
    SELECT id, job_id, project_id, run_id, session_id, command, title,
           description, status, exit_code, created_at, exited_at, slug,
           residency_holder, residency_incarnation_id, cell_epoch, process_generation
    FROM job_terminals
";

pub async fn create_pty(
    orch: &Orchestrator,
    cwd: String,
    cols: u16,
    rows: u16,
    shell: Option<String>,
) -> Result<String, String> {
    let slug = format!("scratch-{}", Uuid::new_v4());
    let resource =
        crate::mcp::handlers::terminal::terminal_resource_for_cwd(&orch.db.local, &cwd, slug)
            .await?;
    crate::mcp::handlers::terminal::create_interactive_terminal_from_resource(
        orch,
        resource,
        None,
        "Terminal".to_string(),
        shell,
        cairn_common::executor_protocol::ResidentPtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        },
    )
    .await
}

/// Deliver what an operator typed or pasted into the terminal pane.
///
/// This is command delivery into the same job residence an agent's append targets,
/// so it takes the same alignment-aware path: pressing Enter after a logical write
/// must execute against the new content, not against whatever the shell's checkout
/// held when it spawned. The cost lands per delivered line rather than per
/// keystroke, because the delimiter predicate gates it.
///
/// `submit: false` — the pane sends exactly what the user produced, including the
/// `\r` an Enter key emits, and appending a newline here would submit a line the
/// user had not finished.
pub async fn write_pty(
    orch: &Orchestrator,
    session_id: String,
    data: String,
) -> Result<(), String> {
    crate::mcp::handlers::terminal::deliver_terminal_input(orch, &session_id, &data, false)
        .await
        .map(|_| ())
}

pub async fn resize_pty(
    orch: &Orchestrator,
    session_id: String,
    cols: u16,
    rows: u16,
) -> Result<(), String> {
    crate::mcp::handlers::terminal::resize_terminal_by_session(orch, &session_id, cols, rows).await
}

pub async fn close_pty(orch: &Orchestrator, session_id: String) -> Result<(), String> {
    crate::mcp::handlers::terminal::stop_terminal_by_session(orch, &session_id).await
}

pub fn check_pty_session_exists(orch: &Orchestrator, session_id: String) -> Result<bool, String> {
    Ok(orch
        .pty_state
        .sessions
        .lock()
        .map_err(|e| e.to_string())?
        .contains_key(&session_id))
}

pub fn get_pty_buffer(orch: &Orchestrator, session_id: String) -> Result<String, String> {
    let sessions = orch.pty_state.sessions.lock().map_err(|e| e.to_string())?;
    let session = sessions.get(&session_id).ok_or("Session not found")?;
    let session = session.lock().map_err(|e| e.to_string())?;
    match &session.output_buffer {
        Some(buffer) => {
            let bytes: Vec<u8> = buffer
                .lock()
                .map_err(|e| e.to_string())?
                .iter()
                .copied()
                .collect();
            Ok(String::from_utf8_lossy(&bytes).to_string())
        }
        None => Ok(String::new()),
    }
}

pub fn get_pty_command_states(
    orch: &Orchestrator,
    session_ids: Vec<String>,
) -> Result<HashMap<String, PtyCommandSnapshot>, String> {
    let sessions = orch.pty_state.sessions.lock().map_err(|e| e.to_string())?;
    let mut states = HashMap::new();
    for session_id in session_ids {
        let snapshot = sessions
            .get(&session_id)
            .and_then(|session| session.lock().ok())
            .and_then(|session| {
                session
                    .command_state
                    .as_ref()
                    .and_then(|state| state.lock().ok())
                    .map(|state| PtyCommandSnapshot {
                        busy: state.busy,
                        exit_code: state.last_exit_code,
                        duration_ms: state.last_duration_ms,
                    })
            })
            .unwrap_or(PtyCommandSnapshot {
                busy: false,
                exit_code: None,
                duration_ms: None,
            });
        states.insert(session_id, snapshot);
    }
    Ok(states)
}

async fn delete_terminal(db: Arc<LocalDb>, terminal_id: String) -> Result<(), String> {
    db.write(|conn| {
        let terminal_id = terminal_id.clone();
        Box::pin(async move {
            conn.execute(
                "DELETE FROM job_terminals WHERE id = ?1",
                params![terminal_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_terminal_by_session(
    db: Arc<LocalDb>,
    session_id: String,
) -> Result<JobTerminal, String> {
    db.read(|conn| {
        let session_id = session_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!("{TERMINAL_SELECT} WHERE session_id = ?1 LIMIT 1"),
                    params![session_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| terminal_from_row(&row))
                .transpose()?
                .ok_or_else(|| DbError::Row(format!("Terminal not found: {session_id}")))
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_job_terminals(db: Arc<LocalDb>, job_id: String) -> Result<Vec<JobTerminal>, String> {
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!("{TERMINAL_SELECT} WHERE job_id = ?1 ORDER BY created_at ASC"),
                    params![job_id.as_str()],
                )
                .await?;
            let mut terminals = Vec::new();
            while let Some(row) = rows.next().await? {
                terminals.push(terminal_from_row(&row)?);
            }
            Ok(terminals)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn slug_exists(
    conn: &cairn_db::turso::Connection,
    job_id: Option<&str>,
    project_id: Option<&str>,
    slug: &str,
) -> DbResult<bool> {
    let mut rows = if let Some(job_id) = job_id {
        conn.query(
            "SELECT COUNT(*) FROM job_terminals WHERE job_id = ?1 AND slug = ?2",
            params![job_id, slug],
        )
        .await?
    } else if let Some(project_id) = project_id {
        conn.query("SELECT COUNT(*) FROM job_terminals WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2", params![project_id, slug]).await?
    } else {
        return Ok(false);
    };
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row("missing slug count".to_string()))?;
    Ok(row.i64(0)? > 0)
}

async fn generate_slug_from_title(
    db: Arc<LocalDb>,
    job_id: Option<String>,
    project_id: Option<String>,
    title: String,
) -> Result<String, String> {
    let base_slug = slugify(&title);
    db.read(|conn| {
        let job_id = job_id.clone();
        let project_id = project_id.clone();
        let base_slug = base_slug.clone();
        Box::pin(async move {
            let mut counter = 1;
            let mut candidate = base_slug.clone();
            loop {
                if !slug_exists(conn, job_id.as_deref(), project_id.as_deref(), &candidate).await? {
                    return Ok(candidate);
                }
                counter += 1;
                candidate = format!("{base_slug}-{counter}");
            }
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn create_job_terminal(
    orch: &Orchestrator,
    job_id: String,
    title: String,
    initial_command: Option<String>,
) -> Result<JobTerminal, String> {
    let slug = generate_slug_from_title(
        orch.db.local.clone(),
        Some(job_id.clone()),
        None,
        title.clone(),
    )
    .await?;
    let resource =
        crate::mcp::handlers::terminal::terminal_resource_for_job(&orch.db.local, &job_id, slug)
            .await?;
    let session_id = crate::mcp::handlers::terminal::create_interactive_terminal_from_resource(
        orch,
        resource,
        initial_command,
        title,
        None,
        cairn_common::executor_protocol::ResidentPtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        },
    )
    .await?;
    load_terminal_by_session(orch.db.local.clone(), session_id).await
}

pub async fn get_job_terminals(db: &DbState, job_id: String) -> Result<Vec<JobTerminal>, String> {
    Ok(load_job_terminals(db.local.clone(), job_id)
        .await?
        .into_iter()
        .filter(|terminal| terminal.status == "running")
        .collect())
}

async fn terminal_close_session(db: Arc<LocalDb>, terminal_id: String) -> Result<String, String> {
    db.read(|conn| {
        let terminal_id = terminal_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT session_id FROM job_terminals WHERE id = ?1 LIMIT 1",
                    params![terminal_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Err(DbError::Row(format!("Terminal not found: {terminal_id}")));
            };
            row.text(0)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn close_job_terminal(orch: &Orchestrator, terminal_id: String) -> Result<(), String> {
    let session_id = match terminal_close_session(orch.db.local.clone(), terminal_id.clone()).await
    {
        Ok(session_id) => session_id,
        Err(error) if error.contains("Terminal not found") => return Ok(()),
        Err(error) => return Err(error),
    };
    let transitioned = orch
        .db
        .local
        .write(|conn| {
            let terminal_id = terminal_id.clone();
            Box::pin(async move {
                Ok(conn
                    .execute(
                        "UPDATE job_terminals SET status = 'closing',
                             exited_at = COALESCE(exited_at, ?2)
                         WHERE id = ?1 AND status IN ('running', 'recovering')",
                        (terminal_id.as_str(), chrono::Utc::now().timestamp()),
                    )
                    .await?)
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    if transitioned > 0 {
        emit_terminal_change(orch, "update");
    }
    if crate::mcp::handlers::terminal::release_terminal_by_session(orch, &session_id)
        .await
        .is_ok()
    {
        delete_terminal(orch.db.local.clone(), terminal_id).await?;
        emit_terminal_change(orch, "delete");
    }
    Ok(())
}

pub async fn get_running_terminals(db: &DbState) -> Result<Vec<ActiveTerminal>, String> {
    db.local.read(|conn| Box::pin(async move {
        let mut all = Vec::new();
        // Owner-aware in the same shape as `home_uri_for_job_conn`: a thread's
        // job has no issue and no execution, so joining them inwards silently
        // dropped every thread-owned terminal from this listing. That listing is
        // what draws the facet strip, so a thread terminal ran with no chip to
        // reach it by (CAIRN-3841).
        let mut rows = conn.query("SELECT jt.id, jt.session_id, jt.command, jt.job_id, j.issue_id, i.number, COALESCE(i.project_id, j.project_id), jt.slug, j.node_name, e.seq, p.key, jt.created_at, t.name FROM job_terminals jt JOIN jobs j ON j.id = jt.job_id LEFT JOIN issues i ON i.id = j.issue_id LEFT JOIN jobs parent ON parent.id = j.parent_job_id LEFT JOIN threads t ON t.id = COALESCE(j.thread_id, parent.thread_id) JOIN projects p ON p.id = COALESCE(i.project_id, j.project_id) LEFT JOIN executions e ON e.id = j.execution_id WHERE jt.status = 'running' AND jt.job_id IS NOT NULL ORDER BY jt.created_at DESC", ()).await?;
        while let Some(row) = rows.next().await? {
            all.push((row.i64(11)?, ActiveTerminal { id: row.text(0)?, session_id: row.text(1)?, command: row.text(2)?, job_id: row.opt_text(3)?, issue_id: row.opt_text(4)?, issue_number: row.opt_i64(5)?.map(|v| v as i32), project_id: row.text(6)?, slug: row.opt_text(7)?, node_name: row.opt_text(8)?, exec_seq: row.opt_i64(9)?.map(|v| v as i32), project_key: row.text(10)?, created_at: row.i64(11)?, thread_name: row.opt_text(12)? }));
        }
        let mut rows = conn.query("SELECT jt.id, jt.session_id, jt.command, p.id, p.key, jt.slug, jt.created_at FROM job_terminals jt JOIN projects p ON p.id = jt.project_id WHERE jt.status = 'running' AND jt.project_id IS NOT NULL AND jt.job_id IS NULL ORDER BY jt.created_at DESC", ()).await?;
        while let Some(row) = rows.next().await? {
            all.push((row.i64(6)?, ActiveTerminal { id: row.text(0)?, session_id: row.text(1)?, command: row.text(2)?, job_id: None, issue_id: None, issue_number: None, project_id: row.text(3)?, slug: row.opt_text(5)?, node_name: None, exec_seq: None, project_key: row.text(4)?, created_at: row.i64(6)?, thread_name: None }));
        }
        all.sort_by_key(|(created_at, _)| std::cmp::Reverse(*created_at));
        Ok(all.into_iter().map(|(_, terminal)| terminal).collect())
    })).await.map_err(|error| error.to_string())
}

pub async fn create_project_terminal(
    orch: &Orchestrator,
    project_id: String,
    title: String,
    initial_command: Option<String>,
) -> Result<JobTerminal, String> {
    let slug = generate_slug_from_title(
        orch.db.local.clone(),
        None,
        Some(project_id.clone()),
        title.clone(),
    )
    .await?;
    let resource = crate::mcp::handlers::terminal::terminal_resource_for_project(
        &orch.db.local,
        &project_id,
        slug,
    )
    .await?;
    let session_id = crate::mcp::handlers::terminal::create_interactive_terminal_from_resource(
        orch,
        resource,
        initial_command,
        title,
        None,
        cairn_common::executor_protocol::ResidentPtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        },
    )
    .await?;
    load_terminal_by_session(orch.db.local.clone(), session_id).await
}

pub async fn open_project_terminal(
    orch: &Orchestrator,
    project_id: String,
) -> Result<JobTerminal, String> {
    let sessions: HashSet<String> = orch
        .pty_state
        .sessions
        .lock()
        .map_err(|e| e.to_string())?
        .keys()
        .cloned()
        .collect();
    for terminal in
        load_running_project_terminals(orch.db.local.clone(), project_id.clone()).await?
    {
        if sessions.contains(&terminal.session_id) {
            return Ok(terminal);
        }
    }
    create_project_terminal(orch, project_id, "Terminal".to_string(), None).await
}

async fn load_running_project_terminals(
    db: Arc<LocalDb>,
    project_id: String,
) -> Result<Vec<JobTerminal>, String> {
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let mut rows = conn.query(&format!("{TERMINAL_SELECT} WHERE project_id = ?1 AND status = 'running' ORDER BY created_at DESC"), params![project_id.as_str()]).await?;
            let mut terminals = Vec::new();
            while let Some(row) = rows.next().await? { terminals.push(terminal_from_row(&row)?); }
            Ok(terminals)
        })
    }).await.map_err(|error| error.to_string())
}

pub async fn get_project_terminals(
    orch: &Orchestrator,
    project_id: String,
) -> Result<Vec<JobTerminal>, String> {
    let sessions: HashSet<String> = orch
        .pty_state
        .sessions
        .lock()
        .map_err(|e| e.to_string())?
        .keys()
        .cloned()
        .collect();
    Ok(
        load_running_project_terminals(orch.db.local.clone(), project_id)
            .await?
            .into_iter()
            .filter(|terminal| sessions.contains(&terminal.session_id))
            .collect(),
    )
}
