//! Workflow run lifecycle (CAIRN-2487): the workflow sibling of `calls.rs`.
//!
//! A workflow run is a node-less job+session+run whose process is a supervised
//! `bun <script>` (see `backends/workflow.rs`), NOT an agent session. It reuses
//! the same real-run row shape as an ephemeral call — `insert_child_job_session_run`,
//! a per-run `output_contract`, a `none`/`inherit` worktree — so the delegated
//! task/call wait/suspend/resume tail runs it unchanged. It differs from a call
//! in skipping ALL agent-config/model/preset resolution (a script has no model)
//! and in creating its own initial turn here (a call's turn is created by
//! `start_agent_session`, which the lean workflow path bypasses).
//!
//! CAIRN-2498 adds the `workflow_run` re-dispatch record and the startup
//! `redispatch_crashed_workflows` pass that re-spawns an interrupted run for its
//! SAME run_id so the journal replays completed calls.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use super::turns::create_initial_turn_db;
use super::*;
use crate::models::DelegatedOutputContract;

pub(crate) struct CreateWorkflowRunInput {
    pub parent_job_id: String,
    pub execution_id: Option<String>,
    pub workflow_id: String,
    /// Absolute path to the workflow's resolved script entry.
    pub script_path: PathBuf,
    pub description: String,
    /// The validated named args, forwarded to the script as `CAIRN_WORKFLOW_ARGS`.
    pub args_json: String,
    pub output_contract: DelegatedOutputContract,
    pub worktree: CallWorktree,
    pub label: Option<String>,
    pub phase: Option<String>,
    pub parent_tool_use_id: Option<String>,
    pub task_index: Option<i32>,
}

pub(crate) struct PreparedWorkflowRun {
    pub job_id: String,
    pub run_id: String,
    pub session_id: String,
    pub working_dir: String,
    pub script_path: PathBuf,
    pub args_json: String,
    pub execution_id: Option<String>,
    pub turn_id: String,
    pub workflow_id: String,
    /// The declared output artifact name (`CAIRN_WORKFLOW_OUTPUT`); the harness
    /// `output()` writes `cairn:~/{name}` to complete the run.
    pub output_name: String,
}

/// Mint the job+session+run rows and the initial turn for a workflow run, seed
/// its transcript, and return the handle `start_workflow_run` spawns from.
///
/// Split from [`start_workflow_run`] so the caller can persist the workflow
/// packet BETWEEN them (closing the fast-finish resume race), exactly as
/// `prepare_call_run`/`start_call_run` do.
pub(crate) fn prepare_workflow_run(
    orch: &Orchestrator,
    input: CreateWorkflowRunInput,
) -> Result<PreparedWorkflowRun, String> {
    let db = run_db({
        let dbs = orch.db.clone();
        let parent_job_id = input.parent_job_id.clone();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &parent_job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;

    let parent_job = run_db(load_job(
        db.clone(),
        input.parent_job_id.clone(),
        "Parent job not found",
    ))?;
    let project_id = parent_job.project_id.clone();
    let issue_id = parent_job.issue_id.clone();
    let execution_id = input
        .execution_id
        .clone()
        .or_else(|| parent_job.execution_id.clone());
    let node_path = run_db({
        let db = db.clone();
        let project_id = project_id.clone();
        async move { Ok(project_node_modules(db, project_id).await) }
    })?;

    let job_id = ids::mint_child(&project_id);
    let run_id = ids::mint_child(&job_id);
    let session_id = ids::mint_session_id().into_string();
    let turn_id = ids::mint_child(&run_id);
    let now = chrono::Utc::now().timestamp() as i32;

    // Worktree mode mirrors a call: `inherit` shares the caller's tree; `none`
    // runs in a fresh scratch dir with no project-tree binding.
    let (worktree_path, working_dir, base_commit) = match input.worktree {
        CallWorktree::Inherit => {
            let path = parent_job
                .worktree_path
                .clone()
                .ok_or("Parent job has no worktree - cannot inherit for a workflow")?;
            let base = worktree_head_commit(orch, Path::new(&path));
            (Some(path.clone()), path, base)
        }
        CallWorktree::None => {
            let scratch = std::env::temp_dir().join(format!("cairn-workflow-{run_id}"));
            std::fs::create_dir_all(&scratch)
                .map_err(|e| format!("Failed to create workflow scratch dir: {e}"))?;
            (None, scratch.to_string_lossy().into_owned(), None)
        }
    };

    let output_name = input.output_contract.artifact_name();
    let output_contract_json = serde_json::to_string(&input.output_contract)
        .map_err(|e| format!("Failed to serialize workflow output contract: {e}"))?;

    run_db(insert_child_job_session_run(
        db.clone(),
        ChildInsert {
            job_id: job_id.clone(),
            run_id: run_id.clone(),
            session_id: session_id.clone(),
            parent_job_id: input.parent_job_id.clone(),
            worktree_path: worktree_path.clone(),
            branch: None,
            // A workflow has no agent; the synthetic id marks the job as a workflow
            // node without loading an agent config.
            agent_config_id: "workflow".to_string(),
            project_id: project_id.clone(),
            issue_id: issue_id.clone(),
            execution_id: execution_id.clone(),
            description: input.description.clone(),
            model: None,
            base_commit,
            owns_ephemeral_worktree: false,
            output_contract: Some(output_contract_json),
            label: input.label.clone(),
            phase: input.phase.clone(),
            parent_tool_use_id: input.parent_tool_use_id.clone(),
            task_index: input.task_index,
            now,
        },
    ))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::job_db_change_ids(
            "insert",
            &job_id,
            issue_id.as_deref(),
            execution_id.as_deref(),
            Some(&input.parent_job_id),
            None,
            &project_id,
        ),
    );
    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::run_db_change_ids("insert", &run_id, Some(&job_id)),
    );

    // Seed the transcript with the invocation, anchoring the turn.
    let seed = format!(
        "Workflow `{}` invoked.\n\nArgs:\n{}",
        input.workflow_id, input.args_json
    );
    store_user_event(orch, &run_id, &session_id, &seed, now, -1)?;

    // Create the initial turn (pending) and point the job at it; the backend
    // moves it to running once the process spawns.
    super::turns::create_initial_turn(orch, &turn_id, &session_id, &job_id)?;

    // Record the durable spawn params (private, runner-transient) so a crash
    // mid-run is re-dispatchable for the SAME run_id on startup. Best-effort:
    // the run still starts if this write fails; only re-dispatch is forfeited.
    let workflow_run_row = WorkflowRunRow {
        run_id: run_id.clone(),
        job_id: job_id.clone(),
        session_id: Some(session_id.clone()),
        workflow_id: input.workflow_id.clone(),
        script_path: input.script_path.to_string_lossy().into_owned(),
        args_json: input.args_json.clone(),
        working_dir: working_dir.clone(),
        output_name: output_name.clone(),
        node_path: node_path.clone(),
    };
    if let Err(e) = run_db({
        let db = orch.db.local.clone();
        async move { persist_workflow_run_row(&db, &workflow_run_row).await }
    }) {
        log::warn!("Failed to record workflow_run spawn params for {run_id}: {e}");
    }

    Ok(PreparedWorkflowRun {
        job_id,
        run_id,
        session_id,
        working_dir,
        script_path: input.script_path,
        args_json: input.args_json,
        execution_id,
        turn_id,
        workflow_id: input.workflow_id,
        output_name,
    })
}

/// The durable spawn parameters of a workflow run, recorded in `workflow_run`
/// (private, runner-transient) so startup re-dispatch can re-spawn the SAME
/// run_id after a crash. The run row alone cannot reconstruct the process (the
/// script path and args are not on it), so this is the re-dispatch source of
/// truth. `turn_id` is intentionally absent: re-dispatch mints a fresh turn,
/// since the crashed run's turn is terminal (interrupted).
#[derive(Debug, Clone)]
pub(crate) struct WorkflowRunRow {
    pub run_id: String,
    pub job_id: String,
    pub session_id: Option<String>,
    pub workflow_id: String,
    pub script_path: String,
    pub args_json: String,
    pub working_dir: String,
    pub output_name: String,
    /// The invoking project repo's `node_modules`, recorded diagnostically. It no
    /// longer drives resolution — the harness is linked into a `node_modules`
    /// beside the script at spawn (see `backends::workflow::ensure_harness_link`).
    pub node_path: Option<String>,
}

/// Record a workflow run's spawn parameters. Best-effort at spawn time: a
/// failure only forfeits re-dispatch of THIS run on a later crash, never the
/// run itself.
pub(crate) async fn persist_workflow_run_row(
    db: &LocalDb,
    row: &WorkflowRunRow,
) -> Result<(), String> {
    let row = row.clone();
    let now = chrono::Utc::now().timestamp();
    db.write(move |conn| {
        let row = row.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT OR REPLACE INTO workflow_run \
                 (run_id, job_id, session_id, workflow_id, script_path, args_json, \
                  working_dir, output_name, node_path, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    row.run_id.as_str(),
                    row.job_id.as_str(),
                    row.session_id.as_deref(),
                    row.workflow_id.as_str(),
                    row.script_path.as_str(),
                    row.args_json.as_str(),
                    row.working_dir.as_str(),
                    row.output_name.as_str(),
                    row.node_path.as_deref(),
                    now
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Load every recorded workflow run's spawn parameters. Startup re-dispatch
/// filters these by each run's live status via routing (a workflow run may live
/// in a team replica while this private table does not), so no run-status join
/// happens here.
pub(crate) async fn load_all_workflow_run_rows(
    db: &LocalDb,
) -> Result<Vec<WorkflowRunRow>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT run_id, job_id, session_id, workflow_id, script_path, \
                     args_json, working_dir, output_name, node_path \
                     FROM workflow_run",
                    (),
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(WorkflowRunRow {
                    run_id: row.text(0)?,
                    job_id: row.text(1)?,
                    session_id: row.opt_text(2)?,
                    workflow_id: row.text(3)?,
                    script_path: row.text(4)?,
                    args_json: row.text(5)?,
                    working_dir: row.text(6)?,
                    output_name: row.text(7)?,
                    node_path: row.opt_text(8)?,
                });
            }
            Ok(out)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Drop a workflow run's spawn record once the run reaches a terminal outcome,
/// so it is never re-dispatched and the table stays bounded.
pub(crate) async fn delete_workflow_run_row(db: &LocalDb, run_id: &str) -> Result<(), String> {
    let run_id = run_id.to_string();
    db.write(move |conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            conn.execute(
                "DELETE FROM workflow_run WHERE run_id = ?1",
                params![run_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// The invoking project repo's `node_modules` directory, when it exists on disk,
/// recorded on the `workflow_run` row as a diagnostic of where the workflow was
/// invoked from. It no longer drives harness resolution (see
/// `backends::workflow::ensure_harness_link`). Returns `None` for a repo with no
/// installed dependencies.
async fn project_node_modules(db: Arc<LocalDb>, project_id: String) -> Option<String> {
    let repo_path: String = db
        .read(|conn| {
            let project_id = project_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT repo_path FROM projects WHERE id = ?1 LIMIT 1",
                        (project_id.as_str(),),
                    )
                    .await?;
                Ok(rows.next().await?.and_then(|row| row.text(0).ok()))
            })
        })
        .await
        .ok()
        .flatten()?;
    let node_modules = Path::new(&repo_path).join("node_modules");
    node_modules
        .is_dir()
        .then(|| node_modules.to_string_lossy().into_owned())
}

/// Re-dispatch every workflow run that was still in flight when the host died
/// (CAIRN-2498). For each durable `workflow_run` record whose run is
/// non-terminal, terminalize its orphaned turn, reset the run, mint a fresh
/// turn, and re-spawn `bun <script>` for the SAME run_id — so the workflow
/// journal (keyed by that run_id) replays the calls it already completed instead
/// of re-running them. Best-effort and idempotent: a stale record (the run
/// finished before its record was cleared) is dropped rather than re-run. Called
/// once at host startup, after the callback server is bound.
pub(crate) async fn redispatch_crashed_workflows(orch: &Orchestrator) {
    let rows = match load_all_workflow_run_rows(&orch.db.local).await {
        Ok(rows) => rows,
        Err(e) => {
            log::warn!("workflow re-dispatch: failed to load spawn records: {e}");
            return;
        }
    };
    if rows.is_empty() {
        return;
    }
    let mut redispatched = 0usize;
    for row in rows {
        match redispatch_one_workflow(orch, &row).await {
            Ok(true) => redispatched += 1,
            Ok(false) => {}
            Err(e) => log::warn!("workflow re-dispatch for run {} failed: {e}", row.run_id),
        }
    }
    if redispatched > 0 {
        log::info!("Re-dispatched {redispatched} crashed workflow run(s) on startup");
    }
}

/// Re-dispatch a single workflow run at startup. Returns `Ok(true)` when a
/// process was re-spawned, `Ok(false)` when the record was stale (dropped),
/// preserved (a user-restartable terminal run), or unrebuildable.
///
/// Record fate for a terminal run splits (CAIRN-2516): a run is never
/// auto-re-dispatched here, but its record is PRESERVED when the run is
/// user-restartable (a deliberate `user_stop`, or a `failed` script) so the
/// header Restart survives an app relaunch — while a genuinely stale (missing)
/// row and a cleanly-completed leftover are dropped so the table stays bounded.
/// A non-terminal (crashed) run is re-spawned in place via
/// [`respawn_workflow_run`].
async fn redispatch_one_workflow(
    orch: &Orchestrator,
    row: &WorkflowRunRow,
) -> Result<bool, String> {
    let owning = crate::execution::routing::owning_db_for_run(&orch.db, &row.run_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());

    // A missing run is a stale record (the run finished before its record was
    // cleared); drop it.
    let Some(status) = current_run_status(&owning, &row.run_id).await? else {
        let _ = delete_workflow_run_row(&orch.db.local, &row.run_id).await;
        return Ok(false);
    };
    if matches!(
        status.as_str(),
        "exited" | "failed" | "complete" | "completed"
    ) {
        // Terminal: never auto-re-dispatched. Preserve the record only when the
        // run is user-restartable so an explicit Restart works across a relaunch;
        // drop a cleanly-completed leftover.
        let exit_reason = current_run_exit_reason(&owning, &row.run_id).await?;
        let restartable = status == "failed" || exit_reason.as_deref() == Some("user_stop");
        if !restartable {
            let _ = delete_workflow_run_row(&orch.db.local, &row.run_id).await;
        }
        return Ok(false);
    }

    // Non-terminal (crashed): re-spawn in place.
    respawn_workflow_run(orch, owning, row).await
}

/// Reset a workflow run and re-spawn its supervised `bun <script>` for the SAME
/// run_id, so the workflow journal (keyed by run_id) replays the calls it already
/// completed. The re-dispatch tail shared by the startup crash sweep
/// ([`redispatch_one_workflow`]) and the explicit header [`restart_workflow`].
/// Returns `Ok(false)` when the record cannot be rebuilt (no session id).
async fn respawn_workflow_run(
    orch: &Orchestrator,
    owning: Arc<LocalDb>,
    row: &WorkflowRunRow,
) -> Result<bool, String> {
    // Without a session id the turn cannot be rebuilt; leave the record for a
    // future pass rather than spawn a malformed run.
    let Some(session_id) = row.session_id.clone() else {
        return Ok(false);
    };

    // Terminalize the prior run's orphaned turn(s) and reset the run to
    // `starting` so the spawn path (which only transitions starting -> live) can
    // re-mint it under the SAME run_id.
    reset_run_for_redispatch(&owning, &row.run_id, &row.job_id).await?;

    // Fresh pending turn (the prior turn is now terminal, clearing the
    // active-turn guard).
    let turn_id = ids::mint_child(&row.run_id);
    create_initial_turn_db(
        owning.clone(),
        turn_id.clone(),
        session_id.clone(),
        row.job_id.clone(),
    )
    .await?;

    crate::backends::workflow::spawn_workflow_process(
        orch,
        crate::backends::workflow::WorkflowSpawnParams {
            run_id: row.run_id.clone(),
            working_dir: row.working_dir.clone(),
            script_path: PathBuf::from(&row.script_path),
            args_json: row.args_json.clone(),
            turn_id,
            session_id: Some(session_id),
            output_name: row.output_name.clone(),
        },
    )?;
    Ok(true)
}

/// Explicitly re-dispatch a stopped or failed workflow node (CAIRN-2516).
///
/// The header Restart action: load the kept `workflow_run` record for the node's
/// job and run the re-dispatch tail directly — WITHOUT the startup sweep's
/// terminal-status skip, since we intentionally re-run a terminal (Stopped or
/// Failed) run. The fresh `bun <script>` runs for the SAME run_id, so the journal
/// replays completed calls with zero new call jobs and continues live from the
/// first un-journaled call. Rejects a workflow whose latest run is still live
/// (nothing stopped to restart) or whose record is gone (a cleanly-completed
/// workflow is not restartable).
pub async fn restart_workflow(orch: &Orchestrator, workflow_job_id: &str) -> Result<(), String> {
    let owning = crate::execution::routing::owning_db_for_job(&orch.db, workflow_job_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    let Some(row) = load_workflow_run_row_by_job(&orch.db.local, workflow_job_id).await? else {
        return Err(format!(
            "No restartable workflow record for job {workflow_job_id} (already completed?)"
        ));
    };
    // Refuse to restart a run that is still live — that would spawn a second
    // process for the same run_id.
    match current_run_status(&owning, &row.run_id).await? {
        Some(status)
            if matches!(
                status.as_str(),
                "exited" | "failed" | "crashed" | "complete" | "completed"
            ) => {}
        Some(other) => {
            return Err(format!(
                "Workflow run {} is {other}, not stopped/failed; refusing restart",
                row.run_id
            ));
        }
        None => return Err(format!("Workflow run {} not found", row.run_id)),
    }
    respawn_workflow_run(orch, owning, &row).await.map(|_| ())
}

/// Load a workflow run's spawn record by its node job id (the header Restart
/// path). `None` when no record exists (a cleanly-completed workflow's record
/// was dropped, so it is not restartable).
pub(crate) async fn load_workflow_run_row_by_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<WorkflowRunRow>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT run_id, job_id, session_id, workflow_id, script_path, \
                     args_json, working_dir, output_name, node_path \
                     FROM workflow_run WHERE job_id = ?1 LIMIT 1",
                    (job_id.as_str(),),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(WorkflowRunRow {
                    run_id: row.text(0)?,
                    job_id: row.text(1)?,
                    session_id: row.opt_text(2)?,
                    workflow_id: row.text(3)?,
                    script_path: row.text(4)?,
                    args_json: row.text(5)?,
                    working_dir: row.text(6)?,
                    output_name: row.text(7)?,
                    node_path: row.opt_text(8)?,
                })),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// The `exit_reason` of a run row, or `None` when unset or the row is gone.
async fn current_run_exit_reason(db: &LocalDb, run_id: &str) -> Result<Option<String>, String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT exit_reason FROM runs WHERE id = ?1 LIMIT 1",
                    (run_id.as_str(),),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(row.opt_text(0)?),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Status of a run row, or `None` when the row is gone.
async fn current_run_status(db: &LocalDb, run_id: &str) -> Result<Option<String>, String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status FROM runs WHERE id = ?1 LIMIT 1",
                    (run_id.as_str(),),
                )
                .await?;
            Ok(rows.next().await?.and_then(|row| row.text(0).ok()))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Terminalize a crashed workflow run's orphaned turn(s) (running -> interrupted,
/// pending -> failed, matching the startup recovery matrix) and reset the run to
/// `starting`, clearing its crash exit fields, so it can be re-spawned in place.
async fn reset_run_for_redispatch(db: &LocalDb, run_id: &str, job_id: &str) -> Result<(), String> {
    let run_id = run_id.to_string();
    let job_id = job_id.to_string();
    db.write(move |conn| {
        let run_id = run_id.clone();
        let job_id = job_id.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp() as i32;
            conn.execute(
                "UPDATE turns SET state = 'interrupted', ended_at = ?1, updated_at = ?1 \
                 WHERE job_id = ?2 AND state = 'running'",
                params![now, job_id.as_str()],
            )
            .await?;
            conn.execute(
                "UPDATE turns SET state = 'failed', ended_at = ?1, updated_at = ?1 \
                 WHERE job_id = ?2 AND state = 'pending'",
                params![now, job_id.as_str()],
            )
            .await?;
            conn.execute(
                "UPDATE runs SET status = 'starting', exit_reason = NULL, exited_at = NULL, \
                 updated_at = ?1 WHERE id = ?2",
                params![now, run_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Spawn the supervised `bun <script>` process for a prepared workflow run.
pub(crate) fn start_workflow_run(
    orch: &Orchestrator,
    prepared: &PreparedWorkflowRun,
) -> Result<(), String> {
    crate::backends::workflow::spawn_workflow_process(
        orch,
        crate::backends::workflow::WorkflowSpawnParams {
            run_id: prepared.run_id.clone(),
            working_dir: prepared.working_dir.clone(),
            script_path: prepared.script_path.clone(),
            args_json: prepared.args_json.clone(),
            turn_id: prepared.turn_id.clone(),
            session_id: Some(prepared.session_id.clone()),
            output_name: prepared.output_name.clone(),
        },
    )
}

#[cfg(test)]
mod redispatch_tests {
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use std::sync::Arc;

    async fn migrated_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.keep().join("wf-redispatch.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    fn sample_row() -> WorkflowRunRow {
        WorkflowRunRow {
            run_id: "wf-run".to_string(),
            job_id: "j-wf".to_string(),
            session_id: Some("sess".to_string()),
            workflow_id: "deep-research".to_string(),
            script_path: "/wf/main.ts".to_string(),
            args_json: "{\"q\":1}".to_string(),
            working_dir: "/tmp/wf".to_string(),
            output_name: "return".to_string(),
            node_path: Some("/repo/node_modules".to_string()),
        }
    }

    async fn turn_state(db: &LocalDb, turn_id: &str) -> Option<String> {
        let turn_id = turn_id.to_string();
        db.read(|conn| {
            let turn_id = turn_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT state FROM turns WHERE id = ?1", (turn_id.as_str(),))
                    .await?;
                Ok(rows.next().await?.and_then(|r| r.text(0).ok()))
            })
        })
        .await
        .ok()
        .flatten()
    }

    async fn seed_crashed_run(db: &LocalDb) {
        for sql in [
            // FK chain: workspace -> project -> issue -> execution -> job ->
            // session, then the crashed run + orphaned turns.
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',4,'T','active',1,1)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, agent_config_id, created_at, updated_at) VALUES ('j-wf','e','i','p','running','flow','Flow','workflow',1,1)",
            "INSERT INTO sessions (id, job_id, created_at, updated_at) VALUES ('sess','j-wf',1,1)",
            "INSERT INTO runs (id, job_id, issue_id, status, created_at, updated_at) \
             VALUES ('wf-run','j-wf','i','crashed',1,1)",
            // The crashed work turn (running) plus a stray pending successor.
            "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, \
             start_reason, created_at, updated_at) \
             VALUES ('t-run','sess','wf-run','j-wf',1,'running','initial',1,1)",
            "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, \
             start_reason, created_at, updated_at) \
             VALUES ('t-pend','sess',NULL,'j-wf',2,'pending','dependency_unblock',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
    }

    async fn orch_with_db() -> (Orchestrator, Arc<LocalDb>) {
        let local = Arc::new(
            LocalDb::open(tempfile::tempdir().unwrap().keep().join("sweep.db"))
                .await
                .unwrap(),
        );
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(local.clone(), search));
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();
        (orch, local)
    }

    async fn seed_chain(db: &LocalDb) {
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',9,'T','active',1,1)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e','recipe','i','p','running',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
    }

    /// Seed a terminal workflow run + its `workflow_run` record. `with_run=false`
    /// records only the spawn row (a stale record whose run is gone).
    async fn seed_terminal_run(
        db: &LocalDb,
        tag: &str,
        status: &str,
        exit_reason: Option<&str>,
        with_run: bool,
    ) {
        let job = format!("job-{tag}");
        if with_run {
            db.execute(
                &format!(
                    "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, agent_config_id, created_at, updated_at) VALUES ('{job}','e','i','p','running','{tag}','{tag}','workflow',1,1)"
                ),
                (),
            )
            .await
            .unwrap();
            db.execute(
                &format!("INSERT INTO sessions (id, job_id, created_at, updated_at) VALUES ('sess-{tag}','{job}',1,1)"),
                (),
            )
            .await
            .unwrap();
            let er = exit_reason.map_or("NULL".to_string(), |r| format!("'{r}'"));
            db.execute(
                &format!("INSERT INTO runs (id, job_id, issue_id, status, exit_reason, created_at, updated_at) VALUES ('{tag}','{job}','i','{status}',{er},1,1)"),
                (),
            )
            .await
            .unwrap();
        }
        let row = WorkflowRunRow {
            run_id: tag.to_string(),
            job_id: job,
            session_id: Some(format!("sess-{tag}")),
            workflow_id: "deep-research".to_string(),
            script_path: "/wf/main.ts".to_string(),
            args_json: "{}".to_string(),
            working_dir: "/tmp/wf".to_string(),
            output_name: "return".to_string(),
            node_path: None,
        };
        persist_workflow_run_row(db, &row).await.unwrap();
    }

    /// The startup sweep must NOT resurrect a deliberately-stopped workflow (the
    /// trap this issue turns on) and must PRESERVE the records that make an
    /// explicit Restart work across an app relaunch (a `user_stop` exit and a
    /// `failed` script), while still pruning a stale record and a cleanly-
    /// completed leftover so the table stays bounded.
    #[tokio::test]
    async fn startup_sweep_preserves_restartable_terminal_records_and_prunes_the_rest() {
        let (orch, db) = orch_with_db().await;
        seed_chain(&db).await;
        seed_terminal_run(&db, "wf-stop", "exited", Some("user_stop"), true).await;
        seed_terminal_run(&db, "wf-fail", "failed", None, true).await;
        seed_terminal_run(&db, "wf-done", "exited", None, true).await;
        seed_terminal_run(&db, "wf-missing", "exited", None, false).await;

        redispatch_crashed_workflows(&orch).await;

        let rows = load_all_workflow_run_rows(&db).await.unwrap();
        let ids: std::collections::HashSet<&str> = rows.iter().map(|r| r.run_id.as_str()).collect();
        // Preserved: user_stop + failed are user-restartable.
        assert!(
            ids.contains("wf-stop"),
            "a stopped workflow's record must survive the sweep for Restart"
        );
        assert!(
            ids.contains("wf-fail"),
            "a failed workflow's record must survive the sweep for Restart"
        );
        // Pruned: cleanly-completed leftover + stale (missing-run) record.
        assert!(
            !ids.contains("wf-done"),
            "a cleanly-completed leftover is dropped"
        );
        assert!(
            !ids.contains("wf-missing"),
            "a stale missing-run record is dropped"
        );
        // The stopped run was NOT re-dispatched: it stays terminal.
        assert_eq!(
            current_run_status(&db, "wf-stop").await.unwrap().as_deref(),
            Some("exited")
        );
    }

    #[tokio::test]
    async fn workflow_run_row_roundtrips_and_deletes() {
        let db = migrated_db().await;
        assert!(load_all_workflow_run_rows(&db).await.unwrap().is_empty());
        persist_workflow_run_row(&db, &sample_row()).await.unwrap();
        let rows = load_all_workflow_run_rows(&db).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].run_id, "wf-run");
        assert_eq!(rows[0].script_path, "/wf/main.ts");
        assert_eq!(rows[0].args_json, "{\"q\":1}");
        assert_eq!(rows[0].node_path.as_deref(), Some("/repo/node_modules"));
        delete_workflow_run_row(&db, "wf-run").await.unwrap();
        assert!(load_all_workflow_run_rows(&db).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn current_run_status_reads_status_and_missing_is_none() {
        let db = migrated_db().await;
        assert_eq!(current_run_status(&db, "wf-run").await.unwrap(), None);
        seed_crashed_run(&db).await;
        assert_eq!(
            current_run_status(&db, "wf-run").await.unwrap().as_deref(),
            Some("crashed")
        );
    }

    /// The reset makes a crashed run re-spawnable: its run returns to `starting`
    /// (the only status the spawn path transitions from) and its orphaned turns
    /// terminalize so the fresh-turn active-turn guard passes.
    #[tokio::test]
    async fn reset_terminalizes_turns_and_resets_run() {
        let db = migrated_db().await;
        seed_crashed_run(&db).await;
        reset_run_for_redispatch(&db, "wf-run", "j-wf")
            .await
            .unwrap();
        assert_eq!(
            current_run_status(&db, "wf-run").await.unwrap().as_deref(),
            Some("starting")
        );
        assert_eq!(
            turn_state(&db, "t-run").await.as_deref(),
            Some("interrupted")
        );
        assert_eq!(turn_state(&db, "t-pend").await.as_deref(), Some("failed"));
    }
}
