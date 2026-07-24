//! CAIRN-2499: the workflow monitoring panel's node-scoped read query.
//!
//! `get_workflow_monitor` composes one workflow node's live picture from data
//! that already exists on the base: the durable `workflow_progress` phase/log
//! timeline, the node's child `agent()` call runs (`runs.label`/`runs.phase`,
//! `jobs.model`, `runs.status`, run timing), and the analytics rollups keyed by
//! run (`token_rollup.billable_tokens`, `tool_invocations`). It is a JOIN over
//! those tables plus a Rust-side phase-spine fold -- reusing the analytics
//! extraction rather than building a parallel one.

use serde::Serialize;

use cairn_db::turso::params;

use super::{list_entries, ProgressEntry};
use crate::storage::{LocalDb, RowExt};

/// One child `agent()` call run under the workflow node.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowCall {
    run_id: String,
    /// The call's job id — the target for drilling into the call's own detail.
    job_id: String,
    label: Option<String>,
    phase: Option<String>,
    model: Option<String>,
    /// Raw run status text (starting | live | exited | crashed, plus legacy).
    status: Option<String>,
    started_at: Option<i64>,
    exited_at: Option<i64>,
    /// Summed billable tokens across the run's rollup rows (0 if none folded).
    ///
    /// This is CUMULATIVE billed tokens across every request the call made, not
    /// the call's context size: for Claude each request re-bills the whole
    /// context (`input + cache_read + cache_create + output`), so a multi-turn
    /// call legitimately sums to several times the context window (the re-sent
    /// `cache_read` dominates). The panel shows this compactly (e.g. `393k`) with
    /// the composition + a "cumulative across every request" note in a tooltip,
    /// so it reads as billed throughput rather than an impossible context size.
    tokens: i64,
    /// Token components summed across the run's rollup rows, backing the display
    /// tooltip's composition breakdown (which shows where the cumulative billed
    /// total went — the re-sent `cache_read` dominates).
    input_tokens: i64,
    cache_read_tokens: i64,
    cache_create_tokens: i64,
    output_tokens: i64,
    /// Tool-invocation count for the run (0 if none extracted).
    tools: i64,
    /// The run's `exit_reason`, e.g. `"user_stop"` for a deliberately stopped
    /// call. Lets the panel render a stop distinctly from a clean completion.
    exit_reason: Option<String>,
    /// True while this call's `workflow_call` journal link still exists — the
    /// call is in-flight/undelivered and thus restartable. A delivered call
    /// (link deleted at finalize) reads false, so restart is refused.
    undelivered: bool,
}

/// One phase in the spine: a `phase()` boundary or a phase tag seen only on
/// calls (e.g. deep-research's interleaved `fetch`). `done`/`total` count the
/// calls tagged with this phase.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowPhase {
    name: String,
    /// The `phase()` boundary timestamp, else the earliest start among its calls.
    started_at: Option<i64>,
    total: i64,
    done: i64,
}

/// One `log()` line from the progress timeline.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowLog {
    text: String,
    created_at: i64,
}

/// The workflow node's OWN latest run status — the authoritative source for the
/// header's Running / Stopped / Failed / Complete state, replacing the
/// calls-only `anyLive` heuristic the panel inferred from before.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowNodeStatus {
    /// Latest run status text (starting | live | exited | crashed | failed), or
    /// `None` when the node has no run yet.
    status: Option<String>,
    /// The latest run's `exit_reason`; `"user_stop"` marks a deliberate stop,
    /// which the header renders as Stopped rather than a crash or a clean exit.
    exit_reason: Option<String>,
}

/// The workflow node's identity: which workflow package it is and the args it
/// was invoked with. `workflow_id` is durable (parsed from the node job's
/// `"Workflow: {id}"` description); `args` is the raw invocation `args_json`
/// read best-effort from the private, runner-transient `workflow_run` record,
/// which is dropped on clean completion — so `args` reads `None` once a workflow
/// finishes cleanly. Both are `None` for a job that is not a workflow node.
#[derive(Debug, Clone, Serialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowIdentity {
    workflow_id: Option<String>,
    /// Raw invocation args JSON (verbatim as validated at spawn), or `None`.
    args: Option<String>,
}

/// The full monitoring picture for a workflow node.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowMonitor {
    /// The workflow node's own run status — drives the header's stop/restart
    /// affordances (Running -> Stop, Stopped/Failed -> Restart).
    workflow: WorkflowNodeStatus,
    /// What the workflow IS: its package id and invocation args, for the panel's
    /// identity line (CAIRN-2636).
    identity: WorkflowIdentity,
    phases: Vec<WorkflowPhase>,
    calls: Vec<WorkflowCall>,
    logs: Vec<WorkflowLog>,
}

/// A run counts as "done" once its process has terminated. Mirrors
/// `RunStatus::is_terminal` plus the legacy stored spellings.
fn is_terminal(status: Option<&str>) -> bool {
    matches!(
        status,
        Some("exited" | "crashed" | "complete" | "completed" | "failed")
    )
}

/// Build the phase spine from the progress `phase` entries and the call rows.
///
/// The spine is the ordered UNION of (a) phases declared via `phase()` (in
/// append order, carrying their boundary timestamp) and (b) any phase tag seen
/// only on calls, appended in first-seen order. The union is load-bearing: a
/// workflow can tag calls with a phase it never announces a boundary for
/// (deep-research's fetches run interleaved inside the search pipeline with no
/// `phase("fetch")` call), and those calls must still attribute to a spine phase
/// rather than vanish into a 0/0. Each phase's `total`/`done` counts its tagged
/// calls; a call-only phase's `started_at` is the earliest call start.
fn build_phase_spine(calls: &[WorkflowCall], entries: &[ProgressEntry]) -> Vec<WorkflowPhase> {
    let mut phases: Vec<WorkflowPhase> = Vec::new();
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();

    for entry in entries.iter().filter(|e| e.kind == "phase") {
        if seen.insert(entry.text.as_str()) {
            phases.push(WorkflowPhase {
                name: entry.text.clone(),
                started_at: Some(entry.created_at),
                total: 0,
                done: 0,
            });
        }
    }
    for call in calls {
        if let Some(phase) = call.phase.as_deref() {
            if seen.insert(phase) {
                phases.push(WorkflowPhase {
                    name: phase.to_string(),
                    started_at: None,
                    total: 0,
                    done: 0,
                });
            }
        }
    }
    for phase in &mut phases {
        let mut earliest: Option<i64> = None;
        for call in calls
            .iter()
            .filter(|c| c.phase.as_deref() == Some(phase.name.as_str()))
        {
            phase.total += 1;
            if is_terminal(call.status.as_deref()) {
                phase.done += 1;
            }
            if let Some(start) = call.started_at {
                earliest = Some(earliest.map_or(start, |e| e.min(start)));
            }
        }
        if phase.started_at.is_none() {
            phase.started_at = earliest;
        }
    }
    phases
}

/// Query the monitoring picture for a workflow node's job.
///
/// `db` owns the node's jobs/runs (a team replica for a team workflow, else the
/// private db). `private_db` is always the local/private db, which is where the
/// runner-transient `workflow_call` links live — they never replicate, so the
/// undelivered flag is resolved from a separate query against it rather than a
/// cross-database JOIN (which would error with "no such table" on a replica).
pub async fn get_workflow_monitor(
    db: &LocalDb,
    private_db: &LocalDb,
    node_job_id: &str,
) -> Result<WorkflowMonitor, String> {
    let mut calls = list_calls(db, node_job_id).await?;
    // Mark each call undelivered iff its run still has a `workflow_call` link
    // (in-flight; the link is deleted at finalize). The transient table is tiny,
    // so loading the full run-id set and intersecting is cheaper than a join.
    let undelivered = undelivered_call_run_ids(private_db).await?;
    for call in &mut calls {
        call.undelivered = undelivered.contains(&call.run_id);
    }
    let workflow = node_run_status(db, node_job_id).await?;
    let identity = workflow_identity(db, private_db, node_job_id).await?;
    let entries = list_entries(db, node_job_id).await?;
    let phases = build_phase_spine(&calls, &entries);
    let logs = entries
        .into_iter()
        .filter(|e| e.kind == "log")
        .map(|e| WorkflowLog {
            text: e.text,
            created_at: e.created_at,
        })
        .collect();
    Ok(WorkflowMonitor {
        workflow,
        identity,
        phases,
        calls,
        logs,
    })
}

/// Resolve a workflow node's identity: its package id (durable, from the job's
/// `"Workflow: {id}"` description) and its invocation args (best-effort, from the
/// private `workflow_run` record which is dropped on clean completion). A
/// non-workflow job yields an all-`None` identity.
async fn workflow_identity(
    db: &LocalDb,
    private_db: &LocalDb,
    node_job_id: &str,
) -> Result<WorkflowIdentity, String> {
    let workflow_id = node_workflow_id(db, node_job_id).await?;
    // Best-effort: the transient record is gone after a clean completion, so a
    // read error / absent row degrades to `None` rather than failing the panel.
    let args = workflow_args(private_db, node_job_id).await.ok().flatten();
    Ok(WorkflowIdentity { workflow_id, args })
}

/// The workflow package id parsed from the node job's `"Workflow: {id}"`
/// `task_description` (set at spawn by `prepare_workflow_run`), or `None` when
/// the job has no such description (not a workflow node).
async fn node_workflow_id(db: &LocalDb, node_job_id: &str) -> Result<Option<String>, String> {
    let node_job_id = node_job_id.to_string();
    db.read(|conn| {
        let node_job_id = node_job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT task_description FROM jobs WHERE id = ?1 LIMIT 1",
                    params![node_job_id.as_str()],
                )
                .await?;
            let description = match rows.next().await? {
                Some(row) => row.opt_text(0)?,
                None => None,
            };
            Ok(description.and_then(|d| {
                d.strip_prefix("Workflow: ")
                    .map(|id| id.trim().to_string())
                    .filter(|id| !id.is_empty())
            }))
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The raw invocation `args_json` for a workflow node, read from the private,
/// runner-transient `workflow_run` record. `None` when no record exists (the
/// record is dropped on a clean completion), so a finished workflow shows its id
/// but no args.
async fn workflow_args(private_db: &LocalDb, node_job_id: &str) -> Result<Option<String>, String> {
    let node_job_id = node_job_id.to_string();
    private_db
        .read(|conn| {
            let node_job_id = node_job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT args_json FROM workflow_run WHERE job_id = ?1 LIMIT 1",
                        params![node_job_id.as_str()],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(row.opt_text(0)?),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|error| error.to_string())
}

/// The workflow node's own latest run status + exit_reason (by `created_at`), or
/// an all-`None` status when the node has no run yet.
async fn node_run_status(db: &LocalDb, node_job_id: &str) -> Result<WorkflowNodeStatus, String> {
    let node_job_id = node_job_id.to_string();
    db.read(|conn| {
        let node_job_id = node_job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    // `rowid DESC` tiebreaks the second-resolution `created_at`
                    // so a same-second restart's replacement run is the one the
                    // header status reflects, matching the wait path.
                    "SELECT status, exit_reason FROM runs \
                     WHERE job_id = ?1 ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    params![node_job_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(WorkflowNodeStatus {
                    status: row.opt_text(0)?,
                    exit_reason: row.opt_text(1)?,
                }),
                None => Ok(WorkflowNodeStatus {
                    status: None,
                    exit_reason: None,
                }),
            }
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The set of call run ids that still hold a `workflow_call` link — the
/// in-flight/undelivered calls. Read from the private db (the link table never
/// replicates); the caller intersects it with the node's own calls.
async fn undelivered_call_run_ids(
    private_db: &LocalDb,
) -> Result<std::collections::HashSet<String>, String> {
    private_db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query("SELECT run_id FROM workflow_call", ()).await?;
                let mut out = std::collections::HashSet::new();
                while let Some(row) = rows.next().await? {
                    out.insert(row.text(0)?);
                }
                Ok(out)
            })
        })
        .await
        .map_err(|error| error.to_string())
}

/// The workflow node's child calls — ONE row per call job, reflecting its LATEST
/// run. A call restart spawns a fresh run under the same job (CAIRN-2516), so
/// selecting the latest run collapses the predecessor and replacement into a
/// single row that updates in place, rather than showing two rows that both
/// drill into the same job. It also keeps a restarted call from double-counting
/// toward its phase total. Each row is joined to its job for the model and
/// sub-queries the token/tool rollups keyed by run.
async fn list_calls(db: &LocalDb, node_job_id: &str) -> Result<Vec<WorkflowCall>, String> {
    let node_job_id = node_job_id.to_string();
    db.read(|conn| {
        let node_job_id = node_job_id.clone();
        Box::pin(async move {
            // `tok` LEFT JOIN folds the run's `token_rollup` rows once instead of
            // several correlated subselects; the component sums back the display
            // tooltip's composition breakdown.
            let mut rows = conn
                .query(
                    "SELECT r.id, r.label, r.phase, j.model, r.status, r.started_at, r.exited_at, \
                            COALESCE(tok.billable, 0) AS tokens, \
                            COALESCE(tok.input_tokens, 0) AS input_tokens, \
                            COALESCE(tok.cache_read, 0) AS cache_read_tokens, \
                            COALESCE(tok.cache_create, 0) AS cache_create_tokens, \
                            COALESCE(tok.output_tokens, 0) AS output_tokens, \
                            (SELECT COUNT(*) FROM tool_invocations ti WHERE ti.run_id = r.id) AS tools, \
                            j.id, r.exit_reason \
                     FROM runs r \
                     JOIN jobs j ON r.job_id = j.id \
                     LEFT JOIN ( \
                         SELECT run_id, \
                                SUM(billable_tokens) AS billable, \
                                SUM(input_tokens) AS input_tokens, \
                                SUM(cache_read_tokens) AS cache_read, \
                                SUM(cache_create_tokens) AS cache_create, \
                                SUM(output_tokens) AS output_tokens \
                         FROM token_rollup GROUP BY run_id \
                     ) tok ON tok.run_id = r.id \
                     WHERE j.parent_job_id = ?1 \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM runs r2 WHERE r2.job_id = r.job_id \
                           AND (r2.created_at > r.created_at \
                                OR (r2.created_at = r.created_at AND r2.rowid > r.rowid)) \
                       ) \
                     ORDER BY j.created_at ASC, r.created_at ASC",
                    params![node_job_id.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(WorkflowCall {
                    run_id: row.text(0)?,
                    label: row.opt_text(1)?,
                    phase: row.opt_text(2)?,
                    model: row.opt_text(3)?,
                    status: row.opt_text(4)?,
                    started_at: row.opt_i64(5)?,
                    exited_at: row.opt_i64(6)?,
                    tokens: row.i64(7)?,
                    input_tokens: row.i64(8)?,
                    cache_read_tokens: row.i64(9)?,
                    cache_create_tokens: row.i64(10)?,
                    output_tokens: row.i64(11)?,
                    tools: row.i64(12)?,
                    job_id: row.text(13)?,
                    exit_reason: row.opt_text(14)?,
                    // Filled by `get_workflow_monitor` from the private-db link set.
                    undelivered: false,
                });
            }
            Ok(out)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::super::append_entry;
    use super::*;
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
    use tempfile::tempdir;

    fn call(phase: &str, status: &str, started: i64) -> WorkflowCall {
        WorkflowCall {
            run_id: format!("run-{phase}-{started}"),
            job_id: format!("job-{phase}-{started}"),
            label: None,
            phase: Some(phase.to_string()),
            model: None,
            status: Some(status.to_string()),
            started_at: Some(started),
            exited_at: None,
            tokens: 0,
            input_tokens: 0,
            cache_read_tokens: 0,
            cache_create_tokens: 0,
            output_tokens: 0,
            tools: 0,
            exit_reason: None,
            undelivered: false,
        }
    }

    fn phase_entry(name: &str, ts: i64, seq: i64) -> ProgressEntry {
        ProgressEntry {
            seq,
            kind: "phase".into(),
            text: name.into(),
            created_at: ts,
        }
    }

    #[test]
    fn spine_attributes_calls_to_declared_phases_with_done_total() {
        let calls = vec![
            call("scope", "exited", 100),
            call("search", "exited", 200),
            call("search", "live", 210),
        ];
        let entries = vec![phase_entry("scope", 90, 0), phase_entry("search", 190, 1)];
        let spine = build_phase_spine(&calls, &entries);
        assert_eq!(spine.len(), 2);
        assert_eq!(spine[0].name, "scope");
        assert_eq!((spine[0].total, spine[0].done), (1, 1));
        assert_eq!(spine[0].started_at, Some(90));
        assert_eq!(spine[1].name, "search");
        assert_eq!((spine[1].total, spine[1].done), (2, 1));
        // At least one call attributes to a named phase. Guards CAIRN-2499
        // comment #2: a phase()/agent(phase:) rename that silently zeroed the
        // spine would trip this.
        assert!(spine.iter().any(|p| p.total > 0));
    }

    #[test]
    fn call_only_phase_appears_after_declared_ones() {
        // deep-research's "fetch" carries a phase TAG but has no phase() boundary.
        let calls = vec![
            call("search", "exited", 200),
            call("fetch", "exited", 220),
            call("fetch", "exited", 230),
        ];
        let entries = vec![phase_entry("search", 190, 0)];
        let spine = build_phase_spine(&calls, &entries);
        assert_eq!(
            spine.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            vec!["search", "fetch"]
        );
        let fetch = spine.iter().find(|p| p.name == "fetch").unwrap();
        assert_eq!((fetch.total, fetch.done), (2, 2));
        // A call-only phase derives its start from its earliest call.
        assert_eq!(fetch.started_at, Some(220));
    }

    #[test]
    fn declared_phase_with_no_calls_shows_zero() {
        let entries = vec![phase_entry("synthesize", 500, 0)];
        let spine = build_phase_spine(&[], &entries);
        assert_eq!(spine.len(), 1);
        assert_eq!((spine[0].total, spine[0].done), (0, 0));
    }

    async fn test_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("workflow-monitor.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn exec(db: &LocalDb, sql: &str) {
        let sql = sql.to_string();
        db.write(move |conn| {
            let sql = sql.clone();
            Box::pin(async move {
                conn.execute(sql.as_str(), ()).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// A restarted call has two runs under one job; the monitor shows ONE row
    /// (the latest run), so a restart updates in place instead of spawning a
    /// duplicate row that drills into the same job (CAIRN-2516).
    #[tokio::test]
    async fn collapses_multiple_runs_per_call_to_latest_run() {
        let db = test_db().await;
        exec(
            &db,
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('ws','ws',0,0)",
        )
        .await;
        exec(&db, "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('proj','ws','P','P','/tmp',0,0)").await;
        exec(&db, "INSERT INTO jobs (id, project_id, status, created_at, updated_at) VALUES ('wf-node','proj','live',0,0)").await;
        exec(&db, "INSERT INTO jobs (id, project_id, parent_job_id, model, status, created_at, updated_at) VALUES ('call','proj','wf-node','haiku','running',1,1)").await;
        // Predecessor (killed by restart) then the replacement, newer.
        exec(&db, "INSERT INTO runs (id, job_id, project_id, status, exit_reason, started_at, exited_at, created_at, updated_at, label, phase) VALUES ('r-old','call','proj','exited','user_stop',100,105,1,1,'synth','synthesize')").await;
        exec(&db, "INSERT INTO runs (id, job_id, project_id, status, started_at, created_at, updated_at, label, phase) VALUES ('r-new','call','proj','live',110,2,2,'synth','synthesize')").await;

        let mon = get_workflow_monitor(&db, &db, "wf-node").await.unwrap();
        // One row for the call, and it is the latest (live) run — not the killed one.
        assert_eq!(mon.calls.len(), 1);
        assert_eq!(mon.calls[0].run_id, "r-new");
        assert_eq!(mon.calls[0].status.as_deref(), Some("live"));
        assert_eq!(mon.calls[0].exit_reason, None);
        // The phase counts the call once, not once per run.
        let synth = mon.phases.iter().find(|p| p.name == "synthesize").unwrap();
        assert_eq!((synth.total, synth.done), (1, 0));
    }

    #[tokio::test]
    async fn monitor_aggregates_tokens_tools_duration_status_and_spine() {
        let db = test_db().await;
        exec(
            &db,
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('ws', 'ws', 0, 0)",
        )
        .await;
        exec(&db, "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('proj', 'ws', 'P', 'P', '/tmp', 0, 0)").await;
        // The workflow node job plus two child call jobs.
        exec(&db, "INSERT INTO jobs (id, project_id, status, created_at, updated_at) VALUES ('wf-node', 'proj', 'live', 0, 0)").await;
        exec(&db, "INSERT INTO jobs (id, project_id, parent_job_id, model, status, created_at, updated_at) VALUES ('call-a', 'proj', 'wf-node', 'haiku', 'exited', 1, 1)").await;
        exec(&db, "INSERT INTO jobs (id, project_id, parent_job_id, model, status, created_at, updated_at) VALUES ('call-b', 'proj', 'wf-node', 'sonnet', 'live', 2, 2)").await;
        // A run per call: run-a terminated, run-b still live.
        exec(&db, "INSERT INTO runs (id, job_id, project_id, status, started_at, exited_at, created_at, updated_at, label, phase) VALUES ('run-a', 'call-a', 'proj', 'exited', 100, 160, 1, 1, 'search:x', 'search')").await;
        exec(&db, "INSERT INTO runs (id, job_id, project_id, status, started_at, exited_at, created_at, updated_at, label, phase) VALUES ('run-b', 'call-b', 'proj', 'live', 200, NULL, 2, 2, 'search:y', 'search')").await;
        // Two rollup rows for run-a to prove the component SUMs (and thus the
        // priced cost) fold across rows; none for run-b. billable_tokens equals
        // input + cache_read + cache_create + output per row, mirroring the
        // backend-shaped Claude billable rule the fold writes.
        exec(&db, "INSERT INTO token_rollup (id, bucket_start, run_id, backend, input_tokens, cache_read_tokens, cache_create_tokens, output_tokens, billable_tokens) VALUES ('tr1', 0, 'run-a', 'claude', 100, 800, 0, 100, 1000)").await;
        exec(&db, "INSERT INTO token_rollup (id, bucket_start, run_id, backend, input_tokens, cache_read_tokens, cache_create_tokens, output_tokens, billable_tokens) VALUES ('tr2', 0, 'run-a', 'claude', 50, 400, 0, 50, 500)").await;
        // Two tool invocations for run-a; none for run-b.
        exec(&db, "INSERT INTO tool_invocations (id, event_id, run_id, ts, verb, tool_name, target_scheme, target_kind) VALUES ('ti1', 'e1', 'run-a', 0, 'read', 'read', 'file', 'ext')").await;
        exec(&db, "INSERT INTO tool_invocations (id, event_id, run_id, ts, verb, tool_name, target_scheme, target_kind) VALUES ('ti2', 'e2', 'run-a', 0, 'run', 'run', 'shell', 'cmd')").await;

        // Mark run-b's call in-flight via a workflow_call link (private table).
        exec(&db, "INSERT INTO workflow_call (run_id, workflow_run_id, ordinal, prompt_hash, created_at) VALUES ('run-b', 'wf-node-run', 1, 'h', 0)").await;
        // The workflow node's own run: live (drives the header status).
        exec(&db, "INSERT INTO runs (id, job_id, project_id, status, created_at, updated_at) VALUES ('wf-node-run', 'wf-node', 'proj', 'live', 0, 0)").await;

        let mon = get_workflow_monitor(&db, &db, "wf-node").await.unwrap();
        assert_eq!(mon.calls.len(), 2);
        // The node's own run status surfaces on the header.
        assert_eq!(mon.workflow.status.as_deref(), Some("live"));
        assert_eq!(mon.workflow.exit_reason, None);
        // run-b holds a live link -> undelivered; run-a's link is absent.
        assert!(
            mon.calls
                .iter()
                .find(|c| c.run_id == "run-b")
                .unwrap()
                .undelivered
        );
        assert!(
            !mon.calls
                .iter()
                .find(|c| c.run_id == "run-a")
                .unwrap()
                .undelivered
        );

        let a = mon.calls.iter().find(|c| c.run_id == "run-a").unwrap();
        assert_eq!(a.job_id, "call-a");
        assert_eq!(a.tokens, 1500, "SUM of both rollup rows");
        // Component sums fold across both rows.
        assert_eq!(a.input_tokens, 150);
        assert_eq!(a.cache_read_tokens, 1200);
        assert_eq!(a.cache_create_tokens, 0);
        assert_eq!(a.output_tokens, 150);
        assert_eq!(a.tools, 2);
        assert_eq!(a.model.as_deref(), Some("haiku"));
        assert_eq!(a.status.as_deref(), Some("exited"));
        assert_eq!(a.phase.as_deref(), Some("search"));
        assert_eq!(a.label.as_deref(), Some("search:x"));
        assert_eq!((a.started_at, a.exited_at), (Some(100), Some(160)));

        let b = mon.calls.iter().find(|c| c.run_id == "run-b").unwrap();
        assert_eq!((b.tokens, b.tools), (0, 0), "no rollups for the live run");
        assert_eq!(b.exited_at, None);

        // No phase() entries yet -> the spine is derived purely from call tags.
        let search = mon.phases.iter().find(|p| p.name == "search").unwrap();
        assert_eq!(
            (search.total, search.done),
            (2, 1),
            "two search calls, one terminated"
        );

        // Adding phase()/log() entries surfaces the boundary timestamp + logs.
        append_entry(&db, "wf-node", "phase", "search")
            .await
            .unwrap();
        append_entry(&db, "wf-node", "log", "2 angles")
            .await
            .unwrap();
        let mon = get_workflow_monitor(&db, &db, "wf-node").await.unwrap();
        assert_eq!(mon.phases.len(), 1, "declared + call-tag phase de-duped");
        assert_eq!((mon.phases[0].total, mon.phases[0].done), (2, 1));
        assert_eq!(mon.logs.len(), 1);
        assert_eq!(mon.logs[0].text, "2 angles");
    }

    /// The panel's identity line: the workflow id is parsed from the node job's
    /// `"Workflow: {id}"` description (durable), and the invocation args come from
    /// the private `workflow_run` record while it lives — dropping to `None` once
    /// the record is deleted on clean completion, while the id survives.
    #[tokio::test]
    async fn monitor_surfaces_workflow_identity() {
        let db = test_db().await;
        exec(
            &db,
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('ws','ws',0,0)",
        )
        .await;
        exec(&db, "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('proj','ws','P','P','/tmp',0,0)").await;
        exec(&db, "INSERT INTO jobs (id, project_id, agent_config_id, task_description, status, created_at, updated_at) VALUES ('wf-node','proj','workflow','Workflow: deep-research','live',0,0)").await;
        exec(&db, "INSERT INTO workflow_run (run_id, job_id, session_id, workflow_id, script_path, args_json, working_dir, output_name, created_at) VALUES ('wf-run','wf-node','sess','deep-research','/wf/main.ts','{\"query\":\"rust async\"}','/tmp/wf','return',0)").await;

        let mon = get_workflow_monitor(&db, &db, "wf-node").await.unwrap();
        assert_eq!(mon.identity.workflow_id.as_deref(), Some("deep-research"));
        assert_eq!(
            mon.identity.args.as_deref(),
            Some("{\"query\":\"rust async\"}")
        );

        // Clean completion drops the transient record: the id (durable, from the
        // job description) survives; the args go `None`.
        exec(&db, "DELETE FROM workflow_run WHERE job_id = 'wf-node'").await;
        let mon = get_workflow_monitor(&db, &db, "wf-node").await.unwrap();
        assert_eq!(mon.identity.workflow_id.as_deref(), Some("deep-research"));
        assert_eq!(mon.identity.args, None);
    }

    /// A non-workflow job (no `"Workflow: "` description, no record) yields an
    /// all-`None` identity rather than mislabeling itself.
    #[tokio::test]
    async fn monitor_identity_is_empty_for_non_workflow_job() {
        let db = test_db().await;
        exec(
            &db,
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('ws','ws',0,0)",
        )
        .await;
        exec(&db, "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('proj','ws','P','P','/tmp',0,0)").await;
        exec(&db, "INSERT INTO jobs (id, project_id, status, created_at, updated_at) VALUES ('plain','proj','live',0,0)").await;
        let mon = get_workflow_monitor(&db, &db, "plain").await.unwrap();
        assert_eq!(mon.identity, WorkflowIdentity::default());
    }
}
