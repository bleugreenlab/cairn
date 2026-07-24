//! Job-status recompute: the single writer of `job.status`.
//!
//! Job status is a projection (see [`crate::transitions::projection`]). This
//! module gathers the facts a job's status derives from and writes the derived
//! value as a cache, exactly as execution/issue status are recomputed beneath it.
//!
//! - [`recompute_job_status_conn`] gathers facts for one recipe-backed job and
//!   derives its status without writing.
//! - [`recompute_execution_jobs_conn`] sweeps every job in an execution in
//!   topological order, writes the changed caches, recomputes execution + issue
//!   status, and returns the diffs.
//! - [`recompute_execution_jobs`] runs the sweep in a write transaction and
//!   emits the follow-on effects (lifecycle messages, wakes, JobEnded, DAG
//!   advancement) keyed by each derived edge. This is the single entry
//!   point every former decider now calls.

use super::*;
use crate::models::{RecipeNode, RecipeNodeType, TriggerEvent, TurnState};
use crate::transitions::outcome::{recompute_execution_status_conn, recompute_issue_status_conn};
use crate::transitions::projection::{derive_job_status, CheckpointGate, JobFacts, Resolution};
use std::collections::{HashMap, VecDeque};

/// A job whose derived status differs from its cached status.
#[derive(Debug, Clone)]
pub struct JobStatusChange {
    job_id: String,
    pub from: JobStatus,
    to: JobStatus,
    issue_id: Option<String>,
    project_id: String,
    parent_job_id: Option<String>,
    /// Carried so the scoped `jobs` db-change can invalidate the parent batch's
    /// child-job query (`["child-jobs", parentToolUseId]`) without a cache scan.
    parent_tool_use_id: Option<String>,
    /// When `to == Blocked` because the node ended its turn without its declared
    /// output, a human-readable reason naming the missing artifact. None for
    /// other Blocked causes (approval gate) and for non-Blocked changes.
    blocked_reason: Option<String>,
}

impl JobStatusChange {
    /// JobEnded fires for a top-level job reaching a terminal status.
    /// Delegated children stay silent because they have a parent job.
    fn fires_job_ended(&self) -> bool {
        matches!(self.to, JobStatus::Complete | JobStatus::Failed) && self.parent_job_id.is_none()
    }

    /// Missing required output is an internal Blocked gate, not a user-facing
    /// lifecycle event. Other Blocked causes (approval/checkpoint gates) still
    /// emit the normal blocked message.
    fn emits_blocked_lifecycle_message(&self) -> bool {
        self.to == JobStatus::Blocked && self.blocked_reason.is_none()
    }
}

struct SweepJob {
    id: String,
    recipe_node_id: Option<String>,
    parent_job_id: Option<String>,
    status: JobStatus,
    issue_id: Option<String>,
    project_id: String,
    parent_tool_use_id: Option<String>,
}

/// The checkpoint gate for a node. A standalone checkpoint node runs a command
/// gate; every other node's gate is its terminal (`context-out`) target's confirm
/// policy — `User` blocks once the turn completes until the artifact is confirmed,
/// `Auto` (or no contract) never blocks. The PR auto-confirm override (CAIRN-1219)
/// is already baked into the resolver: a `pr` target forces `Auto`, so a
/// PR-feeding node resolves to `None` here.
fn gate_for(
    node: Option<&RecipeNode>,
    confirm_policy: Option<crate::models::ConfirmPolicy>,
) -> CheckpointGate {
    match node {
        Some(n) if n.node_type == RecipeNodeType::Checkpoint => CheckpointGate::Command,
        _ => match confirm_policy {
            Some(crate::models::ConfirmPolicy::User) => CheckpointGate::ConfirmGate,
            _ => CheckpointGate::None,
        },
    }
}

/// The latest *work* turn state for a job, used to derive `live_turn` /
/// The latest *work* turn state for a job, used to derive the terminal OUTCOME
/// (`turn_complete` / `turn_failed`). The post-completion memory-review turn
/// (`start_reason = 'memory_review'`) is excluded here so a failed review turn
/// never fails the job, and a completed review turn never stands in for the work
/// turn's outcome. Liveness is computed separately by [`latest_turn_is_live`],
/// which *includes* the review turn so the job reads Running while the review
/// agent is active. Confirmation is decoupled from run state — it keys off the
/// unconfirmed artifact, not the job status — so a live review no longer hides
/// the confirm affordance. CAIRN-1576.
async fn latest_turn_state(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<Option<TurnState>> {
    let mut rows = conn
        .query(
            "SELECT state FROM turns
             WHERE job_id = ?1 AND start_reason != 'memory_review'
             ORDER BY created_at DESC, sequence DESC LIMIT 1",
            (job_id,),
        )
        .await?;
    Ok(rows
        .next()
        .await?
        .map(|r| r.text(0))
        .transpose()?
        .and_then(|s| s.parse::<TurnState>().ok()))
}

/// Whether the job's absolute latest turn is live (Running/Pending/Yielded),
/// *including* a memory-review turn. Liveness includes the review so the job
/// stays Running while the post-completion review agent is active; the terminal
/// outcome still derives from the work turn via [`latest_turn_state`].
pub(super) async fn latest_turn_is_live(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT state FROM turns WHERE job_id = ?1 ORDER BY created_at DESC, sequence DESC LIMIT 1",
            (job_id,),
        )
        .await?;
    let state = rows
        .next()
        .await?
        .map(|r| r.text(0))
        .transpose()?
        .and_then(|s| s.parse::<TurnState>().ok());
    Ok(matches!(
        state,
        Some(TurnState::Running | TurnState::Pending | TurnState::Yielded)
    ))
}

async fn latest_artifact_confirmed(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT confirmed FROM artifacts WHERE job_id = ?1 ORDER BY version DESC LIMIT 1",
            (job_id,),
        )
        .await?;
    Ok(rows
        .next()
        .await?
        .map(|r| r.i64(0))
        .transpose()?
        .map(|v| v != 0)
        .unwrap_or(false))
}

/// The confirmation of the job's *terminal* artifact, scoped to its name. A
/// `context-self` living doc writes under its own name and must never resolve the
/// terminal gate, so the confirm flag is read from the terminal artifact chain
/// only. Falls back to the job-scoped latest when the contract is unnamed (a
/// command checkpoint, whose seeded artifact has a NULL `output_name`).
async fn latest_artifact_confirmed_for(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    required_name: Option<&str>,
) -> DbResult<bool> {
    match required_name {
        Some(name) => {
            let mut rows = conn
                .query(
                    "SELECT confirmed FROM artifacts WHERE job_id = ?1 AND output_name = ?2 ORDER BY version DESC LIMIT 1",
                    params![job_id, name],
                )
                .await?;
            Ok(rows
                .next()
                .await?
                .map(|r| r.i64(0))
                .transpose()?
                .map(|v| v != 0)
                .unwrap_or(false))
        }
        None => latest_artifact_confirmed(conn, job_id).await,
    }
}

/// Stored status of the latest run for a job (newest by `created_at`, `rowid`),
/// or `None` when no run exists yet. The turn-less completion path for an
/// ephemeral call derives from this (a call establishes no DB turn).
async fn latest_run_status_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT status FROM runs WHERE job_id = ?1 ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (job_id,),
        )
        .await?;
    rows.next().await?.map(|r| r.text(0)).transpose()
}

/// Terminal stored run statuses (the process has exited). Mirrors
/// `RunStatus::is_terminal` plus the legacy stored spellings.
fn is_terminal_run_status_str(status: &str) -> bool {
    matches!(
        status,
        "exited" | "crashed" | "complete" | "completed" | "failed"
    )
}

/// Whether any artifact row exists for the job — i.e. the node produced its
/// declared output. Distinct from `latest_artifact_confirmed`, which conflates
/// "no artifact" with "artifact awaiting confirmation."
async fn latest_artifact_present(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT 1 FROM artifacts WHERE job_id = ?1 LIMIT 1",
            (job_id,),
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

/// Whether the job has produced its *required* output artifact. When the output
/// contract names a specific artifact, only that artifact counts — a stray
/// checkpoint/other artifact for the same job must not satisfy the gate. The
/// agent writes to `cairn:~/<name>`, which is stored as the artifact's
/// `output_name` (see `store_artifact`), so `output_name` is the contract's
/// `artifact_name`. Falls back to any-artifact presence only when the contract
/// is unnamed (no resolvable `artifact_name`).
async fn artifact_present_for(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    required_name: Option<&str>,
) -> DbResult<bool> {
    match required_name {
        Some(name) => {
            let mut rows = conn
                .query(
                    "SELECT 1 FROM artifacts WHERE job_id = ?1 AND output_name = ?2 LIMIT 1",
                    params![job_id, name],
                )
                .await?;
            Ok(rows.next().await?.is_some())
        }
        None => latest_artifact_present(conn, job_id).await,
    }
}

/// Record a job's failure as a turn fact so the projection derives `Failed`.
///
/// Failure is a *turn* truth, not a job-status truth (the two-axis split). If the
/// latest turn is still live it is failed in place; if it is already terminal
/// (or absent), a synthetic terminal `failed` turn is appended so the projection
/// reads `turn_failed`. Used by `mark_job_failed` and the no-recovery checkpoint
/// rejection path — cases where the job has no recovery edge and must go terminal.
pub(crate) async fn force_fail_job_turn_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<()> {
    let now = chrono::Utc::now().timestamp();
    let latest = {
        let mut rows = conn
            .query(
                "SELECT id, state, session_id, sequence
                 FROM turns WHERE job_id = ?1 ORDER BY created_at DESC, sequence DESC LIMIT 1",
                (job_id,),
            )
            .await?;
        rows.next()
            .await?
            .map(|row| Ok::<_, DbError>((row.text(0)?, row.text(1)?, row.text(2)?, row.i64(3)?)))
            .transpose()?
    };

    match latest {
        Some((turn_id, state, _session, _seq))
            if !state
                .parse::<TurnState>()
                .map(|s| s.is_terminal())
                .unwrap_or(false) =>
        {
            // Live turn: fail it in place.
            conn.execute(
                "UPDATE turns SET state = 'failed', ended_at = ?1, updated_at = ?2 WHERE id = ?3",
                params![now, now, turn_id.as_str()],
            )
            .await?;
        }
        other => {
            // Terminal or absent: append a synthetic failed turn so the latest
            // turn outcome is Failed. Carries the job's session if it has one.
            let (session_id, predecessor_id, sequence) = match other {
                Some((turn_id, _state, session, seq)) => (session, Some(turn_id), seq + 1),
                None => {
                    let session = {
                        let mut rows = conn
                            .query(
                                "SELECT current_session_id FROM jobs WHERE id = ?1",
                                (job_id,),
                            )
                            .await?;
                        rows.next()
                            .await?
                            .and_then(|row| row.opt_text(0).ok().flatten())
                            .unwrap_or_else(|| format!("rejected-{job_id}"))
                    };
                    (session, None, 1)
                }
            };
            let turn_id = ids::mint_child(job_id);
            conn.execute(
                "INSERT INTO turns (id, session_id, job_id, sequence, predecessor_id, state,
                                    start_reason, created_at, ended_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'failed', 'retry', ?6, ?6, ?6)",
                params![
                    turn_id.as_str(),
                    session_id.as_str(),
                    job_id,
                    sequence,
                    predecessor_id.as_deref(),
                    now
                ],
            )
            .await?;
        }
    }
    Ok(())
}

async fn execution_issue_id_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT issue_id FROM executions WHERE id = ?1",
            (execution_id,),
        )
        .await?;
    crate::storage::next_opt_text(&mut rows, 0).await
}

async fn load_sweep_jobs(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
) -> DbResult<Vec<SweepJob>> {
    let mut rows = conn
        .query(
            "SELECT id, recipe_node_id, parent_job_id, status, issue_id, project_id,
                    completed_at, parent_tool_use_id
             FROM jobs WHERE execution_id = ?1",
            (execution_id,),
        )
        .await?;
    let mut jobs = Vec::new();
    while let Some(row) = rows.next().await? {
        let status: JobStatus = row
            .text(3)?
            .parse()
            .map_err(|e: String| DbError::internal(e))?;
        jobs.push(SweepJob {
            id: row.text(0)?,
            recipe_node_id: row.opt_text(1)?,
            parent_job_id: row.opt_text(2)?,
            status,
            issue_id: row.opt_text(4)?,
            project_id: row.text(5)?,
            parent_tool_use_id: row.opt_text(7)?,
        });
    }
    Ok(jobs)
}

async fn write_job_status(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    from: &JobStatus,
    to: &JobStatus,
) -> DbResult<()> {
    let now = chrono::Utc::now().timestamp();
    match to {
        // Entering Running from Pending is a start — stamp started_at.
        JobStatus::Running if *from == JobStatus::Pending => {
            conn.execute(
                "UPDATE jobs SET status = ?1, started_at = ?2, updated_at = ?3 WHERE id = ?4",
                params![to.to_string(), now, now, job_id],
            )
            .await?;
        }
        JobStatus::Complete | JobStatus::Failed => {
            conn.execute(
                "UPDATE jobs SET status = ?1, completed_at = ?2, updated_at = ?3 WHERE id = ?4",
                params![to.to_string(), now, now, job_id],
            )
            .await?;
        }
        _ => {
            conn.execute(
                "UPDATE jobs SET status = ?1, updated_at = ?2 WHERE id = ?3",
                params![to.to_string(), now, job_id],
            )
            .await?;
        }
    }
    Ok(())
}

/// Soft invariant check. Derivation is truth, so a surprising edge is logged for
/// observability rather than blocking the write. Deliberately distinct text from
/// the legacy "Invalid job transition" assertion.
fn check_reachable(from: &JobStatus, to: &JobStatus, job_id: &str) {
    use JobStatus::*;
    let reachable = matches!(
        (from, to),
        (Pending, Running | Blocked | Failed | Complete | Idle)
            | (Running, Complete | Failed | Blocked | Pending | Idle)
            | (Blocked, Complete | Failed | Pending | Running | Idle)
            | (Complete, Running | Pending)
            | (Failed, Running | Pending)
            | (Idle, Running | Pending | Complete | Failed | Blocked)
    );
    if !reachable {
        log::warn!("job-status projection: unexpected transition {from} -> {to} for {job_id}");
    }
}

fn topo_order(node_ids: &[&str], control: &[(String, String)]) -> Vec<String> {
    let mut indeg: HashMap<&str, usize> = node_ids.iter().map(|id| (*id, 0usize)).collect();
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for (s, t) in control {
        if indeg.contains_key(s.as_str()) && indeg.contains_key(t.as_str()) {
            adj.entry(s.as_str()).or_default().push(t.as_str());
            *indeg.get_mut(t.as_str()).unwrap() += 1;
        }
    }
    let mut queue: VecDeque<&str> = node_ids
        .iter()
        .filter(|id| indeg.get(**id).copied().unwrap_or(0) == 0)
        .copied()
        .collect();
    let mut order: Vec<String> = Vec::new();
    while let Some(n) = queue.pop_front() {
        order.push(n.to_string());
        if let Some(ts) = adj.get(n) {
            for t in ts.clone() {
                let d = indeg.get_mut(t).unwrap();
                *d -= 1;
                if *d == 0 {
                    queue.push_back(t);
                }
            }
        }
    }
    // Cycle/disconnected safety: append any node the topo walk did not emit.
    for id in node_ids {
        if !order.iter().any(|o| o == id) {
            order.push((*id).to_string());
        }
    }
    order
}

/// Recompute one recipe-backed job's status from its facts without writing. The
/// execution sweep computes the same facts in bulk.
pub async fn recompute_job_status_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<JobStatus> {
    let jobs_one = {
        let mut rows = conn
            .query(
                "SELECT execution_id, recipe_node_id, status
                 FROM jobs WHERE id = ?1",
                (job_id,),
            )
            .await?;
        rows.next()
            .await?
            .map(|row| Ok::<_, DbError>((row.opt_text(0)?, row.opt_text(1)?, row.text(2)?)))
            .transpose()?
    };
    let Some((execution_id, recipe_node_id, current_status)) = jobs_one else {
        return Err(DbError::internal(format!("job not found: {job_id}")));
    };

    // Cancelled is an explicit, sticky override (a node removed from the snapshot),
    // not a derived projection. Never re-derive it back to a live status.
    if current_status == "cancelled" {
        return Ok(JobStatus::Cancelled);
    }

    let turn = latest_turn_state(conn, job_id).await?;
    let live_turn = latest_turn_is_live(conn, job_id).await?;

    let execution_id = execution_id
        .ok_or_else(|| DbError::internal(format!("job has no execution_id: {job_id}")))?;
    let recipe_node_id = recipe_node_id
        .ok_or_else(|| DbError::internal(format!("job has no recipe_node_id: {job_id}")))?;

    let snapshot = load_execution_snapshot_conn(conn, &execution_id).await?;
    let all_nodes_db = snapshot_nodes_to_db(&snapshot);
    let all_edges_db = snapshot_edges_to_db(&snapshot);
    let node_map_db: HashMap<&str, &DbRecipeNode> =
        all_nodes_db.iter().map(|n| (n.id.as_str(), n)).collect();
    let node = snapshot
        .recipe
        .nodes
        .iter()
        .find(|node| node.id == recipe_node_id);

    let dag_ready = is_action_node_ready_with_snapshot_conn(
        conn,
        &execution_id,
        &recipe_node_id,
        &all_edges_db,
        &node_map_db,
    )
    .await?;

    let upstream_failed =
        upstream_failed_conn(conn, &execution_id, &recipe_node_id, &snapshot).await?;

    let downstream_schema =
        crate::execution::jobs::find_downstream_artifact_schema_with_snapshot_conn(
            conn,
            &snapshot,
            &recipe_node_id,
        )
        .await?;
    let requires_output = downstream_schema.is_some();
    let confirm_policy = downstream_schema
        .as_ref()
        .map(|schema| schema.confirm_policy);
    let required_artifact_name = downstream_schema
        .as_ref()
        .and_then(|schema| schema.artifact_name.clone());
    // Confirmation is scoped to the terminal artifact name: a ctx-self living
    // doc's confirm flag must never resolve the job's gate.
    let confirmed =
        latest_artifact_confirmed_for(conn, job_id, required_artifact_name.as_deref()).await?;
    let artifact_present =
        artifact_present_for(conn, job_id, required_artifact_name.as_deref()).await?;
    let facts = JobFacts {
        dag_ready,
        upstream_failed,
        live_turn,
        turn_failed: matches!(
            turn,
            // Interrupt/cancel is a user-initiated pause, NOT a failure: the
            // job rests resumable (derives Pending) and never cascades to
            // downstream. Only a genuine agent/process failure terminalizes.
            Some(TurnState::Failed)
        ),
        turn_complete: matches!(turn, Some(TurnState::Complete)),
        checkpoint: gate_for(node, confirm_policy),
        resolution: if confirmed {
            Resolution::Confirmed
        } else {
            Resolution::Pending
        },
        requires_output,
        artifact_present,
        long_running: crate::execution::jobs::is_long_running_node(
            &snapshot,
            &recipe_node_id,
            requires_output,
        ),
    };
    let derived = derive_job_status(&facts);
    Ok(derived)
}

async fn upstream_failed_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
    node_id: &str,
    snapshot: &crate::models::ExecutionSnapshot,
) -> DbResult<bool> {
    let parents: Vec<&str> = snapshot
        .recipe
        .edges
        .iter()
        .filter(|e| e.edge_type.to_string() == "control" && e.target_node_id == node_id)
        .map(|e| e.source_node_id.as_str())
        .collect();
    for parent in parents {
        let mut rows = conn
            .query(
                "SELECT 1 FROM jobs
                 WHERE execution_id = ?1 AND recipe_node_id = ?2 AND status = 'failed'
                 LIMIT 1",
                params![execution_id, parent],
            )
            .await?;
        if rows.next().await?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Sweep every job in an execution in topological order, writing changed caches
/// and recomputing execution + issue status. Returns the diffs for effect emission.
pub async fn recompute_execution_jobs_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
) -> DbResult<Vec<JobStatusChange>> {
    let snapshot = load_execution_snapshot_conn(conn, execution_id).await?;
    let node_by_id: HashMap<&str, &RecipeNode> = snapshot
        .recipe
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n))
        .collect();
    let all_nodes_db = snapshot_nodes_to_db(&snapshot);
    let all_edges_db = snapshot_edges_to_db(&snapshot);
    let node_map_db: HashMap<&str, &DbRecipeNode> =
        all_nodes_db.iter().map(|n| (n.id.as_str(), n)).collect();

    let control: Vec<(String, String)> = snapshot
        .recipe
        .edges
        .iter()
        .filter(|e| e.edge_type.to_string() == "control")
        .map(|e| (e.source_node_id.clone(), e.target_node_id.clone()))
        .collect();
    let mut parents: HashMap<String, Vec<String>> = HashMap::new();
    for (s, t) in &control {
        parents.entry(t.clone()).or_default().push(s.clone());
    }

    let jobs = load_sweep_jobs(conn, execution_id).await?;
    let mut node_jobs: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, j) in jobs.iter().enumerate() {
        if let Some(nid) = &j.recipe_node_id {
            node_jobs.entry(nid.clone()).or_default().push(i);
        }
    }
    let mut status: HashMap<String, JobStatus> = jobs
        .iter()
        .map(|j| (j.id.clone(), j.status.clone()))
        .collect();

    let node_ids: Vec<&str> = snapshot
        .recipe
        .nodes
        .iter()
        .map(|n| n.id.as_str())
        .collect();
    let order = topo_order(&node_ids, &control);

    let mut changes = Vec::new();
    for node_id in &order {
        let Some(idxs) = node_jobs.get(node_id).cloned() else {
            continue;
        };
        for i in idxs {
            let j = &jobs[i];
            // Cancelled jobs are archived (node removed from the snapshot): leave
            // their status untouched and never let the projection re-derive them.
            if j.status == JobStatus::Cancelled {
                continue;
            }
            let upstream_failed = parents
                .get(node_id)
                .map(|ps| {
                    ps.iter().any(|p| {
                        node_jobs
                            .get(p)
                            .map(|js| {
                                js.iter()
                                    .any(|&k| status.get(&jobs[k].id) == Some(&JobStatus::Failed))
                            })
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false);

            let node = node_by_id.get(node_id.as_str()).copied();
            let dag_ready = is_action_node_ready_with_snapshot_conn(
                conn,
                execution_id,
                node_id,
                &all_edges_db,
                &node_map_db,
            )
            .await?;
            let turn = latest_turn_state(conn, &j.id).await?;
            // A node "requires output" when it has an effective output contract:
            // the schema of its single context-out edge target (an ArtifactNode,
            // or a downstream `pr`/action input port). This reuses the exact
            // resolver that tells the agent what to write, so the completion gate
            // and the instruction never drift.
            let downstream_schema =
                crate::execution::jobs::find_downstream_artifact_schema_with_snapshot_conn(
                    conn, &snapshot, node_id,
                )
                .await?;
            let requires_output = downstream_schema.is_some();
            let confirm_policy = downstream_schema
                .as_ref()
                .map(|schema| schema.confirm_policy);
            let required_artifact_name = downstream_schema
                .as_ref()
                .and_then(|schema| schema.artifact_name.clone());
            // Confirmation is scoped to the terminal artifact name so a ctx-self
            // living doc never resolves this gate.
            let confirmed =
                latest_artifact_confirmed_for(conn, &j.id, required_artifact_name.as_deref())
                    .await?;
            let artifact_present =
                artifact_present_for(conn, &j.id, required_artifact_name.as_deref()).await?;

            let facts = JobFacts {
                dag_ready,
                upstream_failed,
                live_turn: latest_turn_is_live(conn, &j.id).await?,
                turn_failed: matches!(
                    turn,
                    // Interrupt/cancel is a user-initiated pause, NOT a failure: the
                    // job rests resumable (derives Pending) and never cascades to
                    // downstream. Only a genuine agent/process failure terminalizes.
                    Some(TurnState::Failed)
                ),
                turn_complete: matches!(turn, Some(TurnState::Complete)),
                checkpoint: gate_for(node, confirm_policy),
                resolution: if confirmed {
                    Resolution::Confirmed
                } else {
                    Resolution::Pending
                },
                requires_output,
                artifact_present,
                long_running: crate::execution::jobs::is_long_running_node(
                    &snapshot,
                    node_id,
                    requires_output,
                ),
            };
            let derived = derive_job_status(&facts);
            let current = status
                .get(&j.id)
                .cloned()
                .unwrap_or_else(|| j.status.clone());
            // Never demote a claimed job back to Pending. Advancement claims a
            // ready job `pending→Running` synchronously, but its turn only goes
            // live a moment later (worktree setup, session start). A sweep landing
            // in that start-gap derives Pending (no live turn yet) — honor the
            // claim and leave it Running rather than un-starting and double-starting.
            if current == JobStatus::Running && derived == JobStatus::Pending {
                continue;
            }
            if derived != current {
                // C4: distinguish Blocked-on-missing-output from Blocked-for-
                // approval so the surfaced reason is actionable. The process is
                // warm/idle; writing the artifact recomputes it to Complete.
                let blocked_reason = if derived == JobStatus::Blocked
                    && facts.turn_complete
                    && facts.requires_output
                    && !facts.artifact_present
                {
                    Some(format!(
                        "agent ended its turn without producing required output{}",
                        required_artifact_name
                            .as_deref()
                            .map(|name| format!(": `{name}`"))
                            .unwrap_or_default()
                    ))
                } else {
                    None
                };
                check_reachable(&current, &derived, &j.id);
                write_job_status(conn, &j.id, &current, &derived).await?;
                status.insert(j.id.clone(), derived.clone());
                changes.push(JobStatusChange {
                    job_id: j.id.clone(),
                    from: current,
                    to: derived,
                    issue_id: j.issue_id.clone(),
                    project_id: j.project_id.clone(),
                    parent_job_id: j.parent_job_id.clone(),
                    parent_tool_use_id: j.parent_tool_use_id.clone(),
                    blocked_reason,
                });
            }
        }
    }

    recompute_execution_status_conn(conn, execution_id).await?;
    if let Some(issue_id) = execution_issue_id_conn(conn, execution_id).await? {
        recompute_issue_status_conn(conn, &issue_id).await?;
    }

    Ok(changes)
}

/// Recompute a single recipe-backed job by id by routing to its execution sweep.
/// This is the entry point for deciders that hold a `job_id` rather than an
/// `execution_id`. Jobs without an execution are historical data and are not part
/// of the live status projection.
pub fn recompute_job(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    let owning_db = run_advancement_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let db = owning_db.clone();
    let job_id_owned = job_id.to_string();
    let execution_id = run_advancement_db(async move {
        db.query_opt_text(
            "SELECT execution_id FROM jobs WHERE id = ?1",
            (job_id_owned,),
        )
        .await
        .map_err(|e| e.to_string())
    })?;

    let execution_id = execution_id.ok_or_else(|| {
        format!("Job {job_id} has no execution_id; standalone jobs are not recomputed")
    })?;
    recompute_execution_jobs(orch, &execution_id)
}

/// Terminalize a node-less delegated child job (an ephemeral call or a workflow)
/// directly from its turn + artifact facts.
///
/// The execution sweep ([`recompute_execution_jobs_conn`]) only reduces jobs
/// bound to a recipe node; it iterates the snapshot's recipe nodes and touches a
/// job only through its `recipe_node_id`. A delegated child that carries a
/// *pre-materialized* packet (`create_call_packet` — an ephemeral call or a
/// workflow) has `recipe_node_id IS NULL` and is never lowered into a synthetic
/// node, so it never appears in the sweep and [`recompute_job`] is a silent
/// no-op for it. Without this its job status stays `running` after its run
/// finalizes; its delegated packet (which resolves off the child's JOB status in
/// `refresh_packet_state`) then never reaches a terminal state, and a durably
/// suspended caller blocked on the resume gate hangs forever (CAIRN-2559).
///
/// This closes that gap: run it at the same turn-end sites as `recompute_job`,
/// immediately before `try_resume_delegated_parent`, so the resolved packet
/// wakes the caller in the same finalize pass. Returns the terminal status it
/// wrote, or `None` when there was nothing to do — a node-backed job (the
/// sweep's responsibility), a top-level job, an already-terminal child, or a
/// non-terminal outcome (a live turn, or a crash whose interrupted turn must
/// stay resumable so re-dispatch can replay).
async fn reduce_delegated_child_job_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<Option<JobStatus>> {
    let row = {
        let mut rows = conn
            .query(
                "SELECT recipe_node_id, parent_job_id, status, execution_id, \
                 agent_config_id, output_contract
                 FROM jobs WHERE id = ?1",
                (job_id,),
            )
            .await?;
        rows.next()
            .await?
            .map(|r| {
                Ok::<_, DbError>((
                    r.opt_text(0)?,
                    r.opt_text(1)?,
                    r.text(2)?,
                    r.opt_text(3)?,
                    r.opt_text(4)?,
                    r.opt_text(5)?,
                ))
            })
            .transpose()?
    };
    let Some((
        recipe_node_id,
        parent_job_id,
        status_str,
        execution_id,
        agent_config_id,
        output_contract_json,
    )) = row
    else {
        return Ok(None);
    };
    // Node-backed jobs (including the synthetic `delegated-*-agent` nodes the DAG
    // expander mints for Task-tool delegations) are the execution sweep's
    // responsibility. A pre-materialized node-less delegated CHILD needs this
    // direct reduction; so does a **standalone workflow** — a top-level node-less
    // workflow job (`parent_job_id IS NULL`, agent_config_id 'workflow') launched
    // from the UI with no caller (CAIRN-2651), which the sweep skips just the
    // same. A parent-less non-workflow top-level job stays the sweep's job.
    if recipe_node_id.is_some() {
        return Ok(None);
    }
    let is_standalone_workflow =
        parent_job_id.is_none() && agent_config_id.as_deref() == Some("workflow");
    if parent_job_id.is_none() && !is_standalone_workflow {
        return Ok(None);
    }
    let current: JobStatus = status_str.parse().map_err(DbError::internal)?;
    if matches!(
        current,
        JobStatus::Complete | JobStatus::Failed | JobStatus::Cancelled
    ) {
        return Ok(None);
    }

    // Reduce only a child that IS the result target of a pre-materialized
    // delegated packet (an ephemeral call or a workflow) — the exact set whose
    // resume gate keys on this job's status. A packet-less node-less child (a
    // `create_child_task` sub-agent) is left alone: it persists no output
    // contract yet is still owed a fixed `result` artifact, so reducing it off an
    // absent contract would derive `Complete` even when it never wrote `result`.
    // Reading the required artifact name from the packet's own contract keeps the
    // completion gate from ever drifting from the artifact the child must produce.
    let Some(execution_id) = execution_id else {
        return Ok(None);
    };
    // Resolve the required artifact name from the contract that owns it. A
    // delegated child reads it off its pre-materialized packet (whose resume gate
    // keys on this job's status); a standalone workflow reads it off its OWN
    // persisted `output_contract`, since it carries no packet (no caller to
    // resume). A packet-less node-less CHILD (a `create_child_task` sub-agent) is
    // still left alone: it persists no contract yet is owed a fixed `result`, so
    // reducing it off an absent contract would wrongly derive `Complete`.
    let required_name = if is_standalone_workflow {
        let Some(json) = output_contract_json else {
            return Ok(None);
        };
        let contract: crate::models::DelegatedOutputContract =
            serde_json::from_str(&json).map_err(|e| DbError::internal(e.to_string()))?;
        contract.artifact_name()
    } else {
        let snapshot = load_execution_snapshot_conn(conn, &execution_id).await?;
        let Some(packet) = snapshot
            .delegated_packets
            .iter()
            .find(|packet| packet.result_artifact_job_id.as_deref() == Some(job_id))
        else {
            return Ok(None);
        };
        packet.output_contract.artifact_name()
    };
    let requires_output = true;
    let artifact_present = artifact_present_for(conn, job_id, Some(&required_name)).await?;
    let turn = latest_turn_state(conn, job_id).await?;
    // A node-less ephemeral CALL establishes no DB turn (unlike a workflow or a
    // task, which both run a real turn), so its completion cannot be read from
    // turn facts — the job would spin `running` forever after its run finalizes,
    // and the monitoring panel would show a stuck call even when it succeeded
    // (CAIRN-2677 bug 2). For a turn-less child, derive from the completion
    // CONTRACT instead: the return artifact IS the call's result, so its presence
    // means Complete regardless of how the CLI stream ended — a clean exit, a
    // stream that crashed AFTER the artifact landed, or a warm-parked run that
    // never formally exited (CAIRN-2677 bug 1, applied to the job projection).
    // Only a turn-less child that reached a terminal RUN with no artifact is a
    // Failure; one whose latest run is still live is left running.
    let facts = if turn.is_none() {
        let run_terminal = latest_run_status_conn(conn, job_id)
            .await?
            .map(|s| is_terminal_run_status_str(&s))
            .unwrap_or(false);
        JobFacts {
            dag_ready: true,
            upstream_failed: false,
            live_turn: !artifact_present && !run_terminal,
            turn_failed: !artifact_present && run_terminal,
            turn_complete: artifact_present,
            checkpoint: CheckpointGate::None,
            resolution: Resolution::Pending,
            requires_output,
            artifact_present,
            long_running: false,
        }
    } else {
        JobFacts {
            dag_ready: true,
            upstream_failed: false,
            live_turn: latest_turn_is_live(conn, job_id).await?,
            turn_failed: matches!(turn, Some(TurnState::Failed)),
            turn_complete: matches!(turn, Some(TurnState::Complete)),
            checkpoint: CheckpointGate::None,
            resolution: Resolution::Pending,
            requires_output,
            artifact_present,
            long_running: false,
        }
    };
    let derived = derive_job_status(&facts);
    // Only write a packet-resolving terminal. A live turn (still running) or a
    // crash whose turn is interrupted derives non-terminal — leave the job
    // resumable so re-dispatch can replay, exactly as the sweep leaves a
    // node-backed crash resumable. `Blocked` (a completed turn owing an
    // unwritten output) is likewise left in place: the run had no artifact, so
    // the failure branches route through `fail_run` (turn Failed) instead.
    if !matches!(derived, JobStatus::Complete | JobStatus::Failed) || derived == current {
        return Ok(None);
    }
    check_reachable(&current, &derived, job_id);
    write_job_status(conn, job_id, &current, &derived).await?;
    // A standalone workflow's terminal status must roll up into its OWN execution
    // + issue anchor. A delegated child's execution is recomputed by the resuming
    // caller's turn-end sweep instead; a standalone workflow has no caller, so
    // without this its anchor issue/execution would stay `running` forever after
    // the node terminalizes.
    if is_standalone_workflow {
        recompute_execution_status_conn(conn, &execution_id).await?;
        if let Some(issue_id) = execution_issue_id_conn(conn, &execution_id).await? {
            recompute_issue_status_conn(conn, &issue_id).await?;
        }
    }
    Ok(Some(derived))
}

/// Owning-db wrapper around [`reduce_delegated_child_job_conn`] for the turn-end
/// callers that hold an `Orchestrator` + `job_id`. Emits a coarse `jobs`/
/// `executions` change on a write so the workflow monitoring panel's spinner
/// clears. Node-backed / non-terminal cases are a cheap no-op.
pub fn reduce_delegated_child_job(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<Option<JobStatus>, String> {
    let db = run_advancement_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let job_id_owned = job_id.to_string();
    let derived = run_advancement_db(async move {
        db.write(|conn| {
            let job_id = job_id_owned.clone();
            Box::pin(async move { reduce_delegated_child_job_conn(conn, &job_id).await })
        })
        .await
        .map_err(|e| e.to_string())
    })?;
    if derived.is_some() {
        for table in ["jobs", "executions"] {
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": table, "action": "update"}),
            );
        }
    }
    Ok(derived)
}

/// Startup safety net for the CAIRN-2559 stuck shape: a node-less delegated
/// child (call/workflow) that finalized its run but whose job status was never
/// terminalized — run terminal, job `running`, caller `blocked`. The forward fix
/// (reduce at finalize) prevents this going forward, but a host that crashed
/// under the old code is left holding it, and `redispatch_crashed_workflows`
/// only re-spawns workflows whose latest run is NON-terminal, so it never
/// revisits this shape. For each stuck child, reduce its status from its
/// turn+artifact and — if that terminalized it — fire the resume gate so the
/// durably suspended caller wakes. Idempotent and self-gating: reduce no-ops
/// once terminal, and resume no-ops when there is no suspended parent to wake.
/// Best-effort over EVERY open database (local + team replicas): a team-owned
/// workflow/call writes its job, run, turns, artifacts, and execution snapshot
/// into the team's replica (the forward paths route through
/// `owning_db_for_job(parent_job_id)`), so a local-only scan would miss a stuck
/// team child entirely. `reduce`/`resume` already route by job id, so only the
/// candidate discovery has to fan out across databases. Errors are logged, never
/// fatal.
pub async fn heal_stuck_delegated_children(orch: &Orchestrator) {
    // A job lives in exactly one database, so fan the candidate scan out across
    // every open replica and route the repair per job id.
    let mut stuck: Vec<String> = Vec::new();
    for db in orch.db.all_dbs().await {
        match db
            .read(|conn| {
                Box::pin(async move {
                    // Scope to the genuine hang: a node-less child whose latest
                    // run is terminal AND whose parent is durably suspended on
                    // the resume gate — its `current_turn_id` points at a
                    // still-`pending` successor turn (the shape
                    // `prepare_parent_for_delegated_wait` leaves). This
                    // deliberately excludes children whose caller already moved on
                    // (e.g. a fast call resolved inline, parent `running` with no
                    // pending successor), which need no wake.
                    let mut rows = conn
                        .query(
                            "SELECT j.id FROM jobs j
                             WHERE j.parent_job_id IS NOT NULL
                               AND j.recipe_node_id IS NULL
                               AND j.status IN ('running', 'pending', 'blocked')
                               AND EXISTS (
                                   SELECT 1 FROM runs r
                                   WHERE r.job_id = j.id
                                     AND r.status IN ('exited', 'failed', 'crashed')
                                     AND r.id = (
                                         SELECT id FROM runs
                                         WHERE job_id = j.id
                                         ORDER BY created_at DESC, rowid DESC LIMIT 1
                                     )
                               )
                               AND EXISTS (
                                   SELECT 1 FROM jobs pj
                                   JOIN turns pt ON pt.id = pj.current_turn_id
                                   WHERE pj.id = j.parent_job_id AND pt.state = 'pending'
                               )",
                            (),
                        )
                        .await?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next().await? {
                        out.push(row.text(0)?);
                    }
                    Ok(out)
                })
            })
            .await
        {
            Ok(mut ids) => stuck.append(&mut ids),
            Err(e) => {
                log::warn!(
                    "heal_stuck_delegated_children: candidate scan failed on a database: {e}"
                )
            }
        }
    }
    if stuck.is_empty() {
        return;
    }
    let mut healed = 0usize;
    for job_id in stuck {
        match reduce_delegated_child_job(orch, &job_id) {
            Ok(Some(status)) => {
                healed += 1;
                if let Err(e) =
                    crate::execution::delegation::resume_suspended_parent_after_task_completion(
                        orch, &job_id,
                    )
                {
                    log::warn!(
                        "heal: resume after terminalizing stuck child {job_id} as {status} failed: {e}"
                    );
                }
            }
            Ok(None) => {}
            Err(e) => log::warn!("heal: reduce of stuck delegated child {job_id} failed: {e}"),
        }
    }
    if healed > 0 {
        log::info!("Healed {healed} stuck node-less delegated child job(s) on startup");
    }
}

/// The single entry point every former decider calls: run the execution sweep in
/// a write transaction, then emit the follow-on effects keyed by each derived edge.
pub fn recompute_execution_jobs(orch: &Orchestrator, execution_id: &str) -> Result<(), String> {
    let execution_id_owned = execution_id.to_string();
    let db = run_advancement_db({
        let dbs = orch.db.clone();
        let execution_id_owned = execution_id_owned.clone();
        async move {
            crate::execution::routing::owning_db_for_execution(&dbs, &execution_id_owned)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let changes = run_advancement_db(async move {
        db.write(|conn| {
            let execution_id = execution_id_owned.clone();
            Box::pin(async move { recompute_execution_jobs_conn(conn, &execution_id).await })
        })
        .await
        .map_err(|e| e.to_string())
    })?;

    if changes.is_empty() {
        return Ok(());
    }

    // One scoped jobs event per changed job; the frontend's invalidation batch
    // dedupes them into the minimal scoped set. Executions/issues stay broad.
    for change in &changes {
        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::job_db_change_ids(
                "update",
                &change.job_id,
                change.issue_id.as_deref(),
                Some(execution_id),
                change.parent_job_id.as_deref(),
                change.parent_tool_use_id.as_deref(),
                &change.project_id,
            ),
        );
    }
    for table in ["executions", "issues"] {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": table, "action": "update"}),
        );
    }

    // Recompute is a pure projection-write; it no longer broadcasts a coarse
    // "something changed" poke. The fact-driven emit sites (artifact write,
    // turn-end, webhook PR state, manual issue close) carry their own typed
    // events. The one thing recompute is still responsible for here is
    // surfacing a *terminal* status flip that wasn't already signaled by the
    // calling fact site — e.g. a cascade-failure that closes the issue. Emit
    // one `Resolved` event for each issue this sweep moved to a terminal
    // status; non-terminal flips stay silent.
    let touched_issue_ids: Vec<String> = changes
        .iter()
        .filter_map(|change| change.issue_id.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    for issue_id in touched_issue_ids {
        let dbs = orch.db.clone();
        let issue_id_owned = issue_id.clone();
        let lookup = run_advancement_db(async move {
            let db = crate::issues::crud::owning_db_for_issue(&dbs, &issue_id_owned)
                .await
                .map_err(|e| e.to_string())?;
            crate::orchestrator::attention::read_issue_for_attention(&db, &issue_id_owned).await
        });
        if let Ok(ctx) = lookup {
            if ctx.status.is_terminal() {
                orch.emit_attention_event(crate::orchestrator::AttentionEvent {
                    issue_id,
                    issue_uri: ctx.issue_uri(),
                    fact: crate::orchestrator::AttentionFact::Resolved {
                        final_status: ctx.status.clone(),
                    },
                    attention: ctx.attention,
                    status: ctx.status,
                    updated_at: ctx.updated_at,
                });
            }
        }
    }

    let mut advance = false;
    for change in &changes {
        match change.to {
            JobStatus::Complete => {
                match crate::memories::commands::confirm_drafts_for_completed_job_without_artifact_review(
                    orch,
                    &change.job_id,
                ) {
                    Ok(completion) if completion.confirmed_count > 0 => {
                        log::info!(
                            "Confirmed {} draft memor{} for completed no-artifact job {}",
                            completion.confirmed_count,
                            if completion.confirmed_count == 1 { "y" } else { "ies" },
                            &change.job_id[..change.job_id.len().min(8)]
                        );
                        let triage_orch = orch.clone();
                        let confirmed_scopes = completion.confirmed_scopes.clone();
                        tokio::spawn(async move {
                            if let Err(error) = crate::memories::triage::maybe_spawn_triage(
                                triage_orch,
                                confirmed_scopes,
                            )
                            .await
                            {
                                log::warn!("memory triage check after no-artifact confirmation failed: {error}");
                            }
                        });
                    }
                    Ok(_) => {}
                    Err(error) => log::warn!(
                        "Failed to run no-artifact draft-memory safety net for completed job {}: {error}",
                        &change.job_id[..change.job_id.len().min(8)]
                    ),
                }
                crate::messages::system::emit_job_event(
                    orch,
                    &change.job_id,
                    None,
                    crate::messages::system::JobEvent::Completed,
                );
                advance = true;
            }
            JobStatus::Failed => {
                crate::messages::system::emit_job_event(
                    orch,
                    &change.job_id,
                    None,
                    crate::messages::system::JobEvent::Failed,
                );
            }
            JobStatus::Blocked => {
                if let Some(reason) = &change.blocked_reason {
                    log::info!(
                        "Job {} blocked without lifecycle message: {}",
                        &change.job_id[..change.job_id.len().min(8)],
                        reason
                    );
                }
                if change.emits_blocked_lifecycle_message() {
                    crate::messages::system::emit_job_event(
                        orch,
                        &change.job_id,
                        None,
                        crate::messages::system::JobEvent::Blocked { reason: None },
                    );
                }
            }
            _ => {}
        }
        if change.fires_job_ended() {
            let _ = orch.trigger_events.send(TriggerEvent::JobEnded {
                job_id: change.job_id.clone(),
                status: change.to.to_string(),
                execution_id: Some(execution_id.to_string()),
                issue_id: change.issue_id.clone(),
                project_id: change.project_id.clone(),
            });
        }
    }

    // CAIRN-2483 review-readiness hook: when a job of an issue flips to a terminal
    // or Idle status, the whole child issue may have just gone quiescent. Evaluate
    // review readiness for each such issue — this is the edge that usually fires
    // LAST for a planbuild child (the review agent completing), and it generalizes
    // to any recipe shape (e.g. a build-only child settling after its builder
    // terminalizes and the pr action completes). The evaluator is fully gated and
    // deduped, so re-evaluation is a cheap idempotent no-op when nothing settled.
    let review_issue_ids: std::collections::HashSet<String> = changes
        .iter()
        .filter(|change| {
            matches!(
                change.to,
                JobStatus::Complete | JobStatus::Failed | JobStatus::Idle
            )
        })
        .filter_map(|change| change.issue_id.clone())
        .collect();
    for issue_id in review_issue_ids {
        let orch_for_review = orch.clone();
        crate::orchestrator::lifecycle::detach_onto_runtime(
            async move {
                crate::orchestrator::lifecycle::evaluate_review_readiness(
                    &orch_for_review,
                    &issue_id,
                )
                .await;
            },
            || {},
        );
    }

    // Any terminal job in the sweep can unblock downstream DAG work; fire one
    // AdvanceDag for the execution (reduce_dag is idempotent).
    if advance {
        match crate::effects::outbox::insert_pending_with_payload(
            orch.db.local.clone(),
            "advance_dag",
            execution_id,
            "{}",
        ) {
            Ok(entry_id) => {
                if let Some(ref tx) = orch.effect_tx {
                    let _ = tx.send(crate::effects::types::WorkflowEffect::AdvanceDag {
                        execution_id: execution_id.to_string(),
                        outbox_entry_id: Some(entry_id),
                    });
                } else {
                    log::error!(
                        "No effect_tx configured — cannot advance DAG for execution {}",
                        &execution_id[..execution_id.len().min(8)]
                    );
                }
            }
            Err(e) => log::error!("Failed to enqueue advance_dag outbox entry: {e}"),
        }
    }

    Ok(())
}

#[cfg(test)]
mod gate_tests {
    use super::*;
    use crate::models::{AgentNodeConfig, ConfirmPolicy, NodePosition, SchemaConfig};

    fn agent_node(output_schema: Option<SchemaConfig>) -> RecipeNode {
        RecipeNode {
            id: "n".to_string(),
            node_type: RecipeNodeType::Agent,
            name: "Agent".to_string(),
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: Some(AgentNodeConfig {
                agent_config_id: Some("planner".to_string()),
                output_schema,
                git_config: None,
            }),
            action_config: None,
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            context_config: None,
        }
    }

    #[test]
    fn top_level_terminal_job_fires_job_ended() {
        let change = JobStatusChange {
            job_id: "job-1".to_string(),
            from: JobStatus::Running,
            to: JobStatus::Complete,
            issue_id: Some("issue-1".to_string()),
            project_id: "project-1".to_string(),
            parent_job_id: None,
            parent_tool_use_id: None,
            blocked_reason: None,
        };

        assert!(change.fires_job_ended());
    }

    #[test]
    fn delegated_child_terminal_job_does_not_fire_job_ended() {
        let change = JobStatusChange {
            job_id: "job-1".to_string(),
            from: JobStatus::Running,
            to: JobStatus::Complete,
            issue_id: Some("issue-1".to_string()),
            project_id: "project-1".to_string(),
            parent_job_id: Some("parent-1".to_string()),
            parent_tool_use_id: None,
            blocked_reason: None,
        };

        assert!(!change.fires_job_ended());
    }

    #[test]
    fn missing_output_block_does_not_emit_lifecycle_message() {
        let change = JobStatusChange {
            job_id: "job-1".to_string(),
            from: JobStatus::Running,
            to: JobStatus::Blocked,
            issue_id: Some("issue-1".to_string()),
            project_id: "project-1".to_string(),
            parent_job_id: None,
            parent_tool_use_id: None,
            blocked_reason: Some(
                "agent ended its turn without producing required output: `create-pr`".to_string(),
            ),
        };

        assert!(!change.emits_blocked_lifecycle_message());
    }

    #[test]
    fn approval_block_still_emits_lifecycle_message() {
        let change = JobStatusChange {
            job_id: "job-1".to_string(),
            from: JobStatus::Running,
            to: JobStatus::Blocked,
            issue_id: Some("issue-1".to_string()),
            project_id: "project-1".to_string(),
            parent_job_id: None,
            parent_tool_use_id: None,
            blocked_reason: None,
        };

        assert!(change.emits_blocked_lifecycle_message());
    }

    // The gate is now driven by the resolved terminal (context-out) target's
    // confirm policy, not an inline node schema. A `pr` target is already forced
    // to Auto by the resolver, so a PR-feeding node arrives here as `Auto`.
    #[test]
    fn user_policy_is_confirm_gate() {
        let node = agent_node(None);
        assert_eq!(
            gate_for(Some(&node), Some(ConfirmPolicy::User)),
            CheckpointGate::ConfirmGate
        );
    }

    #[test]
    fn auto_policy_is_no_gate() {
        let node = agent_node(None);
        assert_eq!(
            gate_for(Some(&node), Some(ConfirmPolicy::Auto)),
            CheckpointGate::None
        );
    }

    #[test]
    fn no_contract_is_no_gate() {
        let node = agent_node(None);
        assert_eq!(gate_for(Some(&node), None), CheckpointGate::None);
    }

    #[test]
    fn checkpoint_node_is_command_gate() {
        let node = RecipeNode {
            id: "c".to_string(),
            node_type: RecipeNodeType::Checkpoint,
            name: "Check".to_string(),
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: None,
            action_config: None,
            checkpoint_config: Some(crate::models::CheckpointNodeConfig {
                command: Some("bun run ci".to_string()),
                prompt: None,
            }),
            artifact_config: None,
            condition_config: None,
            context_config: None,
        };
        // A checkpoint node is a command gate regardless of confirm policy.
        assert_eq!(gate_for(Some(&node), None), CheckpointGate::Command);
    }
}

#[cfg(test)]
mod artifact_present_tests {
    use super::{artifact_present_for, recompute_job, recompute_job_status_conn};
    use crate::db::DbState;
    use crate::models::{
        AgentNodeConfig, ConfirmPolicy, ExecutionSnapshot, JobStatus, NodePosition, RecipeFile,
        RecipeNode, RecipeNodeType, RecipeSnapshot, RecipeTrigger, SchemaConfig, TriggerContext,
        TriggerType,
    };
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{
        DbError, LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    async fn test_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("recompute.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    /// Seed the minimal FK chain (workspace -> project -> issue -> execution ->
    /// job) plus a stray `checkpoint` artifact on the job. Deliberately does NOT
    /// write the required `create-pr` output.
    async fn seed_job_with_stray_artifact(db: &LocalDb) {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','P','/tmp/p',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e-1','default','i-1','p-1','running',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-1','e-1','i-1','p-1','running','builder','builder',1,1)", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, data, version, output_name, created_at, updated_at) VALUES ('a-chk','j-1','checkpoint','{}',1,'checkpoint',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    fn review_snapshot_json() -> String {
        let node = RecipeNode {
            id: "builder".to_string(),
            node_type: RecipeNodeType::Agent,
            name: "Builder".to_string(),
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: Some(AgentNodeConfig {
                agent_config_id: Some("builder".to_string()),
                output_schema: Some(SchemaConfig {
                    name: "create-pr".to_string(),
                    schema: None,
                    confirm_policy: ConfirmPolicy::Auto,
                    tool_name: None,
                    description: None,
                }),
                git_config: None,
            }),
            action_config: None,
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            context_config: None,
        };
        // The review fixture models a builder inside a SHIPPING recipe (it
        // writes a create-pr artifact), so the snapshot carries the pr node
        // real build/planbuild shapes have. Without it the topology derivation
        // (`is_long_running_node`) reads a single-agent snapshot as a standing
        // recipe — the fixture's name-only inline schema resolves to no output
        // contract — and settles the job Idle where these tests assert the
        // completion path.
        let pr = RecipeNode {
            id: "pr".to_string(),
            node_type: RecipeNodeType::Pr,
            name: "PR".to_string(),
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: None,
            action_config: None,
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            context_config: None,
        };
        serde_json::to_string(&ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "recipe".to_string(),
                name: "Recipe".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![node, pr],
                edges: vec![],
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: Some("i-review".to_string()),
                project_id: "p-review".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
            presets: None,
            delegated_packets: vec![],
            created_at: 1,
        })
        .unwrap()
    }

    /// A single agent node with no output contract and no edges — no terminal
    /// action node and a contract-less control-terminal — the coordinator-on-main
    /// shape the topology derivation marks long-running, so it settles Idle at
    /// clean turn-end.
    fn long_running_snapshot_json() -> String {
        let node = RecipeNode {
            id: "coordinator".to_string(),
            node_type: RecipeNodeType::Agent,
            name: "Coordinator".to_string(),
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: Some(AgentNodeConfig {
                agent_config_id: Some("coordinator".to_string()),
                output_schema: None,
                git_config: None,
            }),
            action_config: None,
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            context_config: None,
        };
        serde_json::to_string(&ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "recipe".to_string(),
                name: "Recipe".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![node],
                edges: vec![],
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: Some("i-review".to_string()),
                project_id: "p-review".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
            presets: None,
            delegated_packets: vec![],
            created_at: 1,
        })
        .unwrap()
    }

    /// Build an `ExecutionSnapshot` from a bundled recipe YAML, mirroring the
    /// production snapshot build (nodes + edges carried verbatim). Agents/skills
    /// are irrelevant to the topology derivation and left empty.
    fn snapshot_from_yaml(yaml: &str) -> ExecutionSnapshot {
        let recipe = RecipeFile::from_yaml(yaml)
            .expect("bundled recipe parses")
            .into_recipe(Some("default".to_string()), None);
        ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: recipe.id.clone(),
                name: recipe.name.clone(),
                description: recipe.description.clone(),
                trigger: recipe.trigger.clone(),
                nodes: recipe.nodes.clone(),
                edges: recipe.edges.clone(),
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: None,
                project_id: "p".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
            presets: None,
            delegated_packets: vec![],
            created_at: 1,
        }
    }

    /// Assert `is_long_running_node` over every node of `snapshot`, expecting
    /// exactly the agent node whose `agent_config_id` equals `long_running_agent`
    /// (if any) to derive true, and every other node false. Nodes are keyed by
    /// `agent_config_id` rather than node id because `into_recipe` reassigns node
    /// ids to fresh UUIDs. `requires_output` is resolved through the real
    /// terminal-contract resolver, so the derivation is exercised end to end.
    async fn assert_recipe_derivation(
        db: &LocalDb,
        name: &str,
        snapshot: ExecutionSnapshot,
        long_running_agent: Option<&str>,
    ) {
        let name = name.to_string();
        let long_running_agent = long_running_agent.map(str::to_string);
        db.read(move |conn| {
            Box::pin(async move {
                for node in &snapshot.recipe.nodes {
                    let requires_output =
                        crate::execution::jobs::find_downstream_artifact_schema_with_snapshot_conn(
                            conn, &snapshot, &node.id,
                        )
                        .await?
                        .is_some();
                    let derived = crate::execution::jobs::is_long_running_node(
                        &snapshot,
                        &node.id,
                        requires_output,
                    );
                    let agent_config_id = node
                        .agent_config
                        .as_ref()
                        .and_then(|c| c.agent_config_id.as_deref());
                    let expected =
                        long_running_agent.is_some() && agent_config_id == long_running_agent.as_deref();
                    assert_eq!(
                        derived, expected,
                        "{name}: node `{}` (agent {agent_config_id:?}) long-running derivation (requires_output={requires_output})",
                        node.id
                    );
                }
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn long_running_derives_from_recipe_topology() {
        // The long-running fact is derived from recipe topology, not a node flag:
        // an agent node stands idle at clean turn-end iff it declares no output
        // contract, is a control-terminal, AND its recipe ships no terminal action
        // (`pr`/`action`) node. Only main-coordinator's `coordinator` agent node
        // qualifies.
        //
        // Every node of every shipping recipe derives false — crucially including
        // planbuild's contract-less review sink, which is node-shape-identical to
        // the standing coordinator (a contract-less control-terminal agent) yet
        // sits in a recipe that DOES contain a `pr` node. That single recipe-level
        // fact, condition (c), is exactly what separates the two.
        //
        // `assert_recipe_derivation` checks EVERY node, so it also pins the new
        // Instruction node in both coordinator recipes to false: an Instruction
        // node has no agent_config, so it can never derive long-running, and
        // adding it changes no agent node's derivation (it feeds context-IN only).
        // Feature (coordinator.yaml) still terminates via its `pr` node; Manager
        // (main-coordinator.yaml) still stands via its only `coordinator` agent.
        let db = test_db().await;

        let shipping: &[(&str, &str)] = &[
            (
                "planbuild",
                include_str!("../../../../../recipes/planbuild.yaml"),
            ),
            ("build", include_str!("../../../../../recipes/build.yaml")),
            (
                "coordinator",
                include_str!("../../../../../recipes/coordinator.yaml"),
            ),
            (
                "task-list",
                include_str!("../../../../../recipes/task-list.yaml"),
            ),
            ("setup", include_str!("../../../../../recipes/setup.yaml")),
            (
                "memory-triage",
                include_str!("../../../../../recipes/memory-triage.yaml"),
            ),
        ];
        for (name, yaml) in shipping {
            assert_recipe_derivation(&db, name, snapshot_from_yaml(yaml), None).await;
        }

        // main-coordinator: the one standing recipe — no terminal action node.
        // Its `coordinator` agent node is the single node that derives
        // long-running (the board artifact and trigger nodes do not).
        assert_recipe_derivation(
            &db,
            "main-coordinator",
            snapshot_from_yaml(include_str!("../../../../../recipes/main-coordinator.yaml")),
            Some("coordinator"),
        )
        .await;
    }

    #[tokio::test]
    async fn long_running_node_settles_idle_without_advancing() {
        // A topology-derived long-running coordinator ends its turn with no output contract: it derives
        // Idle (non-terminal), the execution stays running, the issue reads
        // waiting/idle, and crucially NO advance_dag fires and the job never
        // completes — the re-firing loop the bug exhibited is gone.
        let temp = tempfile::tempdir().unwrap();
        let db = test_db().await;
        let snapshot = long_running_snapshot_json();
        db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-review','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-review','w-review','P','PRJ','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES ('i-review','p-review',2,'T','active','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-review','recipe','i-review','p-review','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-review','e-review','coordinator','i-review','p-review','running','coordinator','coordinator',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-review','j-review','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-review','s-review','j-review',1,'complete','initial',1,2,2)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let derived = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-review").await }))
            .await
            .unwrap();
        assert_eq!(derived, JobStatus::Idle);

        let search_index =
            Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        let (effect_tx, _effect_rx) = tokio::sync::mpsc::unbounded_channel();
        let orch = OrchestratorBuilder::new(db_state, services, temp.path().join("config"))
            .effect_tx(Some(effect_tx))
            .build();

        recompute_job(&orch, "j-review").unwrap();

        let job_status = orch
            .db
            .local
            .query_one("SELECT status FROM jobs WHERE id = 'j-review'", (), |row| {
                row.text(0)
            })
            .await
            .unwrap();
        assert_eq!(job_status, "idle");

        let exec_status = orch
            .db
            .local
            .query_one(
                "SELECT status FROM executions WHERE id = 'e-review'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(exec_status, "running");

        let (issue_status, issue_attention) = orch
            .db
            .local
            .query_one(
                "SELECT status, attention FROM issues WHERE id = 'i-review'",
                (),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(issue_status, "waiting");
        assert_eq!(issue_attention, "idle");

        let advance_count = orch
            .db
            .local
            .query_one(
                "SELECT COUNT(*) FROM effect_outbox WHERE kind = 'advance_dag' AND dedupe_key = 'e-review'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(advance_count, 0, "an idle node must not advance the DAG");
    }

    #[tokio::test]
    async fn pending_memory_review_allows_completion_and_advancement() {
        let temp = tempfile::tempdir().unwrap();
        let db = test_db().await;
        let snapshot = review_snapshot_json();
        db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-review','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-review','w-review','P','PRJ','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-review','p-review',2,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-review','recipe','i-review','p-review','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, memory_review_state, created_at, updated_at) VALUES ('j-review','e-review','builder','i-review','p-review','running','builder','builder','sent',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-review','j-review','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-review','s-review','j-review',1,'complete','initial',1,2,2)", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-review','j-review','create-pr',1,'{}',1,'create-pr',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let derived = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-review").await }))
            .await
            .unwrap();
        assert_eq!(derived, JobStatus::Complete);

        let search_index =
            Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        let (effect_tx, _effect_rx) = tokio::sync::mpsc::unbounded_channel();
        let orch = OrchestratorBuilder::new(db_state, services, temp.path().join("config"))
            .effect_tx(Some(effect_tx))
            .build();

        recompute_job(&orch, "j-review").unwrap();

        let status = orch
            .db
            .local
            .query_one("SELECT status FROM jobs WHERE id = 'j-review'", (), |row| {
                row.text(0)
            })
            .await
            .unwrap();
        assert_eq!(status, "complete");
        let outbox_count = orch
            .db
            .local
            .query_one(
                "SELECT COUNT(*) FROM effect_outbox WHERE kind = 'advance_dag' AND dedupe_key = 'e-review'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(outbox_count, 1);
    }

    #[tokio::test]
    async fn live_review_turn_keeps_job_running() {
        // A running MemoryReview turn keeps the job Running — the agent is active,
        // so the job must not be artificially blocked. Confirmation is decoupled
        // from run state (it keys off the unconfirmed artifact), so a live review
        // no longer needs the job forced Blocked. The terminal outcome still
        // derives from the work turn once the review ends (CAIRN-1576).
        let db = test_db().await;
        let snapshot = review_snapshot_json();
        db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-review','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-review','w-review','P','PRJ','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-review','p-review',2,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-review','recipe','i-review','p-review','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, memory_review_state, created_at, updated_at) VALUES ('j-review','e-review','builder','i-review','p-review','running','builder','builder','sent',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-review','j-review','codex','open',1,1,1)", ()).await?;
                // Work turn: completed earlier.
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-work','s-review','j-review',1,'complete','initial',1,2,2)", ()).await?;
                // Review turn: running, created after the work turn.
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-review','s-review','j-review',2,'running','memory_review',3,NULL,3)", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-review','j-review','create-pr',1,'{}',1,'create-pr',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let derived = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-review").await }))
            .await
            .unwrap();
        assert_eq!(derived, JobStatus::Running);
    }

    #[tokio::test]
    async fn failed_review_turn_does_not_fail_a_confirmed_complete_job() {
        // A failed MemoryReview turn is excluded from the facts, so a job whose
        // work turn completed (and whose output is confirmed) stays Complete
        // rather than being flipped to Failed by the review turn's failure.
        let db = test_db().await;
        let snapshot = review_snapshot_json();
        db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-review','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-review','w-review','P','PRJ','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-review','p-review',2,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-review','recipe','i-review','p-review','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, memory_review_state, created_at, updated_at) VALUES ('j-review','e-review','builder','i-review','p-review','complete','builder','builder','done',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-review','j-review','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-work','s-review','j-review',1,'complete','initial',1,2,2)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-review','s-review','j-review',2,'failed','memory_review',3,4,4)", ()).await?;
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a-review','j-review','create-pr',1,'{}',1,'create-pr',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let derived = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-review").await }))
            .await
            .unwrap();
        assert_eq!(derived, JobStatus::Complete);
    }

    #[tokio::test]
    async fn interrupt_and_cancel_turns_stay_resumable_for_execution_attached_job() {
        // The execution-attached branch of `recompute_job_status_conn` must honor
        // the `JobFacts` contract: an Interrupted/Cancelled work turn is a
        // user-initiated pause, NOT a failure. The job rests resumable (derives
        // Pending) so it never cascades to downstream — matching the standalone
        // branch and the bulk sweep. Counting interrupt/cancel as failure here
        // would report Failed while the persisted bulk value stays resumable.
        let db = test_db().await;
        let snapshot = review_snapshot_json();
        db.write(|conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-review','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-review','w-review','P','PRJ','/tmp/prj',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-review','p-review',2,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-review','recipe','i-review','p-review','running',1,1,?1)", (snapshot.as_str(),)).await?;
                // Two execution-attached jobs on the same node: one paused via
                // Interrupt, one via Cancel (turn-level cancel, not the sticky
                // job-status override).
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-int','e-review','builder','i-review','p-review','running','builder','builder',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-cancel','e-review','builder','i-review','p-review','running','builder-2','builder',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-int','j-int','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-cancel','j-cancel','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-int','s-int','j-int',1,'interrupted','initial',1,2,2)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-cancel','s-cancel','j-cancel',1,'cancelled','initial',1,2,2)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let interrupted = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-int").await }))
            .await
            .unwrap();
        assert_eq!(
            interrupted,
            JobStatus::Pending,
            "an interrupted work turn must rest resumable (Pending), not Failed"
        );

        let cancelled = db
            .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-cancel").await }))
            .await
            .unwrap();
        assert_eq!(
            cancelled,
            JobStatus::Pending,
            "a turn-cancelled work turn must rest resumable (Pending), not Failed"
        );
    }

    #[tokio::test]
    async fn stray_artifact_does_not_satisfy_missing_required_output() {
        // The Part C guarantee: a node that declares `create-pr` but only wrote
        // some other artifact must NOT be treated as having produced its output.
        let db = test_db().await;
        seed_job_with_stray_artifact(&db).await;

        let required = db
            .read(|conn| {
                Box::pin(async move { artifact_present_for(conn, "j-1", Some("create-pr")).await })
            })
            .await
            .unwrap();
        assert!(
            !required,
            "a stray checkpoint artifact must not satisfy a missing create-pr output"
        );

        // The unnamed-contract fallback still sees any artifact.
        let any = db
            .read(|conn| Box::pin(async move { artifact_present_for(conn, "j-1", None).await }))
            .await
            .unwrap();
        assert!(any, "unnamed contract falls back to any-artifact presence");
    }

    #[tokio::test]
    async fn named_required_artifact_satisfies_gate() {
        let db = test_db().await;
        seed_job_with_stray_artifact(&db).await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, data, version, output_name, created_at, updated_at) VALUES ('a-pr','j-1','pull_request','{}',2,'create-pr',1,1)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();

        let present = db
            .read(|conn| {
                Box::pin(async move { artifact_present_for(conn, "j-1", Some("create-pr")).await })
            })
            .await
            .unwrap();
        assert!(present, "the required create-pr artifact is present");
    }
}

#[cfg(test)]
mod port_gate_tests {
    use super::recompute_job_status_conn;
    use crate::models::{
        AgentNodeConfig, ArtifactNodeConfig, ConfirmPolicy, ExecutionSnapshot, JobStatus,
        NodePosition, RecipeEdge, RecipeEdgeType, RecipeNode, RecipeNodeType, RecipeSnapshot,
        RecipeTrigger, TriggerContext, TriggerType,
    };
    use crate::storage::{DbError, LocalDb, MigrationRunner, RowExt, TURSO_MIGRATIONS};
    use std::collections::HashMap;

    async fn test_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("port_gate.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        std::mem::forget(temp);
        db
    }

    fn agent(id: &str) -> RecipeNode {
        RecipeNode {
            id: id.to_string(),
            node_type: RecipeNodeType::Agent,
            name: id.to_string(),
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: Some(AgentNodeConfig {
                agent_config_id: Some(id.to_string()),
                output_schema: None,
                git_config: None,
            }),
            action_config: None,
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            context_config: None,
        }
    }

    fn artifact_node(id: &str, name: &str, policy: ConfirmPolicy) -> RecipeNode {
        RecipeNode {
            id: id.to_string(),
            node_type: RecipeNodeType::Artifact,
            name: id.to_string(),
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: None,
            action_config: None,
            checkpoint_config: None,
            artifact_config: Some(ArtifactNodeConfig {
                name: name.to_string(),
                schema: Some(serde_json::json!({"type": "object"})),
                confirm_policy: policy,
                content: None,
            }),
            condition_config: None,
            context_config: None,
        }
    }

    fn ctx_edge(id: &str, from: &str, from_handle: &str, to: &str) -> RecipeEdge {
        RecipeEdge {
            id: id.to_string(),
            edge_type: RecipeEdgeType::Context,
            source_node_id: from.to_string(),
            source_handle: from_handle.to_string(),
            target_node_id: to.to_string(),
            target_handle: "context-in".to_string(),
        }
    }

    /// planner --context-out--> plan(user) --context-out--> builder, plus a
    /// ctx-self `notes` doc the planner owns.
    fn snapshot_json() -> String {
        let snap = ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "Recipe".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![
                    agent("planner"),
                    artifact_node("plan-node", "plan", ConfirmPolicy::User),
                    agent("builder"),
                    artifact_node("notes-node", "notes", ConfirmPolicy::Auto),
                ],
                edges: vec![
                    ctx_edge("e1", "planner", "context-out", "plan-node"),
                    ctx_edge("e2", "plan-node", "context-out", "builder"),
                    ctx_edge("e3", "planner", "context-self", "notes-node"),
                ],
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: Some("i-1".to_string()),
                project_id: "p-1".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
            presets: None,
            delegated_packets: vec![],
            created_at: 1,
        };
        snap.to_json().unwrap()
    }

    /// Seed the FK chain + a complete planner turn. Artifacts are added by the
    /// caller to exercise the gate.
    async fn seed(db: &LocalDb) {
        let snapshot = snapshot_json();
        db.write(move |conn| {
            let snapshot = snapshot.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','P','/tmp/p',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-1','recipe-1','i-1','p-1','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-planner','e-1','planner','i-1','p-1','running','planner','planner',1,1)", ()).await?;
                conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-1','j-planner','codex','open',1,1,1)", ()).await?;
                conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-1','s-1','j-planner',1,'complete','initial',1,2,2)", ()).await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    async fn add_artifact(db: &LocalDb, id: &str, name: &str, confirmed: i64, version: i64) {
        let id = id.to_string();
        let name = name.to_string();
        db.write(move |conn| {
            let id = id.clone();
            let name = name.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES (?1,'j-planner',?2,?3,'{}',?4,?2,1,1)",
                    (id.as_str(), name.as_str(), confirmed, version),
                )
                .await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    async fn planner_status(db: &LocalDb) -> JobStatus {
        db.read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "j-planner").await }))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn user_gate_blocks_until_terminal_artifact_confirmed() {
        let db = test_db().await;
        seed(&db).await;
        // Turn complete, no plan yet -> Blocked (required output missing).
        assert_eq!(planner_status(&db).await, JobStatus::Blocked);

        // Plan written but unconfirmed under a `user` ctx-out gate -> Blocked.
        add_artifact(&db, "a-plan", "plan", 0, 1).await;
        assert_eq!(planner_status(&db).await, JobStatus::Blocked);
    }

    #[tokio::test]
    async fn user_gate_completes_when_terminal_artifact_confirmed() {
        let db = test_db().await;
        seed(&db).await;
        add_artifact(&db, "a-plan", "plan", 1, 1).await;
        assert_eq!(planner_status(&db).await, JobStatus::Complete);
    }

    #[tokio::test]
    async fn ctx_self_confirmation_does_not_resolve_terminal_gate() {
        let db = test_db().await;
        seed(&db).await;
        // Terminal plan is unconfirmed; a later, confirmed ctx-self `notes`
        // artifact must NOT flip the gate (confirmation is name-scoped). Under a
        // job-scoped latest-confirmed read this would wrongly derive Complete.
        add_artifact(&db, "a-plan", "plan", 0, 1).await;
        add_artifact(&db, "a-notes", "notes", 1, 2).await;
        assert_eq!(planner_status(&db).await, JobStatus::Blocked);
    }

    #[tokio::test]
    async fn approve_confirms_terminal_not_higher_versioned_ctx_self() {
        let db = test_db().await;
        seed(&db).await;
        // Terminal `plan` written once (v1, unconfirmed under the user gate).
        add_artifact(&db, "a-plan", "plan", 0, 1).await;
        // ctx-self `notes` patched repeatedly so its per-name chain outruns the
        // terminal in version count (v2, v3). "Latest overall" by version DESC
        // would resolve to notes, not plan.
        add_artifact(&db, "a-notes-1", "notes", 1, 2).await;
        add_artifact(&db, "a-notes-2", "notes", 1, 3).await;
        // Pre-fix this derived Blocked forever: approve confirmed the higher
        // versioned `notes` and never the terminal `plan`.
        assert_eq!(planner_status(&db).await, JobStatus::Blocked);

        let confirmed = db
            .write(|conn| {
                Box::pin(async move {
                    crate::execution::checkpoints::confirm_latest_artifact_conn(conn, "j-planner")
                        .await
                })
            })
            .await
            .unwrap();
        // Approve targets the terminal `plan`, not the higher-versioned `notes`.
        assert_eq!(confirmed.output_name.as_deref(), Some("plan"));
        // Gate now resolves -> the job completes.
        assert_eq!(planner_status(&db).await, JobStatus::Complete);
    }

    // ------------------------------------------------------------------
    // Turn-less ephemeral CALL reduction (CAIRN-2677 bug 2)
    //
    // A node-less ephemeral call establishes NO DB turn, so its job status can't
    // be derived from turn facts and used to spin `running` forever after the run
    // finalized. `reduce_delegated_child_job_conn` must derive completion from the
    // run outcome + the return artifact (the call's completion contract) instead.
    // ------------------------------------------------------------------

    /// Seed an execution snapshot carrying a materialized CallTool packet whose
    /// result job is `j-call`, plus the node-less call job, a run with
    /// `run_status`, and optionally its return artifact. NO turn is created — the
    /// exact turn-less shape the reduction must handle.
    async fn seed_call_child(db: &LocalDb, run_status: &str, has_artifact: bool) {
        let contract =
            crate::execution::delegation::resolve_delegated_output_contract(None).unwrap();
        let artifact_name = contract.artifact_name();
        let mut snap = ExecutionSnapshot {
            recipe: RecipeSnapshot {
                id: "recipe-1".to_string(),
                name: "R".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![],
                edges: vec![],
            },
            agents: HashMap::new(),
            skills: HashMap::new(),
            trigger_context: TriggerContext {
                issue_id: Some("i-1".to_string()),
                project_id: "p-1".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
            presets: None,
            delegated_packets: vec![],
            created_at: 1,
        };
        crate::execution::delegation::create_call_packet(
            &mut snap,
            crate::execution::delegation::CreateCallPacketInput {
                parent_job_id: "j-wf",
                parent_turn_id: None,
                parent_tool_use_id: None,
                title: "call",
                problem_statement: "do it",
                agent_config_id: "Explore",
                cwd: "/tmp",
                output_contract: contract,
                result_artifact_job_id: "j-call",
                task_index: Some(0),
                tier_override: None,
                backend_preference: None,
                background: false,
            },
        );
        let snapshot = snap.to_json().unwrap();
        let run_status = run_status.to_string();
        db.write(move |conn| {
            let snapshot = snapshot.clone();
            let run_status = run_status.clone();
            let artifact_name = artifact_name.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
                conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','P','/tmp/p',1,1)", ()).await?;
                conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none',1,1)", ()).await?;
                conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('e-1','recipe-1','i-1','p-1','running',1,1,?1)", (snapshot.as_str(),)).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('j-wf','e-1','i-1','p-1','running','flow','Flow',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, status, uri_segment, node_name, agent_config_id, created_at, updated_at) VALUES ('j-call','e-1','j-wf','i-1','p-1','running','c1','Call','Explore',1,1)", ()).await?;
                conn.execute("INSERT INTO runs (id, job_id, issue_id, status, created_at, updated_at) VALUES ('r-call','j-call','i-1',?1,1,1)", (run_status.as_str(),)).await?;
                if has_artifact {
                    conn.execute("INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at) VALUES ('a1','j-call',?1,1,'{}',1,?1,1,1)", (artifact_name.as_str(),)).await?;
                }
                Ok::<_, DbError>(())
            })
        }).await.unwrap();
    }

    async fn reduce_call(db: &LocalDb) -> Option<JobStatus> {
        db.write(|conn| {
            Box::pin(async move { super::reduce_delegated_child_job_conn(conn, "j-call").await })
        })
        .await
        .unwrap()
    }

    async fn call_job_status(db: &LocalDb) -> Option<String> {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT status FROM jobs WHERE id = 'j-call'", ())
                    .await?;
                Ok(rows.next().await?.and_then(|r| r.text(0).ok()))
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn turnless_call_with_artifact_completes_even_when_run_crashed() {
        // The call wrote its return artifact but its CLI stream crashed. The job
        // must terminalize Complete — the artifact IS the completion contract — not
        // spin `running` forever (CAIRN-2677 bug 2 compounding bug 1).
        let db = test_db().await;
        seed_call_child(&db, "crashed", true).await;
        assert_eq!(reduce_call(&db).await, Some(JobStatus::Complete));
        assert_eq!(call_job_status(&db).await.as_deref(), Some("complete"));
    }

    #[tokio::test]
    async fn turnless_call_with_artifact_completes_on_clean_exit() {
        let db = test_db().await;
        seed_call_child(&db, "exited", true).await;
        assert_eq!(reduce_call(&db).await, Some(JobStatus::Complete));
    }

    #[tokio::test]
    async fn turnless_call_terminal_without_artifact_fails() {
        let db = test_db().await;
        seed_call_child(&db, "exited", false).await;
        assert_eq!(reduce_call(&db).await, Some(JobStatus::Failed));
    }

    #[tokio::test]
    async fn turnless_call_with_live_run_stays_running() {
        // Still working (no artifact, run non-terminal): leave it running.
        let db = test_db().await;
        seed_call_child(&db, "live", false).await;
        assert_eq!(reduce_call(&db).await, None);
        assert_eq!(call_job_status(&db).await.as_deref(), Some("running"));
    }
}
