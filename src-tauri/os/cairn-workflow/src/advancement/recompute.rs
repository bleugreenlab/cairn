//! Job-status recompute: the single writer of `job.status`.
//!
//! Job status is a projection (see [`crate::transitions::projection`]). This
//! module gathers the facts a job's status derives from and writes the derived
//! value as a cache, exactly as execution/issue status are recomputed beneath it.
//!
//! - [`recompute_job_status_conn`] gathers facts for one job and derives its
//!   status without writing. Standalone jobs derive directly from live turns
//!   and runs; execution-attached jobs additionally derive from DAG facts.
//! - [`recompute_execution_jobs_conn`] sweeps every job in an execution in
//!   topological order, writes the changed caches, recomputes execution + issue
//!   status, and returns the diffs.
//! - [`recompute_execution_jobs`] runs the sweep in a write transaction and
//!   emits the follow-on effects (lifecycle messages, wakes, JobEnded, DAG
//!   advancement) keyed by each derived edge. This is the single entry
//!   point every former decider now calls.

use super::*;
use crate::models::{RecipeNode, RecipeNodeType, TurnState};
use crate::transitions::outcome::{recompute_execution_status_conn, recompute_issue_status_conn};
use crate::transitions::projection::{
    derive_job_status, settle_for_terminal_issue, CheckpointGate, JobFacts, Resolution,
};
use std::collections::{HashMap, HashSet, VecDeque};

/// Why a job's cached status moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeCause {
    /// The job's own facts moved: a turn ended, an artifact landed, an upstream
    /// settled. These edges drive lifecycle messages, wakes, `JobEnded`, review
    /// re-evaluation, and DAG advancement.
    Derived,
    /// The job's issue is merged or closed, and the job settled to the outcome
    /// it had already recorded (see [`settle_for_terminal_issue`]). Nothing new
    /// happened to the work, so the change is published as a db-change only:
    /// announcing it would replay completions on long-finished issues, and
    /// advancing a resolved issue's DAG would launch work nobody can use.
    IssueResolved,
}

/// A job whose derived status differs from its cached status.
#[derive(Debug, Clone)]
pub struct JobStatusChange {
    pub job_id: String,
    pub from: JobStatus,
    pub to: JobStatus,
    pub issue_id: Option<String>,
    pub project_id: String,
    pub parent_job_id: Option<String>,
    /// Carried so the scoped `jobs` db-change can invalidate the parent batch's
    /// child-job query (`["child-jobs", parentToolUseId]`) without a cache scan.
    pub parent_tool_use_id: Option<String>,
    /// When `to == Blocked` because the node ended its turn without its declared
    /// output, a human-readable reason naming the missing artifact. None for
    /// other Blocked causes (approval gate) and for non-Blocked changes.
    pub blocked_reason: Option<String>,
    /// What moved the status. Only [`ChangeCause::Derived`] changes carry
    /// follow-on effects beyond the db-change.
    pub cause: ChangeCause,
}

async fn has_active_run_conn(conn: &cairn_db::turso::Connection, job_id: &str) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT 1 FROM runs
             WHERE job_id = ?1 AND status IN ('starting', 'live')
             LIMIT 1",
            (job_id,),
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

impl JobStatusChange {
    fn for_sweep_job(
        job: &SweepJob,
        from: JobStatus,
        to: JobStatus,
        cause: ChangeCause,
        blocked_reason: Option<String>,
    ) -> Self {
        JobStatusChange {
            job_id: job.id.clone(),
            from,
            to,
            issue_id: job.issue_id.clone(),
            project_id: job.project_id.clone(),
            parent_job_id: job.parent_job_id.clone(),
            parent_tool_use_id: job.parent_tool_use_id.clone(),
            blocked_reason,
            cause,
        }
    }

    /// Whether this change carries effects beyond its db-change: lifecycle
    /// messages, wakes, `JobEnded`, review re-evaluation, and DAG advancement.
    /// A settlement onto a resolved issue never does.
    pub fn is_announced(&self) -> bool {
        self.cause == ChangeCause::Derived
    }

    /// JobEnded fires for a top-level job reaching a terminal status.
    /// Delegated children stay silent because they have a parent job.
    pub fn fires_job_ended(&self) -> bool {
        matches!(self.to, JobStatus::Complete | JobStatus::Failed) && self.parent_job_id.is_none()
    }

    /// Missing required output is an internal Blocked gate, not a user-facing
    /// lifecycle event. Other Blocked causes (approval/checkpoint gates) still
    /// emit the normal blocked message.
    pub fn emits_blocked_lifecycle_message(&self) -> bool {
        self.to == JobStatus::Blocked && self.blocked_reason.is_none()
    }

    /// Whether this transition is one after which the issue may have gone
    /// quiescent, and so should re-evaluate the parent's review wake
    /// (CAIRN-2483). Every status this sweep can DERIVE in which a job has
    /// stopped working qualifies; `Running` and `Pending` do not.
    ///
    /// `Blocked` is a full member of that set, not an afterthought (CAIRN-3347).
    /// It is the status of a job parked at a confirmation gate — an agent that
    /// wrote its declared artifact under a `User` confirm policy derives Blocked
    /// rather than Complete — and a child stopping to ask for judgment is the
    /// single most review-worthy thing it can do. `issue_settled` already counts
    /// a blocked job as settled, so excluding it here contradicted the gate the
    /// evaluator itself applies.
    ///
    /// `Cancelled` is a stopped status too, and is absent on purpose. Its one
    /// derived edge is the settlement of a job onto a merged or closed issue
    /// ([`ChangeCause::IssueResolved`]), and those changes are never announced
    /// (see [`Self::is_announced`]): the resolution itself already evaluated
    /// whatever there was to evaluate. Archiving, the other way to Cancelled, is
    /// an explicit sticky override written by `cancel_job_conn`, and the sweep
    /// skips cancelled jobs before deriving anything.
    ///
    /// Archival is therefore covered elsewhere, and only partly: the startup
    /// re-arm's candidate query filters stored status and so recovers archived
    /// work at restart, but there is no live edge at the moment of archival.
    /// Cancelling a `pending` DAG-ready job (the one state holding an issue
    /// unsettled) can settle an issue with reviewable output and evaluate
    /// nothing until the next status change — tracked in CAIRN-3349.
    pub fn settles_for_review(&self) -> bool {
        matches!(
            self.to,
            JobStatus::Complete | JobStatus::Failed | JobStatus::Idle | JobStatus::Blocked
        )
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
    thread_id: Option<String>,
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

/// The turn truths a job's status derives from, read from the `turns` table in
/// a single query so both answers come from one snapshot.
///
/// The two facts are scoped differently on purpose. The terminal OUTCOME
/// (`turn_complete` / `turn_failed`) derives from the latest *work* turn: the
/// post-completion memory-review turn (`start_reason = 'memory_review'`) is
/// excluded so a failed review turn never fails the job, and a completed review
/// turn never stands in for the work turn's outcome. LIVENESS derives from the
/// absolute latest turn, review *included*, so the job reads Running while the
/// review agent is active. Confirmation is decoupled from run state — it keys
/// off the unconfirmed artifact, not the job status — so a live review no longer
/// hides the confirm affordance. CAIRN-1576.
#[derive(Debug, Clone, Default)]
pub struct TurnFacts {
    /// State of the latest work turn, or `None` when the job has run none.
    pub work_state: Option<TurnState>,
    /// State of the latest work turn that reached an outcome (`Complete` or
    /// `Failed`), skipping later interrupted, cancelled, or yielded turns.
    ///
    /// Only a resolved issue reads this. A builder that completed and wrote its
    /// `create-pr`, then had a post-merge launch refused, carries a later
    /// cancelled turn; its recorded outcome is still the completion, and the
    /// settlement must say Complete rather than Cancelled.
    pub outcome_state: Option<TurnState>,
    /// Whether the absolute latest turn — memory review included — is live
    /// (Running/Pending/Yielded).
    pub live: bool,
}

impl TurnFacts {
    /// Whether the latest work turn reached an authoritative outcome — the two
    /// states the projection derives a verdict from. `Yielded` is live work and
    /// `Interrupted`/`Cancelled` are user-initiated pauses that leave the job
    /// resumable, so neither counts (this is deliberately narrower than
    /// `TurnState::is_terminal`).
    fn has_work_outcome(&self) -> bool {
        matches!(
            self.work_state,
            Some(TurnState::Complete | TurnState::Failed)
        )
    }

    /// Whether the job has work in flight, given whether a run row is
    /// `starting`/`live`.
    ///
    /// An active run is **session availability, not work**. A provider process
    /// stays warm past a terminal artifact handoff on purpose — the session is
    /// held open for a reply — so a warm run must never stand in for a work turn
    /// that already reached its outcome. Letting it pins an artifact producer
    /// Running forever and the DAG never advances past a confirmed Plan or
    /// create-PR (CAIRN-4742).
    ///
    /// It therefore supplies liveness only where there is no outcome to mask:
    /// the real start/resume gap, where a run is admitted before its turn row
    /// exists, or resumes past an interrupted/cancelled turn.
    pub fn work_live(&self, active_run: bool) -> bool {
        self.live || (active_run && !self.has_work_outcome())
    }
}

pub async fn turn_facts(conn: &cairn_db::turso::Connection, job_id: &str) -> DbResult<TurnFacts> {
    let mut rows = conn
        .query(
            "SELECT
               (SELECT state FROM turns
                WHERE job_id = ?1 AND start_reason != 'memory_review'
                ORDER BY created_at DESC, sequence DESC LIMIT 1),
               (SELECT state FROM turns
                WHERE job_id = ?1
                ORDER BY created_at DESC, sequence DESC LIMIT 1),
               (SELECT state FROM turns
                WHERE job_id = ?1 AND start_reason != 'memory_review'
                  AND state IN ('complete', 'failed')
                ORDER BY created_at DESC, sequence DESC LIMIT 1)",
            (job_id,),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(TurnFacts::default());
    };
    let state = |raw: Option<String>| raw.and_then(|s| s.parse::<TurnState>().ok());
    let work_state = state(row.opt_text(0)?);
    let latest_state = state(row.opt_text(1)?);
    let outcome_state = state(row.opt_text(2)?);
    Ok(TurnFacts {
        work_state,
        outcome_state,
        live: matches!(
            latest_state,
            Some(TurnState::Running | TurnState::Pending | TurnState::Yielded)
        ),
    })
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

/// The job's required output artifact as one fact: `None` when the job has
/// produced no such artifact, `Some(confirmed)` when it has.
///
/// Presence and confirmation are the same row's two truths, so they are read
/// together: a separate presence probe could answer from a different snapshot
/// than the confirm flag it is paired with. Keeping "absent" distinct from
/// "present but awaiting confirmation" is what the `Option` carries — a bare
/// `confirmed` bool conflates the two.
///
/// When the output contract names an artifact, only that name counts: a stray
/// checkpoint artifact for the same job must not satisfy the gate, and a
/// `context-self` living doc writing under its own name must never resolve the
/// terminal one. The agent writes to `cairn:~/<name>`, stored as the artifact's
/// `output_name` (see `store_artifact`), so `output_name` is the contract's
/// `artifact_name`. Falls back to the job-scoped latest only when the contract
/// is unnamed (a command checkpoint, whose seeded artifact has a NULL
/// `output_name`).
async fn artifact_fact(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    required_name: Option<&str>,
) -> DbResult<Option<bool>> {
    let mut rows = match required_name {
        Some(name) => {
            conn.query(
                "SELECT confirmed FROM artifacts WHERE job_id = ?1 AND output_name = ?2 ORDER BY version DESC LIMIT 1",
                params![job_id, name],
            )
            .await?
        }
        None => {
            conn.query(
                "SELECT confirmed FROM artifacts WHERE job_id = ?1 ORDER BY version DESC LIMIT 1",
                (job_id,),
            )
            .await?
        }
    };
    Ok(rows
        .next()
        .await?
        .map(|r| r.i64(0))
        .transpose()?
        .map(|v| v != 0))
}

/// Record a job's failure as a turn fact so the projection derives `Failed`.
///
/// Failure is a *turn* truth, not a job-status truth (the two-axis split). If the
/// latest turn is still live it is failed in place; if it is already terminal
/// (or absent), a synthetic terminal `failed` turn is appended so the projection
/// reads `turn_failed`. Used by `mark_job_failed` and the no-recovery checkpoint
/// rejection path — cases where the job has no recovery edge and must go terminal.
pub async fn force_fail_job_turn_conn(
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

/// Whether an issue is merged or closed: resolved, and so taking no more work.
pub async fn issue_is_terminal_conn(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT 1 FROM issues WHERE id = ?1 AND status IN ('merged', 'closed') LIMIT 1",
            (issue_id,),
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

/// A node-backed job's derivation facts, gathered once for both the single-job
/// derivation and the execution sweep so the two can never drift, plus what a
/// missing-output Blocked reason names.
struct NodeJobFacts {
    facts: JobFacts,
    turn: TurnFacts,
    required_artifact_name: Option<String>,
}

impl NodeJobFacts {
    /// The same facts as a merged or closed issue reads them: nothing is live,
    /// the outcome is the latest work turn that reached one, and no node keeps
    /// standing (`long_running` rests Idle "until the issue is closed", and
    /// this is that moment).
    fn at_terminal_issue(&self) -> JobFacts {
        let mut facts = self.facts.clone();
        facts.live_turn = false;
        facts.turn_complete = matches!(self.turn.outcome_state, Some(TurnState::Complete));
        facts.turn_failed = matches!(self.turn.outcome_state, Some(TurnState::Failed));
        facts.long_running = false;
        facts
    }
}

#[allow(clippy::too_many_arguments)]
async fn node_job_facts_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    execution_id: &str,
    snapshot: &crate::models::ExecutionSnapshot,
    node_id: &str,
    all_edges_db: &[DbRecipeEdge],
    node_map_db: &HashMap<&str, &DbRecipeNode>,
    upstream_failed: bool,
) -> DbResult<NodeJobFacts> {
    let turn = turn_facts(conn, job_id).await?;
    let active_run = has_active_run_conn(conn, job_id).await?;
    let node = snapshot.recipe.nodes.iter().find(|node| node.id == node_id);
    let dag_ready = is_action_node_ready_with_snapshot_conn(
        conn,
        execution_id,
        node_id,
        all_edges_db,
        node_map_db,
    )
    .await?;
    // A node "requires output" when it has an effective output contract: the
    // schema of its single context-out edge target (an ArtifactNode, or a
    // downstream `pr`/action input port). This reuses the exact resolver that
    // tells the agent what to write, so the completion gate and the instruction
    // never drift.
    let downstream_schema =
        crate::inputs::find_downstream_artifact_schema_with_snapshot_conn(conn, snapshot, node_id)
            .await?;
    let requires_output = downstream_schema.is_some();
    let confirm_policy = downstream_schema
        .as_ref()
        .map(|schema| schema.confirm_policy);
    let required_artifact_name = downstream_schema
        .as_ref()
        .and_then(|schema| schema.artifact_name.clone());
    // The artifact fact is scoped to the terminal artifact name: a ctx-self
    // living doc's confirm flag must never resolve the job's gate.
    let artifact = artifact_fact(conn, job_id, required_artifact_name.as_deref()).await?;
    let facts = JobFacts {
        dag_ready,
        upstream_failed,
        live_turn: turn.work_live(active_run),
        // Interrupt/cancel is a user-initiated pause, NOT a failure: the job
        // rests resumable (derives Pending) and never cascades to downstream.
        // Only a genuine agent/process failure terminalizes.
        turn_failed: matches!(turn.work_state, Some(TurnState::Failed)),
        turn_complete: matches!(turn.work_state, Some(TurnState::Complete)),
        checkpoint: gate_for(node, confirm_policy),
        resolution: if artifact.unwrap_or(false) {
            Resolution::Confirmed
        } else {
            Resolution::Pending
        },
        requires_output,
        artifact_present: artifact.is_some(),
        long_running: crate::inputs::is_long_running_node(snapshot, node_id, requires_output),
    };
    Ok(NodeJobFacts {
        facts,
        turn,
        required_artifact_name,
    })
}

/// The artifact a node-less job owes, read from the contract that owns it — the
/// same contracts [`reduce_delegated_child_job_conn`] completes against:
/// - a delegated child that is a packet's result target owes its packet's
///   artifact (an ephemeral call or a workflow);
/// - any other delegated child (a `create_child_task` sub-agent) owes the fixed
///   `result` artifact;
/// - a standalone workflow owes its own persisted `output_contract`'s artifact;
/// - anything else (a legacy node-less top-level job) owes nothing.
async fn node_less_required_artifact_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<Option<String>> {
    let row = {
        let mut rows = conn
            .query(
                "SELECT parent_job_id, agent_config_id, output_contract, execution_id
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
                    r.opt_text(2)?,
                    r.opt_text(3)?,
                ))
            })
            .transpose()?
    };
    let Some((parent_job_id, agent_config_id, output_contract, execution_id)) = row else {
        return Ok(None);
    };
    if parent_job_id.is_none() {
        if agent_config_id.as_deref() != Some("workflow") {
            return Ok(None);
        }
        let Some(json) = output_contract else {
            return Ok(None);
        };
        let contract: crate::models::DelegatedOutputContract =
            serde_json::from_str(&json).map_err(|e| DbError::internal(e.to_string()))?;
        return Ok(Some(contract.artifact_name()));
    }
    let packet_artifact = match execution_id {
        Some(execution_id) => load_execution_snapshot_conn(conn, &execution_id)
            .await
            .ok()
            .and_then(|snapshot| {
                snapshot
                    .delegated_packets
                    .iter()
                    .find(|packet| packet.result_artifact_job_id.as_deref() == Some(job_id))
                    .map(|packet| packet.output_contract.artifact_name())
            }),
        None => None,
    };
    Ok(Some(
        packet_artifact.unwrap_or_else(|| "result".to_string()),
    ))
}

/// The settled status of a job with no recipe node to read a contract from (a
/// node-less delegated child or workflow, or a node no longer in its snapshot)
/// on a resolved issue.
///
/// A job that owes an artifact settles exactly as the node-less reducer would
/// complete it, read as the resolution reads facts: a completed outcome turn
/// without the artifact is not Complete, and a turn-less ephemeral call
/// completes on its artifact alone. A job that owes nothing settles from its
/// turn outcome, keeping a Complete or Failed it already holds.
async fn node_less_terminal_status_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    current: &JobStatus,
) -> DbResult<JobStatus> {
    let turn = turn_facts(conn, job_id).await?;
    let Some(required_name) = node_less_required_artifact_conn(conn, job_id).await? else {
        return Ok(match turn.outcome_state {
            Some(TurnState::Complete) => JobStatus::Complete,
            Some(TurnState::Failed) => JobStatus::Failed,
            _ if matches!(current, JobStatus::Complete | JobStatus::Failed) => current.clone(),
            _ => JobStatus::Cancelled,
        });
    };
    let artifact_present = artifact_fact(conn, job_id, Some(&required_name))
        .await?
        .is_some();
    let (turn_complete, turn_failed) = if turn.work_state.is_none() {
        // A turn-less call: its return artifact is its result, and a terminal
        // run without one is its failure.
        let run_terminal = latest_run_status_conn(conn, job_id)
            .await?
            .map(|s| is_terminal_run_status_str(&s))
            .unwrap_or(false);
        (artifact_present, !artifact_present && run_terminal)
    } else {
        (
            matches!(turn.outcome_state, Some(TurnState::Complete)),
            matches!(turn.outcome_state, Some(TurnState::Failed)),
        )
    };
    let facts = JobFacts {
        dag_ready: true,
        upstream_failed: false,
        live_turn: false,
        turn_failed,
        turn_complete,
        checkpoint: CheckpointGate::None,
        resolution: Resolution::Pending,
        requires_output: true,
        artifact_present,
        long_running: false,
    };
    Ok(settle_for_terminal_issue(derive_job_status(&facts)))
}

/// Whether a `cancelled` job's cancel can only have come from a terminal
/// resolution's stop, so the resolution's settlement and a later reopen may
/// re-derive it.
///
/// `cancelled` is written four ways, and the job's own facts tell them apart
/// without a marker column:
/// - **supersession** freezes a whole execution, which the canonical fence
///   then reports;
/// - **node removal** archives a job whose node has left the snapshot;
/// - **restart** archives prior attempts of a node that stays in the snapshot,
///   beside a newer job for the same node;
/// - the **resolution stop** cancels what was live or queued on the issue.
///
/// So a cancel is the resolution's when the job is not thread-owned, its
/// execution is not superseded, and it is either node-less or the newest job
/// of a node still in its snapshot.
pub async fn is_resolution_cancel_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<bool> {
    let row = {
        let mut rows = conn
            .query(
                "SELECT execution_id, recipe_node_id, thread_id FROM jobs WHERE id = ?1",
                (job_id,),
            )
            .await?;
        rows.next()
            .await?
            .map(|r| Ok::<_, DbError>((r.opt_text(0)?, r.opt_text(1)?, r.opt_text(2)?)))
            .transpose()?
    };
    let Some((execution_id, recipe_node_id, thread_id)) = row else {
        return Ok(false);
    };
    if thread_id.is_some() {
        return Ok(false);
    }
    let Some(execution_id) = execution_id else {
        // Neither archival nor supersession touches an execution-less job.
        return Ok(true);
    };
    if crate::canonical::fence_for_job_conn(conn, job_id)
        .await?
        .is_superseded()
    {
        return Ok(false);
    }
    let Some(node_id) = recipe_node_id else {
        return Ok(true);
    };
    // An execution with no readable snapshot cannot say whether the node was
    // removed, so its cancel is not claimed for the resolution.
    let Ok(snapshot) = load_execution_snapshot_conn(conn, &execution_id).await else {
        return Ok(false);
    };
    if !snapshot.recipe.nodes.iter().any(|node| node.id == node_id) {
        return Ok(false);
    }
    let mut rows = conn
        .query(
            "SELECT id FROM jobs WHERE execution_id = ?1 AND recipe_node_id = ?2
             ORDER BY created_at DESC, rowid DESC LIMIT 1",
            params![execution_id.as_str(), node_id.as_str()],
        )
        .await?;
    let newest = crate::storage::next_opt_text(&mut rows, 0).await?;
    Ok(newest.as_deref() == Some(job_id))
}

/// Derive the status a job settles to on a merged or closed issue.
///
/// A node-backed job derives from its normal facts as the resolution reads them
/// ([`NodeJobFacts::at_terminal_issue`]), then [`settle_for_terminal_issue`]
/// keeps Complete and Failed and cancels everything else. That keeps today's
/// Complete rule exactly, output contract and User confirm gate included.
pub async fn derive_terminal_issue_status_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    execution_id: Option<&str>,
    recipe_node_id: Option<&str>,
    current: &JobStatus,
) -> DbResult<JobStatus> {
    if let (Some(execution_id), Some(node_id)) = (execution_id, recipe_node_id) {
        let snapshot = load_execution_snapshot_conn(conn, execution_id).await?;
        if snapshot.recipe.nodes.iter().any(|node| node.id == node_id) {
            let all_nodes_db = snapshot_nodes_to_db(&snapshot);
            let all_edges_db = snapshot_edges_to_db(&snapshot);
            let node_map_db: HashMap<&str, &DbRecipeNode> =
                all_nodes_db.iter().map(|n| (n.id.as_str(), n)).collect();
            let upstream_failed =
                upstream_failed_conn(conn, execution_id, node_id, &snapshot).await?;
            let gathered = node_job_facts_conn(
                conn,
                job_id,
                execution_id,
                &snapshot,
                node_id,
                &all_edges_db,
                &node_map_db,
                upstream_failed,
            )
            .await?;
            return Ok(settle_for_terminal_issue(derive_job_status(
                &gathered.at_terminal_issue(),
            )));
        }
    }
    node_less_terminal_status_conn(conn, job_id, current).await
}

/// Derive a node-backed job's status from its facts, ignoring the sticky
/// overrides (`Cancelled`) that [`recompute_job_status_conn`] applies first.
pub async fn derive_node_job_status_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    execution_id: &str,
    node_id: &str,
) -> DbResult<JobStatus> {
    let snapshot = load_execution_snapshot_conn(conn, execution_id).await?;
    let all_nodes_db = snapshot_nodes_to_db(&snapshot);
    let all_edges_db = snapshot_edges_to_db(&snapshot);
    let node_map_db: HashMap<&str, &DbRecipeNode> =
        all_nodes_db.iter().map(|n| (n.id.as_str(), n)).collect();
    let upstream_failed = upstream_failed_conn(conn, execution_id, node_id, &snapshot).await?;
    let gathered = node_job_facts_conn(
        conn,
        job_id,
        execution_id,
        &snapshot,
        node_id,
        &all_edges_db,
        &node_map_db,
        upstream_failed,
    )
    .await?;
    Ok(derive_job_status(&gathered.facts))
}

/// Lift the jobs a terminal resolution cancelled back onto their facts, once
/// their issue has reopened.
///
/// A job is lifted only when [`is_resolution_cancel_conn`] attributes its
/// cancel to the resolution — not a node removal, a restart archive, or a
/// supersession — and it is node-backed (a node-less child's status belongs
/// to its reducer, not to this derivation). A lifted job takes the status
/// its facts derive without any terminal input — an interrupted builder rests
/// Pending exactly as it would on an active issue, and the next advance of its
/// execution claims it. Each touched execution is then swept so its dependents
/// re-derive against the lifted statuses.
///
/// Call this after the issue's own status has been recomputed off its
/// resolution, or the sweep would still read the issue as resolved.
pub async fn rederive_after_reopen_conn(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<Vec<String>> {
    let candidates = {
        let mut rows = conn
            .query(
                "SELECT id, execution_id, recipe_node_id FROM jobs
                 WHERE issue_id = ?1 AND status = 'cancelled' AND thread_id IS NULL
                   AND execution_id IS NOT NULL AND recipe_node_id IS NOT NULL",
                (issue_id,),
            )
            .await?;
        let mut candidates = Vec::new();
        while let Some(row) = rows.next().await? {
            candidates.push((row.text(0)?, row.text(1)?, row.text(2)?));
        }
        candidates
    };

    let mut lifted = Vec::new();
    let mut executions: Vec<String> = Vec::new();
    for (job_id, execution_id, node_id) in candidates {
        if !is_resolution_cancel_conn(conn, &job_id).await? {
            continue;
        }
        let derived = derive_node_job_status_conn(conn, &job_id, &execution_id, &node_id).await?;
        check_reachable(&JobStatus::Cancelled, &derived, &job_id);
        write_job_status(conn, &job_id, &JobStatus::Cancelled, &derived).await?;
        if !derived.is_terminal() {
            conn.execute(
                "UPDATE jobs SET completed_at = NULL WHERE id = ?1",
                (job_id.as_str(),),
            )
            .await?;
        }
        lifted.push(job_id);
        if !executions.contains(&execution_id) {
            executions.push(execution_id);
        }
    }
    for execution_id in &executions {
        recompute_execution_jobs_conn(conn, execution_id, &HashSet::new()).await?;
    }
    Ok(lifted)
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
                    completed_at, parent_tool_use_id, thread_id
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
            thread_id: row.opt_text(8)?,
        });
    }
    Ok(jobs)
}

pub async fn write_job_status(
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
        JobStatus::Complete | JobStatus::Failed | JobStatus::Cancelled => {
            conn.execute(
                "UPDATE jobs SET status = ?1, completed_at = ?2, updated_at = ?3 WHERE id = ?4",
                params![to.to_string(), now, now, job_id],
            )
            .await?;
            if *to == JobStatus::Complete {
                cairn_db::todos::complete_in_progress_todos_conn(conn, job_id, now).await?;
            }
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
pub fn check_reachable(from: &JobStatus, to: &JobStatus, job_id: &str) {
    use JobStatus::*;
    let reachable = matches!(
        (from, to),
        (Pending, Running | Blocked | Failed | Complete | Idle)
            | (Running, Complete | Failed | Blocked | Pending | Idle)
            | (Blocked, Complete | Failed | Pending | Running | Idle)
            | (Complete, Running | Pending)
            | (Failed, Running | Pending)
            | (Idle, Running | Pending | Complete | Failed | Blocked)
            // Settling onto a merged or closed issue, and lifting off it again
            // when the issue reopens.
            | (Pending | Running | Blocked | Idle, Cancelled)
            | (Cancelled, Pending | Running | Blocked | Idle | Complete | Failed)
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

/// Recompute one job's status from its facts without writing. The execution
/// sweep computes the recipe-backed branch's same facts in bulk.
pub async fn recompute_job_status_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<JobStatus> {
    let jobs_one = {
        let mut rows = conn
            .query(
                "SELECT execution_id, recipe_node_id, status, parent_job_id, agent_config_id,
                        thread_id, issue_id
                 FROM jobs WHERE id = ?1",
                (job_id,),
            )
            .await?;
        rows.next()
            .await?
            .map(|row| {
                Ok::<_, DbError>((
                    row.opt_text(0)?,
                    row.opt_text(1)?,
                    row.text(2)?,
                    row.opt_text(3)?,
                    row.opt_text(4)?,
                    row.opt_text(5)?,
                    row.opt_text(6)?,
                ))
            })
            .transpose()?
    };
    let Some((
        execution_id,
        recipe_node_id,
        current_status,
        parent_job_id,
        agent_config_id,
        thread_id,
        issue_id,
    )) = jobs_one
    else {
        return Err(DbError::internal(format!("job not found: {job_id}")));
    };
    let current_status = current_status
        .parse::<JobStatus>()
        .map_err(DbError::internal)?;

    // Cancelled remains the explicit sticky override for every job, with one
    // exception: on a merged or closed issue, a cancel the resolution's own stop
    // wrote is re-derived, so a job that had already recorded its outcome (an
    // idle coordinator whose work completed) settles to that outcome rather than
    // to the stop's archive. Complete and Failed remain derivable for recipe
    // jobs so a delayed retry can make them live again.
    if current_status == JobStatus::Cancelled {
        if let Some(issue_id) = issue_id.as_deref() {
            if issue_is_terminal_conn(conn, issue_id).await?
                && is_resolution_cancel_conn(conn, job_id).await?
            {
                return derive_terminal_issue_status_conn(
                    conn,
                    job_id,
                    execution_id.as_deref(),
                    recipe_node_id.as_deref(),
                    &current_status,
                )
                .await;
            }
        }
        return Ok(current_status);
    }

    // A closed thread's terminal outcome is durable. Thread ownership changes
    // how a resumable session rests, but it must not erase Complete or Failed
    // merely because no process is currently live.
    if thread_id.is_some() && current_status.is_terminal() {
        return Ok(current_status);
    }

    // Execution-less terminal outcomes are explicit, sticky decisions. In
    // particular, delegated parents resolve from the child job's terminal
    // status, so recomputation must never turn that durable result into Idle.
    if execution_id.is_none() && current_status.is_terminal() {
        return Ok(current_status);
    }

    // A merged or closed issue takes no more work: every job on it settles to
    // the outcome it recorded, or to Cancelled. This precedes the delegated
    // child and workflow early return below, so node-less children settle too
    // rather than keeping whatever status they held at resolution. Thread-owned
    // jobs are excluded: dormancy governs them.
    if thread_id.is_none() {
        if let Some(issue_id) = issue_id.as_deref() {
            if issue_is_terminal_conn(conn, issue_id).await? {
                return derive_terminal_issue_status_conn(
                    conn,
                    job_id,
                    execution_id.as_deref(),
                    recipe_node_id.as_deref(),
                    &current_status,
                )
                .await;
            }
        }
    }

    // Node-less delegated children and workflow jobs are reduced by
    // reduce_delegated_child_job_conn; their status represents an outcome
    // rather than the liveness-only projection used by true standalone jobs.
    if execution_id.is_none()
        && (parent_job_id.is_some() || agent_config_id.as_deref() == Some("workflow"))
    {
        return Ok(current_status);
    }

    let turn = turn_facts(conn, job_id).await?;
    let active_run = has_active_run_conn(conn, job_id).await?;

    // A thread remains the owner after it acquires a passive host execution.
    // Its status is liveness-only in both shapes: live work runs, otherwise the
    // warm session rests Idle.
    if thread_id.is_some() {
        return Ok(if turn.work_live(active_run) {
            JobStatus::Running
        } else {
            JobStatus::Idle
        });
    }

    // Other true standalone jobs have no DAG outcome to derive.
    // Their status is only the cached answer to whether work is currently alive:
    // a live head turn or an admitted/live run means Running; otherwise the warm,
    // resumable session is Idle.
    if execution_id.is_none() {
        return Ok(if turn.work_live(active_run) {
            JobStatus::Running
        } else {
            JobStatus::Idle
        });
    }

    let execution_id = execution_id
        .ok_or_else(|| DbError::internal(format!("job has no execution_id: {job_id}")))?;
    let recipe_node_id = recipe_node_id
        .ok_or_else(|| DbError::internal(format!("job has no recipe_node_id: {job_id}")))?;

    derive_node_job_status_conn(conn, job_id, &execution_id, &recipe_node_id).await
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

/// Everything one execution sweep observed, for the effect emission that
/// follows it.
///
/// The job diffs and the terminal edges are gathered together because they are
/// the same transaction's findings. An edge cannot be recovered afterwards: by
/// the time the write commits, "is this issue terminal" reads true forever, and
/// only the sweep that crossed the boundary can say that it did.
#[derive(Debug, Default)]
pub struct RecomputeOutcome {
    /// Job statuses this sweep rewrote.
    pub changes: Vec<JobStatusChange>,
    /// Issues this sweep moved from non-terminal to terminal.
    pub newly_terminal_issue_ids: Vec<String>,
}

/// Sweep every job in an execution in topological order, writing changed caches
/// and recomputing execution + issue status. Returns the diffs and the terminal
/// edges for effect emission.
///
/// On a merged or closed issue the sweep settles instead of deriving: every
/// non-thread, non-cancelled job — node-backed or not — takes the status
/// [`derive_terminal_issue_status_conn`] would give it, with no DAG claim guard
/// (no launch on a resolved issue is real work), and each change carries
/// [`ChangeCause::IssueResolved`] so it is published quietly.
pub async fn recompute_execution_jobs_conn(
    conn: &cairn_db::turso::Connection,
    execution_id: &str,
    claimed_launch_job_ids: &HashSet<String>,
) -> DbResult<RecomputeOutcome> {
    let snapshot = load_execution_snapshot_conn(conn, execution_id).await?;
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

    let issue_id = execution_issue_id_conn(conn, execution_id).await?;
    let issue_terminal = match issue_id.as_deref() {
        Some(issue_id) => issue_is_terminal_conn(conn, issue_id).await?,
        None => false,
    };

    let mut changes = Vec::new();

    // A thread's execution is passive machinery, not a workflow that must
    // complete. Thread session jobs are intentionally node-less in the modern
    // empty-host recipe, so project every thread-owned job independently of the
    // recipe's node order. This also covers historical hosts that did retain a
    // recipe node id.
    for j in jobs.iter().filter(|job| job.thread_id.is_some()) {
        if j.status.is_terminal() {
            continue;
        }
        let turn = turn_facts(conn, &j.id).await?;
        let active_run = has_active_run_conn(conn, &j.id).await?;
        let derived = if turn.work_live(active_run) || claimed_launch_job_ids.contains(&j.id) {
            JobStatus::Running
        } else {
            JobStatus::Idle
        };
        let current = status
            .get(&j.id)
            .cloned()
            .unwrap_or_else(|| j.status.clone());
        if derived != current {
            check_reachable(&current, &derived, &j.id);
            write_job_status(conn, &j.id, &current, &derived).await?;
            status.insert(j.id.clone(), derived.clone());
            changes.push(JobStatusChange::for_sweep_job(
                j,
                current,
                derived,
                ChangeCause::Derived,
                None,
            ));
        }
    }

    let mut visited: HashSet<usize> = HashSet::new();
    for node_id in &order {
        let Some(idxs) = node_jobs.get(node_id).cloned() else {
            continue;
        };
        for i in idxs {
            visited.insert(i);
            let j = &jobs[i];
            // Thread hosts were projected above because modern hosts have no
            // recipe node at all and ownership, not DAG position, defines them.
            if j.thread_id.is_some() {
                continue;
            }
            // Cancelled jobs are archived (node removed or restarted, or the
            // execution superseded): leave their status untouched and never let
            // the projection re-derive them. The one exception is the cancel a
            // resolution's stop wrote, which settles on the resolved issue.
            if j.status == JobStatus::Cancelled
                && !(issue_terminal && is_resolution_cancel_conn(conn, &j.id).await?)
            {
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

            let gathered = node_job_facts_conn(
                conn,
                &j.id,
                execution_id,
                &snapshot,
                node_id,
                &all_edges_db,
                &node_map_db,
                upstream_failed,
            )
            .await?;
            let current = status
                .get(&j.id)
                .cloned()
                .unwrap_or_else(|| j.status.clone());

            let (derived, cause) = if issue_terminal {
                (
                    settle_for_terminal_issue(derive_job_status(&gathered.at_terminal_issue())),
                    ChangeCause::IssueResolved,
                )
            } else {
                let derived = derive_job_status(&gathered.facts);
                // Preserve Running only during the real start-gap. A pending
                // turn or starting/live run already derives Running above; the
                // remaining evidence is the orchestrator's in-memory launch
                // claim. Once none of those facts exists, Running is stale and
                // Pending must be written.
                if current == JobStatus::Running
                    && derived == JobStatus::Pending
                    && claimed_launch_job_ids.contains(&j.id)
                {
                    continue;
                }
                (derived, ChangeCause::Derived)
            };
            if derived != current {
                // C4: distinguish Blocked-on-missing-output from Blocked-for-
                // approval so the surfaced reason is actionable. The process is
                // warm/idle; writing the artifact recomputes it to Complete.
                let facts = &gathered.facts;
                let blocked_reason = if derived == JobStatus::Blocked
                    && facts.turn_complete
                    && facts.requires_output
                    && !facts.artifact_present
                {
                    Some(format!(
                        "agent ended its turn without producing required output{}",
                        gathered
                            .required_artifact_name
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
                changes.push(JobStatusChange::for_sweep_job(
                    j,
                    current,
                    derived,
                    cause,
                    blocked_reason,
                ));
            }
        }
    }

    // The DAG walk above reaches only jobs whose node is in the snapshot. On a
    // resolved issue every other job settles too — node-less delegated children,
    // and nodes no longer in the snapshot — from its turn outcome alone.
    if issue_terminal {
        for (i, j) in jobs.iter().enumerate() {
            if visited.contains(&i) || j.thread_id.is_some() {
                continue;
            }
            if j.status == JobStatus::Cancelled && !is_resolution_cancel_conn(conn, &j.id).await? {
                continue;
            }
            let current = status
                .get(&j.id)
                .cloned()
                .unwrap_or_else(|| j.status.clone());
            let derived = node_less_terminal_status_conn(conn, &j.id, &current).await?;
            if derived != current {
                check_reachable(&current, &derived, &j.id);
                write_job_status(conn, &j.id, &current, &derived).await?;
                status.insert(j.id.clone(), derived.clone());
                changes.push(JobStatusChange::for_sweep_job(
                    j,
                    current,
                    derived,
                    ChangeCause::IssueResolved,
                    None,
                ));
            }
        }
    }

    recompute_execution_status_conn(conn, execution_id).await?;
    let mut newly_terminal_issue_ids = Vec::new();
    if let Some(issue_id) = issue_id {
        if recompute_issue_status_conn(conn, &issue_id)
            .await?
            .became_terminal
        {
            newly_terminal_issue_ids.push(issue_id);
        }
    }

    Ok(RecomputeOutcome {
        changes,
        newly_terminal_issue_ids,
    })
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
pub async fn reduce_delegated_child_job_conn(
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
    let artifact_present = artifact_fact(conn, job_id, Some(&required_name))
        .await?
        .is_some();
    let turn = turn_facts(conn, job_id).await?;
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
    let facts = if turn.work_state.is_none() {
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
            live_turn: turn.live,
            turn_failed: matches!(turn.work_state, Some(TurnState::Failed)),
            turn_complete: matches!(turn.work_state, Some(TurnState::Complete)),
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

/// Recompute one job by id. Recipe-backed jobs route through their execution
/// sweep; standalone jobs write the direct liveness projection through this same
/// entry point so every caller has one answer to "what is this job's status?".
pub fn recompute_job(runtime: &WorkflowRuntime, job_id: &str) -> Result<(), String> {
    let owning_db = runtime.database(WorkflowOwner::Job(job_id))?;
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

    if let Some(execution_id) = execution_id {
        return recompute_execution_jobs(runtime, &execution_id);
    }

    let db = owning_db;
    let job_id = job_id.to_string();
    let changed = run_advancement_db({
        let db = db.clone();
        let job_id = job_id.clone();
        async move {
            db.write(|conn| {
                let job_id = job_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query("SELECT status FROM jobs WHERE id = ?1", (job_id.as_str(),))
                        .await?;
                    let current = rows
                        .next()
                        .await?
                        .ok_or_else(|| DbError::internal(format!("job not found: {job_id}")))?
                        .text(0)?
                        .parse::<JobStatus>()
                        .map_err(DbError::internal)?;
                    let derived = recompute_job_status_conn(conn, &job_id).await?;
                    if current == derived {
                        return Ok(false);
                    }
                    check_reachable(&current, &derived, &job_id);
                    write_job_status(conn, &job_id, &current, &derived).await?;
                    Ok(true)
                })
            })
            .await
            .map_err(|error| error.to_string())
        }
    })?;
    if changed {
        let payload = run_advancement_db({
            let db = db.clone();
            let job_id = job_id.clone();
            async move {
                Ok::<_, String>(
                    cairn_db::changes::job_db_change_for_id(&db, &job_id, "update").await,
                )
            }
        })?;
        let _ = runtime.emitter.emit("db-change", payload);
    }
    Ok(())
}

/// Owning-db wrapper around [`reduce_delegated_child_job_conn`] for the turn-end
/// callers that hold an `Orchestrator` + `job_id`. Emits a coarse `jobs`/
/// `executions` change on a write so the workflow monitoring panel's spinner
/// clears. Node-backed / non-terminal cases are a cheap no-op.
pub fn reduce_delegated_child_job(
    runtime: &WorkflowRuntime,
    job_id: &str,
) -> Result<Option<JobStatus>, String> {
    let db = runtime.database(WorkflowOwner::Job(job_id))?;
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
            let _ = runtime.emitter.emit(
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
pub async fn heal_stuck_delegated_children(runtime: &WorkflowRuntime) {
    // A job lives in exactly one database, so fan the candidate scan out across
    // every open replica and route the repair per job id.
    let mut stuck: Vec<String> = Vec::new();
    for db in runtime.host.databases().await {
        match db
            .read(|conn| {
                Box::pin(async move {
                    // Scope to the genuine hang: a node-less child whose latest
                    // run is terminal AND whose parent is durably suspended on
                    // the resume gate — its `current_turn_id` is either the
                    // yielded dependency anchor (current shape) or a pending
                    // successor written by an older build. This
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
                                   WHERE pj.id = j.parent_job_id
                                     AND (
                                       (pt.state = 'yielded' AND pt.yield_reason = 'dependency_wait')
                                       OR (pt.state = 'pending' AND pt.start_reason = 'dependency_unblock')
                                     )
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
        match reduce_delegated_child_job(runtime, &job_id) {
            Ok(Some(status)) => {
                healed += 1;
                if let Err(e) = runtime.host.resume_parent(&job_id) {
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
pub fn recompute_execution_jobs(
    runtime: &WorkflowRuntime,
    execution_id: &str,
) -> Result<(), String> {
    let execution_id_owned = execution_id.to_string();
    let claimed_launch_job_ids = runtime.host.claimed_launch_job_ids();
    let db = runtime.database(WorkflowOwner::Execution(&execution_id_owned))?;
    let outcome = run_advancement_db(async move {
        db.write(|conn| {
            let execution_id = execution_id_owned.clone();
            let claimed_launch_job_ids = claimed_launch_job_ids.clone();
            Box::pin(async move {
                recompute_execution_jobs_conn(conn, &execution_id, &claimed_launch_job_ids).await
            })
        })
        .await
        .map_err(|e| e.to_string())
    })?;

    if outcome.changes.is_empty() && outcome.newly_terminal_issue_ids.is_empty() {
        return Ok(());
    }

    runtime.host.projection_changed(execution_id, &outcome);
    let advance = outcome
        .changes
        .iter()
        .any(|change| change.is_announced() && change.to == JobStatus::Complete);

    // Any terminal job in the sweep can unblock downstream DAG work; fire one
    // AdvanceDag for the execution (reduce_dag is idempotent). A settlement
    // onto a resolved issue never does: its downstream is settling in the same
    // sweep, and nothing on a resolved issue may be claimed.
    if advance {
        let runtime = runtime.clone();
        let execution_id = execution_id.to_owned();
        if let Err(error) =
            run_advancement_db(async move { runtime.enqueue_advance(&execution_id).await })
        {
            log::error!("Failed to enqueue advance_dag outbox entry: {error}");
        }
    }

    Ok(())
}

#[cfg(any(test, feature = "test-utils"))]
pub async fn artifact_fact_for_test(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    required_name: Option<&str>,
) -> DbResult<Option<bool>> {
    artifact_fact(conn, job_id, required_name).await
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
            decision_config: None,
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
            cause: ChangeCause::Derived,
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
            cause: ChangeCause::Derived,
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
            cause: ChangeCause::Derived,
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
            cause: ChangeCause::Derived,
        };

        assert!(change.emits_blocked_lifecycle_message());
    }

    /// The review-wake vocabulary, pinned (CAIRN-3347). This hook is the covering
    /// edge for the coordinator's durable wake — the PR-open edge needs a PR and
    /// the turn-end check cadence runs only for PR-shipping branches — so a status
    /// silently missing from this set is a class of child event no coordinator ever
    /// hears about. `Blocked` is the gate case that was missing: it is what a
    /// planner derives once its plan awaits confirmation.
    #[test]
    fn every_derivable_stopped_status_settles_for_review() {
        let change = |to: JobStatus| JobStatusChange {
            job_id: "job-1".to_string(),
            from: JobStatus::Running,
            to,
            issue_id: Some("issue-1".to_string()),
            project_id: "project-1".to_string(),
            parent_job_id: None,
            parent_tool_use_id: None,
            blocked_reason: None,
            cause: ChangeCause::Derived,
        };

        assert!(
            change(JobStatus::Blocked).settles_for_review(),
            "a job parked at a confirmation gate must re-evaluate the parent's review wake"
        );
        assert!(change(JobStatus::Complete).settles_for_review());
        assert!(change(JobStatus::Failed).settles_for_review());
        assert!(change(JobStatus::Idle).settles_for_review());

        // A job that is still working has not settled anything to review.
        assert!(!change(JobStatus::Running).settles_for_review());
        assert!(!change(JobStatus::Pending).settles_for_review());

        // Cancelled is stopped but derived only by settling onto a resolved
        // issue, which is never announced; archiving is an explicit sticky
        // override. If that ever stops being true this assertion fails, which is
        // the point — a newly derivable stopped status has to be routed
        // deliberately rather than inherited silently.
        assert!(
            !change(JobStatus::Cancelled).settles_for_review(),
            "cancellation is not an announced transition; archived work is covered \
             by the startup re-arm's stored-status query instead"
        );
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
            decision_config: None,
            context_config: None,
        };
        // A checkpoint node is a command gate regardless of confirm policy.
        assert_eq!(gate_for(Some(&node), None), CheckpointGate::Command);
    }
}
