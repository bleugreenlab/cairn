//! One canonical execution lineage per issue.
//!
//! An issue's current attempt is owned by exactly one execution. Every other
//! execution of that issue is either **superseded** — fenced, so no turn starts
//! and no tool effect lands — or a declared **parallel** alternative that runs
//! freely but never claims to be the issue's implementation.
//!
//! ## Why this module exists
//!
//! On cairn/4658 an execution failed, an operator started a successor twenty-one
//! seconds later, and twenty minutes after that a turn-end checks attention push
//! — spawned before the failure and settled after a 28-minute cold build — woke
//! the dead execution's builder. Two agents then corrected the same compile
//! failure on two near-identical branches.
//!
//! The predicate that would have refused that wake already existed, inlined in
//! exactly one place for one narrow purpose (`orchestrator::wakes::child`'s
//! higher-seq check). This module is that predicate written once, made durable,
//! and installed at every seam an effect can reach.
//!
//! ## Two ordered steps
//!
//! Cancelling rows prevents LATER effects. It does not stop a provider process
//! that is already generating. So supersession does both, in this order:
//!
//! 1. **Publish the fence, transactionally.** Mark the prior canonical
//!    executions superseded, cascade suspension to their jobs, sessions and
//!    turns, insert the successor, and record the audit row.
//! 2. **Stop the surviving processes**, best-effort, through the existing
//!    `stop_job`.
//!
//! The order is what makes step 2 allowed to be best-effort. Once the fence is
//! committed, a process that survives it can do nothing durable: its next turn
//! is refused at admission and its next tool call is refused at dispatch. All
//! step 2 saves is the tokens it would spend before hitting one of those, which
//! in practice is a single tool call away. That is why there is no reconciler
//! here — a crash between the two steps costs at most one turn of an agent
//! whose work is already unable to land.

use cairn_db::turso::{params, Connection};

use crate::models::ExecutionLifecycle;
use crate::storage::{DbResult, LocalDb, RowExt};

/// `execution_supersessions.reason` for a supersession caused by creating the
/// issue's next attempt.
pub const REASON_SUCCESSOR_CREATED: &str = "successor_created";

/// `turns.end_reason` written when supersession terminalizes a live turn.
pub const TURN_END_REASON_SUPERSEDED: &str = "superseded";

// ── The predicate ───────────────────────────────────────────────────────────

/// Whether work belonging to a job or run may proceed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fence {
    /// This work owns its issue's attempt, or belongs to no issue attempt at
    /// all: a thread run, a chat run, a project-scoped execution.
    Clear,
    /// This work was superseded. The fact carries what a refusal needs to say.
    Superseded(Box<SupersessionFact>),
}

impl Fence {
    pub fn is_superseded(&self) -> bool {
        matches!(self, Fence::Superseded(_))
    }

    /// The refusal text, or `None` when the fence is clear.
    pub fn refusal(&self) -> Option<String> {
        match self {
            Fence::Clear => None,
            Fence::Superseded(fact) => Some(fact.refusal()),
        }
    }
}

/// Why work is fenced, and where the work actually went.
///
/// Every refusal names the successor node when there is one, so an agent or
/// operator who hits the fence learns where to look rather than only that
/// something stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupersessionFact {
    pub execution_id: String,
    pub execution_seq: Option<i64>,
    pub issue_id: Option<String>,
    /// The issue's canonical execution NOW, resolved at refusal time so the
    /// pointer is current rather than a snapshot of who took over first.
    pub successor: Option<CanonicalSuccessor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSuccessor {
    pub execution_id: String,
    pub seq: Option<i64>,
    pub node_uri: Option<String>,
}

impl SupersessionFact {
    /// What an agent reads instead of a tool result, or an operator instead of a
    /// resume.
    ///
    /// Phrased to make a model stop rather than retry: it names the terminal
    /// fact, rules out retrying, and says where the work is. Modelled on the
    /// run-ownership refusal in core dispatch.
    pub fn refusal(&self) -> String {
        let attempt = match self.execution_seq {
            Some(seq) => format!("Execution {seq}"),
            None => "This execution".to_string(),
        };
        let destination = match &self.successor {
            Some(CanonicalSuccessor {
                node_uri: Some(uri),
                ..
            }) => format!(" The work continues at {uri}."),
            Some(CanonicalSuccessor { seq: Some(seq), .. }) => {
                format!(" The work continues in execution {seq}.")
            }
            _ => String::new(),
        };
        format!(
            "{attempt} no longer owns this issue's attempt, so nothing it does can become this \
             issue's work: turns are refused and tool effects are refused. Stop here. Retrying \
             will not help.{destination} Commits already on this execution's branch are \
             preserved and unreachable as canonical work."
        )
    }
}

/// One execution's lineage columns.
struct Lineage {
    execution_id: String,
    issue_id: Option<String>,
    seq: Option<i64>,
    lifecycle: ExecutionLifecycle,
}

/// Whether a JOB may take a turn.
///
/// A job with no execution — a thread session, a chat, a workflow agent — is
/// `Clear`. Fencing those would fence the entire thread system on migration day.
pub async fn fence_for_job_conn(conn: &Connection, job_id: &str) -> DbResult<Fence> {
    let mut rows = conn
        .query(
            "SELECT e.id, e.issue_id, e.seq, e.lifecycle
               FROM jobs j JOIN executions e ON e.id = j.execution_id
              WHERE j.id = ?1 LIMIT 1",
            params![job_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(Fence::Clear);
    };
    fence_for(
        conn,
        Lineage {
            execution_id: row.text(0)?,
            issue_id: row.opt_text(1)?,
            seq: row.opt_i64(2)?,
            lifecycle: row.text(3)?.parse().unwrap_or_default(),
        },
    )
    .await
}

/// Whether a RUN's effects may land. Same question as [`fence_for_job_conn`],
/// asked from the coordinate a tool call carries.
pub async fn fence_for_run_conn(conn: &Connection, run_id: &str) -> DbResult<Fence> {
    let mut rows = conn
        .query(
            "SELECT e.id, e.issue_id, e.seq, e.lifecycle
               FROM runs r
               JOIN jobs j ON j.id = r.job_id
               JOIN executions e ON e.id = j.execution_id
              WHERE r.id = ?1 LIMIT 1",
            params![run_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(Fence::Clear);
    };
    fence_for(
        conn,
        Lineage {
            execution_id: row.text(0)?,
            issue_id: row.opt_text(1)?,
            seq: row.opt_i64(2)?,
            lifecycle: row.text(3)?.parse().unwrap_or_default(),
        },
    )
    .await
}

async fn fence_for(conn: &Connection, lineage: Lineage) -> DbResult<Fence> {
    if !lineage.lifecycle.is_fenced() {
        return Ok(Fence::Clear);
    }
    let successor = match lineage.issue_id.as_deref() {
        Some(issue_id) => canonical_successor(conn, issue_id, &lineage.execution_id).await?,
        None => None,
    };
    Ok(Fence::Superseded(Box::new(SupersessionFact {
        execution_id: lineage.execution_id,
        execution_seq: lineage.seq,
        issue_id: lineage.issue_id,
        successor,
    })))
}

/// The issue's canonical execution and the node URI of its root job.
async fn canonical_successor(
    conn: &Connection,
    issue_id: &str,
    excluding_execution_id: &str,
) -> DbResult<Option<CanonicalSuccessor>> {
    let mut rows = conn
        .query(
            "SELECT e.id, e.seq, p.key, i.number
               FROM executions e
               JOIN issues i ON i.id = e.issue_id
               JOIN projects p ON p.id = i.project_id
              WHERE e.issue_id = ?1 AND e.lifecycle = 'canonical' AND e.id <> ?2
              LIMIT 1",
            params![issue_id, excluding_execution_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let execution_id = row.text(0)?;
    let seq = row.opt_i64(1)?;
    let project_key = row.text(2)?;
    let number = row.i64(3)?;

    let mut node_rows = conn
        .query(
            "SELECT uri_segment FROM jobs
              WHERE execution_id = ?1 AND parent_job_id IS NULL
                AND uri_segment IS NOT NULL
              ORDER BY started_at DESC, created_at DESC, rowid DESC
              LIMIT 1",
            params![execution_id.as_str()],
        )
        .await?;
    let node_uri = match (node_rows.next().await?, seq) {
        (Some(row), Some(seq)) => row.opt_text(0)?.map(|segment| {
            cairn_common::uri::build_node_uri(&project_key, number as i32, seq as i32, &segment)
        }),
        _ => None,
    };

    Ok(Some(CanonicalSuccessor {
        execution_id,
        seq,
        node_uri,
    }))
}

/// Whether an issue already has an execution that owns its attempt.
///
/// The question an AUTOMATIC launch has to ask. An operator starting a new
/// execution has supplied the authority to take the attempt from live work; a
/// webhook, a schedule, a `job_ended` trigger or a skill call has not, and must
/// not be able to cancel a running builder and its sub-tasks as a side effect of
/// firing.
pub async fn issue_has_canonical_execution(conn: &Connection, issue_id: &str) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT 1 FROM executions
              WHERE issue_id = ?1 AND lifecycle = 'canonical' LIMIT 1",
            params![issue_id],
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

// ── Database-handle and blocking wrappers ───────────────────────────────────

pub async fn fence_for_job(db: &LocalDb, job_id: &str) -> DbResult<Fence> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move { fence_for_job_conn(conn, &job_id).await })
    })
    .await
}

pub async fn fence_for_run(db: &LocalDb, run_id: &str) -> DbResult<Fence> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move { fence_for_run_conn(conn, &run_id).await })
    })
    .await
}

/// The refusal for a fenced job, or `None` when it may proceed.
///
/// FAILS CLOSED: an unreadable lineage produces a refusal, not an admission. A
/// missed refusal is the incident; a spurious refusal is a legible error an
/// operator can act on.
pub fn job_refusal(db: &LocalDb, job_id: &str) -> Option<String> {
    let read = crate::storage::run_db_blocking(|| async {
        fence_for_job(db, job_id)
            .await
            .map_err(|error| format!("failed to read canonical lineage for job {job_id}: {error}"))
    });
    match read {
        Ok(fence) => fence.refusal(),
        Err(error) => Some(format!(
            "Cannot determine whether this execution still owns its issue's attempt, so the \
             resume is refused rather than risking two agents on one issue: {error}"
        )),
    }
}

// ── Step 1: publishing the fence ───────────────────────────────────────────

/// Who performed a supersession. Recorded on the audit row; display only.
#[derive(Debug, Clone)]
pub struct SupersessionActor {
    pub kind: &'static str,
    pub reference: Option<String>,
}

impl SupersessionActor {
    pub fn operator_ui() -> Self {
        Self {
            kind: "operator-ui",
            reference: None,
        }
    }

    pub fn external(reference: Option<String>) -> Self {
        Self {
            kind: "external",
            reference,
        }
    }

    pub fn system() -> Self {
        Self {
            kind: "system",
            reference: None,
        }
    }
}

/// One execution that step 1 fenced, and the jobs step 2 should interrupt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Supersession {
    pub id: String,
    pub superseded_execution_id: String,
    pub stop_targets: Vec<String>,
}

/// The next `seq` for a new execution on `issue_id`.
///
/// A per-issue maximum, so it must be read inside the same transaction that
/// inserts the execution — which is what the caller does.
pub async fn next_seq_conn(conn: &Connection, issue_id: &str) -> DbResult<i32> {
    let mut rows = conn
        .query(
            "SELECT COALESCE(MAX(seq), 0) FROM executions WHERE issue_id = ?1",
            params![issue_id],
        )
        .await?;
    let max_seq = match rows.next().await? {
        Some(row) => row.i64(0)?,
        None => 0,
    };
    Ok((max_seq + 1) as i32)
}

/// Step 1, as one write inside the caller's transaction.
///
/// Marks every canonical execution of `issue_id` superseded, cascades suspension
/// to their jobs, sessions and live turns, and writes one audit row per
/// superseded execution carrying the jobs it froze.
///
/// The cascade reaches descendant sub-agent jobs without recursion because a
/// delegated child job carries its parent's `execution_id`. Reusing the existing
/// `cancelled` job status rather than minting a new one is deliberate: it
/// already means "removed mid-flight, transcript preserved, excluded from the
/// DAG", which is exactly right.
///
/// The cascade makes `recompute_execution_status_conn` derive `failed` for the
/// superseded executions. That is correct, and harmless: `lifecycle` is a column
/// the recompute does not write.
pub async fn supersede_canonical_conn(
    conn: &Connection,
    issue_id: &str,
    successor_execution_id: &str,
    actor: &SupersessionActor,
    now: i64,
) -> DbResult<Vec<Supersession>> {
    let mut rows = conn
        .query(
            "SELECT id FROM executions WHERE issue_id = ?1 AND lifecycle = 'canonical'",
            params![issue_id],
        )
        .await?;
    let mut targets = Vec::new();
    while let Some(row) = rows.next().await? {
        targets.push(row.text(0)?);
    }

    let mut written = Vec::new();
    for execution_id in targets {
        let stop_targets = freeze_execution_conn(conn, &execution_id, now).await?;
        conn.execute(
            "UPDATE executions SET lifecycle = 'superseded' WHERE id = ?1",
            params![execution_id.as_str()],
        )
        .await?;
        // The cascade above recomputed while the execution was still live, so a
        // blocked condition could have held it running and the issue waiting.
        // Fenced now, the execution must reach a terminal status and the issue
        // must stop waiting on it.
        crate::transitions::outcome::recompute_execution_status_conn(conn, &execution_id).await?;
        crate::transitions::outcome::recompute_issue_status_conn(conn, issue_id).await?;

        let id = cairn_common::ids::mint_child(&execution_id);
        conn.execute(
            "INSERT INTO execution_supersessions (
                 id, issue_id, superseded_execution_id, successor_execution_id,
                 reason, actor_kind, actor_ref, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                id.as_str(),
                issue_id,
                execution_id.as_str(),
                successor_execution_id,
                REASON_SUCCESSOR_CREATED,
                actor.kind,
                actor.reference.as_deref(),
                now,
            ],
        )
        .await?;

        written.push(Supersession {
            id,
            superseded_execution_id: execution_id,
            stop_targets,
        });
    }
    Ok(written)
}

/// Cancel one execution's live jobs, close their sessions, terminalize their
/// live turns, and return the frozen job ids.
async fn freeze_execution_conn(
    conn: &Connection,
    execution_id: &str,
    now: i64,
) -> DbResult<Vec<String>> {
    let mut rows = conn
        .query(
            "SELECT DISTINCT j.id
               FROM jobs j
               LEFT JOIN runs r ON r.job_id = j.id AND r.status IN ('starting', 'live')
              WHERE j.execution_id = ?1
                AND (j.status NOT IN ('complete', 'failed', 'cancelled') OR r.id IS NOT NULL)",
            params![execution_id],
        )
        .await?;
    let mut job_ids = Vec::new();
    while let Some(row) = rows.next().await? {
        job_ids.push(row.text(0)?);
    }

    conn.execute(
        "UPDATE jobs SET status = 'cancelled', completed_at = ?1, updated_at = ?1
          WHERE execution_id = ?2 AND status NOT IN ('complete', 'failed', 'cancelled')",
        params![now, execution_id],
    )
    .await?;
    conn.execute(
        "UPDATE sessions SET status = 'closed', updated_at = ?1
          WHERE job_id IN (SELECT id FROM jobs WHERE execution_id = ?2)
            AND status <> 'closed'",
        params![now, execution_id],
    )
    .await?;
    conn.execute(
        "UPDATE turns
            SET state = 'cancelled', end_reason = ?1, ended_at = ?2, updated_at = ?2
          WHERE job_id IN (SELECT id FROM jobs WHERE execution_id = ?3)
            AND state IN ('pending', 'running', 'yielded')",
        params![TURN_END_REASON_SUPERSEDED, now, execution_id],
    )
    .await?;

    Ok(job_ids)
}
