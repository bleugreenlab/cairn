//! The durable suspend/resume core.
//!
//! Suspending a turn is one mechanism with two clients: an explicit `waitFor`
//! condition ([`super::owned_wait`]) and a `run` batch that outlived its grace
//! window ([`super::run`]). Both persist an `agent_waits` row, park the warm
//! process, and later deliver exactly one synthetic tool result plus exactly one
//! continuation. Only the *trigger* differs, so only the trigger lives with its
//! client; everything here -- the row, the first-writer-wins claim, the
//! successor-collision handling, and startup reconciliation -- is shared.
//!
//! Order matters and is load-bearing: persist the row and yield the turn BEFORE
//! arming anything that can resolve. Arming first let an already-satisfied
//! condition resume the agent, which the subsequent park then re-suspended --
//! the "never resumed" race (CAIRN-2970).

use super::host_wait_successor::emit_successor_turn_events;
use super::permission::{ensure_wait_resolved_successor, yield_turn_for_host, WaitSuccessor};
use super::run::TerminalWaitEvent;
use crate::execution::jobs::{continue_job_impl, ResumeContext};
use crate::models::{TurnState, TurnYieldReason};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_db::turso::params;
use std::{collections::HashSet, sync::Arc, time::Duration};

// In-process poll for a racing continuation to START the predecessor's successor
// it just created. There is deliberately NO fixed cutoff: a healthy continuation
// can legitimately take a while before `start_turn` (cold process spawn, DB
// contention), so the resolver rechecks with exponential backoff (capped) for as
// long as the host lives. Shutdown drops this task, handing ownership to startup
// reconciliation.
const COLLISION_POLL_INITIAL: Duration = Duration::from_millis(50);
const COLLISION_POLL_MAX: Duration = Duration::from_secs(2);

// Pacing for re-driving a resolution that returned an error while the host
// stayed up -- a contended database, most often. Tests shrink it so a retry
// does not cost seconds.
#[cfg(not(test))]
const RESOLVE_RETRY_INITIAL: Duration = Duration::from_secs(1);
#[cfg(test)]
const RESOLVE_RETRY_INITIAL: Duration = Duration::from_millis(5);
#[cfg(not(test))]
const RESOLVE_RETRY_MAX: Duration = Duration::from_secs(30);
#[cfg(test)]
const RESOLVE_RETRY_MAX: Duration = Duration::from_millis(40);
/// How many times the live resolver retries a row whose result is already
/// durable before handing it to the periodic sweep. Past this the failure is
/// not transient, and a task spinning on it forever helps nobody.
const DURABLE_RETRY_ATTEMPTS: u32 = 8;

/// What a suspension is waiting for. Serialized into `agent_waits.condition_json`;
/// the tag keeps the encoding forward-compatible, so adding a variant never
/// invalidates rows written by an earlier host.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Condition {
    Duration,
    Terminal {
        uri: String,
        slug: String,
        /// The row the wait polls, resolved from the ref's own scope when the
        /// wait armed. `None` only on a row armed before the owner was
        /// recorded, which only ever named the waiting job's own terminal.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        owner: Option<super::terminal_ref::TerminalOwner>,
        on: TerminalWaitEvent,
        phrase: Option<String>,
    },
    /// A node's project check lanes settling. The job id is resolved once, when
    /// the wait is armed, so a restart re-arms against the same node rather than
    /// re-resolving a URI whose node may since have been renamed or archived.
    Checks {
        uri: String,
        job_id: String,
        /// The single suite watched, or `None` for the whole node.
        suite: Option<String>,
    },
    /// A `run` batch that outlived its grace window. Unlike the wait conditions
    /// there is nothing to poll: the trigger is the awaited executor result,
    /// held in memory by the host that suspended. A restart therefore cannot
    /// re-drive it, and reconciliation resolves it to a typed failure instead
    /// of stranding the agent (see [`reconcile`]).
    McpContinuation {
        state: super::mcp_continuation::McpContinuationState,
    },
    RunBatch {
        /// The request the batch is placed under. Attempts of it are not named
        /// here on purpose: a batch waiting for room is presented more than
        /// once, so an attempt id would name whichever one happened to be in
        /// flight when the call parked, and the request id is what cancellation
        /// and reconciliation both address.
        request_id: String,
        /// Whether the batch carried a `commit_msg`, so a host-restart failure
        /// can be honest about what could have landed.
        commits: bool,
        /// A short name for the batch, so its result can be told from a
        /// sibling's when one turn resumes with several parked calls at once.
        /// Optional with a serde default because rows written before this
        /// existed carry no label and must stay readable.
        #[serde(default)]
        label: Option<String>,
        /// The batch executes on the host (a response item, with whatever host
        /// items share its batch) and holds no working tree, so it cannot
        /// commit and does not hold its branch at the base-advance boundary.
        /// Defaults to false so rows written before this existed keep holding.
        #[serde(default)]
        host: bool,
        /// The batch's normalized payload, so a detached batch this wait turns
        /// into still refuses an identical reissue.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        identity: Option<String>,
        /// The batch runs against a pinned revision in a checkout of its own
        /// (`branch:`), so the branch moving never disturbs it. Defaults to
        /// false so rows written before this existed keep holding.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        verdict_only: bool,
        /// What a later runner process needs to settle and publish the batch
        /// from a result it adopts after a restart, and where the batch was
        /// placed. Absent on a host batch and on rows written before this
        /// existed, whose commit then falls back to the precise report.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        settlement: Option<Box<super::run::PersistedSettlement>>,
    },
}

/// Retire a wait whose continuation has been taken over by a more specific
/// durable suspension on the same tool call. No result or successor is created:
/// the new owner is solely responsible for both. The pending-state CAS makes the
/// transfer idempotent and refuses to steal from a resolver already in flight.
#[derive(Debug, PartialEq)]
pub(crate) enum RelinquishOutcome {
    Absent,
    Transferred,
    AlreadyClaimed,
}

/// The resolution a wait records when it hands its call over to an inner
/// wait. The inner wait answers the call; this row answers nothing.
pub(crate) const TRANSFERRED_OUTCOME: &str = r#"{"outcome":"transferred_to_inner_wait"}"#;

pub(crate) async fn relinquish_to_inner_wait(
    conn: &cairn_db::storage::TrackedConnection,
    run_id: &str,
    tool_use_id: &str,
) -> DbResult<RelinquishOutcome> {
    let now = chrono::Utc::now().timestamp_millis();
    let resolution = TRANSFERRED_OUTCOME;
    let changed = conn
        .execute(
            "UPDATE agent_waits SET state='resolved',resolution_json=?3,resolved_at=?4 \
             WHERE run_id=?1 AND tool_use_id=?2 AND state='pending' \
             AND successor_turn_id IS NULL AND result_stored_at IS NULL",
            params![run_id, tool_use_id, resolution, now],
        )
        .await?;
    if changed > 0 {
        return Ok(RelinquishOutcome::Transferred);
    }

    let mut rows = conn
        .query(
            "SELECT 1 FROM agent_waits WHERE run_id=?1 AND tool_use_id=?2 LIMIT 1",
            params![run_id, tool_use_id],
        )
        .await?;
    if rows.next().await?.is_some() {
        Ok(RelinquishOutcome::AlreadyClaimed)
    } else {
        Ok(RelinquishOutcome::Absent)
    }
}

/// Whether an external stop cancelled this suspension. A stop marks the row
/// and nothing else (`orchestrator::lifecycle::stop`), so a resolver holding
/// work only it can end looks here. An unreadable row reads as not cancelled:
/// the work then runs to its own bound, which is the safe side.
pub(crate) async fn was_cancelled(db: &LocalDb, id: &str) -> bool {
    wait_row_state(db, id).await.as_deref() == Some("cancelled")
}

/// Whether a trigger polling for this suspension's condition should keep going.
///
/// A trigger loses its row in two ways: a Stop cancels it, or something else
/// resolves it first (an escalated message superseding a `waitFor`, see
/// [`supersede_timed_waits`]). Either way nothing it finds can be delivered, so
/// a polling loop stands down rather than watching a resolved row until its
/// condition happens to hit. No row at all is the inline phase, before the
/// durable row exists, and an unreadable row reads as pending: the trigger then
/// runs to its own bound, which is the safe side.
pub(crate) async fn still_pending(db: &LocalDb, id: &str) -> bool {
    wait_row_state(db, id)
        .await
        .is_none_or(|state| state == "pending")
}

async fn wait_row_state(db: &LocalDb, id: &str) -> Option<String> {
    db.query_text(
        "SELECT state FROM agent_waits WHERE id=?1",
        (id.to_string(),),
    )
    .await
    .ok()
    .flatten()
}

/// What a turn parked on its own durable calls is waiting for, as the hold
/// description and the supersede decision both read it.
#[derive(Debug, Default)]
pub(crate) struct ParkedWaits {
    /// Each active call's [`Condition::label`], in creation order.
    pub(crate) labels: Vec<String>,
    /// Whether an escalated message may end these waits: every active call is a
    /// parked `waitFor`, and at least one of them is still pending.
    pub(crate) supersedable: bool,
}

/// The active (pending or resolving) suspensions a parked turn holds.
async fn active_waits_of_turn(
    conn: &cairn_db::storage::TrackedConnection,
    job_id: &str,
    turn_id: &str,
) -> DbResult<Vec<(Record, String, bool)>> {
    let mut rows = conn
        .query(
            "SELECT id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,deadline_ms,created_at,state,hold FROM agent_waits WHERE job_id=?1 AND predecessor_turn_id=?2 AND state IN ('pending','resolving') ORDER BY created_at, id",
            params![job_id, turn_id],
        )
        .await?;
    let mut found = Vec::new();
    while let Some(row) = rows.next().await? {
        found.push((
            record_from_row(&row)?,
            row.text(9)?,
            row.opt_text(10)?.is_some(),
        ));
    }
    Ok(found)
}

/// Whether these active calls are a pure `waitFor` park an escalated message
/// may end. A `run` batch or an MCP continuation is work in flight whose result
/// would be lost, and a held row is a call still answering on its own socket,
/// so any of them alongside keeps the whole turn held.
fn timed_waits_only(waits: &[(Record, String, bool)]) -> bool {
    waits
        .iter()
        .all(|(record, _, held)| record.condition.is_wait_for() && !held)
        && waits.iter().any(|(_, state, _)| state == "pending")
}

/// What the parked turn `turn_id` of `job_id` is waiting for.
pub(crate) async fn parked_waits_of_turn(
    db: &LocalDb,
    job_id: &str,
    turn_id: &str,
) -> Result<ParkedWaits, String> {
    let (job_id, turn_id) = (job_id.to_string(), turn_id.to_string());
    let waits = db
        .read(|c| {
            let (job_id, turn_id) = (job_id.clone(), turn_id.clone());
            Box::pin(async move { active_waits_of_turn(c, &job_id, &turn_id).await })
        })
        .await
        .map_err(|error| error.to_string())?;
    Ok(ParkedWaits {
        labels: waits
            .iter()
            .map(|(record, _, _)| record.condition.label())
            .collect(),
        supersedable: timed_waits_only(&waits),
    })
}

/// The result a superseded `waitFor` call returns, so the agent reads why its
/// wait ended early. The message itself arrives in the resume briefing.
pub(crate) fn superseded_by_message_result(
    message: &crate::messages::delivery::DirectMessageRef,
) -> String {
    serde_json::json!({
        "outcome": "superseded_by_message",
        "from": message.from,
        "messageId": message.message_id,
    })
    .to_string()
}

/// End a pure `waitFor` park early for an escalated direct message.
///
/// A `waitFor` only observes something, so ending it loses nothing: the agent
/// resumes with the message and can wait again. Anything else parked in the
/// same turn is work in flight, and then nothing is superseded.
///
/// The decision and the claim are one write. Every eligible row moves
/// `pending -> resolving` carrying [`superseded_by_message_result`], exactly the
/// first-writer-wins transition any resolver takes, so a trigger that fires
/// later loses the race harmlessly and a resolver that got there first leaves
/// nothing eligible. The resume itself is the ordinary owner's path
/// ([`drive_resolution`] replaying the claimed rows): it answers each call,
/// elects one resumer, settles any native goal pause, and starts the successor
/// turn that drains the pending `direct:` pushes.
///
/// Returns the superseded calls' labels, or `None` when the park is not a pure
/// `waitFor` park. The resume runs on the ambient runtime; with none, the
/// claimed rows are re-driven by the periodic sweep of stale `resolving` rows.
pub(crate) fn supersede_timed_waits(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    job_id: &str,
    turn_id: &str,
    message: &crate::messages::delivery::DirectMessageRef,
) -> Result<Option<Vec<String>>, String> {
    let result = superseded_by_message_result(message);
    let claimed = crate::storage::run_db_blocking({
        let (db, job_id, turn_id, result) = (
            db.clone(),
            job_id.to_string(),
            turn_id.to_string(),
            result.clone(),
        );
        move || async move { claim_timed_waits(&db, &job_id, &turn_id, &result).await }
    })?;
    let Some(records) = claimed else {
        return Ok(None);
    };
    let labels = records
        .iter()
        .map(|record| record.condition.label())
        .collect();
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => {
            let (orch, db) = (orch.clone(), db.clone());
            runtime.spawn(async move {
                // In order, so every sibling but the last stands down and the
                // last one out drives the turn's single resume.
                for record in records {
                    drive_resolution(&orch, &db, &record, &result, true).await;
                }
            });
        }
        Err(_) => log::warn!(
            "superseded the waits of turn {turn_id} for job {job_id} with no runtime to resume it on; the stale-resolution sweep resumes it"
        ),
    }
    Ok(Some(labels))
}

/// The claim half of [`supersede_timed_waits`]: decide and transition in one
/// write, so no resolver can land between the eligibility read and the claim.
async fn claim_timed_waits(
    db: &LocalDb,
    job_id: &str,
    turn_id: &str,
    result: &str,
) -> Result<Option<Vec<Record>>, String> {
    let (job_id, turn_id, result) = (job_id.to_string(), turn_id.to_string(), result.to_string());
    db.write(|c| {
        let (job_id, turn_id, result) = (job_id.clone(), turn_id.clone(), result.clone());
        Box::pin(async move {
            let waits = active_waits_of_turn(c, &job_id, &turn_id).await?;
            if waits.is_empty() || !timed_waits_only(&waits) {
                return Ok(None);
            }
            let now = chrono::Utc::now().timestamp_millis();
            let mut claimed = Vec::new();
            for (record, state, _) in waits {
                if state != "pending" {
                    continue;
                }
                let changed = c
                    .execute(
                        "UPDATE agent_waits SET state='resolving',resolution_json=?2,resolving_at=?3 WHERE id=?1 AND state='pending'",
                        params![record.id.as_str(), result.as_str(), now],
                    )
                    .await?;
                if changed > 0 {
                    claimed.push(record);
                }
            }
            Ok((!claimed.is_empty()).then_some(claimed))
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The inverse of [`relinquish_to_inner_wait`], for an inner wait that was
/// withdrawn rather than answered: the outer run-batch wait owns the call
/// again, so the batch's own result reaches the agent. Matches only a row that
/// the transfer itself retired, so it cannot reopen one resolved any other way.
pub(crate) async fn reclaim_from_inner_wait(
    conn: &cairn_db::storage::TrackedConnection,
    run_id: &str,
    tool_use_id: &str,
) -> DbResult<bool> {
    let transferred = TRANSFERRED_OUTCOME;
    let changed = conn
        .execute(
            "UPDATE agent_waits SET state='pending',resolution_json=NULL,resolved_at=NULL \
             WHERE run_id=?1 AND tool_use_id=?2 AND state='resolved' AND resolution_json=?3 \
             AND successor_turn_id IS NULL AND result_stored_at IS NULL",
            params![run_id, tool_use_id, transferred],
        )
        .await?;
    Ok(changed > 0)
}

/// Hold a run-batch resolver whose call a permission took over until that
/// permission leaves `pending`.
///
/// A batch whose item did not come back suspended settled without the
/// permission's answer, so the permission is about to be withdrawn (its batch
/// context has closed) and the withdrawal hands this row back. Resolving first
/// would find the row still transferred and drop the batch's result. The wait
/// is bounded by the inline budget: a watched permission wait cannot outlive it.
pub(crate) async fn await_inner_wait_release(db: &LocalDb, record: &Record) {
    let deadline = tokio::time::Instant::now()
        + cairn_common::callback_contract::INLINE_DURABLE_WAIT_BUDGET
        + std::time::Duration::from_secs(5);
    loop {
        let (run_id, tool_use_id, id) = (
            record.run_id.clone(),
            record.tool_use_id.clone(),
            record.id.clone(),
        );
        let held = db
            .read(|conn| {
                let (run_id, tool_use_id, id) = (run_id.clone(), tool_use_id.clone(), id.clone());
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT EXISTS(SELECT 1 FROM agent_waits WHERE id=?1 AND state='resolved' \
                               AND resolution_json='{\"outcome\":\"transferred_to_inner_wait\"}') \
                             AND EXISTS(SELECT 1 FROM permission_requests \
                               WHERE run_id=?2 AND tool_use_id=?3 AND status='pending')",
                            params![id.as_str(), run_id.as_str(), tool_use_id.as_str()],
                        )
                        .await?;
                    Ok(rows.next().await?.map(|row| row.i64(0)).transpose()?.unwrap_or(0) != 0)
                })
            })
            .await
            .unwrap_or(false);
        if !held || tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

impl Condition {
    /// Whether a run batch can write its branch or runs in the node's
    /// residence, which is what makes it hold the replay mutation boundary.
    /// A host batch holds no tree; a verdict-only one without a `commit_msg`
    /// runs in a checkout of its own. Every other condition holds nothing.
    pub(crate) fn holds_branch(&self) -> bool {
        match self {
            Condition::RunBatch {
                commits,
                host,
                verdict_only,
                ..
            } => !host && (*commits || !verdict_only),
            _ => false,
        }
    }

    /// Whether this is a `waitFor` condition, which only observes something,
    /// rather than a call whose result is work in flight.
    pub(crate) fn is_wait_for(&self) -> bool {
        matches!(
            self,
            Condition::Duration | Condition::Terminal { .. } | Condition::Checks { .. }
        )
    }

    /// The serde tag this condition is stored under, which is also the row's
    /// `kind` column.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Condition::Duration => "duration",
            Condition::Terminal { .. } => "terminal",
            Condition::Checks { .. } => "checks",
            Condition::McpContinuation { .. } => "mcp_continuation",
            Condition::RunBatch { .. } => "run_batch",
        }
    }

    /// A short, human-readable name for the call this suspension parked.
    ///
    /// It exists to distinguish one result from another when a turn resumes
    /// with several, so it names what the agent asked for rather than what the
    /// resolver polls: the question it answers is "which of the calls I issued
    /// is this?".
    pub(crate) fn label(&self) -> String {
        match self {
            Condition::Duration => "waitFor: duration".to_string(),
            Condition::Terminal { slug, on, .. } => {
                let on = match on {
                    TerminalWaitEvent::Exit => "exit",
                    TerminalWaitEvent::Output => "output",
                };
                format!("waitFor: terminal {slug} {on}")
            }
            Condition::Checks { suite: None, .. } => "waitFor: checks settled".to_string(),
            Condition::Checks {
                suite: Some(suite), ..
            } => format!("waitFor: checks verdict {suite}"),
            Condition::McpContinuation { state } => format!("MCP: {}/{}", state.server, state.tool),
            Condition::RunBatch {
                label: Some(label), ..
            } => format!("run: {label}"),
            Condition::RunBatch { label: None, .. } => "run batch".to_string(),
        }
    }
}

/// A wait's condition as one `agent_waits` row stores it: the JSON and the
/// columns the scheduler reads instead of opening it.
///
/// The derived columns are a function of the condition and the row's
/// `deadline_ms` alone, never of its `state`, so a state transition cannot make
/// them stale. They are written only by [`INSERT_WAIT_SQL`] and
/// [`STORE_CONDITION_SQL`], in the same statement as the JSON they derive from.
#[derive(Clone)]
pub(crate) struct ConditionColumns {
    pub(crate) json: String,
    pub(crate) kind: &'static str,
    pub(crate) due_at_ms: Option<i64>,
    pub(crate) next_poll_at_ms: Option<i64>,
    pub(crate) pending_prompt_id: Option<String>,
}

impl ConditionColumns {
    pub(crate) fn of(condition: &Condition, deadline_ms: Option<i64>) -> Result<Self, String> {
        let json = serde_json::to_string(condition).map_err(|error| error.to_string())?;
        let schedule = match condition {
            Condition::McpContinuation { state } => {
                super::mcp_continuation::schedule(state, deadline_ms)
            }
            _ => super::mcp_continuation::McpSchedule::default(),
        };
        Ok(Self {
            json,
            kind: condition.kind(),
            due_at_ms: schedule.due_at_ms,
            next_poll_at_ms: schedule.next_poll_at_ms,
            pending_prompt_id: schedule.pending_prompt_id,
        })
    }
}

/// The one statement that creates a wait row.
const INSERT_WAIT_SQL: &str = "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,deadline_ms,kind,due_at_ms,next_poll_at_ms,pending_prompt_id,state,created_at,hold) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,'pending',?13,?14)";

/// The one statement that rewrites an active wait's condition.
const STORE_CONDITION_SQL: &str = "UPDATE agent_waits SET condition_json=?2,deadline_ms=?3,kind=?4,due_at_ms=?5,next_poll_at_ms=?6,pending_prompt_id=?7 WHERE id=?1 AND state IN ('pending','resolving')";

/// Rewrite an active wait's condition and deadline, reporting whether the row
/// was still active.
pub(crate) async fn store_condition(
    db: &LocalDb,
    wait_id: &str,
    condition: &Condition,
    deadline_ms: Option<i64>,
) -> Result<bool, String> {
    let columns = ConditionColumns::of(condition, deadline_ms)?;
    let wait_id = wait_id.to_owned();
    db.write(|conn| {
        let wait_id = wait_id.clone();
        let columns = columns.clone();
        Box::pin(async move { store_columns_conn(conn, &wait_id, &columns, deadline_ms).await })
    })
    .await
    .map_err(|error| error.to_string())
}

/// [`store_condition`] inside the caller's transaction, from columns already
/// derived for `deadline_ms`.
pub(crate) async fn store_columns_conn(
    conn: &cairn_db::storage::TrackedConnection,
    wait_id: &str,
    columns: &ConditionColumns,
    deadline_ms: Option<i64>,
) -> DbResult<bool> {
    Ok(conn
        .execute(
            STORE_CONDITION_SQL,
            params![
                wait_id,
                columns.json.as_str(),
                deadline_ms,
                columns.kind,
                columns.due_at_ms,
                columns.next_poll_at_ms,
                columns.pending_prompt_id.as_deref()
            ],
        )
        .await?
        == 1)
}

/// One suspended call: the parked turn, the tool call awaiting a result, and
/// what it is waiting for.
#[derive(Clone)]
pub(crate) struct Record {
    pub(crate) id: String,
    pub(crate) job_id: String,
    pub(crate) run_id: String,
    pub(crate) session_id: String,
    pub(crate) turn_id: String,
    pub(crate) tool_use_id: String,
    pub(crate) condition: Condition,
    pub(crate) deadline: Option<i64>,
    pub(crate) created: i64,
}

/// The turn a callback that is about to suspend belongs to.
///
/// Normally this is the process's own live turn. But a turn that has already
/// parked one of its calls reports no live turn at all: parking transitions the
/// process to idle, which is what makes a long wait's warm process reclaimable.
/// A sibling call still crossing its own grace window would then find nothing to
/// bind to and fall back to awaiting inline — the exact loss concurrent
/// suspension exists to prevent, reintroduced by a few hundred milliseconds of
/// scheduling jitter between two calls that crossed grace together.
///
/// So a parked run answers with the turn its own active suspension belongs to. A
/// job runs one turn at a time, so an active row names the turn that is parked,
/// and a callback arriving while it is parked came from that turn.
///
/// Guessing wrong is harmless by construction: correlation is scoped to that
/// turn's transcript, so a callback that did not come from it finds no call of
/// its own to claim and falls back to awaiting inline exactly as it does today.
/// Nothing here can answer a call that belongs to someone else.
pub(crate) async fn suspending_turn_id(
    orch: &Orchestrator,
    db: &LocalDb,
    run_id: &str,
) -> Option<String> {
    if let Some(turn_id) = orch.process_state.get_current_turn_id(run_id) {
        return Some(turn_id);
    }
    let run_id = run_id.to_string();
    db.read(|c| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT predecessor_turn_id FROM agent_waits WHERE run_id=?1 AND state IN ('pending','resolving') ORDER BY created_at DESC LIMIT 1",
                    params![run_id],
                )
                .await?;
            rows.next().await?.map(|row| row.text(0)).transpose()
        })
    })
    .await
    .ok()
    .flatten()
}

/// The run's session id, which a suspension needs to bind its synthetic result.
pub(crate) async fn run_session(db: &LocalDb, id: &str) -> Result<Option<String>, String> {
    let id = id.to_string();
    db.read(|c| {
        let id = id.clone();
        Box::pin(async move {
            let mut r = c
                .query("SELECT session_id FROM runs WHERE id=?1", params![id])
                .await?;
            r.next()
                .await?
                .map(|x| x.opt_text(0))
                .transpose()
                .map(Option::flatten)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// The typed outcome that marks a suspension abandoned, in the transcript and
/// in the row's own stored resolution. Startup reads it to tell a decision
/// already made apart from a wait still to be driven.
const ABANDONED_OUTCOME: &str = "abandoned";

/// Why a suspension was abandoned.
///
/// Both causes store the same `outcome`, so everything that recognizes an
/// abandonment — the resolver's stand-down, startup reconciliation, the
/// continuation claim — keeps recognizing it. The cause is the discriminator a
/// reader uses to say *who* abandoned it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AbandonCause {
    /// A newer turn of the same job suspended, proving this one's turn gone.
    Superseded,
    /// The operator compacted the job's session, killing the turn that parked.
    OperatorCompact,
}

impl AbandonCause {
    const ALL: [AbandonCause; 2] = [AbandonCause::Superseded, AbandonCause::OperatorCompact];

    /// The cause a stored resolution names. A resolution written before causes
    /// existed carries none, and every such row was a supersession.
    fn of(resolution: Option<&str>) -> Self {
        let cause = resolution
            .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
            .and_then(|value| {
                value
                    .get("cause")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            });
        match cause.as_deref() {
            Some("operator_compact") => Self::OperatorCompact,
            _ => Self::Superseded,
        }
    }
}

/// What an abandoned suspension's own call is told. A wait row can outlive the
/// turn that created it, and that turn's tool use would otherwise dangle in the
/// transcript with no result at all.
///
/// The supersession payload carries no `cause` field: it is byte-identical to
/// what rows written before causes existed hold, which keeps the exact-payload
/// comparison in [`claim_continuation`] true for them.
fn abandoned_result(cause: AbandonCause) -> String {
    match cause {
        AbandonCause::Superseded => serde_json::json!({
            "outcome":ABANDONED_OUTCOME,
            "error":"This call's turn was replaced before its wait finished, so the wait was abandoned. Reissue it if the work still matters."
        }),
        AbandonCause::OperatorCompact => serde_json::json!({
            "outcome":ABANDONED_OUTCOME,
            "cause":"operator_compact",
            "error":"The operator compacted this session before this wait finished, so the wait was abandoned and the session restarted from its digest. Reissue it if the work still matters."
        }),
    }
    .to_string()
}

/// What any failure to establish suspension is told. The underlying error
/// is storage detail an agent cannot act on, so it goes to the log.
const SUSPENSION_UNAVAILABLE: &str =
    "This call could not be suspended, so nothing is waiting on it now. Reissue it if the work still matters.";

/// A suspension's deferred park, handed to the caller that must wait for it.
///
/// The park is what interrupts the agent's turn, and it is deliberately not
/// immediate: the suspension's own tool result has to reach the agent first, or
/// the CLI attributes the cancelled call to the user (see
/// [`crate::orchestrator::lifecycle::SUSPEND_HANDOFF_GRACE`]). Resolution is
/// therefore gated on this instead of starting the moment [`suspend`] returns.
pub(crate) struct ParkHandoff(tokio::sync::oneshot::Receiver<()>);

impl ParkHandoff {
    /// Wait for the predecessor turn to be parked.
    ///
    /// `false` means the park failed, so the predecessor is still live: the
    /// caller must NOT resolve, and leaves its pending row for startup
    /// reconciliation rather than resuming a run that was never suspended.
    pub(crate) async fn parked(self) -> bool {
        self.0.await.is_ok()
    }
}

/// Establish durable suspension: persist the row, yield the turn, and schedule
/// the park.
///
/// Two phases, and the split is load-bearing in both directions. The row and the
/// yielded turn land synchronously, before this returns, so nothing can resolve
/// ahead of them (CAIRN-2970). The park is deferred by the handoff grace so the
/// suspension's own tool result reaches the agent ahead of the interrupt
/// (CAIRN-3162). Callers arm their resolver on the returned [`ParkHandoff`],
/// which restores the original order: row, yield, park, then resolve.
pub(crate) async fn suspend(
    orch: &Orchestrator,
    db: &LocalDb,
    record: &Record,
) -> Result<ParkHandoff, String> {
    // A suspension exists to answer one specific provider call, so it cannot be
    // established without one. Every caller binds an id before it gets here;
    // refusing rather than inserting is what keeps a blank id out of the table,
    // where the per-call uniqueness bound would silently stop bounding anything.
    if record.tool_use_id.trim().is_empty() {
        log::warn!(
            "refusing to suspend run {} on no bound tool call",
            record.run_id
        );
        return Err(SUSPENSION_UNAVAILABLE.to_string());
    }
    // Reject a known competing owner before touching provider state. The same
    // check remains in the insert transaction below because permission can win
    // after this read and before the row lands.
    validate_run_batch_ownership(db, record).await?;
    // Codex decides whether to continue a goal as soon as its provider turn
    // becomes idle. Claim that decision before publishing the yielded turn or
    // returning the suspension marker: the delayed interrupt is intentionally
    // after the marker, and pausing only there leaves the marker-to-interrupt
    // window owned by the provider. The status response is validated, so a
    // provider that acknowledges the request without applying it fails closed.
    let goal_pause = pause_goal_for_park(orch, record)?;
    let established = async {
        supersede_abandoned(orch, db, record).await?;
        insert(db, record).await
    }
    .await;
    let yielded = settle_goal_pause(orch, record, goal_pause, established)?;
    Ok(finish_park(orch, db, record, yielded).await)
}

/// Claim the native goal's continue decision for a park, or fail.
fn pause_goal_for_park(
    orch: &Orchestrator,
    record: &Record,
) -> Result<Option<crate::backends::codex::DurableGoalPause>, String> {
    crate::backends::stdin::pause_goal_for_durable_wait(
        &orch.process_state,
        &record.run_id,
        crate::agent_process::process::PauseIntent::Stake(
            crate::agent_process::process::GoalPause::Owned(
                crate::agent_process::process::GoalPauseOwner::DurableWait {
                    wait_id: record.id.clone(),
                },
            ),
        ),
    )
}

/// Pass an establishing write's outcome through, restoring the native goal's
/// prior status when the write failed.
fn settle_goal_pause<T>(
    orch: &Orchestrator,
    record: &Record,
    goal_pause: Option<crate::backends::codex::DurableGoalPause>,
    established: Result<T, String>,
) -> Result<T, String> {
    match established {
        Ok(yielded) => Ok(yielded),
        Err(error) => {
            if let Err(rollback_error) =
                crate::backends::stdin::restore_goal_after_failed_durable_wait(
                    &orch.process_state,
                    &record.run_id,
                    goal_pause,
                )
            {
                log::error!(
                    "failed to restore native goal after durable suspension failed for run {}: {}",
                    record.run_id,
                    rollback_error
                );
                return Err(format!(
                    "{error}; native goal rollback also failed: {rollback_error}"
                ));
            }
            Err(error)
        }
    }
}

/// Hold a call open on its socket under the durable record a park would take.
///
/// A `run` call that outlives the grace window but is still inside its
/// declared work window stays on its socket, and this row is what gives it a
/// park's restart behavior while it does: one row, one resolver binding, held
/// open rather than handed off. Nothing is yielded and nothing is parked. The
/// call then leaves the hold exactly one way: [`deliver_held_inline`] when it
/// answers in time, or [`convert_hold_to_park`] when it cannot. A host restart
/// mid-hold is reconciled like any park ([`reconcile`]).
pub(crate) async fn hold(orch: &Orchestrator, db: &LocalDb, record: &Record) -> Result<(), String> {
    if record.tool_use_id.trim().is_empty() {
        log::warn!(
            "refusing to hold run {} on no bound tool call",
            record.run_id
        );
        return Err(SUSPENSION_UNAVAILABLE.to_string());
    }
    validate_run_batch_ownership(db, record).await?;
    supersede_abandoned(orch, db, record).await?;
    insert_as(db, record, Establish::Hold).await.map(|_| ())
}

/// How a held call's inline delivery went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeldDelivery {
    /// The row is resolved as delivered on the call's own socket; nothing will
    /// wake for it.
    Delivered,
    /// The row is still held, but its turn is no longer running (a sibling
    /// call parked it), so the result has to reach the turn durably instead.
    TurnYielded,
    /// Something else settled the row first: a stop cancelled it, or an
    /// item's own permission took the call over.
    Gone,
}

/// The resolution a held call's row records when it answered on its socket.
pub(crate) const DELIVERED_INLINE_OUTCOME: &str = r#"{"outcome":"delivered_inline"}"#;

/// Settle a held call that is answering on its own socket, BEFORE the reply
/// leaves the host: the row resolves with no stored result and no successor,
/// and the reconciler (which selects only pending and resolving rows) never
/// wakes for it.
pub(crate) async fn deliver_held_inline(
    db: &LocalDb,
    record: &Record,
) -> Result<HeldDelivery, String> {
    let (id, turn_id) = (record.id.clone(), record.turn_id.clone());
    db.write(|c| {
        let (id, turn_id) = (id.clone(), turn_id.clone());
        Box::pin(async move {
            let changed = c
                .execute(
                    "UPDATE agent_waits SET state='resolved',resolution_json=?3,resolved_at=?4 \
                     WHERE id=?1 AND state='pending' AND hold='inline' \
                     AND EXISTS(SELECT 1 FROM turns WHERE id=?2 AND state='running')",
                    params![
                        id.as_str(),
                        turn_id.as_str(),
                        DELIVERED_INLINE_OUTCOME,
                        chrono::Utc::now().timestamp_millis()
                    ],
                )
                .await?;
            if changed > 0 {
                return Ok(HeldDelivery::Delivered);
            }
            let mut rows = c
                .query(
                    "SELECT 1 FROM agent_waits WHERE id=?1 AND state='pending' AND hold='inline'",
                    params![id.as_str()],
                )
                .await?;
            Ok(if rows.next().await?.is_some() {
                HeldDelivery::TurnYielded
            } else {
                HeldDelivery::Gone
            })
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Turn a held call into an ordinary park, in place: the same row stops
/// being held, the turn yields, and the park is scheduled exactly as
/// [`suspend`] schedules it. Fails when the row is no longer held (a stop or
/// an item's permission settled it first), and then nothing is yielded.
pub(crate) async fn convert_hold_to_park(
    orch: &Orchestrator,
    db: &LocalDb,
    record: &Record,
) -> Result<ParkHandoff, String> {
    let goal_pause = pause_goal_for_park(orch, record)?;
    let (id, turn_id) = (record.id.clone(), record.turn_id.clone());
    let converted = db
        .write(|c| {
            let (id, turn_id) = (id.clone(), turn_id.clone());
            Box::pin(async move {
                let changed = c
                    .execute(
                        "UPDATE agent_waits SET hold=NULL WHERE id=?1 AND state='pending' AND hold='inline'",
                        params![id.as_str()],
                    )
                    .await?;
                if changed == 0 {
                    return Err(DbError::internal("the held call was already settled"));
                }
                yield_turn_for_host(c, &turn_id, TurnYieldReason::Wait)
                    .await
                    .map_err(|e| DbError::internal(e.to_string()))
            })
        })
        .await
        .map_err(|error| {
            log::warn!(
                "converting held run batch {} to a park failed: {error}",
                record.id
            );
            SUSPENSION_UNAVAILABLE.to_string()
        });
    let yielded = settle_goal_pause(orch, record, goal_pause, converted)?;
    Ok(finish_park(orch, db, record, yielded).await)
}

/// Turn the pending wait of a `run` batch (`batch`) into a more specific wait
/// of the same call under `condition`, in place: an MCP item whose server
/// asked for input, or opened a task, after its batch was held or parked.
/// Returns the row as it stands now.
///
/// In place because a call has exactly one wait row, ever: `agent_waits` is
/// unique on `(run_id, tool_use_id)` whatever the row's state. So the row keeps
/// its id and the batch's identity (job, run, session, turn, tool call, and the
/// native goal pause staked under that id), and only its condition changes,
/// through the one condition writer. Its age restarts with the new condition,
/// so a continuation's scheduler still holds its first transition back until
/// the item's suspension marker has reached the provider. The batch's resolver
/// stands down when its batch answers with that marker.
///
/// A batch still holding its call on the socket is parked by this, as
/// [`suspend`] parks a call. One that had already parked keeps the park it
/// has, so the turn is parked once.
///
/// A row whose batch held the branch stops holding it here, so the takeover
/// releases that hold through the same seam a batch's resolution does.
///
/// Fails, writing nothing, when the row is no longer the batch's pending
/// wait: a stop cancelled it, or its own resolver claimed it first.
pub(crate) async fn take_over_run_batch(
    orch: &Orchestrator,
    db: &LocalDb,
    batch: &Record,
    condition: Condition,
    deadline: Option<i64>,
) -> Result<Record, String> {
    if !matches!(batch.condition, Condition::RunBatch { .. }) {
        log::warn!(
            "refusing to take over wait {} of run {}: it is not a run batch",
            batch.id,
            batch.run_id
        );
        return Err(SUSPENSION_UNAVAILABLE.to_string());
    }
    let record = Record {
        condition,
        deadline,
        created: chrono::Utc::now().timestamp_millis(),
        ..batch.clone()
    };
    let goal_pause = pause_goal_for_park(orch, &record)?;
    let converted = convert_run_batch(db, &record).await;
    let (yielded, parks) = settle_goal_pause(orch, &record, goal_pause, converted)?;
    // A pending batch row holds the branch; the continuation it became holds
    // nothing and will never resolve as a batch. So this is where that hold
    // ends, and the replay deferred behind it is served from here, as from
    // every other exit of a batch's wait.
    if batch.condition.holds_branch() {
        crate::orchestrator::base_advance::on_branch_hold_released(orch, &batch.job_id);
    }
    if parks {
        // Nothing awaits this park: the continuation's scheduler is what
        // holds its first transition back.
        let _ = finish_park(orch, db, &record, yielded).await;
    }
    Ok(record)
}

/// The write half of [`take_over_run_batch`]: rewrite the batch's pending row
/// as `r`, and yield its turn when the batch was still holding the call.
/// Reports whether the turn yielded here and whether the call still has its
/// park to schedule (it does only when it was held).
async fn convert_run_batch(db: &LocalDb, r: &Record) -> Result<(bool, bool), String> {
    let run_id = r.run_id.clone();
    let r = r.clone();
    db.write(|c| {
        let r = r.clone();
        Box::pin(async move {
            // Closed before the writes below touch the same table.
            let held = {
                let mut rows = c
                    .query(
                        "SELECT hold FROM agent_waits WHERE id=?1 AND run_id=?2 AND tool_use_id=?3 \
                         AND predecessor_turn_id=?4 AND kind='run_batch' AND state='pending'",
                        params![
                            r.id.as_str(),
                            r.run_id.as_str(),
                            r.tool_use_id.as_str(),
                            r.turn_id.as_str()
                        ],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Err(DbError::internal(
                        "the row is no longer this batch's pending wait",
                    ));
                };
                row.opt_text(0)?.is_some()
            };
            let columns =
                ConditionColumns::of(&r.condition, r.deadline).map_err(DbError::internal)?;
            store_columns_conn(c, r.id.as_str(), &columns, r.deadline).await?;
            c.execute(
                "UPDATE agent_waits SET hold=NULL,created_at=?2 WHERE id=?1",
                params![r.id.as_str(), r.created],
            )
            .await?;
            if !held {
                return Ok((false, false));
            }
            let yielded = yield_turn_for_host(c, &r.turn_id, TurnYieldReason::Wait)
                .await
                .map_err(|e| DbError::internal(e.to_string()))?;
            Ok((yielded, true))
        })
    })
    .await
    .map_err(|error| {
        log::warn!("converting run batch wait for run {run_id} failed: {error}");
        SUSPENSION_UNAVAILABLE.to_string()
    })
}

/// The tail every park shares once its row is pending and its turn yielded:
/// announce the yield, release the turn, and schedule the park.
async fn finish_park(
    orch: &Orchestrator,
    db: &LocalDb,
    record: &Record,
    yielded: bool,
) -> ParkHandoff {
    // Say out loud that the turn parked. The row and the yield land inside one
    // write, which announces nothing on its own, and every reader of turn state
    // caches until something invalidates it -- so without this the node keeps
    // reading as it did before the park for as long as the park lasts. The
    // transcript is the sharpest case: a parked call's row has no verdict to
    // draw and no turn it can call running, which is how a live batch came to
    // render as neither (CAIRN-3340). The prompt and permission suspensions
    // announce their own yields the same way (`planning`, `permission`).
    if yielded {
        let change = crate::notify::turn_db_change_for_id(db, &record.turn_id, "update").await;
        let _ = orch.services.emitter.emit("db-change", change);
    }
    orch.process_state
        .yield_for_host(&record.run_id, &record.turn_id);
    let (parked_tx, parked_rx) = tokio::sync::oneshot::channel();
    crate::orchestrator::lifecycle::suspend_run_for_durable_wait_after_handoff_then(
        orch,
        &record.run_id,
        "durable_suspend",
        crate::agent_process::process::GoalPauseOwner::DurableWait {
            wait_id: record.id.clone(),
        },
        move |parked| async move {
            if parked.is_ok() {
                let _ = parked_tx.send(());
            }
        },
    );
    ParkHandoff(parked_rx)
}

async fn validate_run_batch_ownership(db: &LocalDb, r: &Record) -> Result<(), String> {
    if !matches!(r.condition, Condition::RunBatch { .. }) {
        return Ok(());
    }
    let (run_id, tool_use_id) = (r.run_id.clone(), r.tool_use_id.clone());
    let permission_owns_call = db
        .read(|c| {
            Box::pin(async move {
                let mut rows = c
                    .query(
                        "SELECT 1 FROM permission_requests WHERE run_id=?1 AND tool_use_id=?2 AND status='pending' LIMIT 1",
                        params![run_id, tool_use_id],
                    )
                    .await?;
                Ok(rows.next().await?.is_some())
            })
        })
        .await
        .map_err(|error| {
            log::warn!(
                "durable suspension ownership validation failed for run {}: {}",
                r.run_id,
                error
            );
            SUSPENSION_UNAVAILABLE.to_string()
        })?;
    if permission_owns_call {
        log::warn!(
            "refusing durable suspension for run {} because permission owns tool call {}",
            r.run_id,
            r.tool_use_id
        );
        return Err(SUSPENSION_UNAVAILABLE.to_string());
    }
    Ok(())
}

/// Which kind of active row an establishing write lands.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Establish {
    /// An ordinary park: the row and the yielded turn land together.
    Park,
    /// A call held open on its socket: the row lands with `hold='inline'` and
    /// the turn keeps running.
    Hold,
}

/// Persist the suspension and, for a park, yield its turn in one write,
/// reporting whether the turn was the one that yielded (it is already parked
/// when a sibling call of the same turn got there first; a hold never yields).
async fn insert(db: &LocalDb, r: &Record) -> Result<bool, String> {
    insert_as(db, r, Establish::Park).await
}

async fn insert_as(db: &LocalDb, r: &Record, establish: Establish) -> Result<bool, String> {
    let run_id = r.run_id.clone();
    let r = r.clone();
    db.write(|c|{let r=r.clone();Box::pin(async move{
        if matches!(r.condition, Condition::RunBatch { .. }) {
            let mut rows = c.query(
                "SELECT 1 FROM permission_requests WHERE run_id=?1 AND tool_use_id=?2 AND status='pending' LIMIT 1",
                params![r.run_id.as_str(), r.tool_use_id.as_str()],
            ).await?;
            if rows.next().await?.is_some() {
                return Err(DbError::internal("permission request already owns this tool call"));
            }
        }
        let columns=ConditionColumns::of(&r.condition,r.deadline).map_err(DbError::internal)?;
        let hold = (establish == Establish::Hold).then_some("inline");
        c.execute(INSERT_WAIT_SQL,params![r.id.as_str(),r.job_id.as_str(),r.run_id.as_str(),r.session_id.as_str(),r.turn_id.as_str(),r.tool_use_id.as_str(),columns.json.as_str(),r.deadline,columns.kind,columns.due_at_ms,columns.next_poll_at_ms,columns.pending_prompt_id.as_deref(),r.created,hold]).await?;
        if establish == Establish::Hold {
            return Ok(false);
        }
        let yielded=yield_turn_for_host(c,&r.turn_id,TurnYieldReason::Wait).await.map_err(|e|DbError::internal(e.to_string()))?;
        Ok(yielded)
    })}).await.map_err(|error|{log::warn!("durable suspension insert failed for run {run_id}: {error}");SUSPENSION_UNAVAILABLE.to_string()})
}

/// What answering one suspension's call needs: which row, and which tool call in
/// which turn it belongs to.
struct SuspendedCall {
    id: String,
    run_id: String,
    session_id: String,
    turn_id: String,
    tool_use_id: String,
    /// What the call waits on, when its stored condition could be read.
    condition: Option<Condition>,
}

impl SuspendedCall {
    fn of(r: &Record) -> Self {
        Self {
            id: r.id.clone(),
            run_id: r.run_id.clone(),
            session_id: r.session_id.clone(),
            turn_id: r.turn_id.clone(),
            tool_use_id: r.tool_use_id.clone(),
            condition: Some(r.condition.clone()),
        }
    }
}

/// Clear the way for a new suspension, abandoning another turn's active row
/// only on evidence about THAT row (tenet 1421).
///
/// A partial unique index on `(job_id, tool_use_id)` allows at most one active
/// `agent_waits` row per call. A row that outlives the turn which created it
/// would refuse that call's next suspension for the rest of the job's life --
/// permanently degrading it to inline waiting -- unless it is cleared here
/// (CAIRN-3159). Another turn suspending is not, by itself, evidence that a
/// row was abandoned: a job does not run one turn at a time in every carrier
/// (a Codex exec cell outlives the provider turn that opened it), and an
/// inline-held run batch on an earlier turn is live work whose result the job
/// still needs (cairn/6145 comment #8). The evidence that counts:
///
/// 0. its resolution finished in fact and only its final write is missing;
/// 1. the row's call is the incoming one (same tool use id), so it would
///    collide with the index;
/// 2. a run batch whose batch is over: its task and its fleet request are both
///    gone from this runner, which placed it (a batch from before a restart
///    was settled by startup reconciliation);
/// 3. any other wait whose session is no longer the job's current one: the
///    session reconstruction CAIRN-3159 was written for, whose waiter is gone.
///
/// A row of the SAME turn is a sibling call parked alongside this one and is
/// never touched (CAIRN-3232). A live run batch on an earlier turn is left
/// pending; when it resolves, [`resolve`] hands its result to the job rather
/// than resuming a turn the job has moved past.
async fn supersede_abandoned(orch: &Orchestrator, db: &LocalDb, r: &Record) -> Result<(), String> {
    let current_session = db
        .query_opt(
            "SELECT current_session_id FROM jobs WHERE id=?1",
            params![r.job_id.clone()],
            |row| row.opt_text(0),
        )
        .await
        .ok()
        .flatten()
        .flatten();
    for wait in active_waits_for_job(db, &r.job_id).await? {
        if wait.turn_id == r.turn_id {
            continue;
        }
        // A row whose resolver already claimed its successor and delivered its
        // result is finished in fact; its own columns say so.
        complete_resolution_in_fact(orch, db, &wait).await?;
        let evidence = if wait.tool_use_id == r.tool_use_id {
            "its call is the one suspending now"
        } else {
            match &wait.condition {
                Some(Condition::RunBatch { request_id, .. }) => {
                    if super::run::detached::batch_is_live(orch, request_id) {
                        continue;
                    }
                    "its batch is no longer running"
                }
                _ if current_session
                    .as_deref()
                    .is_some_and(|current| current != wait.session_id) =>
                {
                    "its session is no longer the job's current one"
                }
                _ => continue,
            }
        };
        log::warn!(
            "superseding abandoned suspension {} (turn {}) so job {} can suspend turn {}: {evidence}",
            wait.id,
            wait.turn_id,
            r.job_id,
            r.turn_id
        );
        abandon(orch, db, &wait, AbandonCause::Superseded).await?;
    }
    Ok(())
}

/// Abandon every active suspension of `job_id` because the operator compacted
/// its session, reporting how many this call abandoned.
///
/// A row whose own resolver already claimed its successor is not abandoned
/// here (see [`abandon`]); that successor turn is settled by the compaction
/// itself, so its later adoption fails rather than reaching the new session.
pub(crate) async fn abandon_job_waits_for_operator_compact(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
) -> Result<usize, String> {
    let mut abandoned = 0;
    for wait in active_waits_for_job(db, job_id).await? {
        if abandon(orch, db, &wait, AbandonCause::OperatorCompact).await? {
            abandoned += 1;
        }
    }
    Ok(abandoned)
}

/// Abandon one suspension: record the decision, answer its call, close the row.
///
/// The order is the crash contract, and each step covers a different way a host
/// can die mid-abandonment.
///
/// The typed resolution is written FIRST, before anything observable, because it
/// is what startup reads to tell an abandonment apart from a live wait. Without
/// it, a restart would re-drive the row as an ordinary suspension, create a
/// `WaitResolved` successor for a turn the job has already left, and resume an
/// obsolete run — resurrecting the very suspension being discarded.
///
/// Closing the row comes LAST, because a row still in the re-driven set is a row
/// reconciliation can finish. Closing first would hide an unanswered call inside
/// a terminal state where nothing would ever look for it again.
async fn abandon(
    orch: &Orchestrator,
    db: &LocalDb,
    call: &SuspendedCall,
    cause: AbandonCause,
) -> Result<bool, String> {
    if !mark_abandoned(db, &call.id, cause).await? {
        // Its own resolver owns the resume, so this must not answer or cancel it.
        // But a row whose resolver already did everything a resolution does --
        // claimed its successor, delivered its result -- is finished in fact and
        // missing only its final state write, and leaving it active is what makes
        // waiting single-use for the rest of the job's life. Complete it instead.
        // A resolver genuinely mid-flight is left alone; this suspension is then
        // refused rather than raced, and the retry succeeds.
        complete_resolution_in_fact(orch, db, call).await?;
        return Ok(false);
    }
    // Answering holds the resolver guard when it is free, so the periodic sweep
    // never mistakes this delivery claim for a stale one. A live resolver that
    // already holds the guard is raced by the delivery claim itself, as before.
    let _guard = claim_resolver(orch, &call.id);
    finish_abandonment(orch, db, call, cause).await?;
    Ok(true)
}

/// Everything after the decision is recorded, and therefore also what startup
/// runs to finish an abandonment a restart interrupted. It never creates a
/// successor and never resumes the run.
async fn finish_abandonment(
    orch: &Orchestrator,
    db: &LocalDb,
    call: &SuspendedCall,
    cause: AbandonCause,
) -> Result<(), String> {
    deliver_abandonment(orch, db, call, cause).await;
    cancel_abandoned(db, &call.id, cause).await
}

/// Write the terminal state a resolution earned but never recorded.
///
/// A resolution that claimed its successor and delivered its result has done
/// everything a resolution does; if it then returned before its final
/// `resolving -> resolved` write — a failed continuation, a dropped task — the row
/// stays in the active set forever, and the partial unique index turns that into
/// "the first wait works and every later one fails" for the rest of the job's
/// life. The evidence for finishing it is the row's own columns, so this claims
/// nothing and races nothing: a resolver still mid-flight has not set them yet
/// and is left alone.
async fn complete_resolution_in_fact(
    orch: &Orchestrator,
    db: &LocalDb,
    call: &SuspendedCall,
) -> Result<(), String> {
    let id = call.id.clone();
    let now = chrono::Utc::now().timestamp_millis();
    db.write(|c| {
        let id = id.clone();
        Box::pin(async move {
            let completed = c
                .execute(
                    "UPDATE agent_waits SET state='resolved',resolved_at=COALESCE(resolved_at,?2) WHERE id=?1 AND state='resolving' AND successor_turn_id IS NOT NULL AND result_stored_at IS NOT NULL",
                    params![id.clone(), now],
                )
                .await?;
            if completed > 0 {
                log::warn!("suspension {id} had resolved in fact but never recorded it; completed");
            }
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())?;
    if matches!(
        call.condition,
        Some(Condition::RunBatch { host: false, .. })
    ) {
        crate::orchestrator::base_advance::on_branch_hold_released_for_run(orch, &call.run_id);
    }
    Ok(())
}

/// Record the decision to abandon a suspension, reporting whether abandonment
/// won the race against the row's own resolver.
///
/// `resolving` keeps the row in the set startup re-drives, and the typed
/// resolution is the durable discriminator that sends it to
/// [`finish_abandonment`] instead of a resume. Refusing a row whose continuation
/// is already owned is the other half of [`claim_continuation`]'s exclusion:
/// between abandoning this suspension and resuming it, exactly one write wins.
async fn mark_abandoned(db: &LocalDb, id: &str, cause: AbandonCause) -> Result<bool, String> {
    let id = id.to_string();
    db.write(|c| {
        let (id, result) = (id.clone(), abandoned_result(cause));
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT job_id,run_id,condition_json FROM agent_waits WHERE id=?1",
                    params![id.clone()],
                )
                .await?;
            let row = match rows.next().await? {
                Some(row) => Some((row.text(0)?, row.text(1)?, row.text(2)?)),
                None => None,
            };
            drop(rows);
            let now = chrono::Utc::now().timestamp_millis();
            let marked = c
                .execute(
                    "UPDATE agent_waits SET state='resolving',resolution_json=?2,resolving_at=COALESCE(resolving_at,?3) WHERE id=?1 AND state IN ('pending','resolving') AND successor_turn_id IS NULL",
                    params![id, result, now],
                )
                .await?;
            // Supersession ends only the wait: keep the batch's result and branch
            // hold durable. Operator Compact instead cancels session-owned work,
            // so it must not leave a detached row claiming the batch still runs.
            if let (AbandonCause::Superseded, true, Some((job_id, run_id, condition))) =
                (cause, marked > 0, row)
            {
                if let Ok(condition) = serde_json::from_str::<Condition>(&condition) {
                    super::run::detached::insert_converted(
                        c,
                        &job_id,
                        &run_id,
                        &condition,
                        "its wait was abandoned while the batch ran",
                        now,
                    )
                    .await?;
                }
            }
            Ok(marked > 0)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Answer an abandoned call, at most once, by winning the same claim the row's
/// own resolver would take. Best effort past the claim: the abandoned turn may
/// already be gone, in which case there is nothing to answer.
async fn deliver_abandonment(
    orch: &Orchestrator,
    db: &LocalDb,
    call: &SuspendedCall,
    cause: AbandonCause,
) {
    match claim_result_delivery(db, &call.id).await {
        Ok(true) => {}
        Ok(false) => return,
        Err(error) => {
            log::warn!(
                "could not claim delivery for suspension {}: {error}",
                call.id
            );
            return;
        }
    }
    // Only supersession leaves the batch running. Compact answers with its
    // typed cancellation cause, just like the other session-owned waits.
    let text = match (cause, &call.condition) {
        (AbandonCause::Superseded, Some(Condition::RunBatch { request_id, .. })) => serde_json::json!({
            "outcome": ABANDONED_OUTCOME,
            "error": format!(
                "This call's wait ended before its batch finished, but the batch keeps running. Cairn delivers its result to this job as a notice, and `{}` shows it now. Do not re-run it.",
                super::run::detached::handle_uri(request_id)
            ),
        })
        .to_string(),
        _ => abandoned_result(cause),
    };
    if let Err(error) = crate::execution::jobs::store_tool_result_event_with_turn(
        orch,
        &call.run_id,
        &call.session_id,
        &call.tool_use_id,
        &text,
        true,
        chrono::Utc::now().timestamp() as i32,
        Some(&call.turn_id),
    ) {
        log::warn!(
            "abandoned suspension {} left no result on its own turn: {error}",
            call.id
        );
        release_result_delivery(db, &call.id).await;
    }
}

/// Whether this suspension has visibly been overtaken — abandoned by a
/// reconstructed turn, or cancelled by a user Stop — since the resolver claimed
/// it.
///
/// This is an early out, deliberately not the fence. A read can only report the
/// past, so acting on it is a race no matter how close it sits to the action;
/// [`claim_continuation`] is what actually decides who resumes. What this buys
/// is not correctness but tidiness: an abandoned suspension stops before
/// building a successor turn nobody will ever start.
///
/// A row that has vanished is overtaken. A row that cannot be read is not: a
/// transient read error should not strand a healthy wait forever.
async fn overtaken(db: &LocalDb, id: &str) -> bool {
    match load_resolution(db, id).await {
        Ok(Some((state, resolution, _))) => {
            let overtaken = state == "cancelled" || is_abandonment(resolution.as_deref());
            if overtaken {
                log::warn!("suspension {id} was overtaken while resolving; standing down");
            }
            overtaken
        }
        Ok(None) => true,
        Err(error) => {
            log::warn!("could not re-read the decision for suspension {id}: {error}");
            false
        }
    }
}

/// Whether a stored resolution is an abandonment. This is the durable record of
/// a decision already made, so startup finishes it rather than reopening it.
fn is_abandonment(resolution: Option<&str>) -> bool {
    resolution
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
        .and_then(|value| {
            value
                .get("outcome")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .as_deref()
        == Some(ABANDONED_OUTCOME)
}

async fn active_waits_for_job(db: &LocalDb, job_id: &str) -> Result<Vec<SuspendedCall>, String> {
    let job_id = job_id.to_string();
    db.read(|c| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = c.query(
                "SELECT id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json FROM agent_waits WHERE job_id=?1 AND state IN ('pending','resolving')",
                params![job_id],
            ).await?;
            let mut found = Vec::new();
            while let Some(row) = rows.next().await? {
                found.push(SuspendedCall {
                    id: row.text(0)?,
                    run_id: row.text(1)?,
                    session_id: row.text(2)?,
                    turn_id: row.text(3)?,
                    tool_use_id: row.text(4)?,
                    condition: serde_json::from_str(&row.text(5)?).ok(),
                });
            }
            Ok(found)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Take an abandoned row out of the pending set with a typed outcome, so the
/// job can suspend again. Delivering that outcome is a separate step behind the
/// shared delivery claim, so cancelling never races another writer's answer.
async fn cancel_abandoned(db: &LocalDb, id: &str, cause: AbandonCause) -> Result<(), String> {
    let id = id.to_string();
    let now = chrono::Utc::now().timestamp_millis();
    db.write(|c| {
        let (id, result) = (id.clone(), abandoned_result(cause));
        Box::pin(async move {
            c.execute(
                "UPDATE agent_waits SET state='cancelled',resolution_json=COALESCE(resolution_json,?2),resolved_at=COALESCE(resolved_at,?3) WHERE id=?1 AND state IN ('pending','resolving')",
                params![id, result, now],
            ).await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Answer a suspended call, and — if it is the last of its turn's parked calls
/// to settle — resume the turn exactly once.
///
/// A turn may park several of its calls at once, so this splits into per-call
/// work and per-turn work. Answering the call on its own provider id is
/// per-call, and every resolver does it. Building the successor turn, claiming
/// the continuation and driving the resume are per-turn, and belong to whichever
/// call settles last. "Last one out drives" is not a policy choice: a turn
/// cannot resume while one of its own calls is still unanswered, so it is the
/// only correct rule.
pub(crate) async fn resolve(
    orch: &Orchestrator,
    db: &LocalDb,
    r: &Record,
    result: &str,
    replay: bool,
) -> Result<(), String> {
    let outcome = resolve_inner(orch, db, r, result, replay).await;
    // Every exit may follow a terminal state write, including sibling stand-down
    // and diversion to the job. The seam rechecks all branch holds, so an error
    // or a duplicate resolver that left this row active cannot release it early.
    if let Condition::RunBatch { host: false, .. } = &r.condition {
        crate::orchestrator::base_advance::on_branch_hold_released(orch, &r.job_id);
    }
    outcome
}

async fn resolve_inner(
    orch: &Orchestrator,
    db: &LocalDb,
    r: &Record,
    result: &str,
    replay: bool,
) -> Result<(), String> {
    // First-writer-wins claim: pending -> resolving. The row count distinguishes
    // the live resolver that won the transition from a duplicate that lost it; a
    // startup replay of an existing `resolving` row re-drives idempotently. A crash
    // from here on leaves a `resolving` row that replay re-drives idempotently
    // instead of losing the resume behind a `resolved` row.
    let (id, out) = (r.id.clone(), result.to_string());
    let claimed = db
        .write(|c| {
            let (id, out) = (id.clone(), out.clone());
            Box::pin(async move {
                let changed = c
                    .execute(
                        "UPDATE agent_waits SET state='resolving',resolution_json=?2,resolving_at=?3 WHERE id=?1 AND state='pending'",
                        params![id, out, chrono::Utc::now().timestamp_millis()],
                    )
                    .await?;
                Ok(changed)
            })
        })
        .await
        .map_err(|e| e.to_string())?;

    let Some((state, stored_result, stored_successor)) = load_resolution(db, &r.id).await? else {
        return Err(format!("suspension disappeared: {}", r.id));
    };
    match state.as_str() {
        // User Stop cancelled the suspension, or a prior resolution finished it.
        "cancelled" | "resolved" => return Ok(()),
        "resolving" => {}
        other => return Err(format!("suspension has invalid resolution state: {other}")),
    }
    // A live resolver that did not win the pending -> resolving transition defers to
    // the winner. Only a startup replay legitimately re-drives a `resolving` row.
    if !replay && claimed == 0 {
        return Ok(());
    }
    let result = stored_result.as_deref().unwrap_or(result);

    // Per-call work, done by every resolver of this turn: answer this call on
    // its own tool use id, exactly once.
    //
    // It happens BEFORE the election because a resolver that stands down never
    // reaches the per-turn work below, and its call is owed an answer either
    // way. Delivering ahead of `claim_continuation` also settles one race
    // deliberately: a resolver later overtaken by abandonment has already
    // written its REAL result, so the abandonment text loses the delivery claim
    // for that call. A real answer beats "this call's turn was replaced".
    store_result_once(orch, db, r, result).await?;

    // A run batch whose turn the job has already moved past (its call was held
    // inline on a turn that ended, or a later turn ran beside it) must not
    // resume that turn: the agent no longer reads it. Its result goes to the
    // job as a notice instead (tenet 1421). A row that already claimed its
    // successor is a replay of a resume in progress and goes on as one.
    if let Condition::RunBatch { .. } = &r.condition {
        if stored_successor.is_none() && turn_moved_past(db, r).await {
            return divert_to_job(orch, db, r, result, replay).await;
        }
    }

    // The election. A sibling call of this same turn still parked means this
    // resolver is not the last one out, and it is finished here.
    if stood_down_for_active_sibling(db, r).await? {
        return Ok(());
    }

    // Per-turn work from here down, reached by exactly one of the turn's
    // resolvers: resolve this turn's WaitResolved successor by explicit
    // identity, record it, and drive exactly one continuation carrying every
    // parked call's result. A racing continuation that already claimed the
    // predecessor's single successor yields a collision: defer to it rather
    // than hijacking a foreign turn.
    let prompt = resume_prompt_for_turn(db, r, result).await;
    let mut backoff = COLLISION_POLL_INITIAL;
    loop {
        // A cheap early out, not the fence: it keeps an already-abandoned
        // suspension from building a successor turn nobody will start. The
        // decision that actually governs resuming is the CAS below.
        if overtaken(db, &r.id).await {
            return Ok(());
        }
        match ensure_successor(orch, db, r, stored_successor.clone()).await? {
            SuccessorOutcome::Ready { turn_id, state } => {
                // The fence: win the row, or stand down. A supersession that
                // abandoned this suspension between the check above and here
                // takes the row instead, and this resolver resumes nothing.
                if !claim_continuation(db, &r.id, &turn_id).await? {
                    return Ok(());
                }
                // Drive continuation only while the successor still awaits its start.
                // A successor already started (or interrupted by a crash mid-resume)
                // means the resume was already delivered once; replay must not launch
                // a second one -- crash recovery owns an interrupted successor here.
                if state == TurnState::Pending {
                    let goal_owner = resolved_cohort_goal_owner(orch, db, r).await;
                    #[cfg(test)]
                    tests::before_owned_continuation(&r.id).await;
                    continue_job_impl(
                        orch,
                        &r.job_id,
                        Some(&prompt),
                        None,
                        Some(ResumeContext {
                            suppress_user_event: true,
                            preclaimed_successor_turn_id: Some(turn_id),
                            answers_open_tool_call: true,
                            goal_reactivation: goal_owner.map(
                                crate::execution::jobs::GoalReactivation::OwnedConditionResolved,
                            ),
                            ..Default::default()
                        }),
                    )?;
                }
                break;
            }
            SuccessorOutcome::Collision {
                state: TurnState::Pending,
                ..
            } => {
                // The predecessor's single successor was created by another
                // continuation that has NOT started it yet, so the run is still parked
                // -- it has not resumed. Recheck (with backoff) until that continuation
                // starts it (running) or terminalizes it, rather than falsely
                // resolving now (which would strand the parked agent) or giving up on
                // an arbitrary cutoff a slow-but-healthy continuation could exceed.
                // Host shutdown drops this task; startup reconciliation re-drives the
                // still-`resolving` row.
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(COLLISION_POLL_MAX);
            }
            SuccessorOutcome::Collision {
                turn_id,
                start_reason,
                state,
            } => {
                // A running or already-finished continuation resumed the run through
                // the predecessor's successor (e.g. a user steer mid-suspension): the
                // run resumed exactly once via that path. The awaited result is
                // already in the transcript (delivered above), so resolve without a
                // second, competing resume. Live delivery into an already-active turn
                // is precluded by one-active-turn, so the result is preserved there
                // rather than reaching this turn's prompt.
                log::warn!(
                    "suspension {} resolved into an existing {start_reason} successor {turn_id} (state {state}); recorded result without a second resume",
                    r.id
                );
                break;
            }
        }
    }

    // Mark resolved (idempotent CAS on the resolving state).
    let id = r.id.clone();
    db.write(|c|{let id=id.clone();Box::pin(async move{c.execute("UPDATE agent_waits SET state='resolved',resolved_at=?2 WHERE id=?1 AND state='resolving'",params![id,chrono::Utc::now().timestamp_millis()]).await?;Ok(())})}).await.map_err(|e|e.to_string())?;
    Ok(())
}

/// Whether the job has moved past the turn this call came from: a later turn
/// of the same session exists, or the job now runs a different session. A
/// read failure is not evidence, so it reads as not moved past.
async fn turn_moved_past(db: &LocalDb, r: &Record) -> bool {
    db.query_one(
        "SELECT EXISTS(SELECT 1 FROM turns t JOIN turns p ON p.id=?1 \
            WHERE t.session_id=p.session_id AND t.sequence>p.sequence) \
         OR EXISTS(SELECT 1 FROM jobs WHERE id=?2 AND current_session_id IS NOT NULL \
            AND current_session_id<>?3)",
        params![r.turn_id.clone(), r.job_id.clone(), r.session_id.clone()],
        |row| row.i64(0),
    )
    .await
    .is_ok_and(|moved| moved != 0)
}

/// Settle a run-batch wait whose turn is gone by handing its batch's result to
/// the job: the wait resolves, and the batch becomes a detached one in the
/// same write. A live resolver settles that row with the batch's verdict right
/// after; a replay has no resolver behind it and settles it here.
async fn divert_to_job(
    orch: &Orchestrator,
    db: &LocalDb,
    r: &Record,
    result: &str,
    replay: bool,
) -> Result<(), String> {
    let Condition::RunBatch { request_id, .. } = r.condition.clone() else {
        return Ok(());
    };
    let (id, job_id, run_id, condition) = (
        r.id.clone(),
        r.job_id.clone(),
        r.run_id.clone(),
        r.condition.clone(),
    );
    let diverted = db.write(|c| {
        let (id, job_id, run_id, condition) =
            (id.clone(), job_id.clone(), run_id.clone(), condition.clone());
        Box::pin(async move {
            // Compact or supersession may have overtaken the resolver since its
            // state read. The abandonment owns the answer and terminal state;
            // never convert it back into an ordinary detached result.
            let mut rows = c.query(
                "SELECT state,resolution_json,successor_turn_id FROM agent_waits WHERE id=?1",
                params![id.clone()],
            ).await?;
            let Some(row) = rows.next().await? else {
                return Ok(false);
            };
            let state = row.text(0)?;
            let resolution = row.opt_text(1)?;
            let successor = row.opt_text(2)?;
            drop(rows);
            if state != "resolving" || successor.is_some() || is_abandonment(resolution.as_deref()) {
                return Ok(false);
            }
            let now = chrono::Utc::now().timestamp_millis();
            super::run::detached::insert_converted(
                c,
                &job_id,
                &run_id,
                &condition,
                "its turn had ended when the batch finished",
                now,
            )
            .await?;
            c.execute(
                "UPDATE agent_waits SET state='resolved',resolved_at=?2 WHERE id=?1 AND state='resolving'",
                params![id, now],
            )
            .await?;
            Ok(true)
        })
    })
    .await
    .map_err(|e| e.to_string())?;
    if !diverted {
        return Ok(());
    }
    log::info!(
        "run batch suspension {} resolved after its turn ended; its result goes to job {} as a notice",
        r.id,
        r.job_id
    );
    if replay {
        super::run::detached::settle_and_deliver(
            orch,
            db,
            &request_id,
            &super::run::detached::verdict_from_text(result),
            result,
        )
        .await;
    }
    Ok(())
}

/// Decide, in one statement, whether this resolver is the last of its turn's
/// parked calls to settle.
///
/// `true` means a sibling of the same turn is still parked, so this row stood
/// down: it is marked resolved and its work is over. `false` means it is the
/// last one out and owns the turn's single resume.
///
/// It is ONE statement on purpose, and that is what makes it a single-winner
/// election rather than a race. Two siblings both reach it; whichever the writer
/// serializes second sees the first already `resolved`, fails the `EXISTS`, and
/// becomes the driver. Neither can stand down on the other, and neither can both
/// drive. Split into a read and a write, both would observe an active sibling in
/// the window between them and both would retire, stranding the parked turn.
///
/// Standing down is crash-safe because it happens only after this row has
/// delivered its own result: leaving the set startup re-drives costs nothing,
/// while the driver stays `resolving` until after its continuation, so a crash
/// leaves exactly one row for reconciliation to re-drive.
///
/// With a single parked call there is no sibling, the `EXISTS` is false, nothing
/// is updated, and everything below runs exactly as it always has.
/// The last resolver delivers the entire predecessor's cohort. Its own call
/// need not be the last one that staked the provider pause. Authority to release
/// that other call comes from its settled durable row, never just the live claim.
async fn resolved_cohort_goal_owner(
    orch: &Orchestrator,
    db: &LocalDb,
    r: &Record,
) -> Option<crate::agent_process::process::GoalPauseOwner> {
    use crate::agent_process::process::{GoalPause, GoalPauseOwner};
    let control = orch
        .process_state
        .processes
        .lock()
        .ok()
        .and_then(|processes| {
            processes
                .get(&r.run_id)
                .map(|process| process.goal_control())
        });
    let claim = control.and_then(|control| {
        control
            .with_control(|_, claim| Ok(claim.inspect()))
            .ok()
            .flatten()
    });
    let wait_id = match claim {
        Some(GoalPause::Owned(GoalPauseOwner::DurableWait { wait_id })) => wait_id,
        _ => r.id.clone(),
    };
    let verified = db.query_one(
        "SELECT EXISTS(SELECT 1 FROM agent_waits owner WHERE owner.id=?1 AND owner.run_id=?2 AND owner.session_id=?3 AND owner.job_id=?4 AND owner.predecessor_turn_id=?5 AND owner.state IN ('resolving','resolved') AND owner.result_stored_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM agent_waits sibling WHERE sibling.job_id=owner.job_id AND sibling.predecessor_turn_id=owner.predecessor_turn_id AND sibling.state IN ('pending','resolving') AND sibling.id<>?6))",
        params![wait_id.clone(), r.run_id.clone(), r.session_id.clone(), r.job_id.clone(), r.turn_id.clone(), r.id.clone()],
        |row| row.i64(0),
    ).await;
    match verified {
        Ok(1) => Some(GoalPauseOwner::DurableWait { wait_id }),
        Ok(_) => None,
        Err(error) => {
            log::warn!(
                "could not verify native goal cohort for wait {}: {error}",
                r.id
            );
            None
        }
    }
}

async fn stood_down_for_active_sibling(db: &LocalDb, r: &Record) -> Result<bool, String> {
    let (id, job_id, turn_id) = (r.id.clone(), r.job_id.clone(), r.turn_id.clone());
    let now = chrono::Utc::now().timestamp_millis();
    let stood_down = db
        .write(|c| {
            let (id, job_id, turn_id) = (id.clone(), job_id.clone(), turn_id.clone());
            Box::pin(async move {
                let changed = c
                    .execute(
                        "UPDATE agent_waits SET state='resolved',resolved_at=?4 WHERE id=?1 AND state='resolving' AND EXISTS (SELECT 1 FROM agent_waits sibling WHERE sibling.job_id=?2 AND sibling.predecessor_turn_id=?3 AND sibling.id<>?1 AND sibling.state IN ('pending','resolving'))",
                        params![id, job_id, turn_id, now],
                    )
                    .await?;
                Ok(changed)
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    if stood_down > 0 {
        log::debug!(
            "suspension {} settled while a sibling call of turn {} was still parked; standing down",
            r.id,
            r.turn_id
        );
        return Ok(true);
    }
    // Zero rows also covers a row that is no longer `resolving` -- cancelled by
    // a Stop, or abandoned by a reconstructed turn. Falling through is right:
    // `overtaken` and `claim_continuation` below are precisely the checks that
    // handle it, and they already say so in the log.
    Ok(false)
}

/// One parked call of a turn, as the composed resume prompt needs it.
struct ParkedCall {
    condition: Condition,
    tool_use_id: String,
    resolution: Option<String>,
}

/// The single prompt the resumed turn wakes with, carrying every one of its
/// parked calls' results.
///
/// This is the channel the MODEL reads, and it is why the turn resumes once
/// rather than N times. Each parked call separately delivers its own synthetic
/// `tool_result` to its own provider id, which keeps the transcript well-formed
/// and the UI honest -- but the resumed agent reads this string, forwarded as
/// its resume prompt (`execution::jobs::lifecycle::assemble_resume_prompt`). So
/// this is the one place N results have to become one message.
async fn resume_prompt_for_turn(db: &LocalDb, r: &Record, driver_result: &str) -> String {
    let calls = match parked_calls_of_turn(db, r).await {
        Ok(calls) => calls,
        Err(error) => {
            // A prompt is owed regardless. Resuming with this call's own result
            // gives the agent less than it should have; resuming with nothing
            // strands it.
            log::warn!(
                "could not read the parked calls of turn {} ({error}); resuming with only this call's result",
                r.turn_id
            );
            Vec::new()
        }
    };
    compose_resume_prompt(&calls, driver_result)
}

/// Compose one resume prompt from a turn's parked calls.
///
/// With a single answered call the prompt is that call's result VERBATIM. That
/// is the overwhelmingly common path and its bytes are the contract: every
/// suspended `run` batch and `waitFor` reads exactly what it read before this
/// existed. Only a genuinely concurrent turn gets the labeled form, because an
/// unlabeled concatenation would ask the model to guess which answer belongs to
/// which call.
///
/// A row with no recorded resolution is skipped rather than rendered empty. It
/// cannot happen in practice -- a row only leaves the active set with its
/// resolution written, and an active sibling would have won the election -- and
/// if it somehow did, degrading to this call's own result beats emitting a
/// section that answers nothing. So is a row whose resolution answers nothing
/// on resume: a held call that already answered on its own socket
/// ([`DELIVERED_INLINE_OUTCOME`]), and a row that handed its call over to an
/// inner wait ([`TRANSFERRED_OUTCOME`]), whose own row answers that call.
fn compose_resume_prompt(calls: &[ParkedCall], driver_result: &str) -> String {
    let answered: Vec<&ParkedCall> = calls
        .iter()
        .filter(|call| {
            call.resolution.as_deref().is_some_and(|resolution| {
                resolution != TRANSFERRED_OUTCOME && resolution != DELIVERED_INLINE_OUTCOME
            })
        })
        .collect();
    if answered.len() < 2 {
        return driver_result.to_string();
    }
    let sections: Vec<String> = answered
        .iter()
        .map(|call| {
            format!(
                "--- {} ({}) ---\n{}",
                call.condition.label(),
                call.tool_use_id,
                call.resolution.as_deref().unwrap_or_default()
            )
        })
        .collect();
    format!(
        "{} calls in this turn were suspended and have now finished. Each result below is labeled with the call it answers.\n\n{}",
        answered.len(),
        sections.join("\n\n")
    )
}

/// Every call one turn parked, oldest first, whatever state each has reached.
/// Siblings that already stood down are included: their results are exactly
/// what the composed prompt exists to carry.
async fn parked_calls_of_turn(db: &LocalDb, r: &Record) -> Result<Vec<ParkedCall>, String> {
    let (job_id, turn_id) = (r.job_id.clone(), r.turn_id.clone());
    db.read(|c| {
        let (job_id, turn_id) = (job_id.clone(), turn_id.clone());
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT condition_json,tool_use_id,resolution_json FROM agent_waits WHERE job_id=?1 AND predecessor_turn_id=?2 ORDER BY created_at,id",
                    params![job_id, turn_id],
                )
                .await?;
            let mut found = Vec::new();
            while let Some(row) = rows.next().await? {
                let condition_json = row.text(0)?;
                let condition = serde_json::from_str::<Condition>(&condition_json)
                    .map_err(|error| DbError::internal(error.to_string()))?;
                found.push(ParkedCall {
                    condition,
                    tool_use_id: row.text(1)?,
                    resolution: row.opt_text(2)?,
                });
            }
            Ok(found)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Resolution of a suspension's successor turn.
enum SuccessorOutcome {
    /// This suspension's own `WaitResolved` successor and its current state. A
    /// `Pending` state still awaits its start (drive the continuation); any other
    /// state means the resume was already delivered (replay must not re-drive it).
    Ready { turn_id: String, state: TurnState },
    /// A racing continuation already owns the predecessor's single successor; the
    /// suspension must not hijack that foreign turn. `state` decides the handling
    /// -- a pending foreign turn means the run has not resumed yet.
    Collision {
        turn_id: String,
        start_reason: String,
        state: TurnState,
    },
}

/// Resolve this suspension's `WaitResolved` successor by explicit identity (never
/// by predecessor alone): reuse its own successor on replay, create it pending
/// when absent, and report a collision when a foreign continuation already claimed
/// the predecessor's single successor.
async fn ensure_successor(
    orch: &Orchestrator,
    db: &LocalDb,
    r: &Record,
    stored_successor: Option<String>,
) -> Result<SuccessorOutcome, String> {
    let outcome = ensure_wait_resolved_successor(db, &r.run_id, &r.turn_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("suspension successor context missing for run {}", r.run_id))?;
    match outcome {
        WaitSuccessor::Ready(update) => {
            // Emits the insert db-change only when this attempt created the turn.
            emit_successor_turn_events(db, &*orch.services.emitter, &update).await;
            if let Some(stored) = stored_successor.as_deref() {
                if stored != update.turn_id {
                    log::warn!(
                        "suspension {} recorded successor {stored} but resolved {}",
                        r.id,
                        update.turn_id
                    );
                }
            }
            let state = load_turn_state(db, &update.turn_id).await?;
            Ok(SuccessorOutcome::Ready {
                turn_id: update.turn_id,
                state,
            })
        }
        WaitSuccessor::Collision {
            turn_id,
            start_reason,
            state,
        } => Ok(SuccessorOutcome::Collision {
            turn_id,
            start_reason,
            state,
        }),
    }
}

async fn load_turn_state(db: &LocalDb, turn_id: &str) -> Result<TurnState, String> {
    let turn_id = turn_id.to_string();
    db.read(|c| {
        let turn_id = turn_id.clone();
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT state FROM turns WHERE id=?1 LIMIT 1",
                    params![turn_id.clone()],
                )
                .await?;
            let state = rows
                .next()
                .await?
                .ok_or_else(|| DbError::Row(format!("turn not found: {turn_id}")))?
                .text(0)?;
            state.parse::<TurnState>().map_err(DbError::Row)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn load_resolution(
    db: &LocalDb,
    id: &str,
) -> Result<Option<(String, Option<String>, Option<String>)>, String> {
    let id = id.to_string();
    db.read(|c| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT state,resolution_json,successor_turn_id FROM agent_waits WHERE id=?1",
                    params![id],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| Ok((row.text(0)?, row.opt_text(1)?, row.opt_text(2)?)))
                .transpose()
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Claim the sole right to resume this run, reporting whether this resolver won
/// it.
///
/// This is the linearization point between resolving a suspension and
/// abandoning it. It and [`mark_abandoned`] are two updates to one row with
/// mutually exclusive predicates — this one refuses a row already marked
/// abandoned, that one refuses a row whose continuation is already owned — so
/// the database picks exactly one winner and the loser stands down.
///
/// Re-reading the decision before resuming cannot do this job. However close the
/// read sits to the resume, an abandonment can commit in between, and the stale
/// resolver resumes anyway; moving the read only shrinks the window. The winner
/// has to be decided by the same write that acts on it.
///
/// Owning the successor is re-entrant for a replay re-driving its own recorded
/// successor, which is how crash recovery re-enters this path after an
/// interrupted resume.
///
/// The abandonment test is an exact payload comparison rather than SQL-side JSON
/// extraction, because `resolution_json` also carries settled `run` batch output,
/// which is not JSON at all. It therefore names every abandonment payload
/// ([`AbandonCause::ALL`]); one it missed would let a resolver resume a turn
/// that was deliberately abandoned.
async fn claim_continuation(db: &LocalDb, id: &str, successor: &str) -> Result<bool, String> {
    let (id, successor) = (id.to_string(), successor.to_string());
    db.write(|c| {
        let (id, successor) = (id.clone(), successor.clone());
        let [superseded, compacted] = AbandonCause::ALL.map(abandoned_result);
        Box::pin(async move {
            let owned = c
                .execute(
                    "UPDATE agent_waits SET successor_turn_id=?2 WHERE id=?1 AND state='resolving' AND (successor_turn_id IS NULL OR successor_turn_id=?2) AND (resolution_json IS NULL OR resolution_json NOT IN (?3,?4))",
                    params![id, successor, superseded, compacted],
                )
                .await?;
            Ok(owned > 0)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Claim the sole right to answer a suspended call, reporting whether this
/// writer won it.
///
/// Two writers can answer one suspended call: its live resolver and a
/// supersession that found the row abandoned. Both claim through this single
/// statement and only the winner appends an event, so one tool use is never
/// answered twice.
///
/// The claim must be taken BEFORE the append, and atomically. Reading the slot,
/// appending, then marking it leaves a window in which a racing writer reads an
/// unclaimed slot and appends behind the first — which is what a
/// read-then-append-then-mark resolver did, defeating the marker entirely.
async fn claim_result_delivery(db: &LocalDb, id: &str) -> Result<bool, String> {
    let id = id.to_string();
    let now = chrono::Utc::now().timestamp_millis();
    db.write(|c| {
        let id = id.clone();
        Box::pin(async move {
            let claimed = c
                .execute(
                    "UPDATE agent_waits SET result_stored_at=?2 WHERE id=?1 AND result_stored_at IS NULL",
                    params![id, now],
                )
                .await?;
            Ok(claimed > 0)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Whether this call's answer is already in the transcript.
///
/// The event is the fact; `result_stored_at` is only a lock taken over it. They
/// cannot be written atomically together — the event insert is routed to the
/// run's owning replica through its own transaction — so anything that must
/// survive a restart asks the transcript, not the lock.
async fn result_already_delivered(
    db: &LocalDb,
    run_id: &str,
    tool_use_id: &str,
) -> Result<bool, String> {
    let (run_id, tool_use_id) = (run_id.to_string(), tool_use_id.to_string());
    db.read(|c| {
        let (run_id, tool_use_id) = (run_id.clone(), tool_use_id.clone());
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT 1 FROM events WHERE run_id=?1 AND event_type='tool_result' AND json_extract(data,'$.toolUseId')=?2 LIMIT 1",
                    params![run_id, tool_use_id],
                )
                .await?;
            Ok(rows.next().await?.is_some())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Release a delivery claim whose writer is gone, so the re-drive can still
/// answer the call.
///
/// A claim is a lock, not a fact. A host that died between committing the claim
/// and appending the event leaves it held with nothing behind it, and every
/// later writer — including startup's own replay — would then decline to answer,
/// losing the result permanently. No delivery survives a restart, so at startup
/// (`stale_after: None`) a claim with no event behind it is stale by
/// construction.
///
/// While the host is up, a claim can also be orphaned by a failed release after
/// a failed append. The periodic sweep passes a staleness bound: a real append
/// is one write with seconds of retry, so a claim older than the bound with no
/// event behind it is a failed release, never a live append. An unreadable
/// transcript is treated as delivered: declining to answer twice is the safer
/// failure.
async fn reclaim_interrupted_delivery(
    db: &LocalDb,
    call: &SuspendedCall,
    stale_after: Option<Duration>,
) {
    let Ok(Some(claimed_at)) = load_delivery_claim(db, &call.id).await else {
        return;
    };
    if let Some(bound) = stale_after {
        let age_ms = chrono::Utc::now().timestamp_millis() - claimed_at;
        if age_ms < bound.as_millis() as i64 {
            return;
        }
    }
    if result_already_delivered(db, &call.run_id, &call.tool_use_id)
        .await
        .unwrap_or(true)
    {
        return;
    }
    log::warn!(
        "suspension {} held a delivery claim with no result behind it; reclaimed",
        call.id
    );
    release_result_delivery(db, &call.id).await;
}

async fn load_delivery_claim(db: &LocalDb, id: &str) -> Result<Option<i64>, String> {
    let id = id.to_string();
    db.read(|c| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = c
                .query(
                    "SELECT result_stored_at FROM agent_waits WHERE id=?1",
                    params![id],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| row.opt_i64(0))
                .transpose()
                .map(Option::flatten)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Give back a claim whose append never landed, so a later attempt can retry it.
/// This covers a returned error; a host that dies mid-append is covered by
/// [`reclaim_interrupted_delivery`] at startup.
async fn release_result_delivery(db: &LocalDb, id: &str) {
    let owned = id.to_string();
    let released = db
        .write(|c| {
            let id = owned.clone();
            Box::pin(async move {
                c.execute(
                    "UPDATE agent_waits SET result_stored_at=NULL WHERE id=?1",
                    params![id],
                )
                .await?;
                Ok(())
            })
        })
        .await;
    if let Err(error) = released {
        log::warn!("failed to release the delivery claim for suspension {id}: {error}");
    }
}

/// Append the answer this writer has already claimed, releasing the claim if the
/// append fails. Split from the claim so the two halves are separately
/// observable: everything between them is the window a racing writer must not
/// be able to answer in.
async fn append_claimed_result(
    orch: &Orchestrator,
    db: &LocalDb,
    r: &Record,
    result: &str,
) -> Result<(), String> {
    #[cfg(test)]
    if let Some(error) = tests::fail_result_append(&r.id) {
        release_result_delivery(db, &r.id).await;
        return Err(error);
    }
    if let Err(error) = crate::execution::jobs::store_tool_result_event_with_turn(
        orch,
        &r.run_id,
        &r.session_id,
        &r.tool_use_id,
        result,
        false,
        chrono::Utc::now().timestamp() as i32,
        Some(&r.turn_id),
    ) {
        release_result_delivery(db, &r.id).await;
        return Err(error);
    }
    Ok(())
}

async fn store_result_once(
    orch: &Orchestrator,
    db: &LocalDb,
    r: &Record,
    result: &str,
) -> Result<(), String> {
    if !claim_result_delivery(db, &r.id).await? {
        return Ok(());
    }
    append_claimed_result(orch, db, r, result).await
}

/// This host's claim to be the one live resolver of a suspension.
///
/// The claim lives in memory, not in a lease column, because every resolver is
/// a task in the one host that owns resolution -- startup reconciliation already
/// rests on "no delivery survives a restart". A lease would need renewing
/// through the unbounded collision backoff in [`resolve`] and an expiry policy,
/// all for a cross-host case nothing else supports. Dropping the guard (return,
/// panic, or a cancelled task) releases the claim.
pub(crate) struct ResolverGuard {
    resolvers: Arc<std::sync::Mutex<HashSet<String>>>,
    id: String,
}

impl Drop for ResolverGuard {
    fn drop(&mut self) {
        self.resolvers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

/// Become the live resolver of suspension `id`, or `None` if another task in
/// this host already is.
pub(crate) fn claim_resolver(orch: &Orchestrator, id: &str) -> Option<ResolverGuard> {
    let claimed = orch
        .suspension_resolvers
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(id.to_string());
    claimed.then(|| ResolverGuard {
        resolvers: orch.suspension_resolvers.clone(),
        id: id.to_string(),
    })
}

/// The suspensions with a live resolver in this host right now.
pub(crate) fn live_resolvers(orch: &Orchestrator) -> HashSet<String> {
    orch.suspension_resolvers
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// The one entry point that drives a suspension's resolution. Every resolver --
/// a settled `run` batch, an owned wait's trigger, an MCP continuation, startup
/// reconciliation, and the periodic sweep -- goes through here rather than
/// calling [`resolve`] directly.
///
/// A resolution that fails while the host stays up is retried here, not left
/// for the next boot: a parked agent is reached only through its own resolution
/// (an escalated message ends a `waitFor` park by driving this same path), so a
/// row stranded in `resolving` strands the agent with it (cairn/5778).
pub(crate) async fn drive_resolution(
    orch: &Orchestrator,
    db: &LocalDb,
    record: &Record,
    result: &str,
    replay: bool,
) {
    let Some(guard) = claim_resolver(orch, &record.id) else {
        log::info!(
            "suspension {} already has a live resolver in this host; deferring to it",
            record.id
        );
        return;
    };
    drive_claimed(orch, db, record, result, replay, &guard).await;
}

/// The retry loop behind [`drive_resolution`], for a caller already holding the
/// suspension's guard.
///
/// After each failure the row's durable state decides what happens next:
///
/// - Gone, `resolved`, or `cancelled`: something else finished it.
/// - `pending`: the `pending -> resolving` write never landed, so `result` held
///   here is the only copy of the outcome. Keep trying, without a bound.
/// - `resolving` with an abandonment: finish the abandonment, never resume.
/// - `resolving` otherwise: the stored resolution is authoritative, so re-drive
///   it as a replay. That is bounded; past [`DURABLE_RETRY_ATTEMPTS`] the
///   periodic sweep owns the long tail.
///
/// A recorded successor does not stop the retry. A continuation that failed
/// after `claim_continuation` leaves its successor `pending` with nothing to
/// start it, and a replay is exactly what relaunches it.
///
/// Before any retry, a row whose node can no longer take a turn is settled
/// rather than retried (see [`settle_if_unresumable`]): that failure is
/// permanent, and retrying it is a stuck row with log spam.
async fn drive_claimed(
    orch: &Orchestrator,
    db: &LocalDb,
    record: &Record,
    result: &str,
    mut replay: bool,
    _guard: &ResolverGuard,
) {
    let mut backoff = RESOLVE_RETRY_INITIAL;
    let mut durable_attempts = 0;
    for attempt in 1u32.. {
        let error = match resolve(orch, db, record, result, replay).await {
            Ok(()) => return,
            Err(error) => error,
        };
        log::warn!(
            "suspension {} resolution attempt {attempt} failed: {error}",
            record.id
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(RESOLVE_RETRY_MAX);
        let (state, stored) = match load_resolution(db, &record.id).await {
            Ok(Some((state, stored, _))) => (state, stored),
            Ok(None) => return,
            Err(read_error) => {
                log::warn!(
                    "could not re-read suspension {} before retrying: {read_error}",
                    record.id
                );
                continue;
            }
        };
        match state.as_str() {
            "resolved" | "cancelled" => return,
            "pending" | "resolving" => {}
            other => {
                log::warn!(
                    "suspension {} has invalid resolution state {other}; not retrying",
                    record.id
                );
                return;
            }
        }
        if state == "resolving" && is_abandonment(stored.as_deref()) {
            let cause = AbandonCause::of(stored.as_deref());
            if let Err(error) =
                finish_abandonment(orch, db, &SuspendedCall::of(record), cause).await
            {
                log::warn!(
                    "could not finish abandoned suspension {}: {error}",
                    record.id
                );
            }
            return;
        }
        if settle_if_unresumable(orch, db, record).await {
            return;
        }
        if state == "pending" {
            continue;
        }
        replay = true;
        durable_attempts += 1;
        if durable_attempts >= DURABLE_RETRY_ATTEMPTS {
            log::warn!(
                "suspension {} still failing after {durable_attempts} retries of its stored result; the periodic sweep owns it now",
                record.id
            );
            return;
        }
    }
}

/// Why this suspension's node can no longer take a turn, or `None` if it can.
///
/// These are the permanent refusals: a terminal job (a terminal issue settles
/// its jobs), a job that has moved on to a newer session than the one this
/// call parked in, and a current session that refuses every further turn. A
/// read failure is not evidence of any of them.
async fn unresumable_cause(db: &LocalDb, record: &Record) -> Option<String> {
    let job = db
        .query_opt(
            "SELECT status,current_session_id FROM jobs WHERE id=?1",
            params![record.job_id.as_str()],
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await;
    let (status, current_session) = match job {
        Ok(Some(job)) => job,
        Ok(None) => return Some("its node no longer exists".into()),
        Err(_) => return None,
    };
    if matches!(status.as_str(), "complete" | "failed" | "cancelled") {
        return Some(format!("its node's job is {status}"));
    }
    if current_session
        .as_deref()
        .is_some_and(|current| current != record.session_id)
    {
        return Some("its node has moved on to a newer session".into());
    }
    if crate::execution::jobs::current_session_refuses_turns(db, &record.job_id).await {
        return Some("its node's session is closed and cannot take another turn".into());
    }
    None
}

/// The resolution a suspension is settled with when its node can no longer
/// take a turn. It names the cause and keeps whatever result the row held.
fn unresumable_resolution(cause: &str, stored: Option<&str>) -> String {
    serde_json::json!({
        "outcome": "cancelled",
        "error": format!("This call's wait was settled without resuming because {cause}."),
        "result": stored,
    })
    .to_string()
}

/// Settle a suspension whose node can no longer take a turn, the way a Stop
/// settles one: `cancelled`, with a resolution naming why. Reports whether it
/// settled the row.
async fn settle_if_unresumable(orch: &Orchestrator, db: &LocalDb, record: &Record) -> bool {
    let Some(cause) = unresumable_cause(db, record).await else {
        return false;
    };
    let (id, now) = (record.id.clone(), chrono::Utc::now().timestamp_millis());
    let settled = db
        .write(|c| {
            let (id, cause) = (id.clone(), cause.clone());
            Box::pin(async move {
                let mut rows = c
                    .query(
                        "SELECT resolution_json FROM agent_waits WHERE id=?1",
                        params![id.clone()],
                    )
                    .await?;
                let stored = match rows.next().await? {
                    Some(row) => row.opt_text(0)?,
                    None => return Ok(false),
                };
                drop(rows);
                let changed = c
                    .execute(
                        "UPDATE agent_waits SET state='cancelled',resolution_json=?2,resolved_at=COALESCE(resolved_at,?3) WHERE id=?1 AND state IN ('pending','resolving')",
                        params![id, unresumable_resolution(&cause, stored.as_deref()), now],
                    )
                    .await?;
                Ok(changed > 0)
            })
        })
        .await;
    match settled {
        Ok(settled) => {
            if settled {
                log::warn!("suspension {} settled without resuming: {cause}", record.id);
                if let Condition::RunBatch { host: false, .. } = &record.condition {
                    crate::orchestrator::base_advance::on_branch_hold_released(
                        orch,
                        &record.job_id,
                    );
                }
            }
            settled
        }
        Err(error) => {
            log::warn!(
                "could not settle unresumable suspension {}: {error}",
                record.id
            );
            false
        }
    }
}

/// Re-drive every suspension left pending or mid-resolution by a host restart.
///
/// A waitFor condition is level-triggered and can simply be re-armed. A run
/// batch cannot: its trigger was an in-memory join handle that died with the
/// old process. But a restart is not evidence the batch died, since it may
/// still be running on its machine, so it is neither cancelled nor declared
/// lost: its wait becomes a detached batch (which keeps holding its branch),
/// the call is told so, and the batch's machine's report settles it later
/// (`run::detached::sweep_unreported`).
pub(crate) async fn reconcile(orch: &Orchestrator) {
    for db in orch.db.all_dbs().await {
        let records = db.read(|conn| Box::pin(async move {
            let mut rows = conn.query(
                "SELECT id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,deadline_ms,created_at,state,resolution_json,hold FROM agent_waits WHERE state IN ('pending','resolving')",
                (),
            ).await?;
            let mut found = Vec::new();
            while let Some(row) = rows.next().await? {
                found.push((record_from_row(&row)?, row.text(9)?, row.opt_text(10)?, row.opt_text(11)?.is_some()));
            }
            Ok(found)
        })).await;
        let Ok(records) = records else {
            log::warn!("failed to load pending suspensions during startup");
            continue;
        };
        for (record, row_state, stored_result, held) in records {
            // A call held open on its socket when the host died: its socket is
            // gone with the host, so it becomes the park it would have been,
            // and this reconciler is its turn's only resumer. Yielding here as
            // well as in the stale-run sweep makes the two boot sweeps
            // order-independent.
            if held {
                if let Err(error) = release_held_turn(&db, &record).await {
                    log::warn!(
                        "startup could not release held call {}'s turn: {error}",
                        record.id
                    );
                }
            }
            // A pending MCP continuation is owned by the one host-global
            // scheduler. At startup its durable row is already normalized; do
            // not create a per-wait task or wait for its next poll here. A
            // `resolving` one already holds its result, which the scheduler
            // never looks at again, so it is re-driven below like any other.
            if row_state == "pending" {
                if let Condition::McpContinuation { mut state } = record.condition.clone() {
                    super::mcp_continuation::ensure_deadline(
                        &mut state,
                        chrono::Utc::now().timestamp_millis(),
                    );
                    if let Err(error) =
                        super::mcp_continuation::store(&db, &record.id, &state).await
                    {
                        log::warn!(
                            "startup could not normalize MCP wait {}: {error}",
                            record.id
                        );
                    }
                    reclaim_interrupted_delivery(&db, &SuspendedCall::of(&record), None).await;
                    continue;
                }
            }
            // A run batch on a machine is not answered at boot: its machine
            // may still be running it, and will send its result again once it
            // reattaches. The wait stays parked for `run::adopted` to settle
            // from that result, or from what the machine says became of it.
            // A delivery claim the dead process held is reclaimed now all the
            // same, or that later answer could never land.
            if row_state == "pending"
                && matches!(record.condition, Condition::RunBatch { host: false, .. })
            {
                reclaim_interrupted_delivery(&db, &SuspendedCall::of(&record), None).await;
                continue;
            }
            let (owned_orch, owned_db) = (orch.clone(), db.clone());
            tokio::spawn(async move {
                if row_state == "resolving" {
                    redrive_resolving(&owned_orch, &owned_db, &record, stored_result, None).await;
                    return;
                }
                let Some(guard) = claim_resolver(&owned_orch, &record.id) else {
                    return;
                };
                reclaim_interrupted_delivery(&owned_db, &SuspendedCall::of(&record), None).await;
                let result = resolution_after_restart(&owned_orch, &owned_db, &record).await;
                drive_claimed(&owned_orch, &owned_db, &record, &result, true, &guard).await;
            });
        }
    }
}

/// Read through `idx_agent_waits_pending_run_batch`, whose predicate this
/// spells exactly.
const PENDING_RUN_BATCH_WAITS: &str = "SELECT id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,deadline_ms,created_at FROM agent_waits INDEXED BY idx_agent_waits_pending_run_batch WHERE state = 'pending' AND kind = 'run_batch'";

/// Every run-batch wait still pending in this database.
pub(crate) async fn pending_run_batch_waits(db: &LocalDb) -> Result<Vec<Record>, String> {
    db.query_all(PENDING_RUN_BATCH_WAITS, (), record_from_row)
        .await
        .map_err(|error| error.to_string())
}

/// The pending wait of one run batch, if it has one.
pub(crate) async fn pending_run_batch_wait(
    db: &LocalDb,
    request_id: &str,
) -> Result<Option<Record>, String> {
    db.query_opt(
        format!(
            "{PENDING_RUN_BATCH_WAITS} AND json_extract(condition_json,'$.request_id')=?1 LIMIT 1"
        ),
        params![request_id.to_string()],
        record_from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// A suspension's [`Record`] from the first nine columns of an `agent_waits`
/// row: `id, job_id, run_id, session_id, predecessor_turn_id, tool_use_id,
/// condition_json, deadline_ms, created_at`.
fn record_from_row(row: &cairn_db::turso::Row) -> DbResult<Record> {
    let condition = serde_json::from_str::<Condition>(&row.text(6)?)
        .map_err(|error| DbError::internal(error.to_string()))?;
    Ok(Record {
        id: row.text(0)?,
        job_id: row.text(1)?,
        run_id: row.text(2)?,
        session_id: row.text(3)?,
        turn_id: row.text(4)?,
        tool_use_id: row.text(5)?,
        condition,
        deadline: row.opt_i64(7)?,
        created: row.i64(8)?,
    })
}

/// Re-drive a `resolving` row from its durable state. This is the one step
/// startup reconciliation and the periodic sweep share.
///
/// The guard comes first, so nothing below races a live resolver. Then any
/// delivery claim whose writer is gone is given back (see
/// [`reclaim_interrupted_delivery`] for what `stale_claim_after` means), and
/// the row is finished from what it stored.
///
/// An abandonment is FINISHED, never re-driven. Its turn is gone, so driving it
/// as an ordinary wait would create a successor and resume a run the job has
/// already moved past -- resurrecting the suspension that was discarded.
async fn redrive_resolving(
    orch: &Orchestrator,
    db: &LocalDb,
    record: &Record,
    stored_result: Option<String>,
    stale_claim_after: Option<Duration>,
) {
    let Some(guard) = claim_resolver(orch, &record.id) else {
        return;
    };
    let call = SuspendedCall::of(record);
    reclaim_interrupted_delivery(db, &call, stale_claim_after).await;
    if is_abandonment(stored_result.as_deref()) {
        let cause = AbandonCause::of(stored_result.as_deref());
        if let Err(error) = finish_abandonment(orch, db, &call, cause).await {
            log::warn!(
                "could not finish abandoned suspension {}: {error}",
                record.id
            );
        }
        return;
    }
    let result = stored_result.unwrap_or_else(|| {
        serde_json::json!({"outcome":"error","error":"wait resolution payload missing"}).to_string()
    });
    drive_claimed(orch, db, record, &result, true, &guard).await;
}

/// How long a suspension may sit in `resolving` before it counts as stuck.
///
/// One bound serves the periodic sweep, which re-drives a stuck row, and the
/// node summary, which shows it, so the two always agree on what "stuck" is.
/// It is well past the live resolver's own retry window for a transient error.
pub(crate) const STUCK_AFTER: Duration = Duration::from_secs(120);

/// How often the periodic sweep looks for stuck resolutions.
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Where a resolution past [`STUCK_AFTER`] is stuck.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum StuckPhase {
    /// The call's result never reached the agent's transcript.
    Delivering,
    /// The result was delivered, but resuming the turn keeps failing.
    Resuming,
    /// The result was delivered, and the live resolver is waiting for another
    /// continuation to start the turn. Nothing has failed; it is only slow.
    AwaitingContinuation,
}

/// A suspension that has been `resolving` for longer than [`STUCK_AFTER`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StuckResolution {
    pub(crate) job_id: String,
    pub(crate) wait_id: String,
    /// The parked call, as [`Condition::label`] names it.
    pub(crate) label: String,
    /// When the row entered `resolving`, i.e. when the call settled.
    pub(crate) since_ms: i64,
    pub(crate) phase: StuckPhase,
}

/// The suspensions of `job_ids` stuck mid-resolution at `now_ms`, by the same
/// [`STUCK_AFTER`] bound the sweep uses.
///
/// A row whose result is already in the transcript is told apart by `live`, the
/// set from [`live_resolvers`]: one a live resolver holds is waiting in its
/// collision backoff, which is slow rather than failed, and must not be shown
/// as a failure. Abandonments are left out; they never resume the agent.
pub(crate) async fn stuck_resolutions(
    db: &LocalDb,
    job_ids: &[String],
    now_ms: i64,
    live: &HashSet<String>,
) -> Result<Vec<StuckResolution>, String> {
    let cutoff_ms = now_ms - STUCK_AFTER.as_millis() as i64;
    let mut stuck = Vec::new();
    for job_id in job_ids {
        let rows = db
            .read(|c| {
                let job_id = job_id.clone();
                let [superseded, compacted] = AbandonCause::ALL.map(abandoned_result);
                Box::pin(async move {
                    let mut rows = c
                        .query(
                            "SELECT w.id,w.condition_json,COALESCE(w.resolving_at,w.created_at),EXISTS (SELECT 1 FROM events e WHERE e.run_id=w.run_id AND e.event_type='tool_result' AND json_extract(e.data,'$.toolUseId')=w.tool_use_id) FROM agent_waits w WHERE w.job_id=?1 AND w.state='resolving' AND COALESCE(w.resolving_at,w.created_at)<=?2 AND (w.resolution_json IS NULL OR w.resolution_json NOT IN (?3,?4)) ORDER BY 3",
                            params![job_id, cutoff_ms, superseded, compacted],
                        )
                        .await?;
                    let mut found = Vec::new();
                    while let Some(row) = rows.next().await? {
                        found.push((row.text(0)?, row.text(1)?, row.i64(2)?, row.i64(3)? != 0));
                    }
                    Ok(found)
                })
            })
            .await
            .map_err(|error| error.to_string())?;
        for (wait_id, condition_json, since_ms, delivered) in rows {
            let label = serde_json::from_str::<Condition>(&condition_json)
                .map(|condition| condition.label())
                .unwrap_or_else(|_| "suspended call".to_string());
            let phase = if !delivered {
                StuckPhase::Delivering
            } else if live.contains(&wait_id) {
                StuckPhase::AwaitingContinuation
            } else {
                StuckPhase::Resuming
            };
            stuck.push(StuckResolution {
                job_id: job_id.clone(),
                wait_id,
                label,
                since_ms,
                phase,
            });
        }
    }
    Ok(stuck)
}

/// What one pass of [`sweep_stuck_resolutions`] did.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct SweepOutcome {
    /// Rows handed to a fresh re-drive.
    pub(crate) redriven: usize,
    /// Rows settled because their node can no longer take a turn.
    pub(crate) settled: usize,
}

/// The periodic backstop for a resolution that failed while the host stayed up.
///
/// Startup reconciliation re-drives every `resolving` row once, at boot; this
/// runs the same [`redrive_resolving`] step every [`SWEEP_INTERVAL`] for rows
/// stuck past [`STUCK_AFTER`] that no live resolver in this host holds. A row
/// whose node can no longer take a turn is settled here instead, and so leaves
/// the set the next pass reads.
pub(crate) async fn sweep_stuck_resolutions(orch: &Orchestrator) -> SweepOutcome {
    let cutoff_ms = chrono::Utc::now().timestamp_millis() - STUCK_AFTER.as_millis() as i64;
    let live = live_resolvers(orch);
    let mut outcome = SweepOutcome::default();
    for db in orch.db.all_dbs().await {
        let rows = match stale_resolving(&db, cutoff_ms).await {
            Ok(rows) => rows,
            Err(error) => {
                log::warn!("stuck-resolution sweep could not read suspensions: {error}");
                continue;
            }
        };
        for (record, stored_result) in rows {
            if live.contains(&record.id) {
                continue;
            }
            if !is_abandonment(stored_result.as_deref())
                && settle_if_unresumable(orch, &db, &record).await
            {
                outcome.settled += 1;
                continue;
            }
            let (owned_orch, owned_db) = (orch.clone(), db.clone());
            tokio::spawn(async move {
                redrive_resolving(
                    &owned_orch,
                    &owned_db,
                    &record,
                    stored_result,
                    Some(STUCK_AFTER),
                )
                .await;
            });
            outcome.redriven += 1;
        }
    }
    outcome
}

/// The sweep's candidates, read through `idx_agent_waits_resolving` so a pass
/// touches only the rows in `resolving`.
const STALE_RESOLVING_SQL: &str = "SELECT id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,deadline_ms,created_at,resolution_json FROM agent_waits WHERE state='resolving' AND COALESCE(resolving_at,created_at)<=?1";

/// `resolving` rows that entered that state at or before `cutoff_ms`, with
/// their stored resolution.
async fn stale_resolving(
    db: &LocalDb,
    cutoff_ms: i64,
) -> Result<Vec<(Record, Option<String>)>, String> {
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn.query(STALE_RESOLVING_SQL, params![cutoff_ms]).await?;
            let mut found = Vec::new();
            while let Some(row) = rows.next().await? {
                found.push((record_from_row(&row)?, row.opt_text(9)?));
            }
            Ok(found)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Spawn the single host-global scheduler for durable waits. It arms due MCP
/// continuations every tick and runs [`sweep_stuck_resolutions`] every
/// [`SWEEP_INTERVAL`]. Resolution belongs to the execution host, so only that
/// host spawns this.
pub(crate) fn spawn_scheduler(orch: Orchestrator) {
    super::run::adopted::start(&orch);
    tokio::spawn(async move {
        let mut mcp = tokio::time::interval(super::mcp_continuation::SCHEDULER_INTERVAL);
        let mut sweep = tokio::time::interval(SWEEP_INTERVAL);
        sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = mcp.tick() => super::mcp_continuation::scheduler_tick(&orch).await,
                _ = sweep.tick() => {
                    let outcome = sweep_stuck_resolutions(&orch).await;
                    if outcome != SweepOutcome::default() {
                        log::warn!(
                            "stuck-resolution sweep re-drove {} and settled {} suspension(s)",
                            outcome.redriven,
                            outcome.settled
                        );
                    }
                    super::run::adopted::judge_unheard(&orch).await;
                    let lost = super::run::detached::sweep_unreported(&orch).await;
                    if lost > 0 {
                        log::warn!("{lost} detached run batch(es) can no longer report and were settled as lost");
                    }
                }
            }
        }
    });
}

/// Turn a held row whose host died into an ordinary park: the row stops being
/// held and its turn, if still running, yields for the wait.
async fn release_held_turn(db: &LocalDb, record: &Record) -> Result<(), String> {
    let (id, turn_id) = (record.id.clone(), record.turn_id.clone());
    db.write(|c| {
        let (id, turn_id) = (id.clone(), turn_id.clone());
        Box::pin(async move {
            c.execute(
                "UPDATE agent_waits SET hold=NULL WHERE id=?1 AND hold='inline'",
                params![id.as_str()],
            )
            .await?;
            yield_turn_for_host(c, &turn_id, TurnYieldReason::Wait)
                .await
                .map_err(|e| DbError::internal(e.to_string()))?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The result a `pending` row resolves to after a restart. (A `resolving` row
/// already holds its result; see [`redrive_resolving`].)
async fn resolution_after_restart(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    record: &Record,
) -> String {
    match &record.condition {
        Condition::McpContinuation { .. } => {
            match super::mcp_continuation::drive(orch.clone(), db.clone(), record.clone()).await {
                super::mcp_continuation::DriveOutcome::Terminal(result) => result,
                super::mcp_continuation::DriveOutcome::Pending => {
                    serde_json::json!({"outcome":"pending"}).to_string()
                }
            }
        }
        // Only a host batch reaches here (see `reconcile`): it ran inside the
        // process that died, so nothing of it is left to wait for.
        Condition::RunBatch { request_id, .. } => {
            super::run::adopted::host_restart_text(request_id)
        }
        // Level-triggered conditions re-arm from their own durable state, so a
        // restart simply asks them again.
        Condition::Duration | Condition::Terminal { .. } | Condition::Checks { .. } => {
            match super::owned_wait::trigger(orch.clone(), db.clone(), record.clone()).await {
                Ok(result) => result,
                Err(error) => serde_json::json!({"outcome":"error","error":error}).to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::internal::services::testing::CapturingEmitter;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use async_trait::async_trait;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    use crate::mcp::gateway::{
        McpCallOutcome, McpGateway, McpResourceDef, McpTaskOutcome, McpToolCallResult,
        McpToolCatalog,
    };

    // ---- Durable resolution path -------------------------------------------

    /// Full continue-ready seed: an open session, a live run, and a `running`
    /// predecessor turn wired as the job's current turn. `insert` yields the
    /// predecessor and creates the pending wait row before resolution runs.
    async fn durable_env() -> (Orchestrator, Record, Condition) {
        let (orch, record, _) = durable_env_capturing().await;
        (orch, record, Condition::Duration)
    }

    /// An `EventEmitter` several handles can share, so a test can read what the
    /// orchestrator announced.
    struct SharedCapturingEmitter(Arc<CapturingEmitter>);

    impl crate::services::EventEmitter for SharedCapturingEmitter {
        fn emit(&self, event: &str, payload: serde_json::Value) -> Result<(), String> {
            self.0.emit(event, payload)
        }

        fn emit_empty(&self, event: &str) -> Result<(), String> {
            self.0.emit_empty(event)
        }
    }

    /// The same seed, handing back the emitter so a test can assert on what the
    /// suspension told clients.
    async fn durable_env_capturing() -> (Orchestrator, Record, Arc<CapturingEmitter>) {
        let root = tempfile::tempdir().unwrap().keep();
        let local = LocalDb::open_migrated_for_test(root.join("test.db"))
            .await
            .unwrap();
        for sql in [
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)",
            "INSERT INTO issues (id,project_id,number,title,status,created_at,updated_at) VALUES ('i','p',1,'T','active',1,1)",
            "INSERT INTO executions (id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id,execution_id,issue_id,project_id,node_name,status,created_at,updated_at,uri_segment) VALUES ('job-1','e','i','p','Builder','running',1,1,'builder')",
            "INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-1','job-1','open','handle-1',1,1)",
            "INSERT INTO runs (id,issue_id,project_id,job_id,status,session_id,created_at,updated_at) VALUES ('run-1','i','p','job-1','live','session-1',1,1)",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('pred-turn','session-1','run-1','job-1',1,'running','initial',1,1)",
            // current_session_id / current_turn_id carry FKs, so wire them only
            // after the session and turn rows exist.
            "UPDATE jobs SET current_session_id='session-1', current_turn_id='pred-turn' WHERE id='job-1'",
        ] {
            local.execute(sql, ()).await.unwrap();
        }
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let emitter = Arc::new(CapturingEmitter::new());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(
                TestServicesBuilder::new()
                    .with_emitter(SharedCapturingEmitter(emitter.clone()))
                    .build(),
            ),
            root,
        )
        .build();
        let now = chrono::Utc::now().timestamp_millis();
        let record = Record {
            id: "wait-1".into(),
            job_id: "job-1".into(),
            run_id: "run-1".into(),
            session_id: "session-1".into(),
            turn_id: "pred-turn".into(),
            tool_use_id: "tool-1".into(),
            condition: Condition::Duration,
            deadline: Some(now - 1000),
            created: now,
        };
        (orch, record, emitter)
    }

    /// Every `db-change` this test's orchestrator announced for the turns table.
    fn turn_changes(emitter: &CapturingEmitter) -> Vec<serde_json::Value> {
        emitter
            .events_named("db-change")
            .into_iter()
            .filter(|payload| payload.get("table").and_then(|t| t.as_str()) == Some("turns"))
            .collect()
    }

    /// A `run` batch suspension: no pollable condition, only the request the
    /// suspending host was awaiting.
    fn run_batch_record(base: &Record, commits: bool) -> Record {
        Record {
            condition: run_batch_condition(commits, None),
            deadline: None,
            ..base.clone()
        }
    }

    fn run_batch_condition(commits: bool, label: Option<&str>) -> Condition {
        Condition::RunBatch {
            request_id: "request-1".into(),
            commits,
            label: label.map(str::to_string),
            host: false,
            identity: None,
            verdict_only: false,
            settlement: None,
        }
    }

    fn ownership_test_run_batch_record(mut record: Record) -> Record {
        record.condition = Condition::RunBatch {
            request_id: "request".to_string(),
            commits: false,
            label: None,
            host: false,
            identity: None,
            verdict_only: false,
            settlement: None,
        };
        record
    }

    async fn seed_pending_permission(db: &LocalDb, record: &Record) {
        let record = record.clone();
        db.write(|conn| {
            let record = record.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO permission_requests(id,run_id,job_id,tool_use_id,tool_name,tool_input,status,created_at,turn_id) VALUES(?1,?2,?3,?4,'run','{}','pending',?5,?6)",
                    params![
                        format!("permission-{}", record.id),
                        record.run_id,
                        record.job_id,
                        record.tool_use_id,
                        chrono::Utc::now().timestamp() as i32,
                        record.turn_id,
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn active_wait_count_for_call(db: &LocalDb, record: &Record) -> i64 {
        let (run_id, tool_use_id) = (record.run_id.clone(), record.tool_use_id.clone());
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT COUNT(*) FROM agent_waits WHERE run_id=?1 AND tool_use_id=?2 AND state IN ('pending','resolving')",
                        params![run_id, tool_use_id],
                    )
                    .await?;
                rows.next().await?.unwrap().i64(0)
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn run_batch_handoff_is_not_published_without_a_confirmed_goal_pause() {
        let (orch, record, _) = durable_env().await;
        let record = run_batch_record(&record, false);
        let (stdin, requests, _) = crate::backends::codex::scripted_owned_goal_stdin_for_test(
            "native-thread",
            "provider-predecessor",
            vec![
                owned_goal_reply("active"),
                Ok(serde_json::Value::Null),
                owned_goal_reply("active"),
                owned_goal_reply("active"),
                owned_goal_reply("active"),
            ],
        );
        let mut handle = crate::agent_process::process::RunHandle::new(
            Arc::new(std::sync::Mutex::new(None)),
            Arc::new(std::sync::Mutex::new(Some(stdin))),
            Some("session-1".to_string()),
            Some("job-1".to_string()),
        );
        handle.backend = Some("codex".to_string());
        handle.begin_turn("pred-turn");
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("run-1".to_string(), handle);

        let error = match suspend(&orch, &orch.db.local, &record).await {
            Ok(_) => panic!("an unconfirmed provider pause must fail closed"),
            Err(error) => error,
        };
        assert!(
            error.contains("Invalid Codex thread/goal/set response"),
            "{error}"
        );
        assert_eq!(active_wait_count_for_call(&orch.db.local, &record).await, 0);
        assert_eq!(turn_state_of(&orch, "pred-turn").await, "running");
        let sent = requests.lock().unwrap();
        assert_eq!(sent.len(), 5);
        assert_eq!(sent[0]["method"], "thread/goal/get");
        assert_eq!(sent[1]["method"], "thread/goal/set");
        assert_eq!(sent[2]["method"], "thread/goal/get");
        assert_eq!(sent[3]["method"], "thread/goal/get");
        assert_eq!(sent[4]["method"], "thread/goal/set");
        assert_eq!(sent[4]["params"]["status"], "active");
        assert!(sent
            .iter()
            .all(|request| request["method"] != "turn/interrupt"));
    }

    #[tokio::test]
    async fn durable_wait_without_a_provider_process_still_establishes_the_park() {
        let (orch, record, _) = durable_env().await;
        let record = run_batch_record(&record, false);

        let _handoff = suspend(&orch, &orch.db.local, &record)
            .await
            .expect("a missing provider process has no native continuation to fence");

        assert_eq!(wait_state(&orch).await, "pending");
        assert_eq!(turn_state_of(&orch, "pred-turn").await, "yielded");
    }

    #[tokio::test]
    async fn durable_wait_without_provider_stdin_still_establishes_the_park() {
        let (orch, record, _) = durable_env().await;
        let record = run_batch_record(&record, false);
        let mut handle = crate::agent_process::process::RunHandle::new(
            Arc::new(std::sync::Mutex::new(None)),
            Arc::new(std::sync::Mutex::new(None)),
            Some("session-1".to_string()),
            Some("job-1".to_string()),
        );
        handle.backend = Some("codex".to_string());
        handle.begin_turn("pred-turn");
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("run-1".to_string(), handle);

        let _handoff = suspend(&orch, &orch.db.local, &record)
            .await
            .expect("a process without stdin has no native continuation to fence");

        assert_eq!(wait_state(&orch).await, "pending");
        assert_eq!(turn_state_of(&orch, "pred-turn").await, "yielded");
    }

    #[tokio::test]
    async fn permission_first_refuses_a_second_outer_run_batch_owner() {
        let (orch, record, _) = durable_env().await;
        let record = ownership_test_run_batch_record(record);
        seed_pending_permission(&orch.db.local, &record).await;

        assert!(insert(&orch.db.local, &record).await.is_err());
        assert_eq!(active_wait_count_for_call(&orch.db.local, &record).await, 0);
    }

    #[tokio::test]
    async fn permission_owner_rejects_run_batch_before_native_goal_pause() {
        let (orch, record, _) = durable_env().await;
        let record = ownership_test_run_batch_record(record);
        seed_pending_permission(&orch.db.local, &record).await;
        let (stdin, requests, _) = crate::backends::codex::scripted_durable_wait_stdin_for_test(
            "native-thread",
            "provider-predecessor",
        );
        let mut handle = crate::agent_process::process::RunHandle::new(
            Arc::new(std::sync::Mutex::new(None)),
            Arc::new(std::sync::Mutex::new(Some(stdin))),
            Some("session-1".to_string()),
            Some("job-1".to_string()),
        );
        handle.backend = Some("codex".to_string());
        handle.begin_turn("pred-turn");
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("run-1".to_string(), handle);

        assert!(suspend(&orch, &orch.db.local, &record).await.is_err());
        assert_eq!(active_wait_count_for_call(&orch.db.local, &record).await, 0);
        assert_eq!(turn_state_of(&orch, "pred-turn").await, "running");
        assert!(
            requests.lock().unwrap().is_empty(),
            "permission ownership must be decided before the native goal is read or changed"
        );
    }

    #[tokio::test]
    async fn failed_insert_restores_the_native_goals_prior_status() {
        let (orch, record, _) = durable_env().await;
        let record = run_batch_record(&record, false);
        let condition = serde_json::to_string(&Condition::Duration).unwrap();
        orch.db
            .local
            .execute(
                "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at) VALUES(?1,?2,?3,?4,?5,'different-tool',?6,'resolved',?7)",
                params![
                    record.id.as_str(),
                    record.job_id.as_str(),
                    record.run_id.as_str(),
                    record.session_id.as_str(),
                    record.turn_id.as_str(),
                    condition,
                    record.created,
                ],
            )
            .await
            .unwrap();
        let (stdin, requests, _) =
            crate::backends::codex::scripted_durable_wait_rollback_stdin_for_test(
                "native-thread",
                "provider-predecessor",
            );
        let mut handle = crate::agent_process::process::RunHandle::new(
            Arc::new(std::sync::Mutex::new(None)),
            Arc::new(std::sync::Mutex::new(Some(stdin))),
            Some("session-1".to_string()),
            Some("job-1".to_string()),
        );
        handle.backend = Some("codex".to_string());
        handle.begin_turn("pred-turn");
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("run-1".to_string(), handle);

        assert!(suspend(&orch, &orch.db.local, &record).await.is_err());
        assert_eq!(turn_state_of(&orch, "pred-turn").await, "running");
        let sent = requests.lock().unwrap();
        assert_eq!(sent.len(), 4);
        assert_eq!(sent[0]["method"], "thread/goal/get");
        assert_eq!(sent[1]["params"]["status"], "paused");
        assert_eq!(sent[2]["method"], "thread/goal/get");
        assert_eq!(sent[3]["params"]["status"], "active");
    }

    #[derive(Debug, PartialEq)]
    struct OwnershipSnapshot {
        state: String,
        resolution: Option<String>,
        resolved_at: Option<i64>,
        successor_turn_id: Option<String>,
        result_stored_at: Option<i64>,
    }

    async fn ownership_snapshot(db: &LocalDb, id: &str) -> OwnershipSnapshot {
        let id = id.to_string();
        db.query_one(
            "SELECT state,resolution_json,resolved_at,successor_turn_id,result_stored_at \
             FROM agent_waits WHERE id=?1",
            params![id],
            |row| {
                Ok(OwnershipSnapshot {
                    state: row.text(0)?,
                    resolution: row.opt_text(1)?,
                    resolved_at: row.opt_i64(2)?,
                    successor_turn_id: row.opt_text(3)?,
                    result_stored_at: row.opt_i64(4)?,
                })
            },
        )
        .await
        .unwrap()
    }

    async fn relinquish(db: &LocalDb, record: &Record) -> DbResult<RelinquishOutcome> {
        let (run_id, tool_use_id) = (record.run_id.clone(), record.tool_use_id.clone());
        db.write(|conn| {
            let (run_id, tool_use_id) = (run_id.clone(), tool_use_id.clone());
            Box::pin(async move { relinquish_to_inner_wait(conn, &run_id, &tool_use_id).await })
        })
        .await
    }

    #[tokio::test]
    async fn relinquish_resolves_one_pending_owner_once_without_result_or_successor() {
        let (orch, record, _) = durable_env().await;
        let record = ownership_test_run_batch_record(record);
        insert(&orch.db.local, &record).await.unwrap();

        assert_eq!(
            relinquish(&orch.db.local, &record).await.unwrap(),
            RelinquishOutcome::Transferred
        );
        let transferred = ownership_snapshot(&orch.db.local, &record.id).await;
        assert_eq!(transferred.state, "resolved");
        assert_eq!(
            transferred.resolution.as_deref(),
            Some(r#"{"outcome":"transferred_to_inner_wait"}"#)
        );
        assert!(transferred.resolved_at.is_some());
        assert!(transferred.successor_turn_id.is_none());
        assert!(transferred.result_stored_at.is_none());

        assert_eq!(
            relinquish(&orch.db.local, &record).await.unwrap(),
            RelinquishOutcome::AlreadyClaimed
        );
        assert_eq!(
            ownership_snapshot(&orch.db.local, &record.id).await,
            transferred,
            "an idempotent retry must not rewrite the resolution timestamp"
        );
    }

    #[tokio::test]
    async fn relinquish_refuses_claimed_or_result_bearing_owners() {
        for (state, successor, result_stored_at) in [
            ("resolving", None, None),
            ("pending", Some("pred-turn"), None),
            ("pending", None, Some(123_i64)),
        ] {
            let (orch, mut record, _) = durable_env().await;
            record.id = format!(
                "wait-{state}-{}-{}",
                successor.is_some(),
                result_stored_at.is_some()
            );
            let record = ownership_test_run_batch_record(record);
            insert(&orch.db.local, &record).await.unwrap();
            orch.db
                .local
                .execute(
                    "UPDATE agent_waits SET state=?2,successor_turn_id=?3,result_stored_at=?4 \
                     WHERE id=?1",
                    params![record.id.as_str(), state, successor, result_stored_at],
                )
                .await
                .unwrap();
            let before = ownership_snapshot(&orch.db.local, &record.id).await;

            assert_eq!(
                relinquish(&orch.db.local, &record).await.unwrap(),
                RelinquishOutcome::AlreadyClaimed
            );
            assert_eq!(ownership_snapshot(&orch.db.local, &record.id).await, before);
        }
    }

    #[tokio::test]
    async fn relinquish_reports_an_absent_outer_owner() {
        let (orch, record, _) = durable_env().await;
        assert_eq!(
            relinquish(&orch.db.local, &record).await.unwrap(),
            RelinquishOutcome::Absent
        );
    }

    #[tokio::test]
    async fn relinquish_propagates_database_failure() {
        let root = tempfile::tempdir().unwrap();
        let db = LocalDb::open(root.path().join("unmigrated.db"))
            .await
            .unwrap();
        let error = db
            .write(|conn| {
                Box::pin(async move {
                    relinquish_to_inner_wait(conn, "missing-run", "missing-tool").await
                })
            })
            .await
            .unwrap_err();

        assert!(error.to_string().contains("agent_waits"), "{error}");
    }

    /// A second call parked by the SAME turn: its own row and its own provider
    /// call, sharing everything that makes it a sibling.
    fn sibling_record(base: &Record) -> Record {
        Record {
            id: "wait-2".into(),
            tool_use_id: "tool-2".into(),
            ..base.clone()
        }
    }

    /// A restart is not evidence a remote batch died: its machine may still be
    /// running it, and retains its result until a runner acknowledges it. So
    /// reconciliation answers nothing at boot. The wait stays parked, with no
    /// detached row and no result, for `run::adopted` to settle from the
    /// machine's resent result or from what the machine says became of it.
    #[tokio::test]
    async fn a_pending_remote_run_batch_stays_parked_through_a_restart() {
        let (orch, base, _) = durable_env().await;
        register_warm(&orch);
        let record = run_batch_record(&base, true);
        insert(&orch.db.local, &record).await.unwrap();

        reconcile(&orch).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(wait_state(&orch).await, "pending");
        let detached: i64 = orch
            .db
            .local
            .query_one("SELECT COUNT(*) FROM run_batch_detached", (), |r| r.i64(0))
            .await
            .unwrap();
        assert_eq!(detached, 0, "nothing is released at boot");
        assert_eq!(tool_result_count(&orch).await, 0);
    }

    /// cairn/5536: a batch that outlives a host restart, settled from the
    /// result its machine retained and sent again, or from what its machine
    /// says became of it. The restart is simulated by a fresh orchestrator over
    /// a database holding the parked wait, whose process-local registries hold
    /// nothing of the batch.
    mod adoption {
        use super::*;
        use crate::mcp::handlers::run::{adopted, PersistedSettlement, PlacedOn};
        use cairn_common::executor_protocol::{
            CellOutcome, ExecutorMessage, MutationDelta, ProcessBatchItemOutcome,
            ReauthorizedTransfer,
        };
        use tokio::sync::mpsc::UnboundedReceiver;

        fn completed(orch: &Orchestrator, body: &str, delta: Option<&str>) -> CellOutcome {
            let items: Vec<ProcessBatchItemOutcome> = serde_json::from_value(serde_json::json!([{
                "header": "$ echo",
                "body": body,
                "succeeded": true,
                "suspended": false,
                "exitCode": 0,
                "startedAtUnixMs": 1,
                "finishedAtUnixMs": 2,
                "durationMs": 1
            }]))
            .unwrap();
            CellOutcome::Completed {
                request_id: "request-1".into(),
                attempt_id: "attempt-1".into(),
                exit_code: Some(0),
                output: serde_json::to_string(&items).unwrap(),
                timed_out: false,
                metadata: orch.fleet.execution_meta_for_test(),
                mutation_delta: delta.map(|commit| {
                    Box::new(MutationDelta {
                        base_commit: "b".repeat(40),
                        delta_commit: commit.into(),
                        upload_receipt: None,
                    })
                }),
                sandbox_denials: Vec::new(),
                tracked_modifications: None,
                node_scratch: None,
            }
        }

        fn result(outcome: CellOutcome) -> ExecutorMessage {
            ExecutorMessage::Result {
                request_id: "request-1".into(),
                attempt_id: "attempt-1".into(),
                outcome,
            }
        }

        fn with_settlement(record: &mut Record, settlement: PersistedSettlement) {
            if let Condition::RunBatch {
                settlement: slot, ..
            } = &mut record.condition
            {
                *slot = Some(Box::new(settlement));
            }
        }

        /// The next frame of `kind` this runner sent the machine, skipping others.
        async fn next_frame(
            frames: &mut UnboundedReceiver<ExecutorMessage>,
            wanted: fn(&ExecutorMessage) -> bool,
        ) -> ExecutorMessage {
            loop {
                let frame = tokio::time::timeout(Duration::from_secs(10), frames.recv())
                    .await
                    .expect("the runner sends the frame")
                    .expect("the machine's link is open");
                if wanted(&frame) {
                    return frame;
                }
            }
        }

        fn is_ack(frame: &ExecutorMessage) -> bool {
            matches!(frame, ExecutorMessage::ResultAck { .. })
        }

        fn no_ack_pending(frames: &mut UnboundedReceiver<ExecutorMessage>) -> bool {
            while let Ok(frame) = frames.try_recv() {
                if is_ack(&frame) {
                    return false;
                }
            }
            true
        }

        async fn adopted_state(orch: &Orchestrator) -> Option<String> {
            orch.db
                .local
                .query_opt(
                    "SELECT state FROM run_batch_adopted_outcomes WHERE request_id='request-1'",
                    (),
                    |r| r.text(0),
                )
                .await
                .unwrap()
        }

        async fn resolution(orch: &Orchestrator) -> String {
            for _ in 0..400 {
                if wait_state(orch).await == "resolved" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert_eq!(wait_state(orch).await, "resolved");
            orch.db
                .local
                .query_one(
                    "SELECT resolution_json FROM agent_waits WHERE id='wait-1'",
                    (),
                    |r| r.text(0),
                )
                .await
                .unwrap()
        }

        async fn parked(orch: &Orchestrator, record: &Record) {
            register_warm(orch);
            insert(&orch.db.local, record).await.unwrap();
            reconcile(orch).await;
            assert_eq!(wait_state(orch).await, "pending");
        }

        /// Acceptance: a parked batch whose host restarts while its machine
        /// keeps running it delivers its real result exactly once, and the
        /// result is acknowledged only after its settled outcome is recorded.
        /// A resent duplicate delivers nothing and is acknowledged again.
        #[tokio::test]
        async fn a_parked_batch_that_survives_a_restart_delivers_its_real_result_once() {
            let (orch, base, _) = durable_env().await;
            parked(&orch, &run_batch_record(&base, false)).await;
            let mut frames = orch
                .fleet
                .attach_reported_machine_for_test("exec-a", &[("request-1", "attempt-1")]);
            adopted::start(&orch);

            let outcome = completed(&orch, "hello from exec-a", None);
            orch.fleet
                .handle_executor_message("exec-a", 1, result(outcome.clone()));
            let ExecutorMessage::ResultAck {
                request_id,
                attempt_id,
            } = next_frame(&mut frames, is_ack).await
            else {
                unreachable!()
            };
            assert_eq!(
                (request_id.as_str(), attempt_id.as_str()),
                ("request-1", "attempt-1")
            );
            assert!(
                matches!(
                    adopted_state(&orch).await.as_deref(),
                    Some("published" | "delivered")
                ),
                "acknowledged only once the settled outcome was recorded"
            );

            let resolution = resolution(&orch).await;
            assert!(resolution.contains("hello from exec-a"), "{resolution}");
            assert!(resolution.contains("exec-a"), "{resolution}");
            assert_eq!(tool_result_count(&orch).await, 1);
            assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
            for _ in 0..200 {
                if adopted_state(&orch).await.as_deref() == Some("delivered") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert_eq!(adopted_state(&orch).await.as_deref(), Some("delivered"));

            orch.fleet
                .handle_executor_message("exec-a", 1, result(outcome));
            assert!(is_ack(&next_frame(&mut frames, is_ack).await));
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(
                tool_result_count(&orch).await,
                1,
                "the duplicate delivered nothing"
            );
            assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
        }

        /// Execution's condition 1: a result that arrives before this runner
        /// can read its durable rows is not acknowledged, so its machine keeps
        /// it, and it is adopted once the runner can decide.
        #[tokio::test]
        async fn a_result_that_arrives_before_the_runner_can_decide_is_kept_until_it_can() {
            let (orch, base, _) = durable_env().await;
            parked(&orch, &run_batch_record(&base, false)).await;
            let mut frames = orch
                .fleet
                .attach_reported_machine_for_test("exec-a", &[("request-1", "attempt-1")]);

            orch.fleet.handle_executor_message(
                "exec-a",
                1,
                result(completed(&orch, "early result", None)),
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                no_ack_pending(&mut frames),
                "nothing could decide, so nothing was acknowledged"
            );
            assert_eq!(adopted_state(&orch).await, None);

            adopted::start(&orch);
            assert!(is_ack(&next_frame(&mut frames, is_ack).await));
            assert!(resolution(&orch).await.contains("early result"));
        }

        /// Acceptance: a machine that reattaches and reports neither the
        /// attempt nor its completion resolves the wait to the typed lost
        /// text, naming the machine it was placed on.
        #[tokio::test]
        async fn a_machine_that_reports_without_the_batch_resolves_it_as_lost() {
            let (orch, base, _) = durable_env().await;
            let mut record = run_batch_record(&base, false);
            let placed = PlacedOn {
                executor_id: "exec-a".into(),
                name: "exec-a".into(),
            };
            with_settlement(
                &mut record,
                PersistedSettlement::for_test(None, None, Some(placed.clone())),
            );
            parked(&orch, &record).await;
            let _frames = orch.fleet.attach_reported_machine_for_test("exec-a", &[]);

            adopted::judge_unheard(&orch).await;

            let resolution = resolution(&orch).await;
            assert_eq!(
                resolution,
                adopted::lost_text("request-1", Some(&placed), false)
            );
            assert!(resolution.contains("placed on exec-a"), "{resolution}");
        }

        /// While its machine still holds the attempt, the wait stays parked.
        #[tokio::test]
        async fn a_batch_its_machine_still_holds_stays_parked() {
            let (orch, base, _) = durable_env().await;
            parked(&orch, &run_batch_record(&base, false)).await;
            let _frames = orch
                .fleet
                .attach_reported_machine_for_test("exec-a", &[("request-1", "attempt-1")]);
            orch.adoption.backdate_for_test(adopted::RELEASE_AFTER * 2);

            adopted::judge_unheard(&orch).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(wait_state(&orch).await, "pending");
        }

        /// An executor ends its pin on a result's delta when the result is
        /// acknowledged, and this machine is modeled by exactly that rule. An
        /// adopted commit's delta has to be uploaded again after a restart, so
        /// the result is acknowledged only after the driver has asked for the
        /// delta and recorded the settlement; an acknowledgement any earlier
        /// would leave the machine nothing to upload.
        #[tokio::test]
        async fn an_adopted_commit_is_acknowledged_only_after_its_delta_is_asked_for() {
            let (orch, base, _) = durable_env().await;
            let mut record = run_batch_record(&base, true);
            with_settlement(
                &mut record,
                PersistedSettlement::for_test(
                    Some("feat: the batch"),
                    Some(orch.fleet.cell_request_for_test("request-1")),
                    None,
                ),
            );
            parked(&orch, &record).await;
            let mut frames = orch
                .fleet
                .attach_reported_machine_for_test("exec-a", &[("request-1", "attempt-1")]);
            adopted::start(&orch);

            orch.fleet.handle_executor_message(
                "exec-a",
                1,
                result(completed(&orch, "built it", Some(&"d".repeat(40)))),
            );
            let mut pin_released = false;
            let (request_id, attempt_id) = loop {
                match next_frame(&mut frames, |frame| {
                    matches!(
                        frame,
                        ExecutorMessage::ResultAck { .. }
                            | ExecutorMessage::ReauthorizeTransfers { .. }
                    )
                })
                .await
                {
                    ExecutorMessage::ResultAck { .. } => pin_released = true,
                    ExecutorMessage::ReauthorizeTransfers {
                        request_id,
                        attempt_id,
                    } => break (request_id, attempt_id),
                    _ => unreachable!(),
                }
            };
            assert!(
                !pin_released,
                "the result was acknowledged, ending its delta's pin, before its delta was asked for"
            );
            assert!(no_ack_pending(&mut frames));
            orch.fleet.handle_executor_message(
                "exec-a",
                1,
                ExecutorMessage::TransfersReauthorized {
                    request_id,
                    attempt_id,
                    transfers: vec![ReauthorizedTransfer::Delta(Err(
                        "the model machine declines while it still pins the delta".into(),
                    ))],
                },
            );

            assert!(is_ack(&next_frame(&mut frames, is_ack).await));
            assert!(
                matches!(
                    adopted_state(&orch).await.as_deref(),
                    Some("published" | "delivered")
                ),
                "acknowledged once the settled outcome was recorded"
            );
            let resolution = resolution(&orch).await;
            assert!(
                resolution.contains("declines while it still pins"),
                "{resolution}"
            );
        }

        /// Tenet 1437, end to end. A commit_msg batch finishes while its host
        /// is down, so the upload its machine made died with that process and
        /// nothing here holds it staged. The restarted runner adopts the
        /// result, asks the machine to upload the delta again, and publishes
        /// what comes back through `settle_run_batch`: exactly one commit on
        /// the agent branch, the check-on-write wave opened for it, the call
        /// answered once, the row delivered, and the result acknowledged.
        ///
        /// The machine is modeled by the executor's rule that an
        /// acknowledgement ends the delta's pin: acknowledged before it is
        /// asked, it has nothing left to upload and says so.
        #[tokio::test]
        #[serial_test::serial(jj)]
        async fn an_adopted_commit_is_published_once_after_its_machine_uploads_it_again() {
            use crate::jj::tests::git_stdout;
            use crate::mcp::handlers::run::slot_publication_tests::{stage_upload, straddle};
            use cairn_common::executor_protocol::RepositoryLocator;

            let Some(mut fx) = straddle(None, &[("mine.rs", "the batch's work\n")]).await else {
                eprintln!("skipping an_adopted_commit_is_published_once: jj not resolvable");
                return;
            };
            let (seeded, base, _) = durable_env().await;
            // Publication resolves the project's store from its row under the
            // orchestrator's config directory, so this orchestrator shares the
            // seed's database and the fixture's store, and the seed's project
            // and job are the fixture's.
            let orch = Orchestrator::builder(
                seeded.db.clone(),
                Arc::new(TestServicesBuilder::new().build()),
                fx.home().to_path_buf(),
            )
            .jj_binary_path(fx.jj_bin())
            .build();
            orch.db
                .local
                .execute(
                    "UPDATE projects SET repo_path=?1, default_branch='main' WHERE id='p'",
                    params![fx.repository.to_string_lossy().into_owned()],
                )
                .await
                .unwrap();
            orch.db
                .local
                .execute("UPDATE jobs SET branch='feature' WHERE id='job-1'", ())
                .await
                .unwrap();
            // The placement template the suspended batch persisted, in this
            // seed's project.
            fx.request.request_id = "request-1".into();
            fx.request.project_id = "p".into();
            // The parked job's own request: its sealed delta is exempt from the
            // hold its still-pending wait makes on the branch.
            fx.request.requesting_job_id = Some("job-1".into());
            fx.request.repository = RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "repository".into(),
                absolute_path: fx.repository.to_string_lossy().into_owned(),
            };
            let mut record = run_batch_record(&base, true);
            with_settlement(
                &mut record,
                PersistedSettlement::for_test(
                    Some("feat: the batch"),
                    Some(fx.request.clone()),
                    None,
                )
                .publishing_to_for_test(
                    "p",
                    &fx.repository,
                    fx.store(),
                    "feature",
                    &fx.base,
                ),
            );
            parked(&orch, &record).await;
            let mut frames = orch
                .fleet
                .attach_reported_machine_for_test("exec-a", &[("request-1", "attempt-1")]);
            let generation = orch.fleet.link_generation("exec-a").unwrap();
            let waves_before = orch.lane_claims.waves_opened_for_test();
            adopted::start(&orch);

            let mut outcome = completed(&orch, "built it", Some(&fx.delta.delta_commit));
            if let CellOutcome::Completed {
                mutation_delta: Some(delta),
                ..
            } = &mut outcome
            {
                delta.base_commit = fx.base.clone();
            }
            orch.fleet
                .handle_executor_message("exec-a", 1, result(outcome));

            let mut acked_before_asked = false;
            let (request_id, attempt_id) = loop {
                match next_frame(&mut frames, |frame| {
                    matches!(
                        frame,
                        ExecutorMessage::ResultAck { .. }
                            | ExecutorMessage::ReauthorizeTransfers { .. }
                    )
                })
                .await
                {
                    ExecutorMessage::ResultAck { .. } => acked_before_asked = true,
                    ExecutorMessage::ReauthorizeTransfers {
                        request_id,
                        attempt_id,
                    } => break (request_id, attempt_id),
                    _ => unreachable!(),
                }
            };
            let staging = tempfile::tempdir().unwrap();
            let transfer = if acked_before_asked {
                Err(format!(
                    "this machine no longer retains the result of request {request_id} attempt {attempt_id}"
                ))
            } else {
                let mut request = fx.request.clone();
                request.attempt_id = attempt_id.clone();
                Ok(stage_upload(
                    &orch,
                    &fx,
                    &request,
                    "exec-a",
                    generation,
                    staging.path(),
                ))
            };
            orch.fleet.handle_executor_message(
                "exec-a",
                1,
                ExecutorMessage::TransfersReauthorized {
                    request_id,
                    attempt_id,
                    transfers: vec![ReauthorizedTransfer::Delta(transfer)],
                },
            );

            let resolution = resolution(&orch).await;
            assert!(
                resolution.contains("Committed changes"),
                "the adopted commit is published: {resolution}"
            );
            assert!(resolution.contains("built it"), "{resolution}");
            let landed = fx.bookmark();
            assert_ne!(landed, fx.base, "the agent branch advanced");
            assert_eq!(
                git_stdout(&fx.repository, &["rev-parse", &format!("{landed}^")]),
                fx.base,
                "exactly one commit landed on the branch"
            );
            assert_eq!(fx.blob(&landed, "mine.rs"), "the batch's work");
            assert!(resolution.contains(&landed), "{resolution}");
            assert!(
                orch.lane_claims.waves_opened_for_test() > waves_before,
                "the landed commit opened its check-on-write wave"
            );
            assert_eq!(
                tool_result_count(&orch).await,
                1,
                "the call is answered once"
            );
            assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
            for _ in 0..200 {
                if adopted_state(&orch).await.as_deref() == Some("delivered") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert_eq!(adopted_state(&orch).await.as_deref(), Some("delivered"));
            assert!(is_ack(&next_frame(&mut frames, is_ack).await));
            assert!(
                !acked_before_asked,
                "acknowledged only after the delta was asked for"
            );
        }

        /// A commit whose delta its machine can no longer upload is reported
        /// precisely, naming the delta, the machine and the reason, and nothing
        /// is published.
        #[tokio::test]
        async fn a_commit_whose_delta_is_gone_gets_the_precise_report() {
            let (orch, base, _) = durable_env().await;
            let mut record = run_batch_record(&base, true);
            with_settlement(
                &mut record,
                PersistedSettlement::for_test(
                    Some("feat: the batch"),
                    Some(orch.fleet.cell_request_for_test("request-1")),
                    None,
                ),
            );
            parked(&orch, &record).await;
            let mut frames = orch
                .fleet
                .attach_reported_machine_for_test("exec-a", &[("request-1", "attempt-1")]);
            adopted::start(&orch);

            let delta = "d".repeat(40);
            orch.fleet.handle_executor_message(
                "exec-a",
                1,
                result(completed(&orch, "built it", Some(&delta))),
            );
            let asked = next_frame(&mut frames, |frame| {
                matches!(frame, ExecutorMessage::ReauthorizeTransfers { .. })
            })
            .await;
            let ExecutorMessage::ReauthorizeTransfers {
                request_id,
                attempt_id,
            } = asked
            else {
                unreachable!()
            };
            orch.fleet.handle_executor_message(
                "exec-a",
                1,
                ExecutorMessage::TransfersReauthorized {
                    request_id,
                    attempt_id,
                    transfers: vec![ReauthorizedTransfer::Delta(Err(
                        "pin gone: this machine no longer pins it".into(),
                    ))],
                },
            );

            let resolution = resolution(&orch).await;
            assert!(
                resolution.starts_with(crate::mcp::handlers::run::RUN_NOT_PUBLISHED),
                "{resolution}"
            );
            assert!(resolution.contains(&delta[..12]), "{resolution}");
            assert!(resolution.contains("exec-a"), "{resolution}");
            assert!(resolution.contains("pin gone"), "{resolution}");
            assert!(
                resolution.contains("built it"),
                "the full output follows: {resolution}"
            );
            assert!(!resolution.contains("Committed changes"), "{resolution}");
        }

        /// The bound: a machine that has not reconnected leaves the call parked
        /// until this process has been up for the bound, then releases it into
        /// a detached row whose branch hold stands. When the machine returns
        /// with the result, that row settles and its notice is queued.
        #[tokio::test]
        async fn a_machine_that_stays_away_releases_the_call_and_settles_the_row_on_return() {
            let (orch, base, _) = durable_env().await;
            parked(&orch, &run_batch_record(&base, false)).await;

            adopted::judge_unheard(&orch).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(
                wait_state(&orch).await,
                "pending",
                "before the bound, nothing moves"
            );

            orch.adoption.backdate_for_test(adopted::RELEASE_AFTER);
            adopted::judge_unheard(&orch).await;
            let resolution = resolution(&orch).await;
            assert_eq!(resolution, adopted::release_text("request-1", &[], false));
            let (state, holds): (String, i64) = orch
                .db
                .local
                .query_one(
                    "SELECT state, holds_branch FROM run_batch_detached WHERE request_id='request-1'",
                    (),
                    |r| Ok((r.text(0)?, r.i64(1)?)),
                )
                .await
                .unwrap();
            assert_eq!((state.as_str(), holds), ("running", 1), "the hold stands");

            let mut frames = orch
                .fleet
                .attach_reported_machine_for_test("exec-a", &[("request-1", "attempt-1")]);
            adopted::start(&orch);
            orch.fleet.handle_executor_message(
                "exec-a",
                1,
                result(completed(&orch, "finished while away", None)),
            );
            assert!(is_ack(&next_frame(&mut frames, is_ack).await));
            let mut settled = None;
            for _ in 0..400 {
                let row: (String, Option<String>) = orch
                    .db
                    .local
                    .query_one(
                        "SELECT state, output FROM run_batch_detached WHERE request_id='request-1'",
                        (),
                        |r| Ok((r.text(0)?, r.opt_text(1)?)),
                    )
                    .await
                    .unwrap();
                if row.0 == "settled" {
                    settled = row.1;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert!(
                settled
                    .as_deref()
                    .is_some_and(|output| output.contains("finished while away")),
                "{settled:?}"
            );
            let notices: i64 = orch
                .db
                .local
                .query_one(
                    "SELECT COUNT(*) FROM suppressed_wakes WHERE source_kind='run_batch' AND source_ref='request-1'",
                    (),
                    |r| r.i64(0),
                )
                .await
                .unwrap();
            assert_eq!(notices, 1);
            assert_eq!(
                tool_result_count(&orch).await,
                1,
                "only the release answered the call"
            );
        }

        /// A batch on the colocated executor cannot outlive the host (tenet
        /// 316): its call is answered with the restart, naming the request and
        /// `local`, without waiting.
        #[tokio::test]
        async fn a_local_batch_is_answered_with_the_restart_at_once() {
            let (orch, base, _) = durable_env().await;
            let mut record = run_batch_record(&base, true);
            with_settlement(
                &mut record,
                PersistedSettlement::for_test(
                    Some("feat: x"),
                    None,
                    Some(PlacedOn {
                        executor_id: crate::fleet::COLOCATED_EXECUTOR_ID.into(),
                        name: "local".into(),
                    }),
                ),
            );
            parked(&orch, &record).await;

            adopted::judge_unheard(&orch).await;
            let resolution = resolution(&orch).await;
            assert_eq!(resolution, adopted::local_restart_text("request-1", true));
            assert!(resolution.contains("request-1") && resolution.contains("`local`"));
        }
    }

    /// A host batch ran inside the process that died, so there is nothing to
    /// wait for: its call is answered at once, naming the request, and no
    /// detached row is left running.
    #[tokio::test]
    async fn a_pending_host_run_batch_resolves_to_a_typed_host_restart() {
        let (orch, base, _) = durable_env().await;
        register_warm(&orch);
        let mut record = run_batch_record(&base, false);
        if let Condition::RunBatch { host, .. } = &mut record.condition {
            *host = true;
        }
        insert(&orch.db.local, &record).await.unwrap();

        reconcile(&orch).await;
        for _ in 0..200 {
            if wait_state(&orch).await == "resolved" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(wait_state(&orch).await, "resolved");

        let resolution = orch
            .db
            .local
            .query_one(
                "SELECT resolution_json FROM agent_waits WHERE id='wait-1'",
                (),
                |r| r.text(0),
            )
            .await
            .unwrap();
        assert_eq!(
            resolution,
            super::super::run::adopted::host_restart_text("request-1")
        );
        assert!(resolution.contains("request-1"));
        let detached: i64 = orch
            .db
            .local
            .query_one("SELECT COUNT(*) FROM run_batch_detached", (), |r| r.i64(0))
            .await
            .unwrap();
        assert_eq!(detached, 0);
        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
        assert_eq!(tool_result_count(&orch).await, 1);
    }

    /// Abandoning a run batch's wait converts its batch with the scope the wait
    /// recorded: a verdict-only batch without a `commit_msg` holds no branch,
    /// and its identity survives for the reissue refusal.
    #[tokio::test]
    async fn conversion_keeps_a_batchs_identity_and_verdict_only_scope() {
        let (orch, base, _) = durable_env().await;
        let mut record = run_batch_record(&base, false);
        if let Condition::RunBatch {
            identity,
            verdict_only,
            ..
        } = &mut record.condition
        {
            *identity = Some("same-batch".into());
            *verdict_only = true;
        }
        assert!(!record.condition.holds_branch());
        insert(&orch.db.local, &record).await.unwrap();
        assert!(
            mark_abandoned(&orch.db.local, "wait-1", AbandonCause::Superseded)
                .await
                .unwrap()
        );
        let (identity, holds): (Option<String>, i64) = orch
            .db
            .local
            .query_one(
                "SELECT identity, holds_branch FROM run_batch_detached WHERE request_id='request-1'",
                (),
                |r| Ok((r.opt_text(0)?, r.i64(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(identity.as_deref(), Some("same-batch"));
        assert_eq!(holds, 0, "a verdict-only batch never holds its branch");
    }

    /// A run batch has no waitFor condition to await; asking for one is a bug,
    /// not a silent no-op that would hang the resolver.
    #[tokio::test]
    async fn a_run_batch_has_no_pollable_wait_condition() {
        let (orch, base, _) = durable_env().await;
        let record = run_batch_record(&base, false);
        let error = super::super::owned_wait::trigger(orch.clone(), orch.db.local.clone(), record)
            .await
            .unwrap_err();
        assert!(error.contains("no waitFor condition"), "{error}");
    }

    type RecordedToolCall = (Option<serde_json::Value>, Option<String>, Option<String>);
    type RecordedTaskUpdate = (String, serde_json::Value, Option<String>);

    #[derive(Default)]
    struct FakeMcpGateway {
        calls: Mutex<Vec<RecordedToolCall>>,
        call_outcomes: Mutex<VecDeque<McpCallOutcome>>,
        fail_call_after_dispatch: AtomicBool,
        task_outcomes: Mutex<VecDeque<Result<McpTaskOutcome, String>>>,
        task_polls: Mutex<Vec<String>>,
        updates: Mutex<Vec<RecordedTaskUpdate>>,
        fail_update_after_dispatch: AtomicBool,
    }

    #[async_trait]
    impl McpGateway for FakeMcpGateway {
        async fn list_tools(
            &self,
            _: &str,
            _: &str,
            _: &crate::security::BrokeredMcpConfig,
        ) -> Result<McpToolCatalog, String> {
            Ok(McpToolCatalog::default())
        }
        async fn list_resources(
            &self,
            _: &str,
            _: &str,
            _: &crate::security::BrokeredMcpConfig,
        ) -> Result<Vec<McpResourceDef>, String> {
            Ok(vec![])
        }
        async fn read_resource(
            &self,
            _: &str,
            _: &str,
            _: &crate::security::BrokeredMcpConfig,
            _: &str,
        ) -> Result<String, String> {
            Ok(String::new())
        }
        async fn call_tool_once(
            &self,
            _: &str,
            _: &str,
            _: &crate::security::BrokeredMcpConfig,
            _: &str,
            _: serde_json::Value,
            input: Option<serde_json::Value>,
            request_state: Option<String>,
            _: Option<u32>,
            operation_id: Option<&str>,
        ) -> Result<McpCallOutcome, String> {
            self.calls.lock().unwrap().push((
                input,
                request_state,
                operation_id.map(str::to_owned),
            ));
            if self.fail_call_after_dispatch.swap(false, Ordering::SeqCst) {
                return Err("injected crash after tools/call dispatch".into());
            }
            self.call_outcomes
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| "unexpected tool retry".into())
        }
        async fn get_task(
            &self,
            _: &str,
            _: &str,
            _: &crate::security::BrokeredMcpConfig,
            task_id: &str,
        ) -> Result<McpTaskOutcome, String> {
            self.task_polls.lock().unwrap().push(task_id.into());
            self.task_outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err("task was lost".into()))
        }
        async fn update_task(
            &self,
            _: &str,
            _: &str,
            _: &crate::security::BrokeredMcpConfig,
            task_id: &str,
            input: serde_json::Value,
            operation_id: Option<&str>,
        ) -> Result<(), String> {
            self.updates.lock().unwrap().push((
                task_id.into(),
                input,
                operation_id.map(str::to_owned),
            ));
            if self
                .fail_update_after_dispatch
                .swap(false, Ordering::SeqCst)
            {
                return Err("injected crash after tasks/update dispatch".into());
            }
            Ok(())
        }
        async fn close_session(&self, _: &str) {}
    }

    fn mcp_state() -> super::super::mcp_continuation::McpContinuationState {
        super::super::mcp_continuation::McpContinuationState {
            server: "fake".into(),
            session_key: "job-1".into(),
            config: serde_json::from_value(serde_json::json!({})).unwrap(),
            tool: "durable".into(),
            arguments: serde_json::json!({"original": true}),
            request_state: None,
            timeout_ms: None,
            pending_operation: None,
            pending_input_requests: None,
            task_input_pending: false,
            mrtr_round: 0,
            pending_prompt_id: None,
            task: None,
            next_poll_at_ms: None,
            deadline_ms: None,
        }
    }

    async fn drive_mcp_until_terminal(
        orch: Orchestrator,
        db: Arc<LocalDb>,
        record: Record,
    ) -> String {
        loop {
            match super::super::mcp_continuation::drive(orch.clone(), db.clone(), record.clone())
                .await
            {
                super::super::mcp_continuation::DriveOutcome::Terminal(result) => return result,
                super::super::mcp_continuation::DriveOutcome::Pending => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }

    async fn answer_latest_prompt(orch: &Orchestrator, answer: serde_json::Value) -> String {
        let prompt_id = loop {
            if let Ok(id) = orch.db.local.query_one(
                "SELECT id FROM prompts WHERE mcp_wait_id='wait-1' ORDER BY created_at DESC LIMIT 1", (), |r| r.text(0)
            ).await { break id; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let encoded = answer.to_string();
        let answers = crate::models::question::PromptAnswers::new(
            1,
            vec![crate::models::question::QuestionAnswer {
                index: 0,
                answer: encoded.clone(),
            }],
        )
        .unwrap();
        orch.db
            .local
            .execute(
                "UPDATE prompts SET response=?1, answered_at=1 WHERE id=?2",
                (answers.to_stored(), prompt_id.clone()),
            )
            .await
            .unwrap();
        let _ = orch.prompt_responses.send((
            prompt_id.clone(),
            crate::mcp::handlers::planning::AnsweredPrompt {
                answers,
                rendered: encoded,
            },
        ));
        prompt_id
    }

    async fn leave_prompt_pending_after_host_loss(orch: &Orchestrator, record: &Record) -> String {
        let driver = tokio::spawn(drive_mcp_until_terminal(
            orch.clone(),
            orch.db.local.clone(),
            record.clone(),
        ));
        let prompt_id = loop {
            if let Ok(id) = orch.db.local.query_one(
                "SELECT id FROM prompts WHERE mcp_wait_id='wait-1' ORDER BY created_at DESC LIMIT 1", (), |r| r.text(0)
            ).await {
                let persisted = super::super::mcp_continuation::load(&orch.db.local, "wait-1")
                    .await
                    .unwrap()
                    .and_then(|state| state.pending_prompt_id);
                if persisted.as_deref() == Some(id.as_str()) {
                    break id;
                }
            }
            tokio::task::yield_now().await;
        };
        driver.abort();
        let _ = driver.await;
        prompt_id
    }

    async fn wait_for_reconciled_resolution(orch: &Orchestrator) {
        for _ in 0..200 {
            super::super::mcp_continuation::scheduler_tick(orch).await;
            if wait_state(orch).await == "resolved" {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("restart reconciliation did not resolve the MCP wait");
    }

    #[tokio::test]
    async fn restart_reconciliation_redrives_a_pending_direct_prompt_once() {
        let (orch, mut record, _) = durable_env().await;
        register_warm(&orch);
        let gateway = Arc::new(FakeMcpGateway::default());
        gateway
            .call_outcomes
            .lock()
            .unwrap()
            .push_back(McpCallOutcome::Complete(McpToolCallResult::from_text(
                "restart complete",
            )));
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut state = mcp_state();
        state.pending_input_requests = Some(serde_json::json!([{"id":"approval"}]));
        state.request_state = Some("restart-round".into());
        record.condition = Condition::McpContinuation { state };
        record.deadline = None;
        insert(&orch.db.local, &record).await.unwrap();
        leave_prompt_pending_after_host_loss(&orch, &record).await;
        answer_latest_prompt(&orch, serde_json::json!({"approval":true})).await;

        reconcile(&orch).await;
        wait_for_reconciled_resolution(&orch).await;

        assert_eq!(gateway.calls.lock().unwrap().len(), 1);
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
    }

    // ---- The schedule columns and the scheduler that reads them ------------

    #[test]
    fn every_condition_kind_is_its_serde_tag() {
        for condition in [
            Condition::Duration,
            Condition::Terminal {
                uri: "cairn:~/terminal/t".into(),
                slug: "t".into(),
                owner: None,
                on: TerminalWaitEvent::Exit,
                phrase: None,
            },
            Condition::Checks {
                uri: "cairn:~/checks".into(),
                job_id: "job-1".into(),
                suite: None,
            },
            Condition::McpContinuation { state: mcp_state() },
            run_batch_condition(false, None),
        ] {
            let json = serde_json::to_value(&condition).unwrap();
            assert_eq!(json["kind"], condition.kind(), "{json}");
        }
    }

    /// The plan's `detail` column for `sql`, with its placeholders replaced by
    /// literals.
    async fn query_plan(db: &LocalDb, sql: String) -> Vec<String> {
        db.read(move |conn| {
            let sql = format!("EXPLAIN QUERY PLAN {sql}");
            Box::pin(async move {
                let mut rows = conn.query(&sql, ()).await?;
                let mut steps = Vec::new();
                while let Some(row) = rows.next().await? {
                    steps.push(row.text(3)?);
                }
                Ok(steps)
            })
        })
        .await
        .unwrap()
    }

    fn due_sql_with_literals(sql: &str) -> String {
        sql.replace("?1", "1000000")
            .replace("?2", "1000")
            .replace("?3", "64")
    }

    /// The scheduler's tick and the stuck-resolution sweep read their partial
    /// indexes rather than the table, and a lookup by id still goes through
    /// the primary key. Every one of these returns correct rows on a full scan
    /// too, so only the plan witnesses a regression.
    #[tokio::test]
    async fn scheduler_and_sweep_reads_use_their_partial_indexes() {
        use super::super::mcp_continuation::{DUE_SQL, LOAD_RECORD_SQL, REFERENCE_DUE_SQL};
        let (orch, _, _) = durable_env().await;
        let db = &orch.db.local;

        let reference = query_plan(db, due_sql_with_literals(REFERENCE_DUE_SQL)).await;
        eprintln!("reference due plan:\n{}", reference.join("\n"));
        let due = query_plan(db, due_sql_with_literals(DUE_SQL)).await;
        eprintln!("due_wait_ids plan:\n{}", due.join("\n"));
        assert!(
            due.iter()
                .any(|step| step.contains("idx_agent_waits_mcp_due")),
            "{due:#?}"
        );
        assert!(
            !due.iter()
                .any(|step| step.starts_with("SCAN agent_waits") && !step.contains("INDEX")),
            "the tick must not scan the table: {due:#?}"
        );
        assert!(
            !due.iter().any(|step| step.starts_with("SCAN prompts")),
            "the awaited prompt is probed by primary key: {due:#?}"
        );

        let sweep = query_plan(db, STALE_RESOLVING_SQL.replace("?1", "1000")).await;
        eprintln!("sweep plan:\n{}", sweep.join("\n"));
        assert!(
            sweep
                .iter()
                .any(|step| step.contains("idx_agent_waits_resolving")),
            "{sweep:#?}"
        );

        let load = query_plan(db, LOAD_RECORD_SQL.replace("?1", "'wait-1'")).await;
        assert!(
            load.iter()
                .any(|step| step.contains("sqlite_autoindex_agent_waits_1")),
            "a pending lookup by id must stay on the primary key: {load:#?}"
        );
    }

    async fn answer_through_the_real_path(orch: &Orchestrator, prompt_id: &str) {
        let answers = crate::models::question::PromptAnswers::new(
            1,
            vec![crate::models::question::QuestionAnswer {
                index: 0,
                answer: r#"{"approval":true}"#.into(),
            }],
        )
        .unwrap();
        crate::mcp::handlers::planning::answer_prompt_id(
            orch,
            prompt_id,
            answers,
            crate::turns::queries::ResolutionProvenance::new(
                cairn_common::identity::AppearanceTransport::ResourcePatch,
                Some("operator:test".into()),
            ),
        )
        .await
        .unwrap();
    }

    /// An MCP continuation that asked for input and is awaiting the answer.
    async fn awaiting_mcp_answer() -> (Orchestrator, String, Arc<FakeMcpGateway>) {
        let (orch, mut record, _) = durable_env().await;
        register_warm(&orch);
        let gateway = Arc::new(FakeMcpGateway::default());
        gateway
            .call_outcomes
            .lock()
            .unwrap()
            .push_back(McpCallOutcome::Complete(McpToolCallResult::from_text(
                "answered",
            )));
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut state = mcp_state();
        state.pending_input_requests = Some(serde_json::json!([{"id":"approval"}]));
        record.condition = Condition::McpContinuation { state };
        record.deadline = None;
        insert(&orch.db.local, &record).await.unwrap();
        let prompt_id = leave_prompt_pending_after_host_loss(&orch, &record).await;
        (orch, prompt_id, gateway)
    }

    async fn due_now(orch: &Orchestrator) -> Vec<String> {
        // Past the park handoff for a wait created just now.
        let after_handoff = chrono::Utc::now().timestamp_millis() + 2_000;
        super::super::mcp_continuation::due_wait_ids(&orch.db.local, after_handoff)
            .await
            .unwrap()
    }

    /// The answer is level-triggered durable state: committing it makes the
    /// wait due, and the next tick drives it to its result. Nothing arms it.
    #[tokio::test]
    async fn a_committed_answer_to_the_awaited_prompt_makes_its_wait_due() {
        let (orch, prompt_id, gateway) = awaiting_mcp_answer().await;
        assert!(!due_now(&orch).await.contains(&"wait-1".to_string()));

        answer_through_the_real_path(&orch, &prompt_id).await;
        assert!(due_now(&orch).await.contains(&"wait-1".to_string()));

        wait_for_reconciled_resolution(&orch).await;
        assert_eq!(gateway.calls.lock().unwrap().len(), 1);
        assert_eq!(tool_result_count(&orch).await, 1);
    }

    /// An answer committed while no host was driving the wait is found after a
    /// restart from the row alone: reconciliation's store rewrites the
    /// schedule columns, and they still lead the tick to the answer.
    #[tokio::test]
    async fn an_answer_committed_across_a_restart_is_found_from_the_row() {
        let (orch, prompt_id, gateway) = awaiting_mcp_answer().await;
        answer_through_the_real_path(&orch, &prompt_id).await;

        reconcile(&orch).await;
        let (pending_prompt, due_at) = orch
            .db
            .local
            .query_one(
                "SELECT pending_prompt_id,due_at_ms FROM agent_waits WHERE id='wait-1'",
                (),
                |row| Ok((row.opt_text(0)?, row.opt_i64(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(pending_prompt.as_deref(), Some(prompt_id.as_str()));
        assert!(
            due_at.is_some_and(|at| at > chrono::Utc::now().timestamp_millis()),
            "its own state makes it due only at its deadline; the answer is what makes it due now"
        );

        wait_for_reconciled_resolution(&orch).await;
        assert_eq!(gateway.calls.lock().unwrap().len(), 1);
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
    }

    /// `source` without its inline `#[cfg(test)]` modules, whatever they are
    /// named. A module's extent is found by counting braces outside string
    /// literals, which the SQL in test fixtures is full of.
    fn production_portion(source: &str) -> String {
        let bytes = source.as_bytes();
        let mut kept = String::new();
        let mut from = 0;
        while let Some(at) = source[from..].find("#[cfg(test)]") {
            let start = from + at;
            let after = source[start + "#[cfg(test)]".len()..].trim_start();
            let after = after
                .strip_prefix("pub(crate) ")
                .or_else(|| after.strip_prefix("pub(super) "))
                .or_else(|| after.strip_prefix("pub "))
                .unwrap_or(after);
            let header_end = after.find(['{', ';']).unwrap_or(after.len());
            if !after.starts_with("mod ") || !after[header_end..].starts_with('{') {
                kept.push_str(&source[from..start + 1]);
                from = start + 1;
                continue;
            }
            kept.push_str(&source[from..start]);
            let mut i = source.len() - after.len() + header_end;
            let mut depth = 0_usize;
            while i < bytes.len() {
                match bytes[i] {
                    b'{' => depth += 1,
                    b'}' => {
                        depth -= 1;
                        if depth == 0 {
                            i += 1;
                            break;
                        }
                    }
                    b'\'' if bytes.get(i + 2) == Some(&b'\'') => i += 2,
                    b'r' if bytes.get(i + 1) == Some(&b'#') && bytes.get(i + 2) == Some(&b'"') => {
                        i += source[i + 3..]
                            .find("\"#")
                            .map_or(bytes.len(), |end| end + 4);
                    }
                    b'"' => {
                        i += 1;
                        while i < bytes.len() && bytes[i] != b'"' {
                            i += if bytes[i] == b'\\' { 2 } else { 1 };
                        }
                    }
                    _ => {}
                }
                i += 1;
            }
            from = i.min(bytes.len());
        }
        kept.push_str(&source[from..]);
        kept
    }

    /// Each string literal in `source` from an `agent_waits` insert or update
    /// to the literal's closing quote.
    fn agent_wait_writes(source: &str) -> Vec<&str> {
        let bytes = source.as_bytes();
        let mut statements = Vec::new();
        for needle in ["INSERT INTO agent_waits", "UPDATE agent_waits"] {
            let mut from = 0;
            while let Some(at) = source[from..].find(needle) {
                let start = from + at;
                let mut end = start;
                while end < bytes.len() && bytes[end] != b'"' {
                    end += if bytes[end] == b'\\' { 2 } else { 1 };
                }
                statements.push(&source[start..end.min(bytes.len())]);
                from = start + needle.len();
            }
        }
        statements
    }

    /// The schedule columns are only as true as the one writer that derives
    /// them, so no other production statement may write a wait's condition or
    /// deadline, and both writer statements carry every derived column.
    #[test]
    fn only_the_condition_writer_writes_a_waits_condition() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut writers = Vec::new();
        let mut dirs = vec![root.clone()];
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    dirs.push(path);
                    continue;
                }
                let name = path.file_name().unwrap().to_string_lossy().into_owned();
                if !name.ends_with(".rs") || name.ends_with("_tests.rs") {
                    continue;
                }
                let source = std::fs::read_to_string(&path).unwrap();
                for statement in agent_wait_writes(&production_portion(&source)) {
                    if statement.contains("condition_json") || statement.contains("deadline_ms") {
                        writers.push((
                            path.strip_prefix(&root).unwrap().display().to_string(),
                            statement.to_string(),
                        ));
                    }
                }
            }
        }
        let mut found: Vec<&str> = writers.iter().map(|(_, sql)| sql.as_str()).collect();
        found.sort_unstable();
        let mut expected = vec![INSERT_WAIT_SQL, STORE_CONDITION_SQL];
        expected.sort_unstable();
        assert_eq!(found, expected, "{writers:#?}");
        for sql in expected {
            for column in ["kind", "due_at_ms", "next_poll_at_ms", "pending_prompt_id"] {
                assert!(sql.contains(column), "{sql} must write {column}");
            }
        }
    }

    /// A tick against a long history: `resolved` finished rows with large
    /// results, `others` pending non-MCP rows with large conditions, five due
    /// continuations and two that are not. Times both selections over `ticks`
    /// ticks and prints them.
    async fn tick_cost(resolved: usize, others: usize, ticks: u32) {
        use super::super::mcp_continuation::{
            due_wait_ids, query_due, DUE_SQL, REFERENCE_DUE_SQL, SCHEDULER_SCAN_LIMIT,
        };
        let (orch, _, _) = durable_env().await;
        let db = orch.db.local.clone();
        let now = chrono::Utc::now().timestamp_millis();
        let old = now - 60_000;
        let deadline = Some(now + 3_600_000);
        let result = format!(
            r#"{{"outcome":"completed","result":"{}"}}"#,
            "x".repeat(6_000)
        );
        let phrase = "y".repeat(8_192);

        let mut rows: Vec<(String, Condition, Option<i64>, bool)> = Vec::new();
        for n in 0..resolved {
            rows.push((
                format!("resolved-{n}"),
                run_batch_condition(false, Some("cargo test")),
                None,
                true,
            ));
        }
        for n in 0..others {
            rows.push((
                format!("terminal-{n}"),
                Condition::Terminal {
                    uri: "cairn:~/terminal/t".into(),
                    slug: "t".into(),
                    owner: None,
                    on: TerminalWaitEvent::Output,
                    phrase: Some(phrase.clone()),
                },
                None,
                false,
            ));
        }
        let mut due = Vec::new();
        for n in 0..5 {
            let mut state = mcp_state();
            state.pending_operation = Some(
                super::super::mcp_continuation::PendingOperation::ContinueTool {
                    operation_id: format!("op-{n}"),
                    input_responses: serde_json::json!({}),
                    request_state: None,
                },
            );
            due.push(format!("due-{n}"));
            rows.push((
                format!("due-{n}"),
                Condition::McpContinuation { state },
                deadline,
                false,
            ));
        }
        let mut awaiting = mcp_state();
        awaiting.pending_input_requests = Some(serde_json::json!([{"id":"q"}]));
        awaiting.pending_prompt_id = Some("prompt-unanswered".into());
        let mut polling = mcp_state();
        super::super::mcp_continuation::set_task(&mut polling, "task".into(), 600_000, now, None);
        for (id, state) in [("not-due-prompt", awaiting), ("not-due-poll", polling)] {
            rows.push((
                id.into(),
                Condition::McpContinuation { state },
                deadline,
                false,
            ));
        }

        for chunk in rows.chunks(500) {
            let chunk = chunk.to_vec();
            let result = result.clone();
            db.write(|conn| {
                let (chunk, result) = (chunk.clone(), result.clone());
                Box::pin(async move {
                    for (id, condition, deadline, finished) in &chunk {
                        let columns =
                            ConditionColumns::of(condition, *deadline).map_err(DbError::internal)?;
                        conn.execute(
                            INSERT_WAIT_SQL,
                            params![
                                id.as_str(),
                                "job-1",
                                "run-1",
                                "session-1",
                                "pred-turn",
                                id.as_str(),
                                columns.json,
                                *deadline,
                                columns.kind,
                                columns.due_at_ms,
                                columns.next_poll_at_ms,
                                columns.pending_prompt_id,
                                old,
                                None::<String>
                            ],
                        )
                        .await?;
                        if *finished {
                            conn.execute(
                                "UPDATE agent_waits SET state='resolved',resolution_json=?2,resolved_at=?3 WHERE id=?1",
                                params![id.as_str(), result.as_str(), old],
                            )
                            .await?;
                        }
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
        }

        let mut selected = due_wait_ids(&db, now).await.unwrap();
        selected.sort();
        assert_eq!(selected, due);
        let mut reference = query_due(&db, REFERENCE_DUE_SQL, now, SCHEDULER_SCAN_LIMIT)
            .await
            .unwrap();
        reference.sort();
        assert_eq!(reference, due);
        let plan = query_plan(&db, due_sql_with_literals(DUE_SQL)).await;
        assert!(
            plan.iter()
                .any(|step| step.contains("idx_agent_waits_mcp_due")),
            "{plan:#?}"
        );

        let mut elapsed = Vec::new();
        for sql in [REFERENCE_DUE_SQL, DUE_SQL] {
            let started = std::time::Instant::now();
            for _ in 0..ticks {
                query_due(&db, sql, now, SCHEDULER_SCAN_LIMIT)
                    .await
                    .unwrap();
            }
            elapsed.push(started.elapsed() / ticks);
        }
        eprintln!(
            "tick cost with {resolved} resolved and {others} pending non-MCP rows, over {ticks} ticks: \
             reference {:?} per tick, schedule columns {:?} per tick",
            elapsed[0], elapsed[1]
        );
    }

    /// The full-size measurement quoted on cairn/5866. Run explicitly:
    /// `bun run test:rust -p cairn-core --lib mcp::handlers::durable_suspend::tests::tick_cost_against_a_long_history -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore = "measurement: 20,000 resolved rows take a while to seed"]
    async fn tick_cost_against_a_long_history() {
        tick_cost(20_000, 500, 20).await;
    }

    type OwnedContinuationBarrier = (
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    );
    static OWNED_CONTINUATION_BARRIERS: std::sync::LazyLock<
        Mutex<std::collections::HashMap<String, OwnedContinuationBarrier>>,
    > = std::sync::LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

    /// Injected result-append failures, keyed by wait id, counting down. Keyed
    /// by id because tests share the process: a test arming a fault uses a wait
    /// id no other test does.
    static RESULT_APPEND_FAULTS: std::sync::LazyLock<
        Mutex<std::collections::HashMap<String, usize>>,
    > = std::sync::LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

    pub(super) fn fail_result_append(wait_id: &str) -> Option<String> {
        let mut faults = RESULT_APPEND_FAULTS.lock().unwrap();
        let remaining = faults.get_mut(wait_id)?;
        if *remaining == 0 {
            return None;
        }
        *remaining -= 1;
        Some(format!(
            "database transaction failed: injected result-append failure for {wait_id}"
        ))
    }

    fn arm_result_append_failures(wait_id: &str, count: usize) {
        RESULT_APPEND_FAULTS
            .lock()
            .unwrap()
            .insert(wait_id.to_string(), count);
    }

    fn remaining_result_append_failures(wait_id: &str) -> usize {
        RESULT_APPEND_FAULTS
            .lock()
            .unwrap()
            .get(wait_id)
            .copied()
            .unwrap_or(0)
    }

    pub(super) async fn before_owned_continuation(wait_id: &str) {
        let barrier = OWNED_CONTINUATION_BARRIERS.lock().unwrap().remove(wait_id);
        if let Some((entered, release)) = barrier {
            let _ = entered.send(());
            release
                .await
                .expect("race fixture must release its continuation");
        }
    }

    fn hold_owned_continuation(
        record: &Record,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        assert!(OWNED_CONTINUATION_BARRIERS
            .lock()
            .unwrap()
            .insert(record.id.clone(), (entered_tx, release_rx))
            .is_none());
        (entered_rx, release_tx)
    }

    fn owned_goal_reply(status: &str) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({"result":{"goal":{
            "threadId":"native-thread", "objective":"finish assigned issue",
            "status":status, "tokenBudget":null, "tokensUsed":0,
            "timeUsedSeconds":0, "createdAt":1, "updatedAt":2
        }}}))
    }

    type OwnedGoalRequests = Arc<Mutex<Vec<serde_json::Value>>>;

    async fn owned_goal_env() -> (Orchestrator, Record, OwnedGoalRequests, Arc<Mutex<String>>) {
        let (orch, base, _) = durable_env().await;
        // Match owned_wait's durable carrier construction; the same minted id
        // is persisted by suspend and read by resolve, never a separate token.
        let record = Record {
            id: cairn_common::ids::mint_child(&base.run_id),
            ..run_batch_record(&base, false)
        };
        let requests = Arc::new(Mutex::new(Vec::new()));
        let goal_status = Arc::new(Mutex::new("active".to_string()));
        let provider_goal = goal_status.clone();
        let client = Arc::new(
            cairn_models::codex::app_server::AppServerClient::for_test_responding(
                move |request| match request["method"].as_str().unwrap() {
                    "thread/goal/get" => owned_goal_reply(&provider_goal.lock().unwrap()),
                    "thread/goal/set" => {
                        let status = request["params"]["status"].as_str().unwrap().to_string();
                        *provider_goal.lock().unwrap() = status.clone();
                        owned_goal_reply(&status)
                    }
                    "turn/interrupt" => Ok(serde_json::Value::Null),
                    "turn/start" => Ok(
                        serde_json::json!({"result":{"turn":{"id":cairn_common::ids::mint_child("provider")}}}),
                    ),
                    method => Err(format!("unexpected owned-goal fixture method: {method}")),
                },
                requests.clone(),
            ),
        );
        let stdin: Box<dyn crate::agent_process::process::SessionControl> =
            Box::new(cairn_models::session::codex::CodexControl::new(
                client,
                "native-thread".into(),
                "/tmp/project".into(),
                None,
                None,
                None,
                Arc::new(Mutex::new(Some("provider-predecessor".into()))),
            ));
        let mut handle = crate::agent_process::process::RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Some(stdin))),
            Some(record.session_id.clone()),
            Some(record.job_id.clone()),
        );
        handle.backend = Some("codex".into());
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register(record.run_id.clone(), handle);
        orch.process_state
            .set_current_turn_id(&record.run_id, Some(&record.turn_id));
        (orch, record, requests, goal_status)
    }

    fn owned_prompts(requests: &OwnedGoalRequests) -> Vec<String> {
        requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .map(|request| {
                request["params"]["input"][0]["text"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect()
    }

    fn owned_active_requests(requests: &OwnedGoalRequests) -> usize {
        requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| {
                request["method"] == "thread/goal/set" && request["params"]["status"] == "active"
            })
            .count()
    }

    async fn finish_owned_delivery(orch: &Orchestrator) {
        let turn = orch.process_state.get_current_turn_id("run-1").unwrap();
        orch.db
            .local
            .execute("UPDATE turns SET state='complete' WHERE id=?1", (turn,))
            .await
            .unwrap();
        orch.process_state.transition_to_warm("run-1");
    }

    async fn deliver_owned_check_push(orch: &Orchestrator) {
        use crate::orchestrator::attention_push::{push, Boundary, Wake};
        let uri = "cairn://p/prj/1/1/builder/checks";
        orch.db.local.execute(
            "INSERT INTO check_result_cache(project_id,tree_hash,input_hash,check_name,exit_code,passed,output_tail,duration_ms,ran_at,job_id) VALUES('p','tree','input','ownership-check',0,1,'',1,1,'job-1')", ()
        ).await.unwrap();
        let (id, _) = push(
            &orch.db.local,
            "job-1",
            uri,
            Wake::Wake,
            Boundary::Turn,
            &format!("turn-checks:{uri}"),
        )
        .await
        .unwrap();
        continue_job_impl(
            orch,
            "job-1",
            None,
            None,
            Some(ResumeContext {
                suppress_user_event: true,
                ..Default::default()
            }),
        )
        .unwrap();
        let delivered = orch
            .db
            .local
            .query_one(
                "SELECT delivered_event_id IS NOT NULL FROM attention_pushes WHERE id=?1",
                (id,),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(
            delivered, 1,
            "the real check push must reach the resumed turn"
        );
    }

    async fn assert_owned_sibling_completion_order(b_first: bool) {
        let (orch, a, requests, goal_status) = owned_goal_env().await;
        let b = Record {
            id: cairn_common::ids::mint_child(&a.run_id),
            tool_use_id: "tool-2".into(),
            ..a.clone()
        };
        assert_ne!(a.id, b.id);
        assert_eq!(a.turn_id, b.turn_id);
        assert!(
            suspend(&orch, &orch.db.local, &a)
                .await
                .unwrap()
                .parked()
                .await
        );
        assert!(
            suspend(&orch, &orch.db.local, &b)
                .await
                .unwrap()
                .parked()
                .await
        );
        let (first, first_result, last, last_result) = if b_first {
            (&b, "B sibling result", &a, "A sibling result")
        } else {
            (&a, "A sibling result", &b, "B sibling result")
        };
        resolve(&orch, &orch.db.local, first, first_result, false)
            .await
            .unwrap();
        assert_eq!(
            owned_active_requests(&requests),
            0,
            "a pending sibling still owns the pause"
        );
        assert_eq!(*goal_status.lock().unwrap(), "paused");
        assert!(
            owned_prompts(&requests).is_empty(),
            "no continuation before every sibling resolves"
        );
        assert_eq!(tool_results(&orch).await.len(), 1);
        assert_eq!(
            load_resolution(&orch.db.local, &last.id)
                .await
                .unwrap()
                .unwrap()
                .0,
            "pending"
        );

        resolve(&orch, &orch.db.local, last, last_result, false)
            .await
            .unwrap();
        assert_eq!(
            owned_active_requests(&requests),
            1,
            "the last sibling must release the cohort's live owner"
        );
        assert_eq!(*goal_status.lock().unwrap(), "active");
        {
            let sent = requests.lock().unwrap();
            let active = sent
                .iter()
                .position(|r| r["method"] == "thread/goal/set" && r["params"]["status"] == "active")
                .unwrap();
            let delivery = sent
                .iter()
                .position(|r| r["method"] == "turn/start")
                .unwrap();
            assert!(
                active < delivery,
                "sibling cohort reactivation must precede delivery"
            );
        }
        let prompts = owned_prompts(&requests);
        assert_eq!(prompts.len(), 1);
        for expected in ["A sibling result", "B sibling result", "tool-1", "tool-2"] {
            assert!(
                prompts[0].contains(expected),
                "missing {expected}: {}",
                prompts[0]
            );
        }
        assert!(
            !prompts[0].contains("[Goal state: paused"),
            "{}",
            prompts[0]
        );
        let results = tool_results(&orch).await;
        assert_eq!(results.len(), 2);
        assert!(results
            .iter()
            .any(|(_, result)| result.contains("tool-1") && result.contains("A sibling result")));
        assert!(results
            .iter()
            .any(|(_, result)| result.contains("tool-2") && result.contains("B sibling result")));
        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
    }

    #[tokio::test]
    async fn owned_sibling_a_then_b_resolves_after_the_last_result() {
        assert_owned_sibling_completion_order(false).await;
    }

    #[tokio::test]
    async fn owned_sibling_b_then_a_resolves_after_the_last_result() {
        assert_owned_sibling_completion_order(true).await;
    }

    #[tokio::test]
    async fn owned_park_after_operator_stop_stays_off() {
        let (orch, mut record, requests, goal_status) = owned_goal_env().await;
        crate::orchestrator::lifecycle::operator_stop_run(&orch, &record.run_id).unwrap();
        // Ordinary delivery is admitted even though autonomous goal
        // continuation is off. Its tool may need to park in turn.
        deliver_owned_attention(&orch, false).await;
        let next_turn = orch
            .process_state
            .get_current_turn_id(&record.run_id)
            .unwrap();
        assert_ne!(next_turn, record.turn_id);
        record.turn_id = next_turn;
        assert_eq!(*goal_status.lock().unwrap(), "paused");
        assert_eq!(owned_active_requests(&requests), 0);
        let earlier_deliveries = owned_prompts(&requests).len();
        assert!(
            suspend(&orch, &orch.db.local, &record)
                .await
                .unwrap()
                .parked()
                .await
        );
        assert_eq!(owned_active_requests(&requests), 0);
        resolve(
            &orch,
            &orch.db.local,
            &record,
            "result under pre-existing hold",
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            owned_active_requests(&requests),
            0,
            "a wait resolution must not turn the operator's goal back on"
        );
        assert_eq!(*goal_status.lock().unwrap(), "paused");
        let prompts = owned_prompts(&requests);
        assert_eq!(prompts.len(), earlier_deliveries + 1);
        let delivered = prompts.last().unwrap();
        assert!(!delivered.contains("[Goal state"), "{delivered}");
        assert!(delivered.contains("result under pre-existing hold"));
        assert_eq!(tool_results(&orch).await.len(), 1);
        assert!(owned_event_data(&orch, "system:codex_goal_reactivated")
            .await
            .is_empty());
        assert_eq!(
            crate::execution::jobs::goal_intent_for_job(&orch, &record.job_id).unwrap(),
            crate::execution::jobs::GoalIntent::Off
        );
    }

    #[tokio::test]
    async fn owned_park_after_claimless_provider_pause_acquires_and_releases_ownership() {
        let (orch, record, requests, goal_status) = owned_goal_env().await;
        *goal_status.lock().unwrap() = "paused".into();
        assert!(
            suspend(&orch, &orch.db.local, &record)
                .await
                .unwrap()
                .parked()
                .await
        );
        let control = orch
            .process_state
            .processes
            .lock()
            .unwrap()
            .get(&record.run_id)
            .unwrap()
            .goal_control();
        assert_eq!(
            control
                .with_control(|_, claim| Ok(claim.inspect()))
                .unwrap(),
            Some(crate::agent_process::process::GoalPause::Owned(
                crate::agent_process::process::GoalPauseOwner::DurableWait {
                    wait_id: record.id.clone()
                }
            ))
        );
        assert_eq!(owned_active_requests(&requests), 0);
        resolve(&orch, &orch.db.local, &record, "owned result", false)
            .await
            .unwrap();
        assert_eq!(owned_active_requests(&requests), 1);
        assert_eq!(*goal_status.lock().unwrap(), "active");
        let prompts = owned_prompts(&requests);
        assert_eq!(prompts.len(), 1);
        assert!(prompts[0].contains("owned result"));
        assert!(!prompts[0].contains("operator Stop"));
        let events = owned_event_data(&orch, "system:codex_goal_reactivated").await;
        assert_eq!(events.len(), 1);
        assert!(events[0].contains("durable_wait"));
        assert_eq!(
            control
                .with_control(|_, claim| Ok(claim.inspect()))
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn owned_run_timeout_reactivates_before_delivery_without_pause_notice() {
        let (orch, record, requests, goal_status) = owned_goal_env().await;
        assert!(
            suspend(&orch, &orch.db.local, &record)
                .await
                .unwrap()
                .parked()
                .await
        );
        resolve(&orch, &orch.db.local, &record, "batch timed out", false)
            .await
            .unwrap();
        {
            let sent = requests.lock().unwrap();
            let active = sent
                .iter()
                .position(|r| r["method"] == "thread/goal/set" && r["params"]["status"] == "active")
                .expect("the owned timeout must reactivate the goal");
            let delivery = sent
                .iter()
                .position(|r| r["method"] == "turn/start")
                .unwrap();
            assert!(
                active < delivery,
                "reactivation must precede the model-facing result"
            );
        }
        let prompts = owned_prompts(&requests);
        assert_eq!(prompts.len(), 1);
        assert!(prompts[0].contains("batch timed out"));
        assert!(
            !prompts[0].contains("[Goal state: paused"),
            "{}",
            prompts[0]
        );
        assert_eq!(tool_results(&orch).await.len(), 1);
        assert_eq!(*goal_status.lock().unwrap(), "active");
    }

    #[tokio::test]
    async fn owned_check_push_after_resolution_has_no_stale_pause() {
        let (orch, record, requests, goal_status) = owned_goal_env().await;
        assert!(
            suspend(&orch, &orch.db.local, &record)
                .await
                .unwrap()
                .parked()
                .await
        );
        resolve(&orch, &orch.db.local, &record, "batch timed out", false)
            .await
            .unwrap();
        assert_eq!(
            owned_active_requests(&requests),
            1,
            "resolution, not check attention, owns reactivation"
        );
        finish_owned_delivery(&orch).await;
        deliver_owned_check_push(&orch).await;
        let prompts = owned_prompts(&requests);
        assert_eq!(prompts.len(), 2);
        assert!(prompts[1].contains("ownership-check"), "{}", prompts[1]);
        assert!(
            prompts
                .iter()
                .all(|prompt| !prompt.contains("[Goal state: paused")),
            "{prompts:?}"
        );
        assert_eq!(owned_active_requests(&requests), 1);
        assert_eq!(*goal_status.lock().unwrap(), "active");
    }

    #[tokio::test]
    async fn owned_check_push_midpark_preserves_the_wait_and_arrives_as_written() {
        let (orch, record, requests, goal_status) = owned_goal_env().await;
        assert!(
            suspend(&orch, &orch.db.local, &record)
                .await
                .unwrap()
                .parked()
                .await
        );
        deliver_owned_check_push(&orch).await;
        assert_eq!(
            load_resolution(&orch.db.local, &record.id)
                .await
                .unwrap()
                .unwrap()
                .0,
            "pending"
        );
        assert_eq!(owned_active_requests(&requests), 0);
        let prompts = owned_prompts(&requests);
        assert_eq!(prompts.len(), 1);
        assert!(prompts[0].contains("ownership-check"));
        assert!(!prompts[0].contains("[Goal state"), "{}", prompts[0]);
        assert_eq!(*goal_status.lock().unwrap(), "paused");
    }

    async fn deliver_owned_attention(orch: &Orchestrator, operator: bool) {
        continue_job_impl(
            orch,
            "job-1",
            Some(if operator {
                "operator resume"
            } else {
                "peer direct message"
            }),
            None,
            Some(ResumeContext {
                suppress_user_event: !operator,
                goal_reactivation: operator.then(|| {
                    crate::execution::jobs::GoalReactivation::operator_message("operator-message")
                }),
                ..Default::default()
            }),
        )
        .unwrap();
    }

    async fn owned_event_data(orch: &Orchestrator, event_type: &str) -> Vec<String> {
        let event_type = event_type.to_string();
        orch.db.local.read(|conn| { let event_type = event_type.clone(); Box::pin(async move {
            let mut rows = conn.query("SELECT data FROM events WHERE run_id='run-1' AND event_type=?1 ORDER BY sequence", (event_type,)).await?;
            let mut result = Vec::new();
            while let Some(row) = rows.next().await? { result.push(row.text(0)?); }
            Ok(result)
        }) }).await.unwrap()
    }

    /// Stop turns the goal off. Neither the wait it raced, a peer's attention,
    /// nor the operator's own message turns it back on; each still runs its
    /// own turn. Only the operator's toggle does.
    #[tokio::test]
    async fn only_the_toggle_lifts_an_operator_stop() {
        let (orch, record, requests, goal_status) = owned_goal_env().await;
        assert!(
            suspend(&orch, &orch.db.local, &record)
                .await
                .unwrap()
                .parked()
                .await
        );
        let (entered, release) = hold_owned_continuation(&record);
        let resolving = resolve(&orch, &orch.db.local, &record, "real timeout result", false);
        let stopping = async {
            entered.await.unwrap();
            crate::orchestrator::lifecycle::operator_stop_run(&orch, &record.run_id).unwrap();
            release.send(()).unwrap();
        };
        let (resolved, ()) = tokio::join!(resolving, stopping);
        resolved.unwrap();
        assert_eq!(owned_active_requests(&requests), 0);
        assert!(owned_prompts(&requests)[0].contains("real timeout result"));
        assert_eq!(tool_results(&orch).await.len(), 1);
        finish_owned_delivery(&orch).await;
        deliver_owned_attention(&orch, false).await;
        assert_eq!(owned_active_requests(&requests), 0);
        finish_owned_delivery(&orch).await;
        deliver_owned_attention(&orch, true).await;
        assert_eq!(
            owned_active_requests(&requests),
            0,
            "an operator message runs its turn without turning the goal on"
        );
        let prompts = owned_prompts(&requests);
        assert_eq!(prompts.len(), 3);
        assert!(
            prompts.iter().all(|prompt| !prompt.contains("[Goal state")),
            "{prompts:?}"
        );
        assert_eq!(*goal_status.lock().unwrap(), "paused");
        assert_eq!(
            crate::execution::jobs::goal_intent_for_job(&orch, &record.job_id).unwrap(),
            crate::execution::jobs::GoalIntent::Off
        );

        let state = crate::execution::jobs::set_job_goal_intent(
            &orch,
            &record.job_id,
            crate::execution::jobs::GoalIntent::On,
        )
        .unwrap();
        assert_eq!(state.intent, crate::execution::jobs::GoalIntent::On);
        assert_eq!(owned_active_requests(&requests), 1);
        assert_eq!(*goal_status.lock().unwrap(), "active");
    }

    #[tokio::test]
    async fn owned_complete_remains_terminal_across_resolution_peer_and_operator() {
        let (orch, record, requests, goal_status) = owned_goal_env().await;
        assert!(
            suspend(&orch, &orch.db.local, &record)
                .await
                .unwrap()
                .parked()
                .await
        );
        *goal_status.lock().unwrap() = "complete".into();
        resolve(&orch, &orch.db.local, &record, "completed batch", false)
            .await
            .unwrap();
        finish_owned_delivery(&orch).await;
        deliver_owned_attention(&orch, false).await;
        finish_owned_delivery(&orch).await;
        deliver_owned_attention(&orch, true).await;
        assert_eq!(owned_active_requests(&requests), 0);
        let prompts = owned_prompts(&requests);
        assert_eq!(prompts.len(), 3);
        assert!(
            prompts.iter().all(|p| !p.contains("[Goal state")),
            "{prompts:?}"
        );
        assert_eq!(tool_results(&orch).await.len(), 1);
        assert!(owned_event_data(&orch, "system:codex_goal_reactivated")
            .await
            .is_empty());
        assert_eq!(*goal_status.lock().unwrap(), "complete");
    }

    #[tokio::test]
    async fn owned_stale_inflight_wait_cannot_lift_newer_wait() {
        let (orch, a, requests, goal_status) = owned_goal_env().await;
        assert!(
            suspend(&orch, &orch.db.local, &a)
                .await
                .unwrap()
                .parked()
                .await
        );
        let b = Record {
            id: cairn_common::ids::mint_child(&a.run_id),
            turn_id: cairn_common::ids::mint_child(&a.session_id),
            tool_use_id: "tool-2".into(),
            ..a.clone()
        };
        assert_ne!(a.id, b.id);
        let (entered, release) = hold_owned_continuation(&a);
        let resolving = resolve(&orch, &orch.db.local, &a, "A result", false);
        let newer_park = async {
            entered.await.unwrap();
            // A owns its already-created successor. A separately admitted turn
            // can park B before A reaches the warm goal-release seam.
            orch.db.local.execute(
                "INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES(?1,'session-1','run-1','job-1',(SELECT MAX(sequence)+1 FROM turns WHERE session_id='session-1'),'running','follow_up',1,1)",
                (b.turn_id.clone(),),
            ).await.unwrap();
            orch.process_state
                .set_current_turn_id(&b.run_id, Some(&b.turn_id));
            assert!(
                suspend(&orch, &orch.db.local, &b)
                    .await
                    .unwrap()
                    .parked()
                    .await
            );
            release.send(()).unwrap();
        };
        let (resolved, ()) = tokio::join!(resolving, newer_park);
        resolved.unwrap();
        assert_eq!(
            owned_active_requests(&requests),
            0,
            "A must not consume B's pause"
        );
        let prompts = owned_prompts(&requests);
        assert_eq!(prompts.len(), 1);
        assert!(prompts[0].contains("A result"));
        assert!(!prompts[0].contains("[Goal state"), "{}", prompts[0]);
        assert_eq!(tool_results(&orch).await.len(), 1);
        assert_eq!(
            load_resolution(&orch.db.local, &b.id)
                .await
                .unwrap()
                .unwrap()
                .0,
            "pending"
        );
        finish_owned_delivery(&orch).await;
        resolve(&orch, &orch.db.local, &b, "B result", false)
            .await
            .unwrap();
        assert_eq!(owned_active_requests(&requests), 1);
        assert_eq!(tool_results(&orch).await.len(), 2);
        let prompts = owned_prompts(&requests);
        assert_eq!(prompts.len(), 2);
        assert!(prompts[1].contains("B result"));
        assert!(!prompts[1].contains("[Goal state: paused"));
        assert_eq!(*goal_status.lock().unwrap(), "active");
    }

    #[tokio::test]
    async fn run_batch_park_owns_native_goal_before_handoff_and_resumes_original_call() {
        let (orch, record, _) = durable_env().await;
        let record = run_batch_record(&record, false);
        let (stdin, requests, _) = crate::backends::codex::scripted_durable_wait_stdin_for_test(
            "native-thread",
            "provider-predecessor",
        );
        let child = Arc::new(std::sync::Mutex::new(None));
        let stdin = Arc::new(std::sync::Mutex::new(Some(stdin)));
        let mut handle = crate::agent_process::process::RunHandle::new(
            child,
            stdin,
            Some("session-1".to_string()),
            Some("job-1".to_string()),
        );
        handle.backend = Some("codex".to_string());
        handle.begin_turn("pred-turn");
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("run-1".to_string(), handle);

        let handoff = suspend(&orch, &orch.db.local, &record).await.unwrap();
        {
            let sent = requests.lock().unwrap();
            assert_eq!(
                sent.len(),
                2,
                "pause is acknowledged before handoff returns"
            );
            assert_eq!(sent[0]["method"], "thread/goal/get");
            assert_eq!(sent[1]["method"], "thread/goal/set");
            assert_eq!(sent[1]["params"]["status"], "paused");
        }
        assert_eq!(wait_state(&orch).await, "pending");
        assert_eq!(turn_state_of(&orch, "pred-turn").await, "yielded");
        assert!(matches!(
            crate::execution::jobs::admit_codex_goal_turn(
                &orch,
                orch.db.local.clone(),
                "run-1",
                "session-1",
                "job-1",
                "provider-raced-goal",
                Some("pred-turn"),
            )
            .unwrap(),
            crate::execution::jobs::CodexGoalTurnAdmission::Deferred { .. }
        ));
        assert_eq!(
            orch.db
                .local
                .query_one(
                    "SELECT COUNT(*) FROM turns WHERE job_id='job-1'",
                    (),
                    |row| { row.i64(0) }
                )
                .await
                .unwrap(),
            1,
            "a raced provider goal must not mint a Cairn successor"
        );

        assert!(handoff.parked().await);
        {
            let sent = requests.lock().unwrap();
            assert_eq!(sent.len(), 5);
            assert_eq!(sent[2]["method"], "thread/goal/get");
            assert_eq!(sent[3]["method"], "thread/goal/set");
            assert_eq!(sent[3]["params"]["status"], "paused");
            assert_eq!(sent[4]["method"], "turn/interrupt");
            assert!(sent.iter().all(|request| request["method"] != "turn/start"));
        }

        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();
        let results = tool_results(&orch).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.as_deref(), Some("pred-turn"));
        assert!(results[0].1.contains("tool-1"));
        assert!(results[0].1.contains("elapsed"));
        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1);
        assert_eq!(successors[0].1, "running");
        assert_eq!(
            orch.db
                .local
                .query_one(
                    "SELECT successor_turn_id FROM agent_waits WHERE id='wait-1'",
                    (),
                    |row| row.text(0),
                )
                .await
                .unwrap(),
            successors[0].0,
            "the resolver owns the WaitResolved successor"
        );
    }

    #[tokio::test]
    async fn restart_reconciliation_reacquires_and_completes_a_pending_mcp_task() {
        let (orch, mut record, _) = durable_env().await;
        register_warm(&orch);
        let gateway = Arc::new(FakeMcpGateway::default());
        gateway
            .task_outcomes
            .lock()
            .unwrap()
            .push_back(Ok(McpTaskOutcome::Complete(McpToolCallResult::from_text(
                "reacquired task",
            ))));
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut state = mcp_state();
        super::super::mcp_continuation::set_task(
            &mut state,
            "task-restart".into(),
            1,
            chrono::Utc::now().timestamp_millis() - 1_000,
            Some(30_000),
        );
        record.condition = Condition::McpContinuation { state };
        record.deadline = None;
        insert(&orch.db.local, &record).await.unwrap();

        reconcile(&orch).await;
        wait_for_reconciled_resolution(&orch).await;

        assert_eq!(
            gateway.task_polls.lock().unwrap().as_slice(),
            &["task-restart"]
        );
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
    }

    #[tokio::test]
    async fn duplicate_answer_and_poll_wakes_emit_one_update_result_and_successor() {
        let (orch, mut record, _) = durable_env().await;
        register_warm(&orch);
        let gateway = Arc::new(FakeMcpGateway::default());
        gateway
            .task_outcomes
            .lock()
            .unwrap()
            .push_back(Ok(McpTaskOutcome::Complete(McpToolCallResult::from_text(
                "race complete",
            ))));
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut state = mcp_state();
        super::super::mcp_continuation::set_task(
            &mut state,
            "task-race".into(),
            1,
            chrono::Utc::now().timestamp_millis() - 1_000,
            Some(30_000),
        );
        state.pending_input_requests = Some(serde_json::json!([{"id":"choice"}]));
        state.task_input_pending = true;
        record.condition = Condition::McpContinuation { state };
        record.deadline = None;
        insert(&orch.db.local, &record).await.unwrap();
        leave_prompt_pending_after_host_loss(&orch, &record).await;
        answer_latest_prompt(&orch, serde_json::json!({"choice":"a"})).await;

        tokio::join!(reconcile(&orch), reconcile(&orch));
        wait_for_reconciled_resolution(&orch).await;

        let (update_count, updated_task) = {
            let updates = gateway.updates.lock().unwrap();
            (updates.len(), updates[0].0.clone())
        };
        assert_eq!(update_count, 1);
        assert_eq!(updated_task, "task-race");
        assert_eq!(
            gateway.task_polls.lock().unwrap().as_slice(),
            &["task-race"]
        );
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
    }

    #[tokio::test]
    async fn one_shot_drive_returns_while_prompt_or_future_poll_is_pending() {
        let (orch, mut record, _) = durable_env().await;
        let gateway = Arc::new(FakeMcpGateway::default());
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut prompt_state = mcp_state();
        prompt_state.pending_input_requests = Some(serde_json::json!([{"id":"approval"}]));
        record.condition = Condition::McpContinuation {
            state: prompt_state,
        };
        insert(&orch.db.local, &record).await.unwrap();

        assert_eq!(
            super::super::mcp_continuation::drive(
                orch.clone(),
                orch.db.local.clone(),
                record.clone()
            )
            .await,
            super::super::mcp_continuation::DriveOutcome::Pending
        );
        assert_eq!(
            super::super::mcp_continuation::drive(
                orch.clone(),
                orch.db.local.clone(),
                record.clone()
            )
            .await,
            super::super::mcp_continuation::DriveOutcome::Pending
        );

        let mut future = mcp_state();
        super::super::mcp_continuation::set_task(
            &mut future,
            "future-task".into(),
            60_000,
            chrono::Utc::now().timestamp_millis(),
            Some(120_000),
        );
        super::super::mcp_continuation::store(&orch.db.local, &record.id, &future)
            .await
            .unwrap();
        assert_eq!(
            super::super::mcp_continuation::drive(orch.clone(), orch.db.local.clone(), record)
                .await,
            super::super::mcp_continuation::DriveOutcome::Pending
        );
        assert!(gateway.task_polls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn mcp_input_prompt_consumes_one_answer_and_completes_the_original_call_once() {
        let (orch, mut record, _) = durable_env().await;
        register_warm(&orch);
        let gateway = Arc::new(FakeMcpGateway::default());
        gateway
            .call_outcomes
            .lock()
            .unwrap()
            .push_back(McpCallOutcome::Complete(McpToolCallResult::from_text(
                "finished",
            )));
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut state = mcp_state();
        state.pending_input_requests = Some(serde_json::json!([{"id":"approval"}]));
        state.request_state = Some("round-1".into());
        record.condition = Condition::McpContinuation { state };
        record.deadline = None;
        insert(&orch.db.local, &record).await.unwrap();

        let driver = tokio::spawn(drive_mcp_until_terminal(
            orch.clone(),
            orch.db.local.clone(),
            record.clone(),
        ));
        let prompt_id = answer_latest_prompt(&orch, serde_json::json!({"approval":true})).await;
        let result = driver.await.unwrap();
        assert!(result.contains("finished"), "{result}");
        let prompt = orch
            .db
            .local
            .query_one(
                "SELECT mcp_wait_id,turn_id FROM prompts WHERE id=?1",
                (prompt_id,),
                |r| Ok((r.text(0)?, r.opt_text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            prompt,
            ("wait-1".into(), None),
            "prompt must be visibly owned by the MCP wait, not an independent turn"
        );
        assert_eq!(
            gateway.calls.lock().unwrap()[0].0,
            Some(serde_json::json!({"approval":true}))
        );

        resolve(&orch, &orch.db.local, &record, &result, false)
            .await
            .unwrap();
        assert_eq!(tool_result_count(&orch).await, 1);
        let correlation = orch.db.local.query_one("SELECT run_id,json_extract(data,'$.toolUseId') FROM events WHERE event_type='tool_result'", (), |r| Ok((r.text(0)?, r.text(1)?))).await.unwrap();
        assert_eq!(correlation, ("run-1".into(), "tool-1".into()));
    }

    #[tokio::test]
    async fn persisted_continue_tool_replays_same_operation_after_dispatch_crash() {
        use super::super::mcp_continuation::{load, PendingOperation};
        let (orch, mut record, _) = durable_env().await;
        let gateway = Arc::new(FakeMcpGateway::default());
        gateway
            .fail_call_after_dispatch
            .store(true, Ordering::SeqCst);
        gateway
            .call_outcomes
            .lock()
            .unwrap()
            .push_back(McpCallOutcome::Complete(McpToolCallResult::from_text(
                "replayed",
            )));
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut state = mcp_state();
        state.pending_operation = Some(PendingOperation::ContinueTool {
            operation_id: "stable-continue".into(),
            input_responses: serde_json::json!({"approval": true}),
            request_state: Some("round-1".into()),
        });
        record.condition = Condition::McpContinuation { state };
        insert(&orch.db.local, &record).await.unwrap();

        let first =
            drive_mcp_until_terminal(orch.clone(), orch.db.local.clone(), record.clone()).await;
        assert!(first.contains("injected crash"), "{first}");
        assert!(load(&orch.db.local, &record.id)
            .await
            .unwrap()
            .unwrap()
            .pending_operation
            .is_some());
        let db = orch.db.local.clone();
        let second = drive_mcp_until_terminal(orch, db, record).await;
        assert!(second.contains("replayed"), "{second}");
        let calls = gateway.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].2.as_deref(), Some("stable-continue"));
        assert_eq!(calls[1].2, calls[0].2);
    }

    #[tokio::test]
    async fn persisted_update_task_replays_same_operation_after_dispatch_crash() {
        use super::super::mcp_continuation::{load, PendingOperation};
        let (orch, mut record, _) = durable_env().await;
        let gateway = Arc::new(FakeMcpGateway::default());
        gateway
            .fail_update_after_dispatch
            .store(true, Ordering::SeqCst);
        gateway
            .task_outcomes
            .lock()
            .unwrap()
            .push_back(Ok(McpTaskOutcome::Complete(McpToolCallResult::from_text(
                "task replayed",
            ))));
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut state = mcp_state();
        super::super::mcp_continuation::set_task(
            &mut state,
            "task-1".into(),
            1,
            chrono::Utc::now().timestamp_millis() - 100,
            Some(30_000),
        );
        state.task_input_pending = true;
        state.pending_operation = Some(PendingOperation::UpdateTask {
            operation_id: "stable-update".into(),
            task_id: "task-1".into(),
            input_responses: serde_json::json!({"choice": "a"}),
        });
        record.condition = Condition::McpContinuation { state };
        insert(&orch.db.local, &record).await.unwrap();

        let first =
            drive_mcp_until_terminal(orch.clone(), orch.db.local.clone(), record.clone()).await;
        assert!(first.contains("injected crash"), "{first}");
        assert!(load(&orch.db.local, &record.id)
            .await
            .unwrap()
            .unwrap()
            .pending_operation
            .is_some());
        let db = orch.db.local.clone();
        let second = drive_mcp_until_terminal(orch, db, record).await;
        assert!(second.contains("task replayed"), "{second}");
        let updates = gateway.updates.lock().unwrap();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].2.as_deref(), Some("stable-update"));
        assert_eq!(updates[1].2, updates[0].2);
    }

    #[tokio::test]
    async fn mcp_task_failed_cancelled_lost_and_ttl_are_terminal_errors() {
        for (outcome, expected) in [
            (
                Some(Ok(McpTaskOutcome::Failed {
                    message: "broken".into(),
                })),
                "MCP task failed: broken",
            ),
            (
                Some(Ok(McpTaskOutcome::Cancelled)),
                "MCP task was cancelled",
            ),
            (Some(Err("task was lost".into())), "task was lost"),
            (None, "MCP task TTL expired"),
        ] {
            let (orch, mut record, _) = durable_env().await;
            let gateway = Arc::new(FakeMcpGateway::default());
            if let Some(outcome) = outcome {
                gateway.task_outcomes.lock().unwrap().push_back(outcome);
            }
            assert!(orch.set_mcp_gateway(gateway).is_ok());
            let mut state = mcp_state();
            super::super::mcp_continuation::set_task(
                &mut state,
                "task-1".into(),
                1,
                chrono::Utc::now().timestamp_millis() - 100,
                Some(30_000),
            );
            if expected.contains("TTL") {
                state.deadline_ms = Some(chrono::Utc::now().timestamp_millis() - 1);
            }
            record.condition = Condition::McpContinuation { state };
            record.deadline = None;
            insert(&orch.db.local, &record).await.unwrap();
            let db = orch.db.local.clone();
            let result = drive_mcp_until_terminal(orch, db, record).await;
            assert!(result.contains(expected), "expected {expected}: {result}");
        }
    }

    /// Register an in-memory warm process for `run-1`/`session-1` so
    /// `continue_job_impl` reuses it instead of spawning a real CLI.
    fn register_warm(orch: &Orchestrator) {
        register_warm_for(orch, "run-1", "session-1");
    }

    fn register_warm_for(orch: &Orchestrator, run_id: &str, session_id: &str) {
        let mut processes = orch.process_state.processes.lock().unwrap();
        let child = Arc::new(std::sync::Mutex::new(None));
        let stdin = Arc::new(std::sync::Mutex::new(Some(
            cairn_models::session::claude::ClaudeControl::boxed(Box::new(Vec::<u8>::new())),
        )));
        let mut handle = crate::agent_process::process::RunHandle::new(
            child,
            stdin,
            Some(session_id.to_string()),
            Some("job-1".to_string()),
        );
        handle.transition_to_warm();
        processes.register(run_id.to_string(), handle);
    }

    async fn wait_state(orch: &Orchestrator) -> String {
        orch.db
            .local
            .query_one("SELECT state FROM agent_waits WHERE id='wait-1'", (), |r| {
                r.text(0)
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn native_goal_turn_waits_for_durable_suspension_successor() {
        let (orch, record, _) = durable_env().await;
        let error = crate::execution::jobs::admit_codex_goal_turn(
            &orch,
            orch.db.local.clone(),
            "run-1",
            "session-1",
            "job-1",
            "provider-without-wait",
            None,
        )
        .expect_err("a running head without a durable wait is an invariant failure");
        assert!(
            error.contains("without durable suspension ownership"),
            "{error}"
        );

        insert(&orch.db.local, &record).await.unwrap();

        for provider_turn_id in ["provider-parked", "provider-parked"] {
            assert_eq!(
                crate::execution::jobs::admit_codex_goal_turn(
                    &orch,
                    orch.db.local.clone(),
                    "run-1",
                    "session-1",
                    "job-1",
                    provider_turn_id,
                    None,
                )
                .unwrap(),
                crate::execution::jobs::CodexGoalTurnAdmission::Deferred {
                    predecessor_turn_id: "pred-turn".to_string()
                }
            );
        }
        assert!(wait_resolved_turns(&orch).await.is_empty());

        register_warm(&orch);
        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();
        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1);
        assert_eq!(successors[0].1, "running");

        for _ in 0..2 {
            assert_eq!(
                crate::execution::jobs::admit_codex_goal_turn(
                    &orch,
                    orch.db.local.clone(),
                    "run-1",
                    "session-1",
                    "job-1",
                    "provider-resumed",
                    Some(&successors[0].0),
                )
                .unwrap(),
                crate::execution::jobs::CodexGoalTurnAdmission::Admitted(successors[0].0.clone())
            );
        }
        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
    }

    async fn wait_resolved_turns(orch: &Orchestrator) -> Vec<(String, String)> {
        orch.db
            .local
            .read(|c| {
                Box::pin(async move {
                    let mut rows = c
                        .query(
                            "SELECT id,state FROM turns WHERE start_reason='wait_resolved' ORDER BY sequence",
                            (),
                        )
                        .await?;
                    let mut v = Vec::new();
                    while let Some(r) = rows.next().await? {
                        v.push((r.text(0)?, r.text(1)?));
                    }
                    Ok(v)
                })
            })
            .await
            .unwrap()
    }

    async fn tool_result_count(orch: &Orchestrator) -> i64 {
        orch.db
            .local
            .query_one(
                "SELECT COUNT(*) FROM events WHERE event_type='tool_result'",
                (),
                |r| r.i64(0),
            )
            .await
            .unwrap()
    }

    async fn turn_state_of(orch: &Orchestrator, id: &str) -> String {
        let id = id.to_string();
        orch.db
            .local
            .query_one("SELECT state FROM turns WHERE id=?1", (id,), |r| r.text(0))
            .await
            .unwrap()
    }

    const ELAPSED: &str = r#"{"outcome":"elapsed","elapsedMs":1,"deadlineMs":1}"#;
    const EXITED: &str = r#"{"outcome":"exited","terminal":"t","exitCode":0}"#;

    #[tokio::test]
    async fn durable_duration_resolves_once_with_single_successor_and_resumes_warm() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();

        // Exactly one WaitResolved successor, and it is running (started on resume).
        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1, "exactly one WaitResolved successor");
        assert_eq!(successors[0].1, "running", "successor started on resume");
        // The synthetic result was delivered once; the predecessor stays yielded.
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(turn_state_of(&orch, "pred-turn").await, "yielded");
        // The wait row reached resolved.
        assert_eq!(wait_state(&orch).await, "resolved");
        // The warm process was resumed onto the successor, not left parked.
        assert_eq!(
            orch.process_state.get_current_turn_id("run-1").as_deref(),
            Some(successors[0].0.as_str()),
            "warm process resumed onto the successor turn"
        );
    }

    /// A transient failure delivering the result, with the host still up, must
    /// not strand the parked call until the next boot (cairn/5778). The live
    /// resolver retries from the row's durable state, so the call gets its
    /// answer and its turn resumes without a restart.
    #[tokio::test]
    async fn a_failed_result_append_is_retried_without_a_restart() {
        let (orch, base, _) = durable_env().await;
        let record = Record {
            id: "wait-append-flake".into(),
            ..base
        };
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);
        arm_result_append_failures(&record.id, 1);

        drive_resolution(&orch, &orch.db.local, &record, ELAPSED, false).await;

        assert_eq!(
            tool_results(&orch).await.len(),
            1,
            "the call is answered once, without a restart"
        );
        assert_eq!(wait_row(&orch, &record.id).await.0, "resolved");
        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1, "exactly one WaitResolved successor");
        assert_eq!(successors[0].1, "running", "the turn resumed");
        assert!(
            live_resolvers(&orch).is_empty(),
            "the guard is released once the resolution lands"
        );
    }

    /// A failure that can never succeed -- here the node's job finished while
    /// its call was parked -- settles the row instead of retrying it forever.
    #[tokio::test]
    async fn a_failing_resolution_for_a_finished_job_settles_instead_of_retrying() {
        let (orch, base, _) = durable_env().await;
        let record = Record {
            id: "wait-finished-job".into(),
            ..base
        };
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);
        orch.db
            .local
            .execute("UPDATE jobs SET status='complete' WHERE id='job-1'", ())
            .await
            .unwrap();
        arm_result_append_failures(&record.id, 50);

        drive_resolution(&orch, &orch.db.local, &record, ELAPSED, false).await;

        let (state, resolution, _) = wait_row(&orch, &record.id).await;
        assert_eq!(state, "cancelled");
        let resolution = resolution.unwrap();
        assert!(resolution.contains("job is complete"), "{resolution}");
        assert!(
            resolution.contains("elapsedMs"),
            "the stored result is kept: {resolution}"
        );
        assert_eq!(
            remaining_result_append_failures(&record.id),
            49,
            "settled after one failure, not retried"
        );
        assert!(wait_resolved_turns(&orch).await.is_empty());
    }

    /// Poll until the wait row reaches `state`, failing loudly if it never does.
    async fn await_wait_state(orch: &Orchestrator, id: &str, state: &str) {
        for _ in 0..400 {
            if wait_row(orch, id).await.0 == state {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!(
            "wait {id} never reached {state}; it is {}",
            wait_row(orch, id).await.0
        );
    }

    /// Move a row to `resolving` with `resolution`, entered `age` ago.
    async fn set_resolving(orch: &Orchestrator, id: &str, resolution: &str, age: Duration) {
        let at = chrono::Utc::now().timestamp_millis() - age.as_millis() as i64;
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving',resolution_json=?2,resolving_at=?3 WHERE id=?1",
                (id.to_string(), resolution.to_string(), at),
            )
            .await
            .unwrap();
    }

    const STALE: Duration = Duration::from_secs(180);

    /// The periodic backstop: a row left `resolving` with its result stored and
    /// no live resolver is re-driven without a restart, once it is old enough.
    #[tokio::test]
    async fn the_sweep_redrives_a_stale_resolving_row_with_no_live_resolver() {
        let (orch, base, _) = durable_env().await;
        let record = Record {
            id: "wait-sweep-stale".into(),
            ..base
        };
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        set_resolving(&orch, &record.id, ELAPSED, Duration::ZERO).await;
        assert_eq!(
            sweep_stuck_resolutions(&orch).await,
            SweepOutcome::default(),
            "a row that only just entered resolving is not stuck yet"
        );

        set_resolving(&orch, &record.id, ELAPSED, STALE).await;
        assert_eq!(
            sweep_stuck_resolutions(&orch).await,
            SweepOutcome {
                redriven: 1,
                settled: 0
            }
        );
        await_wait_state(&orch, &record.id, "resolved").await;

        assert_eq!(tool_results(&orch).await.len(), 1);
        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1);
        assert_eq!(successors[0].1, "running");
    }

    /// A resolver legitimately waiting in its collision backoff holds its guard,
    /// so the sweep leaves it alone however old its row is, and the call is
    /// answered and resolved exactly once when the foreign successor starts.
    #[tokio::test]
    async fn the_sweep_does_not_double_drive_a_resolver_in_collision_backoff() {
        let (orch, base, _) = durable_env().await;
        let record = Record {
            id: "wait-sweep-collision".into(),
            ..base
        };
        insert(&orch.db.local, &record).await.unwrap();
        insert_foreign_successor(&orch, "pending").await;
        register_warm(&orch);

        let driver = tokio::spawn({
            let (orch, record) = (orch.clone(), record.clone());
            async move {
                drive_resolution(&orch, &orch.db.local, &record, ELAPSED, false).await;
            }
        });
        // In the collision loop: the guard is held and the call already answered.
        for _ in 0..400 {
            if live_resolvers(&orch).contains(&record.id) && tool_result_count(&orch).await == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(live_resolvers(&orch).contains(&record.id));
        assert_eq!(wait_row(&orch, &record.id).await.0, "resolving");

        set_resolving(&orch, &record.id, ELAPSED, STALE).await;
        assert_eq!(
            sweep_stuck_resolutions(&orch).await,
            SweepOutcome::default(),
            "the sweep must not re-drive a row its live resolver holds"
        );

        orch.db
            .local
            .execute("UPDATE turns SET state='running' WHERE id='foreign'", ())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), driver)
            .await
            .expect("the live resolver finishes once the foreign successor starts")
            .unwrap();

        assert_eq!(tool_results(&orch).await.len(), 1);
        assert!(wait_resolved_turns(&orch).await.is_empty());
        assert_eq!(wait_row(&orch, &record.id).await.0, "resolved");
        assert!(live_resolvers(&orch).is_empty());
    }

    /// A failed release after a failed append orphans a delivery claim while the
    /// host is up. The sweep's bounded reclaim gives back an old claim with no
    /// event behind it, and leaves a fresh one -- possibly a live append -- alone.
    #[tokio::test]
    async fn the_sweep_releases_a_stale_claim_with_no_event_but_not_a_fresh_one() {
        let (orch, base, _) = durable_env().await;
        let record = Record {
            id: "wait-sweep-claim".into(),
            ..base
        };
        insert(&orch.db.local, &record).await.unwrap();
        let call = SuspendedCall::of(&record);
        assert!(claim_result_delivery(&orch.db.local, &record.id)
            .await
            .unwrap());

        reclaim_interrupted_delivery(&orch.db.local, &call, Some(STUCK_AFTER)).await;
        assert!(
            wait_row(&orch, &record.id).await.2.is_some(),
            "a fresh claim may be a live append"
        );

        let old = chrono::Utc::now().timestamp_millis() - STALE.as_millis() as i64;
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET result_stored_at=?2 WHERE id=?1",
                (record.id.clone(), old),
            )
            .await
            .unwrap();
        reclaim_interrupted_delivery(&orch.db.local, &call, Some(STUCK_AFTER)).await;
        assert_eq!(
            wait_row(&orch, &record.id).await.2,
            None,
            "an old claim with nothing behind it is released"
        );
    }

    /// A permanent refusal must settle the row, not be re-driven every pass: a
    /// stuck row whose job finished is settled by one sweep and absent from the
    /// next.
    #[tokio::test]
    async fn a_stuck_row_whose_job_finished_is_settled_by_one_sweep_and_not_redriven() {
        let (orch, base, _) = durable_env().await;
        let record = Record {
            id: "wait-sweep-finished".into(),
            ..base
        };
        insert(&orch.db.local, &record).await.unwrap();
        set_resolving(&orch, &record.id, ELAPSED, STALE).await;
        orch.db
            .local
            .execute("UPDATE jobs SET status='cancelled' WHERE id='job-1'", ())
            .await
            .unwrap();

        assert_eq!(
            sweep_stuck_resolutions(&orch).await,
            SweepOutcome {
                redriven: 0,
                settled: 1
            }
        );
        let (state, resolution, _) = wait_row(&orch, &record.id).await;
        assert_eq!(state, "cancelled");
        assert!(resolution.unwrap().contains("job is cancelled"));
        assert_eq!(
            sweep_stuck_resolutions(&orch).await,
            SweepOutcome::default(),
            "a settled row is not picked up again"
        );
        assert!(tool_results(&orch).await.is_empty());
        assert!(wait_resolved_turns(&orch).await.is_empty());
    }

    /// Visibility uses the sweep's bound, and tells a failing delivery or resume
    /// from a live resolver merely waiting on another continuation.
    #[tokio::test]
    async fn stuck_resolutions_tell_a_failure_from_a_guarded_collision_wait() {
        let (orch, base, _) = durable_env().await;
        let record = Record {
            id: "wait-stuck-phases".into(),
            ..base
        };
        insert(&orch.db.local, &record).await.unwrap();
        let jobs = ["job-1".to_string()];
        let now = || chrono::Utc::now().timestamp_millis();
        let none = HashSet::new();

        set_resolving(&orch, &record.id, ELAPSED, Duration::ZERO).await;
        assert!(stuck_resolutions(&orch.db.local, &jobs, now(), &none)
            .await
            .unwrap()
            .is_empty());

        set_resolving(&orch, &record.id, ELAPSED, STALE).await;
        let stuck = stuck_resolutions(&orch.db.local, &jobs, now(), &none)
            .await
            .unwrap();
        assert_eq!(stuck.len(), 1);
        assert_eq!(stuck[0].wait_id, record.id);
        assert_eq!(stuck[0].label, "waitFor: duration");
        assert_eq!(stuck[0].phase, StuckPhase::Delivering);

        store_result_once(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();
        let stuck = stuck_resolutions(&orch.db.local, &jobs, now(), &none)
            .await
            .unwrap();
        assert_eq!(stuck[0].phase, StuckPhase::Resuming);

        let _guard = claim_resolver(&orch, &record.id).unwrap();
        let stuck = stuck_resolutions(&orch.db.local, &jobs, now(), &live_resolvers(&orch))
            .await
            .unwrap();
        assert_eq!(stuck[0].phase, StuckPhase::AwaitingContinuation);
    }

    /// The MCP scheduler only reads `pending` rows, so a `resolving` MCP
    /// continuation was skipped at startup and stranded across every restart.
    #[tokio::test]
    async fn startup_reconcile_redrives_a_resolving_mcp_continuation_row() {
        let (orch, base, _) = durable_env().await;
        let record = Record {
            id: "wait-mcp-resolving".into(),
            condition: Condition::McpContinuation { state: mcp_state() },
            deadline: None,
            ..base
        };
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);
        set_resolving(&orch, &record.id, "mcp tool finished", Duration::ZERO).await;

        reconcile(&orch).await;
        await_wait_state(&orch, &record.id, "resolved").await;

        let results = tool_results(&orch).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].1.contains("mcp tool finished"), "{results:?}");
        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
    }

    #[tokio::test]
    async fn duplicate_resolution_creates_no_second_successor_or_result() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();
        // A duplicate live delivery on the already-resolved row is a clean no-op.
        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();

        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_state(&orch).await, "resolved");
    }

    #[tokio::test]
    async fn resolving_replay_reuses_persisted_successor_without_second_continuation() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();
        let successor_id = wait_resolved_turns(&orch).await[0].0.clone();

        // Simulate a crash after the continuation but before the resolved CAS: the
        // row is stuck at `resolving` with its successor persisted, and startup
        // replay re-drives it.
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving' WHERE id='wait-1'",
                (),
            )
            .await
            .unwrap();

        resolve(&orch, &orch.db.local, &record, ELAPSED, true)
            .await
            .unwrap();

        // No second successor, no second tool result — the already-running
        // successor short-circuits the continuation.
        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1);
        assert_eq!(successors[0].0, successor_id);
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_state(&orch).await, "resolved");
    }

    #[tokio::test]
    async fn terminal_condition_durable_path_resolves_and_resumes() {
        let (orch, mut record, _) = durable_env().await;
        record.condition = Condition::Terminal {
            uri: "cairn://p/prj/1/1/builder/terminal/tests".into(),
            slug: "tests".into(),
            owner: None,
            on: TerminalWaitEvent::Exit,
            phrase: None,
        };
        record.deadline = None;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        let exited = r#"{"outcome":"exited","terminal":"t","exitCode":0}"#;
        resolve(&orch, &orch.db.local, &record, exited, false)
            .await
            .unwrap();

        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1);
        assert_eq!(successors[0].1, "running");
        assert_eq!(wait_state(&orch).await, "resolved");
        assert_eq!(
            orch.process_state.get_current_turn_id("run-1").as_deref(),
            Some(successors[0].0.as_str())
        );
    }

    /// Insert a foreign (non-wait_resolved) successor of the predecessor in the
    /// given state, as if a racing continuation had claimed the predecessor's
    /// single successor mid-wait.
    async fn insert_foreign_successor(orch: &Orchestrator, state: &str) {
        let state = state.to_string();
        orch.db
            .local
            .execute(
                "INSERT INTO turns (id,session_id,run_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at) VALUES ('foreign','session-1','run-1','job-1',2,'pred-turn',?1,'follow_up',2,2)",
                (state,),
            )
            .await
            .unwrap();
        orch.db
            .local
            .execute(
                "UPDATE jobs SET current_turn_id='foreign' WHERE id='job-1'",
                (),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn collision_with_running_continuation_records_result_and_resolves() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        // A continuation already RESUMED the run through the predecessor's successor.
        insert_foreign_successor(&orch, "running").await;
        register_warm(&orch);

        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();

        // The run has resumed exactly once (via the foreign turn); the wait records
        // its result and resolves without creating or hijacking a turn.
        assert!(
            wait_resolved_turns(&orch).await.is_empty(),
            "no WaitResolved turn created on collision"
        );
        assert_eq!(
            turn_state_of(&orch, "foreign").await,
            "running",
            "foreign successor is left untouched"
        );
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_state(&orch).await, "resolved");
        let successor: Option<String> = orch
            .db
            .local
            .query_one(
                "SELECT successor_turn_id FROM agent_waits WHERE id='wait-1'",
                (),
                |r| r.opt_text(0),
            )
            .await
            .unwrap();
        assert_eq!(
            successor, None,
            "foreign turn is not recorded as the successor"
        );
    }

    #[tokio::test]
    async fn collision_resolves_in_process_once_foreign_successor_starts() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        // The racing continuation has created the successor but not started it yet.
        insert_foreign_successor(&orch, "pending").await;
        register_warm(&orch);

        // It starts the foreign successor shortly after the resolver first observes
        // it pending — as a warm reuse would, within the poll budget.
        let db = orch.db.local.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            db.execute("UPDATE turns SET state='running' WHERE id='foreign'", ())
                .await
                .unwrap();
        });

        // The SAME live resolver — no restart — polls the transition and resolves.
        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();

        assert!(wait_resolved_turns(&orch).await.is_empty());
        assert_eq!(turn_state_of(&orch, "foreign").await, "running");
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(
            wait_state(&orch).await,
            "resolved",
            "in-process poll resolves the wait once the foreign turn starts"
        );
    }

    #[tokio::test]
    async fn collision_polls_while_unstarted_then_reconciliation_resolves_after_restart() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        // A racing continuation created the predecessor's successor but has NOT
        // started it — the run is still parked, so the wait must not falsely resolve.
        insert_foreign_successor(&orch, "pending").await;
        register_warm(&orch);

        // While the foreign successor stays pending, the resolver keeps polling and
        // never completes (no false resolve, no arbitrary give-up cutoff). Dropping
        // the future when the timeout fires models the resolver task being cancelled
        // by host shutdown.
        let polling = tokio::time::timeout(
            Duration::from_millis(250),
            resolve(&orch, &orch.db.local, &record, ELAPSED, false),
        )
        .await;
        assert!(
            polling.is_err(),
            "resolver keeps waiting on an unstarted foreign successor instead of resolving or giving up"
        );
        assert_eq!(
            wait_state(&orch).await,
            "resolving",
            "wait stays resolving (durable ownership) while the run has not resumed"
        );
        assert!(wait_resolved_turns(&orch).await.is_empty());
        // The call itself IS answered: delivery is per-call and happens before
        // anything turn-shaped, so a resolver still waiting on a foreign
        // successor has already discharged its own obligation.
        assert_eq!(tool_result_count(&orch).await, 1);

        // After restart, the racing continuation has started the foreign turn; startup
        // reconciliation re-drives the still-`resolving` row and resolves it.
        orch.db
            .local
            .execute("UPDATE turns SET state='running' WHERE id='foreign'", ())
            .await
            .unwrap();
        resolve(&orch, &orch.db.local, &record, ELAPSED, true)
            .await
            .unwrap();

        assert!(wait_resolved_turns(&orch).await.is_empty());
        assert_eq!(turn_state_of(&orch, "foreign").await, "running");
        assert_eq!(tool_result_count(&orch).await, 1);
        assert_eq!(wait_state(&orch).await, "resolved");
    }

    // ---- Abandoned rows (CAIRN-3159) ---------------------------------------

    /// Rotate the job onto a fresh session, run, and turn, exactly as an
    /// inactivity reseed or a host restart does, leaving whatever wait row the
    /// previous turn established behind.
    async fn reconstruct_session(orch: &Orchestrator) {
        for sql in [
            "INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-2','job-1','open','handle-2',2,2)",
            "UPDATE sessions SET status='closed', replaced_by_id='session-2' WHERE id='session-1'",
            "INSERT INTO runs (id,issue_id,project_id,job_id,status,session_id,created_at,updated_at) VALUES ('run-2','i','p','job-1','live','session-2',2,2)",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('new-turn','session-2','run-2','job-1',1,'running','initial',2,2)",
            "UPDATE jobs SET current_session_id='session-2', current_turn_id='new-turn' WHERE id='job-1'",
        ] {
            orch.db.local.execute(sql, ()).await.unwrap();
        }
    }

    /// The suspension the reconstructed session's turn attempts.
    fn successor_record(base: &Record) -> Record {
        Record {
            id: "wait-2".into(),
            session_id: "session-2".into(),
            run_id: "run-2".into(),
            turn_id: "new-turn".into(),
            tool_use_id: "tool-2".into(),
            ..base.clone()
        }
    }

    /// state, resolution, and whether a result was delivered, for one wait row.
    async fn wait_row(orch: &Orchestrator, id: &str) -> (String, Option<String>, Option<i64>) {
        let id = id.to_string();
        orch.db
            .local
            .query_one(
                "SELECT state,resolution_json,result_stored_at FROM agent_waits WHERE id=?1",
                (id,),
                |r| Ok((r.text(0)?, r.opt_text(1)?, r.opt_i64(2)?)),
            )
            .await
            .unwrap()
    }

    /// Every delivered tool result as (turn_id, data).
    async fn tool_results(orch: &Orchestrator) -> Vec<(Option<String>, String)> {
        orch.db
            .local
            .read(|c| {
                Box::pin(async move {
                    let mut rows = c
                        .query(
                            "SELECT turn_id,data FROM events WHERE event_type='tool_result' ORDER BY sequence",
                            (),
                        )
                        .await?;
                    let mut v = Vec::new();
                    while let Some(r) = rows.next().await? {
                        v.push((r.opt_text(0)?, r.text(1)?));
                    }
                    Ok(v)
                })
            })
            .await
            .unwrap()
    }

    async fn active_wait_count(orch: &Orchestrator) -> i64 {
        orch.db
            .local
            .query_one(
                "SELECT COUNT(*) FROM agent_waits WHERE state IN ('pending','resolving')",
                (),
                |r| r.i64(0),
            )
            .await
            .unwrap()
    }

    /// Reported from the field: "the first waitFor suspends and resumes
    /// correctly; every later one fails". A wait that completed on the happy path
    /// still strands the job if its row never leaves the active set — a resolution
    /// that resumed the run but returned before its final state write leaves
    /// exactly that. The resumed turn's own next wait must not be refused by it.
    ///
    /// This is the same supersession as a session reconstruction, and for the same
    /// reason: the row belongs to a turn the job has already left. Nothing about
    /// the fix depends on *why* the row was left behind.
    #[tokio::test]
    async fn a_completed_wait_whose_row_stayed_active_does_not_block_the_next_one() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);
        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();
        let successor = wait_resolved_turns(&orch).await[0].0.clone();
        // The wait resolved and the run resumed — but the row stayed active.
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving' WHERE id='wait-1'",
                (),
            )
            .await
            .unwrap();

        // The resumed turn issues its own wait.
        let next = Record {
            id: "wait-2".into(),
            turn_id: successor,
            tool_use_id: "tool-2".into(),
            ..record.clone()
        };
        suspend(&orch, &orch.db.local, &next).await.unwrap();

        assert_eq!(wait_row(&orch, "wait-2").await.0, "pending");
        assert_eq!(active_wait_count(&orch).await, 1);
        // The first wait's call keeps the answer it already got; superseding a
        // completed row must not answer it a second time.
        let results = tool_results(&orch).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].1.contains("elapsed"));
    }

    /// The load-bearing case: a wait was in flight when the session was
    /// reconstructed, so its row was left behind pending. The reconstructed
    /// session's very next suspension must succeed, and the abandoned row must
    /// be closed with a typed outcome delivered to the call it belonged to.
    #[tokio::test]
    async fn suspension_after_session_reconstruction_supersedes_the_abandoned_row() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        reconstruct_session(&orch).await;
        register_warm_for(&orch, "run-2", "session-2");

        suspend(&orch, &orch.db.local, &successor_record(&record))
            .await
            .unwrap();

        // The new suspension is the job's one live wait.
        assert_eq!(wait_row(&orch, "wait-2").await.0, "pending");
        assert_eq!(active_wait_count(&orch).await, 1);

        // The abandoned one is closed with a typed outcome...
        let (state, resolution, delivered) = wait_row(&orch, "wait-1").await;
        assert_eq!(state, "cancelled");
        assert!(
            resolution
                .as_deref()
                .unwrap_or_default()
                .contains("abandoned"),
            "unexpected abandonment resolution: {resolution:?}"
        );
        assert!(delivered.is_some(), "the abandoned call was answered");

        // ...delivered to its own turn, so its tool use does not dangle.
        let results = tool_results(&orch).await;
        assert_eq!(results.len(), 1, "exactly one delivered result");
        assert_eq!(results[0].0.as_deref(), Some("pred-turn"));
        assert!(results[0].1.contains("tool-1") && results[0].1.contains("abandoned"));

        // Superseding answers the abandoned call; it never resumes the job.
        assert!(wait_resolved_turns(&orch).await.is_empty());
    }

    /// cairn/6145 comment #8 (tenet 1421): a turn's run batch is held inline
    /// and still running when a LATER turn of the same session suspends on a
    /// duration. That suspension is no evidence about the batch, so its wait
    /// stays pending; and when the batch settles, its result reaches the job
    /// as Cairn's notice instead of resuming the turn the job has moved past.
    #[tokio::test]
    async fn a_live_held_run_batch_survives_a_later_suspension_and_reaches_the_job() {
        let (orch, base, _) = durable_env().await;
        let held = run_batch_record(&base, false);
        insert_as(&orch.db.local, &held, Establish::Hold)
            .await
            .unwrap();
        let live = super::super::run::detached::LiveBatch::claim_for_test("request-1");
        for sql in [
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('later-turn','session-1','run-1','job-1',2,'running','initial',2,2)",
            "UPDATE jobs SET current_turn_id='later-turn' WHERE id='job-1'",
        ] {
            orch.db.local.execute(sql, ()).await.unwrap();
        }
        register_warm(&orch);
        let later = Record {
            id: "wait-2".into(),
            turn_id: "later-turn".into(),
            tool_use_id: "tool-2".into(),
            ..ten_minute_wait(base.clone())
        };
        suspend(&orch, &orch.db.local, &later).await.unwrap();
        assert_eq!(
            wait_row(&orch, "wait-1").await.0,
            "pending",
            "not abandoned"
        );
        assert_eq!(wait_row(&orch, "wait-2").await.0, "pending");

        drop(live);
        resolve(&orch, &orch.db.local, &held, "the batch's output", true)
            .await
            .unwrap();
        assert_eq!(wait_row(&orch, "wait-1").await.0, "resolved");
        let (state, output): (String, String) = orch
            .db
            .local
            .query_one(
                "SELECT state, output FROM run_batch_detached WHERE request_id='request-1'",
                (),
                |r| Ok((r.text(0)?, r.text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(state, "settled");
        assert_eq!(output, "the batch's output");
        let notices = crate::messages::side_channel::peek_pending_side_channel_for_job_async(
            &orch.db.local,
            "job-1",
        )
        .await
        .unwrap();
        assert_eq!(notices.len(), 1);
        assert!(
            notices[0]
                .render()
                .starts_with("Your run batch 'unnamed' (cairn:~/batches/request-1) finished:"),
            "{}",
            notices[0].render()
        );
        assert!(
            wait_resolved_turns(&orch).await.is_empty(),
            "the turn the job moved past is not resumed"
        );
    }

    /// The same for a suspended `run` batch, whose row carries no pollable
    /// condition and so can only ever be closed by something else. Its batch
    /// is not running in this runner, which is the evidence that clears it.
    #[tokio::test]
    async fn abandoned_run_batch_row_does_not_block_the_reconstructed_session() {
        let (orch, base, _) = durable_env().await;
        insert(&orch.db.local, &run_batch_record(&base, true))
            .await
            .unwrap();
        reconstruct_session(&orch).await;
        register_warm_for(&orch, "run-2", "session-2");

        suspend(&orch, &orch.db.local, &successor_record(&base))
            .await
            .unwrap();

        assert_eq!(wait_row(&orch, "wait-1").await.0, "cancelled");
        assert_eq!(wait_row(&orch, "wait-2").await.0, "pending");
        assert_eq!(active_wait_count(&orch).await, 1);
    }

    /// Parking changes what the node IS -- self-suspended on its own work rather
    /// than working -- and nothing else says so: the wait row and the yield land
    /// inside one write, which announces nothing, and clients cache turn state
    /// until something invalidates it. The transcript is where that silence
    /// showed: a parked call has no verdict to draw and, told nothing, no turn it
    /// can call running either, so a live batch rendered as neither (CAIRN-3340).
    #[tokio::test]
    async fn parking_a_turn_announces_the_yield() {
        let (orch, record, emitter) = durable_env_capturing().await;
        register_warm(&orch);

        suspend(&orch, &orch.db.local, &run_batch_record(&record, false))
            .await
            .unwrap();

        assert_eq!(turn_state_of(&orch, "pred-turn").await, "yielded");
        let announced = turn_changes(&emitter);
        assert_eq!(
            announced.len(),
            1,
            "one park, one announcement: {announced:?}"
        );
        assert_eq!(announced[0]["action"], "update");
        // Clients invalidate by scope, so the job whose turn parked must ride along.
        assert_eq!(announced[0]["jobId"], "job-1");
    }

    /// A turn parks once however many of its calls suspend (CAIRN-2823), so the
    /// sibling that finds it already yielded has no state change to announce.
    #[tokio::test]
    async fn a_sibling_park_announces_nothing_new() {
        let (orch, record, emitter) = durable_env_capturing().await;
        register_warm(&orch);
        suspend(&orch, &orch.db.local, &record).await.unwrap();

        suspend(&orch, &orch.db.local, &sibling_record(&record))
            .await
            .unwrap();

        let announced = turn_changes(&emitter);
        assert_eq!(
            announced.len(),
            1,
            "one yield, one announcement: {announced:?}"
        );
    }

    // ---- Concurrent calls in one turn (CAIRN-3232) --------------------------

    /// A provider routinely emits several tool calls in one assistant event, and
    /// two of them can both outlive their grace window. Both must park: the
    /// bound is one active row per CALL, not per turn.
    #[tokio::test]
    async fn a_second_call_in_the_same_turn_parks_alongside_the_first() {
        let (orch, record, _) = durable_env().await;
        register_warm(&orch);
        suspend(&orch, &orch.db.local, &record).await.unwrap();

        suspend(&orch, &orch.db.local, &sibling_record(&record))
            .await
            .unwrap();

        assert_eq!(wait_row(&orch, "wait-1").await.0, "pending");
        assert_eq!(wait_row(&orch, "wait-2").await.0, "pending");
        assert_eq!(active_wait_count(&orch).await, 2);
        // Parking is not answering: neither call has been told anything yet.
        assert!(tool_results(&orch).await.is_empty());
    }

    /// What the per-call bound still refuses: one CALL parked twice. Nothing
    /// distinguishes the two rows, so the second would answer a call the first
    /// already owns. The refusal names the next step, never the constraint.
    #[tokio::test]
    async fn one_call_still_cannot_park_itself_twice() {
        let (orch, record, _) = durable_env().await;
        register_warm(&orch);
        suspend(&orch, &orch.db.local, &record).await.unwrap();

        let duplicate = Record {
            id: "wait-2".into(),
            ..record.clone()
        };
        // Matched rather than unwrapped: the success value is a park handoff, a
        // one-shot receiver that has no business implementing `Debug` just to
        // satisfy a test's error path.
        let error = match suspend(&orch, &orch.db.local, &duplicate).await {
            Ok(_) => panic!("one tool call must not be parked by two suspensions"),
            Err(error) => error,
        };

        assert_eq!(error, SUSPENSION_UNAVAILABLE);
        assert!(
            !error.contains("UNIQUE") && !error.contains("agent_waits"),
            "refusal leaks storage internals: {error}"
        );
        assert_eq!(wait_state(&orch).await, "pending");
        assert_eq!(active_wait_count(&orch).await, 1);
    }

    /// The per-call bound is only a real bound while every row names a real
    /// call, so a suspension with nothing bound is refused before it can insert
    /// one that names nothing.
    #[tokio::test]
    async fn a_suspension_with_no_bound_call_is_refused() {
        let (orch, record, _) = durable_env().await;
        register_warm(&orch);
        let unbound = Record {
            tool_use_id: String::new(),
            ..record.clone()
        };

        let error = match suspend(&orch, &orch.db.local, &unbound).await {
            Ok(_) => panic!("a suspension with no call to answer must not be established"),
            Err(error) => error,
        };

        assert_eq!(error, SUSPENSION_UNAVAILABLE);
        assert_eq!(active_wait_count(&orch).await, 0);
    }

    /// A turn that has already parked one call reports no live turn, because
    /// parking idles the process. A sibling call crossing its own grace window a
    /// moment later must still find the turn it came from, or it loses its
    /// suspension to nothing more than scheduling jitter.
    #[tokio::test]
    async fn a_parked_turn_is_still_findable_by_its_sibling_call() {
        let (orch, record, _) = durable_env().await;
        register_warm(&orch);

        // Nothing live and nothing parked: there is no turn to hand out, and a
        // callback must never be given one it cannot have come from.
        assert_eq!(
            suspending_turn_id(&orch, &orch.db.local, "run-1").await,
            None
        );

        // A live turn answers for itself.
        orch.process_state.begin_turn("run-1", "pred-turn");
        assert_eq!(
            suspending_turn_id(&orch, &orch.db.local, "run-1")
                .await
                .as_deref(),
            Some("pred-turn")
        );

        // Once one of its calls is parked the process reports no live turn — and
        // the turn is still findable through the row that call parked.
        insert(&orch.db.local, &record).await.unwrap();
        orch.process_state.transition_to_warm("run-1");
        assert_eq!(orch.process_state.get_current_turn_id("run-1"), None);
        assert_eq!(
            suspending_turn_id(&orch, &orch.db.local, "run-1")
                .await
                .as_deref(),
            Some("pred-turn")
        );
    }

    /// The whole point: two parked calls, each answered on its own provider id,
    /// and exactly ONE resume of the turn they share.
    #[tokio::test]
    async fn two_parked_calls_in_one_turn_resume_it_once() {
        let (orch, first, _) = durable_env().await;
        let second = sibling_record(&first);
        register_warm(&orch);
        insert(&orch.db.local, &first).await.unwrap();
        insert(&orch.db.local, &second).await.unwrap();

        resolve(&orch, &orch.db.local, &first, ELAPSED, false)
            .await
            .unwrap();
        resolve(&orch, &orch.db.local, &second, EXITED, false)
            .await
            .unwrap();

        assert_eq!(
            wait_resolved_turns(&orch).await.len(),
            1,
            "a turn resumes once however many of its calls parked"
        );
        let results = tool_results(&orch).await;
        assert_eq!(
            results.len(),
            2,
            "each parked call is answered on its own id"
        );
        assert!(results.iter().any(|(_, data)| data.contains("tool-1")));
        assert!(results.iter().any(|(_, data)| data.contains("tool-2")));
        assert_eq!(wait_row(&orch, "wait-1").await.0, "resolved");
        assert_eq!(wait_row(&orch, "wait-2").await.0, "resolved");
        assert_eq!(active_wait_count(&orch).await, 0);
    }

    /// The turn resumes when the LAST of its calls settles, so the first one to
    /// finish answers its own call and stops there. Resuming earlier would wake
    /// the agent with one of its calls still unanswered.
    #[tokio::test]
    async fn the_first_call_to_settle_stands_down_without_a_successor() {
        let (orch, first, _) = durable_env().await;
        let second = sibling_record(&first);
        register_warm(&orch);
        insert(&orch.db.local, &first).await.unwrap();
        insert(&orch.db.local, &second).await.unwrap();

        resolve(&orch, &orch.db.local, &first, ELAPSED, false)
            .await
            .unwrap();

        // It is finished: answered, closed, and owning no successor.
        assert_eq!(wait_row(&orch, "wait-1").await.0, "resolved");
        assert_eq!(tool_result_count(&orch).await, 1);
        let successor: Option<String> = orch
            .db
            .local
            .query_one(
                "SELECT successor_turn_id FROM agent_waits WHERE id='wait-1'",
                (),
                |r| r.opt_text(0),
            )
            .await
            .unwrap();
        assert_eq!(successor, None);
        // And the turn is still parked, waiting on its other call.
        assert!(wait_resolved_turns(&orch).await.is_empty());
        assert_eq!(orch.process_state.get_current_turn_id("run-1"), None);
        assert_eq!(wait_row(&orch, "wait-2").await.0, "pending");
    }

    /// A host that died holding the driver row re-drives only that row: the
    /// sibling that stood down is already out of the active set, and re-driving
    /// it would answer its call twice.
    #[tokio::test]
    async fn a_crash_after_a_sibling_stood_down_resumes_from_the_driver_row() {
        let (orch, first, _) = durable_env().await;
        let second = sibling_record(&first);
        register_warm(&orch);
        insert(&orch.db.local, &first).await.unwrap();
        insert(&orch.db.local, &second).await.unwrap();
        resolve(&orch, &orch.db.local, &first, ELAPSED, false)
            .await
            .unwrap();
        // The driver claimed its row and recorded its result, then the host died.
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving',resolution_json=?1 WHERE id='wait-2'",
                (EXITED,),
            )
            .await
            .unwrap();

        reconcile(&orch).await;
        for _ in 0..200 {
            if wait_row(&orch, "wait-2").await.0 == "resolved" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        assert_eq!(wait_row(&orch, "wait-2").await.0, "resolved");
        assert_eq!(wait_resolved_turns(&orch).await.len(), 1);
        assert_eq!(
            tool_result_count(&orch).await,
            2,
            "each call keeps exactly one answer across the restart"
        );
    }

    /// Superseding a turn the job has left answers and closes EVERY call it
    /// parked, not just the first one found. A row left behind is what makes
    /// waiting single-use for the rest of the job's life.
    #[tokio::test]
    async fn superseding_a_turn_abandons_every_one_of_its_parked_calls() {
        let (orch, first, _) = durable_env().await;
        insert(&orch.db.local, &first).await.unwrap();
        insert(&orch.db.local, &sibling_record(&first))
            .await
            .unwrap();
        reconstruct_session(&orch).await;
        register_warm_for(&orch, "run-2", "session-2");

        // Its own row and call, distinct from both of the abandoned ones.
        let successor = Record {
            id: "wait-3".into(),
            tool_use_id: "tool-3".into(),
            ..successor_record(&first)
        };
        suspend(&orch, &orch.db.local, &successor).await.unwrap();

        for id in ["wait-1", "wait-2"] {
            let (state, resolution, delivered) = wait_row(&orch, id).await;
            assert_eq!(state, "cancelled", "{id} was left active");
            assert!(resolution
                .as_deref()
                .unwrap_or_default()
                .contains(ABANDONED_OUTCOME));
            assert!(delivered.is_some(), "{id}'s call was never answered");
        }
        assert_eq!(tool_results(&orch).await.len(), 2);
        // The reconstructed turn's own suspension is now the job's only live one.
        assert_eq!(active_wait_count(&orch).await, 1);
        assert!(wait_resolved_turns(&orch).await.is_empty());
    }

    fn parked(condition: Condition, tool_use_id: &str, resolution: Option<&str>) -> ParkedCall {
        ParkedCall {
            condition,
            tool_use_id: tool_use_id.into(),
            resolution: resolution.map(str::to_string),
        }
    }

    /// The bytes of a lone suspension's resume prompt are the contract: every
    /// `run` batch and `waitFor` that suspends today reads its own result and
    /// nothing else, so composing must not put a single word in front of it.
    #[test]
    fn a_single_parked_call_resumes_with_its_result_verbatim() {
        let calls = vec![parked(Condition::Duration, "tool-1", Some(ELAPSED))];
        assert_eq!(compose_resume_prompt(&calls, ELAPSED), ELAPSED);
        // The same holds for a driver whose turn's rows could not be read at all.
        assert_eq!(compose_resume_prompt(&[], ELAPSED), ELAPSED);
    }

    /// With several, each result is named by the call it answers -- an unlabeled
    /// concatenation would ask the model to guess.
    #[test]
    fn the_resume_prompt_carries_every_parked_call_labeled() {
        let calls = vec![
            parked(
                run_batch_condition(false, Some("bun run test:rust")),
                "toolu-tests",
                Some("suite green"),
            ),
            parked(
                Condition::Terminal {
                    uri: "cairn://p/prj/1/1/builder/terminal/dev".into(),
                    slug: "dev".into(),
                    owner: None,
                    on: TerminalWaitEvent::Output,
                    phrase: Some("ready".into()),
                },
                "toolu-dev",
                Some("dev is ready"),
            ),
        ];

        let prompt = compose_resume_prompt(&calls, "suite green");

        for expected in [
            "2 calls",
            "run: bun run test:rust",
            "toolu-tests",
            "suite green",
            "waitFor: terminal dev output",
            "toolu-dev",
            "dev is ready",
        ] {
            assert!(
                prompt.contains(expected),
                "missing {expected} in:\n{prompt}"
            );
        }
    }

    /// A batch row whose call a permission request took over answers nothing
    /// itself, so a sibling's result resumes the turn verbatim rather than as
    /// one of two labeled sections.
    #[test]
    fn a_call_handed_over_to_an_inner_wait_is_left_out_of_the_prompt() {
        let calls = vec![
            parked(
                run_batch_condition(false, None),
                "tool-1",
                Some(TRANSFERRED_OUTCOME),
            ),
            parked(Condition::Duration, "tool-2", Some(ELAPSED)),
        ];
        assert_eq!(compose_resume_prompt(&calls, ELAPSED), ELAPSED);
    }

    /// A held call that answered on its own socket earlier in the turn has
    /// already been read by the agent, so it is no section of the resume.
    #[test]
    fn a_call_that_answered_inline_is_left_out_of_the_prompt() {
        let calls = vec![
            parked(
                run_batch_condition(false, Some("cargo test")),
                "tool-1",
                Some(DELIVERED_INLINE_OUTCOME),
            ),
            parked(
                run_batch_condition(false, Some("cargo build")),
                "tool-2",
                Some(DELIVERED_INLINE_OUTCOME),
            ),
            parked(Condition::Duration, "tool-3", Some(ELAPSED)),
        ];
        assert_eq!(compose_resume_prompt(&calls, ELAPSED), ELAPSED);
    }

    /// The row a batch's call is parked or held on, as it stands now.
    async fn wait_shape(orch: &Orchestrator) -> Vec<(String, String, String, Option<String>, i64)> {
        orch.db
            .local
            .query_all(
                "SELECT id, state, kind, hold, created_at FROM agent_waits WHERE run_id='run-1' AND tool_use_id='tool-1'",
                (),
                |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?, row.opt_text(3)?, row.i64(4)?)),
            )
            .await
            .unwrap()
    }

    /// A takeover turns the batch's own row into the continuation: still the
    /// call's one row, under the same id, now pending as an MCP continuation
    /// with its hold cleared and its age restarted. From a parked batch it
    /// keeps the park the call already has; from a held one it is what parks
    /// the call.
    #[tokio::test]
    async fn a_takeover_turns_the_batch_row_into_the_continuation_and_parks_it_once() {
        for held in [false, true] {
            let (orch, base, emitter) = durable_env_capturing().await;
            let batch = Record {
                created: base.created - 60_000,
                ..run_batch_record(&base, false)
            };
            if held {
                insert_as(&orch.db.local, &batch, Establish::Hold)
                    .await
                    .unwrap();
            } else {
                insert(&orch.db.local, &batch).await.unwrap();
            }
            assert_eq!(
                turn_state_of(&orch, "pred-turn").await,
                if held { "running" } else { "yielded" }
            );
            let announced = turn_changes(&emitter).len();

            let record = take_over_run_batch(
                &orch,
                &orch.db.local,
                &batch,
                Condition::McpContinuation { state: mcp_state() },
                None,
            )
            .await
            .unwrap();

            assert_eq!(record.id, batch.id);
            let rows = wait_shape(&orch).await;
            assert_eq!(rows.len(), 1, "the call still has one row: {rows:?}");
            let (id, state, kind, hold, created) = rows.into_iter().next().unwrap();
            assert_eq!(
                (id.as_str(), state.as_str(), kind.as_str(), hold),
                (batch.id.as_str(), "pending", "mcp_continuation", None)
            );
            assert_eq!(
                created, record.created,
                "its age restarts with the condition"
            );
            assert!(created > batch.created);
            assert!(
                super::super::mcp_continuation::load(&orch.db.local, &batch.id)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert_eq!(turn_state_of(&orch, "pred-turn").await, "yielded");
            assert_eq!(
                turn_changes(&emitter).len(),
                announced + usize::from(held),
                "only a held call's turn yields in the takeover (held: {held})"
            );
        }
    }

    /// A takeover lands only while the row is still the batch's pending wait.
    /// Once a stop has cancelled it nothing is converted and nothing written.
    #[tokio::test]
    async fn a_takeover_of_a_cancelled_batch_row_writes_nothing() {
        let (orch, base, _) = durable_env().await;
        let batch = run_batch_record(&base, false);
        insert(&orch.db.local, &batch).await.unwrap();
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='cancelled' WHERE id='wait-1'",
                (),
            )
            .await
            .unwrap();

        let Err(error) = take_over_run_batch(
            &orch,
            &orch.db.local,
            &batch,
            Condition::McpContinuation { state: mcp_state() },
            None,
        )
        .await
        else {
            panic!("a cancelled batch row handed its call over");
        };

        assert_eq!(error, SUSPENSION_UNAVAILABLE);
        let rows = wait_shape(&orch).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            (rows[0].1.as_str(), rows[0].2.as_str()),
            ("cancelled", "run_batch"),
            "the cancelled row is untouched"
        );
    }

    /// A row with no recorded result answers nothing, so it must not become a
    /// section that says nothing -- and the one real answer stays verbatim.
    #[test]
    fn a_call_with_no_recorded_result_is_left_out_of_the_prompt() {
        let calls = vec![
            parked(Condition::Duration, "tool-1", Some(ELAPSED)),
            parked(Condition::Duration, "tool-2", None),
        ];
        assert_eq!(compose_resume_prompt(&calls, ELAPSED), ELAPSED);
    }

    /// The driver builds its prompt from its TURN's rows rather than its own, so
    /// a sibling that already stood down still reaches the resumed agent.
    #[tokio::test]
    async fn the_driver_carries_a_stood_down_siblings_result_too() {
        let (orch, first, _) = durable_env().await;
        let second = sibling_record(&first);
        register_warm(&orch);
        insert(&orch.db.local, &first).await.unwrap();
        insert(&orch.db.local, &second).await.unwrap();
        resolve(&orch, &orch.db.local, &first, ELAPSED, false)
            .await
            .unwrap();
        // The state the driver holds when it composes: claimed, resolution
        // recorded, election won.
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving',resolution_json=?1 WHERE id='wait-2'",
                (EXITED,),
            )
            .await
            .unwrap();

        let prompt = resume_prompt_for_turn(&orch.db.local, &second, EXITED).await;

        for expected in [ELAPSED, EXITED, "tool-1", "tool-2"] {
            assert!(
                prompt.contains(expected),
                "missing {expected} in:\n{prompt}"
            );
        }
    }

    /// A row whose result a resolver already delivered is still cleared out of
    /// the pending set, but is not answered a second time.
    #[tokio::test]
    async fn an_already_answered_abandoned_row_is_cleared_without_a_second_result() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving' WHERE id='wait-1'",
                (),
            )
            .await
            .unwrap();
        // The resolver answered through the real delivery path, not a
        // hand-placed marker.
        store_result_once(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();
        reconstruct_session(&orch).await;
        register_warm_for(&orch, "run-2", "session-2");

        suspend(&orch, &orch.db.local, &successor_record(&record))
            .await
            .unwrap();

        assert_eq!(wait_row(&orch, "wait-1").await.0, "cancelled");
        assert_eq!(active_wait_count(&orch).await, 1);
        let results = tool_results(&orch).await;
        assert_eq!(
            results.len(),
            1,
            "an already-answered call must not be answered twice"
        );
        assert!(results[0].1.contains("elapsed"));
    }

    /// The interleaving the claim exists for: a live resolver has won the right
    /// to answer its call but has not appended the event yet, and a supersession
    /// runs entirely inside that window. Driving the resolver's two halves
    /// separately is what makes that window observable — a read-then-append
    /// resolver would let the supersession answer behind it, leaving one tool use
    /// with two answers.
    #[tokio::test]
    async fn supersession_cannot_answer_a_call_a_resolver_has_already_claimed() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        assert!(
            claim_result_delivery(&orch.db.local, "wait-1")
                .await
                .unwrap(),
            "the resolver wins the claim first"
        );
        reconstruct_session(&orch).await;
        register_warm_for(&orch, "run-2", "session-2");

        // The whole supersession runs while the resolver is paused mid-delivery.
        suspend(&orch, &orch.db.local, &successor_record(&record))
            .await
            .unwrap();

        assert!(
            tool_results(&orch).await.is_empty(),
            "supersession must not answer a call another writer claimed"
        );
        // The row still leaves the pending set, so the new suspension stands.
        assert_eq!(wait_row(&orch, "wait-1").await.0, "cancelled");
        assert_eq!(active_wait_count(&orch).await, 1);

        // The resolver's own append then lands, and is the only answer.
        append_claimed_result(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();
        let results = tool_results(&orch).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].1.contains("elapsed"));
    }

    /// The same exclusion from the other side: once a supersession has answered
    /// an abandoned call, the resolver that wakes afterwards stands down.
    #[tokio::test]
    async fn a_resolver_cannot_answer_a_call_supersession_already_answered() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        reconstruct_session(&orch).await;
        register_warm_for(&orch, "run-2", "session-2");
        suspend(&orch, &orch.db.local, &successor_record(&record))
            .await
            .unwrap();
        assert_eq!(tool_results(&orch).await.len(), 1);

        store_result_once(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();

        let results = tool_results(&orch).await;
        assert_eq!(results.len(), 1, "one tool use is answered exactly once");
        assert!(results[0].1.contains("abandoned"));
    }

    /// A host that dies between committing a delivery claim and appending the
    /// event leaves the claim held with nothing behind it. Startup must not read
    /// that as an answer, or the call is never answered at all: the claim is a
    /// lock, and the transcript is the fact it locks over.
    #[tokio::test]
    async fn a_claim_interrupted_by_a_restart_still_answers_its_call() {
        let (orch, base, _) = durable_env().await;
        register_warm(&orch);
        // A host batch: the one kind of run batch a restart answers at boot.
        let mut record = run_batch_record(&base, true);
        if let Condition::RunBatch { host, .. } = &mut record.condition {
            *host = true;
        }
        insert(&orch.db.local, &record).await.unwrap();
        // The pre-restart host claimed delivery and died before appending.
        assert!(claim_result_delivery(&orch.db.local, "wait-1")
            .await
            .unwrap());

        reconcile(&orch).await;

        for _ in 0..200 {
            if wait_state(&orch).await == "resolved" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(wait_state(&orch).await, "resolved");
        let results = tool_results(&orch).await;
        assert_eq!(
            results.len(),
            1,
            "the interrupted claim must not swallow the answer"
        );
        assert!(results[0].1.contains("restarted"));
    }

    /// A remote batch's wait is not answered at boot, but a delivery claim its
    /// dead process held is still reclaimed there, so the answer that arrives
    /// later from the batch's machine lands exactly once.
    #[tokio::test]
    async fn a_parked_remote_batch_answered_after_a_restart_is_not_swallowed_by_a_dead_claim() {
        let (orch, base, _) = durable_env().await;
        register_warm(&orch);
        let record = run_batch_record(&base, true);
        insert(&orch.db.local, &record).await.unwrap();
        assert!(claim_result_delivery(&orch.db.local, "wait-1")
            .await
            .unwrap());

        reconcile(&orch).await;
        assert_eq!(wait_state(&orch).await, "pending");
        super::super::run::adopted::deliver_batch_result(
            &orch,
            &orch.db.local,
            "request-1",
            "succeeded",
            "the batch's real result",
        )
        .await;

        assert_eq!(wait_state(&orch).await, "resolved");
        let results = tool_results(&orch).await;
        assert_eq!(
            results.len(),
            1,
            "the dead claim must not swallow the answer"
        );
        assert!(results[0].1.contains("the batch's real result"));
    }

    /// Why the reclaim is load-bearing rather than decorative: a held claim, on
    /// its own, does block delivery — which is the correct behavior against a
    /// live writer and the wrong one against a dead one. This pins both halves.
    #[tokio::test]
    async fn a_held_claim_blocks_delivery_until_it_is_reclaimed() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        assert!(claim_result_delivery(&orch.db.local, "wait-1")
            .await
            .unwrap());

        // A re-drive that trusted the claim alone declines to answer.
        store_result_once(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();
        assert!(
            tool_results(&orch).await.is_empty(),
            "a held claim blocks delivery, which is why a dead writer's claim must be reclaimed"
        );

        // Reclaiming it is exactly what lets the answer land.
        reclaim_interrupted_delivery(&orch.db.local, &SuspendedCall::of(&record), None).await;
        store_result_once(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();
        assert_eq!(tool_results(&orch).await.len(), 1);
    }

    /// Put a row in the state a resolver holds while it works: claimed, and
    /// carrying the resolution it read. `successor_turn_id` carries a foreign
    /// key, so the turns a claim can name have to exist.
    async fn resolver_in_flight(orch: &Orchestrator) {
        for (id, sequence) in [("successor-turn", 2), ("other-turn", 3)] {
            orch.db
                .local
                .execute(
                    "INSERT INTO turns (id,session_id,run_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at) VALUES (?1,'session-1','run-1','job-1',?2,'pred-turn','pending','wait_resolved',2,2)",
                    (id, sequence),
                )
                .await
                .unwrap();
        }
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving',resolution_json=?1 WHERE id='wait-1'",
                (ELAPSED,),
            )
            .await
            .unwrap();
    }

    /// Resuming and abandoning are two writes to one row, and exactly one wins.
    /// Both orderings are driven here by invoking the two operations directly:
    /// no timing, no sleeps, just the two linearization points in each order.
    #[tokio::test]
    async fn abandonment_wins_when_it_commits_before_the_continuation_is_owned() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        resolver_in_flight(&orch).await;

        assert!(
            mark_abandoned(&orch.db.local, "wait-1", AbandonCause::Superseded)
                .await
                .unwrap(),
            "abandonment wins an unowned row"
        );
        assert!(
            !claim_continuation(&orch.db.local, "wait-1", "successor-turn")
                .await
                .unwrap(),
            "the resolver must lose the resume once the row is abandoned"
        );
        assert_eq!(
            wait_row(&orch, "wait-1").await.1.as_deref(),
            Some(abandoned_result(AbandonCause::Superseded).as_str())
        );
    }

    /// Supersession leaves the batch running, whereas Compact cancels session
    /// work. Replaying either abandonment must answer once without resuming.
    #[tokio::test]
    async fn run_batch_abandonment_preserves_only_superseded_work() {
        for cause in AbandonCause::ALL {
            let (orch, base, _) = durable_env().await;
            let record = run_batch_record(&base, true);
            insert(&orch.db.local, &record).await.unwrap();
            let call = SuspendedCall::of(&record);

            // Crash after the decision, before answering or closing the wait.
            assert!(mark_abandoned(&orch.db.local, &record.id, cause)
                .await
                .unwrap());
            // A resolver that already decided to divert a result must lose to
            // the decision too, even before abandonment closes the row.
            divert_to_job(&orch, &orch.db.local, &record, "late batch result", true)
                .await
                .unwrap();
            assert_eq!(wait_state(&orch).await, "resolving");
            finish_abandonment(&orch, &orch.db.local, &call, cause)
                .await
                .unwrap();
            finish_abandonment(&orch, &orch.db.local, &call, cause)
                .await
                .unwrap();
            // A late batch resolver cannot replace the abandonment or resume.
            resolve(&orch, &orch.db.local, &record, "late batch result", true)
                .await
                .unwrap();

            assert_eq!(wait_state(&orch).await, "cancelled");
            assert!(wait_resolved_turns(&orch).await.is_empty());
            let results = tool_results(&orch).await;
            assert_eq!(results.len(), 1, "{cause:?}: {results:?}");
            assert!(!results[0].1.contains("late batch result"));
            let detached = super::super::run::detached::load(&orch.db.local, "request-1")
                .await
                .unwrap();
            match cause {
                AbandonCause::Superseded => {
                    assert_eq!(
                        detached.unwrap().state,
                        super::super::run::detached::DetachedState::Running
                    );
                    assert!(results[0].1.contains("cairn:~/batches/request-1"));
                    assert!(results[0].1.contains("keeps running"));
                }
                AbandonCause::OperatorCompact => {
                    assert!(detached.is_none(), "Compact must not detach cancelled work");
                    assert!(results[0].1.contains("operator_compact"));
                    assert!(results[0].1.contains("operator compacted"));
                    assert!(!results[0].1.contains("keeps running"));
                    assert!(!results[0].1.contains("cairn:~/batches/"));
                }
            }
        }
    }

    /// An operator Compact's abandonment carries its cause in the payload, so
    /// the continuation claim's exact-payload fence must know that payload too:
    /// a resolver already in flight loses the resume and mints nothing.
    #[tokio::test]
    async fn an_operator_compact_abandonment_also_wins_the_continuation() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        resolver_in_flight(&orch).await;
        // The fixture's own claimable turns; resolving must add none.
        let before = wait_resolved_turns(&orch).await;

        assert!(
            mark_abandoned(&orch.db.local, "wait-1", AbandonCause::OperatorCompact)
                .await
                .unwrap()
        );
        assert!(
            !claim_continuation(&orch.db.local, "wait-1", "successor-turn")
                .await
                .unwrap(),
            "a resolver must not resume a turn the operator compacted away"
        );
        resolve(&orch, &orch.db.local, &record, ELAPSED, true)
            .await
            .unwrap();
        assert_eq!(
            wait_resolved_turns(&orch).await,
            before,
            "no successor is minted for a compacted wait"
        );
    }

    /// The supersession payload is byte-identical to what rows written before
    /// causes existed hold, and each stored payload reads back as its cause.
    #[test]
    fn abandonment_causes_round_trip_through_their_payloads() {
        assert_eq!(
            abandoned_result(AbandonCause::Superseded),
            serde_json::json!({
                "outcome": "abandoned",
                "error": "This call's turn was replaced before its wait finished, so the wait was abandoned. Reissue it if the work still matters."
            })
            .to_string()
        );
        for cause in AbandonCause::ALL {
            let payload = abandoned_result(cause);
            assert!(is_abandonment(Some(&payload)));
            assert_eq!(AbandonCause::of(Some(&payload)), cause);
        }
    }

    #[tokio::test]
    async fn the_resolver_wins_when_it_owns_the_continuation_first() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        resolver_in_flight(&orch).await;

        assert!(
            claim_continuation(&orch.db.local, "wait-1", "successor-turn")
                .await
                .unwrap(),
            "the resolver wins an unabandoned row"
        );
        assert!(
            !mark_abandoned(&orch.db.local, "wait-1", AbandonCause::Superseded)
                .await
                .unwrap(),
            "abandonment must lose once the resume is owned"
        );
        // The resolver's own resolution is untouched, so it resumes as planned.
        let (_, resolution, _) = wait_row(&orch, "wait-1").await;
        assert_eq!(resolution.as_deref(), Some(ELAPSED));
    }

    /// Ownership is re-entrant for the replay that re-drives its own recorded
    /// successor — crash recovery depends on re-entering this path.
    #[tokio::test]
    async fn owning_a_continuation_is_re_entrant_for_its_own_successor() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        resolver_in_flight(&orch).await;

        assert!(
            claim_continuation(&orch.db.local, "wait-1", "successor-turn")
                .await
                .unwrap()
        );
        assert!(
            claim_continuation(&orch.db.local, "wait-1", "successor-turn")
                .await
                .unwrap(),
            "a replay re-drives the successor it already owns"
        );
        assert!(
            !claim_continuation(&orch.db.local, "wait-1", "other-turn")
                .await
                .unwrap(),
            "but never a foreign successor"
        );
    }

    /// Continuation ownership, not just delivery: a resolver already past its own
    /// state read holds a stale local result and will drive `continue_job_impl`
    /// unless something fences it. Recording the abandonment while it works is
    /// that fence — it must stand down rather than resume a run the reconstructed
    /// turn has taken over.
    #[tokio::test]
    async fn a_resolver_overtaken_by_abandonment_stands_down_without_resuming() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);
        // The resolver claimed the row and read its own resolution — the state it
        // holds while it works.
        orch.db
            .local
            .execute(
                "UPDATE agent_waits SET state='resolving',resolution_json=?1 WHERE id='wait-1'",
                (ELAPSED,),
            )
            .await
            .unwrap();
        // A reconstructed turn abandons that suspension inside the window.
        assert!(
            mark_abandoned(&orch.db.local, "wait-1", AbandonCause::Superseded)
                .await
                .unwrap()
        );

        // The resolver now proceeds with the result it read before the change.
        resolve(&orch, &orch.db.local, &record, ELAPSED, true)
            .await
            .unwrap();

        assert!(
            wait_resolved_turns(&orch).await.is_empty(),
            "an overtaken resolver must not create a successor"
        );
        assert_eq!(
            orch.process_state.get_current_turn_id("run-1"),
            None,
            "an overtaken resolver must not resume the run"
        );
        assert_eq!(
            turn_state_of(&orch, "pred-turn").await,
            "yielded",
            "the abandoned turn is left where it was"
        );
    }

    /// The fence is the decision, not the mere passage of time: an ordinary
    /// resolution still resolves and still resumes.
    #[tokio::test]
    async fn an_unovertaken_resolver_still_resumes_normally() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        resolve(&orch, &orch.db.local, &record, ELAPSED, false)
            .await
            .unwrap();

        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1);
        assert_eq!(successors[0].1, "running");
        assert_eq!(wait_state(&orch).await, "resolved");
    }

    /// A host that died after answering an abandoned call but before closing its
    /// row must not have that row re-driven as an ordinary wait: its turn is
    /// gone, so a resume would resurrect the suspension being discarded.
    #[tokio::test]
    async fn a_crash_between_answering_and_closing_an_abandonment_never_resumes_the_run() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);
        let call = SuspendedCall::of(&record);
        // The pre-crash host recorded the decision and answered the call, then
        // died before closing the row.
        assert!(
            mark_abandoned(&orch.db.local, "wait-1", AbandonCause::Superseded)
                .await
                .unwrap()
        );
        deliver_abandonment(&orch, &orch.db.local, &call, AbandonCause::Superseded).await;
        assert_eq!(tool_results(&orch).await.len(), 1);

        reconcile(&orch).await;
        for _ in 0..200 {
            if wait_state(&orch).await == "cancelled" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        assert_eq!(wait_state(&orch).await, "cancelled", "the stale row closes");
        assert!(
            wait_resolved_turns(&orch).await.is_empty(),
            "an abandoned suspension is never resumed"
        );
        assert_eq!(
            tool_results(&orch).await.len(),
            1,
            "its call stays answered exactly once"
        );
        assert_eq!(
            turn_state_of(&orch, "pred-turn").await,
            "yielded",
            "the abandoned turn is left where it was"
        );
    }

    /// The earlier window: the decision was recorded but the call was never
    /// answered. Startup finishes the abandonment — answers, then closes — still
    /// without resuming.
    #[tokio::test]
    async fn a_crash_after_recording_an_abandonment_answers_and_closes_without_resuming() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);
        assert!(
            mark_abandoned(&orch.db.local, "wait-1", AbandonCause::Superseded)
                .await
                .unwrap()
        );

        reconcile(&orch).await;
        for _ in 0..200 {
            if wait_state(&orch).await == "cancelled" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        assert_eq!(wait_state(&orch).await, "cancelled");
        assert!(wait_resolved_turns(&orch).await.is_empty());
        let results = tool_results(&orch).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].1.contains(ABANDONED_OUTCOME));
    }

    /// The converse: a claim with its result already in the transcript is a
    /// completed delivery, and reclaiming must leave it alone.
    #[tokio::test]
    async fn a_delivered_result_is_never_reclaimed_or_answered_twice() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        store_result_once(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();
        let claimed_at = wait_row(&orch, "wait-1").await.2;
        assert!(claimed_at.is_some());

        reclaim_interrupted_delivery(&orch.db.local, &SuspendedCall::of(&record), None).await;

        assert_eq!(
            wait_row(&orch, "wait-1").await.2,
            claimed_at,
            "a completed delivery keeps its claim"
        );
        store_result_once(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();
        assert_eq!(tool_results(&orch).await.len(), 1);
    }

    /// A delivery whose append fails holds no claim afterwards, so the answer is
    /// still owed rather than silently swallowed.
    #[tokio::test]
    async fn a_failed_append_gives_back_its_claim() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        // A record whose run does not exist cannot have an event stored against
        // it, so the append fails after the claim is taken.
        let unstorable = Record {
            run_id: "missing-run".into(),
            ..record.clone()
        };
        assert!(claim_result_delivery(&orch.db.local, "wait-1")
            .await
            .unwrap());
        assert!(
            append_claimed_result(&orch, &orch.db.local, &unstorable, ELAPSED)
                .await
                .is_err()
        );

        assert_eq!(
            wait_row(&orch, "wait-1").await.2,
            None,
            "a claim is held only by a delivery that landed"
        );
        // So the real delivery can still answer the call.
        store_result_once(&orch, &orch.db.local, &record, ELAPSED)
            .await
            .unwrap();
        assert_eq!(tool_results(&orch).await.len(), 1);
    }

    /// Upgrade path: a job stranded TODAY carries a row a pre-fix host left
    /// behind -- its turn finished, its session closed, and no resolver survives
    /// to claim it. It recovers on the next suspension, with no surgery.
    #[tokio::test]
    async fn a_job_stranded_by_a_pre_fix_row_recovers_on_its_next_suspension() {
        let (orch, record, _) = durable_env().await;
        orch.db
            .local
            .execute(
                "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at) VALUES('stranded','job-1','run-1','session-1','pred-turn','tool-0','{\"kind\":\"duration\"}','pending',1)",
                (),
            )
            .await
            .unwrap();
        orch.db
            .local
            .execute(
                "UPDATE turns SET state='completed' WHERE id='pred-turn'",
                (),
            )
            .await
            .unwrap();
        reconstruct_session(&orch).await;
        register_warm_for(&orch, "run-2", "session-2");

        suspend(&orch, &orch.db.local, &successor_record(&record))
            .await
            .unwrap();

        assert_eq!(wait_row(&orch, "stranded").await.0, "cancelled");
        assert_eq!(wait_row(&orch, "wait-2").await.0, "pending");
        assert_eq!(active_wait_count(&orch).await, 1);
    }

    #[tokio::test]
    async fn self_suspend_preserves_pending_wait_while_user_stop_cancels() {
        let (orch, record, _) = durable_env().await;
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);

        // A durable self-suspend must NOT cancel its own wait row.
        crate::orchestrator::lifecycle::suspend_run_for_durable_wait(
            &orch,
            "run-1",
            "owned_wait_suspended",
            crate::agent_process::process::GoalPauseOwner::DurableWait {
                wait_id: record.id.clone(),
            },
        )
        .unwrap();
        assert_eq!(
            wait_state(&orch).await,
            "pending",
            "self-suspend must preserve the pending wait"
        );
        assert!(
            orch.process_state
                .processes
                .lock()
                .unwrap()
                .get("run-1")
                .is_some(),
            "self-suspend keeps the process warm for resume"
        );

        assert!(matches!(
            crate::execution::jobs::admit_codex_goal_turn(
                &orch,
                orch.db.local.clone(),
                "run-1",
                "session-1",
                "job-1",
                "provider-before-stop",
                None,
            )
            .unwrap(),
            crate::execution::jobs::CodexGoalTurnAdmission::Deferred { .. }
        ));

        // An external stop cancels the wait and terminalizes the predecessor.
        // Because deferral created no successor-shaped state, there is nothing
        // for cancellation to strand or accidentally start.
        crate::orchestrator::lifecycle::stop_active_turn_for_run(&orch, "run-1", true);
        assert_eq!(
            wait_state(&orch).await,
            "cancelled",
            "external stop cancels the wait"
        );
        assert!(wait_resolved_turns(&orch).await.is_empty());
        // Parked, not running: `interrupt_turn` takes only a running turn, and
        // yielded -> cancelled is the single terminal-to-terminal transition the
        // turn state machine allows. `user_stop` is what distinguishes this from
        // a turn cancelled for any other cause.
        assert_eq!(turn_state_of(&orch, "pred-turn").await, "cancelled");
        assert_eq!(
            orch.db
                .local
                .query_opt_text(
                    "SELECT end_reason FROM turns WHERE id = ?1",
                    ("pred-turn".to_string(),),
                )
                .await
                .unwrap()
                .as_deref(),
            Some("user_stop")
        );
    }

    // ---- Escalated direct messages and waitFor parks -----------------------

    use crate::messages::delivery::{
        nudge_for_direct_message, DirectMessageRef, Hold, NudgeOutcome,
    };
    use crate::messages::queued::DeliveryUrgency;

    /// A direct message to the parked builder from its coordinator, persisted
    /// and pushed the way the message handler does before it nudges.
    async fn seed_direct(orch: &Orchestrator, urgency: &str) -> DirectMessageRef {
        orch.db
            .local
            .execute(
                "INSERT INTO messages (id, channel_type, sender_name, recipient_run_id, content, created_at, urgency)
                 VALUES ('msg-1', 'direct', 'coordinator', 'run-1', 'stop waiting, the plan changed', 1, ?1)",
                (urgency.to_string(),),
            )
            .await
            .unwrap();
        crate::orchestrator::attention_push::push(
            &orch.db.local,
            "job-1",
            "cairn://p/prj/1/1/builder",
            if urgency == "interrupt" {
                crate::orchestrator::attention_push::Wake::Interrupt
            } else {
                crate::orchestrator::attention_push::Wake::Wake
            },
            crate::orchestrator::attention_push::Boundary::Event,
            "direct:msg-1",
        )
        .await
        .unwrap();
        DirectMessageRef {
            from: "cairn://p/prj/1/1/coordinator".into(),
            message_id: "msg-1".into(),
        }
    }

    async fn direct_push_delivered(orch: &Orchestrator) -> bool {
        orch.db
            .local
            .query_opt_text(
                "SELECT delivered_event_id FROM attention_pushes WHERE key = 'direct:msg-1'",
                (),
            )
            .await
            .unwrap()
            .is_some()
    }

    /// A duration wait still ten minutes from firing, so only the message can
    /// end it inside the test.
    fn ten_minute_wait(base: Record) -> Record {
        Record {
            deadline: Some(chrono::Utc::now().timestamp_millis() + 600_000),
            ..base
        }
    }

    #[tokio::test]
    async fn an_escalated_direct_ends_a_waitfor_duration_park_and_resumes_once() {
        let (orch, base, _) = durable_env().await;
        let record = ten_minute_wait(base);
        insert(&orch.db.local, &record).await.unwrap();
        register_warm(&orch);
        let message = seed_direct(&orch, "interrupt").await;

        let outcome =
            nudge_for_direct_message(&orch, "job-1", DeliveryUrgency::Interrupt, &message).unwrap();
        assert_eq!(
            outcome,
            NudgeOutcome::SupersededWait {
                labels: vec!["waitFor: duration".into()]
            }
        );

        await_wait_state(&orch, "wait-1", "resolved").await;
        let successors = wait_resolved_turns(&orch).await;
        assert_eq!(successors.len(), 1, "exactly one successor turn");
        assert_eq!(successors[0].1, "running");
        let results = tool_results(&orch).await;
        assert_eq!(results.len(), 1, "the waitFor call is answered once");
        assert!(
            results[0].1.contains("superseded_by_message") && results[0].1.contains("msg-1"),
            "the call's result says a message ended it: {}",
            results[0].1
        );
        assert!(
            direct_push_delivered(&orch).await,
            "the resume delivers the message itself"
        );
    }

    #[tokio::test]
    async fn an_escalated_direct_ends_terminal_and_checks_parks_and_their_triggers_stand_down() {
        let parks = [
            (
                Condition::Terminal {
                    uri: "cairn://p/prj/1/1/builder/terminal/tests".into(),
                    slug: "tests".into(),
                    owner: None,
                    on: TerminalWaitEvent::Exit,
                    phrase: None,
                },
                "waitFor: terminal tests exit",
            ),
            (
                Condition::Checks {
                    uri: "cairn://p/prj/1/1/reviewer/checks".into(),
                    job_id: "job-other".into(),
                    suite: Some("rust-tests".into()),
                },
                "waitFor: checks verdict rust-tests",
            ),
        ];
        for (condition, label) in parks {
            let (orch, base, _) = durable_env().await;
            let record = Record {
                condition,
                deadline: None,
                ..base
            };
            insert(&orch.db.local, &record).await.unwrap();
            register_warm(&orch);
            let message = seed_direct(&orch, "interrupt").await;

            assert_eq!(
                nudge_for_direct_message(&orch, "job-1", DeliveryUrgency::Interrupt, &message)
                    .unwrap(),
                NudgeOutcome::SupersededWait {
                    labels: vec![label.into()]
                }
            );
            await_wait_state(&orch, "wait-1", "resolved").await;

            // The trigger armed for this park is still polling. Its next pass
            // finds the row settled and stands down rather than watching the
            // terminal or the lanes until they happen to finish.
            let stood_down = tokio::time::timeout(
                Duration::from_secs(5),
                super::super::owned_wait::trigger(
                    orch.clone(),
                    orch.db.local.clone(),
                    record.clone(),
                ),
            )
            .await
            .unwrap_or_else(|_| panic!("{label}: the trigger kept polling a resolved row"));
            assert_eq!(
                stood_down.unwrap_err(),
                "this wait was settled by another resolver",
                "{label}"
            );

            // Whatever it hands on loses the race: no second answer or resume.
            drive_resolution(&orch, &orch.db.local, &record, EXITED, false).await;
            assert_eq!(wait_resolved_turns(&orch).await.len(), 1, "{label}");
            assert_eq!(tool_result_count(&orch).await, 1, "{label}");
            assert_eq!(wait_state(&orch).await, "resolved", "{label}");
        }
    }

    #[tokio::test]
    async fn a_plain_direct_to_a_waitfor_park_is_held_and_names_the_escalation() {
        let (orch, base, _) = durable_env().await;
        insert(&orch.db.local, &ten_minute_wait(base))
            .await
            .unwrap();
        register_warm(&orch);
        let message = seed_direct(&orch, "steer").await;

        let outcome =
            nudge_for_direct_message(&orch, "job-1", DeliveryUrgency::Steer, &message).unwrap();
        let NudgeOutcome::Held(hold) = outcome else {
            panic!("a plain message must not end a wait: {outcome:?}");
        };
        assert_eq!(
            hold,
            Hold::Wait {
                labels: vec!["waitFor: duration".into()],
                supersedable: true
            }
        );
        let said = hold.render();
        assert!(
            said.contains("`waitFor: duration`") && said.contains("escalate:true"),
            "{said}"
        );
        assert_eq!(wait_state(&orch).await, "pending");
        assert!(wait_resolved_turns(&orch).await.is_empty());
        assert!(!direct_push_delivered(&orch).await);
    }

    #[tokio::test]
    async fn an_escalated_direct_holds_for_a_run_park_alone_or_beside_a_waitfor() {
        let batch_of = |base: &Record| Record {
            condition: run_batch_condition(false, Some("cargo test")),
            deadline: None,
            ..base.clone()
        };

        // A run batch alone is work in flight.
        let (orch, base, _) = durable_env().await;
        insert(&orch.db.local, &batch_of(&base)).await.unwrap();
        register_warm(&orch);
        let message = seed_direct(&orch, "interrupt").await;
        assert_eq!(
            nudge_for_direct_message(&orch, "job-1", DeliveryUrgency::Interrupt, &message).unwrap(),
            NudgeOutcome::Held(Hold::Wait {
                labels: vec!["run: cargo test".into()],
                supersedable: false
            })
        );
        assert_eq!(wait_state(&orch).await, "pending");

        // A waitFor parked beside one is not ended either: ending it would
        // resume the turn while the batch's result is still owed.
        let (orch, base, _) = durable_env().await;
        insert(&orch.db.local, &ten_minute_wait(base.clone()))
            .await
            .unwrap();
        insert(&orch.db.local, &batch_of(&sibling_record(&base)))
            .await
            .unwrap();
        register_warm(&orch);
        let message = seed_direct(&orch, "interrupt").await;
        let outcome =
            nudge_for_direct_message(&orch, "job-1", DeliveryUrgency::Interrupt, &message).unwrap();
        let NudgeOutcome::Held(hold) = outcome else {
            panic!("a turn with a run in flight must hold: {outcome:?}");
        };
        assert_eq!(
            hold,
            Hold::Wait {
                labels: vec!["waitFor: duration".into(), "run: cargo test".into()],
                supersedable: false
            }
        );
        assert!(hold.render().contains("`run: cargo test`"));
        assert_eq!(active_wait_count(&orch).await, 2);
        assert!(wait_resolved_turns(&orch).await.is_empty());
    }

    #[tokio::test]
    async fn mcp_task_working_input_and_terminal_outcomes_are_driven_without_an_executor() {
        let (orch, mut record, _) = durable_env().await;
        let gateway = Arc::new(FakeMcpGateway::default());
        gateway.task_outcomes.lock().unwrap().extend([
            Ok(McpTaskOutcome::Working {
                poll_interval_ms: Some(1),
            }),
            Ok(McpTaskOutcome::InputRequired {
                input_requests: serde_json::json!([{"id":"choice"}]),
            }),
            Ok(McpTaskOutcome::Complete(McpToolCallResult::from_text(
                "task done",
            ))),
        ]);
        assert!(orch.set_mcp_gateway(gateway.clone()).is_ok());
        let mut state = mcp_state();
        super::super::mcp_continuation::set_task(
            &mut state,
            "task-1".into(),
            1,
            chrono::Utc::now().timestamp_millis() - 100,
            Some(30_000),
        );
        record.condition = Condition::McpContinuation { state };
        record.deadline = None;
        insert(&orch.db.local, &record).await.unwrap();

        let driver = tokio::spawn(drive_mcp_until_terminal(
            orch.clone(),
            orch.db.local.clone(),
            record,
        ));
        answer_latest_prompt(&orch, serde_json::json!({"choice":"a"})).await;
        let result = driver.await.unwrap();
        assert!(result.contains("task done"), "{result}");
        assert_eq!(gateway.updates.lock().unwrap()[0].0, "task-1");
    }
}
