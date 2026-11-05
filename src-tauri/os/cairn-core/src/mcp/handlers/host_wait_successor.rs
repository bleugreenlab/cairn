//! The handoff from an answered host wait to the turn its job continues in.
//!
//! A permission and a blocking question both suspend the asking turn on the
//! host. When the answer arrives after the inline budget, that turn has yielded
//! and the job continues in a successor turn. This module is the one place that
//! successor is minted, started, driven, and settled, for both kinds:
//!
//! - [`ensure_host_wait_successor_conn`] classifies the run's lineage (and mints
//!   a `pending` successor when the answer owns one) inside the transaction that
//!   records the answer, so a reader never sees an answer whose continuation is
//!   undecided;
//! - [`start_host_wait_successor`] starts it on the inline fast path;
//! - [`continue_into_host_wait_successor`] drives it on the slow path by explicit
//!   id, and settles it with the kind's typed end reason if the job refuses.
//!
//! Only four things differ between the kinds, and [`HostWaitKind`] carries
//! them: the successor's start reason, its refusal end reason, the row the
//! refusal is recorded on, and (through [`HostWaitAnswer`]) how the result is
//! delivered.

use crate::agent_process::process::GoalPauseOwner;
use crate::execution::jobs::GoalReactivation;
use crate::models::{TurnEndReason, TurnStartReason, TurnState};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_common::ids;
use cairn_db::turso::params;

/// Which host wait an answer resolves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostWaitKind {
    /// A permission request (`permission_requests`).
    Permission,
    /// A question (`prompts`).
    Question,
}

impl HostWaitKind {
    /// The start reason of the successor an answer of this kind mints.
    pub(crate) fn start_reason(self) -> TurnStartReason {
        match self {
            HostWaitKind::Permission => TurnStartReason::PermissionResponse,
            HostWaitKind::Question => TurnStartReason::PromptResponse,
        }
    }

    /// How an unserved successor of this kind ends when its job refuses to
    /// continue.
    pub(crate) fn end_reason(self) -> TurnEndReason {
        match self {
            HostWaitKind::Permission => TurnEndReason::PermissionResumeRefused,
            HostWaitKind::Question => TurnEndReason::QuestionResumeRefused,
        }
    }

    fn label(self) -> &'static str {
        match self {
            HostWaitKind::Permission => "permission",
            HostWaitKind::Question => "question",
        }
    }
}

/// What answering a host wait did to its run's turn lineage. Decided by
/// [`ensure_host_wait_successor_conn`], the single place a `permission_response`
/// or `prompt_response` turn is minted. Minting never starts the turn: exactly
/// one caller starts it afterwards -- the inline waiter on the fast path, or
/// [`continue_into_host_wait_successor`] on the slow path.
#[derive(Debug, Clone, PartialEq)]
pub enum HostWaitSuccessor {
    /// The predecessor turn is still open (`running` or `pending`). A live
    /// provider poll owns it -- Codex never yields its turn while it waits, and
    /// a non-blocking question never yields at all -- so nothing is minted and
    /// nothing is resumed. A permission's poll wakes on the broadcast; a
    /// question's answer rides the turn's next tool boundary.
    ProviderTurnOpen,
    /// The run has no owning job or session, or its predecessor turn is gone, so
    /// there is no lineage to continue.
    Unowned,
    /// The job is already serving a turn that is not this predecessor's
    /// continuation. The result is delivered through the job's queue.
    JobBusy { active_turn_id: String },
    /// The predecessor already continued in a turn something else minted (an
    /// operator steer, a follow-up, another continuation). It is not this
    /// answer's to start; the result is delivered through the job's queue.
    ForeignSuccessor { turn_id: String },
    /// The predecessor's successor of this kind: minted by this answer
    /// (`inserted`) or by an earlier answer to a sibling wait raised by the same
    /// turn. `state` is as read inside the answering transaction.
    Successor {
        turn_id: String,
        inserted: bool,
        state: TurnState,
    },
}

impl HostWaitSuccessor {
    /// Whether a durably suspended run is resumed after this answer: driven into
    /// its own successor, or handed the result through the queue.
    pub(crate) fn resumes_job(&self) -> bool {
        matches!(
            self,
            HostWaitSuccessor::Successor { .. }
                | HostWaitSuccessor::JobBusy { .. }
                | HostWaitSuccessor::ForeignSuccessor { .. }
        )
    }
}

/// How the slow path delivered an answer's result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostWaitContinuation {
    /// The job continued into the successor turn; the process was driven.
    Driven,
    /// The job was busy, or another launch had already served or finished the
    /// successor, so the result was queued for the job's next boundary.
    Queued,
    /// The asking turn is still open, so the result rides its next tool boundary.
    /// No turn is minted and nothing is interrupted.
    AtNextToolBoundary,
    /// Another wait holds the asking turn, so the result rides along on whatever
    /// continues the job. No turn is minted and that wait keeps its turn.
    AfterCurrentWait,
    /// Nothing could continue the job. The successor (if unserved) was settled
    /// and the refusal recorded on the answered row.
    Refused { reason: String },
}

/// One answered host wait, as the slow-path driver delivers it.
#[derive(Debug, Clone)]
pub(crate) struct HostWaitAnswer {
    pub(crate) kind: HostWaitKind,
    /// The permission request or prompt id: the goal's pause owner, and the
    /// idempotency key of a queued result.
    pub(crate) id: String,
    /// The result already sits in the transcript as the waiting call's
    /// synthetic `tool_result`, so the continuation stores no user event.
    pub(crate) suppress_user_event: bool,
    /// The result answers a tool call the native conversation is still waiting
    /// on, so the resume never rotates onto a rebuilt session.
    pub(crate) answers_open_tool_call: bool,
    /// What the job's queue receives when the result cannot drive the
    /// successor itself. `None` queues the driven content unchanged.
    pub(crate) queued_content: Option<String>,
}

impl HostWaitAnswer {
    /// A permission answer: its result is always the waiting call's own.
    pub(crate) fn permission(request_id: &str) -> Self {
        Self {
            kind: HostWaitKind::Permission,
            id: request_id.to_string(),
            suppress_user_event: true,
            answers_open_tool_call: true,
            queued_content: None,
        }
    }
}

#[derive(Debug)]
pub struct SuccessorTurnUpdate {
    pub turn_id: String,
    pub(super) inserted: bool,
    pub(super) started: bool,
}

pub(super) async fn emit_successor_turn_events(
    db: &LocalDb,
    emitter: &dyn crate::services::EventEmitter,
    update: &SuccessorTurnUpdate,
) {
    if update.inserted {
        let change = crate::notify::turn_db_change_for_id(db, &update.turn_id, "insert").await;
        let _ = emitter.emit("db-change", change);
    }
    if update.started {
        let change = crate::notify::turn_db_change_for_id(db, &update.turn_id, "update").await;
        let _ = emitter.emit("db-change", change);
    }
}

/// The single minter of host-wait successor turns.
///
/// - An existing successor of the predecessor is returned whatever its state:
///   one with this kind's start reason as [`HostWaitSuccessor::Successor`] (a
///   sibling wait of the same turn already minted it), any other as
///   [`HostWaitSuccessor::ForeignSuccessor`].
/// - A predecessor that is not terminal (`yielded` counts as terminal) is an open
///   provider turn: [`HostWaitSuccessor::ProviderTurnOpen`].
/// - A job serving some other turn is [`HostWaitSuccessor::JobBusy`].
/// - Otherwise a `pending` successor is inserted and becomes the job's current
///   turn.
///
/// It never starts the turn, and never fails for a busy job or an open
/// predecessor, so it can run inside the transaction that records the answer.
pub(crate) async fn ensure_host_wait_successor_conn(
    conn: &cairn_db::turso::Connection,
    run_id: &str,
    predecessor_turn_id: &str,
    kind: HostWaitKind,
) -> DbResult<HostWaitSuccessor> {
    let Some((job_id, session_id)) = run_turn_context(conn, run_id).await? else {
        return Ok(HostWaitSuccessor::Unowned);
    };
    let own_start_reason = kind.start_reason().to_string();

    let existing = {
        let mut rows = conn
            .query(
                "SELECT id, start_reason, state FROM turns WHERE predecessor_id = ?1 \
                 ORDER BY sequence ASC LIMIT 1",
                params![predecessor_turn_id],
            )
            .await?;
        match rows.next().await? {
            Some(row) => Some((row.text(0)?, row.text(1)?, row.text(2)?)),
            None => None,
        }
    };
    if let Some((turn_id, start_reason, state)) = existing {
        if start_reason != own_start_reason {
            return Ok(HostWaitSuccessor::ForeignSuccessor { turn_id });
        }
        let state: TurnState = state.parse().map_err(DbError::Row)?;
        return Ok(HostWaitSuccessor::Successor {
            turn_id,
            inserted: false,
            state,
        });
    }

    let (predecessor_state, predecessor_start_reason) = {
        let mut rows = conn
            .query(
                "SELECT state, start_reason FROM turns WHERE id = ?1 LIMIT 1",
                params![predecessor_turn_id],
            )
            .await?;
        match rows.next().await? {
            Some(row) => (
                row.text(0)?.parse::<TurnState>().map_err(DbError::Row)?,
                row.text(1)?,
            ),
            None => return Ok(HostWaitSuccessor::Unowned),
        }
    };
    // A wait raised while a slow-path resume re-dispatched its verb finds the
    // job's current turn to be that answer's still-pending successor, which
    // stood down for this wait. It is the turn this answer continues in.
    if predecessor_state == TurnState::Pending && predecessor_start_reason == own_start_reason {
        return Ok(HostWaitSuccessor::Successor {
            turn_id: predecessor_turn_id.to_string(),
            inserted: false,
            state: TurnState::Pending,
        });
    }
    if !predecessor_state.is_terminal() {
        return Ok(HostWaitSuccessor::ProviderTurnOpen);
    }

    let active_turn_id = {
        let mut rows = conn
            .query(
                "SELECT id FROM turns WHERE job_id = ?1 AND state IN ('pending', 'running') \
                 ORDER BY sequence DESC LIMIT 1",
                params![job_id.as_str()],
            )
            .await?;
        match rows.next().await? {
            Some(row) => Some(row.text(0)?),
            None => None,
        }
    };
    if let Some(active_turn_id) = active_turn_id {
        return Ok(HostWaitSuccessor::JobBusy { active_turn_id });
    }

    let minted = ensure_successor_turn(
        conn,
        &session_id,
        &job_id,
        predecessor_turn_id,
        kind.start_reason(),
    )
    .await?;
    Ok(HostWaitSuccessor::Successor {
        turn_id: minted.turn_id,
        inserted: minted.inserted,
        state: TurnState::Pending,
    })
}

/// The inline fast path's start: adopt the answer's pending successor and start
/// it on this run. In practice it never mints -- the answer did, before it
/// broadcast -- but it goes through the same minter so the two paths cannot
/// disagree about which turn is the successor, and so a steer's turn is never
/// adopted as the answer's.
pub(crate) async fn start_host_wait_successor(
    db: &LocalDb,
    run_id: &str,
    predecessor_turn_id: &str,
    kind: HostWaitKind,
) -> DbResult<Option<SuccessorTurnUpdate>> {
    let run_id = run_id.to_string();
    let predecessor_turn_id = predecessor_turn_id.to_string();
    db.write(|conn| {
        let run_id = run_id.clone();
        let predecessor_turn_id = predecessor_turn_id.clone();
        Box::pin(async move {
            match ensure_host_wait_successor_conn(conn, &run_id, &predecessor_turn_id, kind).await?
            {
                HostWaitSuccessor::Successor {
                    turn_id, inserted, ..
                } => {
                    let started = start_turn_for_run(conn, &turn_id, &run_id).await?;
                    Ok(Some(SuccessorTurnUpdate {
                        turn_id,
                        inserted,
                        started,
                    }))
                }
                _ => Ok(None),
            }
        })
    })
    .await
}

/// Continue a job into the successor an answer minted, and make sure that turn
/// never outlives a refusal.
///
/// `continue_job_impl` adopts the turn through `preclaimed_successor_turn_id`
/// (validated as this job's, this session's, and still pending), takes a native
/// resume, and starts it on the resumed run.
///
/// Whether a successor went undelivered is never inferred from process
/// occupancy: a process reads as serving no turn while it is `Busy`, and
/// between its `end_turn()` and finalize writing the turn `complete`, so that
/// predicate would cancel a turn a sibling answer's launch delivered. Only two
/// facts mark a successor as undriven, and both are decided under the job's
/// launch lock, the lock every warm launch holds from `start_turn` through
/// prompt delivery:
///
/// - a launch that started it and then failed to deliver settles it itself,
///   before releasing the lock (`ResumeContext::unwind_preclaimed_start_as`).
///   So under the lock, a `running` successor always means a launch delivered it.
/// - one still `pending` under the lock, with no admitted cold launch owning
///   the job, was never started, and is settled here.
///
/// Either way it ends `cancelled` with the kind's end reason
/// ([`HostWaitKind::end_reason`]) and the refusal is written to the answered
/// row's `resume_error`. Otherwise another launch owns the successor (served,
/// finished, or admitted), and the result is queued for its next boundary.
///
/// The settle is diagnostic: the answer is already committed, so a failure here
/// is logged and never fails the answer.
pub(crate) async fn continue_into_host_wait_successor(
    orch: &Orchestrator,
    owning_db: &LocalDb,
    wait: &HostWaitAnswer,
    job_id: &str,
    successor_turn_id: &str,
    content: &str,
) -> HostWaitContinuation {
    let label = wait.kind.label();
    let id = wait.id.as_str();
    let context = crate::execution::jobs::ResumeContext {
        suppress_user_event: wait.suppress_user_event,
        goal_reactivation: Some(GoalReactivation::OwnedConditionResolved(
            GoalPauseOwner::UserInput {
                request_id: id.to_string(),
            },
        )),
        preclaimed_successor_turn_id: Some(successor_turn_id.to_string()),
        answers_open_tool_call: wait.answers_open_tool_call,
        unwind_preclaimed_start_as: Some(wait.kind.end_reason()),
        ..Default::default()
    };
    let end_reason = wait.kind.end_reason().to_string();
    // Only a still-pending successor is this caller's to start. One a sibling
    // answer already drove is classified below without entering the launch
    // funnel, whose own pending check sits after run selection.
    let mut launched = false;
    let refusal = match load_successor_turn(owning_db, successor_turn_id).await {
        Ok(Some((TurnState::Pending, _))) => {
            launched = true;
            match crate::execution::jobs::continue_job_impl(
                orch,
                job_id,
                Some(content),
                None,
                Some(context),
            ) {
                Ok(_) => return HostWaitContinuation::Driven,
                Err(reason) => reason,
            }
        }
        Ok(Some((state, _))) => format!("successor turn {successor_turn_id} is already {state}"),
        Ok(None) => format!("successor turn {successor_turn_id} not found"),
        Err(error) => format!("could not read successor turn {successor_turn_id}: {error}"),
    };

    let settled = match settle_undriven_successor_locked(
        orch,
        owning_db,
        job_id,
        successor_turn_id,
        &end_reason,
    ) {
        Ok(settled) => settled,
        Err(error) => {
            log::warn!(
                "{label} {id}: could not classify successor {successor_turn_id} \
                 after a refused resume ({refusal}): {error}; queueing the result"
            );
            false
        }
    };
    if settled {
        let change =
            crate::notify::turn_db_change_for_id(owning_db, successor_turn_id, "update").await;
        let _ = orch.services.emitter.emit("db-change", change);
    }
    // This call's own launch started the successor and unwound it on failure.
    let unwound = !settled
        && launched
        && matches!(
            load_successor_turn(owning_db, successor_turn_id).await,
            Ok(Some((TurnState::Cancelled, Some(reason)))) if reason == end_reason
        );
    if settled || unwound {
        record_resume_error(orch, owning_db, wait.kind, id, &refusal).await;
        log::warn!(
            "{label} {id}: answered, but the job could not continue into \
             successor {successor_turn_id} ({refusal}); settled it as {end_reason}"
        );
        return HostWaitContinuation::Refused { reason: refusal };
    }

    let queued = wait.queued_content.as_deref().unwrap_or(content);
    match crate::execution::jobs::continue_job_or_enqueue(
        orch,
        job_id,
        Some(queued),
        None,
        Some(id),
    ) {
        Ok(_) => {
            log::info!(
                "{label} {id}: successor {successor_turn_id} is owned by another \
                 launch ({refusal}); queued the result for its next boundary"
            );
            HostWaitContinuation::Queued
        }
        Err(error) => {
            let reason = format!("{refusal}; queueing the result also failed: {error}");
            record_resume_error(orch, owning_db, wait.kind, id, &reason).await;
            log::warn!("{label} {id}: answered, but the result was not delivered: {reason}");
            HostWaitContinuation::Refused { reason }
        }
    }
}

/// A successor turn's state and end reason, if it exists.
async fn load_successor_turn(
    db: &LocalDb,
    turn_id: &str,
) -> DbResult<Option<(TurnState, Option<String>)>> {
    db.query_opt(
        "SELECT state, end_reason FROM turns WHERE id = ?1",
        params![turn_id],
        |row| {
            let state = row.text(0)?.parse::<TurnState>().map_err(DbError::Row)?;
            Ok((state, row.opt_text(1)?))
        },
    )
    .await
}

/// Under the job's launch lock, settle the successor `cancelled` if it is still
/// pending and nothing owns it, and report whether it did. Owners, either of
/// which leaves it alone: an admitted cold start (in-memory launch claim), or a
/// cold continuation queued in durable admission (a `starting` run for the job).
///
/// A `running` successor is never settled here. Every host-wait launch that
/// starts one and fails unwinds it before releasing this lock, so `running`
/// means delivered, even when the serving process has already ended the turn
/// or reads as `Busy`. The compare-and-set on `pending` is belt and braces; the
/// lock is what keeps a launch out of its start window while this decides.
fn settle_undriven_successor_locked(
    orch: &Orchestrator,
    owning_db: &LocalDb,
    job_id: &str,
    turn_id: &str,
    end_reason: &str,
) -> Result<bool, String> {
    let launch_lock = orch.job_launch_lock(job_id);
    let _launch_guard = launch_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if orch.claimed_launch_run(job_id).is_some() {
        return Ok(false);
    }
    crate::storage::run_db_blocking(|| async {
        let Some((TurnState::Pending, _)) = load_successor_turn(owning_db, turn_id)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(false);
        };
        let starting_runs = owning_db
            .query_one(
                "SELECT COUNT(*) FROM runs WHERE job_id = ?1 AND status = 'starting'",
                params![job_id],
                |row| row.i64(0),
            )
            .await
            .map_err(|error| error.to_string())?;
        if starting_runs > 0 {
            return Ok(false);
        }
        let now = chrono::Utc::now().timestamp();
        let settled = owning_db
            .execute(
                "UPDATE turns SET state = 'cancelled', end_reason = ?1, ended_at = ?2, \
                 updated_at = ?2 WHERE id = ?3 AND state = 'pending'",
                params![end_reason, now, turn_id],
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(settled > 0)
    })
}

/// Record on the answered row why its answer did not resume the job, so the
/// refusal is visible where the answer is rather than only in the host log.
pub(crate) async fn record_resume_error(
    orch: &Orchestrator,
    owning_db: &LocalDb,
    kind: HostWaitKind,
    id: &str,
    reason: &str,
) {
    let sql = match kind {
        HostWaitKind::Permission => {
            "UPDATE permission_requests SET resume_error = ?2 WHERE id = ?1"
        }
        HostWaitKind::Question => "UPDATE prompts SET resume_error = ?2 WHERE id = ?1",
    };
    if let Err(error) = owning_db.execute(sql, params![id, reason]).await {
        log::warn!(
            "{} {id}: could not record resume refusal: {error}",
            kind.label()
        );
    }
    let change = match kind {
        HostWaitKind::Permission => {
            crate::notify::permission_request_db_change_for_id(owning_db, id, "update").await
        }
        HostWaitKind::Question => {
            crate::notify::prompt_db_change_for_id(owning_db, id, "update").await
        }
    };
    let _ = orch.services.emitter.emit("db-change", change);
}

pub(super) async fn run_turn_context(
    conn: &cairn_db::turso::Connection,
    run_id: &str,
) -> DbResult<Option<(String, String)>> {
    let mut rows = conn
        .query(
            "SELECT job_id, session_id FROM runs WHERE id = ?1 LIMIT 1",
            params![run_id],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| {
            let job_id = row.opt_text(0)?;
            let session_id = row.opt_text(1)?;
            Ok(job_id.zip(session_id))
        })
        .transpose()
        .map(|value| value.flatten())
}

/// Insert the predecessor's `pending` successor, or return the one it already
/// has. Errors for an open predecessor or a busy job: callers that must not
/// fail classify those first ([`ensure_host_wait_successor_conn`]).
pub(super) async fn ensure_successor_turn(
    conn: &cairn_db::turso::Connection,
    session_id: &str,
    job_id: &str,
    predecessor_turn_id: &str,
    start_reason: TurnStartReason,
) -> DbResult<SuccessorTurnUpdate> {
    let mut rows = conn
        .query(
            "
            SELECT id
            FROM turns
            WHERE predecessor_id = ?1
            ORDER BY sequence ASC
            LIMIT 1
            ",
            params![predecessor_turn_id],
        )
        .await?;
    if let Some(row) = rows.next().await? {
        return Ok(SuccessorTurnUpdate {
            turn_id: row.text(0)?,
            inserted: false,
            started: false,
        });
    }

    let predecessor_state = select_turn_state(conn, predecessor_turn_id).await?;
    if !predecessor_state.is_terminal() {
        return Err(DbError::Row(format!(
            "Predecessor turn {} is in non-terminal state {:?}",
            predecessor_turn_id, predecessor_state
        )));
    }

    let active_turn_count = count_active_job_turns(conn, job_id).await?;
    if active_turn_count > 0 {
        return Err(DbError::Row(format!(
            "Job {} already has an active turn (pending or running)",
            job_id
        )));
    }

    let sequence = next_turn_sequence(conn, session_id).await?;
    let turn_id = ids::mint_child(job_id);
    let start_reason = start_reason.to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    conn.execute(
        "
        INSERT INTO turns (
            id, session_id, run_id, job_id, sequence,
            predecessor_id, state, yield_reason, start_reason,
            created_at, started_at, ended_at, updated_at
        )
        VALUES (?1, ?2, NULL, ?3, ?4, ?5, 'pending', NULL, ?6,
                ?7, NULL, NULL, ?7)
        ",
        params![
            turn_id.as_str(),
            session_id,
            job_id,
            sequence,
            predecessor_turn_id,
            start_reason.as_str(),
            now
        ],
    )
    .await?;
    conn.execute(
        "UPDATE jobs SET current_turn_id = ?1 WHERE id = ?2",
        params![turn_id.as_str(), job_id],
    )
    .await?;

    Ok(SuccessorTurnUpdate {
        turn_id,
        inserted: true,
        started: false,
    })
}

async fn select_turn_state(
    conn: &cairn_db::turso::Connection,
    turn_id: &str,
) -> DbResult<TurnState> {
    let mut rows = conn
        .query(
            "SELECT state FROM turns WHERE id = ?1 LIMIT 1",
            params![turn_id],
        )
        .await?;
    let state = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row(format!("Predecessor turn not found: {}", turn_id)))?
        .text(0)?;
    state.parse().map_err(DbError::Row)
}

async fn count_active_job_turns(conn: &cairn_db::turso::Connection, job_id: &str) -> DbResult<i64> {
    let mut rows = conn
        .query(
            "
            SELECT COUNT(*)
            FROM turns
            WHERE job_id = ?1
              AND state IN ('pending', 'running')
            ",
            params![job_id],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row("missing active turn count".to_string()))?;
    row.i64(0)
}

async fn next_turn_sequence(conn: &cairn_db::turso::Connection, session_id: &str) -> DbResult<i64> {
    let mut rows = conn
        .query(
            "SELECT MAX(sequence) FROM turns WHERE session_id = ?1",
            params![session_id],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row("missing turn sequence".to_string()))?;
    Ok(row.opt_i64(0)?.unwrap_or(0) + 1)
}

async fn start_turn_for_run(
    conn: &cairn_db::turso::Connection,
    turn_id: &str,
    run_id: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT state FROM turns WHERE id = ?1 LIMIT 1",
            params![turn_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Err(DbError::Row(format!("Turn not found: {}", turn_id)));
    };
    let state: TurnState = row.text(0)?.parse().map_err(DbError::Row)?;
    if state != TurnState::Pending {
        return Ok(false);
    }

    let now = chrono::Utc::now().timestamp() as i32;
    conn.execute(
        "
        UPDATE turns
        SET state = 'running',
            run_id = ?1,
            started_at = ?2,
            updated_at = ?2
        WHERE id = ?3
        ",
        params![run_id, now, turn_id],
    )
    .await?;
    Ok(true)
}
