//! The one re-prompt a delegated task gets when a work turn ends without its
//! `return` (cairn/5783).
//!
//! A task's contract is to return. A completed work turn that leaves the
//! packet's artifact unwritten derives the task Blocked, which on its own parks
//! the task, and any parent waiting on it, until a person notices. This seam
//! makes the missing return a protocol step instead. The first such turn end
//! queues one system re-prompt. A later work turn that saw the re-prompt and
//! still ended without the artifact marks the task abandoned, which the
//! job-status derivation reads as Failed, so the parent resumes with a result
//! that says plainly no result came.
//!
//! Both facts live on the job row (`return_followup_turn_id`,
//! `return_abandoned_at`) and the re-prompt is written in the same transaction
//! as the first, so a restart or a replayed turn end can neither repeat the
//! re-prompt nor abandon a task that was never asked.
//!
//! Only the task's own live delegated children hold this off: their completion
//! wakes it. Child issues a task files do not. They belong to the node the task
//! acted for (`owning_node_job_for_run`, CAIRN-1302) and wake that node, never
//! the task, so waiting on them would leave the task parked on something that
//! cannot wake it.

use crate::messages::queued::DeliveryUrgency;
use crate::models::DelegationOrigin;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, RowExt};
use cairn_db::turso::{params, Connection};

/// What a turn end did about a task's missing `return`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MissingReturn {
    /// Nothing is owed, or nothing is owed yet: the job is not a task that
    /// ended a completed work turn without its artifact, or it is still waiting
    /// on live children of its own.
    NotOwed,
    /// The re-prompt was queued against the turn that just ended.
    Asked,
    /// The re-prompt is out, and no work turn that saw it has ended yet. A
    /// replay of the turn end that sent it lands here too.
    AwaitingReply,
    /// A work turn that saw the re-prompt also ended without the artifact. The
    /// task is now abandoned and must be recomputed to derive its failure.
    Abandoned,
}

/// Settle a delegated task's turn end that may have left its `return`
/// unwritten. Call after the turn end's recompute, which is what derives the
/// missing output Blocked, and before the idle flush, which delivers the
/// re-prompt this may queue.
pub(crate) fn settle_missing_return(
    orch: &Orchestrator,
    job_id: &str,
    run_id: &str,
) -> Result<MissingReturn, String> {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    let run_id = run_id.to_string();
    crate::storage::run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
        db.write(|conn| {
            let job_id = job_id.clone();
            let run_id = run_id.clone();
            Box::pin(async move { settle_conn(conn, &job_id, &run_id).await })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

fn reprompt_idempotency_key(job_id: &str) -> String {
    format!("return-followup:{job_id}")
}

fn reprompt_text(artifact_name: &str) -> String {
    format!(
        "Your task ended without calling `return`, so your parent has received nothing. Call \
         `return` now with your result (write it to `cairn:~/{artifact_name}`), or with an \
         account of why you could not finish."
    )
}

/// A job's latest work turn, the memory review excluded, as the job-status
/// derivation orders it.
struct WorkTurn {
    id: String,
    state: String,
    /// The turn, or the run it ran on, was stopped by the operator.
    user_stopped: bool,
    created_at: i64,
    sequence: i64,
}

async fn latest_work_turn(conn: &Connection, job_id: &str) -> DbResult<Option<WorkTurn>> {
    let mut rows = conn
        .query(
            "SELECT t.id, t.state,
                    COALESCE(t.end_reason, '') = 'user_stop'
                      OR COALESCE(r.exit_reason, '') = 'user_stop',
                    t.created_at, t.sequence
             FROM turns t LEFT JOIN runs r ON r.id = t.run_id
             WHERE t.job_id = ?1 AND t.start_reason != 'memory_review'
             ORDER BY t.created_at DESC, t.sequence DESC
             LIMIT 1",
            params![job_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    Ok(Some(WorkTurn {
        id: row.text(0)?,
        state: row.text(1)?,
        user_stopped: row.i64(2)? != 0,
        created_at: row.i64(3)?,
        sequence: row.i64(4)?,
    }))
}

async fn exists(
    conn: &Connection,
    sql: &str,
    args: impl cairn_db::turso::IntoParams,
) -> DbResult<bool> {
    let mut rows = conn.query(sql, args).await?;
    Ok(rows.next().await?.is_some())
}

/// Whether the latest work turn saw the re-prompt: it is the turn the
/// re-prompt's push was delivered into, or a later one. An undelivered push
/// was never seen, whatever turns have ended since it was queued.
async fn reprompt_seen_by(conn: &Connection, job_id: &str, latest: &WorkTurn) -> DbResult<bool> {
    let message_id =
        crate::messages::delivery::system_direct_once_message_id(&reprompt_idempotency_key(job_id));
    let key = crate::orchestrator::attention_push::direct_push_key(&message_id, None);
    let mut rows = conn
        .query(
            "SELECT e.turn_id, t.created_at, t.sequence
             FROM attention_pushes ap
             JOIN events e ON e.id = ap.delivered_event_id
             LEFT JOIN turns t ON t.id = e.turn_id
             WHERE ap.recipient = ?1 AND ap.key = ?2
             LIMIT 1",
            params![job_id, key.as_str()],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(false);
    };
    Ok(match (row.opt_text(0)?, row.opt_i64(1)?, row.opt_i64(2)?) {
        (Some(turn_id), _, _) if turn_id == latest.id => true,
        (Some(_), Some(created_at), Some(sequence)) => {
            (created_at, sequence) < (latest.created_at, latest.sequence)
        }
        // Delivered by an event that names no surviving turn: the delivery
        // itself is the evidence, and it preceded the turn that just ended.
        _ => true,
    })
}

async fn settle_conn(conn: &Connection, job_id: &str, run_id: &str) -> DbResult<MissingReturn> {
    let mut rows = conn
        .query(
            "SELECT status, execution_id, return_followup_turn_id, return_abandoned_at IS NOT NULL
             FROM jobs WHERE id = ?1",
            params![job_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(MissingReturn::NotOwed);
    };
    let (status, execution_id, followup_turn_id, abandoned) = (
        row.text(0)?,
        row.opt_text(1)?,
        row.opt_text(2)?,
        row.i64(3)? != 0,
    );
    drop(rows);
    // `blocked` is the derivation's own verdict that a completed turn left a
    // declared output unwritten (or that a confirm gate is open, which the
    // artifact check below rules out). Reading it rather than re-deriving the
    // contract keeps this seam from ever disagreeing with the job's status.
    if status != "blocked" || abandoned {
        return Ok(MissingReturn::NotOwed);
    }
    let Some(execution_id) = execution_id else {
        return Ok(MissingReturn::NotOwed);
    };
    // Only a delegated task owes a `return`. Calls and workflows are one-shot
    // and reaped after their turn; a job with no packet is not a task at all.
    let Some(packet) =
        cairn_workflow::delegated_packets::find_by_result_job(conn, &execution_id, job_id).await?
    else {
        return Ok(MissingReturn::NotOwed);
    };
    if packet.origin == DelegationOrigin::CallTool {
        return Ok(MissingReturn::NotOwed);
    }
    let artifact_name = packet.output_contract.artifact_name();
    if exists(
        conn,
        "SELECT 1 FROM artifacts WHERE job_id = ?1 AND output_name = ?2 LIMIT 1",
        params![job_id, artifact_name.as_str()],
    )
    .await?
    {
        return Ok(MissingReturn::NotOwed);
    }
    // A turn that yielded on a wait, failed, or was stopped by the operator did
    // not end normally, and owes nothing yet.
    let Some(latest) = latest_work_turn(conn, job_id).await? else {
        return Ok(MissingReturn::NotOwed);
    };
    if latest.state != "complete" || latest.user_stopped {
        return Ok(MissingReturn::NotOwed);
    }
    // A task whose own delegated children are still live is waiting on them,
    // and their completion wakes it to finish.
    if exists(
        conn,
        "SELECT 1 FROM jobs WHERE parent_job_id = ?1
           AND status NOT IN ('complete', 'failed', 'cancelled') LIMIT 1",
        params![job_id],
    )
    .await?
    {
        return Ok(MissingReturn::NotOwed);
    }

    let now = chrono::Utc::now().timestamp();
    match followup_turn_id {
        None => {
            let claimed = conn
                .execute(
                    "UPDATE jobs SET return_followup_turn_id = ?1, updated_at = ?2
                     WHERE id = ?3 AND return_followup_turn_id IS NULL",
                    params![latest.id.as_str(), now, job_id],
                )
                .await?;
            if claimed == 0 {
                return Ok(MissingReturn::AwaitingReply);
            }
            crate::messages::delivery::insert_system_direct_push_once_conn(
                conn,
                job_id,
                run_id,
                &reprompt_text(&artifact_name),
                DeliveryUrgency::Queue,
                &reprompt_idempotency_key(job_id),
            )
            .await?;
            Ok(MissingReturn::Asked)
        }
        Some(asked_at) if asked_at == latest.id => Ok(MissingReturn::AwaitingReply),
        Some(_) => {
            if !reprompt_seen_by(conn, job_id, &latest).await? {
                return Ok(MissingReturn::AwaitingReply);
            }
            let marked = conn
                .execute(
                    "UPDATE jobs SET return_abandoned_at = ?1, updated_at = ?1
                     WHERE id = ?2 AND return_abandoned_at IS NULL",
                    params![now, job_id],
                )
                .await?;
            Ok(if marked == 0 {
                MissingReturn::NotOwed
            } else {
                MissingReturn::Abandoned
            })
        }
    }
}
