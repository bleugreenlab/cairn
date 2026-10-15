//! User-controlled message queue for the job chat composer (CAIRN-1309).
//!
//! Every row is text the operator typed at a job, which makes this table the
//! durable record of what the operator said to that job — the record the
//! watcher catch-up digest reads (CAIRN-3390). Nothing else may write here: a
//! machinery- or agent-authored message recorded as a row would be reported to
//! coordinators as the operator's own words.
//!
//! While a job's agent is running, a user-typed follow-up is persisted here
//! rather than starting a competing turn. The [`Delivery`] tag records the
//! user's chosen timing:
//!
//! - [`Delivery::Queue`] (Enter, default): delivered at turn end / on the next
//!   resume.
//! - [`Delivery::Steer`] (Cmd+Enter): delivered at the next tool boundary,
//!   mid-turn, as soon as possible.
//!
//! A row is pending while `delivered_at` is NULL. Idle and active sends use the
//! same durable row; only a claim records delivery.
//!
//! Two claim paths consume a pending row:
//! the tool-boundary claim ([`claim_steer_for_job_async`], from `dispatch`)
//! takes only `steer` rows; the turn-end/resume claim
//! ([`claim_all_for_job`], from `continue_job_impl`) takes everything still
//! pending. A `steer` that never reaches a tool boundary is therefore never
//! stranded — the resume claim sweeps it up.

use cairn_common::ids;
use cairn_db::turso::params;
use serde::{Deserialize, Serialize};

use crate::storage::{run_db_blocking, DbResult, LocalDb, RowExt};

/// Canonical delivery urgency for inbound job-bound content. Defined in
/// `models::message`; re-exported here (and aliased as [`Delivery`]) so the
/// queued-message CRUD/claim logic keeps resolving the local name.
pub use crate::models::DeliveryUrgency;

pub type Delivery = DeliveryUrgency;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueuedMessage {
    pub(crate) id: String,
    pub(crate) job_id: String,
    pub(crate) content: String,
    pub(crate) delivery: Delivery,
    pub(crate) created_at: i64,
    pub(crate) delivered_at: Option<i64>,
}

impl ClaimFilter {
    fn select_sql(self) -> String {
        let delivery_filter = match self {
            Self::All => "",
            Self::ToolBoundary => " AND delivery IN ('passive', 'steer', 'interrupt')",
        };
        format!(
            "SELECT {SELECT_COLUMNS} FROM queued_messages
             WHERE job_id = ?1 AND delivered_at IS NULL{delivery_filter}
             ORDER BY created_at ASC, rowid ASC"
        )
    }

    fn update_sql(self) -> &'static str {
        match self {
            Self::All => {
                "UPDATE queued_messages SET delivered_at = ?1
                 WHERE job_id = ?2 AND delivered_at IS NULL"
            }
            Self::ToolBoundary => {
                "UPDATE queued_messages SET delivered_at = ?1
                 WHERE job_id = ?2 AND delivered_at IS NULL
                   AND delivery IN ('passive', 'steer', 'interrupt')"
            }
        }
    }
}

fn message_from_row(row: &cairn_db::turso::Row) -> DbResult<QueuedMessage> {
    let delivery = Delivery::parse(&row.text(3)?).map_err(crate::storage::DbError::Row)?;
    Ok(QueuedMessage {
        id: row.text(0)?,
        job_id: row.text(1)?,
        content: row.text(2)?,
        delivery,
        created_at: row.i64(4)?,
        delivered_at: row.opt_i64(5)?,
    })
}

const SELECT_COLUMNS: &str = "id, job_id, content, delivery, created_at, delivered_at";

// ---------------------------------------------------------------------------
// CRUD
// ---------------------------------------------------------------------------

pub fn enqueue(
    db: &LocalDb,
    job_id: &str,
    content: &str,
    delivery: Delivery,
) -> Result<QueuedMessage, String> {
    let job_id = job_id.to_string();
    let content = content.to_string();
    run_db_blocking(move || async move { enqueue_async(db, &job_id, &content, delivery).await })
}

pub(crate) async fn enqueue_async(
    db: &LocalDb,
    job_id: &str,
    content: &str,
    delivery: Delivery,
) -> Result<QueuedMessage, String> {
    record_operator_message(db, job_id, content, delivery, None, None).await
}

pub fn enqueue_idempotent(
    db: &LocalDb,
    job_id: &str,
    content: &str,
    delivery: Delivery,
    request_id: &str,
) -> Result<QueuedMessage, String> {
    let job_id = job_id.to_string();
    let content = content.to_string();
    let request_id = request_id.to_string();
    run_db_blocking(move || async move {
        record_operator_message(db, &job_id, &content, delivery, None, Some(request_id)).await
    })
}

/// Insert one operator message for a job — pending, or already delivered — and
/// give the job's watchers their catch-up copy either way.
async fn record_operator_message(
    db: &LocalDb,
    job_id: &str,
    content: &str,
    delivery: Delivery,
    delivered_at: Option<i64>,
    request_id: Option<String>,
) -> Result<QueuedMessage, String> {
    let timing_started = std::time::Instant::now();
    let id = request_id.unwrap_or_else(|| ids::mint_child(job_id));
    let job_id = job_id.to_string();
    let content = content.to_string();
    let now = chrono::Utc::now().timestamp();

    let recorded = db
        .write(|conn| {
            let id = id.clone();
            let job_id = job_id.clone();
            let content = content.clone();
            Box::pin(async move {
                let sql = format!("SELECT {SELECT_COLUMNS} FROM queued_messages WHERE id = ?1");
                let mut rows = conn.query(&sql, params![id.as_str()]).await?;
                if let Some(row) = rows.next().await? {
                    let existing = message_from_row(&row)?;
                    if existing.job_id != job_id
                        || existing.content != content
                        || existing.delivery != delivery
                    {
                        return Err(crate::storage::DbError::Row(
                            "queued-message request id was reused for different content"
                                .to_string(),
                        ));
                    }
                    return Ok((existing, false));
                }
                conn.execute(
                    "INSERT INTO queued_messages
                 (id, job_id, content, delivery, created_at, delivered_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        id.as_str(),
                        job_id.as_str(),
                        content.as_str(),
                        delivery.as_str(),
                        now,
                        delivered_at
                    ],
                )
                .await?;
                Ok((
                    QueuedMessage {
                        id,
                        job_id,
                        content,
                        delivery,
                        created_at: now,
                        delivered_at,
                    },
                    true,
                ))
            })
        })
        .await
        .map_err(|error| format!("Failed to enqueue message: {error}"));

    // Every row in this table is text the operator typed at a job, so this is
    // where an operator message enters the core. Give the jobs watching that
    // node's issue their passive catch-up copy (CAIRN-3342); without it the
    // operator is the one participant whose interventions a coordinator never
    // sees. Best-effort — a failure here must not lose the operator's message.
    if let Ok((message, inserted)) = &recorded {
        if *inserted {
            let mut event = crate::resume_timing::ResumeTimingEvent::new("queue_enqueue_end")
                .elapsed(timing_started);
            event.job_id = Some(&message.job_id);
            event.queued_message_id = Some(&message.id);
            event.mode = Some(message.delivery.as_str());
            event.count = Some(1);
            event.bytes = Some(message.content.len());
            event.emit();
            if let Err(error) =
                crate::orchestrator::attention_delivery::create_catchup_pushes_for_job(db, &job_id)
                    .await
            {
                log::warn!("catch-up push creation for an operator message failed: {error}");
            }
        }
    }

    recorded.map(|(message, _)| message)
}

/// Pending (undelivered) queued messages for a job, oldest first.
pub fn list_pending_for_job(db: &LocalDb, job_id: &str) -> Result<Vec<QueuedMessage>, String> {
    let job_id = job_id.to_string();
    run_db_blocking(move || async move { list_pending_for_job_async(db, &job_id).await })
}

async fn list_pending_for_job_async(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<QueuedMessage>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let sql = format!(
                "SELECT {SELECT_COLUMNS} FROM queued_messages
                 WHERE job_id = ?1 AND delivered_at IS NULL
                 ORDER BY created_at ASC, rowid ASC"
            );
            let mut rows = conn.query(&sql, params![job_id.as_str()]).await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(message_from_row(&row)?);
            }
            Ok(out)
        })
    })
    .await
    .map_err(|error| format!("Failed to list queued messages: {error}"))
}

/// Edit a pending queued message in place. Delivered rows are left untouched.
pub fn update_content(db: &LocalDb, id: &str, content: &str) -> Result<bool, String> {
    let id = id.to_string();
    let content = content.to_string();
    run_db_blocking(move || async move {
        db.execute(
            "UPDATE queued_messages SET content = ?1
             WHERE id = ?2 AND delivered_at IS NULL",
            params![content.as_str(), id.as_str()],
        )
        .await
        .map(|affected| affected == 1)
        .map_err(|error| format!("Failed to update queued message: {error}"))
    })
}

/// Promote/demote a pending queued message's delivery timing. Returns the
/// owning job id so callers can apply the delivery ladder immediately (for
/// example, promoting a chip to `interrupt` must stop the active turn now).
pub fn set_delivery(db: &LocalDb, id: &str, delivery: Delivery) -> Result<String, String> {
    let id = id.to_string();
    run_db_blocking(move || async move {
        db.write(|conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT job_id FROM queued_messages
                         WHERE id = ?1 AND delivered_at IS NULL",
                        params![id.as_str()],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Err(crate::storage::DbError::Row(
                        "pending queued message not found".to_string(),
                    ));
                };
                let job_id = row.text(0)?;
                conn.execute(
                    "UPDATE queued_messages SET delivery = ?1
                     WHERE id = ?2 AND delivered_at IS NULL",
                    params![delivery.as_str(), id.as_str()],
                )
                .await?;
                Ok(job_id)
            })
        })
        .await
        .map_err(|error| format!("Failed to set queued message delivery: {error}"))
    })
}

/// Withdraw a pending queued message. Delivered rows are left untouched.
///
/// The guard is load-bearing rather than tidy. The composer deletes a chip from
/// a list it read a moment ago, so a delete can always reach a row the child
/// claimed in between — and once claimed, the operator has *said* that message:
/// the child has read it, and this row is the only record that a human authored
/// it, which the watchers' catch-up digest reads. An unguarded delete would race
/// the claim and erase a genuine operator message from every account of it. A
/// delivered row is silently left alone, as it is for an edit: the operator's
/// intent to withdraw simply arrived too late, and the chip is gone from the
/// pending strip either way.
pub fn delete(db: &LocalDb, id: &str) -> Result<(), String> {
    let id = id.to_string();
    run_db_blocking(move || async move {
        db.write(|conn| {
            let id = id.clone();
            Box::pin(async move {
                conn.execute(
                    "DELETE FROM queued_messages WHERE id = ?1 AND delivered_at IS NULL",
                    params![id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|error| format!("Failed to delete queued message: {error}"))
    })
}

// ---------------------------------------------------------------------------
// Claim paths
// ---------------------------------------------------------------------------

/// Atomically claim pending `steer` messages for a job (tool-boundary path).
///
/// Single MVCC transaction: SELECT the pending `steer` rows, then stamp the
/// same set `delivered_at = now`. A concurrent claim (a following tool boundary,
/// or the turn-end resume claim) cannot double-deliver the same row.
pub async fn claim_steer_for_job_async(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<QueuedMessage>, String> {
    claim_for_job_inner(db, job_id, ClaimFilter::ToolBoundary).await
}

/// Atomically claim pending rows that should be delivered at a tool boundary:
/// passive rides along, steer lands promptly, and interrupt leftovers degrade to
/// steer if their backend interrupt raced a natural tool/turn boundary.
pub(crate) async fn claim_tool_boundary_for_job_async(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<QueuedMessage>, String> {
    claim_for_job_inner(db, job_id, ClaimFilter::ToolBoundary).await
}

/// Atomically claim ALL pending messages for a job (turn-end / resume path),
/// regardless of delivery tag. This is what sweeps up a `steer` row that never
/// reached a tool boundary, plus every `queue` row.
pub(crate) fn claim_all_for_job(db: &LocalDb, job_id: &str) -> Result<Vec<QueuedMessage>, String> {
    let job_id = job_id.to_string();
    run_db_blocking(move || async move { claim_all_for_job_async(db, &job_id).await })
}

/// Whether any row a tool boundary would claim is pending for `job_id`,
/// without claiming it.
pub(crate) async fn peek_tool_boundary_pending_async(
    db: &LocalDb,
    job_id: &str,
) -> Result<bool, String> {
    db.query_all(
        ClaimFilter::ToolBoundary.select_sql(),
        params![job_id.to_string()],
        message_from_row,
    )
    .await
    .map(|pending| !pending.is_empty())
    .map_err(|error| format!("Failed to inspect queued messages: {error}"))
}

pub(crate) async fn claim_all_for_job_async(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<QueuedMessage>, String> {
    claim_for_job_inner(db, job_id, ClaimFilter::All).await
}

/// Return exact rows claimed by a resume to the pending queue when launch
/// preparation fails before their canonical transcript events are stored.
pub(crate) fn restore_claimed(db: &LocalDb, ids: &[String]) -> Result<(), String> {
    let ids = ids.to_vec();
    run_db_blocking(move || async move { restore_claimed_async(db, &ids).await })
}

async fn restore_claimed_async(db: &LocalDb, ids: &[String]) -> Result<(), String> {
    let ids = ids.to_vec();
    db.write(|conn| {
        let ids = ids.clone();
        Box::pin(async move {
            for id in ids {
                conn.execute(
                    "UPDATE queued_messages SET delivered_at = NULL WHERE id = ?1",
                    params![id.as_str()],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("Failed to restore claimed queued messages: {error}"))
}

#[derive(Clone, Copy)]
enum ClaimFilter {
    All,
    ToolBoundary,
}

async fn claim_for_job_inner(
    db: &LocalDb,
    job_id: &str,
    filter: ClaimFilter,
) -> Result<Vec<QueuedMessage>, String> {
    let timing_started = std::time::Instant::now();
    let mode = match filter {
        ClaimFilter::All => "all",
        ClaimFilter::ToolBoundary => "tool_boundary",
    };
    let mut event = crate::resume_timing::ResumeTimingEvent::new("queue_claim_start");
    event.job_id = Some(job_id);
    event.mode = Some(mode);
    event.emit();
    let job_id = job_id.to_string();
    let now = chrono::Utc::now().timestamp();
    let pending = db
        .query_all(
            filter.select_sql(),
            params![job_id.as_str()],
            message_from_row,
        )
        .await
        .map_err(|error| format!("Failed to inspect queued messages: {error}"))?;
    if pending.is_empty() {
        let mut event =
            crate::resume_timing::ResumeTimingEvent::new("queue_claim_end").elapsed(timing_started);
        event.job_id = Some(&job_id);
        event.mode = Some(mode);
        event.count = Some(0);
        event.bytes = Some(0);
        event.emit();
        return Ok(Vec::new());
    }
    db.write(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let select_sql = filter.select_sql();
            let update_sql = filter.update_sql();
            let mut rows = conn.query(&select_sql, params![job_id.as_str()]).await?;
            let mut messages = Vec::new();
            while let Some(row) = rows.next().await? {
                messages.push(message_from_row(&row)?);
            }
            drop(rows);

            if !messages.is_empty() {
                conn.execute(&update_sql, params![now, job_id.as_str()])
                    .await?;
                for msg in messages.iter_mut() {
                    msg.delivered_at = Some(now);
                }
            }
            Ok(messages)
        })
    })
    .await
    .inspect(|messages| {
        let mut event =
            crate::resume_timing::ResumeTimingEvent::new("queue_claim_end").elapsed(timing_started);
        event.job_id = Some(&job_id);
        event.mode = Some(mode);
        event.count = Some(messages.len());
        event.bytes = Some(messages.iter().map(|message| message.content.len()).sum());
        event.emit();
    })
    .map_err(|error| format!("Failed to claim queued messages: {error}"))
}

pub async fn peek_pending_count_for_job_async(db: &LocalDb, job_id: &str) -> Result<usize, String> {
    Ok(list_pending_for_job_async(db, job_id).await?.len())
}

pub(crate) fn peek_waking_pending_count_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<usize, String> {
    let job_id = job_id.to_string();
    run_db_blocking(
        move || async move { peek_waking_pending_count_for_job_async(db, &job_id).await },
    )
}

async fn peek_waking_pending_count_for_job_async(
    db: &LocalDb,
    job_id: &str,
) -> Result<usize, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*) FROM queued_messages
                     WHERE job_id = ?1 AND delivered_at IS NULL
                       AND delivery IN ('queue', 'steer', 'interrupt')",
                    params![job_id.as_str()],
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| crate::storage::DbError::Row("missing queued count".to_string()))?;
            Ok(row.i64(0)? as usize)
        })
    })
    .await
    .map_err(|error| format!("Failed to count waking queued messages: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("queued-messages.db").await
    }

    #[tokio::test(flavor = "current_thread")]
    async fn empty_tool_boundary_claim_does_not_open_a_write_transaction() {
        let db = migrated_db().await;
        let writes_before = db.write_transaction_count();

        let claimed = claim_tool_boundary_for_job_async(&db, "job-a")
            .await
            .unwrap();

        assert!(claimed.is_empty());
        assert_eq!(db.write_transaction_count(), writes_before);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_launch_can_restore_exact_claimed_rows() {
        let db = migrated_db().await;
        let claimed = enqueue_async(&db, "job-a", "retry me", Delivery::Queue)
            .await
            .unwrap();
        claim_all_for_job_async(&db, "job-a").await.unwrap();

        restore_claimed(&db, std::slice::from_ref(&claimed.id)).unwrap();

        let retried = claim_all_for_job_async(&db, "job-a").await.unwrap();
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].id, claimed.id);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn enqueue_then_list_returns_pending_in_order() {
        let db = migrated_db().await;
        enqueue_async(&db, "job-a", "first", Delivery::Queue)
            .await
            .unwrap();
        enqueue_async(&db, "job-a", "second", Delivery::Steer)
            .await
            .unwrap();
        enqueue_async(&db, "job-b", "other", Delivery::Queue)
            .await
            .unwrap();

        let pending = list_pending_for_job_async(&db, "job-a").await.unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].content, "first");
        assert_eq!(pending[1].content, "second");
        assert_eq!(pending[1].delivery, Delivery::Steer);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn request_id_is_idempotent_without_conflating_equal_content() {
        let db = migrated_db().await;
        let first =
            enqueue_idempotent(&db, "job-a", "retry me", Delivery::Queue, "request-1").unwrap();
        let retry =
            enqueue_idempotent(&db, "job-a", "retry me", Delivery::Queue, "request-1").unwrap();
        enqueue_idempotent(&db, "job-a", "retry me", Delivery::Queue, "request-2").unwrap();

        assert_eq!(retry.id, first.id);
        let pending = list_pending_for_job_async(&db, "job-a").await.unwrap();
        assert_eq!(pending.len(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tool_boundary_claim_takes_passive_steer_and_interrupt_but_skips_queue() {
        let db = migrated_db().await;
        enqueue_async(&db, "job-a", "q", Delivery::Queue)
            .await
            .unwrap();
        let passive = enqueue_async(&db, "job-a", "p", Delivery::Passive)
            .await
            .unwrap();
        let steer = enqueue_async(&db, "job-a", "s", Delivery::Steer)
            .await
            .unwrap();
        let interrupt = enqueue_async(&db, "job-a", "i", Delivery::Interrupt)
            .await
            .unwrap();

        let claimed = claim_tool_boundary_for_job_async(&db, "job-a")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 3, "tool boundary skips only queue rows");
        assert_eq!(claimed[0].id, passive.id);
        assert_eq!(claimed[1].id, steer.id);
        assert_eq!(claimed[2].id, interrupt.id);
        assert!(claimed.iter().all(|m| m.delivered_at.is_some()));

        // The queue row is still pending and claimable by the resume sweep.
        let remaining = list_pending_for_job_async(&db, "job-a").await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].content, "q");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn claim_all_sweeps_steer_and_queue_and_is_idempotent() {
        let db = migrated_db().await;
        enqueue_async(&db, "job-a", "q", Delivery::Queue)
            .await
            .unwrap();
        enqueue_async(&db, "job-a", "s", Delivery::Steer)
            .await
            .unwrap();

        let claimed = claim_all_for_job_async(&db, "job-a").await.unwrap();
        assert_eq!(claimed.len(), 2, "resume sweep takes everything pending");
        assert!(claimed.iter().all(|m| m.delivered_at.is_some()));

        let again = claim_all_for_job_async(&db, "job-a").await.unwrap();
        assert!(again.is_empty(), "delivered rows are not re-claimed");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn steer_not_stranded_is_swept_by_resume_claim() {
        // A steer row that never reached a tool boundary is still claimed by the
        // turn-end/resume sweep.
        let db = migrated_db().await;
        enqueue_async(&db, "job-a", "s", Delivery::Steer)
            .await
            .unwrap();
        let claimed = claim_all_for_job_async(&db, "job-a").await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].content, "s");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn update_set_delivery_and_delete_affect_only_pending() {
        let db = migrated_db().await;
        let m = enqueue_async(&db, "job-a", "orig", Delivery::Queue)
            .await
            .unwrap();

        assert!(update_content(&db, &m.id, "edited").unwrap());
        set_delivery(&db, &m.id, Delivery::Steer).unwrap();
        let pending = list_pending_for_job_async(&db, "job-a").await.unwrap();
        assert_eq!(pending[0].content, "edited");
        assert_eq!(pending[0].delivery, Delivery::Steer);

        delete(&db, &m.id).unwrap();
        let pending = list_pending_for_job_async(&db, "job-a").await.unwrap();
        assert!(pending.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn update_reports_when_delivery_wins_the_race() {
        let db = migrated_db().await;
        let m = enqueue_async(&db, "job-a", "original", Delivery::Interrupt)
            .await
            .unwrap();
        claim_all_for_job_async(&db, "job-a").await.unwrap();

        assert!(!update_content(&db, &m.id, "revision").unwrap());
        let stored = db
            .query_one(
                "SELECT content FROM queued_messages WHERE id = ?1",
                params![m.id.as_str()],
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(stored, "original");
    }

    /// The composer deletes from a list it read a moment ago, so a delete can
    /// always reach a row the child claimed in between. A claimed message has
    /// been said — the child read it — and this row is the only record that a
    /// human authored it, so the late withdrawal must not erase it. Driven
    /// through the real claim, since the race under test is the claim's own.
    #[tokio::test(flavor = "current_thread")]
    async fn a_delete_racing_the_claim_does_not_erase_a_delivered_message() {
        let db = migrated_db().await;
        let m = enqueue_async(
            &db,
            "job-a",
            "also ship the trusted producer",
            Delivery::Queue,
        )
        .await
        .unwrap();
        claim_all_for_job_async(&db, "job-a").await.unwrap();

        delete(&db, &m.id).unwrap();

        let surviving = db
            .query_one(
                "SELECT COUNT(*) FROM queued_messages WHERE id = ?1",
                params![m.id.as_str()],
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(
            surviving, 1,
            "a message the child has already read stays on the record"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn peek_count_matches_pending() {
        let db = migrated_db().await;
        enqueue_async(&db, "job-a", "a", Delivery::Queue)
            .await
            .unwrap();
        enqueue_async(&db, "job-a", "b", Delivery::Steer)
            .await
            .unwrap();
        enqueue_async(&db, "job-a", "c", Delivery::Passive)
            .await
            .unwrap();
        assert_eq!(
            peek_pending_count_for_job_async(&db, "job-a")
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            peek_waking_pending_count_for_job_async(&db, "job-a")
                .await
                .unwrap(),
            2,
            "passive rows do not wake idle jobs"
        );
        claim_all_for_job_async(&db, "job-a").await.unwrap();
        assert_eq!(
            peek_pending_count_for_job_async(&db, "job-a")
                .await
                .unwrap(),
            0
        );
    }
}
