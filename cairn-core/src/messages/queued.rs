//! User-controlled message queue for the job chat composer (CAIRN-1309).
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
//! A row is pending while `delivered_at` is NULL. Two claim paths consume it:
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
    let id = ids::mint_child(job_id);
    let job_id = job_id.to_string();
    let content = content.to_string();
    let now = chrono::Utc::now().timestamp();

    db.write(|conn| {
        let id = id.clone();
        let job_id = job_id.clone();
        let content = content.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO queued_messages
                 (id, job_id, content, delivery, created_at, delivered_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, NULL)",
                params![
                    id.as_str(),
                    job_id.as_str(),
                    content.as_str(),
                    delivery.as_str(),
                    now
                ],
            )
            .await?;
            Ok(QueuedMessage {
                id,
                job_id,
                content,
                delivery,
                created_at: now,
                delivered_at: None,
            })
        })
    })
    .await
    .map_err(|error| format!("Failed to enqueue message: {error}"))
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
pub fn update_content(db: &LocalDb, id: &str, content: &str) -> Result<(), String> {
    let id = id.to_string();
    let content = content.to_string();
    run_db_blocking(move || async move {
        db.execute(
            "UPDATE queued_messages SET content = ?1
             WHERE id = ?2 AND delivered_at IS NULL",
            params![content.as_str(), id.as_str()],
        )
        .await
        .map(|_| ())
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

pub fn delete(db: &LocalDb, id: &str) -> Result<(), String> {
    let id = id.to_string();
    run_db_blocking(move || async move {
        db.write(|conn| {
            let id = id.clone();
            Box::pin(async move {
                conn.execute(
                    "DELETE FROM queued_messages WHERE id = ?1",
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

pub(crate) async fn claim_all_for_job_async(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<QueuedMessage>, String> {
    claim_for_job_inner(db, job_id, ClaimFilter::All).await
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
    let job_id = job_id.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let (select_sql, update_sql) = match filter {
                ClaimFilter::ToolBoundary => (
                    format!(
                        "SELECT {SELECT_COLUMNS} FROM queued_messages
                         WHERE job_id = ?1 AND delivered_at IS NULL
                           AND delivery IN ('passive', 'steer', 'interrupt')
                         ORDER BY created_at ASC, rowid ASC"
                    ),
                    "UPDATE queued_messages SET delivered_at = ?1
                     WHERE job_id = ?2 AND delivered_at IS NULL
                       AND delivery IN ('passive', 'steer', 'interrupt')"
                        .to_string(),
                ),
                ClaimFilter::All => (
                    format!(
                        "SELECT {SELECT_COLUMNS} FROM queued_messages
                         WHERE job_id = ?1 AND delivered_at IS NULL
                         ORDER BY created_at ASC, rowid ASC"
                    ),
                    "UPDATE queued_messages SET delivered_at = ?1
                     WHERE job_id = ?2 AND delivered_at IS NULL"
                        .to_string(),
                ),
            };
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

        update_content(&db, &m.id, "edited").unwrap();
        set_delivery(&db, &m.id, Delivery::Steer).unwrap();
        let pending = list_pending_for_job_async(&db, "job-a").await.unwrap();
        assert_eq!(pending[0].content, "edited");
        assert_eq!(pending[0].delivery, Delivery::Steer);

        delete(&db, &m.id).unwrap();
        let pending = list_pending_for_job_async(&db, "job-a").await.unwrap();
        assert!(pending.is_empty());
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
