//! Durable effect outbox for crash-safe DAG advancement.
//!
//! Provides functions to drain pending outbox entries, mark them as
//! done/failed, and garbage-collect old processed entries.

use std::future::Future;
use std::sync::Arc;

use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_db::turso::{params, Connection};

/// A pending outbox entry ready for processing.
#[derive(Debug, Clone)]
pub struct OutboxEntry {
    pub id: String,
    pub kind: String,
    pub dedupe_key: String,
    pub(crate) payload_json: String,
}

pub async fn drain_pending_async(db: &LocalDb) -> DbResult<Vec<OutboxEntry>> {
    db.write(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, kind, dedupe_key, payload_json
                     FROM effect_outbox
                     WHERE state = 'pending'
                     ORDER BY created_at ASC",
                    (),
                )
                .await?;

            let mut entries = Vec::new();
            while let Some(row) = rows.next().await? {
                entries.push(OutboxEntry {
                    id: row.text(0)?,
                    kind: row.text(1)?,
                    dedupe_key: row.text(2)?,
                    payload_json: row.text(3)?,
                });
            }

            if entries.is_empty() {
                return Ok(entries);
            }

            let now = chrono::Utc::now().timestamp() as i32;
            for entry in &entries {
                conn.execute(
                    "UPDATE effect_outbox
                     SET state = 'running',
                         attempts = attempts + 1,
                         updated_at = ?1
                     WHERE id = ?2",
                    params![now, entry.id.as_str()],
                )
                .await?;
            }

            Ok(entries)
        })
    })
    .await
}

/// Mark an outbox entry as successfully processed.
pub(crate) fn mark_done(db: Arc<LocalDb>, id: &str) {
    let id = id.to_string();
    if let Err(error) =
        block_on_outbox_db(
            async move { mark_done_async(&db, &id).await.map_err(|e| e.to_string()) },
        )
    {
        log::warn!("Failed to mark outbox entry done: {}", error);
    }
}

async fn mark_done_async(db: &LocalDb, id: &str) -> DbResult<()> {
    db.write(|conn| {
        let id = id.to_string();
        Box::pin(async move { mark_done_conn(conn, &id).await })
    })
    .await
}

async fn mark_done_conn(conn: &Connection, id: &str) -> DbResult<()> {
    let now = chrono::Utc::now().timestamp() as i32;
    conn.execute(
        "UPDATE effect_outbox
         SET state = 'done',
             updated_at = ?1
         WHERE id = ?2",
        params![now, id],
    )
    .await?;
    Ok(())
}

/// Mark an outbox entry as failed with an error message.
pub(crate) fn mark_failed(db: Arc<LocalDb>, id: &str, error: &str) {
    let id = id.to_string();
    let error = error.to_string();
    if let Err(mark_error) = block_on_outbox_db(async move {
        mark_failed_async(&db, &id, &error)
            .await
            .map_err(|e| e.to_string())
    }) {
        log::warn!("Failed to mark outbox entry failed: {}", mark_error);
    }
}

pub async fn mark_failed_async(db: &LocalDb, id: &str, error: &str) -> DbResult<()> {
    db.write(|conn| {
        let id = id.to_string();
        let error = error.to_string();
        Box::pin(async move { mark_failed_conn(conn, &id, &error).await })
    })
    .await
}

async fn mark_failed_conn(conn: &Connection, id: &str, error: &str) -> DbResult<()> {
    let now = chrono::Utc::now().timestamp() as i32;
    conn.execute(
        "UPDATE effect_outbox
         SET state = 'failed',
             last_error = ?1,
             updated_at = ?2
         WHERE id = ?3",
        params![error, now, id],
    )
    .await?;
    Ok(())
}

pub async fn mark_done_by_key_async(db: &LocalDb, kind: &str, dedupe_key: &str) -> DbResult<()> {
    db.write(|conn| {
        let kind = kind.to_string();
        let dedupe_key = dedupe_key.to_string();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp() as i32;
            conn.execute(
                "UPDATE effect_outbox
                 SET state = 'done',
                     updated_at = ?1
                 WHERE kind = ?2
                   AND dedupe_key = ?3
                   AND state IN ('running', 'pending')",
                params![now, kind.as_str(), dedupe_key.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

/// Insert a pending outbox entry with a payload.
pub(crate) fn insert_pending_with_payload(
    db: Arc<LocalDb>,
    kind: &str,
    dedupe_key: &str,
    payload_json: &str,
) -> Result<String, String> {
    let kind = kind.to_string();
    let dedupe_key = dedupe_key.to_string();
    let payload_json = payload_json.to_string();
    block_on_outbox_db(async move {
        insert_pending_with_payload_async(&db, &kind, &dedupe_key, &payload_json)
            .await
            .map_err(|e| e.to_string())
    })
}

pub(crate) async fn insert_pending_with_payload_async(
    db: &LocalDb,
    kind: &str,
    dedupe_key: &str,
    payload_json: &str,
) -> DbResult<String> {
    db.write(|conn| {
        let kind = kind.to_string();
        let dedupe_key = dedupe_key.to_string();
        let payload_json = payload_json.to_string();
        Box::pin(async move {
            insert_pending_with_payload_conn(conn, &kind, &dedupe_key, &payload_json).await
        })
    })
    .await
}

async fn insert_pending_with_payload_conn(
    conn: &Connection,
    kind: &str,
    dedupe_key: &str,
    payload_json: &str,
) -> DbResult<String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let id = format!("eob-{}", uuid::Uuid::new_v4());
    conn.execute(
        "INSERT INTO effect_outbox(
             id, kind, dedupe_key, payload_json, state, created_at, updated_at
         )
         VALUES (?1, ?2, ?3, ?4, 'pending', ?5, ?6)",
        params![id.as_str(), kind, dedupe_key, payload_json, now, now],
    )
    .await?;
    Ok(id)
}

pub async fn reset_running_to_pending_async(db: &LocalDb) -> DbResult<usize> {
    db.write(|conn| {
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp() as i32;
            let count = conn
                .execute(
                    "UPDATE effect_outbox
                     SET state = 'pending',
                         updated_at = ?1
                     WHERE state = 'running'",
                    params![now],
                )
                .await?;
            Ok(count as usize)
        })
    })
    .await
}

/// Delete processed entries older than 24 hours.
pub fn gc(db: Arc<LocalDb>) -> usize {
    match block_on_outbox_db(async move { gc_async(&db).await.map_err(|e| e.to_string()) }) {
        Ok(count) => count,
        Err(error) => {
            log::warn!("Failed to GC outbox entries: {}", error);
            0
        }
    }
}

async fn gc_async(db: &LocalDb) -> DbResult<usize> {
    db.write(|conn| {
        Box::pin(async move {
            let cutoff = chrono::Utc::now().timestamp() as i32 - 86400;
            let count = conn
                .execute(
                    "DELETE FROM effect_outbox
                     WHERE state = 'done'
                       AND updated_at < ?1",
                    params![cutoff],
                )
                .await?;
            Ok(count as usize)
        })
    })
    .await
}

fn block_on_outbox_db<T, Fut>(future: Fut) -> Result<T, String>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || run_outbox_db_future(future))
            .join()
            .map_err(|_| "Outbox database task panicked".to_string())?
    } else {
        run_outbox_db_future(future)
    }
}

fn run_outbox_db_future<T>(future: impl Future<Output = Result<T, String>>) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("Failed to create outbox database runtime: {error}"))?
        .block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
    use tempfile::{tempdir, TempDir};

    async fn test_db() -> (TempDir, LocalDb) {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("outbox-test.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        (temp, db)
    }

    async fn insert_pending_async(db: &LocalDb, kind: &str, dedupe_key: &str) -> DbResult<String> {
        insert_pending_with_payload_async(db, kind, dedupe_key, "{}").await
    }

    async fn insert_advance_dag(db: &LocalDb, dedupe_key: &str) -> String {
        insert_pending_async(db, "advance_dag", dedupe_key)
            .await
            .unwrap()
    }

    async fn outbox_states(db: &LocalDb, dedupe_key: Option<&str>) -> Vec<(String, String)> {
        db.read(|conn| {
            let dedupe_key = dedupe_key.map(str::to_string);
            Box::pin(async move {
                let mut rows = match dedupe_key {
                    Some(ref key) => {
                        conn.query(
                            "SELECT dedupe_key, state
                             FROM effect_outbox
                             WHERE dedupe_key = ?1
                             ORDER BY dedupe_key ASC, id ASC",
                            params![key.as_str()],
                        )
                        .await?
                    }
                    None => {
                        conn.query(
                            "SELECT dedupe_key, state
                             FROM effect_outbox
                             ORDER BY dedupe_key ASC, id ASC",
                            (),
                        )
                        .await?
                    }
                };
                let mut states = Vec::new();
                while let Some(row) = rows.next().await? {
                    states.push((row.text(0)?, row.text(1)?));
                }
                Ok(states)
            })
        })
        .await
        .unwrap()
    }

    async fn count_outbox(db: &LocalDb) -> i64 {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query("SELECT COUNT(*) FROM effect_outbox", ()).await?;
                let row = rows.next().await?.unwrap();
                row.i64(0)
            })
        })
        .await
        .unwrap()
    }

    async fn attempts_for(db: &LocalDb, id: &str) -> i64 {
        db.read(|conn| {
            let id = id.to_string();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT attempts FROM effect_outbox WHERE id = ?1",
                        params![id.as_str()],
                    )
                    .await?;
                let row = rows.next().await?.unwrap();
                row.i64(0)
            })
        })
        .await
        .unwrap()
    }

    async fn backdate(db: &LocalDb, id: &str) {
        let old_time = chrono::Utc::now().timestamp() as i32 - 172800;
        db.write(|conn| {
            let id = id.to_string();
            Box::pin(async move {
                conn.execute(
                    "UPDATE effect_outbox SET updated_at = ?1 WHERE id = ?2",
                    params![old_time, id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_insert_and_drain() {
        let (_temp, db) = test_db().await;

        insert_advance_dag(&db, "exec-1").await;
        insert_advance_dag(&db, "exec-2").await;

        let entries = drain_pending_async(&db).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].kind, "advance_dag");
        assert_eq!(entries[0].payload_json, "{}");

        let pending = drain_pending_async(&db).await.unwrap();
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn test_mark_done() {
        let (_temp, db) = test_db().await;
        insert_advance_dag(&db, "exec-1").await;

        let entries = drain_pending_async(&db).await.unwrap();
        assert_eq!(entries.len(), 1);

        mark_done_async(&db, &entries[0].id).await.unwrap();

        let entries = drain_pending_async(&db).await.unwrap();
        assert!(entries.is_empty());

        let states = outbox_states(&db, None).await;
        assert_eq!(states[0].1, "done");
    }

    #[tokio::test]
    async fn test_mark_failed() {
        let (_temp, db) = test_db().await;
        insert_advance_dag(&db, "exec-1").await;

        let entries = drain_pending_async(&db).await.unwrap();
        mark_failed_async(&db, &entries[0].id, "something broke")
            .await
            .unwrap();

        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT state, last_error FROM effect_outbox LIMIT 1", ())
                    .await?;
                let row = rows.next().await?.unwrap();
                assert_eq!(row.text(0)?, "failed");
                assert_eq!(row.opt_text(1)?.as_deref(), Some("something broke"));
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_mark_done_by_key() {
        let (_temp, db) = test_db().await;
        insert_advance_dag(&db, "exec-1").await;
        insert_advance_dag(&db, "exec-1").await;
        insert_advance_dag(&db, "exec-2").await;

        let _ = drain_pending_async(&db).await.unwrap();

        mark_done_by_key_async(&db, "advance_dag", "exec-1")
            .await
            .unwrap();

        let states = outbox_states(&db, None).await;
        let exec1_done = states
            .iter()
            .filter(|(key, _)| key == "exec-1")
            .all(|(_, state)| state == "done");
        assert!(exec1_done);

        let exec2_running = states
            .iter()
            .filter(|(key, _)| key == "exec-2")
            .all(|(_, state)| state == "running");
        assert!(exec2_running);
    }

    #[tokio::test]
    async fn test_reset_running_to_pending() {
        let (_temp, db) = test_db().await;
        insert_advance_dag(&db, "exec-1").await;

        let _ = drain_pending_async(&db).await.unwrap();

        let reset = reset_running_to_pending_async(&db).await.unwrap();
        assert_eq!(reset, 1);

        let entries = drain_pending_async(&db).await.unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn test_gc_removes_old_done_entries() {
        let (_temp, db) = test_db().await;
        insert_advance_dag(&db, "exec-1").await;

        let entries = drain_pending_async(&db).await.unwrap();
        mark_done_async(&db, &entries[0].id).await.unwrap();
        backdate(&db, &entries[0].id).await;

        let deleted = gc_async(&db).await.unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(count_outbox(&db).await, 0);
    }

    #[tokio::test]
    async fn test_gc_preserves_recent_done_entries() {
        let (_temp, db) = test_db().await;
        insert_advance_dag(&db, "exec-1").await;

        let entries = drain_pending_async(&db).await.unwrap();
        mark_done_async(&db, &entries[0].id).await.unwrap();

        let deleted = gc_async(&db).await.unwrap();
        assert_eq!(deleted, 0);
    }

    #[tokio::test]
    async fn test_gc_preserves_pending_entries() {
        let (_temp, db) = test_db().await;
        let id = insert_advance_dag(&db, "exec-1").await;
        backdate(&db, &id).await;

        let deleted = gc_async(&db).await.unwrap();
        assert_eq!(deleted, 0);
    }

    #[tokio::test]
    async fn test_mark_done_by_key_includes_pending_entries() {
        let (_temp, db) = test_db().await;
        insert_advance_dag(&db, "exec-x").await;
        let _ = drain_pending_async(&db).await.unwrap();
        insert_advance_dag(&db, "exec-x").await;

        mark_done_by_key_async(&db, "advance_dag", "exec-x")
            .await
            .unwrap();

        let states = outbox_states(&db, Some("exec-x")).await;
        assert!(
            states.iter().all(|(_, state)| state == "done"),
            "Both running and pending entries should be marked done, got: {:?}",
            states
        );
    }

    #[tokio::test]
    async fn test_gc_preserves_failed_entries() {
        let (_temp, db) = test_db().await;
        insert_advance_dag(&db, "exec-1").await;

        let entries = drain_pending_async(&db).await.unwrap();
        mark_failed_async(&db, &entries[0].id, "something broke")
            .await
            .unwrap();
        backdate(&db, &entries[0].id).await;

        let deleted = gc_async(&db).await.unwrap();
        assert_eq!(deleted, 0, "GC should not delete failed entries");
        assert_eq!(count_outbox(&db).await, 1);
    }

    #[tokio::test]
    async fn test_drain_increments_attempts() {
        let (_temp, db) = test_db().await;
        insert_advance_dag(&db, "exec-1").await;

        let entries = drain_pending_async(&db).await.unwrap();
        assert_eq!(attempts_for(&db, &entries[0].id).await, 1);

        reset_running_to_pending_async(&db).await.unwrap();
        let entries = drain_pending_async(&db).await.unwrap();
        assert_eq!(attempts_for(&db, &entries[0].id).await, 2);
    }

    #[tokio::test]
    async fn test_insert_pending_with_payload_round_trips() {
        let (_temp, db) = test_db().await;
        insert_pending_with_payload_async(
            &db,
            "embed_event",
            "event-1",
            r#"{"event_id":"event-1"}"#,
        )
        .await
        .unwrap();

        let entries = drain_pending_async(&db).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].kind, "embed_event");
        assert_eq!(entries[0].payload_json, r#"{"event_id":"event-1"}"#);
    }
}
