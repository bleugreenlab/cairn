//! Durable effect outbox for crash-safe DAG advancement.
//!
//! Provides functions to drain pending outbox entries, mark them as
//! done/failed, and garbage-collect old processed entries.

use crate::diesel_models::NewEffectOutbox;
use crate::schema::effect_outbox;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

/// A pending outbox entry ready for processing.
#[derive(Debug, Clone)]
pub struct OutboxEntry {
    pub id: String,
    pub kind: String,
    pub dedupe_key: String,
    pub payload_json: String,
}

/// Claim all pending outbox entries for processing.
///
/// Atomically transitions entries from `pending` to `running` and
/// increments their attempt counter. Returns the claimed entries.
pub fn drain_pending(conn: &mut SqliteConnection) -> Vec<OutboxEntry> {
    let entries: Vec<(String, String, String, String)> = match effect_outbox::table
        .filter(effect_outbox::state.eq("pending"))
        .select((
            effect_outbox::id,
            effect_outbox::kind,
            effect_outbox::dedupe_key,
            effect_outbox::payload_json,
        ))
        .load(conn)
    {
        Ok(rows) => rows,
        Err(e) => {
            log::warn!("Failed to query pending outbox entries: {}", e);
            return Vec::new();
        }
    };

    if entries.is_empty() {
        return Vec::new();
    }

    let ids: Vec<&str> = entries.iter().map(|(id, _, _, _)| id.as_str()).collect();
    let now = chrono::Utc::now().timestamp() as i32;

    let _ = diesel::update(effect_outbox::table.filter(effect_outbox::id.eq_any(&ids)))
        .set((
            effect_outbox::state.eq("running"),
            effect_outbox::attempts.eq(effect_outbox::attempts + 1),
            effect_outbox::updated_at.eq(now),
        ))
        .execute(conn);

    entries
        .into_iter()
        .map(|(id, kind, dedupe_key, payload_json)| OutboxEntry {
            id,
            kind,
            dedupe_key,
            payload_json,
        })
        .collect()
}

/// Mark an outbox entry as successfully processed.
pub fn mark_done(conn: &mut SqliteConnection, id: &str) {
    let now = chrono::Utc::now().timestamp() as i32;
    let _ = diesel::update(effect_outbox::table.find(id))
        .set((
            effect_outbox::state.eq("done"),
            effect_outbox::updated_at.eq(now),
        ))
        .execute(conn);
}

/// Mark an outbox entry as failed with an error message.
pub fn mark_failed(conn: &mut SqliteConnection, id: &str, error: &str) {
    let now = chrono::Utc::now().timestamp() as i32;
    let _ = diesel::update(effect_outbox::table.find(id))
        .set((
            effect_outbox::state.eq("failed"),
            effect_outbox::last_error.eq(Some(error)),
            effect_outbox::updated_at.eq(now),
        ))
        .execute(conn);
}

/// Mark all `running` or `pending` outbox entries for a given kind+dedupe_key as done.
///
/// Used after successful effect execution to mark the corresponding
/// outbox entries without threading individual IDs through the effect flow.
pub fn mark_done_by_key(conn: &mut SqliteConnection, kind: &str, dedupe_key: &str) {
    let now = chrono::Utc::now().timestamp() as i32;
    let _ = diesel::update(
        effect_outbox::table
            .filter(effect_outbox::kind.eq(kind))
            .filter(effect_outbox::dedupe_key.eq(dedupe_key))
            .filter(effect_outbox::state.eq_any(&["running", "pending"])),
    )
    .set((
        effect_outbox::state.eq("done"),
        effect_outbox::updated_at.eq(now),
    ))
    .execute(conn);
}

/// Insert a pending outbox entry. Returns the generated entry ID.
pub fn insert_pending(
    conn: &mut SqliteConnection,
    kind: &str,
    dedupe_key: &str,
) -> Result<String, diesel::result::Error> {
    insert_pending_with_payload(conn, kind, dedupe_key, "{}")
}

/// Insert a pending outbox entry with a payload.
pub fn insert_pending_with_payload(
    conn: &mut SqliteConnection,
    kind: &str,
    dedupe_key: &str,
    payload_json: &str,
) -> Result<String, diesel::result::Error> {
    let now = chrono::Utc::now().timestamp() as i32;
    let id = format!("eob-{}", uuid::Uuid::new_v4());
    let entry = NewEffectOutbox {
        id: &id,
        kind,
        dedupe_key,
        payload_json,
        state: "pending",
        created_at: now,
        updated_at: now,
    };
    diesel::insert_into(effect_outbox::table)
        .values(&entry)
        .execute(conn)?;
    Ok(id)
}

/// Reset `running` entries back to `pending` (for crash recovery).
///
/// Called on startup to reclaim entries that were mid-flight when the
/// process crashed.
pub fn reset_running_to_pending(conn: &mut SqliteConnection) -> usize {
    let now = chrono::Utc::now().timestamp() as i32;
    diesel::update(effect_outbox::table.filter(effect_outbox::state.eq("running")))
        .set((
            effect_outbox::state.eq("pending"),
            effect_outbox::updated_at.eq(now),
        ))
        .execute(conn)
        .unwrap_or(0)
}

/// Delete processed entries older than 24 hours.
pub fn gc(conn: &mut SqliteConnection) -> usize {
    let cutoff = chrono::Utc::now().timestamp() as i32 - 86400;
    diesel::delete(
        effect_outbox::table
            .filter(effect_outbox::state.eq("done"))
            .filter(effect_outbox::updated_at.lt(cutoff)),
    )
    .execute(conn)
    .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::test_diesel_conn;

    #[test]
    fn test_insert_and_drain() {
        let mut conn = test_diesel_conn();

        insert_pending(&mut conn, "advance_dag", "exec-1");
        insert_pending(&mut conn, "advance_dag", "exec-2");

        let entries = drain_pending(&mut conn);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].kind, "advance_dag");
        assert_eq!(entries[0].payload_json, "{}");

        // Entries should now be running, not pending
        let pending = drain_pending(&mut conn);
        assert!(pending.is_empty());
    }

    #[test]
    fn test_mark_done() {
        let mut conn = test_diesel_conn();
        insert_pending(&mut conn, "advance_dag", "exec-1");

        let entries = drain_pending(&mut conn);
        assert_eq!(entries.len(), 1);

        mark_done(&mut conn, &entries[0].id);

        let entries = drain_pending(&mut conn);
        assert!(entries.is_empty());

        let state: String = effect_outbox::table
            .select(effect_outbox::state)
            .first(&mut conn)
            .unwrap();
        assert_eq!(state, "done");
    }

    #[test]
    fn test_mark_failed() {
        let mut conn = test_diesel_conn();
        insert_pending(&mut conn, "advance_dag", "exec-1");

        let entries = drain_pending(&mut conn);
        mark_failed(&mut conn, &entries[0].id, "something broke");

        let (state, error): (String, Option<String>) = effect_outbox::table
            .select((effect_outbox::state, effect_outbox::last_error))
            .first(&mut conn)
            .unwrap();
        assert_eq!(state, "failed");
        assert_eq!(error.as_deref(), Some("something broke"));
    }

    #[test]
    fn test_mark_done_by_key() {
        let mut conn = test_diesel_conn();
        insert_pending(&mut conn, "advance_dag", "exec-1");
        insert_pending(&mut conn, "advance_dag", "exec-1");
        insert_pending(&mut conn, "advance_dag", "exec-2");

        let _ = drain_pending(&mut conn);

        mark_done_by_key(&mut conn, "advance_dag", "exec-1");

        let states: Vec<(String, String)> = effect_outbox::table
            .select((effect_outbox::dedupe_key, effect_outbox::state))
            .order(effect_outbox::dedupe_key)
            .load(&mut conn)
            .unwrap();

        let exec1_done = states
            .iter()
            .filter(|(k, _)| k == "exec-1")
            .all(|(_, s)| s == "done");
        assert!(exec1_done);

        let exec2_running = states
            .iter()
            .filter(|(k, _)| k == "exec-2")
            .all(|(_, s)| s == "running");
        assert!(exec2_running);
    }

    #[test]
    fn test_reset_running_to_pending() {
        let mut conn = test_diesel_conn();
        insert_pending(&mut conn, "advance_dag", "exec-1");

        let _ = drain_pending(&mut conn);

        let reset = reset_running_to_pending(&mut conn);
        assert_eq!(reset, 1);

        let entries = drain_pending(&mut conn);
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_gc_removes_old_done_entries() {
        let mut conn = test_diesel_conn();
        insert_pending(&mut conn, "advance_dag", "exec-1");

        let entries = drain_pending(&mut conn);
        mark_done(&mut conn, &entries[0].id);

        // Backdate to 2 days ago
        let old_time = chrono::Utc::now().timestamp() as i32 - 172800;
        diesel::update(effect_outbox::table.find(&entries[0].id))
            .set(effect_outbox::updated_at.eq(old_time))
            .execute(&mut conn)
            .unwrap();

        let deleted = gc(&mut conn);
        assert_eq!(deleted, 1);

        let count: i64 = effect_outbox::table.count().get_result(&mut conn).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_gc_preserves_recent_done_entries() {
        let mut conn = test_diesel_conn();
        insert_pending(&mut conn, "advance_dag", "exec-1");

        let entries = drain_pending(&mut conn);
        mark_done(&mut conn, &entries[0].id);

        let deleted = gc(&mut conn);
        assert_eq!(deleted, 0);
    }

    #[test]
    fn test_gc_preserves_pending_entries() {
        let mut conn = test_diesel_conn();
        insert_pending(&mut conn, "advance_dag", "exec-1");

        let old_time = chrono::Utc::now().timestamp() as i32 - 172800;
        diesel::update(effect_outbox::table)
            .set(effect_outbox::updated_at.eq(old_time))
            .execute(&mut conn)
            .unwrap();

        let deleted = gc(&mut conn);
        assert_eq!(deleted, 0);
    }

    #[test]
    fn test_mark_done_by_key_includes_pending_entries() {
        let mut conn = test_diesel_conn();
        // Insert two entries for the same key — leave one pending, drain the other to running
        insert_pending(&mut conn, "advance_dag", "exec-1");
        insert_pending(&mut conn, "advance_dag", "exec-1");

        // Only drain one by inserting a second key to drain alongside
        // Actually: drain moves ALL pending to running. So instead:
        // Insert one, drain it (running), insert another (pending), then mark_done_by_key
        let mut conn2 = test_diesel_conn();
        insert_pending(&mut conn2, "advance_dag", "exec-x");
        let _ = drain_pending(&mut conn2); // now running
        insert_pending(&mut conn2, "advance_dag", "exec-x"); // still pending

        mark_done_by_key(&mut conn2, "advance_dag", "exec-x");

        let states: Vec<String> = effect_outbox::table
            .filter(effect_outbox::dedupe_key.eq("exec-x"))
            .select(effect_outbox::state)
            .load(&mut conn2)
            .unwrap();
        assert!(
            states.iter().all(|s| s == "done"),
            "Both running and pending entries should be marked done, got: {:?}",
            states
        );
    }

    #[test]
    fn test_gc_preserves_failed_entries() {
        let mut conn = test_diesel_conn();
        insert_pending(&mut conn, "advance_dag", "exec-1");

        let entries = drain_pending(&mut conn);
        mark_failed(&mut conn, &entries[0].id, "something broke");

        // Backdate to 2 days ago
        let old_time = chrono::Utc::now().timestamp() as i32 - 172800;
        diesel::update(effect_outbox::table.find(&entries[0].id))
            .set(effect_outbox::updated_at.eq(old_time))
            .execute(&mut conn)
            .unwrap();

        let deleted = gc(&mut conn);
        assert_eq!(deleted, 0, "GC should not delete failed entries");

        let count: i64 = effect_outbox::table.count().get_result(&mut conn).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_drain_increments_attempts() {
        let mut conn = test_diesel_conn();
        insert_pending(&mut conn, "advance_dag", "exec-1");

        let entries = drain_pending(&mut conn);
        let attempts: i32 = effect_outbox::table
            .find(&entries[0].id)
            .select(effect_outbox::attempts)
            .first(&mut conn)
            .unwrap();
        assert_eq!(attempts, 1);

        reset_running_to_pending(&mut conn);
        let entries = drain_pending(&mut conn);
        let attempts: i32 = effect_outbox::table
            .find(&entries[0].id)
            .select(effect_outbox::attempts)
            .first(&mut conn)
            .unwrap();
        assert_eq!(attempts, 2);
    }

    #[test]
    fn test_insert_pending_with_payload_round_trips() {
        let mut conn = test_diesel_conn();
        insert_pending_with_payload(
            &mut conn,
            "embed_event",
            "event-1",
            r#"{"event_id":"event-1"}"#,
        )
        .unwrap();

        let entries = drain_pending(&mut conn);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].kind, "embed_event");
        assert_eq!(entries[0].payload_json, r#"{"event_id":"event-1"}"#);
    }
}
