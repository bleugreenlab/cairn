//! Canonical teardown for the graph rooted at jobs owned by one entity.

use std::collections::BTreeMap;

use crate::storage::{DbError, DbResult, RowExt};
use cairn_db::storage::skyline_watermarks::{
    record_event_change, record_vibe_change, snapshot_event,
};
use cairn_db::storage::TrackedConnection;
use cairn_db::turso::{params, Value};

#[derive(Debug, Clone, Copy)]
pub(crate) enum JobOwner {
    Issue,
    Thread,
}

impl JobOwner {
    fn column(self) -> &'static str {
        match self {
            Self::Issue => "issue_id",
            Self::Thread => "thread_id",
        }
    }

    fn runs(self, jobs: &str) -> String {
        match self {
            // Historical issue runs may predate jobs or otherwise have no job_id.
            Self::Issue => format!("SELECT id FROM runs WHERE issue_id = ?1 OR job_id IN ({jobs})"),
            Self::Thread => format!("SELECT id FROM runs WHERE job_id IN ({jobs})"),
        }
    }

    /// Every run the owner's deletion removes, including runs the caller's final
    /// parent delete reaches only by foreign-key cascade. An issue delete cascades
    /// to its executions and on to their jobs and runs, and an event that leaves
    /// that way would bypass the skyline counters.
    fn removed_runs(self, runs: &str) -> String {
        match self {
            Self::Issue => format!(
                "{runs} OR job_id IN (SELECT id FROM jobs WHERE execution_id IN
                     (SELECT id FROM executions WHERE issue_id = ?1))"
            ),
            Self::Thread => runs.to_string(),
        }
    }
}

/// Delete the events matching `events_where` and record each actual removal in
/// the skyline counters of the same transaction.
///
/// Ownership is captured by primary key before the delete because the vibe rows
/// leave by cascade. The delete must remove exactly the captured rows; any
/// mismatch, or a counter write failure, fails the caller's transaction.
pub(crate) async fn delete_counted_events(
    conn: &TrackedConnection,
    events_where: &str,
    params: Vec<Value>,
) -> DbResult<u64> {
    let mut ids = Vec::new();
    let mut rows = conn
        .query(
            &format!("SELECT id FROM events WHERE {events_where}"),
            params.clone(),
        )
        .await?;
    while let Some(row) = rows.next().await? {
        ids.push(row.text(0)?);
    }
    drop(rows);
    let mut removed_events = BTreeMap::<String, i64>::new();
    let mut removed_vibes = Vec::new();
    for id in &ids {
        let Some(snapshot) = snapshot_event(conn, id).await? else {
            continue;
        };
        if let Some(session) = snapshot.session_id {
            *removed_events.entry(session).or_default() += 1;
        }
        if let Some(session) = snapshot.vibe_session_id {
            removed_vibes.push((session, snapshot.event_id));
        }
    }
    let deleted = conn
        .execute(&format!("DELETE FROM events WHERE {events_where}"), params)
        .await?;
    if deleted != ids.len() as u64 {
        return Err(DbError::Internal(format!(
            "counted event delete removed {deleted} rows but captured {}",
            ids.len()
        )));
    }
    for (session, count) in removed_events {
        record_event_change(conn, &session, -count, true).await?;
    }
    for (session, event_id) in removed_vibes {
        record_vibe_change(conn, &session, &event_id, -1).await?;
    }
    Ok(deleted)
}

/// Delete the vibes matching `vibes_where`, journalling each actual removal so
/// readers can clear a color whose vibe no longer exists.
pub(crate) async fn delete_counted_vibes(
    conn: &TrackedConnection,
    vibes_where: &str,
    params: Vec<Value>,
) -> DbResult<u64> {
    let mut captured = Vec::new();
    let mut rows = conn
        .query(
            &format!("SELECT event_id, session_id FROM event_vibes WHERE {vibes_where}"),
            params.clone(),
        )
        .await?;
    while let Some(row) = rows.next().await? {
        captured.push((row.text(0)?, row.opt_text(1)?));
    }
    drop(rows);
    let deleted = conn
        .execute(
            &format!("DELETE FROM event_vibes WHERE {vibes_where}"),
            params,
        )
        .await?;
    if deleted != captured.len() as u64 {
        return Err(DbError::Internal(format!(
            "counted vibe delete removed {deleted} rows but captured {}",
            captured.len()
        )));
    }
    for (event_id, session) in captured {
        if let Some(session) = session {
            record_vibe_change(conn, &session, &event_id, -1).await?;
        }
    }
    Ok(deleted)
}

/// Delete every job selected by one owner and the non-cascading graph beneath it.
///
/// The owner column comes from a closed enum, never user input. Pointer cycles are
/// broken before leaves are removed in immediate-foreign-key order.
pub(crate) async fn delete_owned_jobs(
    conn: &TrackedConnection,
    owner: JobOwner,
    owner_id: &str,
) -> DbResult<()> {
    let column = owner.column();
    let jobs = format!("SELECT id FROM jobs WHERE {column} = ?1");
    let runs = owner.runs(&jobs);

    conn.execute(
        &format!(
            "UPDATE jobs SET current_session_id = NULL, current_turn_id = NULL,
             resume_session_id = NULL WHERE {column} = ?1"
        ),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!(
            "UPDATE turns SET predecessor_id = NULL
             WHERE job_id IN ({jobs}) OR run_id IN ({runs})"
        ),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!(
            "UPDATE sessions SET replaced_by_id = NULL, parent_session_id = NULL
             WHERE job_id IN ({jobs})"
        ),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!("UPDATE artifacts SET parent_version_id = NULL WHERE job_id IN ({jobs})"),
        params![owner_id],
    )
    .await?;

    conn.execute(
        &format!("DELETE FROM provider_limit_holds WHERE job_id IN ({jobs}) OR run_id IN ({runs})"),
        params![owner_id],
    )
    .await?;

    delete_counted_events(
        conn,
        &format!("run_id IN ({})", owner.removed_runs(&runs)),
        vec![Value::Text(owner_id.to_string())],
    )
    .await?;
    for table in ["prompts", "permission_requests"] {
        conn.execute(
            &format!("DELETE FROM {table} WHERE run_id IN ({runs})"),
            params![owner_id],
        )
        .await?;
    }
    conn.execute(
        &format!("DELETE FROM turns WHERE job_id IN ({jobs}) OR run_id IN ({runs})"),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!("DELETE FROM sessions WHERE job_id IN ({jobs})"),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!("DELETE FROM execution_trigger_sources WHERE source_job_id IN ({jobs})"),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!("DELETE FROM merge_requests WHERE job_id IN ({jobs})"),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!("DELETE FROM runs WHERE id IN ({runs})"),
        params![owner_id],
    )
    .await?;
    conn.execute(
        &format!("DELETE FROM memories WHERE job_id IN ({jobs})"),
        params![owner_id],
    )
    .await?;
    for table in [
        "thread_compaction_entries",
        "thread_compactions",
        "thread_compaction_marks",
        "thread_eras",
        "thread_era_children",
    ] {
        conn.execute(
            &format!("DELETE FROM {table} WHERE job_id IN ({jobs})"),
            params![owner_id],
        )
        .await?;
    }
    conn.execute(
        &format!("DELETE FROM jobs WHERE {column} = ?1"),
        params![owner_id],
    )
    .await?;
    Ok(())
}
