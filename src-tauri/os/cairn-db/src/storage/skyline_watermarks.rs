//! Session-local skyline cache invalidation metadata.
use super::{DbResult, RowExt, TrackedConnection};

/// Record an actual event mutation in its owning write transaction.
/// Exact under one writer per session, including team replicas: these counters
/// are cache-invalidation hints, never the source of truth for transcript data.
/// Ordering/session rewrites must use `record_event_rewrite` to raise the edge;
/// this lower-level delta is for removals and rewrites that leave ordering alone.
pub async fn record_event_change(
    conn: &TrackedConnection,
    session_id: &str,
    count_delta: i64,
    reset: bool,
) -> DbResult<()> {
    conn.execute(
        "INSERT INTO session_skyline_watermarks (session_id) VALUES (?1)
         ON CONFLICT(session_id) DO NOTHING",
        [session_id],
    )
    .await?;
    conn.execute(
        "UPDATE session_skyline_watermarks SET
            event_count = event_count + ?2,
            event_revision = event_revision + 1,
            event_reset_revision = CASE WHEN ?3 = 1 THEN event_revision + 1 ELSE event_reset_revision END
         WHERE session_id = ?1",
        turso::params![session_id, count_delta, i64::from(reset)],
    ).await?;
    Ok(())
}

/// Call after a successful insert, not an ignored duplicate, in the same transaction.
/// Ordering metadata is fetched by event/run primary keys, never by history scans.
pub async fn record_event_insert(conn: &TrackedConnection, event_id: &str) -> DbResult<()> {
    let mut rows = conn.query(
        "SELECT e.session_id, r.session_id, CASE WHEN r.session_id = e.session_id THEN 0 ELSE 1 END,
                CASE WHEN r.session_id = e.session_id THEN r.created_at ELSE 0 END,
                CASE WHEN r.session_id = e.session_id THEN r.rowid ELSE 0 END, e.sequence, e.rowid
         FROM events e LEFT JOIN runs r ON r.id = e.run_id WHERE e.id = ?1",
        [event_id],
    ).await?;
    let Some(row) = rows.next().await? else {
        return Ok(());
    };
    let Some(session_id) = row.opt_text(0)? else {
        return Ok(());
    };
    let run_session = row.opt_text(1)?;
    let edge = [
        row.i64(2)?,
        row.i64(3)?,
        row.i64(4)?,
        row.i64(5)?,
        row.i64(6)?,
    ];
    drop(rows);
    let mut rows = conn
        .query(
            "SELECT edge_present, edge_run_unknown, edge_run_created_at, edge_run_rowid,
                edge_sequence, edge_event_rowid
         FROM session_skyline_watermarks WHERE session_id = ?1",
            [session_id.as_str()],
        )
        .await?;
    let previous = match rows.next().await? {
        Some(row) if row.i64(0)? != 0 => Some([
            row.i64(1)?,
            row.i64(2)?,
            row.i64(3)?,
            row.i64(4)?,
            row.i64(5)?,
        ]),
        _ => None,
    };
    drop(rows);
    let cross_session = run_session
        .as_deref()
        .is_some_and(|run_session| run_session != session_id);
    let reset = cross_session || previous.is_some_and(|previous| edge <= previous);
    record_event_change(conn, &session_id, 1, reset).await?;
    // Never lower the edge after a rewrite/delete: a false reset is safe,
    // whereas classifying an earlier insert as a tail append loses prefix bars.
    if previous.is_none_or(|previous| edge > previous) {
        conn.execute(
            "UPDATE session_skyline_watermarks SET edge_present = 1,
                edge_run_unknown = ?2, edge_run_created_at = ?3, edge_run_rowid = ?4,
                edge_sequence = ?5, edge_event_rowid = ?6 WHERE session_id = ?1",
            turso::params![session_id, edge[0], edge[1], edge[2], edge[3], edge[4]],
        )
        .await?;
    }
    Ok(())
}

/// Record one actual vibe insert (+1), rewrite (0), or removal (-1).
/// For ownership/id moves record both old and new keys. Keep journal tombstones
/// after removal so readers can clear colors that no longer have a live vibe.
pub async fn record_vibe_change(
    conn: &TrackedConnection,
    session_id: &str,
    event_id: &str,
    count_delta: i64,
) -> DbResult<()> {
    conn.execute(
        "INSERT INTO session_skyline_watermarks (session_id) VALUES (?1)
         ON CONFLICT(session_id) DO NOTHING",
        [session_id],
    )
    .await?;
    conn.execute(
        "UPDATE session_skyline_watermarks SET
            vibe_count = vibe_count + ?2,
            vibe_revision = vibe_revision + 1 WHERE session_id = ?1",
        turso::params![session_id, count_delta],
    )
    .await?;
    conn.execute(
        "INSERT INTO skyline_vibe_mutations (session_id, event_id, revision)
         SELECT session_id, ?2, vibe_revision FROM session_skyline_watermarks WHERE session_id = ?1
         ON CONFLICT(session_id, event_id) DO UPDATE SET revision = excluded.revision",
        turso::params![session_id, event_id],
    )
    .await?;
    Ok(())
}

/// Capture ownership before an actual deletion or an ordering/session rewrite.
/// The event and optional vibe are both primary-key lookups.
#[derive(Debug, Clone)]
pub struct EventMutationSnapshot {
    pub event_id: String,
    pub session_id: Option<String>,
    pub vibe_session_id: Option<String>,
}

pub async fn snapshot_event(
    conn: &TrackedConnection,
    event_id: &str,
) -> DbResult<Option<EventMutationSnapshot>> {
    let mut rows = conn
        .query(
            "SELECT e.session_id, v.session_id FROM events e
         LEFT JOIN event_vibes v ON v.event_id = e.id WHERE e.id = ?1",
            [event_id],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| {
            Ok(EventMutationSnapshot {
                event_id: event_id.to_owned(),
                session_id: row.opt_text(0)?,
                vibe_session_id: row.opt_text(1)?,
            })
        })
        .transpose()
}

/// Call only after the captured event was actually deleted, in the same transaction.
/// The snapshot retains vibe ownership even when an event FK cascaded its removal.
pub async fn record_event_delete(
    conn: &TrackedConnection,
    previous: &EventMutationSnapshot,
) -> DbResult<()> {
    if let Some(session) = previous.session_id.as_deref() {
        record_event_change(conn, session, -1, true).await?;
    }
    if let Some(session) = previous.vibe_session_id.as_deref() {
        record_vibe_change(conn, session, &previous.event_id, -1).await?;
    }
    Ok(())
}

/// Record an actual rewrite after capturing the previous event session.
/// Balancing old/new ownership also raises the new ordering edge. A vibe's
/// independent ownership is unchanged unless its own row was also rewritten.
pub async fn record_event_rewrite(
    conn: &TrackedConnection,
    event_id: &str,
    previous_session_id: Option<&str>,
) -> DbResult<()> {
    if let Some(session) = previous_session_id {
        record_event_change(conn, session, -1, true).await?;
    }
    record_event_insert(conn, event_id).await
}

/// Remove one vibe and its invalidation metadata atomically by event primary key.
/// Missing rows do not advance revisions. The stored vibe owns the mutation,
/// even when its parent event belongs to another session.
pub async fn delete_vibe(conn: &TrackedConnection, event_id: &str) -> DbResult<u64> {
    let mut rows = conn
        .query(
            "SELECT session_id FROM event_vibes WHERE event_id = ?1",
            [event_id],
        )
        .await?;
    let session = match rows.next().await? {
        Some(row) => row.opt_text(0)?,
        None => return Ok(0),
    };
    drop(rows);
    let deleted = conn
        .execute("DELETE FROM event_vibes WHERE event_id = ?1", [event_id])
        .await?;
    if deleted > 0 {
        if let Some(session) = session {
            record_vibe_change(conn, &session, event_id, -1).await?;
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{DbError, LocalDb, MigrationRunner, TURSO_MIGRATIONS};

    async fn fixture() -> (tempfile::TempDir, LocalDb) {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("skyline-hints.db"))
            .await
            .unwrap();
        db.execute_batch(
            "CREATE TABLE sessions(id TEXT PRIMARY KEY);
             CREATE TABLE runs(id TEXT PRIMARY KEY, session_id TEXT, created_at INTEGER);
             CREATE TABLE events(id TEXT PRIMARY KEY, session_id TEXT, run_id TEXT, sequence INTEGER);
             CREATE TABLE event_vibes(event_id TEXT PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE, revision INTEGER, session_id TEXT);
             CREATE TABLE session_skyline_cache(session_id TEXT, latest_event_created_at INTEGER);
             INSERT INTO sessions VALUES('a'),('b');
             INSERT INTO runs VALUES('r','a',10),('other','b',20);
             INSERT INTO events VALUES('e','a','r',1);
             INSERT INTO event_vibes VALUES('e',500,'a');
             INSERT INTO session_skyline_cache VALUES('a',123);"
        ).await.unwrap();
        let migration = *TURSO_MIGRATIONS
            .iter()
            .find(|migration| migration.name() == "session_skyline_watermarks")
            .unwrap();
        MigrationRunner::new(vec![migration])
            .run(&db)
            .await
            .unwrap();
        (temp, db)
    }

    async fn snapshot(db: &LocalDb, session: &str) -> Vec<i64> {
        db.query_all(
            "SELECT event_count,event_revision,event_reset_revision,vibe_count,vibe_revision
             FROM session_skyline_watermarks WHERE session_id=?1",
            [session.to_owned()],
            |row| (0..5).map(|i| row.i64(i)).collect::<Result<Vec<_>, _>>(),
        )
        .await
        .unwrap()
        .remove(0)
    }

    #[tokio::test]
    async fn application_hints_follow_actual_mutations_in_their_transaction() {
        let (_temp, db) = fixture().await;
        assert_eq!(snapshot(&db, "a").await, [1, 1, 1, 1, 1]);
        assert_eq!(
            db.query_all("SELECT COUNT(*) FROM session_skyline_cache", (), |row| row
                .i64(0))
                .await
                .unwrap(),
            [0]
        );
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO events VALUES('tail','a','r',2)", ())
                    .await?;
                record_event_insert(conn, "tail").await
            })
        })
        .await
        .unwrap();
        let tail = snapshot(&db, "a").await;
        assert_eq!(tail, [2, 2, 1, 1, 1]);
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("UPDATE events SET sequence=10 WHERE id='e'", ())
                    .await?;
                record_event_rewrite(conn, "e", Some("a")).await?;
                conn.execute("INSERT INTO events VALUES('middle','a','r',3)", ())
                    .await?;
                record_event_insert(conn, "middle").await?;
                conn.execute(
                    "UPDATE event_vibes SET revision=revision WHERE event_id='e'",
                    (),
                )
                .await?;
                record_vibe_change(conn, "a", "e", 0).await
            })
        })
        .await
        .unwrap();
        let rewritten = snapshot(&db, "a").await;
        assert_eq!(rewritten[0], 3);
        assert_eq!(rewritten[1], rewritten[2]);
        assert_eq!((rewritten[3], rewritten[4]), (1, 2));
        db.write(|conn| {
            Box::pin(async move {
                let previous = snapshot_event(conn, "e").await?.unwrap();
                conn.execute(
                    "UPDATE events SET session_id='b',run_id='other' WHERE id='e'",
                    (),
                )
                .await?;
                record_event_rewrite(conn, "e", previous.session_id.as_deref()).await?;
                let previous = snapshot_event(conn, "e").await?.unwrap();
                conn.execute("DELETE FROM events WHERE id='e'", ()).await?;
                record_event_delete(conn, &previous).await
            })
        })
        .await
        .unwrap();
        assert_eq!(snapshot(&db, "a").await[0], 2);
        assert_eq!(snapshot(&db, "a").await[3], 0);
        assert_eq!(snapshot(&db, "b").await[0], 0);
        let journal = db
            .query_all(
                "SELECT revision FROM skyline_vibe_mutations WHERE session_id='a' AND event_id='e'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(journal, [3]);
        let before = snapshot(&db, "a").await;
        let result: DbResult<()> = db
            .write(|conn| {
                Box::pin(async move {
                    conn.execute("INSERT INTO events VALUES('rollback','a','r',11)", ())
                        .await?;
                    record_event_insert(conn, "rollback").await?;
                    Err(DbError::Internal("abort probe".into()))
                })
            })
            .await;
        assert!(result.is_err());
        assert_eq!(snapshot(&db, "a").await, before);
        db.write(|conn| {
            Box::pin(async move {
                record_event_change(conn, "a", -2, true).await?;
                conn.execute("DELETE FROM events WHERE session_id='a'", ())
                    .await?;
                conn.execute("DELETE FROM sessions WHERE id='a'", ())
                    .await?;
                conn.execute("INSERT INTO sessions VALUES('a')", ()).await?;
                conn.execute("INSERT INTO events VALUES('again','a','r',0)", ())
                    .await?;
                record_event_insert(conn, "again").await
            })
        })
        .await
        .unwrap();
        let reinserted = snapshot(&db, "a").await;
        assert_eq!(reinserted[0], 1);
        assert!(reinserted[1] > before[1]);
        assert!(reinserted[2] > before[2]);
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("INSERT INTO event_vibes VALUES('again',501,'b')", ())
                    .await?;
                record_vibe_change(conn, "b", "again", 1).await?;
                assert_eq!(delete_vibe(conn, "again").await?, 1);
                assert_eq!(delete_vibe(conn, "again").await?, 0);
                Ok(())
            })
        })
        .await
        .unwrap();
        let vibe_deleted = snapshot(&db, "b").await;
        assert_eq!((vibe_deleted[3], vibe_deleted[4]), (0, 3));
        db.execute_batch("UPDATE session_skyline_watermarks SET event_revision=9007199254740991 WHERE session_id='a';
                          UPDATE session_skyline_watermarks SET vibe_revision=9007199254740991 WHERE session_id='b';")
            .await.unwrap();
        assert!(db
            .write(|conn| Box::pin(async move { record_event_change(conn, "a", 0, true).await }))
            .await
            .is_err());
        assert!(db
            .write(|conn| Box::pin(async move { record_vibe_change(conn, "b", "again", 0).await }))
            .await
            .is_err());
        assert_eq!(snapshot(&db, "a").await[1], 9_007_199_254_740_991);
        assert_eq!(snapshot(&db, "b").await[4], 9_007_199_254_740_991);
    }
}
