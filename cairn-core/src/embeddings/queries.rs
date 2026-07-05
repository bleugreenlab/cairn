//! Database queries for the Cohere embedding storage model.
//!
//! Two stores:
//! - `resource_embeddings`: persisted corpus vectors (artifacts, issues, skills,
//!   memories) keyed by canonical cairn:// URI. Queried in-engine for recall.
//! - `event_vibes`: persisted per-event vibe color. The event vector itself is
//!   transient — computed by the async worker, used to assign a color, discarded.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use crate::config::slugify_resource_segment;
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_db::turso::{params, Connection, Row, Value};

#[derive(Debug, Clone)]
pub struct ResourceEmbeddingRecord {
    pub uri: String,
    pub embedding: Vec<u8>,
    pub model: String,
    pub dims: i32,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct EventVibeRecord {
    pub event_id: String,
    pub css_color: String,
    /// PHASE coordinate in [0,1] (0 = explore, 1 = ship).
    pub phase: f32,
    /// FRICTION coordinate in [0,1] (0 = smooth, 1 = post-error friction).
    pub friction: f32,
    pub model: String,
    pub created_at: i64,
}

// ===== resource_embeddings =====

/// Insert or replace the embedding for a corpus resource (sync wrapper).
pub fn upsert_resource_embedding(
    db: Arc<LocalDb>,
    uri: &str,
    embedding_bytes: &[u8],
    model: &str,
    dims: i32,
) -> Result<(), String> {
    let uri = uri.to_string();
    let embedding_bytes = embedding_bytes.to_vec();
    let model = model.to_string();
    block_on_embedding_db(async move {
        upsert_resource_embedding_async(&db, &uri, &embedding_bytes, &model, dims)
            .await
            .map_err(|e| format!("Failed to upsert resource embedding: {}", e))
    })
}

pub async fn upsert_resource_embedding_async(
    db: &LocalDb,
    uri: &str,
    embedding_bytes: &[u8],
    model: &str,
    dims: i32,
) -> DbResult<()> {
    db.write(|conn| {
        let uri = uri.to_string();
        let embedding_bytes = embedding_bytes.to_vec();
        let model = model.to_string();
        Box::pin(async move {
            upsert_resource_embedding_conn(conn, &uri, &embedding_bytes, &model, dims).await
        })
    })
    .await
}

pub async fn upsert_resource_embedding_conn(
    conn: &Connection,
    uri: &str,
    embedding_bytes: &[u8],
    model: &str,
    dims: i32,
) -> DbResult<()> {
    let now = chrono::Utc::now().timestamp();
    conn.execute(
        "INSERT INTO resource_embeddings(uri, embedding, model, dims, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?5)
         ON CONFLICT(uri) DO UPDATE SET
             embedding = excluded.embedding,
             model = excluded.model,
             dims = excluded.dims,
             updated_at = excluded.updated_at",
        params![uri, embedding_bytes.to_vec(), model, dims, now],
    )
    .await?;
    Ok(())
}

pub async fn get_resource_embedding_async(
    db: &LocalDb,
    uri: &str,
) -> DbResult<Option<ResourceEmbeddingRecord>> {
    let uri = uri.to_string();
    db.query_opt(
        "SELECT uri, embedding, model, dims, created_at, updated_at
         FROM resource_embeddings
         WHERE uri = ?1",
        params![uri.as_str()],
        resource_from_row,
    )
    .await
}

pub async fn delete_resource_embedding_async(db: &LocalDb, uri: &str) -> DbResult<()> {
    let uri = uri.to_string();
    db.execute(
        "DELETE FROM resource_embeddings WHERE uri = ?1",
        params![uri.as_str()],
    )
    .await
    .map(|_| ())
}

pub async fn count_resource_embeddings_async(db: &LocalDb) -> DbResult<i64> {
    db.query_one("SELECT COUNT(*) FROM resource_embeddings", (), |row| {
        row.i64(0)
    })
    .await
}

// ===== event_vibes =====

/// Insert or replace the persisted vibe color for an event (sync wrapper).
pub fn upsert_event_vibe(
    db: Arc<LocalDb>,
    event_id: &str,
    session_id: Option<&str>,
    css_color: &str,
    phase: f32,
    friction: f32,
    model: &str,
) -> Result<(), String> {
    let event_id = event_id.to_string();
    let session_id = session_id.map(ToString::to_string);
    let css_color = css_color.to_string();
    let model = model.to_string();
    block_on_embedding_db(async move {
        upsert_event_vibe_async(
            &db,
            &event_id,
            session_id.as_deref(),
            &css_color,
            phase,
            friction,
            &model,
        )
        .await
        .map_err(|e| format!("Failed to upsert event vibe: {}", e))
    })
}

pub async fn upsert_event_vibe_async(
    db: &LocalDb,
    event_id: &str,
    session_id: Option<&str>,
    css_color: &str,
    phase: f32,
    friction: f32,
    model: &str,
) -> DbResult<()> {
    db.write(|conn| {
        let event_id = event_id.to_string();
        let session_id = session_id.map(ToString::to_string);
        let css_color = css_color.to_string();
        let model = model.to_string();
        Box::pin(async move {
            upsert_event_vibe_conn(
                conn,
                &event_id,
                session_id.as_deref(),
                &css_color,
                phase,
                friction,
                &model,
            )
            .await
        })
    })
    .await
}

pub async fn upsert_event_vibe_conn(
    conn: &Connection,
    event_id: &str,
    session_id: Option<&str>,
    css_color: &str,
    phase: f32,
    friction: f32,
    model: &str,
) -> DbResult<()> {
    let now = chrono::Utc::now().timestamp();
    conn.execute(
        "INSERT OR REPLACE INTO event_vibes(
             event_id, session_id, css_color, phase, friction, model, created_at
         )
         VALUES (?1, COALESCE(?2, (SELECT session_id FROM events WHERE id = ?1)), ?3, ?4, ?5, ?6, ?7)",
        params![
            event_id,
            session_id,
            css_color,
            phase as f64,
            friction as f64,
            model,
            now
        ],
    )
    .await?;
    Ok(())
}

pub async fn issue_id_for_event_async(db: &LocalDb, event_id: &str) -> DbResult<Option<String>> {
    let event_id = event_id.to_string();
    db.query_opt_text(
        "SELECT r.issue_id
         FROM events e
         INNER JOIN runs r ON r.id = e.run_id
         WHERE e.id = ?1
         LIMIT 1",
        params![event_id.as_str()],
    )
    .await
}

/// Get persisted vibe records for a session, ordered by event creation order.
pub fn get_event_vibes_for_session(
    db: Arc<LocalDb>,
    session_id: &str,
) -> Result<Vec<EventVibeRecord>, String> {
    let session_id = session_id.to_string();
    block_on_embedding_db(async move {
        get_event_vibes_for_session_async(&db, &session_id)
            .await
            .map_err(|e| format!("Failed to get session vibes: {}", e))
    })
}

/// Get persisted vibe records for a bounded set of event IDs.
pub fn get_event_vibes_for_events(
    db: Arc<LocalDb>,
    event_ids: &[String],
) -> Result<Vec<EventVibeRecord>, String> {
    let event_ids = event_ids.to_vec();
    block_on_embedding_db(async move {
        get_event_vibes_for_events_async(&db, &event_ids)
            .await
            .map_err(|e| format!("Failed to get event vibes: {}", e))
    })
}

pub async fn get_event_vibes_for_session_async(
    db: &LocalDb,
    session_id: &str,
) -> DbResult<Vec<EventVibeRecord>> {
    let session_id = session_id.to_string();
    db.query_all(
        "SELECT ev.event_id, ev.css_color, ev.phase, ev.friction, ev.model, ev.created_at
         FROM event_vibes ev
         INNER JOIN events e ON e.id = ev.event_id
         WHERE ev.session_id = ?1
         ORDER BY e.created_at ASC, e.sequence ASC",
        params![session_id.as_str()],
        event_vibe_from_row,
    )
    .await
}

pub async fn get_event_vibes_for_events_async(
    db: &LocalDb,
    event_ids: &[String],
) -> DbResult<Vec<EventVibeRecord>> {
    if event_ids.is_empty() {
        return Ok(Vec::new());
    }

    // One batched read on the event_vibes PRIMARY KEY (event_id), replacing the
    // former per-event N+1 loop (one round-trip per id). Chunked to stay clear
    // of the SQL bind-variable limit for large id sets; every chunk's rows merge
    // into one map, then we emit in the caller's input order. Output is
    // byte-for-byte identical to the old loop: input order preserved, ids
    // without a vibe skipped, and a repeated input id yields its row once per
    // occurrence.
    const CHUNK: usize = 500;

    let fetched: Vec<EventVibeRecord> = db
        .read(|conn| {
            let event_ids = event_ids.to_vec();
            Box::pin(async move {
                let mut rows_out = Vec::new();
                for chunk in event_ids.chunks(CHUNK) {
                    let placeholders = vec!["?"; chunk.len()].join(", ");
                    let sql = format!(
                        "SELECT event_id, css_color, phase, friction, model, created_at
                         FROM event_vibes
                         WHERE event_id IN ({placeholders})"
                    );
                    let params: Vec<Value> =
                        chunk.iter().map(|id| Value::Text(id.clone())).collect();
                    let mut rows = conn.query(&sql, params).await?;
                    while let Some(row) = rows.next().await? {
                        rows_out.push(event_vibe_from_row(&row)?);
                    }
                }
                Ok(rows_out)
            })
        })
        .await?;

    // event_id is the table PRIMARY KEY, so at most one row per id.
    let by_id: HashMap<&str, &EventVibeRecord> =
        fetched.iter().map(|r| (r.event_id.as_str(), r)).collect();

    Ok(event_ids
        .iter()
        .filter_map(|id| by_id.get(id.as_str()).map(|r| (*r).clone()))
        .collect())
}

// ===== sessions.current_pos (live semantic position) =====

/// Persist a session's live position vector (little-endian f32 bytes).
/// A no-op UPDATE (session row absent) is harmless — the position is
/// best-effort continuity state.
pub async fn set_session_current_pos_async(
    db: &LocalDb,
    session_id: &str,
    bytes: &[u8],
) -> DbResult<()> {
    db.execute(
        "UPDATE sessions SET current_pos = ?2 WHERE id = ?1",
        params![session_id, bytes],
    )
    .await
    .map(|_| ())
}

/// Read a session's persisted live position vector, if any.
pub async fn get_session_current_pos_async(
    db: &LocalDb,
    session_id: &str,
) -> DbResult<Option<Vec<u8>>> {
    let session_id = session_id.to_string();
    db.query_opt(
        "SELECT current_pos FROM sessions WHERE id = ?1",
        params![session_id.as_str()],
        |row| row.opt_blob(0),
    )
    .await
    .map(Option::flatten)
}

/// Resolve the node URI a session rolls its summary centroid up into.
///
/// Job sessions resolve to `cairn://p/PROJECT/NUMBER/EXEC/NODE`. Returns `None`
/// when the session is unknown or its components can't be assembled (the live
/// position still works without an owner URI — only the summary is skipped).
pub async fn resolve_session_owner_uri_async(
    db: &LocalDb,
    session_id: &str,
) -> DbResult<Option<String>> {
    let session_id = session_id.to_string();
    db.query_opt(
        "SELECT s.job_id,
                p.key, i.number, ex.seq, j.uri_segment, j.node_name
         FROM sessions s
         LEFT JOIN jobs j ON s.job_id = j.id
         LEFT JOIN executions ex ON j.execution_id = ex.id
         LEFT JOIN issues i ON j.issue_id = i.id
         LEFT JOIN projects p ON j.project_id = p.id
         WHERE s.id = ?1",
        params![session_id.as_str()],
        |row| {
            if row.opt_text(0)?.is_some() {
                // Job session → node URI.
                let key = row.opt_text(1)?;
                let number = row.opt_i64(2)?.map(|n| n as i32);
                let seq = row.opt_i64(3)?.map(|n| n as i32);
                let segment = row.opt_text(4)?.filter(|s| !s.is_empty()).or_else(|| {
                    row.opt_text(5)
                        .ok()
                        .flatten()
                        .map(|name| slugify_resource_segment(&name))
                        .filter(|s| !s.is_empty())
                });
                return Ok(match (key, number, seq, segment) {
                    (Some(key), Some(number), Some(seq), Some(segment)) => Some(
                        cairn_common::uri::build_node_uri(&key, number, seq, &segment),
                    ),
                    _ => None,
                });
            }

            Ok(None)
        },
    )
    .await
    .map(Option::flatten)
}

// ===== row mappers =====

fn resource_from_row(row: &Row) -> DbResult<ResourceEmbeddingRecord> {
    Ok(ResourceEmbeddingRecord {
        uri: row.text(0)?,
        embedding: row.blob(1)?,
        model: row.text(2)?,
        dims: row.i64(3)? as i32,
        created_at: row.i64(4)?,
        updated_at: row.i64(5)?,
    })
}

fn event_vibe_from_row(row: &Row) -> DbResult<EventVibeRecord> {
    Ok(EventVibeRecord {
        event_id: row.text(0)?,
        css_color: row.text(1)?,
        phase: row.f64(2)? as f32,
        friction: row.f64(3)? as f32,
        model: row.text(4)?,
        created_at: row.i64(5)?,
    })
}

fn block_on_embedding_db<T, Fut>(future: Fut) -> Result<T, String>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || run_embedding_db_future(future))
            .join()
            .map_err(|_| "Embedding database task panicked".to_string())?
    } else {
        run_embedding_db_future(future)
    }
}

fn run_embedding_db_future<T>(
    future: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("Failed to create embedding database runtime: {error}"))?
        .block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("cairn-embeddings-queries.db").await
    }

    async fn exec(db: &LocalDb, sql: &'static str) {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(sql, ()).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn current_pos_round_trips() {
        let db = migrated_db().await;
        exec(
            &db,
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj', 'default', 'Project', 'PROJ', '/tmp/proj', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, project_id, status, created_at, updated_at)
             VALUES ('job-1', 'proj', 'running', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO sessions(id, job_id, created_at, updated_at)
             VALUES ('sess-1', 'job-1', 1, 1)",
        )
        .await;

        // Unset → None.
        assert_eq!(
            get_session_current_pos_async(&db, "sess-1").await.unwrap(),
            None
        );

        let bytes = crate::embeddings::vector::to_bytes(&[0.6_f32, 0.8, -0.25]);
        set_session_current_pos_async(&db, "sess-1", &bytes)
            .await
            .unwrap();
        let got = get_session_current_pos_async(&db, "sess-1")
            .await
            .unwrap()
            .expect("current_pos should be set");
        assert_eq!(got, bytes);
        assert_eq!(
            crate::embeddings::vector::from_bytes(&got),
            vec![0.6_f32, 0.8, -0.25]
        );

        // Unknown session → None.
        assert_eq!(
            get_session_current_pos_async(&db, "nope").await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn resolve_owner_uri_for_job_session() {
        let db = migrated_db().await;
        exec(
            &db,
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj', 'default', 'Project', 'PROJ', '/tmp/proj', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO issues(id, project_id, number, title, created_at, updated_at)
             VALUES ('issue-1', 'proj', 7, 'Issue', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1', 'recipe-1', 'issue-1', 'proj', 'running', 1, 2)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, node_name, status, uri_segment, created_at, updated_at)
             VALUES ('job-1', 'exec-1', 'builder', 'issue-1', 'proj', 'Builder', 'running', 'builder', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO sessions(id, job_id, created_at, updated_at)
             VALUES ('sess-job', 'job-1', 1, 1)",
        )
        .await;

        assert_eq!(
            resolve_session_owner_uri_async(&db, "sess-job")
                .await
                .unwrap(),
            Some("cairn://p/PROJ/7/2/builder".to_string())
        );
    }

    #[tokio::test]
    async fn resolve_owner_uri_unknown_session_is_none() {
        let db = migrated_db().await;
        assert_eq!(
            resolve_session_owner_uri_async(&db, "ghost").await.unwrap(),
            None
        );
    }

    /// The original per-event implementation, kept verbatim as a test oracle so
    /// the batched query is provably output-equivalent to the loop it replaced.
    async fn vibes_per_event_loop(db: &LocalDb, event_ids: &[String]) -> Vec<EventVibeRecord> {
        db.read(|conn| {
            let event_ids = event_ids.to_vec();
            Box::pin(async move {
                let mut out = Vec::new();
                for event_id in event_ids {
                    let mut rows = conn
                        .query(
                            "SELECT event_id, css_color, phase, friction, model, created_at
                             FROM event_vibes
                             WHERE event_id = ?1",
                            params![event_id.as_str()],
                        )
                        .await?;
                    if let Some(row) = rows.next().await? {
                        out.push(event_vibe_from_row(&row)?);
                    }
                }
                Ok(out)
            })
        })
        .await
        .unwrap()
    }

    // f32 fields compared by bit pattern: same stored row, so exact, and no
    // float-equality lint.
    fn as_tuples(records: &[EventVibeRecord]) -> Vec<(String, String, u32, u32, String, i64)> {
        records
            .iter()
            .map(|r| {
                (
                    r.event_id.clone(),
                    r.css_color.clone(),
                    r.phase.to_bits(),
                    r.friction.to_bits(),
                    r.model.clone(),
                    r.created_at,
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn vibes_for_events_batched_matches_per_event_loop() {
        let db = migrated_db().await;

        // event_vibes.event_id has a FOREIGN KEY onto events(id), so seed a run
        // and the parent events before inserting vibes.
        exec(
            &db,
            "INSERT INTO runs(id, created_at, updated_at) VALUES ('run-1', 1, 1)",
        )
        .await;
        exec(
            &db,
            "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at)
             VALUES
                ('e1', 'run-1', 'sess-1', 1, 1, 'assistant', '{}', 1),
                ('e2', 'run-1', 'sess-1', 2, 1, 'assistant', '{}', 1),
                ('e3', 'run-1', 'sess-1', 3, 1, 'assistant', '{}', 1),
                ('e4', 'run-1', 'sess-1', 4, 1, 'assistant', '{}', 1),
                ('e5', 'run-1', 'sess-1', 5, 1, 'assistant', '{}', 1)",
        )
        .await;

        // Seed vibes for several events.
        let seeded = ["e1", "e2", "e3", "e4", "e5"];
        for (i, id) in seeded.iter().enumerate() {
            upsert_event_vibe_async(
                &db,
                id,
                Some("sess-1"),
                &format!("#0000{i:02}"),
                0.1 * i as f32,
                0.2 * i as f32,
                "vibe-model",
            )
            .await
            .unwrap();
        }

        // Input mixes out-of-insertion order, an id with no vibe ("ghost"), and a
        // duplicate id ("e2") — exactly the positional cases the old loop handled.
        let input: Vec<String> = ["e3", "ghost", "e1", "e2", "e5", "e2"]
            .iter()
            .map(ToString::to_string)
            .collect();

        let batched = get_event_vibes_for_events_async(&db, &input).await.unwrap();
        let oracle = vibes_per_event_loop(&db, &input).await;
        assert_eq!(as_tuples(&batched), as_tuples(&oracle));

        // Spell out the contract the oracle encodes: input order kept, ghost
        // skipped, duplicate e2 emitted twice.
        assert_eq!(
            batched
                .iter()
                .map(|r| r.event_id.as_str())
                .collect::<Vec<_>>(),
            vec!["e3", "e1", "e2", "e5", "e2"]
        );

        // Empty input short-circuits to empty output.
        assert!(get_event_vibes_for_events_async(&db, &[])
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn summary_upsert_keyed_by_node_uri_round_trips() {
        let db = migrated_db().await;
        let uri = "cairn://p/PROJ/7/2/builder";
        let bytes = crate::embeddings::vector::to_bytes(&[0.1_f32, 0.2, 0.3]);
        upsert_resource_embedding_async(&db, uri, &bytes, "cohere-test", 3)
            .await
            .unwrap();
        let record = get_resource_embedding_async(&db, uri)
            .await
            .unwrap()
            .expect("summary row should exist");
        assert_eq!(record.embedding, bytes);
        assert_eq!(record.dims, 3);
    }
}
