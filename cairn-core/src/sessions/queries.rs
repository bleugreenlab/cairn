//! Session CRUD operations and rotation logic.

use crate::models::{Session, SessionStatus};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_common::ids;
use cairn_db::turso::params;

pub async fn create_with_id(
    db: &LocalDb,
    id: &str,
    job_id: Option<&str>,
    chat_id: Option<&str>,
    backend: &str,
) -> Result<Session, String> {
    create_with_id_and_lineage(db, id, job_id, chat_id, backend, None, 1).await
}

async fn create_with_id_and_lineage(
    db: &LocalDb,
    id: &str,
    job_id: Option<&str>,
    chat_id: Option<&str>,
    backend: &str,
    parent_session_id: Option<&str>,
    sequence: i32,
) -> Result<Session, String> {
    let id = id.to_string();
    let job_id = job_id.map(str::to_string);
    let chat_id = chat_id.map(str::to_string);
    let backend = backend.to_string();
    let parent_session_id = parent_session_id.map(str::to_string);

    db.write(|conn| {
        let id = id.clone();
        let job_id = job_id.clone();
        let chat_id = chat_id.clone();
        let backend = backend.clone();
        let parent_session_id = parent_session_id.clone();
        Box::pin(async move {
            create_with_id_and_lineage_conn(
                conn,
                &id,
                job_id.as_deref(),
                chat_id.as_deref(),
                &backend,
                parent_session_id.as_deref(),
                sequence,
            )
            .await
        })
    })
    .await
    .map_err(|e| format!("Failed to create session: {e}"))
}

pub async fn get(db: &LocalDb, session_id: &str) -> Result<Session, String> {
    let session_id = session_id.to_string();
    db.query_opt(
        "SELECT id, job_id, chat_id, backend, status, parent_session_id,
                replaced_by_id, terminal_reason, sequence, created_at,
                closed_at, updated_at, backend_id
         FROM sessions
         WHERE id = ?1",
        params![session_id.as_str()],
        session_from_row,
    )
    .await
    .and_then(|session| {
        session.ok_or_else(|| DbError::Row(format!("Session not found: {session_id}")))
    })
    .map_err(|e| format!("Session not found ({session_id}): {e}"))
}

pub async fn update_status(
    db: &LocalDb,
    session_id: &str,
    status: SessionStatus,
    reason: Option<&str>,
) -> Result<(), String> {
    let session_id = session_id.to_string();
    let status = status.to_string();
    let reason = reason.map(str::to_string);
    db.write(|conn| {
        let session_id = session_id.clone();
        let status = status.clone();
        let reason = reason.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            let closed_at = if status == "open" { None } else { Some(now) };
            conn.execute(
                "UPDATE sessions
                 SET status = ?1, terminal_reason = ?2, closed_at = ?3, updated_at = ?4
                 WHERE id = ?5",
                params![
                    status.as_str(),
                    reason.as_deref(),
                    closed_at,
                    now,
                    session_id.as_str()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("Failed to update session status: {e}"))
}

pub(crate) async fn rotate_job_session(
    db: &LocalDb,
    old: &Session,
    job_id: &str,
) -> Result<Session, String> {
    rotate_session(db, old, job_id, true, None).await
}

/// Rotate a job's session onto a different backend.
///
/// Used when a job's model changes to one served by a different provider
/// (e.g. Claude -> Codex). The prior backend's resume handle is invalid on the
/// new backend, so continuation must start a fresh session; the successor
/// records `new_backend` instead of inheriting the source session's backend.
pub(crate) async fn rotate_job_session_to_backend(
    db: &LocalDb,
    old: &Session,
    job_id: &str,
    new_backend: &str,
) -> Result<Session, String> {
    rotate_session(db, old, job_id, true, Some(new_backend.to_string())).await
}

async fn rotate_session(
    db: &LocalDb,
    source: &Session,
    job_id: &str,
    make_active: bool,
    target_backend: Option<String>,
) -> Result<Session, String> {
    let source = source.clone();
    let job_id = job_id.to_string();
    let new_id = ids::mint_session_id().into_string();
    // The successor inherits the source backend unless a switch was requested.
    let backend = target_backend.unwrap_or_else(|| source.backend.clone());

    db.write(|conn| {
        let source = source.clone();
        let job_id = job_id.clone();
        let new_id = new_id.clone();
        let backend = backend.clone();
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            let session = create_with_id_and_lineage_conn(
                conn,
                &new_id,
                Some(job_id.as_str()),
                None,
                &backend,
                Some(&source.id),
                source.sequence + 1,
            )
            .await?;

            if make_active {
                let rows = conn
                    .execute(
                        "UPDATE jobs
                         SET current_session_id = ?1, updated_at = ?2
                         WHERE id = ?3 AND current_session_id = ?4",
                        params![new_id.as_str(), now, job_id.as_str(), source.id.as_str()],
                    )
                    .await?;

                if rows == 0 {
                    conn.execute("DELETE FROM sessions WHERE id = ?1", params![new_id.as_str()])
                        .await?;
                    return Err(DbError::internal(
                        "Concurrent session rotation detected; current_session_id was already updated",
                    ));
                }
            }

            conn.execute(
                "UPDATE sessions SET replaced_by_id = ?1, updated_at = ?2 WHERE id = ?3",
                params![new_id.as_str(), now, source.id.as_str()],
            )
            .await?;

            Ok(session)
        })
    })
    .await
    .map_err(|e| format!("Failed to rotate session: {e}"))
}

async fn create_with_id_and_lineage_conn(
    conn: &cairn_db::turso::Connection,
    id: &str,
    job_id: Option<&str>,
    chat_id: Option<&str>,
    backend: &str,
    parent_session_id: Option<&str>,
    sequence: i32,
) -> DbResult<Session> {
    let now = chrono::Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sessions(
            id, job_id, chat_id, backend, status, parent_session_id,
            replaced_by_id, terminal_reason, sequence, created_at,
            closed_at, updated_at, backend_id
         )
         VALUES (?1, ?2, ?3, ?4, 'open', ?5, NULL, NULL, ?6, ?7, NULL, ?8, NULL)",
        params![
            id,
            job_id,
            chat_id,
            backend,
            parent_session_id,
            sequence,
            now,
            now
        ],
    )
    .await?;

    load_session_conn(conn, id)
        .await?
        .ok_or_else(|| DbError::internal(format!("created session not found: {id}")))
}

async fn load_session_conn(
    conn: &cairn_db::turso::Connection,
    session_id: &str,
) -> DbResult<Option<Session>> {
    let mut rows = conn
        .query(
            "SELECT id, job_id, chat_id, backend, status, parent_session_id,
                    replaced_by_id, terminal_reason, sequence, created_at,
                    closed_at, updated_at, backend_id
             FROM sessions
             WHERE id = ?1",
            params![session_id],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| session_from_row(&row))
        .transpose()
}

fn session_from_row(row: &cairn_db::turso::Row) -> DbResult<Session> {
    let status = row.text(4)?.parse().map_err(|e: String| DbError::Row(e))?;

    Ok(Session {
        id: row.text(0)?,
        job_id: row.opt_text(1)?,
        chat_id: row.opt_text(2)?,
        backend: row.text(3)?,
        status,
        parent_session_id: row.opt_text(5)?,
        replaced_by_id: row.opt_text(6)?,
        terminal_reason: row.opt_text(7)?,
        sequence: row.i64(8)? as i32,
        created_at: row.i64(9)?,
        closed_at: row.opt_i64(10)?,
        updated_at: row.i64(11)?,
        backend_id: row.opt_text(12)?,
    })
}
