//! Durable backend conversation identity and account binding.
use cairn_db::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_db::turso::params;
use cairn_domain::{Session, SessionStatus};

pub async fn create_with_id(
    db: &LocalDb,
    id: &str,
    job_id: Option<&str>,
    chat_id: Option<&str>,
    backend: &str,
) -> Result<Session, String> {
    create_with_id_and_lineage(db, id, job_id, chat_id, backend, None, 1).await
}

pub async fn create_with_id_and_lineage(
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
        "SELECT id, job_id, chat_id, backend, account_id, status, parent_session_id,
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

pub async fn create_with_id_and_lineage_conn(
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
            closed_at, updated_at, backend_id, channel_cursor_rowid
         )
         VALUES (
            ?1, ?2, ?3, ?4, 'open', ?5, NULL, NULL, ?6, ?7, NULL, ?8, NULL,
            (SELECT channel_cursor_rowid FROM sessions WHERE id = ?5)
         )",
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

pub async fn load_session_conn(
    conn: &cairn_db::turso::Connection,
    session_id: &str,
) -> DbResult<Option<Session>> {
    let mut rows = conn
        .query(
            "SELECT id, job_id, chat_id, backend, account_id, status, parent_session_id,
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
    let status = row.text(5)?.parse().map_err(|e: String| DbError::Row(e))?;

    Ok(Session {
        id: row.text(0)?,
        job_id: row.opt_text(1)?,
        chat_id: row.opt_text(2)?,
        backend: row.text(3)?,
        account_id: row.opt_text(4)?,
        status,
        parent_session_id: row.opt_text(6)?,
        replaced_by_id: row.opt_text(7)?,
        terminal_reason: row.opt_text(8)?,
        sequence: row.i64(9)? as i32,
        created_at: row.i64(10)?,
        closed_at: row.opt_i64(11)?,
        updated_at: row.i64(12)?,
        backend_id: row.opt_text(13)?,
    })
}

/// Persist the provider account selected for this backend conversation.
pub async fn set_account_id(
    db: &LocalDb,
    session_id: &str,
    account_id: &str,
) -> Result<(), String> {
    let changed = db
        .execute(
            "UPDATE sessions SET account_id = ?1, updated_at = ?2 WHERE id = ?3",
            params![account_id, chrono::Utc::now().timestamp(), session_id],
        )
        .await
        .map_err(|e| format!("Failed to pin session account: {e}"))?;
    if changed == 0 {
        return Err(format!("Session not found: {session_id}"));
    }
    Ok(())
}

/// Move an existing pin only if it still has the value the resolver observed.
/// A false result means another cold launch won and its durable choice must be
/// re-read and used for authentication.
pub async fn compare_and_set_account_id(
    db: &LocalDb,
    session_id: &str,
    expected_account_id: &str,
    account_id: &str,
) -> Result<bool, String> {
    db.execute(
        "UPDATE sessions SET account_id = ?1, updated_at = ?2 WHERE id = ?3 AND account_id = ?4",
        params![
            account_id,
            chrono::Utc::now().timestamp(),
            session_id,
            expected_account_id
        ],
    )
    .await
    .map(|changed| changed > 0)
    .map_err(|e| format!("Failed to atomically repin session account: {e}"))
}

/// Claim an unpinned session account without overwriting a concurrent winner.
pub async fn set_account_id_if_unset(
    db: &LocalDb,
    session_id: &str,
    account_id: &str,
) -> Result<bool, String> {
    db.execute(
        "UPDATE sessions SET account_id = ?1, updated_at = ?2 WHERE id = ?3 AND account_id IS NULL",
        params![account_id, chrono::Utc::now().timestamp(), session_id],
    )
    .await
    .map(|changed| changed > 0)
    .map_err(|e| format!("Failed to atomically pin session account: {e}"))
}

/// Count sessions assigned to each account after its latest health snapshot.
/// The per-account cutoff is applied by the caller because snapshots differ.
pub async fn account_assignments(db: &LocalDb) -> Result<Vec<(String, i64)>, String> {
    db.query_all(
        "SELECT account_id, created_at FROM sessions WHERE account_id IS NOT NULL",
        (),
        |row| Ok((row.text(0)?, row.i64(1)?)),
    )
    .await
    .map_err(|e| format!("Failed to load session account assignments: {e}"))
}
