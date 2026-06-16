//! Turn CRUD operations.

use crate::models::{Turn, TurnStartReason, TurnState, TurnYieldReason};
use crate::services::EventEmitter;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use turso::params;

#[derive(Debug, Clone)]
pub struct NewTurn<'a> {
    pub id: &'a str,
    pub session_id: &'a str,
    pub run_id: Option<&'a str>,
    pub job_id: Option<&'a str>,
    pub sequence: i32,
    pub predecessor_id: Option<&'a str>,
    pub state: &'a str,
    pub yield_reason: Option<&'a str>,
    pub start_reason: &'a str,
    pub created_at: i32,
    pub started_at: Option<i32>,
    pub ended_at: Option<i32>,
    pub updated_at: i32,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateTurnChangeset<'a> {
    pub run_id: Option<Option<&'a str>>,
    pub state: Option<&'a str>,
    pub yield_reason: Option<Option<&'a str>>,
    pub started_at: Option<Option<i32>>,
    pub ended_at: Option<Option<i32>>,
    pub updated_at: Option<i32>,
}

#[derive(Debug, Clone)]
struct NewTurnData {
    id: String,
    session_id: String,
    run_id: Option<String>,
    job_id: Option<String>,
    sequence: i32,
    predecessor_id: Option<String>,
    state: String,
    yield_reason: Option<String>,
    start_reason: String,
    created_at: i32,
    started_at: Option<i32>,
    ended_at: Option<i32>,
    updated_at: i32,
}

impl From<&NewTurn<'_>> for NewTurnData {
    fn from(turn: &NewTurn<'_>) -> Self {
        Self {
            id: turn.id.to_string(),
            session_id: turn.session_id.to_string(),
            run_id: turn.run_id.map(str::to_string),
            job_id: turn.job_id.map(str::to_string),
            sequence: turn.sequence,
            predecessor_id: turn.predecessor_id.map(str::to_string),
            state: turn.state.to_string(),
            yield_reason: turn.yield_reason.map(str::to_string),
            start_reason: turn.start_reason.to_string(),
            created_at: turn.created_at,
            started_at: turn.started_at,
            ended_at: turn.ended_at,
            updated_at: turn.updated_at,
        }
    }
}

pub async fn create_turn(
    db: &LocalDb,
    new_turn: &NewTurn<'_>,
    emitter: &dyn EventEmitter,
) -> Result<Turn, String> {
    let new_turn = NewTurnData::from(new_turn);
    let turn = db
        .write(|conn| {
            let new_turn = new_turn.clone();
            Box::pin(async move { create_turn_conn(conn, &new_turn).await })
        })
        .await
        .map_err(|e| format!("Failed to create turn: {e}"))?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "turns", "action": "insert"}),
    );
    Ok(turn)
}

pub async fn get_turn(db: &LocalDb, turn_id: &str) -> Result<Turn, String> {
    let turn_id = turn_id.to_string();
    db.query_opt(
        "SELECT id, session_id, run_id, job_id, sequence,
                predecessor_id, state, yield_reason, start_reason, created_at,
                started_at, ended_at, updated_at
         FROM turns
         WHERE id = ?1",
        params![turn_id.as_str()],
        turn_from_row,
    )
    .await
    .and_then(|turn| turn.ok_or_else(|| DbError::Row(format!("Turn not found: {turn_id}"))))
    .map_err(|e| format!("Turn not found: {e}"))
}

pub async fn get_head_turn(db: &LocalDb, job_id: &str) -> Result<Option<Turn>, String> {
    load_one_turn_by_query(
        db,
        "SELECT id, session_id, run_id, job_id, sequence,
                predecessor_id, state, yield_reason, start_reason, created_at,
                started_at, ended_at, updated_at
         FROM turns
         WHERE job_id = ?1
         ORDER BY sequence DESC
         LIMIT 1",
        job_id,
    )
    .await
}

pub async fn get_successor_turn(
    db: &LocalDb,
    predecessor_id: &str,
) -> Result<Option<Turn>, String> {
    load_one_turn_by_query(
        db,
        "SELECT id, session_id, run_id, job_id, sequence,
                predecessor_id, state, yield_reason, start_reason, created_at,
                started_at, ended_at, updated_at
         FROM turns
         WHERE predecessor_id = ?1
         ORDER BY sequence ASC
         LIMIT 1",
        predecessor_id,
    )
    .await
}

pub async fn list_by_session(db: &LocalDb, session_id: &str) -> Result<Vec<Turn>, String> {
    list_by_column(db, "session_id", session_id).await
}

pub async fn list_by_job(db: &LocalDb, job_id: &str) -> Result<Vec<Turn>, String> {
    list_by_column(db, "job_id", job_id).await
}

pub async fn update_turn(
    db: &LocalDb,
    turn_id: &str,
    changeset: &UpdateTurnChangeset<'_>,
    emitter: &dyn EventEmitter,
) -> Result<Turn, String> {
    let turn_id = turn_id.to_string();
    let changeset = OwnedTurnChangeset::from(changeset);
    let turn = db
        .write(|conn| {
            let turn_id = turn_id.clone();
            let changeset = changeset.clone();
            Box::pin(async move {
                let current = load_turn_conn(conn, &turn_id)
                    .await?
                    .ok_or_else(|| DbError::Row(format!("Turn not found: {turn_id}")))?;

                let run_id = changeset.run_id.unwrap_or(current.run_id);
                let state = changeset.state.unwrap_or_else(|| current.state.to_string());
                let yield_reason = changeset
                    .yield_reason
                    .unwrap_or_else(|| current.yield_reason.map(|reason| reason.to_string()));
                let started_at = changeset
                    .started_at
                    .map(|value| value.map(i64::from))
                    .unwrap_or(current.started_at);
                let ended_at = changeset
                    .ended_at
                    .map(|value| value.map(i64::from))
                    .unwrap_or(current.ended_at);
                let updated_at = changeset
                    .updated_at
                    .unwrap_or_else(|| chrono::Utc::now().timestamp() as i32);

                conn.execute(
                    "UPDATE turns
                     SET run_id = ?1, state = ?2, yield_reason = ?3,
                         started_at = ?4, ended_at = ?5, updated_at = ?6
                     WHERE id = ?7",
                    params![
                        run_id.as_deref(),
                        state.as_str(),
                        yield_reason.as_deref(),
                        started_at,
                        ended_at,
                        updated_at,
                        turn_id.as_str()
                    ],
                )
                .await?;

                load_turn_conn(conn, &turn_id)
                    .await?
                    .ok_or_else(|| DbError::Row(format!("Turn not found: {turn_id}")))
            })
        })
        .await
        .map_err(|e| format!("Failed to update turn: {e}"))?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "turns", "action": "update"}),
    );
    Ok(turn)
}

pub async fn next_sequence(db: &LocalDb, session_id: &str) -> Result<i32, String> {
    let session_id = session_id.to_string();
    db.read(|conn| {
        let session_id = session_id.clone();
        Box::pin(async move { next_sequence_conn(conn, &session_id).await })
    })
    .await
    .map_err(|e| format!("Failed to get max sequence: {e}"))
}

pub async fn create_initial_turn(
    db: &LocalDb,
    turn_id: &str,
    session_id: &str,
    job_id: &str,
    emitter: &dyn EventEmitter,
) -> Result<Turn, String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let sequence = next_sequence(db, session_id).await?;
    let state = TurnState::Pending.to_string();
    let reason = TurnStartReason::Initial.to_string();
    let new_turn = NewTurn {
        id: turn_id,
        session_id,
        run_id: None,
        job_id: Some(job_id),
        sequence,
        predecessor_id: None,
        state: &state,
        yield_reason: None,
        start_reason: &reason,
        created_at: now,
        started_at: None,
        ended_at: None,
        updated_at: now,
    };

    create_turn(db, &new_turn, emitter).await
}

pub async fn create_successor_turn(
    db: &LocalDb,
    turn_id: &str,
    session_id: &str,
    job_id: &str,
    predecessor_id: &str,
    start_reason: TurnStartReason,
    emitter: &dyn EventEmitter,
) -> Result<Turn, String> {
    let turn_id = turn_id.to_string();
    let session_id = session_id.to_string();
    let job_id = job_id.to_string();
    let predecessor_id = predecessor_id.to_string();
    let reason = start_reason.to_string();

    let turn = db
        .write(|conn| {
            let turn_id = turn_id.clone();
            let session_id = session_id.clone();
            let job_id = job_id.clone();
            let predecessor_id = predecessor_id.clone();
            let reason = reason.clone();
            Box::pin(async move {
                create_successor_turn_conn(
                    conn,
                    &turn_id,
                    &session_id,
                    &job_id,
                    &predecessor_id,
                    &reason,
                )
                .await
            })
        })
        .await
        .map_err(|e| format!("Failed to create successor turn: {e}"))?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "turns", "action": "insert"}),
    );
    Ok(turn)
}

pub async fn ensure_successor_turn(
    db: &LocalDb,
    turn_id: &str,
    session_id: &str,
    job_id: &str,
    predecessor_id: &str,
    start_reason: TurnStartReason,
    emitter: &dyn EventEmitter,
) -> Result<Turn, String> {
    if let Some(existing) = get_successor_turn(db, predecessor_id).await? {
        return Ok(existing);
    }

    create_successor_turn(
        db,
        turn_id,
        session_id,
        job_id,
        predecessor_id,
        start_reason,
        emitter,
    )
    .await
}

#[derive(Debug, Clone)]
pub struct HostWaitResume {
    pub run_id: String,
    pub issue_id: Option<String>,
    pub predecessor_turn_id: Option<String>,
    pub successor_turn_id: Option<String>,
    pub duplicate: bool,
}

pub async fn record_prompt_response(
    db: &LocalDb,
    prompt_id: &str,
    response: &str,
    answered_at: i32,
    emitter: &dyn EventEmitter,
) -> Result<HostWaitResume, String> {
    let prompt_id = prompt_id.to_string();
    let response = response.to_string();
    let resume = db
        .write(|conn| {
            let prompt_id = prompt_id.clone();
            let response = response.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT p.run_id, r.issue_id, p.turn_id, r.job_id, r.session_id,
                                CASE WHEN p.response IS NULL THEN 0 ELSE 1 END
                         FROM prompts p
                         JOIN runs r ON p.run_id = r.id
                         WHERE p.id = ?1",
                        params![prompt_id.as_str()],
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row(format!("Prompt not found: {prompt_id}")))?;
                let run_id = row.text(0)?;
                let issue_id = row.opt_text(1)?;
                let predecessor_turn_id = row.opt_text(2)?;
                let job_id = row.opt_text(3)?;
                let session_id = row.opt_text(4)?;
                let already_answered = row.i64(5)? != 0;
                drop(rows);

                let duplicate = if already_answered {
                    true
                } else {
                    conn.execute(
                        "UPDATE prompts
                         SET response = ?1, answered_at = ?2
                         WHERE id = ?3 AND response IS NULL",
                        params![response.as_str(), answered_at, prompt_id.as_str()],
                    )
                    .await?
                        == 0
                };

                let successor_turn_id = ensure_resume_successor_conn(
                    conn,
                    predecessor_turn_id.as_deref(),
                    job_id.as_deref(),
                    session_id.as_deref(),
                    TurnStartReason::PromptResponse,
                )
                .await?;

                Ok(HostWaitResume {
                    run_id,
                    issue_id,
                    predecessor_turn_id,
                    successor_turn_id,
                    duplicate,
                })
            })
        })
        .await
        .map_err(|e| e.to_string())?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "prompts", "action": "update"}),
    );
    if resume.successor_turn_id.is_some() {
        let _ = emitter.emit(
            "db-change",
            serde_json::json!({"table": "turns", "action": "insert"}),
        );
    }
    Ok(resume)
}

pub async fn record_permission_response(
    db: &LocalDb,
    request_id: &str,
    status: &str,
    response_json: &str,
    responded_at: i32,
    emitter: &dyn EventEmitter,
) -> Result<HostWaitResume, String> {
    let request_id = request_id.to_string();
    let status = status.to_string();
    let response_json = response_json.to_string();
    let resume = db
        .write(|conn| {
            let request_id = request_id.clone();
            let status = status.clone();
            let response_json = response_json.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT pr.run_id, r.issue_id, pr.turn_id, r.job_id, r.session_id, pr.status
                         FROM permission_requests pr
                         JOIN runs r ON pr.run_id = r.id
                         WHERE pr.id = ?1",
                        params![request_id.as_str()],
                    )
                    .await?;
                let row = rows.next().await?.ok_or_else(|| {
                    DbError::Row(format!("Permission request not found: {request_id}"))
                })?;
                let run_id = row.text(0)?;
                let issue_id = row.opt_text(1)?;
                let predecessor_turn_id = row.opt_text(2)?;
                let job_id = row.opt_text(3)?;
                let session_id = row.opt_text(4)?;
                let current_status = row.text(5)?;
                drop(rows);

                let duplicate = if current_status != "pending" {
                    true
                } else {
                    conn.execute(
                        "UPDATE permission_requests
                         SET status = ?1, response = ?2, responded_at = ?3
                         WHERE id = ?4 AND status = 'pending'",
                        params![
                            status.as_str(),
                            response_json.as_str(),
                            responded_at,
                            request_id.as_str()
                        ],
                    )
                    .await?
                        == 0
                };

                let successor_turn_id = ensure_resume_successor_conn(
                    conn,
                    predecessor_turn_id.as_deref(),
                    job_id.as_deref(),
                    session_id.as_deref(),
                    TurnStartReason::PermissionResponse,
                )
                .await?;

                Ok(HostWaitResume {
                    run_id,
                    issue_id,
                    predecessor_turn_id,
                    successor_turn_id,
                    duplicate,
                })
            })
        })
        .await
        .map_err(|e| e.to_string())?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "permission_requests", "action": "update"}),
    );
    if resume.successor_turn_id.is_some() {
        let _ = emitter.emit(
            "db-change",
            serde_json::json!({"table": "turns", "action": "insert"}),
        );
    }
    Ok(resume)
}

#[derive(Debug, Clone, Default)]
struct OwnedTurnChangeset {
    run_id: Option<Option<String>>,
    state: Option<String>,
    yield_reason: Option<Option<String>>,
    started_at: Option<Option<i32>>,
    ended_at: Option<Option<i32>>,
    updated_at: Option<i32>,
}

impl From<&UpdateTurnChangeset<'_>> for OwnedTurnChangeset {
    fn from(changeset: &UpdateTurnChangeset<'_>) -> Self {
        Self {
            run_id: changeset.run_id.map(|value| value.map(str::to_string)),
            state: changeset.state.map(str::to_string),
            yield_reason: changeset
                .yield_reason
                .map(|value| value.map(str::to_string)),
            started_at: changeset.started_at,
            ended_at: changeset.ended_at,
            updated_at: changeset.updated_at,
        }
    }
}

async fn load_one_turn_by_query(
    db: &LocalDb,
    sql: &'static str,
    value: &str,
) -> Result<Option<Turn>, String> {
    let value = value.to_string();
    db.query_opt(sql, params![value.as_str()], turn_from_row)
        .await
        .map_err(|e| e.to_string())
}

async fn list_by_column(db: &LocalDb, column: &str, value: &str) -> Result<Vec<Turn>, String> {
    let value = value.to_string();
    let sql = format!(
        "SELECT id, session_id, run_id, job_id, sequence,
                predecessor_id, state, yield_reason, start_reason, created_at,
                started_at, ended_at, updated_at
         FROM turns
         WHERE {column} = ?1
         ORDER BY sequence ASC"
    );
    db.query_all(sql, params![value.as_str()], turn_from_row)
        .await
        .map_err(|e| e.to_string())
}

async fn create_turn_conn(conn: &turso::Connection, new_turn: &NewTurnData) -> DbResult<Turn> {
    if let Some(job_id) = new_turn.job_id.as_deref() {
        let active_count = count_conn(
            conn,
            "SELECT COUNT(*)
             FROM turns
             WHERE job_id = ?1 AND state IN ('pending', 'running')",
            job_id,
        )
        .await?;
        if active_count > 0 {
            return Err(DbError::internal(format!(
                "Job {job_id} already has an active turn (pending or running)"
            )));
        }
    }

    if let Some(predecessor_id) = new_turn.predecessor_id.as_deref() {
        let successor_count = count_conn(
            conn,
            "SELECT COUNT(*) FROM turns WHERE predecessor_id = ?1",
            predecessor_id,
        )
        .await?;
        if successor_count > 0 {
            return Err(DbError::internal(format!(
                "Turn {predecessor_id} already has a successor"
            )));
        }
    }

    conn.execute(
        "INSERT INTO turns(
            id, session_id, run_id, job_id, sequence,
            predecessor_id, state, yield_reason, start_reason,
            created_at, started_at, ended_at, updated_at
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            new_turn.id.as_str(),
            new_turn.session_id.as_str(),
            new_turn.run_id.as_deref(),
            new_turn.job_id.as_deref(),
            new_turn.sequence,
            new_turn.predecessor_id.as_deref(),
            new_turn.state.as_str(),
            new_turn.yield_reason.as_deref(),
            new_turn.start_reason.as_str(),
            new_turn.created_at,
            new_turn.started_at,
            new_turn.ended_at,
            new_turn.updated_at
        ],
    )
    .await?;

    if let Some(job_id) = new_turn.job_id.as_deref() {
        conn.execute(
            "UPDATE jobs SET current_turn_id = ?1 WHERE id = ?2",
            params![new_turn.id.as_str(), job_id],
        )
        .await?;
    }

    load_turn_conn(conn, &new_turn.id)
        .await?
        .ok_or_else(|| DbError::internal(format!("created turn not found: {}", new_turn.id)))
}

async fn create_successor_turn_conn(
    conn: &turso::Connection,
    turn_id: &str,
    session_id: &str,
    job_id: &str,
    predecessor_id: &str,
    start_reason: &str,
) -> DbResult<Turn> {
    let predecessor = load_turn_conn(conn, predecessor_id)
        .await?
        .ok_or_else(|| DbError::Row(format!("Predecessor turn not found: {predecessor_id}")))?;

    if !predecessor.state.is_terminal() {
        return Err(DbError::internal(format!(
            "Predecessor turn {predecessor_id} is in non-terminal state {:?}",
            predecessor.state
        )));
    }

    let now = chrono::Utc::now().timestamp() as i32;
    let state = TurnState::Pending.to_string();
    let sequence = next_sequence_conn(conn, session_id).await?;
    let new_turn = NewTurnData {
        id: turn_id.to_string(),
        session_id: session_id.to_string(),
        run_id: None,
        job_id: Some(job_id.to_string()),
        sequence,
        predecessor_id: Some(predecessor_id.to_string()),
        state,
        yield_reason: None,
        start_reason: start_reason.to_string(),
        created_at: now,
        started_at: None,
        ended_at: None,
        updated_at: now,
    };

    create_turn_conn(conn, &new_turn).await
}

async fn ensure_resume_successor_conn(
    conn: &turso::Connection,
    predecessor_id: Option<&str>,
    job_id: Option<&str>,
    session_id: Option<&str>,
    start_reason: TurnStartReason,
) -> DbResult<Option<String>> {
    let (Some(predecessor_id), Some(job_id), Some(session_id)) =
        (predecessor_id, job_id, session_id)
    else {
        return Ok(None);
    };

    if let Some(existing) = load_successor_turn_conn(conn, predecessor_id).await? {
        return Ok(Some(existing.id));
    }

    let successor = create_successor_turn_conn(
        conn,
        &uuid::Uuid::new_v4().to_string(),
        session_id,
        job_id,
        predecessor_id,
        &start_reason.to_string(),
    )
    .await?;
    Ok(Some(successor.id))
}

async fn load_turn_conn(conn: &turso::Connection, turn_id: &str) -> DbResult<Option<Turn>> {
    let mut rows = conn
        .query(
            "SELECT id, session_id, run_id, job_id, sequence,
                    predecessor_id, state, yield_reason, start_reason, created_at,
                    started_at, ended_at, updated_at
             FROM turns
             WHERE id = ?1",
            params![turn_id],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| turn_from_row(&row))
        .transpose()
}

async fn load_successor_turn_conn(
    conn: &turso::Connection,
    predecessor_id: &str,
) -> DbResult<Option<Turn>> {
    let mut rows = conn
        .query(
            "SELECT id, session_id, run_id, job_id, sequence,
                    predecessor_id, state, yield_reason, start_reason, created_at,
                    started_at, ended_at, updated_at
             FROM turns
             WHERE predecessor_id = ?1
             ORDER BY sequence ASC
             LIMIT 1",
            params![predecessor_id],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| turn_from_row(&row))
        .transpose()
}

async fn next_sequence_conn(conn: &turso::Connection, session_id: &str) -> DbResult<i32> {
    let mut rows = conn
        .query(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM turns WHERE session_id = ?1",
            params![session_id],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| row.i64(0).map(|value| value as i32))
        .transpose()?
        .ok_or_else(|| DbError::internal("failed to compute next turn sequence"))
}

async fn count_conn(conn: &turso::Connection, sql: &str, value: &str) -> DbResult<i64> {
    let mut rows = conn.query(sql, params![value]).await?;
    rows.next()
        .await?
        .map(|row| row.i64(0))
        .transpose()?
        .ok_or_else(|| DbError::internal("count query returned no rows"))
}

fn turn_from_row(row: &turso::Row) -> DbResult<Turn> {
    let state = row.text(6)?.parse::<TurnState>().map_err(DbError::Row)?;
    let yield_reason = row
        .opt_text(7)?
        .map(|value| value.parse::<TurnYieldReason>().map_err(DbError::Row))
        .transpose()?;
    let start_reason = row
        .text(8)?
        .parse::<TurnStartReason>()
        .map_err(DbError::Row)?;

    Ok(Turn {
        id: row.text(0)?,
        session_id: row.text(1)?,
        run_id: row.opt_text(2)?,
        job_id: row.opt_text(3)?,
        sequence: row.i64(4)? as i32,
        predecessor_id: row.opt_text(5)?,
        state,
        yield_reason,
        start_reason,
        created_at: row.i64(9)?,
        started_at: row.opt_i64(10)?,
        ended_at: row.opt_i64(11)?,
        updated_at: row.i64(12)?,
    })
}
