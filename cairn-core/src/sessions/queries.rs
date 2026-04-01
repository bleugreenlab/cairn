//! Session CRUD operations and rotation logic.

use crate::diesel_models::{DbSession, NewSession};
use crate::models::{Session, SessionStatus};
use crate::schema::{chats, jobs, sessions};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use uuid::Uuid;

/// Convert DbSession to Session model.
pub fn db_session_to_session(db: DbSession) -> Session {
    Session {
        id: db.id,
        job_id: db.job_id,
        chat_id: db.chat_id,
        backend: db.backend,
        status: db.status.parse().unwrap_or(SessionStatus::Open),
        parent_session_id: db.parent_session_id,
        replaced_by_id: db.replaced_by_id,
        terminal_reason: db.terminal_reason,
        sequence: db.sequence,
        created_at: db.created_at as i64,
        closed_at: db.closed_at.map(|t| t as i64),
        updated_at: db.updated_at as i64,
        backend_id: db.backend_id,
    }
}

/// Create a new session for a job.
pub fn create_for_job(
    conn: &mut SqliteConnection,
    job_id: &str,
    backend: &str,
) -> Result<Session, String> {
    let id = Uuid::new_v4().to_string();
    create_with_id(conn, &id, Some(job_id), None, backend)
}

/// Create a new session for a chat.
pub fn create_for_chat(
    conn: &mut SqliteConnection,
    chat_id: &str,
    backend: &str,
) -> Result<Session, String> {
    let id = Uuid::new_v4().to_string();
    create_with_id(conn, &id, None, Some(chat_id), backend)
}

/// Create a session with a specific ID (used when pre-generating UUIDs).
pub fn create_with_id(
    conn: &mut SqliteConnection,
    id: &str,
    job_id: Option<&str>,
    chat_id: Option<&str>,
    backend: &str,
) -> Result<Session, String> {
    create_with_id_and_lineage(conn, id, job_id, chat_id, backend, None, 1)
}

fn create_with_id_and_lineage(
    conn: &mut SqliteConnection,
    id: &str,
    job_id: Option<&str>,
    chat_id: Option<&str>,
    backend: &str,
    parent_session_id: Option<&str>,
    sequence: i32,
) -> Result<Session, String> {
    let now = chrono::Utc::now().timestamp() as i32;

    let new_session = NewSession {
        id,
        job_id,
        chat_id,
        backend,
        status: "open",
        parent_session_id,
        replaced_by_id: None,
        terminal_reason: None,
        sequence,
        created_at: now,
        closed_at: None,
        updated_at: now,
        backend_id: None,
    };

    diesel::insert_into(sessions::table)
        .values(&new_session)
        .execute(conn)
        .map_err(|e| format!("Failed to create session: {}", e))?;

    get(conn, id)
}

/// Get a session by ID.
pub fn get(conn: &mut SqliteConnection, session_id: &str) -> Result<Session, String> {
    let db_session: DbSession = sessions::table
        .find(session_id)
        .first(conn)
        .map_err(|e| format!("Session not found ({}): {}", session_id, e))?;

    Ok(db_session_to_session(db_session))
}

/// Get the current session for a job (via job.current_session_id).
pub fn get_for_job(conn: &mut SqliteConnection, job_id: &str) -> Result<Option<Session>, String> {
    let session_id: Option<String> = jobs::table
        .find(job_id)
        .select(jobs::current_session_id)
        .first(conn)
        .map_err(|e| format!("Job not found ({}): {}", job_id, e))?;

    match session_id {
        Some(sid) => Ok(Some(get(conn, &sid)?)),
        None => Ok(None),
    }
}

/// Get the current session for a chat (via chat.current_session_id).
pub fn get_for_chat(conn: &mut SqliteConnection, chat_id: &str) -> Result<Option<Session>, String> {
    let session_id: Option<String> = chats::table
        .find(chat_id)
        .select(chats::current_session_id)
        .first(conn)
        .map_err(|e| format!("Chat not found ({}): {}", chat_id, e))?;

    match session_id {
        Some(sid) => Ok(Some(get(conn, &sid)?)),
        None => Ok(None),
    }
}

/// Update session status with optional terminal reason.
/// Sets `closed_at` when transitioning to a terminal state.
pub fn update_status(
    conn: &mut SqliteConnection,
    session_id: &str,
    status: SessionStatus,
    reason: Option<&str>,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let closed_at = if status != SessionStatus::Open {
        Some(now)
    } else {
        None
    };

    diesel::update(sessions::table.find(session_id))
        .set((
            sessions::status.eq(status.to_string()),
            sessions::terminal_reason.eq(reason),
            sessions::closed_at.eq(closed_at),
            sessions::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| format!("Failed to update session status: {}", e))?;

    Ok(())
}

/// Rotate a job's session: create a successor, CAS-update job.current_session_id.
///
/// Does NOT change the old session's status — caller decides (Failed, Closed, etc.)
/// before or after calling rotate.
///
/// Returns the new session.
pub fn rotate_job_session(
    conn: &mut SqliteConnection,
    old: &Session,
    job_id: &str,
) -> Result<Session, String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let new_id = Uuid::new_v4().to_string();

    create_with_id_and_lineage(
        conn,
        &new_id,
        Some(job_id),
        None,
        &old.backend,
        Some(&old.id),
        old.sequence + 1,
    )?;

    let rows = diesel::update(jobs::table.find(job_id))
        .filter(jobs::current_session_id.eq(Some(&old.id)))
        .set((
            jobs::current_session_id.eq(Some(&new_id)),
            jobs::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| format!("Failed to update job session: {}", e))?;

    if rows == 0 {
        diesel::delete(sessions::table.find(&new_id))
            .execute(conn)
            .map_err(|e| format!("Failed to clean up rotated session after CAS failure: {}", e))?;
        return Err(
            "Concurrent session rotation detected — job.current_session_id was already updated"
                .to_string(),
        );
    }

    diesel::update(sessions::table.find(&old.id))
        .set((
            sessions::replaced_by_id.eq(Some(&new_id)),
            sessions::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| format!("Failed to update old session: {}", e))?;

    get(conn, &new_id)
}

pub fn fork_job_session(
    conn: &mut SqliteConnection,
    source: &Session,
    job_id: &str,
    make_active: bool,
) -> Result<Session, String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let new_id = Uuid::new_v4().to_string();

    create_with_id_and_lineage(
        conn,
        &new_id,
        Some(job_id),
        None,
        &source.backend,
        Some(&source.id),
        source.sequence + 1,
    )?;

    if make_active {
        diesel::update(jobs::table.find(job_id))
            .set((
                jobs::current_session_id.eq(Some(&new_id)),
                jobs::updated_at.eq(now),
            ))
            .execute(conn)
            .map_err(|e| format!("Failed to update forked job session: {}", e))?;
    }

    get(conn, &new_id)
}

/// Rotate a chat's session: create a successor, CAS-update chat.current_session_id.
pub fn rotate_chat_session(
    conn: &mut SqliteConnection,
    old: &Session,
    chat_id: &str,
) -> Result<Session, String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let new_id = Uuid::new_v4().to_string();

    create_with_id_and_lineage(
        conn,
        &new_id,
        None,
        Some(chat_id),
        &old.backend,
        Some(&old.id),
        old.sequence + 1,
    )?;

    let rows = diesel::update(chats::table.find(chat_id))
        .filter(chats::current_session_id.eq(Some(&old.id)))
        .set((
            chats::current_session_id.eq(Some(&new_id)),
            chats::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| format!("Failed to update chat session: {}", e))?;

    if rows == 0 {
        diesel::delete(sessions::table.find(&new_id))
            .execute(conn)
            .map_err(|e| format!("Failed to clean up rotated chat session after CAS failure: {}", e))?;
        return Err(
            "Concurrent session rotation detected — chat.current_session_id was already updated"
                .to_string(),
        );
    }

    diesel::update(sessions::table.find(&old.id))
        .set((
            sessions::replaced_by_id.eq(Some(&new_id)),
            sessions::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| format!("Failed to update old session: {}", e))?;

    get(conn, &new_id)
}

pub fn fork_chat_session(
    conn: &mut SqliteConnection,
    source: &Session,
    chat_id: &str,
    make_active: bool,
) -> Result<Session, String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let new_id = Uuid::new_v4().to_string();

    create_with_id_and_lineage(
        conn,
        &new_id,
        None,
        Some(chat_id),
        &source.backend,
        Some(&source.id),
        source.sequence + 1,
    )?;

    if make_active {
        diesel::update(chats::table.find(chat_id))
            .set((
                chats::current_session_id.eq(Some(&new_id)),
                chats::updated_at.eq(now),
            ))
            .execute(conn)
            .map_err(|e| format!("Failed to update forked chat session: {}", e))?;
    }

    get(conn, &new_id)
}

/// Store the backend conversation ID on a session.
/// For Claude: the Claude session ID (same as session.id since we prescribe it).
/// For Codex: the thread ID from thread/start response.
pub fn set_backend_id(
    conn: &mut SqliteConnection,
    session_id: &str,
    backend_id: &str,
) -> Result<(), String> {
    diesel::update(sessions::table.find(session_id))
        .set((
            sessions::backend_id.eq(Some(backend_id)),
            sessions::updated_at.eq(chrono::Utc::now().timestamp() as i32),
        ))
        .execute(conn)
        .map_err(|e| format!("Failed to set backend_id: {}", e))?;
    Ok(())
}

/// Close all open sessions for a given issue by finding jobs linked to the issue
/// and transitioning their sessions to Closed. This is called when an issue is
/// resolved (merged/closed).
///
/// Returns the IDs of sessions that were closed, so the caller can evict
/// any warm processes still indexed by those session IDs.
pub fn close_sessions_for_issue(
    conn: &mut SqliteConnection,
    issue_id: &str,
    reason: &str,
) -> Result<Vec<String>, String> {
    let now = chrono::Utc::now().timestamp() as i32;

    // Find all open sessions belonging to jobs on this issue
    let open_session_ids: Vec<String> = sessions::table
        .inner_join(jobs::table.on(sessions::job_id.eq(jobs::id.nullable())))
        .filter(jobs::issue_id.eq(issue_id))
        .filter(sessions::status.eq("open"))
        .select(sessions::id)
        .load(conn)
        .map_err(|e| format!("Failed to find sessions for issue: {}", e))?;

    if open_session_ids.is_empty() {
        return Ok(vec![]);
    }

    diesel::update(sessions::table.filter(sessions::id.eq_any(&open_session_ids)))
        .set((
            sessions::status.eq("closed"),
            sessions::terminal_reason.eq(Some(reason)),
            sessions::closed_at.eq(Some(now)),
            sessions::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| format!("Failed to close sessions: {}", e))?;

    log::info!(
        "Closed {} session(s) for issue {} (reason: {})",
        open_session_ids.len(),
        issue_id,
        reason
    );

    Ok(open_session_ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{create_test_project, test_diesel_conn};

    fn create_test_job(conn: &mut SqliteConnection, project_id: &str) -> String {
        let job_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;
        let new_job = crate::diesel_models::NewJob {
            id: &job_id,
            execution_id: None,
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: None,
            worktree_path: None,
            branch: None,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status: "pending",
            agent_config_id: None,
            issue_id: None,
            project_id,
            task_description: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: None,
            task_index: None,
            started_at: None,
            model: None,
            node_name: None,
            base_branch: None,
            current_turn_id: None,
        };
        diesel::insert_into(jobs::table)
            .values(&new_job)
            .execute(conn)
            .unwrap();
        job_id
    }

    fn create_test_chat(conn: &mut SqliteConnection, project_id: &str) -> String {
        let chat_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;
        let new_chat = crate::diesel_models::NewChat {
            id: &chat_id,
            project_id,
            current_session_id: None,
            status: "running",
            created_at: now,
            updated_at: now,
        };
        diesel::insert_into(chats::table)
            .values(&new_chat)
            .execute(conn)
            .unwrap();
        chat_id
    }

    #[test]
    fn test_create_for_job() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let job_id = create_test_job(&mut conn, &project_id);

        let session = create_for_job(&mut conn, &job_id, "claude").unwrap();
        assert_eq!(session.job_id.as_deref(), Some(job_id.as_str()));
        assert_eq!(session.chat_id, None);
        assert_eq!(session.backend, "claude");
        assert_eq!(session.status, SessionStatus::Open);
        assert_eq!(session.sequence, 1);
        assert!(session.parent_session_id.is_none());
        assert!(session.replaced_by_id.is_none());
    }

    #[test]
    fn test_create_for_chat() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let chat_id = create_test_chat(&mut conn, &project_id);

        let session = create_for_chat(&mut conn, &chat_id, "claude").unwrap();
        assert_eq!(session.chat_id.as_deref(), Some(chat_id.as_str()));
        assert_eq!(session.job_id, None);
    }

    #[test]
    fn test_get_for_job() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let job_id = create_test_job(&mut conn, &project_id);

        // No session yet
        let result = get_for_job(&mut conn, &job_id).unwrap();
        assert!(result.is_none());

        // Create session and link it
        let session = create_for_job(&mut conn, &job_id, "claude").unwrap();
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::update(jobs::table.find(&job_id))
            .set((
                jobs::current_session_id.eq(Some(&session.id)),
                jobs::updated_at.eq(now),
            ))
            .execute(&mut conn)
            .unwrap();

        let result = get_for_job(&mut conn, &job_id).unwrap();
        assert_eq!(result.unwrap().id, session.id);
    }

    #[test]
    fn test_update_status_to_closed() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let job_id = create_test_job(&mut conn, &project_id);
        let session = create_for_job(&mut conn, &job_id, "claude").unwrap();

        update_status(
            &mut conn,
            &session.id,
            SessionStatus::Closed,
            Some("issue_closed"),
        )
        .unwrap();

        let updated = get(&mut conn, &session.id).unwrap();
        assert_eq!(updated.status, SessionStatus::Closed);
        assert_eq!(updated.terminal_reason.as_deref(), Some("issue_closed"));
        assert!(updated.closed_at.is_some());
    }

    #[test]
    fn test_update_status_to_failed() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let job_id = create_test_job(&mut conn, &project_id);
        let session = create_for_job(&mut conn, &job_id, "claude").unwrap();

        update_status(
            &mut conn,
            &session.id,
            SessionStatus::Failed,
            Some("backend rejected session"),
        )
        .unwrap();

        let updated = get(&mut conn, &session.id).unwrap();
        assert_eq!(updated.status, SessionStatus::Failed);
    }

    #[test]
    fn test_rotate_job_session() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let job_id = create_test_job(&mut conn, &project_id);

        // Create and link initial session
        let session = create_for_job(&mut conn, &job_id, "claude").unwrap();
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::update(jobs::table.find(&job_id))
            .set((
                jobs::current_session_id.eq(Some(&session.id)),
                jobs::updated_at.eq(now),
            ))
            .execute(&mut conn)
            .unwrap();

        // Rotate
        let new_session = rotate_job_session(&mut conn, &session, &job_id).unwrap();

        // New session has incremented sequence
        assert_eq!(new_session.sequence, 2);
        assert_eq!(new_session.status, SessionStatus::Open);
        assert_eq!(new_session.job_id.as_deref(), Some(job_id.as_str()));

        // Rotation creates lineage and replacement links
        assert_eq!(new_session.parent_session_id.as_deref(), Some(session.id.as_str()));
        let old = get(&mut conn, &session.id).unwrap();
        assert_eq!(old.replaced_by_id.as_deref(), Some(new_session.id.as_str()));

        // Job now points to new session
        let job_session_id: Option<String> = jobs::table
            .find(&job_id)
            .select(jobs::current_session_id)
            .first(&mut conn)
            .unwrap();
        assert_eq!(job_session_id.as_deref(), Some(new_session.id.as_str()));
    }

    #[test]
    fn test_rotate_cas_fails_on_stale_session() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let job_id = create_test_job(&mut conn, &project_id);

        let session = create_for_job(&mut conn, &job_id, "claude").unwrap();
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::update(jobs::table.find(&job_id))
            .set((
                jobs::current_session_id.eq(Some(&session.id)),
                jobs::updated_at.eq(now),
            ))
            .execute(&mut conn)
            .unwrap();

        // Simulate concurrent rotation by changing the session_id
        diesel::update(jobs::table.find(&job_id))
            .set(jobs::current_session_id.eq(Some("other-session")))
            .execute(&mut conn)
            .unwrap();

        // Rotate should fail — CAS detects stale pointer
        let result = rotate_job_session(&mut conn, &session, &job_id);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Concurrent session rotation detected"));
    }

    #[test]
    fn test_get_for_chat() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let chat_id = create_test_chat(&mut conn, &project_id);

        // No session yet
        let result = get_for_chat(&mut conn, &chat_id).unwrap();
        assert!(result.is_none());

        // Create session and link it
        let session = create_for_chat(&mut conn, &chat_id, "claude").unwrap();
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::update(chats::table.find(&chat_id))
            .set((
                chats::current_session_id.eq(Some(&session.id)),
                chats::updated_at.eq(now),
            ))
            .execute(&mut conn)
            .unwrap();

        let result = get_for_chat(&mut conn, &chat_id).unwrap();
        assert_eq!(result.unwrap().id, session.id);
    }

    #[test]
    fn test_rotate_chat_session() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let chat_id = create_test_chat(&mut conn, &project_id);

        // Create and link initial session
        let session = create_for_chat(&mut conn, &chat_id, "claude").unwrap();
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::update(chats::table.find(&chat_id))
            .set((
                chats::current_session_id.eq(Some(&session.id)),
                chats::updated_at.eq(now),
            ))
            .execute(&mut conn)
            .unwrap();

        // Rotate
        let new_session = rotate_chat_session(&mut conn, &session, &chat_id).unwrap();

        // New session has incremented sequence
        assert_eq!(new_session.sequence, 2);
        assert_eq!(new_session.status, SessionStatus::Open);
        assert_eq!(new_session.chat_id.as_deref(), Some(chat_id.as_str()));
        assert!(new_session.job_id.is_none());

        // Rotation creates lineage and replacement links
        assert_eq!(new_session.parent_session_id.as_deref(), Some(session.id.as_str()));
        let old = get(&mut conn, &session.id).unwrap();
        assert_eq!(old.replaced_by_id.as_deref(), Some(new_session.id.as_str()));

        // Chat now points to new session
        let chat_session_id: Option<String> = chats::table
            .find(&chat_id)
            .select(chats::current_session_id)
            .first(&mut conn)
            .unwrap();
        assert_eq!(chat_session_id.as_deref(), Some(new_session.id.as_str()));
    }

    #[test]
    fn test_rotate_chat_cas_fails_on_stale_session() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let chat_id = create_test_chat(&mut conn, &project_id);

        let session = create_for_chat(&mut conn, &chat_id, "claude").unwrap();
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::update(chats::table.find(&chat_id))
            .set((
                chats::current_session_id.eq(Some(&session.id)),
                chats::updated_at.eq(now),
            ))
            .execute(&mut conn)
            .unwrap();

        // Simulate concurrent rotation
        diesel::update(chats::table.find(&chat_id))
            .set(chats::current_session_id.eq(Some("other-session")))
            .execute(&mut conn)
            .unwrap();

        let result = rotate_chat_session(&mut conn, &session, &chat_id);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Concurrent session rotation detected"));
    }

    #[test]
    fn test_close_sessions_for_issue() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = crate::test_utils::create_test_issue(&mut conn, &project_id, "Test Issue");

        // Create two jobs on the same issue with open sessions
        let job1 = create_test_job(&mut conn, &project_id);
        let job2 = create_test_job(&mut conn, &project_id);
        // Link jobs to issue
        diesel::update(jobs::table.find(&job1))
            .set(jobs::issue_id.eq(Some(&issue_id)))
            .execute(&mut conn)
            .unwrap();
        diesel::update(jobs::table.find(&job2))
            .set(jobs::issue_id.eq(Some(&issue_id)))
            .execute(&mut conn)
            .unwrap();

        let session1 = create_for_job(&mut conn, &job1, "claude").unwrap();
        let session2 = create_for_job(&mut conn, &job2, "claude").unwrap();

        // Close a third job's session that's on a different issue — should NOT be affected
        let other_issue = crate::test_utils::create_test_issue(&mut conn, &project_id, "Other");
        let job3 = create_test_job(&mut conn, &project_id);
        diesel::update(jobs::table.find(&job3))
            .set(jobs::issue_id.eq(Some(&other_issue)))
            .execute(&mut conn)
            .unwrap();
        let session3 = create_for_job(&mut conn, &job3, "claude").unwrap();

        // Close sessions for the first issue
        let closed_ids = close_sessions_for_issue(&mut conn, &issue_id, "issue_merged").unwrap();
        assert_eq!(closed_ids.len(), 2);

        // Both sessions for the issue are now closed
        let s1 = get(&mut conn, &session1.id).unwrap();
        assert_eq!(s1.status, SessionStatus::Closed);
        assert_eq!(s1.terminal_reason.as_deref(), Some("issue_merged"));
        assert!(s1.closed_at.is_some());

        let s2 = get(&mut conn, &session2.id).unwrap();
        assert_eq!(s2.status, SessionStatus::Closed);

        // Session for the other issue is still open
        let s3 = get(&mut conn, &session3.id).unwrap();
        assert_eq!(s3.status, SessionStatus::Open);
    }

    #[test]
    fn test_close_sessions_for_issue_skips_already_closed() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = crate::test_utils::create_test_issue(&mut conn, &project_id, "Test Issue");

        let job_id = create_test_job(&mut conn, &project_id);
        diesel::update(jobs::table.find(&job_id))
            .set(jobs::issue_id.eq(Some(&issue_id)))
            .execute(&mut conn)
            .unwrap();

        let session = create_for_job(&mut conn, &job_id, "claude").unwrap();
        // Manually close it first
        update_status(
            &mut conn,
            &session.id,
            SessionStatus::Failed,
            Some("already failed"),
        )
        .unwrap();

        // close_sessions_for_issue should find 0 open sessions
        let closed_ids = close_sessions_for_issue(&mut conn, &issue_id, "issue_closed").unwrap();
        assert!(closed_ids.is_empty());

        // Status should still be Failed, not overwritten to Closed
        let s = get(&mut conn, &session.id).unwrap();
        assert_eq!(s.status, SessionStatus::Failed);
    }

    #[test]
    fn test_close_sessions_for_issue_no_sessions() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = crate::test_utils::create_test_issue(&mut conn, &project_id, "Test Issue");

        let closed_ids = close_sessions_for_issue(&mut conn, &issue_id, "issue_closed").unwrap();
        assert!(closed_ids.is_empty());
    }

    // =========================================================================
    // set_backend_id
    // =========================================================================

    #[test]
    fn test_set_backend_id_stores_value() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let job_id = create_test_job(&mut conn, &project_id);

        let session = create_for_job(&mut conn, &job_id, "codex").unwrap();
        assert!(
            session.backend_id.is_none(),
            "new session starts with no backend_id"
        );

        set_backend_id(&mut conn, &session.id, "thread_abc123").unwrap();

        let reloaded = get(&mut conn, &session.id).unwrap();
        assert_eq!(reloaded.backend_id.as_deref(), Some("thread_abc123"));
    }

    #[test]
    fn test_set_backend_id_overwrites_previous() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let job_id = create_test_job(&mut conn, &project_id);

        let session = create_for_job(&mut conn, &job_id, "codex").unwrap();
        set_backend_id(&mut conn, &session.id, "thread_v1").unwrap();
        set_backend_id(&mut conn, &session.id, "thread_v2").unwrap();

        let reloaded = get(&mut conn, &session.id).unwrap();
        assert_eq!(reloaded.backend_id.as_deref(), Some("thread_v2"));
    }

    // =========================================================================
    // backend_id through lifecycle
    // =========================================================================

    #[test]
    fn test_create_for_job_backend_id_is_none() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let job_id = create_test_job(&mut conn, &project_id);

        let session = create_for_job(&mut conn, &job_id, "claude").unwrap();
        assert!(session.backend_id.is_none());
    }

    #[test]
    fn test_create_for_chat_backend_id_is_none() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let chat_id = create_test_chat(&mut conn, &project_id);

        let session = create_for_chat(&mut conn, &chat_id, "claude").unwrap();
        assert!(session.backend_id.is_none());
    }

    #[test]
    fn test_rotate_job_session_backend_id_is_none() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let job_id = create_test_job(&mut conn, &project_id);

        let session = create_for_job(&mut conn, &job_id, "codex").unwrap();
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::update(jobs::table.find(&job_id))
            .set((
                jobs::current_session_id.eq(Some(&session.id)),
                jobs::updated_at.eq(now),
            ))
            .execute(&mut conn)
            .unwrap();

        // Set a backend_id on the original session
        set_backend_id(&mut conn, &session.id, "thread_old").unwrap();

        // Rotate — the new session should NOT inherit backend_id
        let new_session = rotate_job_session(&mut conn, &session, &job_id).unwrap();
        assert!(
            new_session.backend_id.is_none(),
            "rotated session must start fresh with no backend_id"
        );
    }

    #[test]
    fn test_fork_job_session_sets_parent_without_replacement() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let job_id = create_test_job(&mut conn, &project_id);

        let session = create_for_job(&mut conn, &job_id, "codex").unwrap();
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::update(jobs::table.find(&job_id))
            .set((
                jobs::current_session_id.eq(Some(&session.id)),
                jobs::updated_at.eq(now),
            ))
            .execute(&mut conn)
            .unwrap();

        let forked = fork_job_session(&mut conn, &session, &job_id, false).unwrap();
        assert_eq!(forked.parent_session_id.as_deref(), Some(session.id.as_str()));
        assert!(forked.replaced_by_id.is_none());

        let source = get(&mut conn, &session.id).unwrap();
        assert!(source.replaced_by_id.is_none());

        let current_session_id: Option<String> = jobs::table
            .find(&job_id)
            .select(jobs::current_session_id)
            .first(&mut conn)
            .unwrap();
        assert_eq!(current_session_id.as_deref(), Some(session.id.as_str()));
    }

    #[test]
    fn test_fork_chat_session_can_become_active() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let chat_id = create_test_chat(&mut conn, &project_id);

        let session = create_for_chat(&mut conn, &chat_id, "claude").unwrap();
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::update(chats::table.find(&chat_id))
            .set((
                chats::current_session_id.eq(Some(&session.id)),
                chats::updated_at.eq(now),
            ))
            .execute(&mut conn)
            .unwrap();

        let forked = fork_chat_session(&mut conn, &session, &chat_id, true).unwrap();
        assert_eq!(forked.parent_session_id.as_deref(), Some(session.id.as_str()));

        let source = get(&mut conn, &session.id).unwrap();
        assert!(source.replaced_by_id.is_none());

        let current_session_id: Option<String> = chats::table
            .find(&chat_id)
            .select(chats::current_session_id)
            .first(&mut conn)
            .unwrap();
        assert_eq!(current_session_id.as_deref(), Some(forked.id.as_str()));
    }

    #[test]
    fn test_db_session_to_session_maps_backend_id() {
        let db = DbSession {
            id: "sess-1".to_string(),
            job_id: None,
            chat_id: None,
            backend: "codex".to_string(),
            status: "open".to_string(),
            parent_session_id: Some("sess-parent".to_string()),
            replaced_by_id: None,
            terminal_reason: None,
            sequence: 1,
            created_at: 100,
            closed_at: None,
            updated_at: 100,
            backend_id: Some("thread_xyz".to_string()),
        };
        let session = db_session_to_session(db);
        assert_eq!(session.parent_session_id.as_deref(), Some("sess-parent"));
        assert_eq!(session.backend_id.as_deref(), Some("thread_xyz"));
    }

    // =========================================================================
    // db_session_to_session edge cases
    // =========================================================================

    #[test]
    fn test_db_session_to_session_unknown_status_defaults_open() {
        let db = DbSession {
            id: "sess-1".to_string(),
            job_id: None,
            chat_id: None,
            backend: "claude".to_string(),
            status: "garbage_status".to_string(),
            parent_session_id: None,
            replaced_by_id: None,
            terminal_reason: None,
            sequence: 1,
            created_at: 100,
            closed_at: None,
            updated_at: 100,
            backend_id: None,
        };
        let session = db_session_to_session(db);
        // Unknown status falls back to Open
        assert_eq!(session.status, SessionStatus::Open);
    }
}
