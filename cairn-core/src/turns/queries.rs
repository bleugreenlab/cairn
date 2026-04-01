//! Turn CRUD operations.

use crate::diesel_models::{DbTurn, NewTurn, UpdateTurnChangeset};
use crate::models::{Turn, TurnStartReason, TurnState};
use crate::schema::{jobs, permission_requests, prompts, runs, turns};
use crate::services::EventEmitter;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

/// Create a new turn and update the job's current_turn_id pointer.
///
/// Enforces invariants:
/// - Only one Pending or Running turn per job at a time
/// - No duplicate successors for a yielded turn
pub fn create_turn(
    conn: &mut SqliteConnection,
    new_turn: &NewTurn,
    emitter: &dyn EventEmitter,
) -> Result<Turn, String> {
    // Invariant: only one pending/running turn per job
    if let Some(job_id) = new_turn.job_id {
        let active_count: i64 = turns::table
            .filter(turns::job_id.eq(job_id))
            .filter(turns::state.eq("pending").or(turns::state.eq("running")))
            .count()
            .get_result(conn)
            .map_err(|e| format!("Failed to check active turns: {}", e))?;

        if active_count > 0 {
            return Err(format!(
                "Job {} already has an active turn (pending or running)",
                job_id
            ));
        }
    }

    // Invariant: no duplicate successor for a yielded turn
    if let Some(pred_id) = new_turn.predecessor_id {
        let existing_successor: i64 = turns::table
            .filter(turns::predecessor_id.eq(pred_id))
            .count()
            .get_result(conn)
            .map_err(|e| format!("Failed to check successor: {}", e))?;

        if existing_successor > 0 {
            return Err(format!("Turn {} already has a successor", pred_id));
        }
    }

    diesel::insert_into(turns::table)
        .values(new_turn)
        .execute(conn)
        .map_err(|e| format!("Failed to create turn: {}", e))?;

    if new_turn.manager_id.is_none() {
        if let Some(job_id) = new_turn.job_id {
            let manager_id: Option<String> = jobs::table
                .find(job_id)
                .select(jobs::manager_id)
                .first(conn)
                .unwrap_or(None);
            if let Some(manager_id) = manager_id {
                diesel::update(turns::table.find(new_turn.id))
                    .set(turns::manager_id.eq(Some(manager_id.as_str())))
                    .execute(conn)
                    .map_err(|e| format!("Failed to backfill turn manager_id: {}", e))?;
            }
        }
    }

    // Update job's current_turn_id pointer
    if let Some(job_id) = new_turn.job_id {
        diesel::update(jobs::table.find(job_id))
            .set(jobs::current_turn_id.eq(Some(new_turn.id)))
            .execute(conn)
            .map_err(|e| format!("Failed to update job current_turn_id: {}", e))?;
    }

    let db_turn: DbTurn = turns::table
        .find(new_turn.id)
        .first(conn)
        .map_err(|e| format!("Failed to load created turn: {}", e))?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "turns", "action": "insert"}),
    );

    Ok(db_turn.into())
}

/// Get a turn by ID.
pub fn get_turn(conn: &mut SqliteConnection, turn_id: &str) -> Result<Turn, String> {
    let db_turn: DbTurn = turns::table
        .find(turn_id)
        .first(conn)
        .map_err(|e| format!("Turn not found: {}", e))?;

    Ok(db_turn.into())
}

/// Get the head (most recent) turn for a job.
pub fn get_head_turn(conn: &mut SqliteConnection, job_id: &str) -> Result<Option<Turn>, String> {
    let result: Option<DbTurn> = turns::table
        .filter(turns::job_id.eq(job_id))
        .order(turns::sequence.desc())
        .first(conn)
        .optional()
        .map_err(|e| format!("Failed to query head turn: {}", e))?;

    Ok(result.map(|t| t.into()))
}

/// Get the successor turn for a predecessor turn, if any.
pub fn get_successor_turn(
    conn: &mut SqliteConnection,
    predecessor_id: &str,
) -> Result<Option<Turn>, String> {
    let result: Option<DbTurn> = turns::table
        .filter(turns::predecessor_id.eq(predecessor_id))
        .order(turns::sequence.asc())
        .first(conn)
        .optional()
        .map_err(|e| format!("Failed to query successor turn: {}", e))?;

    Ok(result.map(|t| t.into()))
}

/// List turns for a session, ordered by sequence.
pub fn list_by_session(conn: &mut SqliteConnection, session_id: &str) -> Result<Vec<Turn>, String> {
    let db_turns: Vec<DbTurn> = turns::table
        .filter(turns::session_id.eq(session_id))
        .order(turns::sequence.asc())
        .load(conn)
        .map_err(|e| format!("Failed to list turns: {}", e))?;

    Ok(db_turns.into_iter().map(|t| t.into()).collect())
}

/// List turns for a job, ordered by sequence.
pub fn list_by_job(conn: &mut SqliteConnection, job_id: &str) -> Result<Vec<Turn>, String> {
    let db_turns: Vec<DbTurn> = turns::table
        .filter(turns::job_id.eq(job_id))
        .order(turns::sequence.asc())
        .load(conn)
        .map_err(|e| format!("Failed to list turns: {}", e))?;

    Ok(db_turns.into_iter().map(|t| t.into()).collect())
}

/// Update a turn using a changeset.
pub fn update_turn(
    conn: &mut SqliteConnection,
    turn_id: &str,
    changeset: &UpdateTurnChangeset,
    emitter: &dyn EventEmitter,
) -> Result<Turn, String> {
    diesel::update(turns::table.find(turn_id))
        .set(changeset)
        .execute(conn)
        .map_err(|e| format!("Failed to update turn: {}", e))?;

    let _ = emitter.emit(
        "db-change",
        serde_json::json!({"table": "turns", "action": "update"}),
    );

    get_turn(conn, turn_id)
}

/// Get the next sequence number for a turn in a session.
pub fn next_sequence(conn: &mut SqliteConnection, session_id: &str) -> Result<i32, String> {
    let max_seq: Option<i32> = turns::table
        .filter(turns::session_id.eq(session_id))
        .select(diesel::dsl::max(turns::sequence))
        .first(conn)
        .map_err(|e| format!("Failed to get max sequence: {}", e))?;

    Ok(max_seq.unwrap_or(0) + 1)
}

/// Create the initial turn for a new job.
///
/// This is called from `prepare_job` when a job first starts.
pub fn create_initial_turn(
    conn: &mut SqliteConnection,
    turn_id: &str,
    session_id: &str,
    job_id: &str,
    emitter: &dyn EventEmitter,
) -> Result<Turn, String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let sequence = next_sequence(conn, session_id)?;

    let new_turn = NewTurn {
        id: turn_id,
        session_id,
        run_id: None, // Late-bound when process attaches
        job_id: Some(job_id),
        manager_id: None,
        sequence,
        predecessor_id: None,
        state: &TurnState::Pending.to_string(),
        yield_reason: None,
        start_reason: &TurnStartReason::Initial.to_string(),
        created_at: now,
        started_at: None,
        ended_at: None,
        updated_at: now,
    };

    create_turn(conn, &new_turn, emitter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::CapturingEmitter;
    use crate::test_utils::{
        create_test_issue, create_test_job, create_test_project, test_diesel_conn,
    };

    fn emitter() -> CapturingEmitter {
        CapturingEmitter::new()
    }

    fn insert_run(conn: &mut SqliteConnection, run_id: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(crate::schema::runs::table)
            .values(&crate::diesel_models::NewRun {
                id: run_id,
                issue_id: None,
                project_id: None,
                job_id: None,
                chat_id: None,
                status: Some("live"),
                session_id: None,
                error_message: None,
                started_at: Some(now),
                exited_at: None,
                created_at: now,
                updated_at: now,
                backend: None,
                exit_reason: None,
                start_mode: None,
            })
            .execute(conn)
            .unwrap();
    }

    // ---- create_initial_turn ----

    #[test]
    fn create_initial_turn_sets_defaults() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        let turn = create_initial_turn(&mut conn, "t1", "s1", &job, &emitter()).unwrap();

        assert_eq!(turn.state, TurnState::Pending);
        assert_eq!(turn.start_reason, TurnStartReason::Initial);
        assert_eq!(turn.sequence, 1);
        assert!(turn.predecessor_id.is_none());
        assert!(turn.run_id.is_none());
        assert_eq!(turn.job_id, Some(job.clone()));
    }

    #[test]
    fn create_initial_turn_updates_job_current_turn_id() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        create_initial_turn(&mut conn, "t1", "s1", &job, &emitter()).unwrap();

        let current: Option<String> = jobs::table
            .find(&job)
            .select(jobs::current_turn_id)
            .first(&mut conn)
            .unwrap();
        assert_eq!(current, Some("t1".to_string()));
    }

    // ---- create_turn invariants ----

    #[test]
    fn rejects_second_active_turn_for_same_job() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        create_initial_turn(&mut conn, "t1", "s1", &job, &emitter()).unwrap();
        let result = create_initial_turn(&mut conn, "t2", "s1", &job, &emitter());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already has an active turn"));
    }

    #[test]
    fn allows_new_turn_after_prior_completed() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        create_initial_turn(&mut conn, "t1", "s1", &job, &emitter()).unwrap();

        // Move turn to running then complete
        insert_run(&mut conn, "r1");
        crate::transitions::start_turn(&mut conn, "t1", "r1", &emitter()).unwrap();
        crate::transitions::apply_turn_outcome(&mut conn, "t1", TurnState::Complete, &emitter())
            .unwrap();

        // Now a new turn should be allowed
        let t2 = create_successor_turn(
            &mut conn,
            "t2",
            "s1",
            &job,
            "t1",
            TurnStartReason::FollowUp,
            &emitter(),
        );
        assert!(t2.is_ok());
    }

    #[test]
    fn rejects_duplicate_successor() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        create_initial_turn(&mut conn, "t1", "s1", &job, &emitter()).unwrap();

        // Complete t1
        insert_run(&mut conn, "r1");
        crate::transitions::start_turn(&mut conn, "t1", "r1", &emitter()).unwrap();
        crate::transitions::apply_turn_outcome(&mut conn, "t1", TurnState::Complete, &emitter())
            .unwrap();

        // First successor succeeds
        create_successor_turn(
            &mut conn,
            "t2",
            "s1",
            &job,
            "t1",
            TurnStartReason::FollowUp,
            &emitter(),
        )
        .unwrap();

        // Complete t2 so it's not blocking "active turn" invariant
        insert_run(&mut conn, "r2");
        crate::transitions::start_turn(&mut conn, "t2", "r2", &emitter()).unwrap();
        crate::transitions::apply_turn_outcome(&mut conn, "t2", TurnState::Complete, &emitter())
            .unwrap();

        // Second successor with same predecessor fails
        let result = create_successor_turn(
            &mut conn,
            "t3",
            "s1",
            &job,
            "t1",
            TurnStartReason::FollowUp,
            &emitter(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already has a successor"));
    }

    // ---- create_successor_turn predecessor validation ----

    #[test]
    fn successor_rejects_non_terminal_predecessor() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        create_initial_turn(&mut conn, "t1", "s1", &job, &emitter()).unwrap();

        // t1 is Pending (non-terminal) — successor should fail
        let result = create_successor_turn(
            &mut conn,
            "t2",
            "s1",
            &job,
            "t1",
            TurnStartReason::FollowUp,
            &emitter(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("non-terminal"));
    }

    #[test]
    fn successor_accepts_yielded_predecessor() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        create_initial_turn(&mut conn, "t1", "s1", &job, &emitter()).unwrap();
        insert_run(&mut conn, "r1");
        crate::transitions::start_turn(&mut conn, "t1", "r1", &emitter()).unwrap();
        crate::transitions::yield_turn(
            &mut conn,
            "t1",
            crate::models::TurnYieldReason::UserInput,
            &emitter(),
        )
        .unwrap();

        let t2 = create_successor_turn(
            &mut conn,
            "t2",
            "s1",
            &job,
            "t1",
            TurnStartReason::PromptResponse,
            &emitter(),
        );
        assert!(t2.is_ok());
        assert_eq!(t2.unwrap().start_reason, TurnStartReason::PromptResponse);
    }

    #[test]
    fn successor_rejects_nonexistent_predecessor() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        let result = create_successor_turn(
            &mut conn,
            "t2",
            "s1",
            &job,
            "ghost",
            TurnStartReason::FollowUp,
            &emitter(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    // ---- next_sequence ----

    #[test]
    fn sequence_starts_at_one() {
        let mut conn = test_diesel_conn();
        assert_eq!(next_sequence(&mut conn, "empty-session").unwrap(), 1);
    }

    #[test]
    fn sequence_increments() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        create_initial_turn(&mut conn, "t1", "s1", &job, &emitter()).unwrap();
        assert_eq!(next_sequence(&mut conn, "s1").unwrap(), 2);
    }

    // ---- get_turn / get_head_turn / list queries ----

    #[test]
    fn get_turn_returns_created_turn() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        create_initial_turn(&mut conn, "t1", "s1", &job, &emitter()).unwrap();
        let turn = get_turn(&mut conn, "t1").unwrap();
        assert_eq!(turn.id, "t1");
        assert_eq!(turn.session_id, "s1");
    }

    #[test]
    fn get_turn_nonexistent_errors() {
        let mut conn = test_diesel_conn();
        assert!(get_turn(&mut conn, "ghost").is_err());
    }

    #[test]
    fn get_head_turn_returns_latest_by_sequence() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        create_initial_turn(&mut conn, "t1", "s1", &job, &emitter()).unwrap();

        // Complete t1 so we can create a successor
        insert_run(&mut conn, "r1");
        crate::transitions::start_turn(&mut conn, "t1", "r1", &emitter()).unwrap();
        crate::transitions::apply_turn_outcome(&mut conn, "t1", TurnState::Complete, &emitter())
            .unwrap();

        create_successor_turn(
            &mut conn,
            "t2",
            "s1",
            &job,
            "t1",
            TurnStartReason::FollowUp,
            &emitter(),
        )
        .unwrap();

        let head = get_head_turn(&mut conn, &job).unwrap().unwrap();
        assert_eq!(head.id, "t2");
        assert_eq!(head.sequence, 2);
    }

    #[test]
    fn get_head_turn_empty_job_returns_none() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        assert!(get_head_turn(&mut conn, &job).unwrap().is_none());
    }

    #[test]
    fn list_by_session_returns_ordered() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        create_initial_turn(&mut conn, "t1", "sess-A", &job, &emitter()).unwrap();

        // Complete t1
        insert_run(&mut conn, "r1");
        crate::transitions::start_turn(&mut conn, "t1", "r1", &emitter()).unwrap();
        crate::transitions::apply_turn_outcome(&mut conn, "t1", TurnState::Complete, &emitter())
            .unwrap();

        create_successor_turn(
            &mut conn,
            "t2",
            "sess-A",
            &job,
            "t1",
            TurnStartReason::FollowUp,
            &emitter(),
        )
        .unwrap();

        let turns = list_by_session(&mut conn, "sess-A").unwrap();
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].id, "t1");
        assert_eq!(turns[1].id, "t2");
        assert!(turns[0].sequence < turns[1].sequence);
    }

    #[test]
    fn list_by_job_returns_ordered() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        create_initial_turn(&mut conn, "t1", "s1", &job, &emitter()).unwrap();

        insert_run(&mut conn, "r1");
        crate::transitions::start_turn(&mut conn, "t1", "r1", &emitter()).unwrap();
        crate::transitions::apply_turn_outcome(&mut conn, "t1", TurnState::Complete, &emitter())
            .unwrap();

        create_successor_turn(
            &mut conn,
            "t2",
            "s1",
            &job,
            "t1",
            TurnStartReason::FollowUp,
            &emitter(),
        )
        .unwrap();

        let turns = list_by_job(&mut conn, &job).unwrap();
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].sequence, 1);
        assert_eq!(turns[1].sequence, 2);
    }

    #[test]
    fn list_by_session_excludes_other_sessions() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job_a = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);
        let job_b = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        create_initial_turn(&mut conn, "t1", "sess-A", &job_a, &emitter()).unwrap();
        create_initial_turn(&mut conn, "t2", "sess-B", &job_b, &emitter()).unwrap();

        let turns_a = list_by_session(&mut conn, "sess-A").unwrap();
        assert_eq!(turns_a.len(), 1);
        assert_eq!(turns_a[0].id, "t1");
    }

    // ---- update_turn ----

    #[test]
    fn update_turn_applies_changeset() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);

        create_initial_turn(&mut conn, "t1", "s1", &job, &emitter()).unwrap();
        insert_run(&mut conn, "r1");

        let changeset = crate::diesel_models::UpdateTurnChangeset {
            run_id: Some(Some("r1")),
            ..Default::default()
        };

        let updated = update_turn(&mut conn, "t1", &changeset, &emitter()).unwrap();
        assert_eq!(updated.run_id, Some("r1".to_string()));
    }

    // ---- db-change events ----

    #[test]
    fn create_turn_emits_db_change() {
        let mut conn = test_diesel_conn();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Test");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "pending", None);
        let em = emitter();

        create_initial_turn(&mut conn, "t1", "s1", &job, &em).unwrap();

        let events = em.events_named("db-change");
        let tables: Vec<&str> = events
            .iter()
            .filter_map(|p| p.get("table").and_then(|v| v.as_str()))
            .collect();
        assert!(tables.contains(&"turns"));
    }

    fn insert_session(conn: &mut SqliteConnection, session_id: &str, job_id: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(crate::schema::sessions::table)
            .values(&crate::diesel_models::NewSession {
                id: session_id,
                job_id: Some(job_id),
                chat_id: None,
                backend: "claude",
                status: "open",
                parent_session_id: None,
                replaced_by_id: None,
                terminal_reason: None,
                sequence: 1,
                created_at: now,
                closed_at: None,
                updated_at: now,
                backend_id: None,
            })
            .execute(conn)
            .unwrap();
    }

    fn insert_bound_run(
        conn: &mut SqliteConnection,
        run_id: &str,
        issue_id: &str,
        project_id: &str,
        job_id: &str,
        session_id: &str,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(crate::schema::runs::table)
            .values(&crate::diesel_models::NewRun {
                id: run_id,
                issue_id: Some(issue_id),
                project_id: Some(project_id),
                job_id: Some(job_id),
                chat_id: None,
                status: Some("live"),
                session_id: Some(session_id),
                error_message: None,
                started_at: Some(now),
                exited_at: None,
                created_at: now,
                updated_at: now,
                backend: None,
                exit_reason: None,
                start_mode: Some("resume"),
            })
            .execute(conn)
            .unwrap();
    }

    fn insert_yielded_turn(
        conn: &mut SqliteConnection,
        turn_id: &str,
        session_id: &str,
        run_id: &str,
        job_id: &str,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(crate::schema::turns::table)
            .values(&crate::diesel_models::NewTurn {
                id: turn_id,
                session_id,
                run_id: Some(run_id),
                job_id: Some(job_id),
                manager_id: None,
                sequence: 1,
                predecessor_id: None,
                state: "yielded",
                yield_reason: Some("user_input"),
                start_reason: "initial",
                created_at: now,
                started_at: Some(now),
                ended_at: None,
                updated_at: now,
            })
            .execute(conn)
            .unwrap();

        diesel::update(jobs::table.find(job_id))
            .set(jobs::current_turn_id.eq(Some(turn_id)))
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn record_prompt_response_is_idempotent_and_reuses_successor() {
        let mut conn = test_diesel_conn();
        let em = emitter();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Prompt Recovery");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "running", None);
        insert_session(&mut conn, "s1", &job);
        insert_bound_run(&mut conn, "r1", &issue, &proj, &job, "s1");
        insert_yielded_turn(&mut conn, "t1", "s1", "r1", &job);

        diesel::insert_into(crate::schema::prompts::table)
            .values(&crate::diesel_models::NewPrompt {
                id: "prompt-1",
                run_id: "r1",
                questions: "[]",
                response: None,
                created_at: chrono::Utc::now().timestamp() as i32,
                answered_at: None,
                turn_id: Some("t1"),
            })
            .execute(&mut conn)
            .unwrap();

        let first = record_prompt_response(&mut conn, "prompt-1", "approved", 111, &em).unwrap();
        let second = record_prompt_response(&mut conn, "prompt-1", "duplicate", 222, &em).unwrap();

        assert!(!first.duplicate);
        assert!(second.duplicate);
        assert_eq!(first.run_id, "r1");
        assert_eq!(first.predecessor_turn_id.as_deref(), Some("t1"));
        assert_eq!(first.successor_turn_id, second.successor_turn_id);

        let prompt_row: (Option<String>, Option<i32>) = crate::schema::prompts::table
            .find("prompt-1")
            .select((
                crate::schema::prompts::response,
                crate::schema::prompts::answered_at,
            ))
            .first(&mut conn)
            .unwrap();
        assert_eq!(prompt_row.0.as_deref(), Some("approved"));
        assert_eq!(prompt_row.1, Some(111));

        let successor_count: i64 = crate::schema::turns::table
            .filter(crate::schema::turns::predecessor_id.eq("t1"))
            .count()
            .get_result(&mut conn)
            .unwrap();
        assert_eq!(successor_count, 1);

        let current_turn: Option<String> = jobs::table
            .find(&job)
            .select(jobs::current_turn_id)
            .first(&mut conn)
            .unwrap();
        assert_eq!(current_turn, first.successor_turn_id);
    }

    #[test]
    fn record_permission_response_is_idempotent_and_reuses_successor() {
        let mut conn = test_diesel_conn();
        let em = emitter();
        let proj = create_test_project(&mut conn, "P", "P");
        let issue = create_test_issue(&mut conn, &proj, "Permission Recovery");
        let job = create_test_job(&mut conn, &issue, &proj, "plan", "running", None);
        insert_session(&mut conn, "s2", &job);
        insert_bound_run(&mut conn, "r2", &issue, &proj, &job, "s2");
        insert_yielded_turn(&mut conn, "t2", "s2", "r2", &job);

        diesel::insert_into(crate::schema::permission_requests::table)
            .values(&crate::diesel_models::NewPermissionRequest {
                id: "perm-1",
                run_id: "r2",
                tool_use_id: "tool-1",
                tool_name: "bash",
                tool_input: "{}",
                status: "pending",
                created_at: chrono::Utc::now().timestamp() as i32,
                turn_id: Some("t2"),
            })
            .execute(&mut conn)
            .unwrap();

        let first =
            record_permission_response(&mut conn, "perm-1", "approved", "{\"ok\":true}", 333, &em)
                .unwrap();
        let second =
            record_permission_response(&mut conn, "perm-1", "denied", "{\"ok\":false}", 444, &em)
                .unwrap();

        assert!(!first.duplicate);
        assert!(second.duplicate);
        assert_eq!(first.run_id, "r2");
        assert_eq!(first.predecessor_turn_id.as_deref(), Some("t2"));
        assert_eq!(first.successor_turn_id, second.successor_turn_id);

        let permission_row: (String, Option<String>, Option<i32>) =
            crate::schema::permission_requests::table
                .find("perm-1")
                .select((
                    crate::schema::permission_requests::status,
                    crate::schema::permission_requests::response,
                    crate::schema::permission_requests::responded_at,
                ))
                .first(&mut conn)
                .unwrap();
        assert_eq!(permission_row.0, "approved");
        assert_eq!(permission_row.1.as_deref(), Some("{\"ok\":true}"));
        assert_eq!(permission_row.2, Some(333));

        let successor_count: i64 = crate::schema::turns::table
            .filter(crate::schema::turns::predecessor_id.eq("t2"))
            .count()
            .get_result(&mut conn)
            .unwrap();
        assert_eq!(successor_count, 1);
    }
}

/// Create a successor turn after a yield or follow-up.
///
/// The predecessor must be in a terminal state (yielded, complete, failed, interrupted).
pub fn create_successor_turn(
    conn: &mut SqliteConnection,
    turn_id: &str,
    session_id: &str,
    job_id: &str,
    predecessor_id: &str,
    start_reason: TurnStartReason,
    emitter: &dyn EventEmitter,
) -> Result<Turn, String> {
    // Verify predecessor is terminal
    let pred_state: String = turns::table
        .find(predecessor_id)
        .select(turns::state)
        .first(conn)
        .map_err(|e| format!("Predecessor turn not found: {}", e))?;

    let pred_state: TurnState = pred_state
        .parse()
        .map_err(|e: String| format!("Invalid predecessor state: {}", e))?;

    if !pred_state.is_terminal() {
        return Err(format!(
            "Predecessor turn {} is in non-terminal state {:?}",
            predecessor_id, pred_state
        ));
    }

    let now = chrono::Utc::now().timestamp() as i32;
    let sequence = next_sequence(conn, session_id)?;

    let state_str = TurnState::Pending.to_string();
    let reason_str = start_reason.to_string();

    let new_turn = NewTurn {
        id: turn_id,
        session_id,
        run_id: None, // Late-bound
        job_id: Some(job_id),
        manager_id: None,
        sequence,
        predecessor_id: Some(predecessor_id),
        state: &state_str,
        yield_reason: None,
        start_reason: &reason_str,
        created_at: now,
        started_at: None,
        ended_at: None,
        updated_at: now,
    };

    create_turn(conn, &new_turn, emitter)
}

/// Return the existing successor turn if one already exists, otherwise create it.
pub fn ensure_successor_turn(
    conn: &mut SqliteConnection,
    turn_id: &str,
    session_id: &str,
    job_id: &str,
    predecessor_id: &str,
    start_reason: TurnStartReason,
    emitter: &dyn EventEmitter,
) -> Result<Turn, String> {
    if let Some(existing) = get_successor_turn(conn, predecessor_id)? {
        return Ok(existing);
    }

    create_successor_turn(
        conn,
        turn_id,
        session_id,
        job_id,
        predecessor_id,
        start_reason,
        emitter,
    )
}

#[derive(Debug, Clone)]
pub struct HostWaitResume {
    pub run_id: String,
    pub issue_id: Option<String>,
    pub predecessor_turn_id: Option<String>,
    pub successor_turn_id: Option<String>,
    pub duplicate: bool,
}

/// Persist a prompt response exactly once and ensure the yielded turn has at most one successor.
pub fn record_prompt_response(
    conn: &mut SqliteConnection,
    prompt_id: &str,
    response: &str,
    answered_at: i32,
    emitter: &dyn EventEmitter,
) -> Result<HostWaitResume, String> {
    conn.transaction::<HostWaitResume, diesel::result::Error, _>(|conn| {
        let (run_id, issue_id, predecessor_turn_id, job_id, session_id, already_answered): (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            bool,
        ) = prompts::table
            .inner_join(runs::table.on(prompts::run_id.eq(runs::id)))
            .filter(prompts::id.eq(prompt_id))
            .select((
                prompts::run_id,
                runs::issue_id,
                prompts::turn_id,
                runs::job_id,
                runs::session_id,
                prompts::response.is_not_null(),
            ))
            .first(conn)?;

        let duplicate = if already_answered {
            true
        } else {
            diesel::update(
                prompts::table
                    .filter(prompts::id.eq(prompt_id))
                    .filter(prompts::response.is_null()),
            )
            .set((
                prompts::response.eq(Some(response)),
                prompts::answered_at.eq(Some(answered_at)),
            ))
            .execute(conn)?
                == 0
        };

        let successor_turn_id = if let (Some(pred_turn_id), Some(job_id), Some(session_id)) = (
            predecessor_turn_id.as_deref(),
            job_id.as_deref(),
            session_id.as_deref(),
        ) {
            let successor = ensure_successor_turn(
                conn,
                &uuid::Uuid::new_v4().to_string(),
                session_id,
                job_id,
                pred_turn_id,
                TurnStartReason::PromptResponse,
                emitter,
            )
            .map_err(|e| diesel::result::Error::QueryBuilderError(e.into()))?;
            Some(successor.id)
        } else {
            None
        };

        Ok(HostWaitResume {
            run_id,
            issue_id,
            predecessor_turn_id,
            successor_turn_id,
            duplicate,
        })
    })
    .map_err(|e| e.to_string())
}

/// Persist a permission response exactly once and ensure the yielded turn has at most one successor.
pub fn record_permission_response(
    conn: &mut SqliteConnection,
    request_id: &str,
    status: &str,
    response_json: &str,
    responded_at: i32,
    emitter: &dyn EventEmitter,
) -> Result<HostWaitResume, String> {
    conn.transaction::<HostWaitResume, diesel::result::Error, _>(|conn| {
        let (run_id, issue_id, predecessor_turn_id, job_id, session_id, current_status): (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
        ) = permission_requests::table
            .inner_join(runs::table.on(permission_requests::run_id.eq(runs::id)))
            .filter(permission_requests::id.eq(request_id))
            .select((
                permission_requests::run_id,
                runs::issue_id,
                permission_requests::turn_id,
                runs::job_id,
                runs::session_id,
                permission_requests::status,
            ))
            .first(conn)?;

        let duplicate = if current_status != "pending" {
            true
        } else {
            diesel::update(
                permission_requests::table
                    .filter(permission_requests::id.eq(request_id))
                    .filter(permission_requests::status.eq("pending")),
            )
            .set((
                permission_requests::status.eq(status),
                permission_requests::response.eq(Some(response_json)),
                permission_requests::responded_at.eq(Some(responded_at)),
            ))
            .execute(conn)?
                == 0
        };

        let successor_turn_id = if let (Some(pred_turn_id), Some(job_id), Some(session_id)) = (
            predecessor_turn_id.as_deref(),
            job_id.as_deref(),
            session_id.as_deref(),
        ) {
            let successor = ensure_successor_turn(
                conn,
                &uuid::Uuid::new_v4().to_string(),
                session_id,
                job_id,
                pred_turn_id,
                TurnStartReason::PermissionResponse,
                emitter,
            )
            .map_err(|e| diesel::result::Error::QueryBuilderError(e.into()))?;
            Some(successor.id)
        } else {
            None
        };

        Ok(HostWaitResume {
            run_id,
            issue_id,
            predecessor_turn_id,
            successor_turn_id,
            duplicate,
        })
    })
    .map_err(|e| e.to_string())
}
