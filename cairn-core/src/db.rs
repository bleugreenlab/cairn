//! Database types for Cairn Core.
//!
//! Contains the connection state wrapper and migration status types.
//! Database initialization and path resolution remain in the Tauri crate
//! since they depend on platform-specific app data directories.

use diesel::sqlite::SqliteConnection;
use diesel_migrations::{embed_migrations, EmbeddedMigrations};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

// Embed Diesel migrations
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("../../diesel_migrations");

/// Database state wrapping a Diesel SQLite connection.
///
/// Thread-safe via Mutex. Used by both Tauri state management
/// and potential server deployments.
pub struct DbState {
    pub conn: Mutex<SqliteConnection>,
}

// ============================================================================
// Migration Status Types (for frontend communication)
// ============================================================================

/// Status check result for migration UI
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationStatus {
    pub needed: bool,
    pub pending_migrations: Vec<String>,
    pub current_db_path: String,
    pub error_message: Option<String>,
}

/// Schema change detected during migration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaChange {
    pub table: String,
    pub change_type: String,
    pub old_name: Option<String>,
    pub new_name: Option<String>,
    pub auto_mapped: bool,
}

/// Per-table result for frontend display
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMigrationResult {
    pub name: String,
    pub old_count: usize,
    pub new_count: usize,
    pub status: String,
    pub error: Option<String>,
}

/// Final migration result for frontend display
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationResult {
    pub success: bool,
    pub tables: Vec<TableMigrationResult>,
    pub schema_changes: Vec<SchemaChange>,
    pub total_rows_restored: usize,
    pub total_rows_attempted: usize,
    pub warnings: Vec<String>,
}

/// Results from startup recovery.
///
/// Contains outbox entries to replay and manager job IDs to wake.
pub struct StartupRecovery {
    /// Pending outbox entries that need to be replayed after Orchestrator is built.
    pub outbox_entries: Vec<crate::effects::outbox::OutboxEntry>,
    /// Job IDs on managed issues that were reset to failed and need manager wakes.
    pub manager_wake_job_ids: Vec<String>,
}

/// Prepare effect outbox for startup replay.
///
/// 1. Resets `running` outbox entries to `pending` (they were mid-flight during crash)
/// 2. GCs old `done` entries
/// 3. Drains and returns pending entries for the caller to replay
pub fn prepare_outbox_replay(
    conn: &mut SqliteConnection,
) -> Vec<crate::effects::outbox::OutboxEntry> {
    let reset = crate::effects::outbox::reset_running_to_pending(conn);
    if reset > 0 {
        log::info!(
            "Reset {} in-flight outbox entries to pending for replay",
            reset
        );
    }

    let gc_count = crate::effects::outbox::gc(conn);
    if gc_count > 0 {
        log::debug!("GC'd {} old outbox entries", gc_count);
    }

    crate::effects::outbox::drain_pending(conn)
}

/// Collect job IDs on managed issues that were reset to failed during orphan cleanup.
///
/// These need manager wake-ups after the Orchestrator is built.
pub fn recover_orphaned_streams(conn: &mut SqliteConnection) -> usize {
    let recoverable =
        crate::transcripts::stream_store::list_recoverable_streams(conn).unwrap_or_default();
    let mut recovered = 0;

    for stream in recoverable {
        let result = if stream.content.is_empty() && stream.thinking.is_empty() {
            crate::transcripts::stream_store::abort_stream(
                conn,
                stream.stream_id(),
                stream.version(),
                "crash",
            )
            .map(|_| ())
        } else {
            crate::transcripts::stream_store::finalize_stream(
                conn,
                stream.stream_id(),
                stream.version(),
                None,
            )
            .map(|_| ())
        };

        match result {
            Ok(()) => recovered += 1,
            Err(error) => {
                log::warn!(
                    "Failed to recover stream {} during startup: {}",
                    stream.stream_id(),
                    error
                );
            }
        }
    }

    if recovered > 0 {
        log::info!("Recovered {} orphaned transcript streams", recovered);
    }

    recovered
}

pub fn collect_manager_wake_jobs(
    conn: &mut SqliteConnection,
    affected_job_ids: &[String],
) -> Vec<String> {
    if affected_job_ids.is_empty() {
        return Vec::new();
    }

    use crate::schema::{issues, jobs};
    use diesel::prelude::*;

    jobs::table
        .inner_join(issues::table)
        .filter(jobs::id.eq_any(affected_job_ids))
        .filter(issues::manager_id.is_not_null())
        .filter(jobs::parent_job_id.is_null())
        .select(jobs::id)
        .load(conn)
        .unwrap_or_default()
}

/// Cleanup orphaned running states from previous crashes or force-quits.
///
/// On startup, any in-flight jobs/runs/turns cannot actually still be active
/// since the process just started. This resets them to terminal states and
/// recomputes execution and issue status so the UI reflects reality.
///
/// Returns the IDs of jobs that were reset to failed (for manager wake-up).
pub fn cleanup_orphaned_states(conn: &mut SqliteConnection) -> Vec<String> {
    use crate::schema::*;
    use diesel::prelude::*;

    let now = chrono::Utc::now().timestamp() as i32;

    let running_jobs: Vec<(String, Option<String>, Option<String>, Option<String>)> = jobs::table
        .filter(jobs::status.eq("running"))
        .select((
            jobs::id,
            jobs::current_turn_id,
            jobs::execution_id,
            jobs::issue_id,
        ))
        .load(conn)
        .unwrap_or_default();

    let mut resumable_job_ids = Vec::new();
    let mut resumable_pending_turn_ids = Vec::new();
    let mut failed_job_ids = Vec::new();
    let mut affected_exec_ids = Vec::new();
    let mut affected_issue_ids = Vec::new();

    for (job_id, current_turn_id, execution_id, issue_id) in running_jobs {
        let mut preserve_job = false;

        if let Some(turn_id) = current_turn_id.as_deref() {
            let turn_info: Option<(String, Option<String>, Option<String>)> = turns::table
                .find(turn_id)
                .select((turns::state, turns::yield_reason, turns::predecessor_id))
                .first(conn)
                .optional()
                .unwrap_or_default();

            if let Some((state, _yield_reason, predecessor_id)) = turn_info {
                if state == "yielded" {
                    let has_open_prompt: bool = prompts::table
                        .filter(prompts::turn_id.eq(turn_id))
                        .filter(prompts::response.is_null())
                        .count()
                        .get_result::<i64>(conn)
                        .unwrap_or(0)
                        > 0;
                    let has_open_permission: bool = permission_requests::table
                        .filter(permission_requests::turn_id.eq(turn_id))
                        .filter(permission_requests::status.eq("pending"))
                        .count()
                        .get_result::<i64>(conn)
                        .unwrap_or(0)
                        > 0;
                    preserve_job = has_open_prompt || has_open_permission;
                } else if state == "pending" && predecessor_id.is_some() {
                    // Response delivery may have already created the successor turn before restart.
                    preserve_job = true;
                    resumable_pending_turn_ids.push(turn_id.to_string());
                }
            }
        }

        if let Some(exec_id) = execution_id {
            affected_exec_ids.push(exec_id);
        }
        if let Some(issue_id) = issue_id {
            affected_issue_ids.push(issue_id);
        }

        if preserve_job {
            resumable_job_ids.push(job_id);
        } else {
            failed_job_ids.push(job_id);
        }
    }

    affected_exec_ids.sort();
    affected_exec_ids.dedup();
    affected_issue_ids.sort();
    affected_issue_ids.dedup();

    // Reset jobs stuck at running status, except resumable host waits.
    match diesel::update(
        jobs::table
            .filter(jobs::status.eq("running"))
            .filter(jobs::id.ne_all(&resumable_job_ids)),
    )
    .set(jobs::status.eq("failed"))
    .execute(conn)
    {
        Ok(count) if count > 0 => {
            log::info!("Reset {} orphaned jobs to failed status", count);
        }
        Ok(_) => {}
        Err(e) => {
            log::warn!("Failed to reset orphaned jobs: {}", e);
        }
    }

    if !resumable_job_ids.is_empty() {
        log::info!(
            "Preserved {} running jobs as resumable host waits during startup recovery",
            resumable_job_ids.len()
        );
    }

    // Reset runs stuck at starting or live (new statuses) or legacy pending/running
    match diesel::update(
        runs::table.filter(runs::status.eq_any(&["starting", "live", "pending", "running"])),
    )
    .set((
        runs::status.eq("crashed"),
        runs::exit_reason.eq(Some("crash")),
        runs::exited_at.eq(Some(now)),
    ))
    .execute(conn)
    {
        Ok(count) if count > 0 => {
            log::info!("Reset {} orphaned runs to crashed status", count);
        }
        Ok(_) => {}
        Err(e) => {
            log::warn!("Failed to reset orphaned runs: {}", e);
        }
    }

    match diesel::update(
        turns::table
            .filter(turns::state.eq("pending"))
            .filter(turns::id.ne_all(&resumable_pending_turn_ids)),
    )
    .set((
        turns::state.eq("failed"),
        turns::ended_at.eq(Some(now)),
        turns::updated_at.eq(now),
    ))
    .execute(conn)
    {
        Ok(count) if count > 0 => {
            log::info!("Reset {} orphaned pending turns to failed", count);
        }
        Ok(_) => {}
        Err(e) => {
            log::warn!("Failed to reset orphaned pending turns: {}", e);
        }
    }

    match diesel::update(turns::table.filter(turns::state.eq("running")))
        .set((
            turns::state.eq("interrupted"),
            turns::ended_at.eq(Some(now)),
            turns::updated_at.eq(now),
        ))
        .execute(conn)
    {
        Ok(count) if count > 0 => {
            log::info!("Reset {} orphaned running turns to interrupted", count);
        }
        Ok(_) => {}
        Err(e) => {
            log::warn!("Failed to reset orphaned running turns: {}", e);
        }
    }

    // Delete terminals stuck at running status (PTY sessions don't persist across restarts)
    match diesel::delete(job_terminals::table.filter(job_terminals::status.eq("running")))
        .execute(conn)
    {
        Ok(count) if count > 0 => {
            log::info!("Deleted {} orphaned terminals", count);
        }
        Ok(_) => {}
        Err(e) => {
            log::warn!("Failed to delete orphaned terminals: {}", e);
        }
    }

    // Recompute execution status for affected executions
    for exec_id in &affected_exec_ids {
        crate::transitions::recompute_execution_status(conn, exec_id);
    }

    // Recompute issue status for affected issues
    for issue_id in &affected_issue_ids {
        crate::transitions::recompute_issue_status(conn, issue_id);
    }

    if !affected_exec_ids.is_empty() {
        log::info!(
            "Recomputed status for {} executions and {} issues after orphan cleanup",
            affected_exec_ids.len(),
            affected_issue_ids.len()
        );
    }

    failed_job_ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::{NewExecution, NewJob, NewJobTerminal, NewRun, NewTurn};
    use crate::schema::*;
    use crate::test_utils::{create_test_issue, create_test_project, test_diesel_conn};
    use diesel::prelude::*;

    fn insert_job(
        conn: &mut SqliteConnection,
        id: &str,
        status: &str,
        project_id: &str,
        execution_id: Option<&str>,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_job = NewJob {
            id,
            execution_id,
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: None,
            worktree_path: None,
            branch: None,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status,
            agent_config_id: None,
            issue_id: None,
            project_id,
            task_description: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: None,
            task_index: None,
            started_at: if status == "running" { Some(now) } else { None },
            model: None,
            node_name: None,
            base_branch: None,
            current_turn_id: None,
        };
        diesel::insert_into(jobs::table)
            .values(&new_job)
            .execute(conn)
            .unwrap();
    }

    fn insert_run(conn: &mut SqliteConnection, id: &str, status: &str, job_id: Option<&str>) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_run = NewRun {
            id,
            issue_id: None,
            project_id: None,
            job_id,
            status: Some(status),
            session_id: None,
            error_message: None,
            started_at: if status == "running" { Some(now) } else { None },
            exited_at: None,
            created_at: now,
            updated_at: now,
            backend: None,
            exit_reason: None,
            start_mode: None,
            chat_id: None,
        };
        diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(conn)
            .unwrap();
    }

    fn insert_terminal(conn: &mut SqliteConnection, id: &str, status: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_terminal = NewJobTerminal {
            id,
            job_id: None,
            project_id: None,
            run_id: None,
            session_id: "sess-1",
            command: "echo test",
            title: None,
            description: None,
            status,
            exit_code: None,
            created_at: now,
            exited_at: None,
            slug: None,
        };
        diesel::insert_into(job_terminals::table)
            .values(&new_terminal)
            .execute(conn)
            .unwrap();
    }

    fn insert_turn(conn: &mut SqliteConnection, id: &str, state: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_turn = NewTurn {
            id,
            session_id: id,
            run_id: None,
            job_id: None,
            manager_id: None,
            sequence: 1,
            predecessor_id: None,
            state,
            yield_reason: None,
            start_reason: "initial",
            created_at: now,
            started_at: if state == "running" { Some(now) } else { None },
            ended_at: None,
            updated_at: now,
        };
        diesel::insert_into(turns::table)
            .values(&new_turn)
            .execute(conn)
            .unwrap();
    }

    fn insert_turn_for_job(
        conn: &mut SqliteConnection,
        id: &str,
        session_id: &str,
        run_id: Option<&str>,
        job_id: &str,
        predecessor_id: Option<&str>,
        state: &str,
        yield_reason: Option<&str>,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_turn = NewTurn {
            id,
            session_id,
            run_id,
            job_id: Some(job_id),
            manager_id: None,
            sequence: if predecessor_id.is_some() { 2 } else { 1 },
            predecessor_id,
            state,
            yield_reason,
            start_reason: if predecessor_id.is_some() {
                "prompt_response"
            } else {
                "initial"
            },
            created_at: now,
            started_at: if matches!(state, "running" | "yielded") {
                Some(now)
            } else {
                None
            },
            ended_at: None,
            updated_at: now,
        };
        diesel::insert_into(turns::table)
            .values(&new_turn)
            .execute(conn)
            .unwrap();
    }

    fn insert_session(conn: &mut SqliteConnection, id: &str, job_id: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_session = crate::diesel_models::NewSession {
            id,
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
        };
        diesel::insert_into(sessions::table)
            .values(&new_session)
            .execute(conn)
            .unwrap();
    }

    fn insert_execution(conn: &mut SqliteConnection, id: &str, issue_id: Option<&str>) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_exec = NewExecution {
            id,
            recipe_id: "recipe-1",
            issue_id,
            project_id: None,
            status: "running",
            started_at: now,
            completed_at: None,
            snapshot: None,
            seq: Some(1),
            initiator_sub: None,
            initiator_auth_mode: None,
            initiator_org_id: None,
            triggered_by: "manual",
        };
        diesel::insert_into(executions::table)
            .values(&new_exec)
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn test_resets_orphaned_running_jobs() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");

        insert_job(&mut conn, "job-running", "running", &project_id, None);
        insert_job(&mut conn, "job-pending", "pending", &project_id, None);
        insert_job(&mut conn, "job-complete", "complete", &project_id, None);

        cleanup_orphaned_states(&mut conn);

        let statuses: Vec<(String, String)> = jobs::table
            .select((jobs::id, jobs::status))
            .order(jobs::id)
            .load(&mut conn)
            .unwrap();

        let status_map: std::collections::HashMap<_, _> = statuses.into_iter().collect();
        assert_eq!(status_map["job-running"], "failed");
        assert_eq!(status_map["job-pending"], "pending"); // untouched
        assert_eq!(status_map["job-complete"], "complete"); // untouched
    }

    #[test]
    fn test_resets_orphaned_pending_and_running_runs() {
        let mut conn = test_diesel_conn();

        insert_run(&mut conn, "run-live", "live", None);
        insert_run(&mut conn, "run-starting", "starting", None);
        insert_run(&mut conn, "run-exited", "exited", None);
        insert_run(&mut conn, "run-legacy-running", "running", None);

        cleanup_orphaned_states(&mut conn);

        let statuses: Vec<(String, Option<String>)> = runs::table
            .select((runs::id, runs::status))
            .order(runs::id)
            .load(&mut conn)
            .unwrap();

        let status_map: std::collections::HashMap<_, _> = statuses.into_iter().collect();
        assert_eq!(status_map["run-live"].as_deref(), Some("crashed"));
        assert_eq!(status_map["run-starting"].as_deref(), Some("crashed"));
        assert_eq!(status_map["run-exited"].as_deref(), Some("exited")); // untouched
        assert_eq!(status_map["run-legacy-running"].as_deref(), Some("crashed"));
        // legacy compat
    }

    #[test]
    fn test_deletes_orphaned_running_terminals() {
        let mut conn = test_diesel_conn();

        insert_terminal(&mut conn, "term-running", "running");
        insert_terminal(&mut conn, "term-exited", "exited");

        cleanup_orphaned_states(&mut conn);

        let remaining: Vec<String> = job_terminals::table
            .select(job_terminals::id)
            .order(job_terminals::id)
            .load(&mut conn)
            .unwrap();

        assert_eq!(remaining, vec!["term-exited"]);
    }

    #[test]
    fn test_terminalizes_orphaned_pending_and_running_turns() {
        let mut conn = test_diesel_conn();

        insert_turn(&mut conn, "turn-pending", "pending");
        insert_turn(&mut conn, "turn-running", "running");
        insert_turn(&mut conn, "turn-yielded", "yielded");

        cleanup_orphaned_states(&mut conn);

        let turns_after: Vec<(String, String, Option<i32>)> = turns::table
            .select((turns::id, turns::state, turns::ended_at))
            .order(turns::id)
            .load(&mut conn)
            .unwrap();

        let turn_map: std::collections::HashMap<_, _> = turns_after
            .into_iter()
            .map(|(id, state, ended_at)| (id, (state, ended_at)))
            .collect();

        assert_eq!(turn_map["turn-pending"].0, "failed");
        assert!(turn_map["turn-pending"].1.is_some());
        assert_eq!(turn_map["turn-running"].0, "interrupted");
        assert!(turn_map["turn-running"].1.is_some());
        assert_eq!(turn_map["turn-yielded"].0, "yielded");
        assert!(turn_map["turn-yielded"].1.is_none());
    }

    #[test]
    fn test_recomputes_execution_and_issue_status_after_cleanup() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test Issue");

        // Create execution linked to issue
        insert_execution(&mut conn, "exec-1", Some(&issue_id));

        // Create a running job in that execution — will be reset to failed
        insert_job(&mut conn, "job-1", "running", &project_id, Some("exec-1"));

        // Issue should be active before cleanup (has running execution)
        let status_before: String = issues::table
            .find(&issue_id)
            .select(issues::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(status_before, "backlog"); // initial status from create_test_issue

        cleanup_orphaned_states(&mut conn);

        // After cleanup: job is failed, execution should be recomputed to failed,
        // issue should be recomputed (still backlog since execution is failed, not running)
        let job_status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(job_status, "failed");

        let exec_status: String = executions::table
            .find("exec-1")
            .select(executions::status)
            .first(&mut conn)
            .unwrap();
        // Execution with all-failed jobs should be recomputed to failed
        assert_eq!(exec_status, "failed");
    }

    #[test]
    fn test_preserves_running_job_with_open_prompt_wait() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "WAITP");
        let issue_id = create_test_issue(&mut conn, &project_id, "Prompt wait");

        insert_execution(&mut conn, "exec-wait", Some(&issue_id));
        insert_job(
            &mut conn,
            "job-wait",
            "running",
            &project_id,
            Some("exec-wait"),
        );
        insert_session(&mut conn, "session-wait", "job-wait");
        insert_run(&mut conn, "run-wait", "live", Some("job-wait"));
        diesel::update(runs::table.find("run-wait"))
            .set((
                runs::issue_id.eq(Some(issue_id.as_str())),
                runs::project_id.eq(Some(project_id.as_str())),
                runs::session_id.eq(Some("session-wait")),
            ))
            .execute(&mut conn)
            .unwrap();
        insert_turn_for_job(
            &mut conn,
            "turn-wait",
            "session-wait",
            Some("run-wait"),
            "job-wait",
            None,
            "yielded",
            Some("user_input"),
        );
        diesel::update(jobs::table.find("job-wait"))
            .set((
                jobs::issue_id.eq(Some(issue_id.as_str())),
                jobs::execution_id.eq(Some("exec-wait")),
                jobs::current_turn_id.eq(Some("turn-wait")),
            ))
            .execute(&mut conn)
            .unwrap();
        diesel::insert_into(prompts::table)
            .values(&crate::diesel_models::NewPrompt {
                id: "prompt-open",
                run_id: "run-wait",
                questions: "[]",
                response: None,
                created_at: chrono::Utc::now().timestamp() as i32,
                answered_at: None,
                turn_id: Some("turn-wait"),
            })
            .execute(&mut conn)
            .unwrap();
        diesel::update(issues::table.find(&issue_id))
            .set((
                issues::status.eq("active"),
                issues::progress.eq("active"),
                issues::attention.eq("none"),
            ))
            .execute(&mut conn)
            .unwrap();

        let failed_jobs = cleanup_orphaned_states(&mut conn);

        assert!(failed_jobs.is_empty());
        let job_status: String = jobs::table
            .find("job-wait")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(job_status, "running");

        let turn_state: String = turns::table
            .find("turn-wait")
            .select(turns::state)
            .first(&mut conn)
            .unwrap();
        assert_eq!(turn_state, "yielded");

        let issue_projection: (String, String, String) = issues::table
            .find(&issue_id)
            .select((issues::status, issues::progress, issues::attention))
            .first(&mut conn)
            .unwrap();
        assert_eq!(issue_projection.0, "waiting");
        assert_eq!(issue_projection.1, "active");
        assert_eq!(issue_projection.2, "needs_input");
    }

    #[test]
    fn test_preserves_pending_successor_turn_created_before_restart() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "WAITS");
        let issue_id = create_test_issue(&mut conn, &project_id, "Successor wait");

        insert_execution(&mut conn, "exec-successor", Some(&issue_id));
        let now = chrono::Utc::now().timestamp() as i32;
        let new_job = NewJob {
            id: "job-successor",
            execution_id: Some("exec-successor"),
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: None,
            worktree_path: None,
            branch: None,
            base_commit: None,
            current_session_id: Some("session-successor"),
            resume_session_id: None,
            status: "running",
            agent_config_id: None,
            issue_id: Some(&issue_id),
            project_id: &project_id,
            task_description: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: None,
            task_index: None,
            started_at: Some(now),
            model: None,
            node_name: None,
            base_branch: None,
            current_turn_id: None,
        };
        diesel::insert_into(jobs::table)
            .values(&new_job)
            .execute(&mut conn)
            .unwrap();
        insert_session(&mut conn, "session-successor", "job-successor");
        insert_run(&mut conn, "run-successor", "live", Some("job-successor"));
        diesel::update(runs::table.find("run-successor"))
            .set((
                runs::issue_id.eq(Some(issue_id.as_str())),
                runs::project_id.eq(Some(project_id.as_str())),
                runs::session_id.eq(Some("session-successor")),
            ))
            .execute(&mut conn)
            .unwrap();
        insert_turn_for_job(
            &mut conn,
            "turn-predecessor",
            "session-successor",
            Some("run-successor"),
            "job-successor",
            None,
            "yielded",
            Some("permission"),
        );
        diesel::insert_into(permission_requests::table)
            .values(&crate::diesel_models::NewPermissionRequest {
                id: "perm-answered",
                run_id: "run-successor",
                tool_use_id: "tool-1",
                tool_name: "bash",
                tool_input: "{}",
                status: "approved",
                created_at: now,
                turn_id: Some("turn-predecessor"),
            })
            .execute(&mut conn)
            .unwrap();
        insert_turn_for_job(
            &mut conn,
            "turn-successor",
            "session-successor",
            None,
            "job-successor",
            Some("turn-predecessor"),
            "pending",
            None,
        );
        diesel::update(jobs::table.find("job-successor"))
            .set(jobs::current_turn_id.eq(Some("turn-successor")))
            .execute(&mut conn)
            .unwrap();
        diesel::update(issues::table.find(&issue_id))
            .set((
                issues::status.eq("waiting"),
                issues::progress.eq("active"),
                issues::attention.eq("needs_authorization"),
            ))
            .execute(&mut conn)
            .unwrap();

        let failed_jobs = cleanup_orphaned_states(&mut conn);

        assert!(failed_jobs.is_empty());
        let job_status: String = jobs::table
            .find("job-successor")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(job_status, "running");

        let turn_state: String = turns::table
            .find("turn-successor")
            .select(turns::state)
            .first(&mut conn)
            .unwrap();
        assert_eq!(turn_state, "pending");

        let issue_projection: (String, String, String) = issues::table
            .find(&issue_id)
            .select((issues::status, issues::progress, issues::attention))
            .first(&mut conn)
            .unwrap();
        assert_eq!(issue_projection.0, "active");
        assert_eq!(issue_projection.1, "active");
        assert_eq!(issue_projection.2, "none");
    }

    #[test]
    fn test_noop_when_no_orphans() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");

        insert_job(&mut conn, "job-1", "complete", &project_id, None);
        insert_run(&mut conn, "run-1", "complete", None);

        // Should not panic or error
        cleanup_orphaned_states(&mut conn);

        let job_status: String = jobs::table
            .find("job-1")
            .select(jobs::status)
            .first(&mut conn)
            .unwrap();
        assert_eq!(job_status, "complete");
    }

    // === Outbox and manager wake tests ===

    #[test]
    fn test_cleanup_returns_affected_job_ids() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "AFF");

        insert_job(&mut conn, "job-running", "running", &project_id, None);
        insert_job(&mut conn, "job-complete", "complete", &project_id, None);

        let affected = cleanup_orphaned_states(&mut conn);
        assert_eq!(affected, vec!["job-running"]);
    }

    fn insert_manager(conn: &mut SqliteConnection, id: &str, project_id: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        let mgr = crate::diesel_models::NewManager {
            id,
            project_id,
            home_project_id: None,
            scope_kind: "branch",
            name: "Test Manager",
            description: "test",
            branch: Some("main"),
            job_id: None,
            status: "active",
            current_session_id: None,
            current_turn_id: None,
            last_wake_at: None,
            last_turn_completed_at: None,
            last_error: None,
            agent_config_id: None,
            model: None,
            parent_manager_id: None,
            created_at: now,
            updated_at: now,
            execution_id: None,
        };
        diesel::insert_into(crate::schema::managers::table)
            .values(&mgr)
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn test_collect_manager_wake_jobs_filters_managed_issues() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "MWK");
        let issue_id = create_test_issue(&mut conn, &project_id, "Managed issue");

        // Create a manager and assign it to the issue
        insert_manager(&mut conn, "mgr-1", &project_id);
        diesel::update(issues::table.find(&issue_id))
            .set(issues::manager_id.eq(Some("mgr-1")))
            .execute(&mut conn)
            .unwrap();

        // Insert a top-level job on the managed issue
        let now = chrono::Utc::now().timestamp() as i32;
        let managed_job = NewJob {
            id: "job-managed",
            execution_id: None,
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: None,
            worktree_path: None,
            branch: None,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status: "failed",
            agent_config_id: None,
            issue_id: Some(&issue_id),
            project_id: &project_id,
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
            .values(&managed_job)
            .execute(&mut conn)
            .unwrap();

        // Insert a job without an issue (should not appear in results)
        insert_job(&mut conn, "job-no-issue", "failed", &project_id, None);

        let wake_jobs = collect_manager_wake_jobs(
            &mut conn,
            &["job-managed".to_string(), "job-no-issue".to_string()],
        );
        assert_eq!(wake_jobs, vec!["job-managed"]);
    }

    #[test]
    fn test_collect_manager_wake_jobs_excludes_child_jobs() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "MWC");
        let issue_id = create_test_issue(&mut conn, &project_id, "Managed issue");

        insert_manager(&mut conn, "mgr-1", &project_id);
        diesel::update(issues::table.find(&issue_id))
            .set(issues::manager_id.eq(Some("mgr-1")))
            .execute(&mut conn)
            .unwrap();

        // Insert the parent job first (to satisfy FK)
        insert_job(&mut conn, "job-parent", "failed", &project_id, None);

        // Insert a child job (has parent_job_id) on the managed issue
        let now = chrono::Utc::now().timestamp() as i32;
        let child_job = NewJob {
            id: "job-child",
            execution_id: None,
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: Some("job-parent"),
            worktree_path: None,
            branch: None,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status: "failed",
            agent_config_id: None,
            issue_id: Some(&issue_id),
            project_id: &project_id,
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
            .values(&child_job)
            .execute(&mut conn)
            .unwrap();

        let wake_jobs = collect_manager_wake_jobs(&mut conn, &["job-child".to_string()]);
        assert!(
            wake_jobs.is_empty(),
            "Child jobs should not trigger manager wake"
        );
    }

    #[test]
    fn test_prepare_outbox_replay_resets_and_drains() {
        let mut conn = test_diesel_conn();

        // Insert a pending and a running entry
        crate::effects::outbox::insert_pending(&mut conn, "advance_dag", "exec-1");
        crate::effects::outbox::insert_pending(&mut conn, "advance_dag", "exec-2");

        // Drain exec-2 to set it to running (simulating mid-flight crash)
        let entries = crate::effects::outbox::drain_pending(&mut conn);
        assert_eq!(entries.len(), 2);

        // Now simulate crash recovery — prepare_outbox_replay should:
        // 1. Reset running back to pending
        // 2. GC old done entries (none here)
        // 3. Drain and return all pending
        let replay_entries = prepare_outbox_replay(&mut conn);
        assert_eq!(replay_entries.len(), 2);
    }

    #[test]
    fn test_recover_orphaned_streams_finalizes_non_empty_streams() {
        let mut conn = test_diesel_conn();
        insert_run(&mut conn, "run-stream", "live", None);
        let active = crate::transcripts::stream_store::open_stream(
            &mut conn,
            "run-stream",
            Some("session-stream"),
            None,
            "claude",
            Some(0),
        )
        .unwrap();
        let active = crate::transcripts::stream_store::append_chunks(
            &mut conn,
            active.stream_id(),
            active.version(),
            &[crate::transcripts::stream_store::StreamChunkInput::content(
                "hello",
            )],
        )
        .unwrap();
        assert_eq!(active.version(), 1);

        let recovered = recover_orphaned_streams(&mut conn);
        assert_eq!(recovered, 1);

        let event_count: i64 = events::table.count().get_result(&mut conn).unwrap();
        assert_eq!(event_count, 1);
    }

    #[test]
    fn test_collect_manager_wake_jobs_empty_on_no_affected() {
        let mut conn = test_diesel_conn();
        let result = collect_manager_wake_jobs(&mut conn, &[]);
        assert!(result.is_empty());
    }
}
