//! Regression coverage for the worktree-fence suspend/resume path (CAIRN-2123).
//!
//! The bug: a fenced `run()` whose shell command the kernel sandbox blocks
//! raised a permission crossing, and if it was raised during the warm-reuse
//! `Busy`-without-turn window the `permission_requests` row was stored with
//! `turn_id = NULL`. On the durable-suspend (slow) path the answer handler then
//! read a NULL predecessor, skipped successor-turn creation, and the run parked
//! forever — allow and deny both no-ops.
//!
//! These tests pin the three independent fixes:
//!  1. `record_permission_response` recovers a NULL predecessor from the job's
//!     persisted `jobs.current_turn_id` (job-owned only), so the successor turn
//!     is created and the run can resume.
//!  2. The project-chat case (no owning job) keeps its NULL turn and is never
//!     force-resumed.
//!  3. A fence-driven self-suspend leaves the run's sibling inline commands
//!     alive, while an external stop still reaps them.

use crate::common;

use std::sync::{Arc, Mutex};

use crate::common::orchestrator;
use cairn_core::internal::mcp::handlers::permission::record_permission_response;
use cairn_core::internal::orchestrator::lifecycle::{stop_session, suspend_run_for_durable_wait};
use cairn_core::internal::services::testing::MockChildProcess;
use cairn_core::internal::services::ChildProcess;
use cairn_core::internal::storage::{LocalDb, RowExt};
use turso::params;

/// Options for the suspend/resume fixture.
struct Fixture {
    /// Whether the run is owned by a job (false models a project-chat run).
    job_owned: bool,
    /// Whether the owning job has a persisted `current_turn_id` to recover.
    job_has_current_turn: bool,
}

/// Insert project/issue/execution/job/session/run plus one terminal predecessor
/// turn and a single job-owned fence `permission_requests` row stored with
/// `turn_id = NULL` (the warm-reuse race signature).
async fn insert_fixture(db: &LocalDb, opts: &Fixture) {
    let project_id = common::create_project(db, "TRS").await;
    let job_id_for_run: Option<&str> = if opts.job_owned { Some("job-1") } else { None };
    let job_has_current_turn = opts.job_has_current_turn;
    db.write(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
                 VALUES ('issue-1', ?1, 1, 'Fence resume test', 'active', 'needs_input', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
                 VALUES ('exec-1', 'recipe-1', 'issue-1', ?1, 'running', 1, 1, '{}', 'manual')",
                params![project_id.as_str()],
            )
            .await?;
            // jobs.current_turn_id references turns.id, so insert the job with a
            // NULL turn first and set it after the predecessor turn exists.
            conn.execute(
                "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
                 VALUES ('job-1', 'exec-1', 'issue-1', ?1, 'builder', 'running', 'session-1', 'builder', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                 VALUES ('session-1', 'job-1', 'active', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, issue_id, job_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('run-1', ?1, 'issue-1', ?2, 'live', 'session-1', 1, 1, 'resume')",
                params![project_id.as_str(), job_id_for_run],
            )
            .await?;
            // The predecessor work turn, interrupted to a terminal state by the
            // durable suspend — exactly the state the recovered predecessor is in
            // by answer time.
            conn.execute(
                "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
                 VALUES ('turn-pred', 'session-1', 'run-1', 'job-1', 1, 'interrupted', 'user_message', 1, 1)",
                (),
            )
            .await?;
            // Now the turn exists, point the job at it (the persisted
            // jobs.current_turn_id the resume-time fallback recovers).
            if job_has_current_turn {
                conn.execute(
                    "UPDATE jobs SET current_turn_id = 'turn-pred' WHERE id = 'job-1'",
                    (),
                )
                .await?;
            }
            // The fence crossing, stored with turn_id = NULL (the bug signature).
            conn.execute(
                "INSERT INTO permission_requests(id, run_id, job_id, tool_use_id, tool_name, tool_input, status, created_at, turn_id, uri_segment)
                 VALUES ('perm-row-1', 'run-1', ?1, 'toolu-perm', 'run', '{\"kind\":\"shell\",\"verb\":\"run\",\"descriptor\":\"x\",\"summary\":\"s\",\"request\":{\"cwd\":\"/wt\",\"tool\":\"run\",\"payload\":{},\"tool_use_id\":\"toolu-perm\"}}', 'pending', 1, NULL, 'perm-1')",
                params![job_id_for_run],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// State of the turn whose predecessor is `predecessor`, if a successor exists.
async fn successor_turn_state(db: &LocalDb, predecessor: &str) -> Option<String> {
    let predecessor = predecessor.to_string();
    db.read(|conn| {
        let predecessor = predecessor.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT state FROM turns WHERE predecessor_id = ?1 LIMIT 1",
                    params![predecessor.as_str()],
                )
                .await?;
            rows.next().await?.map(|row| row.opt_text(0)).transpose()
        })
    })
    .await
    .unwrap()
    .flatten()
}

/// The core regression: a NULL-turn, job-owned fence request whose job has a
/// persisted `current_turn_id` recovers that turn as the predecessor and starts
/// a successor turn. Fails on `main` (predecessor stays None, no successor).
#[tokio::test]
async fn null_turn_request_recovers_predecessor_and_starts_successor() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_fixture(
        &db,
        &Fixture {
            job_owned: true,
            job_has_current_turn: true,
        },
    )
    .await;
    let _orch = orchestrator(&temp, db.clone());

    let resume =
        record_permission_response(&db, "perm-row-1", "allowed", r#"{"behavior":"allow"}"#, 100)
            .await
            .unwrap();

    assert!(!resume.duplicate, "first answer must not be a duplicate");
    assert_eq!(
        resume.predecessor_turn_id.as_deref(),
        Some("turn-pred"),
        "NULL stored turn must be recovered from jobs.current_turn_id"
    );
    assert!(
        resume.successor_turn_id.is_some(),
        "a successor turn must be created so the run can resume"
    );
    assert_eq!(
        successor_turn_state(&db, "turn-pred").await.as_deref(),
        Some("running"),
        "the successor turn must be started (running)"
    );
}

/// A project-chat request (no owning job) legitimately has no turn: the NULL
/// stays NULL, no successor is fabricated, and the run is not force-resumed.
#[tokio::test]
async fn project_chat_null_turn_request_is_not_force_resumed() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_fixture(
        &db,
        &Fixture {
            job_owned: false,
            job_has_current_turn: true,
        },
    )
    .await;
    let _orch = orchestrator(&temp, db.clone());

    let resume =
        record_permission_response(&db, "perm-row-1", "allowed", r#"{"behavior":"allow"}"#, 100)
            .await
            .unwrap();

    assert!(
        resume.predecessor_turn_id.is_none(),
        "no owning job means no predecessor to recover"
    );
    assert!(
        resume.successor_turn_id.is_none(),
        "a job-less request must not fabricate a successor turn"
    );
    assert!(
        successor_turn_state(&db, "turn-pred").await.is_none(),
        "no successor turn must exist"
    );
}

/// A job-owned request whose job has no persisted `current_turn_id` has nothing
/// to recover: predecessor stays None and no successor is fabricated.
#[tokio::test]
async fn job_without_current_turn_recovers_nothing() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_fixture(
        &db,
        &Fixture {
            job_owned: true,
            job_has_current_turn: false,
        },
    )
    .await;
    let _orch = orchestrator(&temp, db.clone());

    let resume =
        record_permission_response(&db, "perm-row-1", "allowed", r#"{"behavior":"allow"}"#, 100)
            .await
            .unwrap();

    assert!(
        resume.predecessor_turn_id.is_none(),
        "no jobs.current_turn_id means nothing to recover"
    );
    assert!(resume.successor_turn_id.is_none());
}

fn mock_inline_child(id: u32) -> Arc<Mutex<Box<dyn ChildProcess>>> {
    Arc::new(Mutex::new(
        Box::new(MockChildProcess::with_stdout(id, Vec::new())) as Box<dyn ChildProcess>,
    ))
}

fn inline_count(orch: &cairn_core::internal::orchestrator::Orchestrator, run_id: &str) -> usize {
    orch.pty_state
        .inline_commands
        .lock()
        .unwrap()
        .get(run_id)
        .map(|m| m.len())
        .unwrap_or(0)
}

/// A fence-driven self-suspend must leave the run's still-executing sibling
/// inline commands alone, so a parallel `run()` batch can collect their outcomes
/// before returning the suspend marker. On `main` `suspend_run_for_durable_wait`
/// reaped them.
#[tokio::test]
async fn self_suspend_leaves_sibling_inline_commands_alive() {
    let (temp, db) = common::migrated_db().await;
    let orch = orchestrator(&temp, Arc::new(db));

    orch.pty_state.register_inline_command(
        "run-x".to_string(),
        "cmd-a".to_string(),
        mock_inline_child(1),
    );
    orch.pty_state.register_inline_command(
        "run-x".to_string(),
        "cmd-b".to_string(),
        mock_inline_child(2),
    );
    assert_eq!(inline_count(&orch, "run-x"), 2);

    suspend_run_for_durable_wait(&orch, "run-x", "permission_wait_suspended").unwrap();

    assert_eq!(
        inline_count(&orch, "run-x"),
        2,
        "a self-suspend must not reap the run's inline sibling commands"
    );
}

/// An external stop (user Stop / cascade) still reaps the run's inline commands.
#[tokio::test]
async fn external_stop_still_reaps_inline_commands() {
    let (temp, db) = common::migrated_db().await;
    let orch = orchestrator(&temp, Arc::new(db));

    orch.pty_state.register_inline_command(
        "run-y".to_string(),
        "cmd-a".to_string(),
        mock_inline_child(1),
    );
    assert_eq!(inline_count(&orch, "run-y"), 1);

    stop_session(&orch, "run-y").unwrap();

    assert_eq!(
        inline_count(&orch, "run-y"),
        0,
        "an external stop must reap the run's inline commands"
    );
}
