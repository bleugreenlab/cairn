//! OpenRouter owns its turn/tool loop in-process and has no warm process, so a
//! foreground question append suspends the turn immediately rather than entering
//! the inline 45s wait + AwaitingHost dance used by Claude/Codex. This test pins
//! that owned-loop branch: the handler returns the suspend marker without
//! blocking, records a structured suspend on the run, stores the prompt keyed to
//! the live turn (so a successor can form on answer), and does NOT enter
//! AwaitingHost (keeping `should_resume` true at answer time).

use crate::common;

use std::sync::{Arc, Mutex};

use crate::common::orchestrator;
use cairn_core::internal::agent_process::process::{RunHandle, SuspendKind};
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use serde_json::json;

async fn seed(db: &LocalDb) {
    let project_id = common::create_project(db, "TOR").await;
    db.write(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
                 VALUES ('issue-1', ?1, 1, 'OpenRouter suspend', 'active', 'working', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
                 VALUES ('exec-1', 'recipe-1', 'issue-1', ?1, 'running', 1, 1, '{}', 'manual')",
                params![project_id.as_str()],
            )
            .await?;
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
                 VALUES ('run-1', ?1, 'issue-1', 'job-1', 'live', 'session-1', 1, 1, 'new')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
                 VALUES ('turn-1', 'session-1', 'run-1', 'job-1', 0, 'running', 'initial', 1, 1)",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn prompt_turn_id(db: &LocalDb) -> Option<String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT turn_id FROM prompts WHERE run_id = 'run-1'", ())
                .await?;
            rows.next().await?.map(|row| row.opt_text(0)).transpose()
        })
    })
    .await
    .unwrap()
    .flatten()
}

/// Register an OpenRouter-style run handle: owned-loop, mid-turn on `turn-1`.
fn register_owned_loop_run(orch: &cairn_core::internal::orchestrator::Orchestrator) {
    let mut handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(None)),
        Some("session-1".to_string()),
        Some("job-1".to_string()),
    );
    handle.owns_turn_loop = true;
    handle.begin_turn("turn-1");
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("run-1".to_string(), handle);
}

#[tokio::test]
async fn owned_loop_question_suspends_without_inline_wait() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db).await;
    let orch = orchestrator(&temp, db.clone());
    register_owned_loop_run(&orch);

    let request = McpCallbackRequest {
        thread_id: None,
        cwd: temp.path().display().to_string(),
        run_id: Some("run-1".to_string()),
        tool: "write".to_string(),
        payload: json!({
            "changes": [{
                "target": "cairn://p/TOR/1/1/builder/questions",
                "mode": "append",
                "payload": {
                    "questions": [{
                        "question": "Proceed?",
                        "header": "Confirm",
                        "options": [{"label": "Yes", "description": "go"}],
                        "multiSelect": false
                    }]
                }
            }]
        }),
        tool_use_id: Some("toolu-or-write".to_string()),
    };

    // Returns promptly with the suspend marker (no 45s inline wait).
    let result = handle_write(&orch, &request).await;
    assert!(
        result.contains("Prompt suspended"),
        "expected suspend marker, got: {result}"
    );

    // The prompt is stored keyed to the live turn so a successor can form on
    // answer; without a turn id no successor forms and resume never fires.
    assert_eq!(prompt_turn_id(&db).await.as_deref(), Some("turn-1"));

    // The owned loop did NOT enter AwaitingHost, keeping `should_resume` true.
    assert!(!orch.process_state.is_awaiting_host("run-1", Some("turn-1")));

    // A structured suspend was requested for the owned loop to consume.
    assert_eq!(
        orch.process_state.take_suspend("run-1"),
        Some(SuspendKind::Prompt)
    );
}
