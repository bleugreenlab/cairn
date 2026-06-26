mod common;

use cairn_core::internal::orchestrator::lifecycle;
use cairn_core::internal::storage::LocalDb;
use turso::params;

async fn insert_project_job_run_turn(db: &LocalDb, turn_state: &str) {
    let project_id = common::create_project(db, "STOP").await;
    let turn_state = turn_state.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let turn_state = turn_state.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
                 VALUES ('job-1', ?1, 'running', 'session-1', 1, 1)",
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
                "INSERT INTO runs(id, project_id, job_id, chat_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('run-1', ?1, 'job-1', NULL, 'live', 'session-1', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
                 VALUES ('turn-1', 'session-1', 'run-1', 'job-1', 1, ?1, 1, 1)",
                params![turn_state.as_str()],
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_turn_id = 'turn-1' WHERE id = 'job-1'",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn insert_pending_run_tool_event(db: &LocalDb) {
    let data = serde_json::json!({
        "eventType": "assistant",
        "sessionId": "session-1",
        "parentToolUseId": null,
        "content": null,
        "thinking": null,
        "toolName": null,
        "toolInput": null,
        "toolUses": [
            {
                "id": "tool-run-1",
                "name": "mcp__cairn__run",
                "input": { "commands": [{ "command": "sleep 60" }] }
            },
            {
                "id": "tool-read-1",
                "name": "mcp__cairn__read",
                "input": { "paths": ["file:README.md"] }
            }
        ],
        "toolUseId": null,
        "toolResult": null,
        "isError": false,
        "raw": null
    })
    .to_string();
    db.write(|conn| {
        let data = data.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at, turn_id)
                 VALUES ('event-assistant-1', 'run-1', 'session-1', 1, 1, 'assistant', ?1, 1, 'turn-1')",
                params![data.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn insert_issue_job_run_turn(db: &LocalDb, project_id: &str) {
    let project_id = project_id.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at)
                 VALUES ('issue-1', ?1, 1, 'Title', 'desc', 'active', 'active', 'none', 0, 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
                 VALUES ('job-1', ?1, 'issue-1', 'running', 'session-1', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                 VALUES ('session-1', 'job-1', 'open', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, job_id, chat_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('run-1', ?1, 'job-1', NULL, 'live', 'session-1', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
                 VALUES ('turn-1', 'session-1', 'run-1', 'job-1', 1, 'running', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_turn_id = 'turn-1' WHERE id = 'job-1'",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// A user marking an issue terminal must not be refused while an agent runs:
/// the issue reaches its terminal state and the live run's turn is interrupted
/// so teardown can clean up without stranding the agent.
#[tokio::test]
async fn marking_issue_closed_stops_active_runs() {
    let (_temp, orch) = common::test_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "CLOSE").await;
    insert_issue_job_run_turn(&orch.db.local, &project_id).await;

    cairn_core::issues::status::update_status(&orch, "issue-1", "closed")
        .await
        .unwrap();

    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT state FROM turns WHERE id = ?1",
            "turn-1"
        )
        .await,
        Some("interrupted".to_string())
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT status FROM issues WHERE id = ?1",
            "issue-1"
        )
        .await,
        Some("closed".to_string())
    );
}

async fn insert_issue_with_job(db: &LocalDb, project_id: &str, job_status: &str) {
    let project_id = project_id.to_string();
    let job_status = job_status.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let job_status = job_status.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at)
                 VALUES ('issue-1', ?1, 1, 'Title', 'desc', 'active', 'active', 'none', 0, 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
                 VALUES ('job-1', ?1, 'issue-1', ?2, 1, 1)",
                params![project_id.as_str(), job_status.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// An automated (recipe/coordinator) terminal resolution must see an active
/// reviewer/job on the issue as a blocker so it refuses rather than resolving
/// out from under live work.
#[tokio::test]
async fn terminal_resolution_blockers_flag_active_job() {
    let (_temp, orch) = common::test_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "GATE").await;
    insert_issue_with_job(&orch.db.local, &project_id, "running").await;

    let blockers = cairn_core::issues::status::terminal_resolution_blockers(&orch, "issue-1")
        .await
        .unwrap();

    assert!(
        !blockers.is_empty(),
        "a running job on the issue should block automated resolution"
    );
}

/// Once the issue's jobs are terminal, the gate is clear and the automated
/// resolution may proceed.
#[tokio::test]
async fn terminal_resolution_blockers_empty_when_jobs_complete() {
    let (_temp, orch) = common::test_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "GATE").await;
    insert_issue_with_job(&orch.db.local, &project_id, "complete").await;

    let blockers = cairn_core::issues::status::terminal_resolution_blockers(&orch, "issue-1")
        .await
        .unwrap();

    assert!(
        blockers.is_empty(),
        "a complete job should not block automated resolution, got {blockers:?}"
    );
}

#[tokio::test]
async fn stop_session_reconciles_live_run_missing_from_process_map() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local, "running").await;

    lifecycle::stop_session(&orch, "run-1").unwrap();

    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT state FROM turns WHERE id = ?1",
            "turn-1"
        )
        .await,
        Some("interrupted".to_string())
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT status FROM runs WHERE id = ?1",
            "run-1"
        )
        .await,
        Some("exited".to_string())
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT exit_reason FROM runs WHERE id = ?1",
            "run-1"
        )
        .await,
        Some("user_stop".to_string())
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT status FROM jobs WHERE id = ?1",
            "job-1"
        )
        .await,
        Some("running".to_string())
    );
}

#[tokio::test]
async fn stop_session_cancels_pending_turn_missing_from_process_map() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local, "pending").await;

    lifecycle::stop_session(&orch, "run-1").unwrap();

    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT state FROM turns WHERE id = ?1",
            "turn-1"
        )
        .await,
        Some("cancelled".to_string())
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT status FROM runs WHERE id = ?1",
            "run-1"
        )
        .await,
        Some("exited".to_string())
    );
}

#[tokio::test]
async fn stop_session_fails_pending_run_tool_result() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local, "running").await;
    insert_pending_run_tool_event(&orch.db.local).await;

    lifecycle::stop_session(&orch, "run-1").unwrap();

    let data = common::scalar_text_by_id(
        &orch.db.local,
        "SELECT data FROM events WHERE run_id = ?1 AND event_type = 'tool_result'",
        "run-1",
    )
    .await
    .expect("stop should synthesize a tool_result for the pending run call");
    let parsed: serde_json::Value = serde_json::from_str(&data).unwrap();
    assert_eq!(
        parsed.get("toolUseId").and_then(|value| value.as_str()),
        Some("tool-run-1")
    );
    assert_eq!(
        parsed.get("isError").and_then(|value| value.as_bool()),
        Some(true)
    );
    assert_eq!(
        parsed.get("toolResult").and_then(|value| value.as_str()),
        Some("Run interrupted by user stop.")
    );
    assert_eq!(
        common::scalar_i64_by_id(
            &orch.db.local,
            "SELECT COUNT(*) FROM events WHERE run_id = ?1 AND event_type = 'tool_result'",
            "run-1",
        )
        .await,
        1,
        "non-run pending tools should not get synthetic run failure results"
    );
}
