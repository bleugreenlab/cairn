use crate::common;

use std::sync::Arc;

use crate::common::orchestrator;
use cairn_core::internal::mcp::handlers::read::handle_read_file;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::storage::{LocalDb, RowExt};
use serde_json::json;
use turso::params;

async fn insert_question_fixture(db: &LocalDb) {
    let project_id = common::create_project(db, "TQA").await;
    db.write(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
                 VALUES ('issue-1', ?1, 1, 'Question answer test', 'active', 'needs_input', 1, 1)",
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
                 VALUES ('run-1', ?1, 'issue-1', 'job-1', 'live', 'session-1', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO prompts(id, run_id, job_id, questions, response, created_at, answered_at, uri_segment, tool_use_id)
                 VALUES ('prompt-1', 'run-1', 'job-1', '[{\"question\":\"Proceed?\",\"header\":\"Confirm\",\"options\":[{\"label\":\"Yes\",\"description\":\"Proceed\"}],\"multiSelect\":false}]', NULL, 1, NULL, 'q-1', 'toolu-question')",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

fn change_request(payload: serde_json::Value) -> McpCallbackRequest {
    McpCallbackRequest {
        cwd: String::new(),
        run_id: None,
        tool: "write".to_string(),
        payload,
        tool_use_id: None,
    }
}

async fn prompt_response(db: &LocalDb) -> Option<String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT response FROM prompts WHERE id = 'prompt-1'", ())
                .await?;
            rows.next().await?.map(|row| row.opt_text(0)).transpose()
        })
    })
    .await
    .unwrap()
    .flatten()
}

#[tokio::test]
async fn node_question_patch_answers_prompt_once() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_question_fixture(&db).await;
    let orch = orchestrator(&temp, db.clone());

    let target = "cairn://p/TQA/1/1/builder/questions/q-1";
    let result = handle_write(
        &orch,
        &change_request(json!({
            "changes": [{"target": target, "mode": "patch", "payload": {"answer": "Yes"}}]
        })),
    )
    .await;
    assert!(
        result.contains("Answered question q-1"),
        "unexpected result: {result}"
    );
    assert_eq!(prompt_response(&db).await.as_deref(), Some("Yes"));

    let duplicate = handle_write(
        &orch,
        &change_request(json!({
            "changes": [{"target": target, "mode": "patch", "payload": {"answer": "Yes"}}]
        })),
    )
    .await;
    assert!(
        duplicate.contains("already answered"),
        "unexpected duplicate result: {duplicate}"
    );
    assert_eq!(prompt_response(&db).await.as_deref(), Some("Yes"));
}

#[tokio::test]
async fn pending_question_read_shows_answer_action() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_question_fixture(&db).await;
    let orch = orchestrator(&temp, db);

    let output = handle_read_file(
        &orch,
        &McpCallbackRequest {
            cwd: String::new(),
            run_id: None,
            tool: "read".to_string(),
            payload: json!({"path": "cairn://p/TQA/1/1/builder/questions/q-1"}),
            tool_use_id: None,
        },
    )
    .await;
    assert!(output.contains("## actions"), "missing actions: {output}");
    assert!(
        output.contains("answer question"),
        "missing answer action: {output}"
    );
    assert!(
        output.contains("mode:\"patch\"") || output.contains("mode=\"patch\""),
        "missing patch example: {output}"
    );
}

#[tokio::test]
async fn file_read_returns_structured_text_without_implicit_2000_line_cap() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let orch = orchestrator(&temp, db);
    let content = (1..=2105)
        .map(|line| format!("line {line}"))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(temp.path().join("large.txt"), content).unwrap();

    let output = handle_read_file(
        &orch,
        &McpCallbackRequest {
            cwd: temp.path().display().to_string(),
            run_id: None,
            tool: "read".to_string(),
            payload: json!({"path": "file:large.txt"}),
            tool_use_id: None,
        },
    )
    .await;
    // Reads render to structured text: a header reporting the true total plus
    // gutter-numbered lines. The full file is present (past the old 2000 cap).
    assert!(output.contains("of 2105"), "{output}");
    assert!(output.contains("line 2105"), "{output}");
}

#[tokio::test]
async fn file_read_preview_is_bounded_but_reports_true_total() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let orch = orchestrator(&temp, db);
    let content = (1..=5000)
        .map(|line| format!("line {line} {}", "x".repeat(100)))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(temp.path().join("huge.log"), content).unwrap();

    let output = handle_read_file(
        &orch,
        &McpCallbackRequest {
            cwd: temp.path().display().to_string(),
            run_id: None,
            tool: "read".to_string(),
            payload: json!({"path": "file:huge.log"}),
            tool_use_id: None,
        },
    )
    .await;
    // The preview is bounded but the header still reports the true total, and
    // the last line is outside the bounded window.
    assert!(output.contains("of 5000"), "{output}");
    assert!(
        output.len() < 110_000,
        "preview was too large: {}",
        output.len()
    );
    assert!(!output.contains("line 5000"), "{output}");
}

#[tokio::test]
async fn file_read_negative_offset_resolves_to_tail_window() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let orch = orchestrator(&temp, db);
    std::fs::write(temp.path().join("tail.txt"), "a\nb\nc\nd\ne").unwrap();

    let output = handle_read_file(
        &orch,
        &McpCallbackRequest {
            cwd: temp.path().display().to_string(),
            run_id: None,
            tool: "read".to_string(),
            payload: json!({"path": "file:tail.txt", "offset": -2}),
            tool_use_id: None,
        },
    )
    .await;
    // A negative offset resolves to the tail window: the last two of five lines.
    assert!(output.contains("lines 4\u{2013}5 of 5"), "{output}");
    assert!(
        output.trim_end().ends_with("     4\td\n     5\te"),
        "{output}"
    );
}
