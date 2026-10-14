use crate::common;

use std::sync::Arc;

use crate::common::orchestrator;
use cairn_core::internal::mcp::handlers::read::handle_read_file;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use serde_json::json;

async fn insert_question_fixture(db: &LocalDb) {
    let project_id = common::create_project(db, "tqa").await;
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

const THREAD_QUESTIONS: &str = r#"[{"question":"Proceed?","header":"Confirm","options":[{"label":"Yes","description":"Proceed"}],"multiSelect":false}]"#;

/// The PRODUCTION shape of a thread that has actually run, holding a pending
/// question on its session job.
///
/// Hand-written because the shared `test_support::seed_thread_session` helper is
/// `pub(crate)` and an integration test cannot reach it. These columns mirror it
/// and must keep doing so: session job with `issue_id` NULL and `thread_id` set,
/// a synthetic host execution carrying `issue_id` NULL and `seq` NULL, and the
/// reserved `thread` uri_segment with no parent job that `SESSION_JOB_SHAPE`
/// keys on. A fixture that gave the thread an issue would prove nothing about
/// the path under test.
async fn insert_thread_question_fixture(db: &LocalDb) {
    let project_id = common::create_project(db, "tqt").await;
    db.write(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
                 VALUES ('thread-1', ?1, 'design', 'active', 'needs_input', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, project_id, status, started_at)
                 VALUES ('host-exec-1', 'host-thread-job-1', ?1, 'running', 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, thread_id, project_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
                 VALUES ('thread-job-1', 'host-exec-1', 'thread-1', ?1, 'Thread', 'running', 'thread-session-1', 'thread', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                 VALUES ('thread-session-1', 'thread-job-1', 'active', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('run-t1', ?1, 'thread-job-1', 'live', 'thread-session-1', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO prompts(id, run_id, job_id, questions, response, created_at, answered_at, uri_segment, tool_use_id)
                 VALUES ('prompt-t1', 'run-t1', 'thread-job-1', ?1, NULL, 1, NULL, 'q-1', 'toolu-thread')",
                params![THREAD_QUESTIONS],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// A thread answering its own question must record the thread's CANONICAL home
/// as the resolution actor.
///
/// The repair that made this URI resolvable routes through the reserved
/// `(0, 0, thread-name)` coordinate. Provenance built by reassembling those
/// arguments would persist `cairn://p/tqt/0/0/design` — a sentinel that resolves
/// to nothing — into the receipt, and every later audit of who answered would
/// read it that way forever. Asserting the DISPLAYED target cannot catch this;
/// the stored actor is the thing that has to be right.
#[tokio::test]
async fn a_thread_answering_its_question_records_the_canonical_thread_actor() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_thread_question_fixture(&db).await;
    let orch = orchestrator(&temp, db.clone());

    let result = handle_write(
        &orch,
        &change_request(json!({
            "changes": [{
                "target": "cairn://p/tqt/design/questions/q-1",
                "mode": "patch",
                "payload": {"answer": "Yes"}
            }]
        })),
    )
    .await;
    assert!(
        result.contains("Answered question q-1"),
        "the thread's question must answer through its canonical URI: {result}"
    );

    let actor = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT winner_actor FROM channel_ask_resolution WHERE action_ref = 'prompt-t1'",
                        (),
                    )
                    .await?;
                rows.next().await?.map(|row| row.text(0)).transpose()
            })
        })
        .await
        .unwrap();
    assert_eq!(
        actor.as_deref(),
        Some("cairn://p/tqt/design"),
        "the receipt must name the thread's canonical home, not a 0/0 sentinel"
    );
}

fn change_request(payload: serde_json::Value) -> McpCallbackRequest {
    McpCallbackRequest {
        thread_id: None,
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

    let target = "cairn://p/tqa/1/1/builder/questions/q-1";
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
            thread_id: None,
            cwd: String::new(),
            run_id: None,
            tool: "read".to_string(),
            payload: json!({"path": "cairn://p/tqa/1/1/builder/questions/q-1"}),
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
            thread_id: None,
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
            thread_id: None,
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
            thread_id: None,
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
