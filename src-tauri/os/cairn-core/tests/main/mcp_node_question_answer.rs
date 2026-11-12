use crate::common;

use std::sync::Arc;

use crate::common::orchestrator;
use cairn_core::internal::mcp::handlers::read::handle_read_file;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::storage::{LocalDb, RowExt};
use cairn_core::models::question::{PromptAnswers, StoredAnswer};
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

/// Answering a thread's only open question clears the thread's persisted
/// attention in the same write: nothing else will recompute it, and a thread
/// left reading `needs_input` asks the operator for an answer already given.
#[tokio::test]
async fn answering_a_threads_question_clears_its_attention() {
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
    assert!(result.contains("Answered question q-1"), "{result}");

    let attention = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT attention FROM threads WHERE id = 'thread-1'", ())
                    .await?;
                rows.next().await?.map(|row| row.text(0)).transpose()
            })
        })
        .await
        .unwrap();
    assert_eq!(attention.as_deref(), Some("none"));
}

/// An answer body that carries every structure an answer must survive:
/// paragraphs, a numbered list at line starts, a bulleted list, and a fenced
/// code block (cairn/4560).
const STRUCTURED_BODY: &str = "First paragraph of the decision.\n\nSecond paragraph, with a numbered list:\n\n1. a\n2. b\n\n- bullet one\n- bullet two\n\n```rust\nfn main() {\n    println!(\"Question 2: not a boundary\");\n}\n```\n\nClosing line.";

const SECOND_BODY: &str = "Another answer.\n\nIt also spans\nseveral lines.";

const FREE_ONE: &str =
    r#"[{"question":"Which path, and why?","header":null,"options":[],"multiSelect":false}]"#;
const FREE_TWO: &str = r#"[{"question":"Which path?","header":null,"options":[],"multiSelect":false},{"question":"What risks?","header":null,"options":[],"multiSelect":false}]"#;

/// Add free-text prompts beside the base question fixture: two one-question
/// prompts (`q-2`, `q-3`) and one two-question prompt (`q-4`).
async fn insert_free_text_prompts(db: &LocalDb) {
    db.write(|conn| {
        Box::pin(async move {
            for (id, segment, questions) in [
                ("prompt-free-a", "q-2", FREE_ONE),
                ("prompt-free-b", "q-3", FREE_ONE),
                ("prompt-free-two", "q-4", FREE_TWO),
            ] {
                conn.execute(
                    "INSERT INTO prompts(id, run_id, job_id, questions, response, created_at, answered_at, uri_segment)
                     VALUES (?1, 'run-1', 'job-1', ?2, NULL, 1, NULL, ?3)",
                    params![id, questions, segment],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn stored_response(db: &LocalDb, prompt_id: &str) -> Option<String> {
    let prompt_id = prompt_id.to_string();
    db.read(|conn| {
        let prompt_id = prompt_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT response FROM prompts WHERE id = ?1",
                    params![prompt_id.as_str()],
                )
                .await?;
            rows.next().await?.map(|row| row.opt_text(0)).transpose()
        })
    })
    .await
    .unwrap()
    .flatten()
}

/// A multi-paragraph, list- and code-bearing answer is accepted on a
/// one-question prompt through both payload shapes, and inside one answer of a
/// two-question prompt.
#[tokio::test]
async fn structured_multi_line_answers_are_accepted() {
    use cairn_core::internal::mcp::handlers::planning::answer_node_question;

    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_question_fixture(&db).await;
    insert_free_text_prompts(&db).await;
    let orch = orchestrator(&temp, db.clone());

    let indexed = answer_node_question(
        &orch,
        "tqa",
        1,
        1,
        "builder",
        "q-2",
        &json!({"answers": [{"index": 0, "text": STRUCTURED_BODY}]}),
    )
    .await;
    assert!(
        indexed.is_ok(),
        "answers:[{{index:0,text}}] on one question: {indexed:?}"
    );

    let shorthand = answer_node_question(
        &orch,
        "tqa",
        1,
        1,
        "builder",
        "q-3",
        &json!({"answer": STRUCTURED_BODY}),
    )
    .await;
    assert!(
        shorthand.is_ok(),
        "answer shorthand on one question: {shorthand:?}"
    );

    let two = answer_node_question(
        &orch,
        "tqa",
        1,
        1,
        "builder",
        "q-4",
        &json!({"answers": [
            {"index": 0, "text": STRUCTURED_BODY},
            {"index": 1, "text": SECOND_BODY}
        ]}),
    )
    .await;
    assert!(two.is_ok(), "two questions with multi-line bodies: {two:?}");

    // What is stored is the indexed form, and every body comes back byte for
    // byte: no flattening into lines and no trimming.
    for (prompt_id, expected) in [
        ("prompt-free-a", vec![STRUCTURED_BODY]),
        ("prompt-free-b", vec![STRUCTURED_BODY]),
        ("prompt-free-two", vec![STRUCTURED_BODY, SECOND_BODY]),
    ] {
        let stored = stored_response(&db, prompt_id)
            .await
            .unwrap_or_else(|| panic!("{prompt_id} has no stored answer"));
        let StoredAnswer::Answers(answers) = PromptAnswers::decode_stored(&stored) else {
            panic!("{prompt_id} stored a non-indexed answer: {stored}");
        };
        let bodies = answers
            .answers()
            .iter()
            .map(|answer| answer.answer.as_str())
            .collect::<Vec<_>>();
        assert_eq!(bodies, expected, "{prompt_id} bodies");
    }
}

/// The typed line format still parses on a two-question prompt, and a
/// two-answer claim on a one-question prompt is refused naming the extra index.
#[tokio::test]
async fn typed_lines_parse_and_phantom_answers_are_refused() {
    use cairn_core::internal::mcp::handlers::planning::answer_node_question;

    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_question_fixture(&db).await;
    insert_free_text_prompts(&db).await;
    let orch = orchestrator(&temp, db.clone());

    let phantom = answer_node_question(
        &orch,
        "tqa",
        1,
        1,
        "builder",
        "q-2",
        &json!({"answers": [{"index": 0, "text": "a"}, {"index": 1, "text": "b"}]}),
    )
    .await
    .expect_err("a second answer on a one-question prompt must be refused");
    assert!(
        phantom.contains("answer index 1 (Question 2) is out of range"),
        "{phantom}"
    );
    assert_eq!(stored_response(&db, "prompt-free-a").await, None);

    let typed = format!("Question 1: {STRUCTURED_BODY}\nQuestion 2: {SECOND_BODY}");
    answer_node_question(
        &orch,
        "tqa",
        1,
        1,
        "builder",
        "q-4",
        &json!({"answer": typed}),
    )
    .await
    .expect("canonical typed lines answer a two-question prompt");
    let stored = stored_response(&db, "prompt-free-two").await.unwrap();
    let StoredAnswer::Answers(answers) = PromptAnswers::decode_stored(&stored) else {
        panic!("typed answer stored a non-indexed value: {stored}");
    };
    assert_eq!(answers.answers()[0].answer, STRUCTURED_BODY);
    assert_eq!(answers.answers()[1].answer, SECOND_BODY);
}

/// A prompt with no questions could never be answered by index, so asking one
/// is refused before anything is stored.
#[tokio::test]
async fn asking_no_questions_is_refused() {
    use cairn_core::internal::mcp::handlers::planning::ask_questions;
    use cairn_core::internal::mcp::types::AskUserPayload;

    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let orch = orchestrator(&temp, db.clone());
    let outcome = ask_questions(
        &orch,
        &change_request(json!({})),
        AskUserPayload { questions: vec![] },
        true,
        None,
    )
    .await;
    assert!(
        outcome.text.contains("at least one question"),
        "{outcome:?}"
    );
    assert_eq!(outcome.answers, None);
    let stored = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query("SELECT COUNT(*) FROM prompts", ()).await?;
                rows.next().await?.map(|row| row.i64(0)).transpose()
            })
        })
        .await
        .unwrap();
    assert_eq!(stored, Some(0));
}

fn change_request(payload: serde_json::Value) -> McpCallbackRequest {
    McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: String::new(),
        run_id: None,
        tool: "write".to_string(),
        payload,
        tool_use_id: None,
        origin: None,
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
    let yes = r#"[{"index":0,"answer":"Yes"}]"#;
    assert_eq!(prompt_response(&db).await.as_deref(), Some(yes));

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
    assert_eq!(prompt_response(&db).await.as_deref(), Some(yes));
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
            answer_bound_ms: None,
            thread_id: None,
            cwd: String::new(),
            run_id: None,
            tool: "read".to_string(),
            payload: json!({"path": "cairn://p/tqa/1/1/builder/questions/q-1"}),
            tool_use_id: None,
            origin: None,
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
            answer_bound_ms: None,
            thread_id: None,
            cwd: temp.path().display().to_string(),
            run_id: None,
            tool: "read".to_string(),
            payload: json!({"path": "file:large.txt"}),
            tool_use_id: None,
            origin: None,
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
            answer_bound_ms: None,
            thread_id: None,
            cwd: temp.path().display().to_string(),
            run_id: None,
            tool: "read".to_string(),
            payload: json!({"path": "file:huge.log"}),
            tool_use_id: None,
            origin: None,
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
            answer_bound_ms: None,
            thread_id: None,
            cwd: temp.path().display().to_string(),
            run_id: None,
            tool: "read".to_string(),
            payload: json!({"path": "file:tail.txt", "offset": -2}),
            tool_use_id: None,
            origin: None,
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
