//! Integration tests for the `watch` long-poll handler.
//!
//! Covers the current-state catch-up path and the no-poll wake: a `watch`
//! blocked on `attention_changed` returns when a recompute boundary pokes the
//! issue, without any polling.

mod common;

use std::sync::Arc;
use std::time::Duration;

use cairn_core::internal::mcp::handlers::watch::handle_watch;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::{AttentionEvent, Orchestrator};
use cairn_core::internal::storage::{DbError, LocalDb};
use common::orchestrator;
use serde_json::{json, Value};
use tempfile::TempDir;
use turso::params;

const WATCH_URI: &str = "cairn://p/WATCH/1";

async fn insert_issue(
    db: &LocalDb,
    project_id: &str,
    number: i64,
    attention: &str,
    updated_at: i64,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let project_id = project_id.to_string();
    let attention = attention.to_string();
    let row_id = id.clone();
    db.execute(
        "INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'Watch test', 'active', ?4, ?5, ?5)",
        params![id.as_str(), project_id.as_str(), number, attention.as_str(), updated_at],
    )
    .await
    .unwrap();
    row_id
}

async fn set_attention(db: &LocalDb, issue_id: &str, attention: &str, updated_at: i64) {
    let issue_id = issue_id.to_string();
    let attention = attention.to_string();
    db.execute(
        "UPDATE issues SET attention = ?1, updated_at = ?2 WHERE id = ?3",
        params![attention.as_str(), updated_at, issue_id.as_str()],
    )
    .await
    .unwrap();
}

async fn set_status(db: &LocalDb, issue_id: &str, status: &str, updated_at: i64) {
    let issue_id = issue_id.to_string();
    let status = status.to_string();
    db.execute(
        "UPDATE issues SET status = ?1, updated_at = ?2 WHERE id = ?3",
        params![status.as_str(), updated_at, issue_id.as_str()],
    )
    .await
    .unwrap();
}

fn watch_request(issue_uri: &str, since: Option<i64>) -> McpCallbackRequest {
    let payload = match since {
        Some(s) => json!({ "issue_uri": issue_uri, "since": s }),
        None => json!({ "issue_uri": issue_uri }),
    };
    McpCallbackRequest {
        cwd: String::new(),
        run_id: None,
        tool: "watch".to_string(),
        payload,
        tool_use_id: None,
    }
}

struct WatchFixture {
    _temp: TempDir,
    db: Arc<LocalDb>,
    project_id: String,
    issue_id: String,
    orch: Orchestrator,
}

async fn watch_fixture(attention: &str, updated_at: i64) -> WatchFixture {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "WATCH").await;
    let issue_id = insert_issue(&db, &project_id, 1, attention, updated_at).await;
    let orch = orchestrator(&temp, db.clone());
    WatchFixture {
        _temp: temp,
        db,
        project_id,
        issue_id,
        orch,
    }
}

fn spawn_watch(orch: &Orchestrator, since: i64) -> tokio::task::JoinHandle<String> {
    let orch = orch.clone();
    tokio::spawn(async move { handle_watch(&orch, &watch_request(WATCH_URI, Some(since))).await })
}

fn parse_watch_json(result: &str) -> Value {
    serde_json::from_str(result).unwrap()
}

async fn handle_watch_json(orch: &Orchestrator, since: Option<i64>) -> Value {
    let result = handle_watch(orch, &watch_request(WATCH_URI, since)).await;
    parse_watch_json(&result)
}

async fn await_watch_json(
    watcher: tokio::task::JoinHandle<String>,
    timeout_after: Duration,
    expect_message: &str,
) -> Value {
    let result = tokio::time::timeout(timeout_after, watcher)
        .await
        .expect(expect_message)
        .unwrap();
    parse_watch_json(&result)
}

async fn wake_for_issue_event(orch: &Orchestrator, issue_id: &str) -> AttentionEvent {
    let mut rx = orch.attention_changed.subscribe();
    orch.wake_for_issue(issue_id).await;
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("event must arrive")
        .expect("open")
}

#[tokio::test]
async fn watch_returns_immediately_when_already_actionable() {
    let fixture = watch_fixture("needs_approval", 100).await;

    let value = handle_watch_json(&fixture.orch, None).await;
    assert_eq!(value["status"], "actionable");
    assert_eq!(value["attention"], "needs_approval");
    assert_eq!(value["issue_uri"], "cairn://p/WATCH/1");
}

#[tokio::test]
async fn watch_does_not_return_for_state_at_or_before_cursor() {
    let fixture = watch_fixture("needs_input", 100).await;

    // since == updated_at: this attention was already seen, so the handler must
    // block rather than return. A short timeout proves it does not return early.
    let blocked = tokio::time::timeout(
        Duration::from_millis(400),
        handle_watch(&fixture.orch, &watch_request(WATCH_URI, Some(100))),
    )
    .await;
    assert!(
        blocked.is_err(),
        "watch should block when state is not newer than cursor"
    );
}

#[tokio::test]
async fn watch_wakes_on_poke_without_polling() {
    let fixture = watch_fixture("none", 100).await;

    // Block: not actionable yet (attention none), cursor at current updated_at.
    let watcher = spawn_watch(&fixture.orch, 100);

    // Let the watcher subscribe before the poke fires.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // A recompute boundary flips attention and emits.
    set_attention(&fixture.db, &fixture.issue_id, "needs_input", 200).await;
    fixture.orch.wake_for_issue(&fixture.issue_id).await;

    let value = await_watch_json(
        watcher,
        Duration::from_secs(10),
        "watch should wake on the poke",
    )
    .await;
    assert_eq!(value["status"], "actionable");
    assert_eq!(value["attention"], "needs_input");
    assert_eq!(value["updated_at"], 200);
}

#[tokio::test]
async fn watch_returns_resolved_for_an_already_merged_issue() {
    // The reported bug: a merged issue has attention=none, so an attention-only
    // gate would block forever. Terminal status must short-circuit — even when
    // the cursor is at/after the merge's updated_at (a done issue never blocks).
    let fixture = watch_fixture("none", 100).await;
    set_status(&fixture.db, &fixture.issue_id, "merged", 100).await;

    let value = handle_watch_json(&fixture.orch, Some(100)).await;
    assert_eq!(value["status"], "resolved");
    assert_eq!(value["issue_status"], "merged");
    assert_eq!(value["issue_uri"], "cairn://p/WATCH/1");
}

#[tokio::test]
async fn watch_wakes_resolved_when_an_issue_merges_mid_wait() {
    // A watch blocked on a still-active issue must return `resolved` when the
    // issue merges and the resolve path pokes — not stall to budget expiry.
    let fixture = watch_fixture("none", 100).await;

    let watcher = spawn_watch(&fixture.orch, 100);

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Terminal transition: status flips to merged and the resolve path emits.
    set_status(&fixture.db, &fixture.issue_id, "merged", 200).await;
    fixture.orch.wake_for_issue(&fixture.issue_id).await;

    let value = await_watch_json(
        watcher,
        Duration::from_secs(10),
        "watch should wake and resolve on the terminal poke",
    )
    .await;
    assert_eq!(value["status"], "resolved");
    assert_eq!(value["issue_status"], "merged");
    assert_eq!(value["updated_at"], 200);
}

#[tokio::test]
async fn watch_does_not_miss_a_poke_racing_the_catch_up_read() {
    // Stresses the within-call window: the flip + poke fire with no settle delay,
    // racing the watcher's step-1 catch-up read. Because `watch` subscribes
    // before that read, an interim poke is buffered and still wakes the loop
    // (or the read itself observes the committed state) — never a budget stall.
    let fixture = watch_fixture("none", 100).await;

    let watcher = spawn_watch(&fixture.orch, 100);

    // No sleep: flip + emit immediately, contending with the catch-up read.
    set_attention(&fixture.db, &fixture.issue_id, "needs_input", 200).await;
    fixture.orch.wake_for_issue(&fixture.issue_id).await;

    let value = await_watch_json(
        watcher,
        Duration::from_secs(10),
        "watch must not stall when a poke races the catch-up read",
    )
    .await;
    assert_eq!(value["status"], "actionable");
    assert_eq!(value["attention"], "needs_input");
}

// ---------------------------------------------------------------------------
// Typed-event tests — each fact site emits its own variant, dedupe collapses
// near-duplicate emits, and the watch handler renders the event content.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn typed_question_event_carries_inline_questions() {
    use cairn_core::internal::mcp::types::{Question, QuestionOption};
    use cairn_core::internal::orchestrator::attention::QuestionContent;
    use cairn_core::internal::orchestrator::{AttentionEvent, AttentionFact};
    use cairn_core::models::{IssueAttention, IssueStatus};

    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "WATCH").await;
    let issue_id = insert_issue(&db, &project_id, 1, "none", 100).await;
    let orch = orchestrator(&temp, db.clone());

    // Subscriber must exist before the emit so the broadcast is delivered.
    let mut rx = orch.attention_changed.subscribe();

    let event = AttentionEvent {
        issue_id: issue_id.clone(),
        issue_uri: "cairn://p/WATCH/1".to_string(),
        fact: AttentionFact::Question {
            escalate: false,
            detail_uri: "cairn://p/WATCH/1/1/planner/questions/q-1".to_string(),
            content: QuestionContent {
                questions: vec![Question {
                    question: "Continue?".to_string(),
                    header: None,
                    options: vec![
                        QuestionOption {
                            label: "Yes".to_string(),
                            description: String::new(),
                        },
                        QuestionOption {
                            label: "No".to_string(),
                            description: String::new(),
                        },
                    ],
                    multi_select: false,
                }],
            },
        },
        attention: IssueAttention::NeedsInput,
        status: IssueStatus::Active,
        updated_at: 200,
    };

    orch.emit_attention_event(event);

    let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("event must arrive")
        .expect("channel must not be closed");
    assert_eq!(received.issue_id, issue_id);
    match received.fact {
        AttentionFact::Question { content, .. } => {
            assert_eq!(content.questions.len(), 1);
            assert_eq!(content.questions[0].question, "Continue?");
            assert_eq!(content.questions[0].options.len(), 2);
        }
        other => panic!("expected Question fact, got {:?}", other),
    }
}

#[tokio::test]
async fn distinct_facts_for_same_issue_each_pass_through_dedupe() {
    use cairn_core::internal::orchestrator::attention::ArtifactSummary;
    use cairn_core::internal::orchestrator::{AttentionEvent, AttentionFact};
    use cairn_core::models::{IssueAttention, IssueStatus};

    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "WATCH").await;
    let issue_id = insert_issue(&db, &project_id, 1, "needs_approval", 100).await;
    let orch = orchestrator(&temp, db.clone());

    let mut rx = orch.attention_changed.subscribe();

    let artifact = AttentionEvent {
        issue_id: issue_id.clone(),
        issue_uri: "cairn://p/WATCH/1".to_string(),
        fact: AttentionFact::ArtifactWritten {
            escalate: false,
            detail_uri: "cairn://p/WATCH/1/1/builder/pr".to_string(),
            content: ArtifactSummary {
                output_name: "pr".to_string(),
                version: 1,
                confirmed: false,
                title: Some("Initial".to_string()),
                summary: None,
                artifact_type: "pull_request".to_string(),
            },
        },
        attention: IssueAttention::NeedsApproval,
        status: IssueStatus::Active,
        updated_at: 110,
    };
    let idle = AttentionEvent {
        issue_id: issue_id.clone(),
        issue_uri: "cairn://p/WATCH/1".to_string(),
        fact: AttentionFact::AgentIdleWithWork {
            escalate: false,
            detail_uri: "cairn://p/WATCH/1/1/builder/pr".to_string(),
        },
        attention: IssueAttention::NeedsApproval,
        status: IssueStatus::Active,
        updated_at: 111,
    };

    orch.emit_attention_event(artifact);
    orch.emit_attention_event(idle);

    let e1 = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("first event")
        .expect("open");
    let e2 = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("second event — distinct fact must not be deduped")
        .expect("open");
    assert!(matches!(e1.fact, AttentionFact::ArtifactWritten { .. }));
    assert!(matches!(e2.fact, AttentionFact::AgentIdleWithWork { .. }));
}

/// Seed a `(project, issue, execution, job, run)` chain so handler tests can
/// call `ask_questions` / `finalize_run` against a realistic state. Returns
/// `(issue_id, run_id, job_id, node_segment, exec_seq)`.
async fn seed_run_for_issue(
    db: &LocalDb,
    project_id: &str,
    issue_number: i64,
    initial_attention: &str,
) -> (String, String, String, String, i64) {
    let issue_id = insert_issue(db, project_id, issue_number, initial_attention, 100).await;
    let execution_id = format!("e-{}", &issue_id[..8]);
    let job_id = format!("j-{}", &issue_id[..8]);
    let run_id = format!("r-{}", &issue_id[..8]);
    let session_id = format!("s-{}", &issue_id[..8]);
    let exec_seq = 1i64;
    let node_segment = "planner".to_string();
    let project_id = project_id.to_string();
    let i = issue_id.clone();
    let e = execution_id.clone();
    let j = job_id.clone();
    let r = run_id.clone();
    let s = session_id.clone();
    let n = node_segment.clone();
    db.write(move |conn| {
        let p = project_id.clone();
        let i = i.clone();
        let e = e.clone();
        let j = j.clone();
        let r = r.clone();
        let s = s.clone();
        let n = n.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                 VALUES (?1, 'default', ?2, ?3, 'running', 1, ?4)",
                params![e.as_str(), i.as_str(), p.as_str(), exec_seq],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 'running', ?5, ?5, 1, 1)",
                params![j.as_str(), e.as_str(), i.as_str(), p.as_str(), n.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions (id, job_id, status, created_at, updated_at)
                 VALUES (?1, ?2, 'open', 1, 1)",
                params![s.as_str(), j.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO runs (id, job_id, issue_id, session_id, status, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 'live', 1, 1)",
                params![r.as_str(), j.as_str(), i.as_str(), s.as_str()],
            )
            .await?;
            Ok::<_, DbError>(())
        })
    })
    .await
    .unwrap();
    (issue_id, run_id, job_id, node_segment, exec_seq)
}

#[tokio::test]
async fn wake_for_issue_emits_resolved_when_status_is_terminal() {
    // Driving the same emit logic that `emit_for_turn_end` runs from
    // `finalize_run`/`transition_to_warm_state`: when the issue is terminal
    // at the moment the agent goes idle, the broadcast carries `Resolved`.
    use cairn_core::internal::orchestrator::AttentionFact;

    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "WATCH").await;
    let issue_id = insert_issue(&db, &project_id, 1, "none", 100).await;
    set_status(&db, &issue_id, "merged", 200).await;
    let orch = orchestrator(&temp, db.clone());

    let event = wake_for_issue_event(&orch, &issue_id).await;
    assert_eq!(event.issue_id, issue_id);
    assert!(matches!(
        event.fact,
        AttentionFact::Resolved { ref final_status, .. }
            if final_status.to_string() == "merged"
    ));
}

#[tokio::test]
async fn wake_for_issue_emits_idle_with_work_for_needs_approval() {
    // The case the recompute-sweep poke missed before this PR: the agent's
    // turn ended while the issue still needs the driver. `wake_for_issue`
    // (shared with `emit_for_turn_end`) emits `AgentIdleWithWork` so any
    // in-flight `watch` returns rather than stalling to budget.
    use cairn_core::internal::orchestrator::AttentionFact;

    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "WATCH").await;
    let issue_id = insert_issue(&db, &project_id, 1, "needs_approval", 200).await;
    let orch = orchestrator(&temp, db.clone());

    let event = wake_for_issue_event(&orch, &issue_id).await;
    assert_eq!(event.issue_id, issue_id);
    assert!(matches!(
        event.fact,
        AttentionFact::AgentIdleWithWork { .. }
    ));
    assert_eq!(event.attention.to_string(), "needs_approval");
}

#[tokio::test]
async fn wake_for_issue_idle_with_work_points_at_pending_permission_segment() {
    // A `needs_authorization` idle fact must resolve its detail URI to the
    // answerable `.../permissions/perm-N` segment, not the bare issue URI, so a
    // handler can go straight to the decision patch with no enumeration read.
    use cairn_core::internal::orchestrator::AttentionFact;

    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "WATCH").await;
    // seed_run_for_issue uses node_segment "planner", exec_seq 1.
    let (issue_id, run_id, job_id, node_segment, _seq) =
        seed_run_for_issue(&db, &project_id, 1, "needs_authorization").await;

    // One pending permission request on the node, stamped with its `perm-1`
    // segment — the shape `await_permission_decision` inserts.
    {
        let run_id = run_id.clone();
        let job_id = job_id.clone();
        db.write(move |conn| {
            let run_id = run_id.clone();
            let job_id = job_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO permission_requests(id, run_id, job_id, tool_use_id, tool_name, tool_input, status, created_at, uri_segment)
                     VALUES ('perm-row-1', ?1, ?2, 'toolu-1', 'run', '{}', 'pending', 1, 'perm-1')",
                    params![run_id.as_str(), job_id.as_str()],
                )
                .await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    let orch = orchestrator(&temp, db.clone());
    let event = wake_for_issue_event(&orch, &issue_id).await;
    assert_eq!(event.attention.to_string(), "needs_authorization");
    match event.fact {
        AttentionFact::AgentIdleWithWork { detail_uri, .. } => {
            assert_eq!(
                detail_uri,
                format!("cairn://p/WATCH/1/1/{node_segment}/permissions/perm-1")
            );
        }
        other => panic!("expected AgentIdleWithWork, got {:?}", other),
    }
}

#[tokio::test]
async fn wake_for_issue_is_silent_when_no_actionable_state() {
    // attention=None, non-terminal: there is nothing for the watcher to do.
    // `wake_for_issue` must NOT broadcast, otherwise idle-but-progressing
    // recomputes would generate spurious wakes.
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "WATCH").await;
    let issue_id = insert_issue(&db, &project_id, 1, "none", 100).await;
    let orch = orchestrator(&temp, db.clone());

    let mut rx = orch.attention_changed.subscribe();
    orch.wake_for_issue(&issue_id).await;

    let received = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await;
    assert!(
        received.is_err(),
        "wake_for_issue must be silent for non-terminal, attention=None state"
    );
}

#[tokio::test]
async fn ask_questions_handler_emits_typed_question_event() {
    // End-to-end: drive the real MCP handler (`ask_questions` in `background`
    // mode so we don't have to satisfy the foreground yield/process state).
    // The handler stores the prompt, builds the typed event, and broadcasts.
    // A subscriber receives the full questions inline (no follow-up read).
    use cairn_core::internal::mcp::handlers::planning::ask_questions;
    use cairn_core::internal::mcp::types::{
        AskUserPayload, McpCallbackRequest, Question, QuestionOption,
    };
    use cairn_core::internal::orchestrator::AttentionFact;

    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "WATCH").await;
    let (issue_id, run_id, _job_id, _node, _seq) =
        seed_run_for_issue(&db, &project_id, 1, "none").await;
    let orch = orchestrator(&temp, db.clone());

    let mut rx = orch.attention_changed.subscribe();

    let payload = AskUserPayload {
        questions: vec![Question {
            question: "Continue?".to_string(),
            header: Some("Confirm".to_string()),
            options: vec![
                QuestionOption {
                    label: "Yes".to_string(),
                    description: String::new(),
                },
                QuestionOption {
                    label: "No".to_string(),
                    description: String::new(),
                },
            ],
            multi_select: false,
        }],
    };
    let request = McpCallbackRequest {
        cwd: String::new(),
        run_id: Some(run_id.clone()),
        tool: "ask_user".to_string(),
        payload: serde_json::to_value(&payload).unwrap(),
        tool_use_id: Some("tool-1".to_string()),
    };

    // Background=true so the handler stores the prompt + emits the typed event
    // without yielding the parent turn (no process state to satisfy in tests).
    tokio::spawn(async move {
        ask_questions(&orch, &request, payload, true, None).await;
    });

    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("typed Question event must arrive from ask_questions handler")
        .expect("open");
    assert_eq!(event.issue_id, issue_id);
    match event.fact {
        AttentionFact::Question {
            content,
            detail_uri,
            ..
        } => {
            assert_eq!(content.questions.len(), 1);
            assert_eq!(content.questions[0].question, "Continue?");
            assert_eq!(content.questions[0].options.len(), 2);
            assert!(detail_uri.contains("/planner/questions/q-"));
        }
        other => panic!("expected typed Question fact, got {:?}", other),
    }
}

#[tokio::test]
async fn watch_returns_event_fact_in_response_json() {
    use cairn_core::internal::orchestrator::attention::ArtifactSummary;
    use cairn_core::internal::orchestrator::{AttentionEvent, AttentionFact};
    use cairn_core::models::{IssueAttention, IssueStatus};

    let fixture = watch_fixture("none", 100).await;

    let watcher = spawn_watch(&fixture.orch, 100);

    // Let the watcher subscribe.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Bump updated_at past the cursor so the event passes the cursor gate, and
    // emit a typed ArtifactWritten event. The watch handler should render the
    // fact block into the response JSON — no follow-up read.
    set_attention(&fixture.db, &fixture.issue_id, "needs_approval", 200).await;
    fixture.orch.emit_attention_event(AttentionEvent {
        issue_id: fixture.issue_id.clone(),
        issue_uri: "cairn://p/WATCH/1".to_string(),
        fact: AttentionFact::ArtifactWritten {
            escalate: false,
            detail_uri: "cairn://p/WATCH/1/1/builder/pr".to_string(),
            content: ArtifactSummary {
                output_name: "pr".to_string(),
                version: 1,
                confirmed: false,
                title: Some("Add typed events".to_string()),
                summary: Some("swap channel".to_string()),
                artifact_type: "pull_request".to_string(),
            },
        },
        attention: IssueAttention::NeedsApproval,
        status: IssueStatus::Active,
        updated_at: 200,
    });

    let value = await_watch_json(
        watcher,
        Duration::from_secs(5),
        "watch should return on typed event",
    )
    .await;
    assert_eq!(value["status"], "actionable");
    assert_eq!(value["attention"], "needs_approval");
    assert_eq!(value["detail_uri"], "cairn://p/WATCH/1/1/builder/pr");
    assert_eq!(value["fact"]["kind"], "artifact_written");
    assert_eq!(value["fact"]["content"]["title"], "Add typed events");
    assert_eq!(value["fact"]["content"]["version"], 1);
    assert_eq!(value["fact"]["content"]["confirmed"], false);
}

// ---------------------------------------------------------------------------
// Open-PR idle wake — a builder that finished and opened a PR must wake `watch`
// even when the attention projection is still `none` (GitHub mergeability /
// check / review state unknown right after the PR is opened).
// ---------------------------------------------------------------------------

/// Seed an `(execution, job, open merge_request, /pr artifact)` chain for an
/// issue. The merge_request's GitHub fields are left NULL — the unknown-state
/// case the attention projection deliberately leaves as `none`. `mr_updated_at`
/// drives the cursor gate in the catch-up read.
async fn seed_open_pr(
    db: &LocalDb,
    project_id: &str,
    issue_id: &str,
    exec_seq: i64,
    node_segment: &str,
    mr_updated_at: i64,
) {
    let project_id = project_id.to_string();
    let issue_id = issue_id.to_string();
    let node_segment = node_segment.to_string();
    let short = issue_id[..issue_id.len().min(6)].to_string();
    let execution_id = format!("e-{}-{}", short, exec_seq);
    let job_id = format!("j-{}-{}", short, exec_seq);
    let mr_id = format!("mr-{}-{}", short, exec_seq);
    let artifact_id = format!("a-{}-{}", short, exec_seq);
    db.write(move |conn| {
        let project_id = project_id.clone();
        let issue_id = issue_id.clone();
        let node_segment = node_segment.clone();
        let execution_id = execution_id.clone();
        let job_id = job_id.clone();
        let mr_id = mr_id.clone();
        let artifact_id = artifact_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                 VALUES (?1, 'default', ?2, ?3, 'running', 1, ?4)",
                params![
                    execution_id.as_str(),
                    issue_id.as_str(),
                    project_id.as_str(),
                    exec_seq
                ],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 'complete', ?5, ?5, 1, 1)",
                params![
                    job_id.as_str(),
                    execution_id.as_str(),
                    issue_id.as_str(),
                    project_id.as_str(),
                    node_segment.as_str()
                ],
            )
            .await?;
            // Open PR with unknown GitHub state (github_mergeable / github_review
            // / checks_status all NULL) — the reported bug scenario.
            conn.execute(
                "INSERT INTO merge_requests (
                    id, job_id, project_id, issue_id, title, source_branch, target_branch,
                    status, merge_method, opened_at, updated_at, github_pr_number, github_pr_url
                 ) VALUES (?1, ?2, ?3, ?4, 'PR', 'feature', 'main', 'open', 'squash', 1, ?5, 7,
                           'https://github.com/octo/widget/pull/7')",
                params![
                    mr_id.as_str(),
                    job_id.as_str(),
                    project_id.as_str(),
                    issue_id.as_str(),
                    mr_updated_at
                ],
            )
            .await?;
            // The builder's /pr artifact so the resolved URI is .../builder/pr.
            conn.execute(
                "INSERT INTO artifacts (id, job_id, artifact_type, data, version, output_name, created_at, updated_at)
                 VALUES (?1, ?2, 'pull_request', '{}', 1, 'pr', 1, 1)",
                params![artifact_id.as_str(), job_id.as_str()],
            )
            .await?;
            Ok::<_, DbError>(())
        })
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn wake_for_issue_emits_idle_with_work_for_open_pr_even_with_none_attention() {
    // The reported bug: a builder finished and opened a PR, but the issue's
    // attention is still `none` (GitHub state unknown). `wake_for_issue` must
    // still emit `AgentIdleWithWork` — pointing at the builder's `/pr` — so an
    // in-flight `watch` returns rather than stalling to budget.
    use cairn_core::internal::orchestrator::AttentionFact;

    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "WATCH").await;
    let issue_id = insert_issue(&db, &project_id, 1, "none", 100).await;
    seed_open_pr(&db, &project_id, &issue_id, 1, "builder", 150).await;
    let orch = orchestrator(&temp, db.clone());

    let event = wake_for_issue_event(&orch, &issue_id).await;
    assert_eq!(event.issue_id, issue_id);
    assert_eq!(event.attention.to_string(), "none");
    // The event carries the merge_request's updated_at (150), not the stale
    // issue.updated_at (100) — so a watcher whose cursor equals issue.updated_at
    // still clears the gate (Finding 1).
    assert_eq!(event.updated_at, 150);
    match event.fact {
        AttentionFact::AgentIdleWithWork { detail_uri, .. } => {
            assert_eq!(detail_uri, "cairn://p/WATCH/1/1/builder/pr");
        }
        other => panic!("expected AgentIdleWithWork, got {:?}", other),
    }
}

#[tokio::test]
async fn watch_returns_on_idle_with_work_event_with_none_attention() {
    // A typed `AgentIdleWithWork` event whose `attention` is None must still
    // wake an in-flight `watch` when its `updated_at` is past the cursor — the
    // attention projection no longer gates the idle-with-work fact.
    use cairn_core::internal::orchestrator::{AttentionEvent, AttentionFact};
    use cairn_core::models::{IssueAttention, IssueStatus};

    let fixture = watch_fixture("none", 100).await;

    let watcher = spawn_watch(&fixture.orch, 100);
    tokio::time::sleep(Duration::from_millis(300)).await;

    fixture.orch.emit_attention_event(AttentionEvent {
        issue_id: fixture.issue_id.clone(),
        issue_uri: "cairn://p/WATCH/1".to_string(),
        fact: AttentionFact::AgentIdleWithWork {
            escalate: false,
            detail_uri: "cairn://p/WATCH/1/1/builder/pr".to_string(),
        },
        attention: IssueAttention::None,
        status: IssueStatus::Active,
        updated_at: 200,
    });

    let value = await_watch_json(
        watcher,
        Duration::from_secs(5),
        "watch should return on idle-with-work event",
    )
    .await;
    assert_eq!(value["status"], "actionable");
    assert_eq!(value["attention"], "none");
    assert_eq!(value["detail_uri"], "cairn://p/WATCH/1/1/builder/pr");
    assert_eq!(value["fact"]["kind"], "agent_idle_with_work");
}

#[tokio::test]
async fn watch_catch_up_returns_for_open_pr_with_none_attention() {
    // A watcher that starts after the PR opened (so the typed event was missed,
    // not buffered) must still return via the step-1 catch-up read: an open PR
    // updated after the cursor is actionable work even with attention `none`.
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "WATCH").await;
    let issue_id = insert_issue(&db, &project_id, 1, "none", 100).await;
    seed_open_pr(&db, &project_id, &issue_id, 1, "builder", 250).await;
    let orch = orchestrator(&temp, db.clone());

    let value = handle_watch_json(&orch, Some(200)).await;
    assert_eq!(value["status"], "actionable");
    assert_eq!(value["attention"], "none");
    assert_eq!(value["detail_uri"], "cairn://p/WATCH/1/1/builder/pr");
    assert_eq!(value["fact"]["kind"], "agent_idle_with_work");
    assert_eq!(value["updated_at"], 250);
}

#[tokio::test]
async fn watch_catch_up_does_not_return_for_open_pr_at_or_before_cursor() {
    // An open PR not newer than the cursor was already seen — the catch-up read
    // must block rather than re-return it, or the driver would spin.
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "WATCH").await;
    let issue_id = insert_issue(&db, &project_id, 1, "none", 100).await;
    seed_open_pr(&db, &project_id, &issue_id, 1, "builder", 200).await;
    let orch = orchestrator(&temp, db.clone());

    let blocked = tokio::time::timeout(
        Duration::from_millis(400),
        handle_watch(&orch, &watch_request(WATCH_URI, Some(200))),
    )
    .await;
    assert!(
        blocked.is_err(),
        "watch must block when the open PR is not newer than the cursor"
    );
}

#[tokio::test]
async fn watch_live_open_pr_wake_carries_pr_updated_at_not_stale_issue_updated_at() {
    // Finding 1: when create_pr's refresh fails, issue.updated_at is NOT bumped
    // (only recompute bumps it, and recompute runs only on refresh success). The
    // live open-PR wake must still clear a watcher whose cursor equals
    // issue.updated_at — it does so by carrying the merge_request's updated_at,
    // which is strictly fresher. Model that: issue.updated_at = cursor = 100, PR
    // opens with mr.updated_at = 150, issue row left at 100 (no recompute).
    let fixture = watch_fixture("none", 100).await;

    let watcher = spawn_watch(&fixture.orch, 100);
    // Let the watcher clear step-1 (no PR yet) and enter the long-poll.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // PR opens after the watcher is blocked; issue.updated_at left at 100 to
    // model a skipped/failed post-create recompute.
    seed_open_pr(
        &fixture.db,
        &fixture.project_id,
        &fixture.issue_id,
        1,
        "builder",
        150,
    )
    .await;
    fixture.orch.wake_for_issue(&fixture.issue_id).await;

    let value = await_watch_json(
        watcher,
        Duration::from_secs(5),
        "watch must wake on the live open-PR emit despite a stale issue.updated_at",
    )
    .await;
    assert_eq!(value["status"], "actionable");
    assert_eq!(value["attention"], "none");
    assert_eq!(value["detail_uri"], "cairn://p/WATCH/1/1/builder/pr");
    assert_eq!(value["updated_at"], 150);
}

#[tokio::test]
async fn watch_catch_up_resolves_pr_uri_for_pr_state_attention() {
    // Finding 2: synthesize_event (the catch-up fallback when no typed event is
    // buffered) must resolve PR-state attention to the /pr resource via the
    // shared idle_fact_for_issue — matching the live broadcast, not the bare
    // issue URI.
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "WATCH").await;
    let issue_id = insert_issue(&db, &project_id, 1, "needs_approval", 100).await;
    seed_open_pr(&db, &project_id, &issue_id, 1, "builder", 100).await;
    let orch = orchestrator(&temp, db.clone());

    let value = handle_watch_json(&orch, None).await;
    assert_eq!(value["status"], "actionable");
    assert_eq!(value["attention"], "needs_approval");
    assert_eq!(value["detail_uri"], "cairn://p/WATCH/1/1/builder/pr");
}
