//! Question answers resume through the shared host-wait successor handoff
//! (cairn/4963), the one cairn/4936 introduced for permissions.
//!
//! An answer mints its `prompt_response` successor inside the transaction that
//! records it, classified the way a permission answer is: a successor of its
//! own, a provider turn still open, a busy job, a foreign successor, or no
//! owner. The job continues into that successor by id; a busy job or a foreign
//! successor gets the answer through its queue; a refusal settles the unserved
//! successor `question_resume_refused` and is recorded on the question; an open
//! asking turn carries the answer at its next tool boundary. In every case the
//! answer itself is reported as applied.

use crate::common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cairn_common::identity::AppearanceTransport;
use cairn_core::internal::dispatch::dispatch_tool;
use cairn_core::internal::mcp::handlers::host_wait_successor::{
    HostWaitContinuation, HostWaitSuccessor,
};
use cairn_core::internal::mcp::handlers::permission::record_permission_response;
use cairn_core::internal::mcp::handlers::planning::{answer_prompt_id, PromptAnswerOutcome};
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::storage::{LocalDb, RowExt};
use cairn_core::messages::delivery::nudge_job_for_urgency;
use cairn_core::messages::queued::Delivery;
use cairn_core::models::question::{PromptAnswers, QuestionAnswer};
use cairn_core::turns::queries::ResolutionProvenance;
use cairn_db::turso::params;

/// How the asking turn stands when the answer arrives.
struct Asked {
    /// The asking turn's state: `yielded` for a blocking ask that outlived its
    /// inline budget, `running` for a non-blocking ask whose turn carried on.
    turn_state: &'static str,
    /// The originating `write` call, set only for a blocking ask.
    tool_use_id: Option<&'static str>,
    /// Whether the ask was non-blocking.
    background: bool,
}

const BLOCKING: Asked = Asked {
    turn_state: "yielded",
    tool_use_id: Some("toolu-q"),
    background: false,
};

const BACKGROUND_MID_TURN: Asked = Asked {
    turn_state: "running",
    tool_use_id: None,
    background: true,
};

/// A non-blocking ask whose turn has since yielded: a background ask never
/// yields, so another wait holds it.
const BACKGROUND_HELD: Asked = Asked {
    turn_state: "yielded",
    tool_use_id: None,
    background: true,
};

/// A job with an open session, its asking turn, and one pending question
/// `prompt-1` (`q-1`) raised by that turn.
async fn seed_question(db: &LocalDb, asked: &Asked) {
    let project_id = common::create_project(db, "lateq").await;
    for sql in [
        "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
         VALUES ('issue-1', ?1, 1, 'late question', 'active', 1, 1)",
        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
         VALUES ('exec-1', 'recipe-default', 'issue-1', ?1, 'running', 1, 1)",
        "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, uri_segment,
                           status, created_at, updated_at)
         VALUES ('job-1', 'exec-1', 'issue-1', ?1, 'Builder', 'builder', 'running', 1, 1)",
        "INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at, start_mode)
         VALUES ('run-1', 'issue-1', ?1, 'job-1', 'live', 1, 1, 'resume')",
    ] {
        db.execute(sql, params![project_id.as_str()]).await.unwrap();
    }
    let yield_reason = (asked.turn_state == "yielded").then_some("user_input");
    for sql in [
        "INSERT INTO sessions (id, job_id, status, backend_id, created_at, updated_at)
         VALUES ('session-1', 'job-1', 'open', 'handle-1', 1, 1)",
        "UPDATE runs SET session_id = 'session-1' WHERE id = 'run-1'",
    ] {
        db.execute(sql, ()).await.unwrap();
    }
    db.execute(
        "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, yield_reason,
                            start_reason, created_at, updated_at)
         VALUES ('turn-ask', 'session-1', 'run-1', 'job-1', 1, ?1, ?2, 'user_message', 1, 1)",
        params![asked.turn_state, yield_reason],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE jobs SET current_session_id = 'session-1', current_turn_id = 'turn-ask'
         WHERE id = 'job-1'",
        (),
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO prompts (id, run_id, job_id, questions, created_at, turn_id, uri_segment,
                              tool_use_id, background)
         VALUES ('prompt-1', 'run-1', 'job-1',
                 '[{\"question\":\"Which approach?\",\"options\":[]}]', 1, 'turn-ask', 'q-1', ?1, ?2)",
        params![asked.tool_use_id, i64::from(asked.background)],
    )
    .await
    .unwrap();
}

fn answer(text: &str) -> PromptAnswers {
    PromptAnswers::new(
        1,
        vec![QuestionAnswer {
            index: 0,
            answer: text.to_string(),
        }],
    )
    .unwrap()
}

fn provenance() -> ResolutionProvenance {
    ResolutionProvenance {
        id: "answer-resolution".to_string(),
        surface: AppearanceTransport::ResourcePatch,
        provider: None,
        conversation: None,
        actor: Some("cairn://p/lateq/1/1/builder".to_string()),
    }
}

async fn answer_q1(orch: &Orchestrator, text: &str) -> Result<PromptAnswerOutcome, String> {
    answer_prompt_id(orch, "prompt-1", answer(text), provenance()).await
}

async fn count(db: &LocalDb, sql: &'static str) -> i64 {
    db.query_one(sql, (), |row| row.i64(0)).await.unwrap()
}

/// The orchestrator over a seeded database, with the job's warm process
/// parked: the asking call outlived its inline budget and durably suspended.
fn late_answer_host(
    temp: &tempfile::TempDir,
    db: Arc<LocalDb>,
) -> (Orchestrator, Arc<Mutex<Vec<u8>>>) {
    let orch = common::orchestrator(temp, db);
    let delivered = common::register_recording_process(&orch, "run-1", "session-1");
    orch.process_state.transition_to_warm("run-1");
    (orch, delivered)
}

fn delivered_text(delivered: &Arc<Mutex<Vec<u8>>>) -> String {
    String::from_utf8_lossy(&delivered.lock().unwrap()).into_owned()
}

/// `(id, state, run_id, end_reason)` of every `prompt_response` turn.
async fn prompt_response_turns(
    db: &LocalDb,
) -> Vec<(String, String, Option<String>, Option<String>)> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, state, run_id, end_reason FROM turns
                     WHERE start_reason = 'prompt_response' ORDER BY sequence",
                    (),
                )
                .await?;
            let mut turns = Vec::new();
            while let Some(row) = rows.next().await? {
                turns.push((
                    row.text(0)?,
                    row.text(1)?,
                    row.opt_text(2)?,
                    row.opt_text(3)?,
                ));
            }
            Ok(turns)
        })
    })
    .await
    .unwrap()
}

/// Running turns with zero events that no live process is serving.
async fn stranded_running_turns(orch: &Orchestrator, db: &LocalDb) -> Vec<String> {
    let candidates = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT t.id, t.run_id FROM turns t WHERE t.state = 'running'
                         AND NOT EXISTS (SELECT 1 FROM events e WHERE e.turn_id = t.id)",
                        (),
                    )
                    .await?;
                let mut turns = Vec::new();
                while let Some(row) = rows.next().await? {
                    turns.push((row.text(0)?, row.opt_text(1)?));
                }
                Ok(turns)
            })
        })
        .await
        .unwrap();
    candidates
        .into_iter()
        .filter(|(turn_id, run_id)| {
            run_id.as_deref().is_none_or(|run_id| {
                orch.process_state.get_current_turn_id(run_id).as_deref() != Some(turn_id.as_str())
            })
        })
        .map(|(turn_id, _)| turn_id)
        .collect()
}

/// A turn on the asking turn's job, as something other than the answer left it.
async fn insert_turn(
    db: &LocalDb,
    id: &str,
    sequence: i64,
    state: &str,
    start_reason: &str,
    predecessor: Option<&str>,
) {
    db.execute(
        "INSERT INTO turns (id, session_id, run_id, job_id, sequence, predecessor_id, state,
                            start_reason, created_at, updated_at)
         VALUES (?1, 'session-1', 'run-1', 'job-1', ?2, ?3, ?4, ?5, 2, 2)",
        params![id, sequence, predecessor, state, start_reason],
    )
    .await
    .unwrap();
    if state == "running" || state == "pending" {
        db.execute(
            "UPDATE jobs SET current_turn_id = ?1 WHERE id = 'job-1'",
            params![id],
        )
        .await
        .unwrap();
    }
}

/// The positive witness: a blocking question answered after its inline budget
/// resumes the job into exactly one `prompt_response` successor, started on
/// the warm run and served by its process, with the answer stored in place as
/// the waiting call's `tool_result` and no visible user event.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_question_answer_resumes_into_its_successor() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BLOCKING).await;
    let (orch, delivered) = late_answer_host(&temp, db.clone());

    let outcome = answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");

    assert_eq!(outcome.continuation, Some(HostWaitContinuation::Driven));
    let successors = prompt_response_turns(&db).await;
    assert_eq!(successors.len(), 1, "exactly one successor: {successors:?}");
    let (turn_id, state, run_id, _) = &successors[0];
    assert_eq!(state, "running", "the successor is started");
    assert_eq!(run_id.as_deref(), Some("run-1"), "on the warm run");
    assert_eq!(
        orch.process_state.get_current_turn_id("run-1").as_deref(),
        Some(turn_id.as_str()),
        "the warm process serves the successor"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM events WHERE event_type = 'tool_result'"
        )
        .await,
        1,
        "the answer is stored as the waiting call's result"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM events WHERE event_type IN ('user', 'user:continuation')"
        )
        .await,
        0,
        "no visible user event"
    );
    assert!(
        delivered_text(&delivered).contains("Option A"),
        "the warm process is prompted with the answer"
    );
    assert!(stranded_running_turns(&orch, &db).await.is_empty());
}

/// Specimens #1 and #2 on this issue: the asking turn was already continued by
/// a running follow-up when the answer arrived. The answer is applied, the
/// follow-up is not adopted, and the answer is queued for the job's next
/// boundary. On the base the follow-up was adopted as the answer's successor
/// and the write failed with `Failed to resume prompt response` after the
/// answer had committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn answer_with_a_foreign_successor_queues_and_reports_applied() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BLOCKING).await;
    insert_turn(
        &db,
        "turn-follow",
        2,
        "running",
        "follow_up",
        Some("turn-ask"),
    )
    .await;
    let (orch, _delivered) = late_answer_host(&temp, db.clone());
    orch.process_state.begin_turn("run-1", "turn-follow");

    let outcome = answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");

    assert_eq!(outcome.continuation, Some(HostWaitContinuation::Queued));
    let summary = outcome.answered_summary("q-1");
    assert!(
        summary.starts_with("Answered question q-1") && summary.contains("queued"),
        "{summary}"
    );
    assert!(
        prompt_response_turns(&db).await.is_empty(),
        "no prompt_response turn is minted"
    );
    assert_eq!(
        db.query_text("SELECT state FROM turns WHERE id = 'turn-follow'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("running"),
        "the follow-up is left to its own launch"
    );
    let queued = db
        .query_text(
            "SELECT content FROM queued_messages WHERE id = 'prompt-1'",
            (),
        )
        .await
        .unwrap()
        .expect("one queued message keyed by the prompt");
    assert!(
        queued.starts_with("Your question cairn://")
            && queued.contains("/questions/q-1 (")
            && queued.contains(") was answered:")
            && queued.contains("Option A"),
        "{queued}"
    );
    assert_eq!(count(&db, "SELECT COUNT(*) FROM queued_messages").await, 1);
}

/// A sibling launch (a steer adopting the pending head, or an earlier answer
/// from the same turn) already started the answer's successor when its driver
/// runs. Only a still-pending successor enters the launch funnel; this one is
/// left running and the answer is queued for its next boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn answer_whose_successor_was_started_early_queues() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BLOCKING).await;
    insert_turn(
        &db,
        "turn-early",
        2,
        "running",
        "prompt_response",
        Some("turn-ask"),
    )
    .await;
    let (orch, delivered) = late_answer_host(&temp, db.clone());
    orch.process_state.begin_turn("run-1", "turn-early");

    let outcome = answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");

    assert_eq!(outcome.continuation, Some(HostWaitContinuation::Queued));
    assert_eq!(
        prompt_response_turns(&db).await,
        vec![(
            "turn-early".to_string(),
            "running".to_string(),
            Some("run-1".to_string()),
            None
        )],
        "nothing is minted and the running successor is not cancelled"
    );
    assert_eq!(
        orch.process_state.get_current_turn_id("run-1").as_deref(),
        Some("turn-early"),
        "its process still serves it"
    );
    assert!(
        delivered_text(&delivered).trim().is_empty(),
        "the answer is not pushed into the process mid-turn"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM queued_messages WHERE id = 'prompt-1'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM prompts WHERE id = 'prompt-1' AND resume_error IS NOT NULL"
        )
        .await,
        0,
        "a queued answer records no refusal"
    );
}

/// When the job refuses to continue (here its session closed), the minted
/// successor is settled `question_resume_refused`, the refusal is recorded on
/// the question, and the answer is still reported as applied. On the base the
/// successor stayed `pending` as the job's current turn and the write failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_question_resume_settles_the_successor_and_records_why() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BLOCKING).await;
    db.execute(
        "UPDATE sessions SET status = 'closed', terminal_reason = 'issue_closed' WHERE id = 'session-1'",
        (),
    )
    .await
    .unwrap();
    let (orch, delivered) = late_answer_host(&temp, db.clone());

    let outcome = answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");

    let Some(HostWaitContinuation::Refused { reason }) = outcome.continuation.clone() else {
        panic!("the resume is refused: {outcome:?}");
    };
    assert!(
        outcome
            .answered_summary("q-1")
            .starts_with("Answered question q-1 (recorded), but the job could not continue"),
        "{}",
        outcome.answered_summary("q-1")
    );
    let successors = prompt_response_turns(&db).await;
    assert_eq!(successors.len(), 1, "{successors:?}");
    assert_eq!(
        (successors[0].1.as_str(), successors[0].3.as_deref()),
        ("cancelled", Some("question_resume_refused"))
    );
    let resume_error = db
        .query_text("SELECT resume_error FROM prompts WHERE id = 'prompt-1'", ())
        .await
        .unwrap();
    assert_eq!(resume_error.as_deref(), Some(reason.as_str()));
    assert!(reason.contains("closed"), "{reason}");
    assert!(
        db.query_text("SELECT response FROM prompts WHERE id = 'prompt-1'", ())
            .await
            .unwrap()
            .is_some(),
        "the answer itself stands"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM turns WHERE state IN ('pending', 'running')"
        )
        .await,
        0,
        "no turn is left live"
    );
    assert!(
        delivered_text(&delivered).trim().is_empty(),
        "nothing was prompted"
    );
}

/// A launch that starts the successor and then fails to deliver its prompt (a
/// warm process whose stdin is broken) unwinds it as `question_resume_refused`
/// before releasing the job's launch lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_question_launch_that_fails_to_deliver_unwinds_its_successor() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BLOCKING).await;
    let orch = common::orchestrator(&temp, db.clone());
    common::register_broken_process(&orch, "run-1", "session-1");
    orch.process_state.transition_to_warm("run-1");

    let outcome = answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");

    assert!(
        matches!(
            outcome.continuation,
            Some(HostWaitContinuation::Refused { .. })
        ),
        "{outcome:?}"
    );
    let successors = prompt_response_turns(&db).await;
    assert_eq!(successors.len(), 1, "{successors:?}");
    assert_eq!(
        (
            successors[0].1.as_str(),
            successors[0].2.as_deref(),
            successors[0].3.as_deref()
        ),
        ("cancelled", Some("run-1"), Some("question_resume_refused")),
        "the launch started the successor on the warm run, then unwound it"
    );
    assert_eq!(
        orch.process_state.get_current_turn_id("run-1"),
        None,
        "the process does not read as serving a turn it never received"
    );
    assert!(
        db.query_text("SELECT resume_error FROM prompts WHERE id = 'prompt-1'", ())
            .await
            .unwrap()
            .is_some(),
        "the refusal is on the question"
    );
    assert_eq!(count(&db, "SELECT COUNT(*) FROM queued_messages").await, 0);
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM turns WHERE state = 'running'").await,
        0
    );
}

/// The inline waiter can wake on the broadcast and leave `AwaitingHost` before
/// the answer path looks. The answer snapshots the wait before it broadcasts,
/// so the inline answer is never also resumed on the slow path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_waiter_snapshot_precedes_broadcast() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BLOCKING).await;
    let orch = common::orchestrator(&temp, db.clone());
    let delivered = common::register_recording_process(&orch, "run-1", "session-1");
    orch.process_state.yield_for_host("run-1", "turn-ask");

    // The inline waiter: on its answer it takes the turn back at once.
    let mut rx = orch.prompt_responses.subscribe();
    let waiter_orch = orch.clone();
    let waiter = std::thread::spawn(move || {
        let (prompt_id, _) = rx.blocking_recv().expect("the answer is broadcast");
        assert_eq!(prompt_id, "prompt-1");
        waiter_orch.process_state.begin_turn("run-1", "turn-ask");
    });

    let outcome = answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");
    waiter.join().unwrap();

    assert_eq!(
        outcome.continuation, None,
        "the inline waiter owns the answer"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM events WHERE event_type = 'tool_result'"
        )
        .await,
        0,
        "no slow-path tool_result beside the inline result"
    );
    assert_eq!(count(&db, "SELECT COUNT(*) FROM queued_messages").await, 0);
    assert!(
        delivered_text(&delivered).trim().is_empty(),
        "no slow-path prompt"
    );
}

/// A second answer to an answered question changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_question_answer_mints_nothing() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BLOCKING).await;
    let (orch, _delivered) = late_answer_host(&temp, db.clone());

    answer_q1(&orch, "Option A")
        .await
        .expect("the first answer applies");
    let turns = count(&db, "SELECT COUNT(*) FROM turns").await;
    let results = count(
        &db,
        "SELECT COUNT(*) FROM events WHERE event_type = 'tool_result'",
    )
    .await;

    let second = answer_q1(&orch, "Option B")
        .await
        .expect("a duplicate is not an error");

    assert!(second.duplicate);
    assert_eq!(second.continuation, None);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM turns").await, turns);
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM events WHERE event_type = 'tool_result'"
        )
        .await,
        results
    );
    assert_eq!(count(&db, "SELECT COUNT(*) FROM queued_messages").await, 0);
}

/// A non-blocking question answered while the turn that asked it is still
/// running. The answer is recorded and nothing is minted, resumed, or
/// interrupted: that open turn takes the answer at its next tool boundary. On
/// the base this failed inside the
/// answer transaction (`predecessor turn ... is in non-terminal state`),
/// rolling the answer back and returning a database error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_question_answered_mid_turn_mints_nothing() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BACKGROUND_MID_TURN).await;
    let orch = common::orchestrator(&temp, db.clone());
    let delivered = common::register_recording_process(&orch, "run-1", "session-1");

    let outcome = answer_q1(&orch, "Option A").await;

    let outcome = outcome.expect("the answer is applied");
    assert_eq!(
        outcome.continuation,
        Some(HostWaitContinuation::AtNextToolBoundary),
        "nothing resumes; the open turn carries the answer"
    );
    assert_eq!(
        outcome.answered_summary("q-1"),
        "Answered question q-1; the agent is mid-turn and receives the answer at its next tool \
         call"
    );
    assert!(
        db.query_text("SELECT response FROM prompts WHERE id = 'prompt-1'", ())
            .await
            .unwrap()
            .is_some(),
        "the answer is recorded"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM turns WHERE start_reason = 'prompt_response'"
        )
        .await,
        0,
        "no successor is minted for an open turn"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM turns WHERE id = 'turn-ask' AND state = 'running'"
        )
        .await,
        1,
        "the asking turn is not interrupted"
    );
    assert!(
        String::from_utf8_lossy(&delivered.lock().unwrap())
            .trim()
            .is_empty(),
        "nothing is prompted"
    );
}

/// A tool call as the asking agent's model makes it, reading `paths`. A tool no
/// handler answers, when `paths` is empty: the test then measures delivery alone.
fn model_call(run_id: Option<&str>, paths: &[&str]) -> McpCallbackRequest {
    let tool = if paths.is_empty() {
        "bogus_tool"
    } else {
        "read_batch"
    };
    let mut payload = serde_json::json!({ "paths": paths });
    cairn_common::protocol::stamp_callback_origin(
        &mut payload,
        cairn_common::protocol::MODEL_FACING_ORIGIN,
    );
    McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: run_id.map(str::to_string),
        tool: tool.to_string(),
        payload,
        tool_use_id: None,
        origin: None,
    }
}

/// How many of this tool boundary's reminders carry q-1's answer.
async fn answers_at_boundary(orch: &Orchestrator) -> usize {
    let cursors = Mutex::new(HashMap::new());
    let output = dispatch_tool(orch, &model_call(Some("run-1"), &[]), &cursors)
        .await
        .into_inner();
    output
        .reminders
        .iter()
        .filter(|reminder| is_framed_answer(reminder))
        .count()
}

/// The answer as the asker reads it: the question's URI and first line, then
/// the answer.
fn is_framed_answer(text: &str) -> bool {
    text.contains("Your question cairn://")
        && text.contains("/questions/q-1 (")
        && text.contains(") was answered:")
}

async fn pending_answer_rows(db: &LocalDb) -> i64 {
    count(
        db,
        "SELECT COUNT(*) FROM queued_messages
         WHERE id = 'prompt-1' AND delivery = 'steer' AND delivered_at IS NULL",
    )
    .await
}

/// An operator answers while the asker is running: the answer is recorded, no
/// turn is minted, and the asker's next tool boundary carries it exactly once,
/// however often the answer is replayed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_answer_to_an_open_turn_reaches_its_next_tool_boundary_once() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BACKGROUND_MID_TURN).await;
    let orch = common::orchestrator(&temp, db.clone());
    let delivered = common::register_recording_process(&orch, "run-1", "session-1");

    answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");
    assert_eq!(
        pending_answer_rows(&db).await,
        1,
        "one row, keyed by the prompt"
    );

    let replay = answer_q1(&orch, "Option A")
        .await
        .expect("a replay applies");
    assert!(replay.duplicate);
    assert_eq!(replay.continuation, None);
    assert_eq!(
        pending_answer_rows(&db).await,
        1,
        "a replay queues nothing more"
    );

    let cursors = Mutex::new(HashMap::new());
    let output = dispatch_tool(&orch, &model_call(Some("run-1"), &[]), &cursors)
        .await
        .into_inner();
    let carried: Vec<_> = output
        .reminders
        .iter()
        .filter(|reminder| is_framed_answer(reminder))
        .collect();
    assert_eq!(
        carried.len(),
        1,
        "the boundary carries it: {:?}",
        output.reminders
    );
    assert!(
        carried[0].contains("Option A"),
        "with the answer: {}",
        carried[0]
    );

    assert_eq!(answers_at_boundary(&orch).await, 0, "never a second time");
    answer_q1(&orch, "Option A")
        .await
        .expect("a late replay applies");
    assert_eq!(
        answers_at_boundary(&orch).await,
        0,
        "a late replay redelivers nothing"
    );

    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM turns WHERE start_reason = 'prompt_response'"
        )
        .await,
        0,
        "no turn is minted"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM turns WHERE id = 'turn-ask' AND state = 'running'"
        )
        .await,
        1,
        "the asking turn is not interrupted"
    );
    assert!(
        delivered_text(&delivered).trim().is_empty(),
        "nothing is prompted"
    );
}

/// The asking job's own `?wait` read hands it the answer, so its boundary does
/// not repeat it. Another job's reader takes nothing: the asker is still untold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wait_read_by_the_asking_job_takes_the_boundary_delivery() {
    const WAIT: &str = "cairn://p/lateq/1/1/builder/questions/q-1?wait";
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BACKGROUND_MID_TURN).await;
    db.execute(
        "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, uri_segment,
                           status, created_at, updated_at)
         SELECT 'job-other', execution_id, issue_id, project_id, 'Other', 'other',
                'running', 1, 1 FROM jobs WHERE id = 'job-1'",
        (),
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at,
                           start_mode)
         SELECT 'run-other', issue_id, project_id, 'job-other', 'live', 1, 1, 'resume'
         FROM runs WHERE id = 'run-1'",
        (),
    )
    .await
    .unwrap();
    let orch = common::orchestrator(&temp, db.clone());
    common::register_recording_process(&orch, "run-1", "session-1");
    answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");
    let cursors = Mutex::new(HashMap::new());

    let foreign = dispatch_tool(&orch, &model_call(Some("run-other"), &[WAIT]), &cursors)
        .await
        .into_inner();
    assert!(
        foreign.content.contains("Option A"),
        "another job reads the answer: {}",
        foreign.content
    );
    assert_eq!(
        pending_answer_rows(&db).await,
        1,
        "and leaves the asker's delivery"
    );

    let own = dispatch_tool(&orch, &model_call(Some("run-1"), &[WAIT]), &cursors)
        .await
        .into_inner();
    assert!(
        own.content.contains("Option A"),
        "the asker reads the answer: {}",
        own.content
    );
    assert!(
        !own.reminders
            .iter()
            .any(|reminder| is_framed_answer(reminder)),
        "the same call's boundary does not repeat it: {:?}",
        own.reminders
    );
    assert_eq!(answers_at_boundary(&orch).await, 0, "nor does a later one");
}

/// A background question asked before non-blocking asks stopped storing their
/// call id, as the live specimen on cairn/6070 was: the stored id must not make
/// its answer read as a blocking call's result while its turn is still open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_turn_carries_the_answer_of_a_question_that_stored_its_call() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(
        &db,
        &Asked {
            turn_state: "running",
            tool_use_id: Some("exec-q"),
            background: false,
        },
    )
    .await;
    let orch = common::orchestrator(&temp, db.clone());
    common::register_recording_process(&orch, "run-1", "session-1");

    let outcome = answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");

    assert_eq!(
        outcome.continuation,
        Some(HostWaitContinuation::AtNextToolBoundary)
    );
    assert_eq!(answers_at_boundary(&orch).await, 1);
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM events WHERE event_type = 'tool_result'"
        )
        .await,
        0,
        "its call already returned, so no synthetic result"
    );
}

/// A blocking ask whose yield failed leaves its turn open with its own inline
/// waiter. The waiter takes the answer itself, so no row repeats it at the tool
/// boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_inline_waiter_on_an_open_turn_takes_the_answer_alone() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(
        &db,
        &Asked {
            turn_state: "running",
            tool_use_id: Some("toolu-q"),
            background: false,
        },
    )
    .await;
    let orch = common::orchestrator(&temp, db.clone());
    common::register_recording_process(&orch, "run-1", "session-1");
    orch.process_state.yield_for_host("run-1", "turn-ask");

    let outcome = answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");

    assert_eq!(
        outcome.continuation, None,
        "the inline waiter owns the answer"
    );
    assert_eq!(count(&db, "SELECT COUNT(*) FROM queued_messages").await, 0);
}

/// A turn that ends before any tool boundary leaves the answer pending, and the
/// job's next resume claims it: it is never stranded on a turn that ended.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_answer_outlives_a_turn_that_ends_before_its_next_tool_boundary() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BACKGROUND_MID_TURN).await;
    let orch = common::orchestrator(&temp, db.clone());
    let delivered = common::register_recording_process(&orch, "run-1", "session-1");
    answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");

    db.execute(
        "UPDATE turns SET state = 'complete' WHERE id = 'turn-ask'",
        (),
    )
    .await
    .unwrap();
    orch.process_state.transition_to_warm("run-1");

    assert_eq!(
        answers_at_boundary(&orch).await,
        0,
        "an ended turn claims nothing"
    );
    assert_eq!(
        pending_answer_rows(&db).await,
        1,
        "the row waits for the resume claim"
    );

    nudge_job_for_urgency(&orch, "job-1", Delivery::Steer).expect("the job resumes");

    assert_eq!(
        delivered_text(&delivered)
            .matches(") was answered:")
            .count(),
        1,
        "the resume delivers the framed answer once: {}",
        delivered_text(&delivered)
    );
    assert_eq!(pending_answer_rows(&db).await, 0, "the row is claimed");
}

/// A pending permission raised by the turn that asked, which that turn yielded to.
async fn raise_permission_on_asking_turn(db: &LocalDb) {
    db.execute(
        "INSERT INTO permission_requests(id, run_id, job_id, tool_use_id, tool_name, tool_input,
                                         status, created_at, turn_id, uri_segment)
         VALUES ('perm-row-1', 'run-1', 'job-1', 'toolu-perm', 'run',
                 '{\"kind\":\"shell\",\"verb\":\"run\",\"descriptor\":\"x\",\"summary\":\"s\",\"request\":{\"cwd\":\"/wt\",\"tool\":\"run\",\"payload\":{},\"tool_use_id\":\"toolu-perm\"}}',
                 'pending', 1, 'turn-ask', 'perm-1')",
        (),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE turns SET yield_reason = 'permission' WHERE id = 'turn-ask'",
        (),
    )
    .await
    .unwrap();
}

async fn passive_answer_rows(db: &LocalDb) -> i64 {
    count(
        db,
        "SELECT COUNT(*) FROM queued_messages
         WHERE id = 'prompt-1' AND delivery = 'passive' AND delivered_at IS NULL",
    )
    .await
}

/// The operator answers a background question while the same turn waits inline
/// on a permission. The answer takes nothing from the permission: no turn is
/// minted, the permission still mints its own successor, and the answer reaches
/// the asker once that successor is running.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_background_answer_leaves_the_turn_to_the_permission_holding_it() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BACKGROUND_HELD).await;
    raise_permission_on_asking_turn(&db).await;
    let orch = common::orchestrator(&temp, db.clone());
    let delivered = common::register_recording_process(&orch, "run-1", "session-1");
    orch.process_state.yield_for_host("run-1", "turn-ask");

    let outcome = answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");

    assert_eq!(
        outcome.continuation,
        Some(HostWaitContinuation::AfterCurrentWait)
    );
    assert_eq!(
        outcome.answered_summary("q-1"),
        "Answered question q-1; the agent is waiting on something else and receives the answer \
         when it continues"
    );
    assert!(
        prompt_response_turns(&db).await.is_empty(),
        "no turn is minted"
    );
    assert_eq!(passive_answer_rows(&db).await, 1);

    // Nothing that nudges this job later may resume it around the permission.
    nudge_job_for_urgency(&orch, "job-1", Delivery::Steer).expect("the nudge is safe");
    assert_eq!(count(&db, "SELECT COUNT(*) FROM turns").await, 1);
    assert!(
        delivered_text(&delivered).trim().is_empty(),
        "nothing is prompted"
    );
    assert_eq!(passive_answer_rows(&db).await, 1);

    let permission =
        record_permission_response(&db, "perm-row-1", "allowed", r#"{"behavior":"allow"}"#, 100)
            .await
            .unwrap();
    let Some(HostWaitSuccessor::Successor {
        turn_id,
        inserted: true,
        ..
    }) = permission.successor
    else {
        panic!(
            "the permission keeps its own successor: {:?}",
            permission.successor
        );
    };
    db.execute(
        "UPDATE turns SET state = 'running', run_id = 'run-1' WHERE id = ?1",
        params![turn_id.as_str()],
    )
    .await
    .unwrap();
    orch.process_state.begin_turn("run-1", &turn_id);

    assert_eq!(
        answers_at_boundary(&orch).await,
        1,
        "it arrives once running"
    );
    assert_eq!(answers_at_boundary(&orch).await, 0, "and only once");
}

/// The same with no inline waiter: the turn is durably parked. Deliberately no
/// resume around the wait, which keeps its turn; the answer rides its resume.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_background_answer_never_resumes_around_a_durable_park() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BACKGROUND_HELD).await;
    raise_permission_on_asking_turn(&db).await;
    let (orch, delivered) = late_answer_host(&temp, db.clone());

    let outcome = answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");

    assert_eq!(
        outcome.continuation,
        Some(HostWaitContinuation::AfterCurrentWait)
    );
    assert!(
        prompt_response_turns(&db).await.is_empty(),
        "no turn is minted"
    );
    nudge_job_for_urgency(&orch, "job-1", Delivery::Steer).expect("the nudge is safe");
    assert_eq!(count(&db, "SELECT COUNT(*) FROM turns").await, 1);
    assert!(
        delivered_text(&delivered).trim().is_empty(),
        "nothing is prompted"
    );
    assert_eq!(passive_answer_rows(&db).await, 1);
}

/// Once the wait that held the turn has moved on and its successor finished, the
/// turn only reads as yielded. Nothing is holding it, so the answer must still
/// reach an idle job: it takes the waking queue path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_background_answer_after_the_wait_moved_on_takes_the_waking_queue() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BACKGROUND_HELD).await;
    insert_turn(
        &db,
        "turn-after",
        2,
        "complete",
        "permission_response",
        Some("turn-ask"),
    )
    .await;
    let (orch, _delivered) = late_answer_host(&temp, db.clone());

    let outcome = answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");

    assert_eq!(outcome.continuation, Some(HostWaitContinuation::Queued));
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM queued_messages WHERE id = 'prompt-1' AND delivery = 'queue'"
        )
        .await,
        1
    );
    assert!(prompt_response_turns(&db).await.is_empty());
}

/// A sibling blocking question's pending successor is that answer's to start, and
/// its inline waiter belongs to that answer too. A background answer neither
/// drives the turn nor mistakes that waiter for its own: it queues.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_background_answer_never_drives_a_siblings_successor() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BACKGROUND_HELD).await;
    insert_turn(
        &db,
        "turn-sibling",
        2,
        "pending",
        "prompt_response",
        Some("turn-ask"),
    )
    .await;
    let orch = common::orchestrator(&temp, db.clone());
    let delivered = common::register_recording_process(&orch, "run-1", "session-1");
    orch.process_state.yield_for_host("run-1", "turn-ask");

    let outcome = answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");

    assert_eq!(outcome.continuation, Some(HostWaitContinuation::Queued));
    assert_eq!(
        prompt_response_turns(&db).await,
        vec![(
            "turn-sibling".to_string(),
            "pending".to_string(),
            Some("run-1".to_string()),
            None
        )],
        "the sibling's successor is untouched"
    );
    assert!(
        delivered_text(&delivered).trim().is_empty(),
        "nothing is prompted"
    );
}

/// A Codex approval waits inline without yielding its turn. A background answer
/// on that open turn is not the waiter's, so it still takes the steer row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_background_answer_on_an_open_turn_another_wait_holds_still_reaches_the_boundary() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_question(&db, &BACKGROUND_MID_TURN).await;
    let orch = common::orchestrator(&temp, db.clone());
    common::register_recording_process(&orch, "run-1", "session-1");
    orch.process_state.yield_for_host("run-1", "turn-ask");

    let outcome = answer_q1(&orch, "Option A")
        .await
        .expect("the answer applies");

    assert_eq!(
        outcome.continuation,
        Some(HostWaitContinuation::AtNextToolBoundary)
    );
    assert_eq!(pending_answer_rows(&db).await, 1);
}
