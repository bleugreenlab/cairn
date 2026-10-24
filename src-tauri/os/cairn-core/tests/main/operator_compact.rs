//! cairn/4955: Compact is the operator's cold reset.
//!
//! Pressed on a job with live work, Compact ends all of it with a typed
//! `operator_compact` cause — the process, its executor batches, its turn, its
//! parked waits, its pending permissions and questions — closes the session with
//! one boundary written in the same transaction as the close, and continues on a
//! fresh session seeded from the digest, carrying whatever was queued.

use crate::common;

use std::io::Write;
use std::sync::{Arc, Mutex};

use cairn_core::internal::agent_process::process::RunHandle;
use cairn_core::internal::dispatch::deliver_at_tool_boundary;
use cairn_core::internal::execution::operator_compact::{
    compact_job, test_hooks, ColdCompact, CompactResult, OperatorCompactOutcome,
};
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::storage::{LocalDb, RowExt};
use cairn_core::models::{
    ExecutionSnapshot, RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
};
use cairn_db::turso::params;
use cairn_models::session::claude::ClaudeControl;

/// An issue node mid-turn: an open Claude session with a native handle, a live
/// run, a running head turn, and a transcript the digest can render.
async fn seed_running_job(db: &LocalDb) {
    seed_job(db, "claude", "running").await;
}

/// An issue node on `backend` whose head turn sits in `turn_state`.
async fn seed_job(db: &LocalDb, backend: &str, turn_state: &str) {
    let project_id = common::create_project(db, "oc").await;
    let snapshot = ExecutionSnapshot::new(
        RecipeSnapshot {
            id: "recipe-default".to_string(),
            name: "Default".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: Vec::new(),
            edges: Vec::new(),
        },
        Default::default(),
        Default::default(),
        TriggerContext {
            issue_id: Some("issue-1".to_string()),
            project_id: project_id.clone(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    )
    .to_json()
    .unwrap();
    let backend = backend.to_string();
    let turn_state = turn_state.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let snapshot = snapshot.clone();
        let backend = backend.clone();
        let turn_state = turn_state.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                 VALUES ('issue-1', ?1, 7, 'stuck node', 'active', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
                 VALUES ('exec-1', 'recipe-default', 'issue-1', ?1, 'running', 1, 1, ?2)",
                params![project_id.as_str(), snapshot.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, uri_segment,
                                   status, created_at, updated_at)
                 VALUES ('job-1', 'exec-1', 'issue-1', ?1, 'Builder', 'builder', 'running', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions (id, job_id, backend, status, sequence, backend_id, created_at, updated_at)
                 VALUES ('session-1', 'job-1', ?1, 'open', 1, 'handle-1', 1, 1)",
                params![backend.as_str()],
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_session_id = 'session-1' WHERE id = 'job-1'",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs (id, issue_id, project_id, job_id, status, session_id,
                                   created_at, updated_at, start_mode)
                 VALUES ('run-1', 'issue-1', ?1, 'job-1', 'live', 'session-1', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason,
                                    created_at, updated_at)
                 VALUES ('turn-1', 'session-1', 'run-1', 'job-1', 1, ?1, 'initial', 1, 1)",
                params![turn_state.as_str()],
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_turn_id = 'turn-1' WHERE id = 'job-1'",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO events (id, run_id, session_id, sequence, timestamp, event_type, data,
                                     created_at, turn_id)
                 VALUES ('event-1', 'run-1', 'session-1', 1, 1, 'assistant',
                         '{\"type\":\"assistant\",\"content\":\"working on the stuck thing\"}', 1, 'turn-1')",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// The agent's stdin, standing in for a live provider process.
struct DiscardingStdin;

impl Write for DiscardingStdin {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn register_process(orch: &Orchestrator, run_id: &str, session_id: &str) {
    let mut processes = orch.process_state.processes.lock().unwrap();
    let handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(ClaudeControl::boxed(Box::new(
            DiscardingStdin,
        ))))),
        Some(session_id.to_string()),
        Some("job-1".to_string()),
    );
    processes.register(run_id.to_string(), handle);
}

async fn text(db: &LocalDb, sql: &'static str) -> Option<String> {
    db.query_opt(sql, (), |row| row.opt_text(0))
        .await
        .unwrap()
        .flatten()
}

async fn count(db: &LocalDb, sql: &'static str) -> i64 {
    db.query_one(sql, (), |row| row.i64(0)).await.unwrap()
}

/// Compact with the operator's consent to the cold path.
fn compact(orch: &Orchestrator) -> Result<OperatorCompactOutcome, String> {
    match compact_job(orch, "job-1", None, ColdCompact::Confirmed)? {
        CompactResult::Compacted(outcome) => Ok(*outcome),
        CompactResult::ConfirmationRequired(counts) => {
            panic!("a confirmed Compact asked for confirmation: {counts:?}")
        }
    }
}

/// The teardown hooks are one process-wide slot each, so tests that install
/// one take turns; otherwise one test's install or clear replaces another's.
static HOOKS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Whether `db` is the database of `orch`, for hooks shared across tests.
fn owns(orch: &Orchestrator, db: &Arc<LocalDb>) -> bool {
    Arc::ptr_eq(&orch.db.local, db)
}

/// Run `sql` against `db` from a synchronous caller (a teardown hook).
fn execute_blocking(db: Arc<LocalDb>, sql: &'static str) {
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(db.execute(sql, ()))
            .unwrap();
    })
    .join()
    .unwrap();
}

/// Acceptance 1: a live run with a running turn. The process is gone, the run
/// settled, the turn `cancelled/operator_compact`, the session closed with its
/// reason and exactly one operator boundary, and a new session is open from the
/// digest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_compact_kills_settles_and_reseeds() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_running_job(&db).await;
    register_process(&orch, "run-1", "session-1");

    let outcome = compact(&orch).expect("a running job compacts");

    assert!(
        orch.process_state.has_process("run-1") != Some(true),
        "the old process must be gone"
    );
    assert_eq!(
        text(&db, "SELECT status FROM runs WHERE id = 'run-1'")
            .await
            .as_deref(),
        Some("exited")
    );
    assert_eq!(
        text(&db, "SELECT exit_reason FROM runs WHERE id = 'run-1'")
            .await
            .as_deref(),
        Some("operator_compact")
    );
    assert_eq!(
        text(
            &db,
            "SELECT state || '/' || end_reason FROM turns WHERE id = 'turn-1'"
        )
        .await
        .as_deref(),
        Some("cancelled/operator_compact")
    );
    assert_eq!(
        text(
            &db,
            "SELECT status || '/' || terminal_reason FROM sessions WHERE id = 'session-1'"
        )
        .await
        .as_deref(),
        Some("closed/operator_compact")
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM events WHERE event_type = 'system:session_boundary'"
        )
        .await,
        1,
        "exactly one boundary marks the compaction"
    );
    let boundary = text(
        &db,
        "SELECT data FROM events WHERE event_type = 'system:session_boundary'",
    )
    .await
    .unwrap();
    let boundary: serde_json::Value = serde_json::from_str(&boundary).unwrap();
    let raw = boundary.get("raw").unwrap_or(&boundary);
    assert_eq!(raw["kind"], "digest_reseed");
    assert_eq!(raw["initiatedBy"], "operator");
    assert!(raw["seedBytes"].as_u64().unwrap_or(0) > 0, "{raw}");

    let successor = text(
        &db,
        "SELECT current_session_id FROM jobs WHERE id = 'job-1'",
    )
    .await
    .unwrap();
    assert_ne!(successor, "session-1");
    assert_eq!(
        text(&db, "SELECT status FROM sessions WHERE id = (SELECT current_session_id FROM jobs WHERE id = 'job-1')")
            .await
            .as_deref(),
        Some("open")
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM events e JOIN jobs j ON j.current_session_id = e.session_id
              WHERE j.id = 'job-1' AND e.event_type = 'user:seed'"
        )
        .await,
        1,
        "the new session opens from the digest"
    );

    let report = outcome
        .report
        .expect("the cold path reports what it tore down");
    assert_eq!(report.turns_cancelled, 1);
    assert_eq!(report.runs_killed, 1);
    assert!(outcome.diagnostic_error.is_none());
    let recorded = text(
        &db,
        "SELECT data FROM events WHERE event_type = 'system:operator_compact' AND session_id = 'session-1'",
    )
    .await
    .expect("the old session records what was torn down");
    assert!(recorded.contains("turnsCancelled"), "{recorded}");
}

/// Acceptance 2: a run batch parked on a durable wait. The wait is abandoned
/// with the typed operator cause (so no resolver can resume it), the parked
/// call is answered with it, and the executor running the batch is told to
/// cancel it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_compact_abandons_parked_wait_and_cancels_batch() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_job(&db, "claude", "yielded").await;
    db.execute(
        "INSERT INTO agent_waits(id, job_id, run_id, session_id, predecessor_turn_id, tool_use_id,
                                 condition_json, state, created_at)
         VALUES ('wait-1', 'job-1', 'run-1', 'session-1', 'turn-1', 'toolu-run',
                 '{\"kind\":\"run_batch\",\"request_id\":\"req-1\",\"commits\":false}',
                 'pending', 1)",
        (),
    )
    .await
    .unwrap();
    register_process(&orch, "run-1", "session-1");
    let (executor_tx, mut executor_rx) = tokio::sync::mpsc::unbounded_channel();
    let generation = orch.fleet.attach_executor(executor_tx);
    let _batch = orch
        .fleet
        .claim_job_command_for_test("req-1", "job-1", generation);

    let outcome = compact(&orch).expect("a parked job compacts");

    let (state, resolution) = db
        .query_one(
            "SELECT state, resolution_json FROM agent_waits WHERE id = 'wait-1'",
            (),
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .unwrap();
    assert_eq!(state, "cancelled");
    let resolution: serde_json::Value = serde_json::from_str(&resolution.unwrap()).unwrap();
    assert_eq!(resolution["outcome"], "abandoned");
    assert_eq!(resolution["cause"], "operator_compact");
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM events WHERE run_id = 'run-1' AND event_type = 'tool_result'
               AND data LIKE '%operator compacted%'"
        )
        .await,
        1,
        "the parked call is answered with the compaction, once"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM turns WHERE job_id = 'job-1' AND start_reason = 'wait_resolved'"
        )
        .await,
        0,
        "no wait resolver minted a successor"
    );
    let mut cancelled = Vec::new();
    while let Ok(message) = executor_rx.try_recv() {
        if let cairn_common::executor_protocol::ExecutorMessage::Cancel { request_id, .. } = message
        {
            cancelled.push(request_id);
        }
    }
    assert_eq!(
        cancelled,
        vec!["req-1".to_string()],
        "the executor running the batch is told to cancel it"
    );
    let report = outcome.report.unwrap();
    assert_eq!(report.waits_abandoned, 1);
    assert_eq!(report.batches_cancelled, 1);
}

/// Acceptance 3: a pending permission and a pending question are withdrawn
/// with the operator cause, and a late answer to either is refused: it records
/// nothing, replays nothing, and starts no turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_compact_withdraws_permission_and_question() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_job(&db, "claude", "yielded").await;
    db.execute_script(
        "INSERT INTO permission_requests(id, run_id, job_id, tool_use_id, tool_name, tool_input,
                                         status, created_at, turn_id, uri_segment)
           VALUES ('perm-1', 'run-1', 'job-1', 'toolu-perm', 'Bash', '{}', 'pending', 1, 'turn-1', 'perm-1');
         INSERT INTO prompts(id, run_id, job_id, turn_id, questions, created_at, uri_segment)
           VALUES ('q-1', 'run-1', 'job-1', 'turn-1', '[{\"question\":\"Which?\",\"options\":[]}]', 1, 'q-1');",
    )
    .await
    .unwrap();
    register_process(&orch, "run-1", "session-1");

    let outcome = compact(&orch).expect("a job waiting on asks compacts");
    let report = outcome.report.unwrap();
    assert_eq!(report.permissions_withdrawn, 1);
    assert_eq!(report.questions_withdrawn, 1);

    let (status, response) = db
        .query_one(
            "SELECT status, response FROM permission_requests WHERE id = 'perm-1'",
            (),
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .unwrap();
    assert_eq!(status, "withdrawn");
    assert_eq!(
        cairn_core::internal::mcp::handlers::permission::withdrawn_cause(response.as_deref()),
        Some(cairn_core::internal::mcp::handlers::permission::WithdrawalCause::OperatorCompact)
    );
    assert_eq!(
        text(&db, "SELECT withdrawn_cause FROM prompts WHERE id = 'q-1'")
            .await
            .as_deref(),
        Some("operator_compact")
    );

    let turns_before = count(&db, "SELECT COUNT(*) FROM turns").await;
    let late_question = cairn_core::internal::mcp::handlers::planning::answer_prompt_id(
        &orch,
        "q-1",
        "the first one".to_string(),
        cairn_core::turns::queries::ResolutionProvenance::new(
            cairn_common::identity::AppearanceTransport::LocalInvoke,
            None,
        ),
    )
    .await
    .expect("a late answer is refused, not an error");
    let notice = late_question
        .withdrawn
        .expect("the refusal names the withdrawal");
    assert!(notice.contains("compacted"), "{notice}");
    let late_permission =
        cairn_core::internal::mcp::handlers::permission::record_permission_response(
            &db,
            "perm-1",
            "allowed",
            r#"{"behavior":"allow"}"#,
            100,
        )
        .await
        .unwrap();
    assert!(late_permission.withdrawn_notice.is_some());
    assert!(late_permission.successor_turn_id().is_none());

    assert_eq!(
        text(&db, "SELECT response FROM prompts WHERE id = 'q-1'").await,
        None,
        "the late answer was not recorded"
    );
    assert_eq!(
        text(
            &db,
            "SELECT status FROM permission_requests WHERE id = 'perm-1'"
        )
        .await
        .as_deref(),
        Some("withdrawn")
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM turns").await,
        turns_before,
        "no late answer starts a turn"
    );
}

/// Acceptance 4: what the operator sent while the session was stuck lands in
/// the new session's first turn, each message once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_compact_carries_queued_messages() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_running_job(&db).await;
    db.execute_script(
        "INSERT INTO queued_messages(id, job_id, content, delivery, created_at)
           VALUES ('qm-1', 'job-1', 'first stuck message', 'queue', 2);
         INSERT INTO queued_messages(id, job_id, content, delivery, created_at)
           VALUES ('qm-2', 'job-1', 'second stuck message', 'queue', 3);",
    )
    .await
    .unwrap();
    register_process(&orch, "run-1", "session-1");

    compact(&orch).expect("a running job with queued messages compacts");

    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM queued_messages WHERE job_id = 'job-1' AND delivered_at IS NULL"
        )
        .await,
        0,
        "both messages were claimed by the successor"
    );
    for needle in ["%first stuck message%", "%second stuck message%"] {
        let delivered = db
            .query_one(
                "SELECT COUNT(*) FROM events e JOIN jobs j ON j.id = 'job-1'
                  WHERE e.session_id = j.current_session_id
                    AND e.turn_id = j.current_turn_id
                    AND e.event_type = 'user' AND e.data LIKE ?1",
                params![needle],
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(
            delivered, 1,
            "{needle} lands once in the successor's first turn"
        );
    }
}

/// Acceptance 5: a Codex session compacts onto a new provider thread. The
/// successor carries no native handle and starts fresh, so no goal (active,
/// paused, or otherwise) is inherited from the old thread. A goal turn the
/// app-server admits between the settle and the kill is cut off too, and
/// counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_compact_codex_successor_starts_a_fresh_thread() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_job(&db, "codex", "running").await;
    register_process(&orch, "run-1", "session-1");
    let own_db = db.clone();
    let _hooks = HOOKS.lock().await;
    test_hooks::set_after_settle(Some(Box::new(move |orch: &Orchestrator, job_id: &str| {
        if job_id == "job-1" && Arc::ptr_eq(&orch.db.local, &own_db) {
            execute_blocking(
                    own_db.clone(),
                    "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason,
                                        created_at, updated_at)
                     VALUES ('goal-turn', 'session-1', 'run-1', 'job-1', 2, 'running', 'follow_up', 2, 2)",
                );
        }
    })));

    let outcome = compact(&orch);
    test_hooks::set_after_settle(None);
    let outcome = outcome.expect("a Codex job compacts");

    assert_eq!(
        text(
            &db,
            "SELECT state || '/' || end_reason FROM turns WHERE id = 'goal-turn'"
        )
        .await
        .as_deref(),
        Some("cancelled/operator_compact"),
        "a goal turn admitted in the gap is cut off, not completed"
    );
    assert_eq!(outcome.report.unwrap().turns_cancelled, 2);
    let (backend, backend_id) = db
        .query_one(
            "SELECT s.backend, s.backend_id FROM sessions s
               JOIN jobs j ON j.current_session_id = s.id
              WHERE j.id = 'job-1'",
            (),
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .unwrap();
    assert_eq!(backend, "codex");
    assert_eq!(
        backend_id, None,
        "the successor carries no native thread handle"
    );
    assert_eq!(
        text(
            &db,
            "SELECT start_mode FROM runs WHERE job_id = 'job-1' AND id <> 'run-1'"
        )
        .await
        .as_deref(),
        Some("fresh"),
        "the successor starts a new provider thread rather than resuming the old one"
    );
}

/// Acceptance 6: Compact on an idle job is the ordinary digest reseed, with no
/// teardown and no teardown record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_compact_is_the_ordinary_reseed() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_job(&db, "claude", "complete").await;
    db.execute("UPDATE runs SET status = 'exited' WHERE id = 'run-1'", ())
        .await
        .unwrap();

    // An idle Compact needs no consent: there is nothing to tear down.
    let outcome = match compact_job(&orch, "job-1", None, ColdCompact::Unconfirmed)
        .expect("an idle job compacts")
    {
        CompactResult::Compacted(outcome) => *outcome,
        CompactResult::ConfirmationRequired(counts) => {
            panic!("an idle job asked for confirmation: {counts:?}")
        }
    };

    assert!(outcome.report.is_none(), "nothing was live to tear down");
    assert_eq!(
        text(
            &db,
            "SELECT terminal_reason FROM sessions WHERE id = 'session-1'"
        )
        .await
        .as_deref(),
        Some("rotated_reseed")
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM events WHERE event_type = 'system:operator_compact'"
        )
        .await,
        0
    );
}

/// Tenet 1256: Compact on a long-running node seeds the successor within the
/// seed budget, not with the node's whole history. Before it, a coordinator
/// whose history neared its window was compacted onto a session already full.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_of_a_long_history_seeds_a_budgeted_session() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_job(&db, "claude", "complete").await;
    db.execute("UPDATE runs SET status = 'exited' WHERE id = 'run-1'", ())
        .await
        .unwrap();
    // Two hundred more completed turns of about 1,300 tokens each: a history of
    // roughly a quarter million tokens, twenty times the default budget.
    let body = "analysis of the parser module and its callers ".repeat(60);
    for sequence in 2..=201_i64 {
        let turn_id = format!("turn-{sequence}");
        db.execute(
            "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason,
                                created_at, updated_at)
             VALUES (?1, 'session-1', 'run-1', 'job-1', ?2, 'complete', 'follow_up', ?2, ?2)",
            params![turn_id.as_str(), sequence],
        )
        .await
        .unwrap();
        let reply = if sequence == 201 {
            format!("NEWEST_TURN_MARKER {body}")
        } else {
            format!("Reply {sequence}. {body}")
        };
        for (offset, kind, content) in [
            (0, "user", format!("Step {sequence}: carry on. {body}")),
            (1, "assistant", reply),
        ] {
            db.execute(
                "INSERT INTO events (id, run_id, session_id, sequence, timestamp, event_type, data,
                                     created_at, turn_id)
                 VALUES (?1, 'run-1', 'session-1', ?2, ?2, ?3, ?4, ?2, ?5)",
                params![
                    format!("event-{sequence}-{kind}"),
                    sequence * 10 + offset,
                    kind,
                    serde_json::json!({ "type": kind, "content": content }).to_string(),
                    turn_id.as_str()
                ],
            )
            .await
            .unwrap();
        }
    }
    db.execute(
        "UPDATE jobs SET current_turn_id = 'turn-201' WHERE id = 'job-1'",
        (),
    )
    .await
    .unwrap();

    match compact_job(&orch, "job-1", None, ColdCompact::Unconfirmed).expect("an idle job compacts")
    {
        CompactResult::Compacted(_) => {}
        CompactResult::ConfirmationRequired(counts) => {
            panic!("an idle job asked for confirmation: {counts:?}")
        }
    }

    let history: i64 = count(
        &db,
        "SELECT SUM(LENGTH(data)) FROM events WHERE session_id = 'session-1'",
    )
    .await;
    let seed = text(
        &db,
        "SELECT e.data FROM events e JOIN jobs j ON j.current_session_id = e.session_id
          WHERE j.id = 'job-1' AND e.event_type = 'user:seed'",
    )
    .await
    .expect("the successor opens from a seed");
    // The default budget is 12,000 tokens. At a generous five bytes a token plus
    // the header and the event's own JSON, a budgeted seed stays under 80 KB;
    // the history it replaces is over a megabyte.
    assert!(
        history > 1_000_000,
        "the fixture history is only {history} bytes"
    );
    assert!(
        seed.len() < 80_000,
        "a Compact seeded {} bytes from a {history}-byte history",
        seed.len()
    );
    assert!(seed.contains("NEWEST_TURN_MARKER"), "{seed}");
    assert!(seed.contains("## Working documents"), "{seed}");
    assert!(seed.contains("## Earlier turns"), "{seed}");
    assert!(seed.contains("/chat?offset=0&limit="), "{seed}");
    assert!(seed.contains("/todos`"), "{seed}");
}

/// Without the operator's consent, a Compact that finds live work tears
/// nothing down: it reports the work for the operator to confirm against.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unconfirmed_compact_of_live_work_asks_and_touches_nothing() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_running_job(&db).await;
    register_process(&orch, "run-1", "session-1");

    let counts = match compact_job(&orch, "job-1", None, ColdCompact::Unconfirmed)
        .expect("an unconfirmed Compact answers")
    {
        CompactResult::ConfirmationRequired(counts) => counts,
        CompactResult::Compacted(_) => panic!("live work was torn down without consent"),
    };

    assert_eq!(counts.turns, 1);
    assert_eq!(counts.runs, 1);
    assert_eq!(
        text(&db, "SELECT state FROM turns WHERE id = 'turn-1'")
            .await
            .as_deref(),
        Some("running")
    );
    assert_eq!(
        text(&db, "SELECT status FROM sessions WHERE id = 'session-1'")
            .await
            .as_deref(),
        Some("open")
    );
    assert!(
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .contains_key("run-1"),
        "the live process is untouched"
    );
}

/// A bookkeeping failure after the kill is reported, and the job still gets
/// its successor: nothing past the kill may strand a torn-down job.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_compact_reseeds_when_its_teardown_record_fails() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_running_job(&db).await;
    register_process(&orch, "run-1", "session-1");
    let own_db = db.clone();
    let _hooks = HOOKS.lock().await;
    test_hooks::set_record_failure(Some(Box::new(move |orch: &Orchestrator| {
        owns(orch, &own_db).then(|| "injected record failure".to_string())
    })));

    let outcome = compact(&orch);
    test_hooks::set_record_failure(None);
    let outcome = outcome.expect("a failed record does not fail the Compact");

    assert!(
        outcome
            .diagnostic_error
            .as_deref()
            .is_some_and(|error| error.contains("injected record failure")),
        "the failure is reported: {:?}",
        outcome.diagnostic_error
    );
    assert_eq!(
        text(&db, "SELECT status FROM sessions WHERE id = 'session-1'")
            .await
            .as_deref(),
        Some("closed")
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM sessions s JOIN jobs j ON j.current_session_id = s.id
              WHERE j.id = 'job-1' AND s.status = 'open' AND s.id <> 'session-1'"
        )
        .await,
        1,
        "the successor session is open"
    );
}

async fn enqueue_steer(db: &LocalDb, id: &str, content: &str) {
    db.execute(
        "INSERT INTO queued_messages(id, job_id, content, delivery, created_at)
         VALUES (?1, 'job-1', ?2, 'steer', 5)",
        params![id.to_string(), content.to_string()],
    )
    .await
    .unwrap();
}

async fn pending(db: &LocalDb, id: &str) -> bool {
    db.query_one(
        "SELECT COUNT(*) FROM queued_messages WHERE id = ?1 AND delivered_at IS NULL",
        params![id.to_string()],
        |row| row.i64(0),
    )
    .await
    .unwrap()
        == 1
}

/// A tool boundary delivers only into a running turn of its own run, and
/// never while the job's launch lock is held: a held lock means a launch (or a
/// Compact) will deliver instead, and a settled turn means the run is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tool_boundary_delivery_is_fenced_by_the_launch_lock_and_the_running_turn() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_running_job(&db).await;
    enqueue_steer(&db, "qm-steer", "look at the parser").await;

    let lock = orch.job_launch_lock("job-1");
    let held = lock.lock().unwrap();
    let during_launch = deliver_at_tool_boundary(&orch, "run-1", "job-1");
    drop(held);
    assert!(
        during_launch.is_empty(),
        "a held launch lock defers delivery"
    );
    assert!(
        pending(&db, "qm-steer").await,
        "the deferred steer is still pending"
    );

    let delivered = deliver_at_tool_boundary(&orch, "run-1", "job-1");
    assert!(
        delivered
            .iter()
            .any(|reminder| reminder.contains("look at the parser")),
        "a running turn with the lock free receives the steer: {delivered:?}"
    );
    assert!(!pending(&db, "qm-steer").await);

    db.execute(
        "UPDATE turns SET state = 'cancelled' WHERE id = 'turn-1'",
        (),
    )
    .await
    .unwrap();
    enqueue_steer(&db, "qm-late", "too late for this run").await;
    assert!(
        deliver_at_tool_boundary(&orch, "run-1", "job-1").is_empty(),
        "a run with no running turn claims nothing"
    );
    assert!(pending(&db, "qm-late").await);
}

/// A steer whose tool boundary arrives while the Compact is tearing the
/// session down, or after it finished, is not consumed by the dead run: it
/// lands in the successor's first turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_compact_carries_steers_raised_at_old_tool_boundaries() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_running_job(&db).await;
    register_process(&orch, "run-1", "session-1");
    let own_db = db.clone();
    let during: Arc<Mutex<Option<Vec<String>>>> = Arc::new(Mutex::new(None));
    let observed = during.clone();
    let _hooks = HOOKS.lock().await;
    test_hooks::set_after_settle(Some(Box::new(move |orch: &Orchestrator, job_id: &str| {
        if job_id == "job-1" && owns(orch, &own_db) {
            execute_blocking(
                own_db.clone(),
                "INSERT INTO queued_messages(id, job_id, content, delivery, created_at)
                 VALUES ('qm-during', 'job-1', 'raised during the teardown', 'steer', 5)",
            );
            *observed.lock().unwrap() = Some(deliver_at_tool_boundary(orch, "run-1", "job-1"));
        }
    })));

    let outcome = compact(&orch);
    test_hooks::set_after_settle(None);
    outcome.expect("a running job compacts");

    assert_eq!(
        during.lock().unwrap().as_deref(),
        Some(&[][..]),
        "the old run's boundary delivered nothing during the teardown"
    );
    let in_successor = db
        .query_one(
            "SELECT COUNT(*) FROM events e JOIN jobs j ON j.id = 'job-1'
              WHERE e.session_id = j.current_session_id
                AND e.turn_id = j.current_turn_id
                AND e.event_type = 'user' AND e.data LIKE '%raised during the teardown%'",
            (),
            |row| row.i64(0),
        )
        .await
        .unwrap();
    assert_eq!(
        in_successor, 1,
        "the steer lands in the successor's first turn"
    );

    enqueue_steer(&db, "qm-after", "raised after the Compact").await;
    assert!(
        deliver_at_tool_boundary(&orch, "run-1", "job-1").is_empty(),
        "a tool call that outlived the Compact claims nothing"
    );
    assert!(pending(&db, "qm-after").await, "it waits for the successor");
}
