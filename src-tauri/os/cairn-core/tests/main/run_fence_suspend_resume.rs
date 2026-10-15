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
use cairn_core::internal::mcp::handlers::permission::{
    await_permission_for_test, record_permission_response,
};
use cairn_core::internal::orchestrator::lifecycle::{stop_session, suspend_run_for_durable_wait};
use cairn_core::internal::services::testing::MockChildProcess;
use cairn_core::internal::services::ChildProcess;
use cairn_core::internal::storage::{LocalDb, RowExt};
use cairn_db::turso::params;

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
    let project_id = common::create_project(db, "trs").await;
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

/// The production permission suspension establishes the inner owner and retires
/// the correlated outer run-batch owner without manufacturing a continuation or
/// model-facing result. The eventual answer remains the sole continuation owner.
#[tokio::test(start_paused = true)]
async fn permission_suspension_atomically_takes_ownership_without_a_result() {
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
    db.execute("DELETE FROM permission_requests", ())
        .await
        .unwrap();
    db.execute(
        "INSERT INTO agent_waits(
            id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,
            condition_json,state,created_at
         ) VALUES(
            'outer-wait','job-1','run-1','session-1','turn-pred','toolu-perm',
            '{\"kind\":\"run_batch\",\"request_id\":\"request-1\",\"commits\":false}',
            'pending',1
         )",
        (),
    )
    .await
    .unwrap();
    let orch = orchestrator(&temp, db.clone());

    assert_eq!(
        await_permission_for_test(
            &orch,
            "run-1",
            "toolu-perm",
            "run",
            &serde_json::json!({"command":"echo guarded"}),
        )
        .await,
        "suspended"
    );

    let (request_id, status) = db
        .query_one(
            "SELECT id,status FROM permission_requests WHERE run_id='run-1'",
            (),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .unwrap();
    assert_eq!(status, "pending");

    let (state, resolution, resolved_at, successor, result_stored_at) = db
        .query_one(
            "SELECT state,resolution_json,resolved_at,successor_turn_id,result_stored_at
             FROM agent_waits WHERE id='outer-wait'",
            (),
            |row| {
                Ok((
                    row.text(0)?,
                    row.opt_text(1)?,
                    row.opt_i64(2)?,
                    row.opt_text(3)?,
                    row.opt_i64(4)?,
                ))
            },
        )
        .await
        .unwrap();
    assert_eq!(state, "resolved");
    assert_eq!(
        resolution.as_deref(),
        Some(r#"{"outcome":"transferred_to_inner_wait"}"#)
    );
    assert!(resolved_at.is_some());
    assert!(successor.is_none());
    assert!(result_stored_at.is_none());
    let tool_results = db
        .query_one(
            "SELECT COUNT(*) FROM events WHERE event_type='tool_result'",
            (),
            |row| row.i64(0),
        )
        .await
        .unwrap();
    assert_eq!(
        tool_results, 0,
        "suspension is control flow, not a tool result"
    );

    let resume =
        record_permission_response(&db, &request_id, "allowed", r#"{"behavior":"allow"}"#, 100)
            .await
            .unwrap();
    assert!(!resume.duplicate);
    assert_eq!(resume.predecessor_turn_id.as_deref(), Some("turn-pred"));
    assert!(resume.successor_turn_id().is_some());
}

#[tokio::test(start_paused = true)]
async fn permission_handoff_refuses_an_outer_resolver_and_rolls_back_insertion() {
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
    db.execute("DELETE FROM permission_requests", ())
        .await
        .unwrap();
    db.execute(
        "INSERT INTO agent_waits(
            id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,
            condition_json,state,created_at
         ) VALUES(
            'outer-wait','job-1','run-1','session-1','turn-pred','toolu-perm',
            '{\"kind\":\"run_batch\",\"request_id\":\"request-1\",\"commits\":false}',
            'resolving',1
         )",
        (),
    )
    .await
    .unwrap();
    let orch = orchestrator(&temp, db.clone());

    assert_eq!(
        await_permission_for_test(
            &orch,
            "run-1",
            "toolu-perm",
            "run",
            &serde_json::json!({"command":"echo guarded"}),
        )
        .await,
        "unavailable"
    );
    let permission_count = db
        .query_one(
            "SELECT COUNT(*) FROM permission_requests WHERE run_id='run-1'",
            (),
            |row| row.i64(0),
        )
        .await
        .unwrap();
    assert_eq!(
        permission_count, 0,
        "the permission insert must roll back when the outer resolver owns continuation"
    );
    let state = db
        .query_one(
            "SELECT state FROM agent_waits WHERE id='outer-wait'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert_eq!(state, "resolving");
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
/// persisted `current_turn_id` recovers that turn as the predecessor and mints
/// its successor turn -- pending, because recording an answer never starts it.
/// Exactly one caller starts it afterwards: the inline waiter, or the slow-path
/// driver continuing the job into it (cairn/4936).
#[tokio::test]
async fn null_turn_request_recovers_predecessor_and_mints_pending_successor() {
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
        resume.successor_turn_id().is_some(),
        "a successor turn must be created so the run can resume"
    );
    assert_eq!(
        successor_turn_state(&db, "turn-pred").await.as_deref(),
        Some("pending"),
        "recording the answer mints the successor without starting it"
    );
}

/// A Codex approval never yields its turn: `request_codex_permission` stores the
/// still-running turn and polls the broadcast with no time limit. Answering it
/// must deliver the broadcast, mint no successor (the provider turn is still
/// open and the Codex poll owns it), and run no slow-path resume.
///
/// On `main` the answer failed before the broadcast: minting a successor of a
/// `running` predecessor errored, so every job-owned Codex approval hung.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_answer_with_open_provider_turn_broadcasts_and_mints_nothing() {
    use cairn_core::internal::mcp::handlers::permission::{
        resolve_permission_request, AnswerSurface, PermissionAnswer, PermissionDecision,
    };

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
    db.execute(
        "UPDATE turns SET state = 'running' WHERE id = 'turn-pred'",
        (),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE permission_requests
         SET turn_id = 'turn-pred', tool_name = 'codex/commandExecution',
             tool_input = '{\"command\":\"ls\"}'
         WHERE id = 'perm-row-1'",
        (),
    )
    .await
    .unwrap();
    let (orch, responses) = common::orchestrator_with_permission_responses(&temp, db.clone());
    let mut responses = responses.subscribe();

    resolve_permission_request(
        &orch,
        "perm-row-1",
        PermissionAnswer::from_surface(PermissionDecision::Allow, AnswerSurface::ResourcePatch)
            .with_actor("cairn://p/trs/1/1/builder"),
    )
    .await
    .expect("a Codex approval with an open provider turn resolves");

    let (request_id, response) = responses
        .try_recv()
        .expect("the Codex poll's broadcast is delivered");
    assert_eq!(request_id, "perm-row-1");
    assert!(response.contains("accept"), "{response}");
    assert!(
        successor_turn_state(&db, "turn-pred").await.is_none(),
        "no successor is minted while the provider turn is open"
    );
    assert_eq!(
        db.query_text("SELECT state FROM turns WHERE id = 'turn-pred'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("running"),
        "the Codex turn keeps running"
    );
    assert_eq!(
        db.query_one(
            "SELECT COUNT(*) FROM events WHERE event_type = 'tool_result'",
            (),
            |row| row.i64(0)
        )
        .await
        .unwrap(),
        0,
        "no slow-path resume ran"
    );
}

// ── Late answers (cairn/4936) ──────────────────────────────────────────────────────
//
// A permission answered after its inline budget minted its successor already
// `running`, then asked `continue_job_impl` to resume the job, which refused
// because the job had an active turn -- the one just minted. The warm process
// was never driven and the turn sat `running` with no events until an operator
// typed into the composer. These drive the real answer path against the state
// the durable suspend leaves behind.

/// A path-scoped fence crossing as `raise_fence` stores it. Allowing it
/// re-dispatches a `read` with an empty payload, which is harmless here.
fn late_crossing_input(tool_use_id: &str) -> String {
    serde_json::json!({
        "kind": "sensitive_host_read",
        "verb": "read",
        "descriptor": format!("/host/secret-{tool_use_id}"),
        "summary": format!("read of a sensitive host path ({tool_use_id})"),
        "request": {
            "cwd": "/wt",
            "run_id": "run-1",
            "tool": "read",
            "payload": {},
            "tool_use_id": tool_use_id
        },
    })
    .to_string()
}

/// The state a fence crossing leaves once its inline budget expired and the run
/// durably suspended: the predecessor turn `yielded` for the permission, each
/// request still pending against it, the session open, and the warm process
/// parked with no turn. `requests` are `(id, tool_use_id, uri_segment)`.
async fn seed_suspended_at_budget(db: &LocalDb, requests: &[(&str, &str, &str)]) {
    let project_id = common::create_project(db, "late").await;
    for sql in [
        "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
         VALUES ('issue-1', ?1, 1, 'late answer', 'active', 1, 1)",
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
    for sql in [
        "INSERT INTO sessions (id, job_id, status, backend_id, created_at, updated_at)
         VALUES ('session-1', 'job-1', 'open', 'handle-1', 1, 1)",
        "UPDATE runs SET session_id = 'session-1' WHERE id = 'run-1'",
        "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, yield_reason,
                            start_reason, created_at, updated_at)
         VALUES ('turn-pred', 'session-1', 'run-1', 'job-1', 1, 'yielded', 'permission',
                 'user_message', 1, 1)",
        "UPDATE jobs SET current_session_id = 'session-1', current_turn_id = 'turn-pred'
         WHERE id = 'job-1'",
    ] {
        db.execute(sql, ()).await.unwrap();
    }
    for (id, tool_use_id, segment) in requests {
        db.execute(
            "INSERT INTO permission_requests (id, run_id, job_id, tool_use_id, tool_name, tool_input,
                                              status, created_at, turn_id, uri_segment)
             VALUES (?1, 'run-1', 'job-1', ?2, 'read', ?3, 'pending', 1, 'turn-pred', ?4)",
            params![*id, *tool_use_id, late_crossing_input(tool_use_id), *segment],
        )
        .await
        .unwrap();
    }
}

/// The orchestrator over a seeded database, with the job's warm process parked.
fn late_answer_host(
    temp: &tempfile::TempDir,
    db: Arc<LocalDb>,
) -> (
    cairn_core::internal::orchestrator::Orchestrator,
    Arc<Mutex<Vec<u8>>>,
) {
    let orch = orchestrator(temp, db);
    let delivered = common::register_recording_process(&orch, "run-1", "session-1");
    orch.process_state.transition_to_warm("run-1");
    (orch, delivered)
}

async fn answer_late(
    orch: &cairn_core::internal::orchestrator::Orchestrator,
    request_id: &str,
    decision: cairn_core::internal::mcp::handlers::permission::PermissionDecision,
    scope: cairn_core::internal::mcp::handlers::permission::PermissionScope,
) {
    use cairn_core::internal::mcp::handlers::permission::{
        resolve_permission_request, AnswerSurface, PermissionAnswer,
    };
    resolve_permission_request(
        orch,
        request_id,
        PermissionAnswer::from_surface(decision, AnswerSurface::ResourcePatch)
            .with_actor("cairn://p/late/1/1/builder")
            .with_containment_scope(scope),
    )
    .await
    .expect("a late answer resolves");
}

/// `(id, state, run_id, end_reason)` of every `permission_response` turn.
async fn permission_response_turns(
    db: &LocalDb,
) -> Vec<(String, String, Option<String>, Option<String>)> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, state, run_id, end_reason FROM turns
                     WHERE start_reason = 'permission_response' ORDER BY sequence",
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

async fn count(db: &LocalDb, sql: &'static str) -> i64 {
    db.query_one(sql, (), |row| row.i64(0)).await.unwrap()
}

/// The issue's ledger query: running turns with zero events that no live
/// process is serving. After any answer this must be empty.
async fn stranded_running_turns(
    orch: &cairn_core::internal::orchestrator::Orchestrator,
    db: &LocalDb,
) -> Vec<String> {
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

fn delivered_text(delivered: &Arc<Mutex<Vec<u8>>>) -> String {
    String::from_utf8_lossy(&delivered.lock().unwrap()).into_owned()
}

/// One late answer resumes the job: exactly one `permission_response` turn,
/// running on the warm run and served by its process, the synthetic result
/// stored, no visible user event, and the process actually prompted.
async fn assert_late_answer_resumes(
    decision: cairn_core::internal::mcp::handlers::permission::PermissionDecision,
    scope: cairn_core::internal::mcp::handlers::permission::PermissionScope,
) {
    use cairn_core::internal::mcp::handlers::permission::PermissionDecision;
    let label = format!("{decision:?}/{scope:?}");
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_suspended_at_budget(&db, &[("perm-a", "toolu-a", "perm-1")]).await;
    let (orch, delivered) = late_answer_host(&temp, db.clone());

    answer_late(&orch, "perm-a", decision, scope).await;

    let successors = permission_response_turns(&db).await;
    assert_eq!(
        successors.len(),
        1,
        "{label}: exactly one permission_response turn: {successors:?}"
    );
    let (turn_id, state, run_id, _) = &successors[0];
    assert_eq!(state, "running", "{label}: the successor is started");
    assert_eq!(run_id.as_deref(), Some("run-1"), "{label}: on the warm run");
    assert_eq!(
        orch.process_state.get_current_turn_id("run-1").as_deref(),
        Some(turn_id.as_str()),
        "{label}: the warm process serves the successor"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM events WHERE event_type = 'tool_result'"
        )
        .await,
        1,
        "{label}: the synthetic tool_result is stored"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM events WHERE event_type IN ('user', 'user:continuation')"
        )
        .await,
        0,
        "{label}: suppress_user_event is honoured"
    );
    let prompt = delivered_text(&delivered);
    assert!(
        !prompt.trim().is_empty(),
        "{label}: the warm process was prompted"
    );
    if matches!(decision, PermissionDecision::Deny) {
        assert!(
            prompt.contains("Denied by worktree fence"),
            "{label}: the deny reaches the agent: {prompt}"
        );
    }
    assert!(
        stranded_running_turns(&orch, &db).await.is_empty(),
        "{label}: no running turn is left unserved"
    );
}

/// Acceptance 1: a fence permission denied after the inline budget resumes the
/// job. On `main` the successor sat `running` with zero events and the process
/// was never prompted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_fence_deny_once_resumes_the_job_into_its_successor() {
    use cairn_core::internal::mcp::handlers::permission::{PermissionDecision, PermissionScope};
    assert_late_answer_resumes(PermissionDecision::Deny, PermissionScope::Once).await;
}

/// Acceptance 2: the same for every decision and scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_fence_answer_resumes_for_every_decision_and_scope() {
    use cairn_core::internal::mcp::handlers::permission::{PermissionDecision, PermissionScope};
    for (decision, scope) in [
        (PermissionDecision::Deny, PermissionScope::Session),
        (PermissionDecision::Allow, PermissionScope::Once),
        (PermissionDecision::Allow, PermissionScope::Session),
    ] {
        assert_late_answer_resumes(decision, scope).await;
    }
}

/// Acceptance 3, the specimen's shape: two permissions raised by one turn,
/// answered back to back. The first drives the successor; the second finds it
/// served and queues its result rather than minting or stranding a turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_late_answers_for_one_turn_share_one_successor_and_queue_the_second() {
    use cairn_core::internal::mcp::handlers::permission::{PermissionDecision, PermissionScope};
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_suspended_at_budget(
        &db,
        &[
            ("perm-a", "toolu-a", "perm-1"),
            ("perm-b", "toolu-b", "perm-2"),
        ],
    )
    .await;
    let (orch, delivered) = late_answer_host(&temp, db.clone());

    answer_late(
        &orch,
        "perm-a",
        PermissionDecision::Deny,
        PermissionScope::Once,
    )
    .await;
    answer_late(
        &orch,
        "perm-b",
        PermissionDecision::Deny,
        PermissionScope::Once,
    )
    .await;

    let successors = permission_response_turns(&db).await;
    assert_eq!(
        successors.len(),
        1,
        "one successor for one predecessor: {successors:?}"
    );
    assert_eq!(successors[0].1, "running");
    assert_eq!(
        orch.process_state.get_current_turn_id("run-1").as_deref(),
        Some(successors[0].0.as_str())
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM queued_messages WHERE id = 'perm-b'"
        )
        .await,
        1,
        "the second result is queued, keyed by its request"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM events WHERE event_type = 'tool_result'"
        )
        .await,
        2,
        "both answers land their synthetic results"
    );
    assert!(
        delivered_text(&delivered).contains("Denied by worktree fence"),
        "the first answer drove the warm process"
    );
    assert!(
        stranded_running_turns(&orch, &db).await.is_empty(),
        "no running turn with zero events is left unserved after the batch"
    );
}

/// A sibling answer's launch delivered the successor, and then its process
/// stopped reading as serving it: `end_turn()` ran but finalize has not yet
/// written the turn `complete`, or the process is `Busy`. A second answer that
/// lands in that window must queue its result, never cancel a turn that ran.
async fn assert_second_answer_queues_after_sibling_delivered(
    window: &str,
    leave_serving: fn(&cairn_core::internal::orchestrator::Orchestrator),
) {
    use cairn_core::internal::mcp::handlers::permission::{PermissionDecision, PermissionScope};
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_suspended_at_budget(
        &db,
        &[
            ("perm-a", "toolu-a", "perm-1"),
            ("perm-b", "toolu-b", "perm-2"),
        ],
    )
    .await;
    let (orch, delivered) = late_answer_host(&temp, db.clone());

    answer_late(
        &orch,
        "perm-a",
        PermissionDecision::Deny,
        PermissionScope::Once,
    )
    .await;
    let successors = permission_response_turns(&db).await;
    assert_eq!(successors.len(), 1, "{window}: {successors:?}");
    assert_eq!(
        successors[0].1, "running",
        "{window}: the first answer drove it"
    );
    assert!(
        delivered_text(&delivered).contains("Denied by worktree fence"),
        "{window}: the successor's prompt was delivered"
    );
    leave_serving(&orch);
    assert_eq!(
        orch.process_state.get_current_turn_id("run-1"),
        None,
        "{window}: the process no longer reads as serving the successor"
    );

    answer_late(
        &orch,
        "perm-b",
        PermissionDecision::Deny,
        PermissionScope::Once,
    )
    .await;

    let successors = permission_response_turns(&db).await;
    assert_eq!(
        successors.len(),
        1,
        "{window}: no second turn: {successors:?}"
    );
    assert_eq!(
        (successors[0].1.as_str(), successors[0].3.as_deref()),
        ("running", None),
        "{window}: the delivered successor is not cancelled"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM queued_messages WHERE id = 'perm-b'"
        )
        .await,
        1,
        "{window}: the second result is queued for the successor's next boundary"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM permission_requests WHERE resume_error IS NOT NULL"
        )
        .await,
        0,
        "{window}: neither answer records a refusal"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_answer_queues_rather_than_cancel_a_successor_its_sibling_delivered() {
    assert_second_answer_queues_after_sibling_delivered("turn ended, not yet finalized", |orch| {
        orch.process_state.end_turn("run-1");
    })
    .await;
    // `Busy` is entered only from `Idle`: the process ended the turn and is
    // being activated again before finalize wrote it `complete`.
    assert_second_answer_queues_after_sibling_delivered("process busy", |orch| {
        orch.process_state.end_turn("run-1");
        orch.process_state.transition_to_active("run-1");
    })
    .await;
}

/// A launch that starts the successor and then fails to deliver its prompt
/// (here, a warm process whose stdin is broken) settles the successor itself,
/// before releasing the job's launch lock: no lock holder ever sees a started
/// but undelivered `permission_response` turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_launch_that_starts_the_successor_and_fails_to_deliver_settles_it() {
    use cairn_core::internal::mcp::handlers::permission::{PermissionDecision, PermissionScope};
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_suspended_at_budget(&db, &[("perm-a", "toolu-a", "perm-1")]).await;
    let orch = orchestrator(&temp, db.clone());
    common::register_broken_process(&orch, "run-1", "session-1");
    orch.process_state.transition_to_warm("run-1");

    answer_late(
        &orch,
        "perm-a",
        PermissionDecision::Deny,
        PermissionScope::Once,
    )
    .await;

    let successors = permission_response_turns(&db).await;
    assert_eq!(successors.len(), 1, "{successors:?}");
    assert_eq!(
        (
            successors[0].1.as_str(),
            successors[0].2.as_deref(),
            successors[0].3.as_deref()
        ),
        (
            "cancelled",
            Some("run-1"),
            Some("permission_resume_refused")
        ),
        "the launch started the successor on the warm run, then unwound it"
    );
    assert_eq!(
        orch.process_state.get_current_turn_id("run-1"),
        None,
        "the process does not read as serving a turn it never received"
    );
    assert!(
        db.query_text(
            "SELECT resume_error FROM permission_requests WHERE id = 'perm-a'",
            ()
        )
        .await
        .unwrap()
        .is_some(),
        "the refusal is on the permission row"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM queued_messages WHERE id = 'perm-a'"
        )
        .await,
        0,
        "the refused result is not queued into a turn that will never run"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM turns WHERE state = 'running'").await,
        0,
        "no turn is left running"
    );
}

/// Acceptance 4: when the job refuses to continue, the minted successor is
/// settled with a named end reason and the refusal is recorded on the
/// permission row, rather than a `running` turn nothing will ever drive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_late_resume_settles_the_successor_and_records_why() {
    use cairn_core::internal::mcp::handlers::permission::{PermissionDecision, PermissionScope};
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed_suspended_at_budget(&db, &[("perm-a", "toolu-a", "perm-1")]).await;
    db.execute(
        "UPDATE sessions SET status = 'closed', terminal_reason = 'issue_closed' WHERE id = 'session-1'",
        (),
    )
    .await
    .unwrap();
    let (orch, delivered) = late_answer_host(&temp, db.clone());

    answer_late(
        &orch,
        "perm-a",
        PermissionDecision::Deny,
        PermissionScope::Once,
    )
    .await;

    assert_eq!(
        db.query_text(
            "SELECT status FROM permission_requests WHERE id = 'perm-a'",
            ()
        )
        .await
        .unwrap()
        .as_deref(),
        Some("denied"),
        "the answer itself stands"
    );
    let successors = permission_response_turns(&db).await;
    assert_eq!(successors.len(), 1, "{successors:?}");
    assert_eq!(successors[0].1, "cancelled");
    assert_eq!(
        successors[0].3.as_deref(),
        Some("permission_resume_refused")
    );
    let resume_error = db
        .query_text(
            "SELECT resume_error FROM permission_requests WHERE id = 'perm-a'",
            (),
        )
        .await
        .unwrap();
    assert!(
        resume_error
            .as_deref()
            .is_some_and(|reason| reason.contains("closed")),
        "the refusal is on the permission row: {resume_error:?}"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM turns WHERE state = 'running'").await,
        0,
        "no turn is left running"
    );
    assert!(
        delivered_text(&delivered).trim().is_empty(),
        "nothing was prompted"
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
        resume.successor_turn_id().is_none(),
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
    assert!(resume.successor_turn_id().is_none());
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
    insert_fixture(
        &db,
        &Fixture {
            job_owned: true,
            job_has_current_turn: true,
        },
    )
    .await;
    let request_id = db
        .query_one(
            "SELECT id FROM permission_requests WHERE run_id='run-1'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();
    let orch = orchestrator(&temp, Arc::new(db));

    orch.pty_state.register_inline_command(
        "run-1".to_string(),
        "cmd-a".to_string(),
        mock_inline_child(1),
    );
    orch.pty_state.register_inline_command(
        "run-1".to_string(),
        "cmd-b".to_string(),
        mock_inline_child(2),
    );
    assert_eq!(inline_count(&orch, "run-1"), 2);

    suspend_run_for_durable_wait(
        &orch,
        "run-1",
        "permission_wait_suspended",
        cairn_session::agent_process::process::GoalPauseOwner::UserInput { request_id },
    )
    .unwrap();

    assert_eq!(
        inline_count(&orch, "run-1"),
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
